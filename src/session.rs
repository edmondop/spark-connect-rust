use crate::arrow::deserialize;
use crate::dataframe::{DataFrame, DataFrameReader};
use crate::error::{NotImplementedYetError, SparkSessionCreationError};
use crate::error::{SparkError, UnexpectedError};
use crate::plan::SqlPlan;
use crate::spark;
use crate::spark::execute_plan_response::ArrowBatch;
use crate::spark::execute_plan_response::Metrics;
use crate::spark::DataType;
use crate::spark::ExecutePlanResponse;
use arrow::record_batch::RecordBatch;
use prost_types::Any;
use spark::spark_connect_service_client::SparkConnectServiceClient;
use std::rc::Rc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Request;
use tonic::Streaming;
use uuid::Uuid;

pub struct Context {
    pub user_id: String,
    pub user_name: String,
    pub extensions: Vec<Any>,
    pub client_type: String,
}

pub struct RemoteSparkSession {
    client: Mutex<SparkConnectServiceClient<Channel>>,
    session_id: String,
    context: Option<Context>,
}

impl RemoteSparkSession {
    pub async fn new(
        host: String,
        user_context: Option<Context>,
    ) -> Result<Rc<RemoteSparkSession>, SparkSessionCreationError> {
        let client = SparkConnectServiceClient::connect(host).await?;
        Ok(Rc::new(RemoteSparkSession {
            client: Mutex::new(client),
            session_id: Uuid::new_v4().to_string(), // Coherent with Python Spark Connect
            context: user_context,
        }))
    }

    pub fn table(self: Rc<Self>, table_name: String) -> DataFrame {
        self.read().table(table_name)
    }

    pub fn sql(self: Rc<Self>, sql: String) -> DataFrame {
        DataFrame {
            session: self,
            plan: Box::new(SqlPlan { query: sql }),
        }
    }

    pub fn read(self: Rc<Self>) -> DataFrameReader {
        DataFrameReader::new(self)
    }

    fn internal_user_context(&self) -> Option<spark::UserContext> {
        return self.context.as_ref().map(|uc| spark::UserContext {
            user_id: uc.user_id.clone(),
            user_name: uc.user_name.clone(),
            extensions: uc.extensions.clone(),
        });
    }

    fn build_request(&self, opt_type: spark::plan::OpType) -> spark::ExecutePlanRequest {
        spark::ExecutePlanRequest {
            session_id: self.session_id.clone(),
            client_type: self.context.as_ref().map(|ctx| ctx.client_type.clone()),
            user_context: self.internal_user_context(),
            plan: Some(spark::Plan {
                op_type: Some(opt_type),
            }),
        }
    }

    async fn execute_opt(
        &self,
        opt_type: spark::plan::OpType,
    ) -> Result<Streaming<ExecutePlanResponse>, SparkError> {
        let req = self.build_request(opt_type);
        let mut client = self.client.lock().await;
        let res = client.execute_plan(Request::new(req)).await?;
        Ok(res.into_inner())
    }

    pub(crate) async fn save(
        &self,
        write_operation: spark::WriteOperation,
    ) -> Result<(), SparkError> {
        let cmd = spark::Command {
            command_type: Some(spark::command::CommandType::WriteOperation(write_operation)),
        };
        self.execute_opt(spark::plan::OpType::Command(cmd)).await?;
        Ok(())
    }

    pub(crate) async fn fetch(&self, rel: spark::Relation) -> Result<Vec<RecordBatch>, SparkError> {
        let mut stream = self.execute_opt(spark::plan::OpType::Root(rel)).await?;
        let mut collector = Collector::new();
        while let Some(resp) = stream.message().await? {
            collector.process(&resp)?;
        }
        collector.records()
    }
}

#[derive(Debug)]
pub struct Collector {
    schema: Option<DataType>,
    data: Option<ArrowBatch>,
    metrics: Option<Metrics>,
}

impl Collector {
    pub fn new() -> Collector {
        Collector {
            schema: None,
            data: None,
            metrics: None,
        }
    }

    pub fn process(&mut self, response: &ExecutePlanResponse) -> Result<(), SparkError> {
        if let Some(schema) = response.schema.as_ref() {
            self.schema = Some(schema.clone());
        }
        if let Some(metrics) = response.metrics.as_ref() {
            self.metrics = Some(metrics.clone());
        }
        if let Some(data) = response.response_type.as_ref() {
            match data {
                spark::execute_plan_response::ResponseType::ArrowBatch(batch) => {
                    self.data = Some(batch.clone());
                }
                _ => {
                    return Err(SparkError::NotImplementedYet(NotImplementedYetError(
                        format!("Unhandled response type {:?}", self),
                    )));
                }
            }
        }
        Ok(())
    }

    fn records(mut self) -> Result<Vec<RecordBatch>, SparkError> {
        if let Some(data) = self.data.take() {
            return deserialize(data);
        }
        Err(SparkError::Unexpected(UnexpectedError(format!(
            "Unexpected state {:?}",
            &self
        ))))
    }
}
