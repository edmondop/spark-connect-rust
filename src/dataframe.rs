use crate::error::SparkError;
use crate::plan;
use crate::session;
use crate::spark;
use crate::spark::write_operation;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

use spark::expression::ExprType;
use std::rc::Rc;
pub struct DataFrame {
    pub(crate) session: Rc<session::RemoteSparkSession>,
    pub(crate) plan: Box<dyn plan::Plan>,
}

pub struct DataFrameReader {
    format: Option<String>,
    schema: Option<String>,
    options: HashMap<String, String>,
    session: Rc<session::RemoteSparkSession>,
}

impl DataFrameReader {
    pub fn new(session: Rc<session::RemoteSparkSession>) -> Self {
        DataFrameReader {
            session,
            schema: None,
            format: None,
            options: HashMap::new(),
        }
    }

    pub fn schema(mut self, schema: Option<String>) -> Self {
        self.schema = schema;
        self
    }

    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.options = options;
        self
    }

    pub fn format(mut self, format: String) -> Self {
        self.format = Some(format);
        self
    }

    pub fn table(self, table_name: String) -> DataFrame {
        DataFrame {
            session: self.session,
            plan: Box::new(plan::NamedTableReadPlan {
                table_name,
                options: self.options,
            }),
        }
    }

    pub fn load(self, paths: Vec<String>) -> DataFrame {
        DataFrame {
            session: self.session,
            plan: Box::new(plan::LoadPlan {
                paths,
                format: self.format,
                options: self.options,
                schema: self.schema,
            }),
        }
    }
}

impl DataFrame {
    pub async fn collect(&self) -> Result<Vec<RecordBatch>, SparkError> {
        let rows = self.session.fetch(self.plan.collect()).await?;
        Ok(rows)
    }

    pub fn select(&self, attrs: Vec<String>) -> DataFrame {
        self.project(attrs, |attr| {
            ExprType::UnresolvedAttribute(spark::expression::UnresolvedAttribute {
                unparsed_identifier: attr,
                plan_id: None,
            })
        })
    }

    pub fn select_expr(&self, exprs: Vec<String>) -> DataFrame {
        self.project(exprs, |expr| {
            ExprType::ExpressionString(spark::expression::ExpressionString { expression: expr })
        })
    }

    fn project(
        &self,
        attrs: Vec<String>,
        builder: fn(String) -> spark::expression::ExprType,
    ) -> DataFrame {
        let expressions = attrs
            .iter()
            .map(|attr| spark::Expression {
                expr_type: Some(builder(attr.clone())),
            })
            .collect();

        let plan = Box::new(plan::Project {
            expressions,
            input: self.plan.clone(),
        });
        DataFrame {
            session: self.session.clone(),
            plan,
        }
    }

    pub fn write(&self) -> DataFrameWriter {
        DataFrameWriter::new(self.plan.clone(), self.session.clone())
    }
}

pub struct DataFrameWriter {
    bucket_by: Option<write_operation::BucketBy>,
    format: Option<String>,
    mode: write_operation::SaveMode,
    options: HashMap<String, String>,
    partitioning_columns: Vec<String>,
    plan: Box<dyn plan::Plan>,
    session: Rc<session::RemoteSparkSession>,
    sort_column_names: Vec<String>,
}
impl DataFrameWriter {
    pub fn new(plan: Box<dyn plan::Plan>, session: Rc<session::RemoteSparkSession>) -> Self {
        DataFrameWriter {
            bucket_by: None,
            format: None,
            mode: write_operation::SaveMode::default(),
            options: HashMap::new(),
            partitioning_columns: vec![],
            plan,
            session,
            sort_column_names: vec![],
        }
    }

    pub fn bucket_by(mut self, bucket_by: write_operation::BucketBy) -> Self {
        self.bucket_by = Some(bucket_by);
        self
    }

    pub fn format(mut self, format: String) -> Self {
        self.format = Some(format);
        self
    }

    pub fn mode(mut self, mode: write_operation::SaveMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.options = options;
        self
    }

    /// save takes one parameter, save_type, which should be not set if the
    /// destination is neither a path nor a table, such as jdbc and noop,

    pub async fn save(
        self,
        save_type: Option<write_operation::SaveType>,
    ) -> Result<(), SparkError> {
        self.session
            .save(spark::WriteOperation {
                input: Some(self.plan.collect()),
                source: self.format,
                mode: self.mode.into(),
                sort_column_names: self.sort_column_names,
                partitioning_columns: self.partitioning_columns,
                bucket_by: self.bucket_by,
                options: self.options,
                save_type,
            })
            .await?;
        Ok(())
    }
}
