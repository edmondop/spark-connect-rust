use spark_connect_rust::RemoteSparkSession;
use spark_connect_rust::{dataframe::DataFrame, error::SparkSessionCreationError};
use std::collections::HashMap;
use std::error::Error;
use std::{env, rc::Rc};

pub async fn new_session() -> Result<Rc<RemoteSparkSession>, SparkSessionCreationError> {
    let spark_connect_address =
        env::var("SPARK_CONNECT_ADDRESS").unwrap_or("sc://localhost:15002".to_string());
    RemoteSparkSession::new(spark_connect_address, None).await
}

pub async fn create_employees_dataframe() -> Result<DataFrame, Box<dyn Error>> {
    let session = new_session().await?;
    let dataframe = session.sql(
        "SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`".to_owned(),
    );
    Ok(dataframe)
}

pub async fn read_dataframe_from_path(
    session: Rc<RemoteSparkSession>,
    format: String,
    options: HashMap<String, String>,
    path: String,
) -> DataFrame {
    session
        .read()
        .format(format)
        .options(options)
        .load(vec![path])
}
