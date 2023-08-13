mod common;
mod test_util;
use arrow::util::pretty;
use common::new_session;
use spark_connect_rust::error::SparkError;
use std::error::Error;

#[tokio::test]
async fn test_select_from_missing_table() -> Result<(), Box<dyn Error>> {
    let session = new_session().await?;
    let dataframe = session.sql("SELECT * FROM test".to_owned());
    let result = dataframe.collect().await;
    matches!(result, Err(SparkError::TableOrViewNotFound(_)));
    Ok(())
}

#[tokio::test]
async fn test_select_sql_functions() -> Result<(), Box<dyn Error>> {
    let session = new_session().await?;
    let dataframe = session.sql("SHOW TABLES".to_owned());
    let result = dataframe.collect().await;
    matches!(result, Err(SparkError::HiveCatalogNotEnabled(_)));
    Ok(())
}

#[tokio::test]
async fn test_collect_do_not_fail() -> Result<(), Box<dyn Error>> {
    let dataframe = common::create_employees_dataframe().await?;
    let rows = dataframe.collect().await?;
    pretty::print_batches(&rows)?;
    Ok(())
}

#[tokio::test]
async fn test_collect_returns_right_values() -> Result<(), Box<dyn Error>> {
    let dataframe = common::create_employees_dataframe().await?;
    let rows = dataframe.collect().await?;
    assert_batches_eq!(
        vec![
            "+---------+--------+",
            "| name    | salary |",
            "+---------+--------+",
            "| Michael | 3000   |",
            "| Andy    | 4500   |",
            "| Justin  | 3500   |",
            "| Berta   | 4000   |",
            "+---------+--------+",
        ],
        &rows
    );
    Ok(())
}
