mod common;
mod test_util;

use crate::common::new_session;
use rand::Rng;
use spark_connect_rust::dataframe::DataFrame;
use spark_connect_rust::spark::write_operation::SaveMode;
use spark_connect_rust::{error::SparkError, spark};
use std::collections::HashMap;
use std::error::Error;
use std::fs;

#[tokio::test]
async fn test_select_works() -> Result<(), Box<dyn Error>> {
    let dataframe = common::create_employees_dataframe().await?;
    let rows = dataframe.select(vec!["name".to_string()]).collect().await?;
    assert_batches_eq!(
        vec![
            "+---------+",
            "| name    |",
            "+---------+",
            "| Michael |",
            "| Andy    |",
            "| Justin  |",
            "| Berta   |",
            "+---------+",
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn test_select_expr_works() -> Result<(), Box<dyn Error>> {
    let dataframe = common::create_employees_dataframe().await?;
    let rows = dataframe
        .select_expr(vec!["UPPER(name) as NAME".to_string()])
        .collect()
        .await?;
    assert_batches_eq!(
        vec![
            "+---------+",
            "| NAME    |",
            "+---------+",
            "| MICHAEL |",
            "| ANDY    |",
            "| JUSTIN  |",
            "| BERTA   |",
            "+---------+",
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn test_select_wrong_expr_error() -> Result<(), Box<dyn Error>> {
    let dataframe = common::create_employees_dataframe().await?;
    let result = dataframe
        .select(vec!["UPPER(name) as NAME".to_string()])
        .collect()
        .await;
    matches!(result, Err(SparkError::UnresolvedColumnWithSuggestion(_)));
    Ok(())
}

async fn write_to_temp_folder(
    df: &DataFrame,
    dir: &String,
    format: String,
    file_name: String,
    options: HashMap<String, String>,
    mode: Option<SaveMode>,
) -> Result<String, SparkError> {
    let file_path = format!("{}/{}", dir, file_name);
    let writer = if let Some(mode) = mode {
        df.write().format(format).mode(mode)
    } else {
        df.write().format(format)
    };
    writer
        .options(options)
        .save(Some(spark::write_operation::SaveType::Path(
            file_path.clone(),
        )))
        .await?;
    Ok(file_path.clone())
}

fn random_path() -> String {
    // Short path to be used in the Spark current working directory /opt/spark/work-dir
    let rng = rand::thread_rng();
    let random_string: String = rng
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    let temp_dir = format!("tmp_{}", random_string);
    fs::create_dir(&temp_dir).expect("Failed to create temporary directory");
    temp_dir
}

#[tokio::test]
async fn test_double_write_fails() -> Result<(), Box<dyn Error>> {
    let session = new_session().await?;
    let input_df = session.clone().sql(
        "SELECT * FROM json.`/opt/spark/examples/src/main/resources/employees.json`".to_owned(),
    );
    let dir = random_path();
    let format = "csv".to_string();
    let file_name = "employees".to_string();
    write_to_temp_folder(
        &input_df,
        &dir,
        format.clone(),
        file_name.clone(),
        HashMap::new(),
        None,
    )
    .await?;
    let result = write_to_temp_folder(
        &input_df,
        &dir,
        format.clone(),
        file_name.clone(),
        HashMap::new(),
        None,
    )
    .await;
    matches!(result, Err(SparkError::UnresolvedColumnWithSuggestion(_)));
    Ok(())
}

#[tokio::test]
async fn test_write_works() -> Result<(), Box<dyn Error>> {
    let session = new_session().await?;
    let input_df = session.clone().sql(
        "SELECT name FROM json.`/opt/spark/examples/src/main/resources/employees.json`".to_owned(),
    );
    let dir = random_path();
    let format = "csv".to_string();
    let file_name = "employees".to_string();
    let header_options: HashMap<_, _> = vec![("header".to_string(), "true".to_string())]
        .into_iter()
        .collect();
    let file_path = write_to_temp_folder(
        &input_df,
        &dir,
        format.clone(),
        file_name.clone(),
        header_options,
        None,
    )
    .await?;
    let output_dataframe = common::read_dataframe_from_path(
        session.clone(),
        "csv".to_string(),
        vec![("header".to_string(), "true".to_string())]
            .into_iter()
            .collect(),
        file_path,
    )
    .await;
    let rows = output_dataframe.collect().await?;
    assert_batches_eq!(
        vec![
            "+---------+",
            "| name    |",
            "+---------+",
            "| Michael |",
            "| Andy    |",
            "| Justin  |",
            "| Berta   |",
            "+---------+",
        ],
        &rows
    );
    Ok(())
}
