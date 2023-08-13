mod common;
mod test_util;
use common::new_session;
use std::error::Error;

#[tokio::test]
async fn test_collect_named_table_returns_right_values() -> Result<(), Box<dyn Error>> {
    let session = new_session().await?;
    let _ = session
        .clone()
        .sql(
            r#"
            CREATE TABLE IF NOT EXISTS employees
            USING json
            LOCATION '/opt/spark/examples/src/main/resources/employees.json';
        "#
            .to_owned(),
        )
        .collect()
        .await?;
    let dataframe = session.clone().table("employees".to_string());
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
