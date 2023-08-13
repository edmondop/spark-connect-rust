mod common;
mod test_util;
use common::new_session;
use std::error::Error;

#[tokio::test]
async fn test_load_works() -> Result<(), Box<dyn Error>> {
    let session = new_session().await?;
    let rows = session
        .clone()
        .read()
        .format("json".to_string())
        .load(vec![
            "/opt/spark/examples/src/main/resources/employees.json".to_string(),
        ])
        .collect()
        .await?;

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
