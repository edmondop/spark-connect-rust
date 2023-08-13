use arrow::util::pretty;
use spark_connect_rust;
use spark_connect_rust::RemoteSparkSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = RemoteSparkSession::new("http://localhost:15002".to_owned(), None).await?;
    let df = spark.read().format("json".to_string()).load(vec![
        "/opt/spark/examples/src/main/resources/employees.json".to_string(),
    ]);
    let result = df
        .select_expr(vec![
            "name".to_string(),
            "salary".to_string(),
            "salary * 0.1 as bonus".to_string(),
        ])
        .collect()
        .await?;
    pretty::print_batches(result.as_slice())?;
    Ok(())
}
