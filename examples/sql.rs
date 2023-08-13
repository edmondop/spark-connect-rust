use arrow::util::pretty;
use spark_connect_rust;
use spark_connect_rust::RemoteSparkSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = RemoteSparkSession::new("http://localhost:15002".to_owned(), None).await?;
    let df = spark.sql(String::from(
        "select * from json.`/opt/spark/examples/src/main/resources/employees.json`",
    ));
    let result = df.select(vec!["name".to_string()]).collect().await?;
    pretty::print_batches(result.as_slice())?;
    Ok(())
}
