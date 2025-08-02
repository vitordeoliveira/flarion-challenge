//! An example program that demonstrates the `regexp_extract` User Defined Function.

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion_expr::ScalarUDF;
use regexp_extract_datafusion::regexp_extract::RegexpExtract;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("--- DataFusion `regexp_extract` UDF Demo ---");

    // 1. Create a SessionContext
    let ctx = SessionContext::new();

    // 2. Create and register the User Defined Function
    let udf = ScalarUDF::new_from_impl(RegexpExtract::new());
    ctx.register_udf(udf.clone());

    // 3. Create a MemTable with some sample data
    let schema = Arc::new(Schema::new(vec![Field::new(
        "http_log",
        DataType::Utf8,
        false,
    )]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "1.2.3.4 - GET /index.html 200",
            "5.6.7.8 - POST /api/data 404",
            "9.10.11.12 - GET /static/image.png 304",
            "No match here",
        ]))],
    )
    .unwrap();
    let provider = MemTable::try_new(schema, vec![vec![data]]).unwrap();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    // --- Demo 1: Using the SQL Interface ---
    println!("\n--- 1. SQL API Demo ---");
    println!("Extracting IP addresses, HTTP methods, and status codes from a log string.");

    let df_sql = ctx
        .sql(
            r#"
        SELECT
            http_log,
            regexp_extract(http_log, '(\d+\.\d+\.\d+\.\d+)', 0) AS ip_address,
            regexp_extract(http_log, ' - ([A-Z]+) ', 1) AS http_method,
            regexp_extract(http_log, ' (\d{3})$', 0) AS status_code
        FROM logs
        "#,
        )
        .await?;

    df_sql.show().await?;

    // --- Demo 2: Using the DataFrame API ---
    println!("\n--- 2. DataFrame API Demo ---");
    println!("Extracting the path from the URL.");

    let df_api = ctx.table("logs").await?.select(vec![
        col("http_log"),
        udf.call(vec![
            col("http_log"),
            lit(r#" - [A-Z]+ (/[^ ]*)"#),
            lit(1_i64),
        ])
        .alias("path"),
    ])?;

    df_api.show().await?;

    println!("\nDemo complete.");
    Ok(())
}
