
use datafusion::prelude::*;
use regexp_extract_datafusion::regexp_extract::RegexpExtract;
use datafusion_expr::ScalarUDF;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use datafusion::datasource::MemTable;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a new SessionContext
    let ctx = SessionContext::new();

    // Register the UDF
    ctx.register_udf(ScalarUDF::from(RegexpExtract::new()));

    // Create a RecordBatch
    let batch = RecordBatch::try_from_iter(vec![(
        "text",
        Arc::new(StringArray::from(vec!["100-200", "300-400"])) as ArrayRef,
    )])?;

    // Create a MemTable
    let table_provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("my_table", Arc::new(table_provider))?;

    // Execute a query using the UDF
    let df_result = ctx
        .sql("SELECT regexp_extract(text, '(\\d+)-(\\d+)', 1) as extracted FROM my_table")
        .await?;

    // Print the results
    df_result.show().await?;

    Ok(())
}
