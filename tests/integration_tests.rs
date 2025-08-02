use datafusion::prelude::*;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion_expr::ScalarUDF;
use regexp_extract_datafusion::regexp_extract::RegexpExtract;
use std::sync::Arc;

#[tokio::test]
async fn test_regexp_extract_integration() {
    // 1. Create a SessionContext
    let ctx = SessionContext::new();

    // 2. Register the UDF
    ctx.register_udf(ScalarUDF::new_from_impl(RegexpExtract::new()));

    // 3. Create test data and a MemTable
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("pattern", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "100-200",
                "300-400",
                "no-match",
                "500-600",
            ])),
            Arc::new(StringArray::from(vec![
                r"(\d+)-(\d+)",
                r"(\d+)-(\d+)",
                r"(\d+)",
                r"(\d+)-(\d+)",
            ])),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("test_table", Arc::new(provider))
        .unwrap();

    // 4. Execute a SQL query
    let df = ctx
        .sql("SELECT regexp_extract(text, pattern, 1) FROM test_table")
        .await
        .unwrap();

    // 5. Collect and assert the results
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let string_array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected a StringArray");

    let expected = StringArray::from(vec![
        Some("100"),
        Some("300"),
        Some(""), // No match
        Some("500"),
    ]);

    assert_eq!(string_array, &expected, "The extracted values did not match the expected output.");
}

#[tokio::test]
async fn test_regexp_extract_dataframe_api() {
    // 1. Create a SessionContext and register the UDF
    let ctx = SessionContext::new();
    let udf = ScalarUDF::new_from_impl(RegexpExtract::new());
    ctx.register_udf(udf.clone());

    // 2. Create a RecordBatch
    let schema = Arc::new(Schema::new(vec![Field::new(
        "text",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "alpha-10", "beta-20", "gamma-30",
        ]))],
    )
    .unwrap();

    // 3. Create a DataFrame from the RecordBatch
    let df = ctx.read_batch(batch).unwrap();

    // 4. Use the UDF with the DataFrame API
    let pattern = lit(r"([a-z]+)-(\d+)");
    let index = lit(2_i64);
    let df = df
        .select(vec![udf.call(vec![col("text"), pattern, index])])
        .unwrap();

    // 5. Collect and assert results
    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let string_array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected a StringArray");

    let expected = StringArray::from(vec![Some("10"), Some("20"), Some("30")]);
    assert_eq!(string_array, &expected);
} 