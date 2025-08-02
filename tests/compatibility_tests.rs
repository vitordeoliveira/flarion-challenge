use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_expr::ScalarUDF;
use regexp_extract_datafusion::regexp_extract::RegexpExtract;
use std::sync::Arc;

async fn run_compatibility_test(
    input: Vec<Option<&str>>,
    pattern: &str,
    index: i64,
) -> StringArray {
    let ctx = SessionContext::new();
    let udf = ScalarUDF::new_from_impl(RegexpExtract::new());
    ctx.register_udf(udf.clone());

    let schema =
        datafusion::arrow::datatypes::Schema::new(vec![datafusion::arrow::datatypes::Field::new(
            "text",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        )]);

    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(input))]).unwrap();

    let df = ctx.read_batch(batch).unwrap();

    let df = df
        .select(vec![udf.call(vec![col("text"), lit(pattern), lit(index)])])
        .unwrap();

    let results = df.collect().await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);

    column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn spark_compat_no_match_returns_empty_string() {
    let result = run_compatibility_test(vec![Some("abc")], r"(\d+)", 1).await;
    let expected = StringArray::from(vec![Some("")]);
    assert_eq!(result, expected);
}

#[tokio::test]
async fn spark_compat_null_input_propagates_null() {
    let result = run_compatibility_test(vec![None], r"(\w+)", 1).await;
    let expected = StringArray::from(vec![None as Option<&str>]);
    assert_eq!(result, expected);
}

#[tokio::test]
async fn spark_compat_index_out_of_bounds_returns_empty_string() {
    let result = run_compatibility_test(vec![Some("a-b")], r"(a)-(b)", 3).await;
    let expected = StringArray::from(vec![Some("")]);
    assert_eq!(result, expected);
}

#[tokio::test]
async fn spark_compat_no_capture_group_returns_empty_string() {
    let result = run_compatibility_test(vec![Some("abc")], r"a.c", 1).await;
    let expected = StringArray::from(vec![Some("")]);
    assert_eq!(result, expected);
}
