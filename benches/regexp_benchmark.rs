use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_expr::ScalarUDF;
use regexp_extract_datafusion::regexp_extract::RegexpExtract;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_test_df(ctx: &SessionContext, num_rows: usize) -> DataFrame {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "text",
        DataType::Utf8,
        false,
    )]));

    let data = (0..num_rows)
        .map(|i| format!("user-agent-{}", i))
        .collect::<Vec<_>>();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(data))],
    )
    .unwrap();

    ctx.read_batch(batch).unwrap()
}

fn benchmark_regexp_extract(c: &mut Criterion) {
    let mut group = c.benchmark_group("regexp_extract");

    let rt = Runtime::new().unwrap();

    for &num_rows in &[100, 1_000, 10_000] {
        group.bench_function(format!("{} rows", num_rows), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    let udf = ScalarUDF::new_from_impl(RegexpExtract::new());
                    ctx.register_udf(udf.clone());

                    let df = create_test_df(&ctx, num_rows);
                    let pattern = lit(r"user-agent-(\d+)");
                    let index = lit(1_i64);

                    let df = df
                        .select(vec![udf.call(vec![col("text"), pattern, index])])
                        .unwrap();

                    // Use black_box to ensure the compiler doesn't optimize away the computation
                    black_box(df.collect().await.unwrap());
                })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_regexp_extract);
criterion_main!(benches);
