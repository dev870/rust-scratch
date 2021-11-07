use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    // register parquet file with the execution context
    ctx.register_parquet(
        "sample_data",
        "./data/data.parquet",
    ).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT a, b \
        FROM sample_data \
        WHERE d > 1"
        ).unwrap();

    // print the results
    let batches = df.collect().await?;
    print!("{}", batches[0].num_rows());
    Ok(())
}