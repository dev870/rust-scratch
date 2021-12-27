use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use std::sync::Arc;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();
    let file_format = ParquetFormat::default().with_enable_pruning(true);

    let listing_options = ListingOptions {
        file_extension: ".parquet".to_owned(),
        format: Arc::new(file_format),
        table_partition_cols: vec![],
        collect_stat: true,
        target_partitions: 1,
    };

    ctx.register_listing_table(
        "my_table",
        &format!("file://{}", "./data/"),
        listing_options,
        None,
    ).await.unwrap();
    
    // execute the query
    let df = ctx.sql("SELECT * FROM my_table where d < 3 or d > 33").await?;

    // print the results
    df.show().await?;
    Ok(())
}
