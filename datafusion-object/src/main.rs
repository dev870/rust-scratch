use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use std::sync::Arc;
use datafusion::datasource::S3Options;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();
    let file_format = ParquetFormat::default().with_enable_pruning(true);

    let scheme = "s3";

    let s3_store = 
         Arc::new(MinioStore::new(s3_options.endpoint.to_string() ,
            s3_options.access_key.to_string(), 
            s3_options.secret_key.to_string(), 
            s3_options.bucket.to_string()));

    ctx.register_object_store(scheme,  s3_store);
 
    // let df = ctx.read_parquet(path).await?;
    ctx.register_object_store(scheme, s3_store);

    // execute the query
    let df = ctx.sql("SELECT * FROM my_table where d < 3 or d > 33").await?;

    // print the results
    df.show().await?;
    Ok(())
}
