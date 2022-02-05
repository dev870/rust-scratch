// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ObjectStore implementation for the Amazon S3 API

use std::io::Read;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream, AsyncRead};

use datafusion::datasource::object_store::SizedFile;
use datafusion::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::error::{DataFusionError, Result};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Builder, Client, Endpoint, Region, RetryConfig};
use aws_smithy_async::rt::sleep::AsyncSleep;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_smithy_types_convert::date_time::DateTimeExt;
use aws_types::credentials::SharedCredentialsProvider;
use http::Uri;
use aws_types::credentials::Credentials;
use datafusion::datasource::listing::*;
use datafusion::prelude::ExecutionContext;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use bytes::Buf;

pub mod error;
use crate::error::S3Error;

/// new_client creates a new aws_sdk_s3::Client
/// this uses aws_config::load_from_env() as a base config then allows users to override specific settings if required
///
/// an example use case for overriding is to specify an endpoint which is not Amazon S3 such as MinIO or Ceph.
async fn new_client(
    credentials_provider: Option<SharedCredentialsProvider>,
    region: Option<Region>,
    endpoint: Option<Endpoint>,
    retry_config: Option<RetryConfig>,
    sleep: Option<Arc<dyn AsyncSleep>>,
    timeout_config: Option<TimeoutConfig>,
) -> Client {
    let config = aws_config::load_from_env().await;

    let region_provider = RegionProviderChain::first_try(region)
        .or_default_provider()
        .or_else(Region::new("us-west-2"));

    let mut config_builder = Builder::from(&config).region(region_provider.region().await);

    if let Some(credentials_provider) = credentials_provider {
        config_builder = config_builder.credentials_provider(credentials_provider);
    }

    if let Some(endpoint) = endpoint {
        config_builder = config_builder.endpoint_resolver(endpoint);
    }

    if let Some(retry_config) = retry_config {
        config_builder = config_builder.retry_config(retry_config);
    }

    if let Some(sleep) = sleep {
        config_builder = config_builder.sleep_impl(sleep);
    }

    if let Some(timeout_config) = timeout_config {
        config_builder = config_builder.timeout_config(timeout_config);
    };

    let config = config_builder.build();
    Client::from_conf(config)
}

#[derive(Debug)]
// ObjectStore implementation for the Amazon S3 API
pub struct S3FileSystem {
    credentials_provider: Option<SharedCredentialsProvider>,
    region: Option<Region>,
    endpoint: Option<Endpoint>,
    retry_config: Option<RetryConfig>,
    sleep: Option<Arc<dyn AsyncSleep>>,
    timeout_config: Option<TimeoutConfig>,
    client: Client,
}

impl S3FileSystem {
    pub async fn new(
        credentials_provider: Option<SharedCredentialsProvider>,
        region: Option<Region>,
        endpoint: Option<Endpoint>,
        retry_config: Option<RetryConfig>,
        sleep: Option<Arc<dyn AsyncSleep>>,
        timeout_config: Option<TimeoutConfig>,
    ) -> Self {
        Self {
            credentials_provider: credentials_provider.clone(),
            region: region.clone(),
            endpoint: endpoint.clone(),
            retry_config: retry_config.clone(),
            sleep: sleep.clone(),
            timeout_config: timeout_config.clone(),
            client: new_client(credentials_provider, region, endpoint, None, None, None).await,
        }
    }
}

#[async_trait]
impl ObjectStore for S3FileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let (bucket, prefix) = match prefix.split_once("/") {
            Some((bucket, prefix)) => (bucket.to_owned(), prefix),
            None => (prefix.to_owned(), ""),
        };

        let objects = self
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(prefix)
            .send()
            .await
            .map_err(|err| DataFusionError::External(Box::new(S3Error::AWS(format!("{:?}", err)))))?
            .contents()
            .unwrap_or_default()
            .to_vec();

        let result = stream::iter(objects.into_iter().map(move |object| {
            Ok(FileMeta {
                sized_file: SizedFile {
                    path: format!("{}/{}", &bucket, object.key().unwrap_or("")),
                    size: object.size() as u64,
                },
                last_modified: object
                    .last_modified()
                    .map(|last_modified| last_modified.to_chrono_utc()),
            })
        }));

        Ok(Box::pin(result))
    }

    async fn list_dir(&self, _prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(AmazonS3FileReader::new(
            self.credentials_provider.clone(),
            self.region.clone(),
            self.endpoint.clone(),
            self.retry_config.clone(),
            self.sleep.clone(),
            self.timeout_config.clone(),
            file,
        )?))
    }
}

#[allow(dead_code)]
impl S3FileSystem {
    pub async fn default() -> Self {
        S3FileSystem::new(None, None, None, None, None, None).await
    }
}

struct AmazonS3FileReader {
    credentials_provider: Option<SharedCredentialsProvider>,
    region: Option<Region>,
    endpoint: Option<Endpoint>,
    retry_config: Option<RetryConfig>,
    sleep: Option<Arc<dyn AsyncSleep>>,
    timeout_config: Option<TimeoutConfig>,
    file: SizedFile,
}

impl AmazonS3FileReader {
    #[allow(clippy::too_many_arguments)]
    fn new(
        credentials_provider: Option<SharedCredentialsProvider>,
        region: Option<Region>,
        endpoint: Option<Endpoint>,
        retry_config: Option<RetryConfig>,
        sleep: Option<Arc<dyn AsyncSleep>>,
        timeout_config: Option<TimeoutConfig>,
        file: SizedFile,
    ) -> Result<Self> {
        Ok(Self {
            credentials_provider,
            region,
            endpoint,
            retry_config,
            sleep,
            timeout_config,
            file,
        })
    }
}

#[async_trait]
impl ObjectReader for AmazonS3FileReader {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> Result<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>> {
        let credentials_provider = self.credentials_provider.clone();
        let region = self.region.clone();
        let endpoint = self.endpoint.clone();
        let retry_config = self.retry_config.clone();
        let sleep = self.sleep.clone();
        let timeout_config = self.timeout_config.clone();
        let file_path = self.file.path.clone();

        // once the async chunk file readers have been implemented this complexity can be removed
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                // aws_sdk_s3::Client appears bound to the runtime and will deadlock if cloned from the main runtime
                let client = new_client(
                    credentials_provider,
                    region,
                    endpoint,
                    retry_config,
                    sleep,
                    timeout_config,
                )
                .await;

                let (bucket, key) = match file_path.split_once("/") {
                    Some((bucket, prefix)) => (bucket, prefix),
                    None => (file_path.as_str(), ""),
                };

                let get_object = client.get_object().bucket(bucket).key(key);
                let resp = if length > 0 {
                    // range bytes requests are inclusive
                    get_object
                        .range(format!("bytes={}-{}", start, start + (length - 1) as u64))
                        .send()
                        .await
                } else {
                    get_object.send().await
                };

                let bytes = match resp {
                    Ok(res) => {
                        let data = res.body.collect().await;
                        match data {
                            Ok(data) => Ok(data.into_bytes()),
                            Err(err) => Err(DataFusionError::External(Box::new(S3Error::AWS(
                                format!("{:?}", err),
                            )))),
                        }
                    }
                    Err(err) => Err(DataFusionError::External(Box::new(S3Error::AWS(format!(
                        "{:?}",
                        err
                    ))))),
                };

                tx.send(bytes).unwrap();
            })
        });

        let bytes = rx.recv_timeout(Duration::from_secs(10)).map_err(|err| {
            DataFusionError::External(Box::new(S3Error::AWS(format!("{:?}", err))))
        })??;

        Ok(Box::new(bytes.reader()))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

const ACCESS_KEY_ID: &str = "minioadmin";
const SECRET_ACCESS_KEY: &str = "minioadmin";
const PROVIDER_NAME: &str = "Static";
const MINIO_ENDPOINT: &str = "http://localhost:9000";

// Test that a SQL query can be executed on a Parquet file that was read from `S3FileSystem`
#[tokio::main]
async fn main() -> Result<()> {
    let s3_file_system = Arc::new(
        S3FileSystem::new(
            Some(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                PROVIDER_NAME,
            ))),
            None,
            Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
            None,
            None,
            None,
        )
        .await,
    );

    let filename = "data/";

    let listing_options = ListingOptions {
        format: Arc::new(ParquetFormat::default()),
        collect_stat: true,
        file_extension: "parquet".to_owned(),
        target_partitions: num_cpus::get(),
        table_partition_cols: vec![],   
    };

    let resolved_schema = listing_options
        .infer_schema(s3_file_system.clone(), filename)
        .await?;

    let table = ListingTable::new(
        s3_file_system,
        filename.to_owned(),
        resolved_schema,
        listing_options,
    );

    let mut ctx = ExecutionContext::new();

    ctx.register_table("tbl", Arc::new(table)).unwrap();

    let batches = ctx.sql("SELECT * FROM tbl").await?;
    batches.show().await?;

    Ok(())
}