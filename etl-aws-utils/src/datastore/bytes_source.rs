use super::create_client;
use super::rusoto_s3::*;
use super::DataStoreError;
use super::ProfileProvider;
use super::Region;
use super::S3Client;
use super::{AsyncBufReadExt, BufReader};
use crate::datastore::s3_write_bytes_multipart;
use etl_core::datastore::*;
use etl_core::deps::async_trait;
use etl_core::deps::bytes::Bytes;
use etl_core::deps::{anyhow, tokio, tokio::sync::mpsc::channel};
use etl_core::job_manager::JobManagerTx;
use tokio::task::JoinHandle;

pub struct S3Storage {
    pub s3_bucket: String,
    pub s3_keys: Vec<String>,
    /// When using this as a DataOutput this must be filled in
    pub s3_output_key: Option<String>,
    /// path to the credentials file to access aws
    pub credentials_path: Option<String>,
    pub region: Region,
}

impl S3Storage {
    pub async fn get_object_head(&self, key: &str) -> Result<HeadObjectOutput, DataStoreError> {
        let p = match &self.credentials_path {
            Some(credentials_path) => ProfileProvider::with_default_configuration(credentials_path),
            None => ProfileProvider::new().map_err(|e| DataStoreError::FatalIO(e.to_string()))?,
        };
        let client: S3Client = create_client(p, &self.region)?;
        Ok(client
            .head_object(HeadObjectRequest {
                bucket: self.s3_bucket.clone(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                DataStoreError::FatalIO(format!(
                    "S3Storage failed with get_object_head due to: {}",
                    e.to_string()
                ))
            })?)
    }
}

#[async_trait]
impl DataOutput<Bytes> for S3Storage {
    async fn start_stream(
        self: Box<Self>,
        _: JobManagerTx,
    ) -> anyhow::Result<DataOutputTask<Bytes>> {
        let (tx, mut rx): (DataOutputTx<Bytes>, _) = channel(1);
        let name = self.name();
        let p = match &self.credentials_path {
            Some(credentials_path) => ProfileProvider::with_default_configuration(credentials_path),
            None => ProfileProvider::new().map_err(|e| DataStoreError::FatalIO(e.to_string()))?,
        };
        let (s3_tx, s3_rx) = channel(1);
        let s3_output_key = self.s3_output_key.ok_or_else(|| {
            DataStoreError::FatalIO(
                "s3_output_key is required when using as a DataOutput".to_string(),
            )
        })?;
        let s3_jh =
            s3_write_bytes_multipart(p, &self.s3_bucket, &s3_output_key, s3_rx, self.region)
                .await
                .map_err(|e| {
                    DataStoreError::FatalIO(format!("S3Storage Error: {}", e.to_string()))
                })?;
        let jh: JoinHandle<anyhow::Result<DataOutputStats>> = tokio::spawn(async move {
            let mut num_lines = 0_usize;
            loop {
                match rx.recv().await {
                    Some(DataOutputMessage::Data(data)) => {
                        s3_tx.send(data).await?;
                        num_lines += 1;
                    }
                    Some(DataOutputMessage::NoMoreData) => break,
                    None => break,
                }
            }
            drop(s3_tx);
            s3_jh.await??;
            Ok(DataOutputStats {
                name,
                lines_written: num_lines,
            })
        });
        Ok((tx, jh))
    }
}

impl DataSource<Bytes> for S3Storage {
    fn name(&self) -> String {
        format!("S3-{}", &self.s3_bucket)
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<Bytes>, DataStoreError> {
        use rusoto_core::RusotoError;
        let p = match &self.credentials_path {
            Some(credentials_path) => ProfileProvider::with_default_configuration(credentials_path),
            None => ProfileProvider::new().map_err(|e| DataStoreError::FatalIO(e.to_string()))?,
        };
        let client: S3Client = create_client(p, &self.region)?;
        let (tx, rx) = channel(1);
        let files = self.s3_keys.clone();
        let s3_bucket = self.s3_bucket.clone();
        let name = String::from("S3Storage");
        let jh = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            for s3_key in files {
                let request = GetObjectRequest {
                    bucket: s3_bucket.clone(),
                    key: s3_key.to_owned(),
                    ..Default::default()
                };
                match client.get_object(request).await {
                    Ok(res) => {
                        // TODO: handle this unwrap
                        let reader = res.body.unwrap().into_async_read();
                        // 68 mb
                        let r = BufReader::with_capacity(1 << 26, reader);
                        let mut lines = r.lines();
                        loop {
                            if let Some(line) = lines.next_line().await? {
                                lines_scanned += 1;
                                tx.send(Ok(DataSourceMessage::new(&s3_key, Bytes::from(line))))
                                    .await
                                    .map_err(|er| DataStoreError::send_error(&name, &s3_key, er))?;
                            } else {
                                // reached end of file
                                break;
                            }
                        }
                    }
                    Err(RusotoError::Service(GetObjectError::NoSuchKey(key))) => {
                        tx.send(Err(DataStoreError::NotExist {
                            key,
                            error: "Not found".to_string(),
                        }))
                        .await
                        .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                    }
                    Err(e) => {
                        return Err(DataStoreError::FatalIO(format!(
                            "S3Storage Error calling get_object due to: {}",
                            e.to_string()
                        )));
                    }
                }
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
