use super::create_client;
use super::rusoto_s3::*;
use super::DataStoreError;
use super::ProfileProvider;
use super::Region;
use super::S3Client;
use super::{AsyncBufReadExt, BufReader};
use etl_core::datastore::bytes_source::*;
use etl_core::preamble::anyhow;
use etl_core::preamble::tokio;
use etl_core::preamble::tokio::sync::mpsc::{channel};

pub struct S3Storage {
    pub s3_bucket: String,
    pub s3_keys: Vec<String>,
    /// path to the credentials file to access aws
    pub credentials: String,
    pub region: Region,
}

impl BytesSource for S3Storage {
    fn name(&self) -> String {
        format!("S3-{}", &self.s3_bucket)
    }

    fn start_stream(self: Box<Self>) -> Result<BytesSourceTask, DataStoreError> {
        use rusoto_core::RusotoError;
        let p = ProfileProvider::with_default_configuration(&self.credentials);
        let client: S3Client = create_client(p, &self.region)?;
        let (tx, rx) = channel(1);
        let files = self.s3_keys.clone();
        let s3_bucket = self.s3_bucket.clone();
        let name = String::from("S3Storage");
        let jh = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            for s3_key in files {
                //println!("s3_key {}", &s3_key);
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
                                tx.send(Ok(BytesSourceMessage::new(&s3_key, line)))
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
                        tx.send(Err(DataStoreError::from(anyhow::Error::from(e))))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                    }
                }
            }
            Ok(BytesSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
