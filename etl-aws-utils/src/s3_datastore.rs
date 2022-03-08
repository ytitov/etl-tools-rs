use etl_core::datastore::error::*;
use etl_core::datastore::{*, simple::SimpleStore};
use etl_core::deps::{
    anyhow, async_trait,
    bytes::Bytes,
    serde::de::DeserializeOwned,
    serde::Serialize,
    tokio,
    tokio::io::{AsyncBufReadExt, BufReader},
    tokio::sync::mpsc::{channel, Receiver},
    tokio::task::JoinHandle,
    log,
};
use rusoto_core::HttpClient;
pub use rusoto_core::Region;
use rusoto_credential::ProfileProvider;
use rusoto_s3::{self, *};
use std::fmt::Debug;

pub struct S3Storage {
    pub s3_bucket: String,
    /// Only used when using this as a source
    pub s3_keys: Vec<String>,
    /// When using this as a DataOutput this must be filled in
    pub s3_output_key: Option<String>,
    /// path to the credentials file to access aws
    pub credentials_path: Option<String>,
    /// defaults to UsEast1
    pub region: Region,
}

impl Default for S3Storage {
    fn default() -> Self {
        S3Storage {
            s3_bucket: "notset".to_owned(),
            s3_keys: Vec::new(),
            s3_output_key: None,
            credentials_path: None,
            region: Region::UsEast1,
        }
    }
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

    pub fn create_profile_provider(&self) -> Result<ProfileProvider, DataStoreError> {
        match &self.credentials_path {
            Some(credentials_path) => Ok(ProfileProvider::with_default_configuration(
                credentials_path,
            )),
            None => ProfileProvider::new().map_err(|e| DataStoreError::FatalIO(e.to_string())),
        }
    }
}

#[async_trait]
impl DataOutput<Bytes> for S3Storage {
    async fn start_stream(
        self: Box<Self>,
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
                        log::error!("{}", &e);
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

#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> SimpleStore<T> for S3Storage {
    async fn read_file_str(&self, s3_key: &str) -> Result<String, DataStoreError> {
        use tokio::io::AsyncReadExt;
        let p = self.create_profile_provider()?;
        let client = create_client(p, &self.region)?;
        let request = GetObjectRequest {
            bucket: self.s3_bucket.to_owned(),
            key: s3_key.to_owned(),
            ..Default::default()
        };
        let result = client.get_object(request).await;
        match result {
            Ok(res) => {
                let reader = res.body.unwrap().into_async_read();
                let mut r = BufReader::new(reader);
                let mut buf = Vec::new();
                r.read_to_end(&mut buf).await?;
                let body_str = std::str::from_utf8(&buf)?;
                Ok(body_str.to_owned())
            }
            Err(err) => Err(DataStoreError::NotExist {
                key: s3_key.to_owned(),
                error: err.to_string(),
            }),
        }
    }

    async fn write(&self, key: &str, item: T) -> Result<(), DataStoreError> {
        let p = self.create_profile_provider()?;
        match serde_json::to_string_pretty(&item) {
            Ok(body) => {
                s3_write_text_file(p, &self.s3_bucket, key, body).await?;
            }
            Err(err) => {
                return Err(DataStoreError::FatalIO(err.to_string()));
            }
        };
        Ok(())
    }

    async fn load(&self, key: &str) -> Result<T, DataStoreError> {
        let p = self.create_profile_provider()?;
        let text = s3_load_text_file(&p, &self.s3_bucket, key, &self.region).await?;
        match serde_json::from_str::<T>(&text) {
            Ok::<T, _>(obj) => Ok(obj),
            Err(e) => Err(DataStoreError::Deserialize {
                attempted_string: format!("Could not deserialize {}", key),
                message: e.to_string(),
            }),
        }
    }
}

pub fn create_client(
    profile_provider: ProfileProvider,
    region: &Region,
) -> anyhow::Result<S3Client> {
    Ok(S3Client::new_with(
        HttpClient::new()?,
        profile_provider,
        region.clone(),
    ))
}

/// Upload to S3 with 30 mb size increments
pub async fn s3_write_bytes_multipart(
    profile_provider: ProfileProvider,
    s3_bucket: &str,
    s3_key: &str,
    mut body_stream: Receiver<Bytes>,
    region: Region,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let client = S3Client::new_with(HttpClient::new().unwrap(), profile_provider, region);

    let s3_bucket = s3_bucket.to_owned();
    let s3_key = s3_key.to_owned();
    let join_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let max_size = 30_000_000_usize; // 30mb
        let mut completed_parts = Vec::new();
        if let CreateMultipartUploadOutput {
            upload_id: Some(upload_id),
            ..
        } = client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: s3_bucket.to_owned(),
                key: s3_key.to_owned(),
                ..Default::default()
            })
            .await?
        {
            use bytes::BytesMut;
            use rusoto_core::ByteStream;
            let mut buf = BytesMut::with_capacity(max_size);
            let mut part_number = 1;
            loop {
                match body_stream.recv().await {
                    Some(s) => {
                        buf.extend(s);
                        if buf.len() >= max_size {
                            println!("uploading because reached max size");
                            let b = buf;
                            buf = BytesMut::with_capacity(max_size);
                            let request = UploadPartRequest {
                                bucket: s3_bucket.to_owned(),
                                key: s3_key.to_owned(),
                                //content_length: Some(buf.len() as i64),
                                body: Some(ByteStream::from(Bytes::from(b).to_vec())),
                                upload_id: upload_id.clone(),
                                part_number,
                                ..Default::default()
                            };
                            match client.upload_part(request).await {
                                Ok(UploadPartOutput { e_tag, .. }) => {
                                    println!(
                                        "INFO: s3_write_bytes_multipart: uploaded part {} of {}",
                                        part_number, &s3_key
                                    );
                                    completed_parts.push(CompletedPart {
                                        e_tag,
                                        part_number: Some(part_number),
                                    });
                                    part_number += 1;
                                }
                                Err(er) => {
                                    println!("ERROR: s3_write_bytes_multipart: {}", &er);
                                    client
                                        .abort_multipart_upload(AbortMultipartUploadRequest {
                                            bucket: s3_bucket.to_owned(),
                                            key: s3_key.to_owned(),
                                            upload_id: upload_id.clone(),
                                            ..Default::default()
                                        })
                                        .await?;
                                    return Err(anyhow::anyhow!(
                                        "Error uploading part to s3: {}",
                                        er
                                    ));
                                }
                            }
                        }
                    }
                    None => {
                        let request = UploadPartRequest {
                            bucket: s3_bucket.to_owned(),
                            key: s3_key.to_owned(),
                            //content_length: Some(buf.len() as i64),
                            body: Some(ByteStream::from(Bytes::from(buf).to_vec())),
                            upload_id: upload_id.clone(),
                            part_number,
                            ..Default::default()
                        };
                        match client.upload_part(request).await {
                            Ok(UploadPartOutput { e_tag, .. }) => {
                                completed_parts.push(CompletedPart {
                                    e_tag,
                                    part_number: Some(part_number),
                                });
                                //part_number += 1;
                            }
                            Err(er) => {
                                return Err(anyhow::anyhow!("Error uploading part to s3: {}", er));
                            }
                        }
                        break;
                    }
                }
            }
            client
                .complete_multipart_upload(CompleteMultipartUploadRequest {
                    bucket: s3_bucket.to_owned(),
                    key: s3_key.to_owned(),
                    upload_id: upload_id.clone(),
                    multipart_upload: Some(CompletedMultipartUpload {
                        parts: Some(completed_parts),
                    }),
                    ..Default::default()
                })
                .await?;
        } else {
            return Err(anyhow::anyhow!("Did not get upload_id"));
        }

        Ok(())
    });
    //Ok(join_handle)
    Ok(join_handle)
}

/// overwrites the file if it exists already, with built in retry
pub async fn s3_write_text_file(
    profile_provider: ProfileProvider,
    s3_bucket: &str,
    s3_key: &str,
    body: String,
) -> anyhow::Result<()> {
    use anyhow::anyhow;
    use http::status::StatusCode;
    use rusoto_core::ByteStream;
    use rusoto_core::RusotoError;

    let client = S3Client::new_with(
        HttpClient::new().unwrap(),
        profile_provider,
        rusoto_core::Region::UsEast1,
    );
    let request = PutObjectRequest {
        bucket: s3_bucket.to_owned(),
        key: s3_key.to_owned(),
        body: Some(ByteStream::from(body.into_bytes())),
        ..Default::default()
    };
    let result: Result<PutObjectOutput, RusotoError<PutObjectError>> =
        client.put_object(request).await;
    let status_503 = StatusCode::from_u16(503).expect("Incorrect status code");
    let mut wait_ms = 100_usize;
    let mut count = 100_u8; // max number of retries
    let mut num_tries = 1_u32;
    let max_wait_ms = 10_000_usize;
    while count > 0 {
        match result {
            Ok(_) => {
                return Ok(());
            }
            Err(RusotoError::Unknown(ref e)) => {
                if e.status == *&status_503 {
                    println!("Retrying after 503: {}", &s3_key);
                }
            }
            Err(e) => {
                return Err(anyhow!("Error: s3_write_text_file: {}", e));
            }
        };
        count -= 1;
        num_tries += 1;
        wait_ms = 2_usize.pow(num_tries) * wait_ms;
        if wait_ms > max_wait_ms {
            wait_ms = max_wait_ms;
        }
        log::info!("waiting {} ms", wait_ms);
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms as u64)).await;
    }
    Err(anyhow!(
        "Error: s3_write_text_file: timed out trying to put_object"
    ))
}

pub async fn s3_load_text_file(
    profile_provider: &ProfileProvider,
    s3_bucket: &str,
    s3_key: &str,
    region: &Region,
) -> Result<String, DataStoreError> {
    use tokio::io::AsyncReadExt;
    let client = create_client(profile_provider.to_owned(), region)?;
    let request = GetObjectRequest {
        bucket: s3_bucket.to_owned(),
        key: s3_key.to_owned(),
        ..Default::default()
    };
    let result = client.get_object(request).await;
    match result {
        Ok(res) => {
            let reader = res.body.unwrap().into_async_read();
            let mut r = BufReader::new(reader);
            let mut buf = Vec::new();
            r.read_to_end(&mut buf).await?;
            let body_str = std::string::String::from_utf8_lossy(&buf);
            Ok((*body_str).to_owned())
        }
        Err(err) => Err(DataStoreError::NotExist {
            key: s3_key.to_owned(),
            error: err.to_string(),
        }),
    }
}
