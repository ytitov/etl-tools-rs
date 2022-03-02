use etl_core::datastore::error::*;
use etl_core::datastore::*;
use etl_core::preamble::*;
use rusoto_core::HttpClient;
pub use rusoto_core::Region;
use rusoto_credential::ProfileProvider;
use rusoto_s3;
use rusoto_s3::*;
//use std::sync::{Arc, Mutex};
use etl_core::deps::{
    anyhow, async_trait,
    serde::de::DeserializeOwned,
    serde::Serialize,
    tokio,
    tokio::io::{AsyncBufReadExt, BufReader},
    tokio::sync::mpsc::{channel, Receiver},
    tokio::task::JoinHandle,
};
use etl_core::job_manager::JobManagerTx;
use std::fmt::Debug;
//use serde_json::Value;

pub mod bytes_source;
// TODO: convert to use read_content: ReadContentOptions
pub struct S3DataSource {
    pub read_content: ReadContentOptions,
    pub s3_bucket: String,
    pub s3_keys: Vec<String>,
    /// path to the credentials file to access aws
    pub credentials: String,
    pub region: Region,
}

impl Default for S3DataSource {
    fn default() -> Self {
        S3DataSource {
            read_content: ReadContentOptions::Csv(CsvReadOptions::default()),
            s3_bucket: "bucket".to_owned(),
            s3_keys: Vec::new(),
            credentials: "".to_owned(),
            region: Region::UsEast1,
        }
    }
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> DataSource<T>
    for S3DataSource
{
    fn name(&self) -> String {
        format!("S3DataSource-{}", &self.s3_bucket)
    }

    fn start_stream(mut self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use ReadContentOptions::*;
        match self.read_content.clone() {
            Json => self.start_stream_json::<T>(),
            Csv(options) => self.start_stream_csv::<T>(options),
            Text => {
                unimplemented!()
            }
        }
    }
}
#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> SimpleStore<T>
    for S3DataSource
{
    async fn read_file_str(&self, s3_key: &str) -> Result<String, DataStoreError> {
        use tokio::io::AsyncReadExt;
        let p = ProfileProvider::with_default_configuration(&self.credentials);
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
        let p = ProfileProvider::with_default_configuration(&self.credentials);
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
        let p = ProfileProvider::with_default_configuration(&self.credentials);
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

impl S3DataSource {
    fn start_stream_json<T>(&mut self) -> Result<DataSourceTask<T>, DataStoreError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        use rusoto_core::RusotoError;
        let p = ProfileProvider::with_default_configuration(&self.credentials);
        let client = create_client(p, &self.region)?;
        let (tx, rx) = channel(1);
        let files = self.s3_keys.clone();
        let s3_bucket = self.s3_bucket.clone();
        let name = String::from("S3DataSource");
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
                                match serde_json::from_str::<T>(&line) {
                                    Ok(val) => {
                                        tx.send(Ok(DataSourceMessage::new(&s3_key, val)))
                                            .await
                                            .map_err(|e| {
                                                DataStoreError::send_error(&name, "", e)
                                            })?;
                                    }
                                    Err(val) => {
                                        tx.send(Err(DataStoreError::Deserialize {
                                            message: val.to_string(),
                                            attempted_string: line.to_string(),
                                        }))
                                        .await
                                        .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                                    }
                                };
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
            Ok(DataSourceStats { lines_scanned })
        });
        //tokio::task::yield_now().await;
        Ok((rx, jh))
    }

    fn start_stream_csv<T>(
        &mut self,
        csv_options: CsvReadOptions,
    ) -> Result<DataSourceTask<T>, DataStoreError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        use csv::ReaderBuilder;
        use rusoto_core::RusotoError;
        let CsvReadOptions {
            delimiter,
            has_headers: _,
            flexible,
            terminator,
            quote,
            escape,
            double_quote,
            quoting,
            comment,
        } = csv_options;
        let p = ProfileProvider::with_default_configuration(&self.credentials);
        let client = create_client(p, &self.region)?;
        let (tx, rx) = channel(1);
        let files = self.s3_keys.clone();
        let s3_bucket = self.s3_bucket.clone();
        let name = String::from("S3DataSource");
        let jh = tokio::spawn(async move {
            let mut total_lines = 0_usize;
            let mut lines_scanned = 0_usize;
            let mut headers_str = String::from("");
            for s3_key in files {
                let request = GetObjectRequest {
                    bucket: s3_bucket.clone(),
                    key: s3_key.to_owned(),
                    ..Default::default()
                };
                match client.get_object(request).await {
                    Ok(res) => {
                        let reader = res.body.unwrap().into_async_read();
                        //let r = BufReader::new(reader);
                        // 68 mb
                        let r = BufReader::with_capacity(1 << 26, reader);
                        let mut lines = r.lines();
                        loop {
                            if let Some(line) = lines.next_line().await? {
                                if lines_scanned == 0 {
                                    headers_str = line.clone();
                                    lines_scanned += 1;
                                } else {
                                    let data = format!("{}\n{}", headers_str, line);
                                    //let rdr = Reader::from_reader(data.as_bytes());
                                    let rdr = ReaderBuilder::new()
                                        .delimiter(delimiter)
                                        .has_headers(true)
                                        .flexible(flexible)
                                        .terminator(terminator)
                                        .quote(quote)
                                        .escape(escape)
                                        .double_quote(double_quote)
                                        .quoting(quoting)
                                        .comment(comment)
                                        .from_reader(data.as_bytes());
                                    let mut iter = rdr.into_deserialize::<T>();
                                    match iter.next() {
                                        Some(result) => match result {
                                            Ok(item) => {
                                                tx.send(Ok(DataSourceMessage::new(&s3_key, item)))
                                                    .await
                                                    .map_err(|e| {
                                                        DataStoreError::send_error(
                                                            &name, &s3_key, e,
                                                        )
                                                    })?;
                                            }
                                            Err(val) => {
                                                tx.send(Err(DataStoreError::Deserialize {
                                                    message: val.to_string(),
                                                    attempted_string: line.to_string(),
                                                }))
                                                .await
                                                .map_err(|e| {
                                                    DataStoreError::send_error(&name, "", e)
                                                })?;
                                            }
                                        },
                                        None => {
                                            // end of file
                                            break;
                                        }
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                        total_lines += lines_scanned;
                        lines_scanned = 0;
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
            Ok(DataSourceStats {
                lines_scanned: total_lines,
            })
        });
        //tokio::task::yield_now().await;
        Ok((rx, jh))
    }
}

pub struct S3DataOutput {
    pub credentials: String,
    pub s3_bucket: String,
    pub s3_key: String,
    pub region: Region,
    pub write_content: WriteContentOptions,
}

impl Default for S3DataOutput {
    fn default() -> Self {
        S3DataOutput {
            credentials: String::from("~/.aws/credentials"),
            s3_bucket: String::from(""),
            s3_key: String::from(""),
            region: Region::UsEast1,
            write_content: WriteContentOptions::Json,
        }
    }
}

impl S3DataOutput {
    async fn start_stream_output_csv<T>(
        &mut self,
        csv_options: CsvWriteOptions,
        jm_tx: JobManagerTx,
    ) -> anyhow::Result<DataOutputTask<T>>
    where
        T: Serialize + Debug + 'static + Sync + Send,
    {
        use csv::WriterBuilder;
        let CsvWriteOptions {
            delimiter,
            has_headers,
            terminator,
            quote,
            quote_style,
            escape,
            double_quote,
        } = csv_options;
        let (tx, mut rx): (DataOutputTx<T>, _) = channel(1);
        let s3_bucket = self.s3_bucket.clone();
        let s3_key = self.s3_key.clone();
        let region = self.region.clone();
        let credentials = self.credentials.clone();
        let jh: JoinHandle<anyhow::Result<DataOutputStats>> = tokio::spawn(async move {
            let mut num_lines_sent = 0_usize;
            let (s3_tx, s3_rx) = channel(1);
            let p = ProfileProvider::with_default_configuration(credentials);
            let s3_jh = s3_write_bytes_multipart(p, &s3_bucket, &s3_key, s3_rx, region).await?;
            loop {
                let mut wtr = match num_lines_sent {
                    // headers on only the first line
                    0 => WriterBuilder::new()
                        .delimiter(delimiter)
                        .has_headers(has_headers)
                        .terminator(terminator)
                        .quote_style(quote_style)
                        .quote(quote)
                        .double_quote(double_quote)
                        .escape(escape)
                        .from_writer(vec![]),
                    _ => WriterBuilder::new()
                        .delimiter(delimiter)
                        .has_headers(false)
                        .terminator(terminator)
                        .quote_style(quote_style)
                        .quote(quote)
                        .double_quote(double_quote)
                        .escape(escape)
                        .from_writer(vec![]),
                };
                match rx.recv().await {
                    Some(m) => {
                        match m {
                            DataOutputMessage::Data(data) => match wtr.serialize(data) {
                                Ok(()) => {
                                    if let Ok(bytes_vec) = wtr.into_inner() {
                                        match std::str::from_utf8(&bytes_vec) {
                                            Ok(line) => {
                                                s3_tx.send(line.to_owned()).await?;
                                                num_lines_sent += 1;
                                            }
                                            Err(er) => {
                                                let m = format!("WARNING: create_s3_csv_writer skipping bad str due to: {}", er);
                                                jm_tx
                                                    .send(Message::log_err("S3DataOutput", m))
                                                    .await?;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    jm_tx
                                        .send(Message::log_err("S3DataOutput", e.to_string()))
                                        .await?;
                                    return Err(anyhow::anyhow!("{}", e));
                                }
                            },
                            DataOutputMessage::NoMoreData => {
                                println!("S3DataOutput got DataOutputMessage::NoMoreData");
                                break;
                            }
                        };
                    }
                    None => break,
                };
            }
            drop(s3_tx);
            s3_jh.await??;
            Ok(DataOutputStats {
                name: format!("s3://{}/{}", s3_bucket, s3_key),
                lines_written: num_lines_sent,
            })
        });
        Ok((tx, jh))
    }
    async fn start_stream_output_json<T>(
        &mut self,
        jm_tx: JobManagerTx,
    ) -> anyhow::Result<DataOutputTask<T>>
    where
        T: Serialize + Debug + 'static + Sync + Send,
    {
        let (tx, mut rx): (DataOutputTx<T>, _) = channel(1);
        let s3_bucket = self.s3_bucket.clone();
        let s3_key = self.s3_key.clone();
        let region = self.region.clone();
        let credentials = self.credentials.clone();
        let jh: JoinHandle<anyhow::Result<DataOutputStats>> = tokio::spawn(async move {
            let (s3_tx, s3_rx) = channel(1);
            let p = ProfileProvider::with_default_configuration(credentials);
            let s3_jh = s3_write_bytes_multipart(p, &s3_bucket, &s3_key, s3_rx, region).await?;
            let mut lines_written = 0;
            loop {
                match rx.recv().await {
                    Some(m) => {
                        match m {
                            DataOutputMessage::Data(data) => {
                                // serialize
                                match serde_json::to_string(&data) {
                                    Ok(mut line) => {
                                        line.push_str("\n");
                                        s3_tx.send(line).await?;
                                        lines_written += 1;
                                    }
                                    Err(e) => {
                                        println!("S3DataOutput error: {}", e);
                                        jm_tx
                                            .send(Message::log_err("S3DataOutput", e.to_string()))
                                            .await?;
                                    }
                                }
                            }
                            DataOutputMessage::NoMoreData => {
                                println!("S3DataOutput got DataOutputMessage::NoMoreData");
                                break;
                            }
                        };
                    }
                    None => break,
                };
            }
            drop(s3_tx);
            s3_jh.await??;
            Ok(DataOutputStats {
                name: format!("s3://{}/{}", s3_bucket, s3_key),
                lines_written,
            })
        });
        Ok((tx, jh))
    }
}

#[async_trait]
impl<T: Serialize + Debug + Send + Sync + 'static> DataOutput<T> for S3DataOutput {
    async fn start_stream(
        mut self: Box<Self>,
        jm_tx: JobManagerTx,
    ) -> anyhow::Result<DataOutputTask<T>> {
        match self.write_content.clone() {
            WriteContentOptions::Json => self.start_stream_output_json::<T>(jm_tx).await,
            WriteContentOptions::Csv(opts) => self.start_stream_output_csv::<T>(opts, jm_tx).await,
            WriteContentOptions::Text => {
                unimplemented!()
            }
        }
    }

    /*
    async fn shutdown(self: Box<Self>, _: &JobRunner) {
    }
    */
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

pub async fn s3_write_bytes_multipart(
    profile_provider: ProfileProvider,
    s3_bucket: &str,
    s3_key: &str,
    mut body_stream: Receiver<String>,
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
            use bytes::Bytes;
            use bytes::BytesMut;
            use rusoto_core::ByteStream;
            let mut buf = BytesMut::with_capacity(max_size);
            let mut part_number = 1;
            loop {
                match body_stream.recv().await {
                    Some(s) => {
                        buf.extend(Bytes::from(s));
                        if buf.len() >= max_size {
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
                                println!("ERROR: s3_write_bytes_multipart {}", &er);
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
                    //println!("Retrying after 503: {:?}", e);
                    println!("Retrying after 503: {}", &s3_key);
                }
            }
            Err(e) => {
                //println!("ERROR: s3_write_text_file {}", &e);
                return Err(anyhow!("Error: s3_write_text_file: {}", e));
            }
        };
        count -= 1;
        num_tries += 1;
        wait_ms = 2_usize.pow(num_tries) * wait_ms;
        if wait_ms > max_wait_ms {
            wait_ms = max_wait_ms;
        }
        println!("waiting {} ms", wait_ms);
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
