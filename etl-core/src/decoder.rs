use crate::datastore::error::*;
use crate::datastore::*;
use async_trait::async_trait;
use bytes::Bytes;
use csv::ReaderBuilder;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

#[async_trait]
pub trait DecodeStream<T: DeserializeOwned + Debug + 'static + Send>: Sync + Send {
    /*
    async fn start_decode_stream(
        self: Box<Self>,
        source: Box<dyn DataSource<Bytes>>,
    ) -> Result<DataSourceTask<T>, DataStoreError>;
    */

    async fn as_datasource(self: Box<Self>, source: Box<dyn DataSource<Bytes>>)
        -> DecodedSource<T>;
}

pub struct DecodedSource<T: DeserializeOwned + Debug + 'static + Send + Send> {
    source_name: String,
    ds_task_result: Result<DataSourceTask<T>, DataStoreError>,
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static> DataSource<T>
    for DecodedSource<T>
{
    fn name(&self) -> String {
        format!("DecodedSource-{}", &self.source_name)
    }
    async fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        self.ds_task_result
    }
}

/*
#[async_trait]
impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static> DataSource<T>
    for Result<DataSourceTask<T>, DataStoreError>
{
    fn name(&self) -> String {
        format!("DecodedSource")
    }
    async fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        *self
    }

}
*/

impl<T: DeserializeOwned + Debug + Send> From<Result<DataSourceTask<T>, DataStoreError>>
    for Box<dyn DataSource<T>>
{
    fn from(_r: Result<DataSourceTask<T>, DataStoreError>) -> Box<dyn DataSource<T>> {
        unimplemented!("This is not implemented")
    }
}

pub struct CsvDecoder {
    pub csv_options: CsvReadOptions,
}

#[async_trait]
impl<T: DeserializeOwned + Debug + 'static + Send> DecodeStream<T> for CsvDecoder {
    async fn as_datasource(
        self: Box<Self>,
        source: Box<dyn DataSource<Bytes>>,
    ) -> DecodedSource<T> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        let (tx, rx) = channel(1);

        let source_name = source.name();

        //let (mut source_rx, source_stream_jh) = source.start_stream().await?;

        match source.start_stream().await {
            Ok((mut source_rx, source_stream_jh)) => {
                let CsvReadOptions {
                    delimiter,
                    has_headers,
                    flexible,
                    terminator,
                    quote,
                    escape,
                    double_quote,
                    quoting,
                    comment,
                } = self.csv_options;
                let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
                    tokio::spawn(async move {
                        let mut headers_str = String::from("");
                        let mut lines_scanned = 0_usize;
                        loop {
                            match source_rx.recv().await {
                                Some(Ok(DataSourceMessage::Data { source, content })) => {
                                    if lines_scanned == 0 {
                                        headers_str =
                                            std::string::String::from_utf8_lossy(&*content)
                                                .to_string();
                                    } else {
                                        let line = std::string::String::from_utf8_lossy(&*content);
                                        let data = format!("{}\n{}", headers_str, line);
                                        let rdr = ReaderBuilder::new()
                                            .delimiter(delimiter)
                                            .has_headers(has_headers)
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
                                                    tx.send(Ok(DataSourceMessage::new(
                                                        &source, item,
                                                    )))
                                                    .await
                                                    .map_err(|e| {
                                                        DataStoreError::send_error(
                                                            &source,
                                                            "CsvDecoder",
                                                            e,
                                                        )
                                                    })?;
                                                }
                                                Err(er) => {
                                                    match tx
                                                        .send(Err(DataStoreError::Deserialize {
                                                            message: er.to_string(),
                                                            attempted_string: line.to_string(),
                                                        }))
                                                        .await
                                                    {
                                                        Ok(_) => {
                                                            //sent_count += 1;
                                                        }
                                                        Err(e) => {
                                                            return Err(
                                                                DataStoreError::send_error(
                                                                    &source, "", e,
                                                                ),
                                                            );
                                                        }
                                                    }
                                                }
                                            },
                                            None => {
                                                break;
                                            }
                                        }
                                    }
                                    lines_scanned += 1;
                                }
                                Some(Err(e)) => {
                                    println!("An error happened in CsvDecoder: {}", e);
                                    break;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                        source_stream_jh.await??;
                        Ok(DataSourceStats { lines_scanned })
                    });
                return DecodedSource {
                    source_name,
                    ds_task_result: Ok((rx, jh)),
                };
            }
            Err(er) => {
                return DecodedSource {
                    source_name,
                    ds_task_result: Err(er),
                };
            }
        }
    }
}
