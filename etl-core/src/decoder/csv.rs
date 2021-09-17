use ::csv::ReaderBuilder;
use super::*;

pub struct CsvDecoder {
    pub csv_options: CsvReadOptions,
}

impl CsvDecoder {
    pub async fn new<T>(
        csv_options: CsvReadOptions,
        source: Box<dyn DataSource<Bytes>>,
    ) -> Box<dyn DataSource<T>> 
        where T: DeserializeOwned + Debug + Send + Sync + 'static
    {
        DecodeStream::decode_source(Box::new(CsvDecoder { csv_options }), source).await
    }
}

#[async_trait]
impl<T: DeserializeOwned + Debug + 'static + Send + Sync> DecodeStream<T> for CsvDecoder {
    async fn decode_source(
        self: Box<Self>,
        source: Box<dyn DataSource<Bytes>>,
        //) -> DecodedSource<T> {
    ) -> Box<dyn DataSource<T>> {
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
                return Box::new(DecodedSource {
                    source_name,
                    ds_task_result: Ok((rx, jh)),
                });
            }
            Err(er) => {
                return Box::new(DecodedSource {
                    source_name,
                    ds_task_result: Err(er),
                });
            }
        }
    }
}
