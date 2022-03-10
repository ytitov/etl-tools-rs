use super::*;
use std::fmt::Debug;

#[derive(Default)]
pub struct JsonDecoder {}

impl JsonDecoder {
    pub fn new<T>(source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<T>>
    where
        T: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        DecodeStream::decode_source(JsonDecoder {}, source)
    }
    pub fn with_datasource<T, O>(self, source: T) -> Box<dyn DataSource<O>>
    where
        String: Debug + Send + Sync + 'static,
        T: DataSource<Bytes> + 'static,
        O: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        DecodeStream::decode_source(self, Box::new(source) as Box<dyn DataSource<Bytes>>)
    }
}

impl<T: DeserializeOwned + Debug + 'static + Send + Sync> DecodeStream<T> for JsonDecoder {
    fn decode_source(self, source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<T>> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        let (tx, rx) = channel(1);

        //let (mut source_rx, source_stream_jh) = source.start_stream().await?;
        let source_name = source.name();
        let name = format!("JsonDecoder:{}", &source_name);

        match source.start_stream() {
            Ok((mut source_rx, source_stream_jh)) => {
                let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
                    tokio::spawn(async move {
                        let name = name;
                        let mut lines_scanned = 0_usize;
                        loop {
                            match source_rx.recv().await {
                                Some(Ok(DataSourceMessage::Data { source, content })) => {
                                    lines_scanned += 1;
                                    match serde_json::from_slice::<T>(&content) {
                                        Ok(r) => {
                                            tx.send(Ok(DataSourceMessage::new(&name, r)))
                                                .await
                                                .map_err(|e| {
                                                    DataStoreError::send_error(&name, &source, e)
                                                })?;
                                            lines_scanned += 1;
                                        }
                                        Err(val) => {
                                            log::error!("{}: {}", &name, val);
                                            match tx
                                                .send(Err(DataStoreError::Deserialize {
                                                    message: val.to_string(),
                                                    attempted_string: format!("{:?}", content),
                                                }))
                                                .await
                                            {
                                                Ok(_) => {
                                                    lines_scanned += 1;
                                                }
                                                Err(e) => {
                                                    return Err(DataStoreError::send_error(
                                                        &name, &source, e,
                                                    ));
                                                }
                                            }
                                        }
                                    };
                                }
                                Some(Err(e)) => {
                                    log::error!("An error happened in JsonDecoder: {}", e);
                                    break;
                                }
                                None => {
                                    log::info!("JsonDecoder finished");
                                    break;
                                }
                            };
                        }
                        source_stream_jh.await??;
                        Ok(DataSourceStats { lines_scanned })
                    });
                return Box::new(DecodedSource {
                    source_name: source_name.clone(),
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
