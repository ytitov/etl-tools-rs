use super::*;

pub struct JsonDecoder {}

impl JsonDecoder {
    pub async fn new<T>(source: Box<dyn BytesSource>) -> Box<dyn DataSource<T>>
    where
        T: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        DecodeStream::decode_source(Box::new(JsonDecoder {}), source).await
    }
}

#[async_trait]
impl<T: DeserializeOwned + Debug + 'static + Send + Sync> DecodeStream<T> for JsonDecoder {
    async fn decode_source(
        self: Box<Self>,
        source: Box<dyn BytesSource>,
    ) -> Box<dyn DataSource<T>> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        let (tx, rx) = channel(1);

        //let (mut source_rx, source_stream_jh) = source.start_stream().await?;
        let source_name = source.name();
        let name = source_name.clone();

        match source.start_stream() {
            Ok((mut source_rx, source_stream_jh)) => {
                let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
                    tokio::spawn(async move {
                        let name = name;
                        let mut lines_scanned = 0_usize;
                        loop {
                            match source_rx.recv().await {
                                Some(Ok(BytesSourceMessage::Data { source, content })) => {
                                    lines_scanned += 1;
                                    match serde_json::from_slice::<T>(&content) {
                                        Ok(r) => {
                                            tx.send(Ok(DataSourceMessage::new(
                                                "MockJsonDataSource",
                                                r,
                                            )))
                                            .await
                                            .map_err(|e| {
                                                DataStoreError::send_error(&name, &source, e)
                                            })?;
                                            lines_scanned += 1;
                                        }
                                        Err(val) => {
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
                                    println!("An error happened in JsonDecoder: {}", e);
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