use super::*;

pub struct JsonDecoder {}
use crate::transformer::{ TransformFunc, TransformSource };

impl JsonDecoder {
    pub fn new<T>(source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<T>>
    where
        T: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        DecodeStream::decode_source(JsonDecoder {}, source)
    }

    pub fn from_bytes_source<'a, DS, O>(source: DS) -> TransformSource<'a, Bytes, O>
    where
        for<'_a> DS: DataSource<'_a, Bytes>,
        O: Send + Sync + DeserializeOwned + 'static,
    {
        TransformSource::new(source, TransformFunc::new(|content: Bytes| {
            let c = content.to_vec();
            match serde_json::from_slice::<O>(&c) {
                Ok(r) => Ok(r),
                Err(er) => Err(DataStoreError::Deserialize {
                    message: er.to_string(),
                    attempted_string: "".into(),
                }),
            }
        }))
    }

    pub fn from_string_source<'a, DS, O>(source: DS) -> TransformSource<'a, String, O>
    where
        for<'_a> DS: DataSource<'_a, String>,
        O: Send + Sync + DeserializeOwned + 'static,
    {
        TransformSource::new(source, TransformFunc::new(|content: String| {
            match serde_json::from_str::<O>(&content) {
                Ok(r) => Ok(r),
                Err(er) => Err(DataStoreError::Deserialize {
                    message: er.to_string(),
                    attempted_string: "".into(),
                }),
            }
        }))
    }
}

impl<T: DeserializeOwned + Debug + 'static + Send + Sync> DecodeStream<T> for JsonDecoder {
    fn decode_source(self, source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<T>> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);

        let source_name = source.name();
        let name = source_name.clone();

        match source.start_stream() {
            Ok((mut source_rx, source_stream_jh)) => {
                let jh: DataSourceJoinHandle = tokio::spawn(async move {
                    let name = name;
                    let mut lines_scanned = 0_usize;
                    loop {
                        match source_rx.recv().await {
                            Some(Ok(DataSourceMessage::Data { source, content })) => {
                                lines_scanned += 1;
                                match serde_json::from_slice::<T>(&content) {
                                    Ok(r) => {
                                        log::info!("{:?}", &r);
                                        tx.send(Ok(DataSourceMessage::new(
                                            "MockJsonDataSource",
                                            r,
                                        )))
                                        .await
                                        .map_err(
                                            |e| DataStoreError::send_error(&name, &source, e),
                                        )?;
                                        lines_scanned += 1;
                                    }
                                    Err(val) => {
                                        log::error!("{}", &val);
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
                    Ok(DataSourceDetails::Basic { lines_scanned })
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
