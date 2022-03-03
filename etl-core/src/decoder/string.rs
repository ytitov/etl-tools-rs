use super::*;
pub struct StringDecoder {
    /// always uses lossy decoder at this time
    pub lossy: bool,
}

impl Default for StringDecoder {
    fn default() -> Self {
        StringDecoder { lossy: true }
    }
}

impl StringDecoder {
    pub async fn new(self, source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<String>>
    where
        String: Debug + Send + Sync + 'static,
    {
        DecodeStream::decode_source(Box::new(self), source).await
    }
}

#[async_trait]
impl<T: Debug + 'static + Send + Sync> DecodeStream<T> for StringDecoder 
    where DecodedSource<String>: DataSource<T>
{
    async fn decode_source(
        self: Box<Self>,
        source: Box<dyn DataSource<Bytes>>,
    ) -> Box<dyn DataSource<T>> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        let (tx, rx) = channel(1);

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
                                Some(Ok(DataSourceMessage::Data { source, content })) => {
                                    lines_scanned += 1;
                                    let s = String::from_utf8_lossy(&*content).to_string();
                                    tx.send(Ok(DataSourceMessage::new(&name, s)))
                                        .await
                                        .map_err(|e| {
                                            DataStoreError::send_error(&name, &source, e)
                                        })?;
                                    lines_scanned += 1;
                                }
                                Some(Err(e)) => {
                                    println!("An error happened in StringDecoder: {}", e);
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
