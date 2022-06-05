use super::*;
use bytes::Bytes;
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
    pub fn with_datasource_dyn<'a>(
        self,
        source: Box<dyn DataSource<'a, Bytes>>,
    ) -> Box<dyn DataSource<'a, String>>
    where
        String: Send + Sync + 'static,
    {
        DecodeStream::decode_source(self, source)
    }
    /*
    pub fn with_datasource<'a, T>(self, source: T) -> Box<dyn DataSource<'a, String>>
    where
        String: Send + Sync + 'a,
        T: 'a + DataSource<'a, Bytes>,
    {
        DecodeStream::decode_source(self, Box::new(source) as Box<dyn DataSource<'a, Bytes>>)

    }
    */
}

impl<T> DecodeStream<T> for StringDecoder
where
    T: 'static + Send + Sync,
    for<'a> DecodedSource<String>: DataSource<'a, T>,
{
    fn decode_source(self, source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<'_, T>> {
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
                                let s = String::from_utf8_lossy(&*content).to_string();
                                tx.send(Ok(DataSourceMessage::new(&name, s)))
                                    .await
                                    .map_err(|e| DataStoreError::send_error(&name, &source, e))?;
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
