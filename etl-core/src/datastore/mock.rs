use super::*;
use std::collections::HashMap;

#[derive(Default)]
/// Designed as a "null" datastore target.  Items received are not written anywhere
pub struct MockJsonDataOutput {
    pub name: String,
}

#[async_trait]
impl<T: Serialize + std::fmt::Debug + Send + Sync + 'static> DataOutput<T>
    for MockJsonDataOutput
{
    async fn start_stream(
        &mut self,
        _: JobManagerChannel,
    ) -> anyhow::Result<DataOutputTask<T>> {
        use tokio::sync::mpsc::channel;
        let (tx, mut rx): (Sender<DataOutputMessage<T>>, _) = channel(1);
        let jh: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(m) => {
                        match m {
                            DataOutputMessage::Data(data) => {
                                // serialize
                                match serde_json::to_string(&data) {
                                    Ok(line) => {
                                        println!("MockDataOutput received: {}", line);
                                    }
                                    Err(e) => {
                                        println!("MockDataOutput ERROR {}", e);
                                    }
                                }
                            }
                            DataOutputMessage::NoMoreData => {
                                println!("received DataOutputMessage::NoMoreData");
                                break;
                            }
                        };
                    }
                    None => break,
                };
            }
            Ok(())
        });
        Ok((tx, jh))
    }
}

/// Used for testing, simply provide the json strings and this data store will
/// stream them
#[derive(Default)]
pub struct MockJsonDataSource {
    pub lines: Vec<String>,
    pub files: HashMap<String, String>,
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static>
    DataSource<T> for MockJsonDataSource
{
    async fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        // could make this configurable
        let (tx, rx) = channel(1);
        let name = String::from("MockJsonDataSource");
        let lines = self.lines.clone();
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
            tokio::spawn(async move {
                let mut lines_scanned = 0_usize;
                for line in lines {
                    match serde_json::from_str::<T>(&line) {
                        Ok(r) => {
                            tx.send(Ok(DataSourceMessage::new("MockJsonDataSource", r)))
                                .await
                                .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                        }
                        Err(val) => {
                            match tx
                                .send(Err(DataStoreError::Deserialize {
                                    message: val.to_string(),
                                    attempted_string: line.to_string(),
                                }))
                                .await
                            {
                                Ok(_) => {
                                    lines_scanned += 1;
                                }
                                Err(e) => {
                                    return Err(DataStoreError::send_error(&name, "", e));
                                }
                            }
                        }
                    };
                }
                Ok(DataSourceStats { lines_scanned })
            });
        Ok((rx, jh))
    }
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> SimpleStore<T>
    for MockJsonDataSource
{
    // TODO: add ability to look in files and return them (store as vec of strings)
    async fn load(&self, path: &str) -> Result<T, DataStoreError> {
        println!("Loading from MockJsonDataSource will result as not found");
        return Err(DataStoreError::NotExist {
            key: path.to_owned(),
            error: "Loading from MockJsonDataSource will always result in not found"
                .to_string(),
        });
    }

    async fn write(&self, path: &str, item: T) -> Result<(), DataStoreError> {
        match serde_json::to_string_pretty(&item) {
            Ok(content) => {
                println!("{} ==>\n{}", path, content);
                Ok(())
            }
            Err(err) => Err(DataStoreError::FatalIO(std::io::Error::new(
                std::io::ErrorKind::Other,
                err.to_string(),
            ))),
        }
    }
}
