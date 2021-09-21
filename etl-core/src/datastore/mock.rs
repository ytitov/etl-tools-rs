use super::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::fmt::Debug;

pub mod mock_csv;

/// Designed as a "null" datastore target.  Items received are not written anywhere
pub struct MockJsonDataOutput {
    pub name: String,
    pub sleep_duration: Duration,
}

impl Default for MockJsonDataOutput {
    fn default() -> Self {
        MockJsonDataOutput {
            name: String::from("MockJsonDataOutput"),
            sleep_duration: Duration::from_millis(0),
        }
    }
}

#[async_trait]
impl<T: Serialize + Debug + Send + Sync + 'static> DataOutput<T> for MockJsonDataOutput {

    async fn start_stream(&mut self, _: JobManagerChannel) -> anyhow::Result<DataOutputTask<T>> {
        use tokio::sync::mpsc::channel;
        let (tx, mut rx): (Sender<DataOutputMessage<T>>, _) = channel(1);
        let sleep_duration = self.sleep_duration;
        let name = self.name.clone();
        let jh: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let name = name;
            loop {
                tokio::time::sleep(sleep_duration).await;
                match rx.recv().await {
                    Some(m) => {
                        match m {
                            DataOutputMessage::Data(data) => {
                                // serialize
                                match serde_json::to_string(&data) {
                                    Ok(line) => {
                                        println!("{} received: {}", &name, line);
                                    }
                                    Err(e) => {
                                        println!("{} ERROR {}", &name, e);
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
pub struct MockJsonDataSource {
    pub lines: Vec<String>,
    //pub files: HashMap<String, String>,
    pub files: Arc<Mutex<RefCell<HashMap<String, String>>>>,
}

impl Default for MockJsonDataSource {
    fn default() -> Self {
        MockJsonDataSource {
            lines: Vec::new(),
            files: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        }
    }
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> DataSource<T>
    for MockJsonDataSource
{

    fn name(&self) -> String {
        format!("MockJsonDataSource")
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        // could make this configurable
        let (tx, rx) = channel(1);
        let name = String::from("MockJsonDataSource");
        let lines = self.lines.clone();
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            for line in lines {
                match serde_json::from_str::<T>(&line) {
                    Ok(r) => {
                        tx.send(Ok(DataSourceMessage::new("MockJsonDataSource", r)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                        lines_scanned += 1;
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
        match self.files.lock() {
            Ok(files) => {
                let files_hs = files.borrow();
                if let Some(content) = files_hs.get(path) {
                    match serde_json::from_str::<T>(content) {
                        Ok(result) => Ok(result),
                        Err(er) => {
                            return Err(DataStoreError::Deserialize {
                                attempted_string: content.to_owned(),
                                message: er.to_string(),
                            });
                        }
                    }
                } else {
                    println!("Loading from MockJsonDataSource will result as not found");
                    return Err(DataStoreError::NotExist {
                        key: path.to_owned(),
                        error: "Loading from MockJsonDataSource will always result in not found"
                            .to_string(),
                    });
                }
            }
            Err(er) => {
                //TODO: this should return a DataStoreError
                panic!("Got error calling files.lock(): {}", er);
            }
        }
    }

    async fn write(&self, path: &str, item: T) -> Result<(), DataStoreError> {
        match serde_json::to_string_pretty(&item) {
            Ok(content) => {
                match self.files.lock() {
                    Ok(files) => {
                        files.borrow_mut().insert(path.to_string(), content);
                        for (_, f) in files.borrow().iter() {
                            println!("FILE: {} ===>\n{}", path, f);
                        }
                    }
                    Err(er) => {
                        //TODO: this should return a DataStoreError
                        panic!("Got error calling files.lock(): {}", er);
                    }
                };
                Ok(())
            }
            Err(err) => Err(DataStoreError::FatalIO(err.to_string())),
        }
    }
}
