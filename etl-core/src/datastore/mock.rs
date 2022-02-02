use super::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use serde::{Serialize, de::DeserializeOwned};

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
    async fn start_stream(&mut self, jm_tx: JobManagerTx) -> anyhow::Result<DataOutputTask<T>> {
        use tokio::sync::mpsc::channel;
        let (tx, mut rx): (DataOutputTx<T>, _) = channel(1);
        let sleep_duration = self.sleep_duration;
        let name = self.name.clone();
        let jh: JoinHandle<anyhow::Result<DataOutputStats>> = tokio::spawn(async move {
            let name = name;
            let mut lines_written = 0;
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
                                        lines_written += 1;
                                    }
                                    Err(e) => {
                                        let m = format!("{} ERROR {}", &name, e);
                                        jm_tx.send(Message::log_err(&name, m)).await?;
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
            Ok(DataOutputStats {
                name,
                lines_written
            })
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
    async fn load(&self, path: &str) -> Result<T, DataStoreError> {
        //println!("MockJsonDataSource::load({}) current files: {:?}", path, &self.files);
        match self.files.lock() {
            Ok(files) => {
                let files_hs = files.borrow();
                if let Some(content) = files_hs.get(path) {
                    match serde_json::from_str::<T>(content) {
                        Ok(result) => {
                            println!(
                                "MockJsonDataOutput::load: {} ===>\n{}",
                                path,
                                serde_json::to_string_pretty(&result).unwrap()
                            );
                            Ok(result)
                        }
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
                            println!("MockJsonDataOutput::write: {} ===>\n{}", path, f);
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

use crate::queue::QueueClient;
#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> QueueClient<T>
    for MockJsonDataSource
{
    async fn pop(&self) -> anyhow::Result<Option<T>> {
        use std::cell::RefMut;
        if let Ok(inner) = self.files.lock() {
            match inner.try_borrow_mut() {
                Ok::<RefMut<'_, HashMap<String, String>>,_>(mut hs) => {
                    let maybe_key: Option<String> = hs.keys().next().cloned();
                    if let Some(key) = maybe_key {
                        if let Some(item) = hs.remove(&key) {
                            return Ok(serde_json::from_str(&item)?);
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(e));
                }
            }
        }
        Ok(None)
    }
}
