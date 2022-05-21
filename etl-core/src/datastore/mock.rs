use super::*;
use ::log;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use simple::{QueryableStore, SimpleStore};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

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

pub struct MockDataOutput {
    pub name: String,
    pub sleep_duration: Duration,
}

impl Default for MockDataOutput {
    fn default() -> Self {
        MockDataOutput {
            name: String::from("MockDataOutput"),
            sleep_duration: Duration::from_millis(0),
        }
    }
}

impl<T: Serialize + Debug + Send + Sync + 'static> DataOutput<T> for MockDataOutput {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, mut rx): (DataOutputTx<T>, _) = channel(1);
        let sleep_duration = self.sleep_duration;
        let name = self.name.clone();
        let jh: JoinHandle<Result<DataOutputStats, DataStoreError>> = tokio::spawn(async move {
            let name = name;
            let mut lines_written = 0;
            loop {
                tokio::time::sleep(sleep_duration).await;
                match rx.recv().await {
                    Some(m) => {
                        match m {
                            DataOutputMessage::Data(data) => {
                                // serialize
                                match serde_json::to_value(&data) {
                                    Ok(line) => {
                                        //println!("{} received: {}", &name, &line);
                                        log::info!("{} received: {:?}", &name, line);
                                        lines_written += 1;
                                    }
                                    Err(e) => {
                                        log::error!("{} ERROR {}", &name, e);
                                    }
                                }
                            }
                            DataOutputMessage::NoMoreData => {
                                log::debug!("received DataOutputMessage::NoMoreData");
                                break;
                            }
                        };
                    }
                    None => break,
                };
            }
            Ok(DataOutputStats {
                name,
                lines_written,
            })
        });
        Ok((tx, jh))
    }
}

impl DataOutput<Bytes> for MockJsonDataOutput {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<Bytes>, DataStoreError> {
        use serde_json::Value as JsonValue;
        use tokio::sync::mpsc::channel;
        let (tx, mut rx): (DataOutputTx<Bytes>, _) = channel(1);
        let sleep_duration = self.sleep_duration;
        let name = self.name.clone();
        let jh: JoinHandle<Result<DataOutputStats, DataStoreError>> = tokio::spawn(async move {
            let name = name;
            let mut lines_written = 0;
            loop {
                tokio::time::sleep(sleep_duration).await;
                match rx.recv().await {
                    Some(m) => {
                        match m {
                            DataOutputMessage::Data(data) => {
                                // serialize
                                match serde_json::from_slice(&data) {
                                    Ok::<JsonValue, _>(line) => {
                                        log::info!("{} received: {}", &name, line);
                                        lines_written += 1;
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "{} Could not convert to json value due to: {}",
                                            &name,
                                            e
                                        );
                                    }
                                }
                            }
                            DataOutputMessage::NoMoreData => {
                                log::debug!("received DataOutputMessage::NoMoreData");
                                break;
                            }
                        };
                    }
                    None => break,
                };
            }
            Ok(DataOutputStats {
                name,
                lines_written,
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
    pub files: Arc<Mutex<RefCell<HashMap<String, Bytes>>>>,
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
/*
#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> QueryableStore<T>
    for MockJsonDataSource
{
    async fn list_keys(&self, prefix: Option<&str>) -> Result<Vec<String>, DataStoreError> {
        match self.files.lock() {
            Ok(files) => {
                //return Ok(files.borrow().keys().map(|(key,_)| String::from(key)))
                let mut v = Vec::<String>::new();
                for key in files.borrow().keys() {
                    match &prefix {
                        Some(p) => {
                            if key.starts_with(p) {
                                v.push(key.into());
                            }
                        }
                        None => {
                            v.push(key.into());
                        }
                    }
                }
                return Ok(v);
            }
            Err(er) => {
                return Err(DataStoreError::FatalIO(format!(
                    "Could not obtain lock on files due to: {}",
                    er.to_string()
                )));
            }
        }
    }
}
*/
#[async_trait]
impl QueryableStore<Bytes> for MockJsonDataSource {
    async fn list_keys(&self, prefix: Option<&str>) -> Result<Vec<String>, DataStoreError> {
        match self.files.lock() {
            Ok(files) => {
                //return Ok(files.borrow().keys().map(|(key,_)| String::from(key)))
                let mut v = Vec::<String>::new();
                for key in files.borrow().keys() {
                    match &prefix {
                        Some(p) => {
                            if key.starts_with(p) {
                                v.push(key.into());
                            }
                        }
                        None => {
                            v.push(key.into());
                        }
                    }
                }
                return Ok(v);
            }
            Err(er) => {
                return Err(DataStoreError::FatalIO(format!(
                    "Could not obtain lock on files due to: {}",
                    er.to_string()
                )));
            }
        }
    }
}

#[async_trait]
impl SimpleStore<Bytes> for MockJsonDataSource {
    async fn load(&self, path: &str) -> Result<Bytes, DataStoreError> {
        match self.files.lock() {
            Ok(files) => {
                let files_hs = files.borrow();
                if let Some(content) = files_hs.get(path) {
                    return Ok(content.clone());
                } else {
                    log::info!("{} does not exist", path);
                    return Err(DataStoreError::NotExist {
                        key: path.to_owned(),
                        error: format!("{} does not exist", path),
                    });
                }
            }
            Err(er) => {
                return Err(DataStoreError::FatalIO(format!(
                    "Failed locking files: {}",
                    er
                )));
            }
        }
    }

    async fn write(&self, path: &str, item: Bytes) -> Result<(), DataStoreError> {
        match self.files.lock() {
            Ok(files) => {
                files.borrow_mut().insert(path.to_string(), item);
            }
            Err(er) => {
                //TODO: this should return a DataStoreError
                panic!("Got error calling files.lock(): {}", er);
            }
        };
        Ok(())
    }
}

/*
#[async_trait]
impl<T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> SimpleStore<T>
    for MockJsonDataSource
{
    async fn load(&self, path: &str) -> Result<T, DataStoreError> {
        match self.files.lock() {
            Ok(files) => {
                let files_hs = files.borrow();
                if let Some(content) = files_hs.get(path) {
                    match serde_json::from_slice::<T>(content) {
                        Ok(result) => {
                            log::debug!(
                                "MockJsonDataOutput::load: {} ===>\n{}",
                                path,
                                serde_json::to_string_pretty(&result).unwrap()
                            );
                            Ok(result)
                        }
                        Err(er) => {
                            return Err(DataStoreError::Deserialize {
                                attempted_string: "Bytes".into(),
                                message: er.to_string(),
                            });
                        }
                    }
                } else {
                    log::info!("{} does not exist", path);
                    return Err(DataStoreError::NotExist {
                        key: path.to_owned(),
                        error: format!("{} does not exist", path),
                    });
                }
            }
            Err(er) => {
                return Err(DataStoreError::FatalIO(format!(
                    "Failed locking files: {}",
                    er
                )));
            }
        }
    }

    async fn write(&self, path: &str, item: T) -> Result<(), DataStoreError> {
        match serde_json::to_string_pretty(&item) {
            Ok(content) => {
                log::info!("MockJsonDataOutput::write: {} ===>\n{}", path, &content);
                match self.files.lock() {
                    Ok(files) => {
                        files
                            .borrow_mut()
                            .insert(path.to_string(), Bytes::from(content));
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
*/

use crate::queue::QueueClient;
#[async_trait]
impl<T: Hash + Serialize + DeserializeOwned + Debug + Send + Sync + 'static> QueueClient<T>
    for MockJsonDataSource
{
    async fn pop(&self) -> anyhow::Result<Option<T>> {
        use std::cell::RefMut;
        if let Ok(inner) = self.files.lock() {
            match inner.try_borrow_mut() {
                Ok::<RefMut<'_, HashMap<String, Bytes>>, _>(mut hs) => {
                    let maybe_key: Option<String> = hs.keys().next().cloned();
                    if let Some(key) = maybe_key {
                        if let Some(item) = hs.remove(&key) {
                            return Ok(serde_json::from_slice(&item)?);
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

    async fn push(&self, m: T) -> anyhow::Result<()> {
        use std::cell::RefMut;
        if let Ok(inner) = self.files.lock() {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hasher;
            let mut hasher = DefaultHasher::new();
            m.hash(&mut hasher);
            let name = format!("{}", hasher.finish());
            match inner.try_borrow_mut() {
                Ok::<RefMut<'_, HashMap<String, Bytes>>, _>(mut hs) => {
                    let c = serde_json::to_string_pretty(&m)?;
                    log::info!("Pushed into MockJsonDataSource: {}", &c);
                    hs.insert(name, Bytes::from(c));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(e));
                }
            }
        };
        Ok(())
    }
}
