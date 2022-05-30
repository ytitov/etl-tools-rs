use super::error::*;
use crate::datastore::simple::SimpleStore;
use crate::datastore::{
    DataOutput, DataOutputMessage, DataOutputDetails, DataOutputTask, DataOutputTx, DataSource,
    DataSourceMessage, DataSourceDetails, DataSourceTask,
    DataSourceJoinHandle,
    DataOutputJoinHandle,
};
use crate::queue::QueueClient;
use async_trait::async_trait;
use bytes::Bytes;
use log;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::hash::Hash;
use std::path::Path;

pub struct LocalFs {
    pub files: Vec<String>,
    pub home: String,
    pub output_name: Option<String>,
}

impl Default for LocalFs {
    fn default() -> Self {
        LocalFs {
            files: Vec::new(),
            home: "".to_string(),
            output_name: Some("output".to_string()),
        }
    }
}

impl DataSource<Bytes> for LocalFs {
    fn name(&self) -> String {
        format!("LocalFs-{}", &self.home)
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<Bytes>, DataStoreError> {
        use tokio::fs::File;
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let files = self.files.clone();
        let home = self.home.clone();
        let name = self.name();
        let jh: DataSourceJoinHandle = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            for fname in files {
                //TODO: would prefer this to fail higher, but the test is not handled properly
                //higher up.  must create a test then rectify this
                let file = File::open(Path::new(&home).join(&fname))
                    .await
                    .expect("File not found");
                // 68 mb in size
                let mut lines = BufReader::with_capacity(1 << 26, file).lines();
                loop {
                    if let Some(line) = lines.next_line().await? {
                        lines_scanned += 1;
                        tx.send(Ok(DataSourceMessage::new(&fname, Bytes::from(line))))
                            .await
                            .map_err(|er| DataStoreError::send_error(&name, &fname, er))?;
                    } else {
                        break;
                    }
                }
            }
            Ok(DataSourceDetails::Basic { lines_scanned })
        });
        Ok((rx, jh))
    }
}

#[async_trait]
impl SimpleStore<String> for LocalFs {
    async fn load(&self, path: &str) -> Result<String, DataStoreError> {
        use std::path::Path;
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;
        let p = Path::new(&self.home).join(path);
        if false == p.exists() {
            return Err(DataStoreError::NotExist {
                key: format!("{:?}", p),
                error: "Path does not exist".to_string(),
            });
        }
        let mut file = File::open(p).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        let s = std::string::String::from_utf8_lossy(&contents);
        Ok((*s).to_string())
    }

    async fn write(&self, path: &str, item: String) -> Result<(), DataStoreError> {
        use std::path::Path;
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        let full_path = Path::new(&self.home).join(path);
        log::info!("Writing to file {:?}", &full_path);
        if let Some(parent_folder) = full_path.parent() {
            tokio::fs::create_dir_all(parent_folder).await?;
            log::info!("Writing to folder {:?}", &parent_folder);
        } else {
            tokio::fs::create_dir_all(Path::new(&self.home)).await?;
            log::info!("Writing to folder {}", &self.home);
        }
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(Path::new(&self.home).join(path))
            .await?;
        match file.write_all(item.as_bytes()).await {
            Ok(()) => Ok(()),
            Err(err) => Err(DataStoreError::FatalIO(err.to_string())),
        }
    }
}

#[async_trait]
impl<T: Hash + Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static>
    QueueClient<T> for LocalFs
{
    async fn pop(&self) -> anyhow::Result<Option<T>> {
        panic!("QueueClient::pop for LocalFs is not implemented");
    }
    async fn push(&self, m: T) -> anyhow::Result<()> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        m.hash(&mut hasher);
        let name = format!("{}.push.json", hasher.finish());
        self.write(&name, serde_json::to_string(&m)?).await?;
        Ok(())
    }
}

impl LocalFs {
    pub async fn load_toml<T>(p: &str, autocreate: bool) -> anyhow::Result<T>
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + Default,
    {
        use anyhow::anyhow;
        use tokio::fs;
        match fs::read_to_string(p).await {
            Ok(cont) => match toml::from_str(&cont) {
                Ok(cfg) => Ok(cfg),
                Err(err) => Err(anyhow!("There is an error in your config: {}", err)),
            },
            Err(err) => {
                if autocreate == true {
                    use tokio::io::AsyncWriteExt;
                    let cfg = T::default();
                    println!("Creating default config: {:?}", &cfg);
                    let mut file = fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(p)
                        .await?;
                    file.write(toml::Value::try_from(&cfg)?.to_string().as_bytes())
                        .await?;
                    Ok(cfg)
                } else {
                    Err(anyhow!("Error opening Configuration file: {}", err))
                }
            }
        }
    }
}

impl DataOutput<Bytes> for LocalFs {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<Bytes>, DataStoreError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        use tokio::sync::mpsc::channel;
        let filename = match self.output_name {
            Some(n) => n,
            None => "output".to_string(),
        };
        //let filepath = format!("{}/{}", &self.home, &filename);
        let full_path = Path::new(&self.home).join(&filename);
        log::info!("Writing to file {:?}", &full_path);
        let (tx, mut rx): (DataOutputTx<Bytes>, _) = channel(1);
        let self_home = self.home;
        let jh: DataOutputJoinHandle = tokio::spawn(async move {
            if let Some(parent_folder) = full_path.parent() {
                tokio::fs::create_dir_all(parent_folder).await?;
                log::info!("Writing to folder {:?}", &parent_folder);
            } else {
                tokio::fs::create_dir_all(Path::new(&self_home)).await?;
                log::info!("Writing to folder {}", &self_home);
            }
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&full_path)
                .await
                .map_err(|e| {
                    DataStoreError::FatalIO(format!(
                    "LocalFs ran into an error trying to open the file: {:?} caused by error: {}",
                    &full_path, e
                ))
                })?;
            let mut num_lines_sent = 0_usize;
            loop {
                match rx.recv().await {
                    Some(DataOutputMessage::Data(item)) => {
                        file.write_all(&item).await?;
                        num_lines_sent += 1;
                    }
                    Some(DataOutputMessage::NoMoreData) => {
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }
            Ok(DataOutputDetails::Basic {
                lines_written: num_lines_sent,
            })
        });
        Ok((tx, jh))
    }
}
