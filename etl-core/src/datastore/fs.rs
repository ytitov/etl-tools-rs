use std::path::Path;
use crate::datastore::bytes_source::*;
use crate::datastore::SimpleStore;
use async_trait::async_trait;
use super::error::*;
use tokio::task::JoinHandle;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct LocalFs {
    pub files: Vec<String>,
    pub home: String,
}

impl BytesSource for LocalFs {
    fn name(&self) -> String {
        format!("LocalFs-{}", &self.home)
    }

    fn start_stream(self: Box<Self>) -> Result<BytesSourceTask, DataStoreError> {
        use tokio::fs::File;
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let files = self.files.clone();
        let home = self.home.clone();
        let name = self.name();
        let jh: JoinHandle<Result<BytesSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            for fname in files {
                let file = File::open(Path::new(&home).join(&fname)).await?;
                // 68 mb in size
                let mut lines = BufReader::with_capacity(1 << 26, file).lines();
                loop {
                    if let Some(line) = lines.next_line().await? {
                        lines_scanned += 1;
                        tx.send(Ok(BytesSourceMessage::new(&fname, line)))
                            .await
                            .map_err(|er| DataStoreError::send_error(&name, &fname, er))?;
                    } else {
                        break;
                    }
                }
            }
            Ok(BytesSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static>
    SimpleStore<T> for LocalFs
{
    async fn load(&self, path: &str) -> Result<T, DataStoreError> {
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
        match serde_json::from_str::<T>(&s) {
            Ok::<T, _>(obj) => Ok(obj),
            Err(e) => Err(DataStoreError::Deserialize {
                attempted_string: format!("Could not deserialize {}", path),
                message: e.to_string(),
            }),
        }
    }

    async fn write(&self, path: &str, item: T) -> Result<(), DataStoreError> {
        use std::path::Path;
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        tokio::fs::create_dir_all(Path::new(&self.home)).await?;
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(Path::new(&self.home).join(path))
            .await?;
        match serde_json::to_string_pretty(&item) {
            Ok(content) => match file.write_all(content.as_bytes()).await {
                Ok(()) => Ok(()),
            Err(err) => Err(DataStoreError::FatalIO(err.to_string())),
            },
            Err(err) => Err(DataStoreError::FatalIO(err.to_string())),
        }
    }
}
