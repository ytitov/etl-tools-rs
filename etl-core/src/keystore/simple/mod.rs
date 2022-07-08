use async_trait::async_trait;
use std::fmt::Debug;
use crate::datastore::error::*;

/// wraps any SimpleStore and uses tokio channels to route all operations to it thus allowing to
/// be clonable.  This approach avoids mutex locking since there is a single Rx channel which
/// receives all of the data and actually applies the reads and writes
pub mod mpsc;

#[async_trait]
/// This is a simple store that acts like a key-val storage.  It is not streamted
/// so is not meant for big files.  Primarily created for the JobRunner to
/// store the state of the running job somewhere
pub trait SimpleStore<T: 'static + Send>: Sync + Send {
    fn path_sep(&self) -> &'static str {
        "/"
    }

    /// given a key, return the parent folder based on the path separator.  Default path separator
    /// is "/"
    fn parent_folder<'a>(&self, key: &'a str) -> &'a str {
        let sep = self.path_sep();
        if let Some(num) = key.rfind(sep) {
            key.split_at(num + 1).0
        } else {
            ""
        }
    }

    async fn read_file_str(&self, _: &str) -> Result<String, DataStoreError> {
        panic!("This SimpleStore does not support load operation");
    }

    async fn load(&self, _: &str) -> Result<T, DataStoreError> {
        panic!("This SimpleStore does not support load operation");
    }

    async fn write(&self, _: &str, _: T) -> Result<(), DataStoreError> {
        panic!("This SimpleStore does not support write operation");
    }
}

#[async_trait]
pub trait QueryableStore<T>: SimpleStore<T> + Sync + Send
where
    T: 'static + Send,
{
    async fn list_keys(&self, prefix: Option<&str>) -> Result<Vec<String>, DataStoreError>;
}

#[derive(Debug)]
pub struct DataPair<L: Send, R: Send> {
    pub left: Option<L>,
    pub right: Option<R>,
}

/// For storing file pairs where the index is the same
pub struct PairedDataStore<L, R> {
    pub left_ds: Box<dyn SimpleStore<L>>,
    pub right_ds: Box<dyn SimpleStore<R>>,
}

impl<L: 'static + Send + Debug, R: 'static + Send + Debug> PairedDataStore<L, R> {
    pub fn box_simplestore<S, D>(s: S) -> Box<dyn SimpleStore<D>>
    where
        S: 'static + SimpleStore<D>,
        D: 'static + Send + Debug,
    {
        Box::new(s)
    }
}

#[async_trait]
impl<L: 'static + Send + Debug, R: 'static + Send + Debug> SimpleStore<DataPair<L, R>>
    for PairedDataStore<L, R>
{
    async fn load(&self, key: &str) -> Result<DataPair<L, R>, DataStoreError> {
        let left = match self.left_ds.load(key).await {
            Ok(item) => Some(item),
            Err(DataStoreError::NotExist { .. }) => None,
            Err(e) => return Err(e),
        };
        let right = match self.right_ds.load(key).await {
            Ok(item) => Some(item),
            Err(DataStoreError::NotExist { .. }) => None,
            Err(e) => return Err(e),
        };
        match (left, right) {
            (None, None) => Err(DataStoreError::NotExist {
                key: key.to_string(),
                error: "Neither item was found".to_string(),
            }),
            (left, right) => Ok(DataPair { left, right }),
        }
    }

    async fn write(&self, key: &str, items: DataPair<L, R>) -> Result<(), DataStoreError> {
        if let Some(left) = items.left {
            self.left_ds.write(key, left).await?;
        }
        if let Some(right) = items.right {
            self.right_ds.write(key, right).await?;
        }
        Ok(())
    }
}
