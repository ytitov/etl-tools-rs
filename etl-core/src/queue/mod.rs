//use async_trait::async_trait;
use crate::datastore::error::*;
use crate::datastore::*;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use tokio::sync::oneshot;
use crate::deps::*;

#[async_trait]
pub trait QueueClientBuilder<T: Debug + 'static + Send>: Sync + Send {
    async fn build_client(self: Box<Self>) -> Result<Box<dyn QueueClient<T>>, DataStoreError>;
}

/*
#[async_trait]
impl<T: DeserializeOwned + Debug + Send + 'static> QueueClientBuilder<T> for GenDataSource<T> {
    async fn build_client(self: Box<Self>) -> Result<Box<dyn QueueClient<T>>, DataStoreError> {
        Ok(self.ds)
    }
}
*/

#[async_trait]
pub trait QueueClient<T: Debug + 'static + Send>: Sync + Send {
    async fn start_incoming(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        panic!("not implemented");
    }
    async fn pop(&self) -> anyhow::Result<Option<T>> {
        println!("QueueClient::pop default implementation always returns None");
        Ok(None)
    }
    async fn pop_reply(&self) -> anyhow::Result<Option<(T, oneshot::Receiver<T>)>> {
        println!("QueueClient::pop default implementation always returns None");
        Ok(None)
    }
    async fn message_ok(&self, _: T) -> Result<(), DataStoreError> {
        Ok(())
    }
}

/// provide so each DataSource could be a client
#[async_trait]
impl<T: DeserializeOwned + Debug + Send + 'static> QueueClient<T> for DynDataSource<T> {
    async fn start_incoming(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        let ds_name: String = self.ds.name();
        let (mut source_rx, source_stream_jh) = self.ds.start_stream()?;
        let (tx, rx) = channel(1);
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                match source_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data {
                        source,
                        content: item,
                    })) => {
                        lines_scanned += 1;
                        tx.send(Ok(DataSourceMessage::new(&ds_name, item)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&ds_name, &source, e))?;
                    }
                    Some(Err(val)) => {
                        tx.send(Err(DataStoreError::Deserialize {
                            message: val.to_string(),
                            attempted_string: "Unknown".to_string(),
                        }))
                        .await
                        .map_err(|e| DataStoreError::send_error(&ds_name, "", e))?;
                    }
                    None => break,
                }
            }
            source_stream_jh.await??;
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
