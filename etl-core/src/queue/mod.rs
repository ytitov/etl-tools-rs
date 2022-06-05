//use async_trait::async_trait;
use crate::datastore::error::*;
use crate::datastore::*;
use crate::deps::*;
use anyhow::Result;
use std::fmt::Debug;
use tokio::sync::oneshot;

#[async_trait]
pub trait QueueClientBuilder<T: Debug + 'static + Send>: Sync + Send {
    async fn build_client(self: Box<Self>) -> Result<Box<dyn QueueClient<T>>, DataStoreError>;
}

#[async_trait]
pub trait QueueClient<T: Debug + 'static + Send>: Sync + Send {
    /// so any QueueClient can be turned into a DataSource<T>
    async fn start_incoming(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        panic!("not implemented");
    }
    async fn pop(&self) -> anyhow::Result<Option<T>> {
        panic!("QueueClient::pop is not implemented");
    }
    async fn push(&self, _: T) -> Result<()> {
        panic!("QueueClient::pop is not implemented");
    }
    /// For cases when a produced item needs some action to be performed afterward by the queue,
    /// for example to perform some cleanup or deletion by the QueueClient implementation
    async fn pop_result(&self) -> anyhow::Result<Option<(T, oneshot::Receiver<Result<T>>)>> {
        panic!("QueueClient::pop_result is not implemented");
    }
}

/// provide so each DataSource could be a client
#[async_trait]
impl<T> QueueClient<T> for DynDataSource<'static, T>
where
    T: Debug + Send + 'static,
{
    async fn start_incoming(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let ds_name: String = self.ds.name();
        let (mut source_rx, source_stream_jh) = self.ds.start_stream()?;
        let (tx, rx) = channel(1);
        let jh: DataSourceJoinHandle = tokio::spawn(async move {
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
            Ok(DataSourceDetails::Basic { lines_scanned })
        });
        Ok((rx, jh))
    }
}
