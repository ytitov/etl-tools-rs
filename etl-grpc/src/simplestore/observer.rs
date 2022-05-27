use etl_core::datastore::error::DataStoreError;
use etl_core::deps::bytes::Bytes;
use etl_core::datastore::simple::SimpleStore;
use etl_core::deps::async_trait;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum SimpleStoreEvent {
    WriteBytes { key: String, payload: Bytes },
    LoadBytes { key: String },
    SimpleStoreError { key: String, error: String },
}

/// Acts like a simple store but forwards everything to a channel.  The channel is meant to be used
/// with ObservableStoreServer
pub struct SimpleStoreObserver {
    /// the messages are also forwarded here (the same channel ObservableStoreServer listens to)
    pub store_server_tx: mpsc::Sender<SimpleStoreEvent>,
    /// the actual storage to use
    pub storage: Box<dyn SimpleStore<Bytes>>,
}

#[async_trait]
impl SimpleStore<Bytes> for SimpleStoreObserver {
    async fn write(&self, key: &str, payload: Bytes) -> Result<(), DataStoreError> {
        match self.storage.write(key, payload.clone()).await {
            Ok(_) => match self
                .store_server_tx
                .send(SimpleStoreEvent::WriteBytes { key: key.into(), payload })
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => Err(DataStoreError::send_error(
                    "GrpcSimpleStore",
                    "Sender<SimpleStoreEvent>",
                    e.to_string(),
                )),
            },
            Err(e) => Err(e),
        }
    }

    async fn load(&self, key: &str) -> Result<Bytes, DataStoreError> {
        let r = self.storage.load(key).await;
        match self
            .store_server_tx
            .send(SimpleStoreEvent::LoadBytes {
                key: key.to_string(),
            })
            .await
        {
            Ok(_) => unimplemented!(),
            Err(e) => Err(DataStoreError::send_error(
                "GrpcSimpleStore",
                "Sender<ToServerMessage>",
                e.to_string(),
            )),
        }
    }
}
