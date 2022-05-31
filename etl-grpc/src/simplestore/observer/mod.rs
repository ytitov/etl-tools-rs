use etl_core::deps::bytes::Bytes;
use etl_core::datastore::simple::SimpleStore;
use etl_core::deps::async_trait;
use tokio::sync::mpsc;

pub mod server;

#[derive(Debug, Clone)]
pub enum SimpleStoreEvent {
    WriteBytes { key: String, payload: Bytes },
    LoadBytes { key: String },
    SimpleStoreError { key: String, error: String },
}

