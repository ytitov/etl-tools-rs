use crate::proto::etl_grpc::simplestore::bytes_store::observable_bytes_store_client::ObservableBytesStoreClient;
use crate::proto::etl_grpc::simplestore::bytes_store::{
    LoadRequest, LoadResponse, ObserveAllRequest, ObserveKeyRequest, WriteRequest, WriteResponse,
};
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::simple::QueryableStore;
use etl_core::datastore::simple::SimpleStore;
use etl_core::deps::async_trait;
use etl_core::deps::bytes::Bytes;
use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::Response;

pub struct GrpcSimpleStoreClient {
    pub grpc_client: Arc<Mutex<ObservableBytesStoreClient<tonic::transport::Channel>>>,
}

impl GrpcSimpleStoreClient {
    pub async fn connect(url_str: &str) -> Result<Self, DataStoreError> {
        Ok(GrpcSimpleStoreClient {
            grpc_client: Arc::new(Mutex::new(
                ObservableBytesStoreClient::connect(url_str.to_string())
                    .await
                    .map_err(|e| {
                        DataStoreError::transport("Could not connect to grpc simple store", e)
                    })?,
            )),
        })
    }
}
#[async_trait]
impl QueryableStore<Bytes> for GrpcSimpleStoreClient {
    async fn list_keys(&self, _: Option<&str>) -> Result<Vec<String>, DataStoreError> {
        return Ok(Vec::new());
    }
}

#[async_trait]
impl SimpleStore<Bytes> for GrpcSimpleStoreClient {
    async fn write(&self, key: &str, payload: Bytes) -> Result<(), DataStoreError> {
        let grpc_client = Arc::clone(&self.grpc_client);
        let mut client_lock = grpc_client.lock().await;
        match client_lock
            .write(WriteRequest {
                key: key.to_string(),
                bytes_content: payload.to_vec(),
            })
            .await
        {
            Ok(_) => {
                // getting odd behavior when lock is not dropped this way
                drop(client_lock);
                Ok(())
            }
            Err(er) => {
                drop(client_lock);
                Err(DataStoreError::FatalIO(er.to_string()))
            }
        }
    }

    async fn load(&self, key: &str) -> Result<Bytes, DataStoreError> {
        let grpc_client = Arc::clone(&self.grpc_client);
        let mut client_lock = grpc_client.lock().await;
        match client_lock
            .load(LoadRequest {
                key: key.to_string(),
            })
            .await
        {
            Ok(r) => {
                drop(client_lock);
                match r.into_inner() {
                    LoadResponse {
                        key: _,
                        bytes_content,
                        error: None,
                    } => Ok(Bytes::from(bytes_content)),
                    LoadResponse {
                        error: Some(er), ..
                    } => Err(er.into()),
                }
            }
            Err(er) => {
                drop(client_lock);
                Err(DataStoreError::FatalIO(er.to_string()))
            }
        }
    }
}
