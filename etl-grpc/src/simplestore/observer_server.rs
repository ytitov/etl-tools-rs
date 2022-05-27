use crate::proto::etl_grpc::simplestore::bytes_store::observable_bytes_store_server::{
    ObservableBytesStore, ObservableBytesStoreServer as GrpcServer,
};
use crate::proto::etl_grpc::simplestore::bytes_store::{
    LoadRequest, LoadResponse, ObserveAllRequest, ObserveKeyRequest, WriteRequest, WriteResponse,
};
use crate::simplestore::observer::SimpleStoreEvent;
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::simple::SimpleStore;
use etl_core::deps::bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

pub type StoreWriteResultRx = oneshot::Receiver<Result<(), DataStoreError>>;
pub type StoreWriteResultTx = oneshot::Sender<Result<(), DataStoreError>>;
pub type StoreLoadResultRx = oneshot::Receiver<Result<Bytes, DataStoreError>>;
pub type StoreLoadResultTx = oneshot::Sender<Result<Bytes, DataStoreError>>;

pub type MpscTx<T> = mpsc::Sender<Result<T, DataStoreError>>;
pub type MpscRx<T> = mpsc::Receiver<Result<T, DataStoreError>>;

/// forwards messages to connected clients as streams
pub struct ObservableStoreServer {
    /// forward notifications to clients
    pub client_notification_sender: Sender<SimpleStoreEvent>,
    pub simplestore: Box<dyn SimpleStore<Bytes>>,
}

pub struct ObservableStoreServerBuilder {
    pub ip_addr: String,
    pub buffer_size: usize,
    pub simplestore: Box<dyn SimpleStore<Bytes>>,
}

impl ObservableStoreServerBuilder {
    pub fn start(
        self,
    ) -> Result<JoinHandle<Result<(), DataStoreError>>, Box<dyn std::error::Error>> {
        use std::net::ToSocketAddrs;
        let (broadcast_tx, mut broadcast_rx) = channel(self.buffer_size);
        //let (simplestore_bytes_tx, simplestore_bytes_rx): (MpscTx<ServerMessage>, _) = mpsc::channel(1);
        let ip_addr_str = self.ip_addr.clone();
        let simplestore = self.simplestore;
        let jh = tokio::spawn(async move {
            let reflection_svc = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(crate::proto::SIMPLESTORE_REFLECTION_DESCR)
                .build()
                .unwrap();
            Server::builder()
                .add_service(reflection_svc)
                .add_service(GrpcServer::new(ObservableStoreServer {
                    client_notification_sender: broadcast_tx,
                    simplestore,
                }))
                .serve(ip_addr_str.to_socket_addrs().unwrap().next().unwrap())
                .await.map_err(|e| DataStoreError::FatalIO(e.to_string()))
        });
        tokio::spawn(async move {
            use SimpleStoreEvent::*;
            while let Ok(msg) = broadcast_rx.recv().await {
                match msg {
                    WriteBytes { key, payload } => {
                        use etl_core::deps::serde_json::Value as JsonValue;
                        let j = etl_core::deps::serde_json::from_slice::<JsonValue>(&payload);
                        log::info!("WriteBytes: {} - {:#?}", key, j);
                    }
                    LoadBytes { key, .. } => {
                        log::info!("LoadBytes: {}", key);
                    }
                    SimpleStoreError { key, error } => {
                        log::info!("Error: {} - {}", key, error);
                    }
                }
            }
        });
        Ok(jh)
    }
}

type ReadResponseStream = Pin<Box<dyn Stream<Item = Result<LoadResponse, Status>> + Send>>;

#[tonic::async_trait]
impl ObservableBytesStore for ObservableStoreServer {
    type ObserveKeyStream = ReadResponseStream;
    type ObserveAllStream = ReadResponseStream;

    async fn load(&self, req: Request<LoadRequest>) -> Result<Response<LoadResponse>, Status> {
        let LoadRequest { key } = req.into_inner();
        match self.simplestore.load(&key).await {
            Ok(b) => {
                self.client_notification_sender
                    .send(SimpleStoreEvent::LoadBytes { key: key.clone() })
                    .expect("failed sending to clients");
                Ok(Response::new(LoadResponse {
                    key,
                    bytes_content: b.to_vec(),
                    error: None,
                }))
            }
            Err(er) => {
                self.client_notification_sender
                    .send(SimpleStoreEvent::SimpleStoreError {
                        key: key.clone(),
                        error: er.to_string(),
                    })
                    .expect("failed sending to clients");
                Ok(Response::new(LoadResponse {
                    key,
                    bytes_content: Vec::new(),
                    error: Some(er.into()),
                }))
            }
        }
    }

    async fn write(&self, req: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        let WriteRequest { key, bytes_content } = req.into_inner();
        let bytes_content = Bytes::from(bytes_content);
        match self.simplestore.write(&key, bytes_content.clone()).await {
            Ok(_) => {
                self.client_notification_sender
                    .send(SimpleStoreEvent::WriteBytes {
                        key: key.clone(),
                        payload: bytes_content,
                    })
                    .expect("failed sending to clients");
                Ok(Response::new(WriteResponse { key, error: None }))
            }
            Err(er) => Ok(Response::new(WriteResponse {
                key,
                error: Some(er.into()),
            })),
        }
    }

    async fn observe_key(
        &self,
        request: tonic::Request<ObserveKeyRequest>,
    ) -> Result<Response<Self::ObserveKeyStream>, tonic::Status> {
        unimplemented!();
    }

    async fn observe_all(
        &self,
        request: tonic::Request<ObserveAllRequest>,
    ) -> Result<Response<Self::ObserveKeyStream>, tonic::Status> {
        let mut events_rx = self.client_notification_sender.subscribe();
        let (client_tx, client_rx) = mpsc::channel(128);
        tokio::spawn(async move {
            use SimpleStoreEvent::*;
            while let Ok(result) = events_rx.recv().await {
                match result {
                    WriteBytes { key, payload } => client_tx
                        .send(Ok(LoadResponse {
                            key,
                            bytes_content: payload.to_vec(),
                            error: None,
                        }))
                        .await
                        .expect("working rx"),
                    _ => {}
                }
            }
            println!("\tstream ended");
        });
        let out_stream = ReceiverStream::new(client_rx);
        Ok(Response::new(Box::pin(out_stream) as Self::ObserveAllStream))
    }
}
