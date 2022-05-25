use crate::proto::etl_grpc::simplestore::bytes_store::observable_bytes_store_server::{
    ObservableBytesStore, ObservableBytesStoreServer as GrpcServer,
};
use crate::proto::etl_grpc::simplestore::bytes_store::{
    ObserveAllRequest, ReadRequest, ReadResponse,
};
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::simple::SimpleStore;
use etl_core::deps::async_trait;
use etl_core::deps::bytes::Bytes;
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

pub type StoreWriteResultRx = oneshot::Receiver<Result<(), DataStoreError>>;
pub type StoreWriteResultTx = oneshot::Sender<Result<(), DataStoreError>>;
pub type StoreLoadResultRx = oneshot::Receiver<Result<Bytes, DataStoreError>>;
pub type StoreLoadResultTx = oneshot::Sender<Result<Bytes, DataStoreError>>;

pub type MpscTx<T> = mpsc::Sender<Result<T, DataStoreError>>;
pub type MpscRx<T> = mpsc::Receiver<Result<T, DataStoreError>>;

// TODO: callbacks are not needed, these are the messages forwarded
pub enum ServerMessage {
    WriteBytes {
        payload: Bytes,
    },
    LoadBytes {
        key: String,
    },
}
// not used at this point
pub enum Message {
    ToServer(ServerMessage),
    ToClient(Bytes),
}

/// forwards messages to connected clients as streams
pub struct ObservableStoreServer {
    pub simplestore_bytes_rx: MpscRx<ServerMessage>,
    /// forward notifications to clients
    pub client_notification_sender: Sender<Bytes>,
    /// connected clients subscribe to this and get all the bytes written to simplestore
    pub client_notification_receivers: Arc<Mutex<HashMap<String, Receiver<Bytes>>>>,
}

pub struct ObservableStoreServerBuilder {
    pub ip_addr: String,
    pub buffer_size: usize,
}

/// Acts like a simple store but forwards everything to a channel.  The channel is meant to be used
/// with ObservableStoreServer
/// TODO: no need oneshot callbacks at all here, wasn't thinking straight
/// just write the message to simplestore, and send the clone to the channel
pub struct GrpcSimpleStore {
    /// the messages are also forwarded here (the same channel ObservableStoreServer listens to)
    store_server_tx: mpsc::Sender<Message>,
    /// the actual storage to use
    pub storage: Box<dyn SimpleStore<Bytes>>,
}

impl ObservableStoreServerBuilder {
    pub fn start(
        self,
        simplestore_bytes_rx: MpscRx<ServerMessage>,
    ) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
        use std::net::ToSocketAddrs;
        use tonic::transport::Server;
        let (broadcast_tx, broadcast_rx) = channel(self.buffer_size);
        //let (simplestore_bytes_tx, simplestore_bytes_rx): (MpscTx<ServerMessage>, _) = mpsc::channel(1);
        let ip_addr_str = self.ip_addr.clone();
        let jh = tokio::spawn(async move {
            let reflection_svc = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(crate::proto::FILE_DESCRIPTOR_SET)
                .build()
                .unwrap();
            Server::builder()
                .add_service(reflection_svc)
                .add_service(GrpcServer::new(ObservableStoreServer {
                    //ds_tx: Arc::new(Mutex::new(ds_tx)),
                    simplestore_bytes_rx,
                    client_notification_sender: broadcast_tx,
                    // don't really need these as the rx is dropped after the client leaves
                    client_notification_receivers: Arc::new(Mutex::new(HashMap::new())),
                }))
                .serve(ip_addr_str.to_socket_addrs().unwrap().next().unwrap())
                .await
                .unwrap();
            ()
        });
        Ok(jh)
    }
}

// TODO: add writing to datastore
#[async_trait]
impl SimpleStore<Bytes> for GrpcSimpleStore {
    async fn write(&self, _: &str, payload: Bytes) -> Result<(), DataStoreError> {
        match self
            .store_server_tx
            .send(Message::ToServer(ServerMessage::WriteBytes {
                payload,
            }))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(DataStoreError::send_error(
                "GrpcSimpleStore",
                "Sender<ToServerMessage>",
                e.to_string(),
            )),
        }
    }

    async fn load(&self, key: &str) -> Result<Bytes, DataStoreError> {
        let r = self.storage.load(key).await;
        match self
            .store_server_tx
            .send(Message::ToServer(ServerMessage::LoadBytes {
                key: key.to_string(),
            }))
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

type ReadResponseStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send>>;

#[tonic::async_trait]
impl ObservableBytesStore for ObservableStoreServer {
    type ObserveKeyStream = ReadResponseStream;
    type ObserveAllStream = ReadResponseStream;
    /*
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        unimplemented!();
    }
    */
    async fn observe_key(
        &self,
        request: tonic::Request<ReadRequest>,
    ) -> Result<Response<Self::ObserveKeyStream>, tonic::Status> {
        unimplemented!();
    }

    async fn observe_all(
        &self,
        request: tonic::Request<ObserveAllRequest>,
    ) -> Result<Response<Self::ObserveKeyStream>, tonic::Status> {
        unimplemented!();
    }
}
