// example: https://github.com/hyperium/tonic/blob/master/examples/src/streaming/server.rs
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::*;
use etl_core::deps::bytes::Bytes;
use etl_core::joins::CreateDataOutputFn;

use super::datastore::data_store_server::{DataStore as GrpcDataStore, DataStoreServer};
use super::datastore::{
    DataOutputResponse, DataOutputResult, DataOutputStats as GrpcDataOutputStats,
    DataOutputStringMessage, ServerJoinHandle,
};

use tokio_stream::StreamExt;
use tonic::{Code, Request, Response, Status, Streaming};
use std::sync::Arc;
use std::sync::Mutex;

/// Provides a DataSource
pub struct DataSourceServer {
    //ds_tx: Arc<Mutex<DataSourceTx<Bytes>>>,
    ds_tx: DataSourceTx<Bytes>,
    server_name: String,
}
// DataSourceRx<T>

#[derive(Default)]
pub struct DataSourceServerBuilder {
    pub ip_addr: String,
    pub buffer_size: usize,
}

impl DataSourceServerBuilder {
    pub fn start(
        self,
        // ) -> Result<(Box<dyn DataSource<Bytes>>, DataSourceServer), Box<dyn std::error::Error>> {
    ) -> Result<(Box<dyn DataSource<Bytes>>, ServerJoinHandle), Box<dyn std::error::Error>> {
        use etl_core::deps::tokio::sync::mpsc::channel;
        use std::net::ToSocketAddrs;
        use tonic::transport::Server;
        //let (ds_tx, ds_rx): (DataSourceTx<Bytes>, DataSourceRx<Bytes>) = channel(1);
        let (ds_tx, ds_rx) = channel(self.buffer_size);

        let ip_addr_str = self.ip_addr.clone();
        let jh: ServerJoinHandle = tokio::spawn(async move {
            Server::builder()
                .add_service(DataStoreServer::new(DataSourceServer {
                    server_name: "GrpcDataSource".to_string(),
                    //ds_tx: Arc::new(Mutex::new(ds_tx)),
                    ds_tx,
                }))
                .serve(ip_addr_str.to_socket_addrs().unwrap().next().unwrap())
                .await
                .unwrap();
            Ok(())
        });

        //Ok((Box::new(ds_rx) as Box<dyn DataSource<Bytes>>, DataSourceServer { ds_tx } ))
        Ok((Box::new(ds_rx) as Box<dyn DataSource<Bytes>>, jh))
    }
}

#[tonic::async_trait]
impl GrpcDataStore for DataSourceServer {
    // for returning a stream, this is what is needed for a data source
    //type StartStringDataSourceStream = ResponseStream;
    async fn send_text_lines(
        &self,
        req_stream: Request<Streaming<DataOutputStringMessage>>,
    ) -> DataOutputResult<DataOutputResponse> {
        let mut in_stream = req_stream.into_inner();
        let name = self.server_name.clone();
        let ds_tx_clone = self.ds_tx.clone();
        tokio::spawn(async move {
            let ds_tx = &ds_tx_clone;
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(DataOutputStringMessage { content }) => {

                        ds_tx
                            .send(DataSourceMessage::new(&name, Bytes::from(content)))
                            .await
                            .expect("Failure Sending");

                        //drop(ds_tx);

                    }
                    Err(er) => {}
                };
            }
            ()
        })
        .await
        .expect("bad");
        unimplemented!("blah");
    }
}
