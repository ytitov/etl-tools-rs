use etl_core::datastore::mock::MockJsonDataOutput;
use etl_core::datastore::DataOutput;
use etl_core::deps::bytes::Bytes;
use etl_grpc::dataoutput::data_output_server::DataOutputServer;
use etl_grpc::dataoutput_default_server::DefaultDataOutputServer;
use std::{net::ToSocketAddrs};
use tonic::{transport::Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = DefaultDataOutputServer::new(Box::new(|| {
        Box::pin(async {
            Ok(Box::new(MockJsonDataOutput::default()) as Box<dyn DataOutput<Bytes>>)
        })
    }));
    Server::builder()
        .add_service(DataOutputServer::new(server))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}
