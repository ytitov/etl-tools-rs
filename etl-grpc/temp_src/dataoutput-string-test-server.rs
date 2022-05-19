use etl_core::datastore::mock::MockJsonDataOutput;
use etl_core::datastore::DataOutput;
use etl_core::deps::bytes::Bytes;
use etl_grpc::dataoutput::data_output_string_server::DataOutputStringServer;
use etl_grpc::dataoutput::server::string::as_dataoutput_bytes::DefaultDataOutputServer;
use std::net::ToSocketAddrs;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    etl_grpc::log_util::new_info();

    let server = DefaultDataOutputServer::new(Box::new(|| {
        Box::pin(async {
            Ok(Box::new(MockJsonDataOutput::default()) as Box<dyn DataOutput<Bytes>>)
        })
    }));
    Server::builder()
        .add_service(DataOutputStringServer::new(server))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}
