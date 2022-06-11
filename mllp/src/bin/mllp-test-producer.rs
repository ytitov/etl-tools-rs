//use empi_consumer_hcat::event_payload::HciEvent;
//use empi_core::empi_action::EmpiAction;
//use empi_core::empi_action::IntoEmpiActions;
//use etl_core::transformer::TransformFunc;
use etl_core::datastore::fs::LocalFs;
use etl_core::decoder::json::*;
use etl_core::deps::serde_json::Value as JsonValue;
use etl_core::deps::tokio_stream;
use etl_core::streams::*;
use etl_core::{
    deps::bytes::Bytes,
    //datastore::{fs::LocalFs, mock::MockDataOutput},
    //decoder::json::JsonDecoder,
    deps::log,
    deps::tokio,
};
use etl_grpc::transformer::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        //.with_level(LevelFilter::Info)
        // specific logging only for etl_core to be info
        //.with_module_level("grpc_simple_store_server", LevelFilter::Info)
        //.with_module_level("etl_grpc::simplestore::observer::server", LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    use etl_core::streams::transformer::*;
    use etl_core::transformer::TransformFunc;
    use mllp::stream::MllpServer;

    let r = run_data_stream(
        TransformProducer::new(
            // start up server with a transformer that generates ack messages
            //MllpServer::connect("0.0.0.0:50052", TransformFunc::new(|hl7: Bytes| Ok(hl7))).await?,
            MllpServer::connect(
                "0.0.0.0:50052",
                GrpcBytesTransform::new("http://0.0.0.0:50051"),
            )
            .await?,
            // this is for the transform producer, which converts bytes to strings, nothing crazy
            TransformFunc::new(|hl7: Bytes| {
                let s: String = String::from_utf8_lossy(&hl7).into();
                Ok(s)
            }),
        ),
        Vec::new(),
    )
    .await;

    match r.await?.unwrap() {
        ConsumerResult::Details(_details) => {}
        ConsumerResult::WithData { data: _, details } => {
            println!("Got details: {:?}", details);
        }
    };

    Ok(())
}
