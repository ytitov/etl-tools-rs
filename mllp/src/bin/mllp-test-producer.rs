//use empi_consumer_hcat::event_payload::HciEvent;
//use empi_core::empi_action::EmpiAction;
//use empi_core::empi_action::IntoEmpiActions;
//use etl_core::transformer::TransformFunc;
use etl_core::datastore::fs::LocalFs;
use etl_core::decoder::json::*;
use etl_core::deps::serde_json::Value as JsonValue;
use etl_core::deps::tokio_stream;
use etl_core::streams::split::*;
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
        //.with_level(LevelFilter::Info)
        .with_level(LevelFilter::Error)
        // specific logging only for etl_core to be info
        //.with_module_level("grpc_simple_store_server", LevelFilter::Info)
        //.with_module_level("etl_grpc::simplestore::observer::server", LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    use etl_core::streams::transformer::*;
    use etl_core::transformer::TransformFunc;
    use mllp::stream::MllpServer;

    use etl_core::deps::serde::{Deserialize, Serialize};
    use etl_core::deps::serde_json;
    use serde_json::Value as JsonValue;

    #[derive(Serialize, Deserialize, Debug, Default)]
    #[serde(crate = "etl_core::deps::serde", rename_all = "camelCase")]
    struct Message {
        //pub ack_message: String,
        //pub ack_code: String,
        //#[serde(rename = "MSH")]
        //pub msh: String,
        //#[serde(rename = "MSH-4")]
        //pub msh_4: JsonValue,
        #[serde(flatten)]
        pub extra: JsonValue,
    }

    use mllp::stream::MllpResponse;

    let r = run_data_stream(
        TransformProducer::new(
            // start up server with a transformer that generates ack messages
            //MllpServer::connect("0.0.0.0:50052", TransformFunc::new(|hl7: Bytes| Ok(hl7))).await?,
            MllpServer::connect(
                "0.0.0.0:50052",
                GrpcBytesTransform::new("http://0.0.0.0:4000"),
                //TransformFunc::new(|hl7: Bytes| Ok(hl7)),
            )
            .await?,
            // this is for the transform producer, which converts bytes to strings, nothing crazy
            TransformFunc::new(|(_, res): (Bytes, MllpResponse)| {
                //let s: String = String::from_utf8_lossy(&hl7).into();
                //Ok(serde_json::from_slice::<JsonValue>(&hl7))
                match res {
                    MllpResponse::Ack(a) => {
                        Ok(serde_json::from_value::<Message>(a.fields).unwrap_or(Message::default()))
                    }
                    _ => {
                        Ok(Message::default())
                    },
                }
            }),
            //etl_core::decoder::json::JsonDecoder::from_bytes_transformer::<Message>(),
        ),
        SplitStreams::new(
            // write the result to file system and decode into byte strings
            etl_core::encoder::json::as_json_bytes_consumer(LocalFs {
                output_name: Some("messages.hl7".into()),
                home: "./tempdata".into(),
                ..Default::default()
            }),
            // collect the errors in a vec... not recommended as they will go into RAM
            //Vec::new(),
            etl_core::encoder::json::as_json_bytes_consumer(LocalFs {
                output_name: Some("errors.hl7".into()),
                home: "./tempdata".into(),
                ..Default::default()
            }),
            TransformFunc::new(|item: Result<Message, BoxDynError>| match item {
                Ok(input) => Ok(SelectStream::Left(input)),
                Err(e) => Ok(SelectStream::Right(e.to_string())),
            }),
        ),
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
