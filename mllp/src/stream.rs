use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::*;
use etl_core::deps::bytes::{BufMut, Bytes, BytesMut};
use etl_core::deps::log;
use etl_core::deps::serde::Deserialize;
use etl_core::deps::serde_json;
use etl_core::deps::tokio;
use etl_core::deps::tokio::net::{lookup_host, TcpListener, TcpStream, ToSocketAddrs};
use etl_core::deps::tokio::sync::mpsc::Sender;
use etl_core::deps::tokio_stream;
use etl_core::streams::*;
use etl_core::transformer::TransformerBuilder;
use etl_core::transformer::TransformerFut;
use futures::SinkExt;
use hl7_mllp_codec::MllpCodec;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use etl_core::deps::serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
#[serde(crate = "etl_core::deps::serde", rename_all = "camelCase")]
pub struct AckResponse {
    pub code: String,
    pub msg: String,
    #[serde(flatten)]
    //pub fields: HashMap<String, JsonValue>,
    pub fields: JsonValue,
}

#[derive(Deserialize, Debug)]
#[serde(crate = "etl_core::deps::serde",untagged)]
pub enum MllpResponse {
    WithJson {
        ack: AckResponse,
        json_string: String,
    },
    Ack(AckResponse),
    Raw(Bytes),
}

impl MllpResponse {
    pub fn get_ack_bytes(&self) -> Bytes {
        match self {
            MllpResponse::WithJson { ack, .. } => Bytes::from(ack.msg.clone()),
            MllpResponse::Ack(ack) => Bytes::from(ack.msg.clone()),
            MllpResponse::Raw(b) => b.clone(),
        }
    }
}

pub struct MllpServer {
    server_addr: SocketAddr,
    //forward_tx: Sender<Bytes>,
    transformer: Box<dyn TransformerBuilder<'static, Bytes, Bytes>>,
}

impl MllpServer {
    pub async fn connect<I: ToSocketAddrs, TR>(
        addr_str: I,
        transformer: TR,
    ) -> Result<Self, Box<dyn Error>>
    where
        TR: TransformerBuilder<'static, Bytes, Bytes>,
    {
        let mut addrs_iter = lookup_host(addr_str).await?;
        let server_addr = addrs_iter.next().ok_or(DataStoreError::FatalIO(
            "Could not connect to the address specified".into(),
        ))?;
        log::info!("MllpServer starting on: {}", &server_addr);
        Ok(MllpServer {
            server_addr,
            transformer: Box::new(transformer),
        })
    }
}

#[allow(unreachable_code)]
impl<'dp> Producer<'dp, (Bytes, MllpResponse)> for MllpServer {
    fn start_producer(
        self: Box<Self>,
        tx: Sender<(Bytes, MllpResponse)>,
    ) -> ProducerResultFut<'dp, DataSourceDetails> {
        let transformer = self.transformer;
        let server_addr = self.server_addr;
        Box::pin(async move {
            let server_jh: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> =
                tokio::spawn(async move {
                    let forward_tx = tx;
                    let listener = TcpListener::bind(&server_addr).await?;
                    loop {
                        let (stream, _) = listener.accept().await?;
                        let forward_tx = forward_tx.clone();
                        let transformer = transformer.build();
                        tokio::spawn(async move {
                            println!("Connection opened...");
                            if let Err(e) = process(
                                stream,
                                transformer as Box<dyn TransformerFut<Bytes, Bytes>>,
                                forward_tx,
                            )
                            .await
                            {
                                println!("Failed to process connection; error = {}", e);
                            }
                        });
                    }
                    Ok(())
                });
            server_jh.await??;
            Ok(DataSourceDetails::Empty)
        })
    }
}

async fn process(
    stream: TcpStream,
    mut t: Box<dyn TransformerFut<Bytes, Bytes>>,
    tx: Sender<(Bytes, MllpResponse)>,
) -> Result<(), Box<dyn Error>>
where
        //TR: TransformerFut<'static, Bytes, Bytes>,
{
    let mut transport = Framed::new(stream, MllpCodec::new());

    while let Some(result) = transport.next().await {
        match result {
            Ok(_message) => {
                //msh::extract_msg_info(&_message);
                //use std::io::{self, BufRead};
                //let full_message = String::from_utf8_lossy(&_message);
                let basic_ack = "MSH|^~\\&|Main_HIS|XYZ_HOSPITAL|iFW|ABC_LAB|20160915003015||ACK|9B38584D|P|2.6.1|MSA|AA|9B38584D|All GOOD|";
                //println!("Got message: {:?}", _message);
                //print!("*");

                //let ack_msg = BytesMut::from("\x06"); //<ACK> ascii char, simple ack
                //let nack_msg = BytesMut::from("\x15"); //<ACK> ascii char, simple ack

                let bytes_incoming = Bytes::from(_message);
                match t.transform(bytes_incoming.clone()).await {
                    Ok(reply_msg) => {
                        log::info!("From transform: {:?}", &reply_msg);

                        let (ack_msg, mllp_res) =
                            match serde_json::from_slice::<MllpResponse>(&reply_msg) {
                                Ok(res) => {
                                    let mut buf = BytesMut::with_capacity(reply_msg.len());
                                    buf.put(res.get_ack_bytes());
                                    (buf, res)
                                }
                                _ => {
                                    // if we can't deserialize, assuming the whole
                                    // thing is an ack
                                    let mut buf = BytesMut::with_capacity(reply_msg.len());
                                    buf.put(reply_msg.clone());
                                    (buf, MllpResponse::Raw(reply_msg))
                                }
                            };
                        // reply with ack
                        transport.send(ack_msg).await?;
                        // forward the actual message downstream
                        match tx.send((bytes_incoming, mllp_res)).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("FATAL, downstream consumer must be down: {}; should reply with NACK here",e);
                            }
                        };
                    }
                    Err(e) => {
                        log::error!("Got error, should be replying with NACK message: {}", e);
                        let ack_msg = BytesMut::from(basic_ack); //<ACK> ascii char, simple ack
                        transport.send(ack_msg).await?;
                    }
                };
                //println!("sent ack");
                //println!("  ACK sent...");
            }
            Err(e) => {
                println!("Error from MLLP transport: {:?}", e);
                return Err(e.into());
            }
        }
    }
    println!("Connection closed...");
    Ok(())
}
