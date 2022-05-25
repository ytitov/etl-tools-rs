/// implements the grpc client which talks to a grpc server which implements the
/// Transformer service proto rpc
pub mod client;
//use crate::proto::etl_grpc::basetypes::ds_error::GrpcDataStoreError;
use crate::proto::etl_grpc::transformers::transform::{
    transformer_client::TransformerClient, TransformPayload, TransformResponse,
};
use etl_core::datastore::error::DataStoreError;
//use etl_core::datastore::BoxedDataSource;
//use etl_core::datastore::DataOutputStats;
use etl_core::datastore::*;
use etl_core::deps::serde::{Deserialize, Serialize};
use etl_core::deps::serde_json;
use std::fmt::Debug;
use tonic::transport::Channel;

//use tokio::sync::oneshot::{Receiver as OneShotRx, Sender as OneShotTx};

use etl_core::transformer::Transformer;
pub struct GrpcStringTransform {
    pub grpc_client: TransformerClient<Channel>,
}

use std::future::Future;
impl<'a> GrpcStringTransform {
    pub fn new_transformer(
        url: &str,
    ) -> impl Future<Output = Result<Box<dyn Transformer<'a, String, String>>, DataStoreError>> + 'a
    {
        let url = url.to_string();
        async move {
            Ok(Box::new(GrpcStringTransform {
                grpc_client: TransformerClient::connect(url.clone()).await.map_err(|e| {
                    DataStoreError::transport(
                        format!("Could not connect to GrpcServer at {}", url),
                        e,
                    )
                })?,
            }) as Box<dyn Transformer<'a, String, String>>)
        }
    }
}

use etl_core::deps::async_trait;
#[async_trait]
impl<'a> Transformer<'a, String, String> for GrpcStringTransform {
    async fn transform(&mut self, s: String) -> Result<String, DataStoreError> {
        let request = tonic::Request::new(TransformPayload {
            string_content: Some(s),
            ..Default::default()
        });
        match self.grpc_client.transform(request).await {
            Ok(res) => match res.into_inner() {
                TransformResponse {
                    result:
                        Some(TransformPayload {
                            string_content: str_cont,
                            bytes_content: b_cont,
                            json_string_content: json_cont,
                        }),
                    ..
                } => match (str_cont, b_cont, json_cont) {
                    (Some(str_cont), None, None) => Ok(str_cont),
                    (None, Some(b_cont), None) => Ok(String::from_utf8_lossy(&b_cont).to_string()),
                    (None, None, Some(json_cont)) => Err(DataStoreError::FatalIO(format!(
                        "Expected a normal string but got back Json String: {}",
                        json_cont
                    ))),
                    _ => panic!("Got a completely empty from server"),
                },
                TransformResponse {
                    result: None,
                    error: Some(grpc_ds_err),
                } => Err(DataStoreError::FatalIO(
                    "Could not reply to datasource".into(),
                )),
                _other => Err(DataStoreError::FatalIO(
                    "Could not reply to datasource".into(),
                )),
            },
            Err(status) => {
                panic!("error status is not handled")
            }
        }
    }
}

/// Creates a grpc client which maps input DataSource<(I, CallbackTx<O>)>
/// and acts like a DataSource<O>.  Internally
/// it is calling an external grpc server which does the mapping.  The stream maintains ordering,
/// therefor adds some small amount of network latency in the process.
/// The issue is, this doesn't follow the usual pattern, so TODO: is
/// create a transformer which accepts a DataSource<I> and creates a DataSource<O>.  Finally create
/// a Transformer<I,O> trait similar to how SimpleStore functions
pub struct GrpcTransformerClient<I, O> {
    pub grpc_client: TransformerClient<Channel>,
    pub source: Box<dyn DataSource<(I, CallbackTx<O>)>>,
}

// should add serde traits to this
impl<I, O> DataSource<O> for GrpcTransformerClient<I, O>
where
    I: 'static + Debug + Send + Serialize,
    for<'de> O: 'static + Debug + Send + Deserialize<'de>,
{
    fn name(&self) -> String {
        "GrpcTransformerClient".to_string()
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc;
        let (output_tx, output_rx): (_, _) = mpsc::channel(1);
        let (mut source_rx, source_jh) = self.source.start_stream()?;
        let mut client = self.grpc_client;
        let task_jh = tokio::spawn(async move {
            let lines_scanned = 0_usize;
            loop {
                match source_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data {
                        source,
                        content: (data, reply_tx),
                    })) => {
                        let json_str = serde_json::to_string(&data)
                            .map_err(|e| DataStoreError::FatalIO(e.to_string()))?;
                        let request = tonic::Request::new(TransformPayload {
                            string_content: Some(json_str),
                            ..Default::default()
                        });
                        match client.transform(request).await {
                            Ok(res) => {
                                match res.into_inner() {
                                    TransformResponse {
                                        result:
                                            Some(TransformPayload {
                                                string_content: str_cont,
                                                bytes_content: b_cont,
                                                json_string_content: json_cont,
                                            }),
                                        ..
                                    } => {
                                        let p = match (str_cont, b_cont, json_cont) {
                                            (Some(str_cont), None, None) => {
                                                serde_json::from_str(&str_cont)
                                            }
                                            (None, Some(b_cont), None) => {
                                                serde_json::from_slice(&b_cont)
                                            }
                                            (None, None, Some(json_cont)) => {
                                                serde_json::from_str(&json_cont)
                                            }
                                            _ => panic!("Got a completely empty from server"),
                                        };
                                        match p {
                                            Ok::<O, _>(payload) => {
                                                reply_tx.send(Ok(payload)).map_err(|_| {
                                                    DataStoreError::FatalIO(
                                                        "Could not reply to datasource".into(),
                                                    )
                                                })?;
                                            }
                                            Err(er) => {
                                                reply_tx
                                                    .send(Err(DataStoreError::Deserialize {
                                                        message: er.to_string(),
                                                        attempted_string: "".into(),
                                                    }))
                                                    .map_err(|_| {
                                                        DataStoreError::FatalIO(
                                                            "Could not reply to datasource".into(),
                                                        )
                                                    })?;
                                            }
                                        };
                                    }
                                    TransformResponse {
                                        result: None,
                                        error: Some(grpc_ds_err),
                                    } => {
                                        reply_tx.send(Err(grpc_ds_err.into())).map_err(|_| {
                                            DataStoreError::FatalIO(
                                                "Could not reply to datasource".into(),
                                            )
                                        })?;
                                    }
                                    _other => {
                                        reply_tx
                                            .send(Err("Got a response that I can't handle".into()))
                                            .map_err(|_| {
                                                DataStoreError::FatalIO(
                                                    "Could not reply to datasource".into(),
                                                )
                                            })?;
                                    }
                                };
                            }
                            Err(status) => {
                                // TODO
                                /*
                                use tonic::Status;
                                let Status {
                                    code, message, details, metadata
                                } = status;
                                */
                            }
                        };
                    }
                    Some(Err(err)) => {}
                    None => {
                        break;
                    }
                }
            }
            source_jh.await??;
            Ok::<_, DataStoreError>(DataSourceStats { lines_scanned })
        });
        Ok((output_rx, task_jh))
    }
}

/*
impl<DS, I: Debug + 'static + Send, O: Debug + 'static + Send> DataSource<O>
    for GrpcTransformerClient
where
    DS: Transformable<I, O>,
    I: 'static + Send + Debug,
    O: 'static + Sync + Send + Debug,
{
}
*/

// this all seems overkill
// all we really need here is DataSource<(I, TransformerResultTx<O>)>
/*
impl<I: Debug + 'static + Send, O: Debug + 'static + Send> TransformSource<I, O>
    for GrpcTransformerClient
where
    I: 'static + Send + Debug,
    O: 'static + Sync + Send + Debug,
{
    /// for each I it receives replies back with the result O on the one shot channel
    fn transform_source(
        self: Box<Self>,
        //input: BoxedDataSource<(I, TransformerResultTx<O>)>,
    ) -> Result<DataSourceTask<O>, DataStoreError> {
        unimplemented!();
        /*
        use tokio::sync::mpsc::channel;
        let req_ch: TransformerItemRequestChannel<I> = channel(1);
        let (out_tx, out_rx): (_, TransformerResultRx<O>) = channel(1);
        let jh = tokio::spawn(async move {
            let lines_written = 0_usize;
            Ok::<_, DataStoreError>(DataOutputStats {
                lines_written,
                name: String::from("GrpcTransformerClient"),
            })
        });
        Ok((out_rx, jh))
        */
        //unimplemented!();
    }
}
*/

/*
use etl_core::datastore::DataSource;
impl<I: Debug + 'static + Send, O: Debug + 'static + Send> DataSource<(I, TransformerResultTx<O>)>
    for GrpcTransformerClient
{
}
*/
