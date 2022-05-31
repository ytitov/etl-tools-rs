use crate::proto::etl_grpc::basetypes::ds_error::{FatalIoError, GrpcDataStoreError};
use crate::proto::etl_grpc::transformers::transform::{
    transformer_client::TransformerClient, TransformPayload, TransformResponse,
};
use etl_core::datastore::error::*;
use etl_core::datastore::*;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Streaming;

/// run the data fed to this into a grpc service, and results are written to the given output
pub struct GrpcTransformerClient<O> {
    //pub input: Box<dyn DataSource<I>>,
    /// final output of the transformation goes here
    pub result_output: Box<dyn DataOutput<O>>,
    //pub p: PhantomData<O>,
}

impl From<TransformResponse> for Result<TransformPayload, GrpcDataStoreError> {
    fn from(tr: TransformResponse) -> Self {
        match tr {
            TransformResponse {
                result: Some(result),
                error: None,
            } => Ok(result),
            TransformResponse {
                result: None,
                error: Some(err),
            } => Err(err),
            other => Err(GrpcDataStoreError {
                fatal_io: Some(FatalIoError {}),
                error: format!("Received a bad TransformResponse: {:?}", other),
                ..Default::default()
            }),
        }
    }
}

fn create_stream(
    mut source_rx: Receiver<TransformPayload>,
) -> (
    JoinHandle<()>,
    Receiver<Result<TransformPayload, GrpcDataStoreError>>,
) {
    use tokio::sync::mpsc::channel;
    use tokio::sync::mpsc::error::TryRecvError;
    let (output_tx, output_rx) = channel(1);
    (
        tokio::spawn(async move {
            // populate with one item to get the stream going
            let (grpc_client_tx, client_rx) = channel(1);
            // TODO: must handle error in case source_rx is closed
            // or waiting is too long
            while let Some(item) = source_rx.recv().await {
                //println!("sending FIRST to grpc server {:?}", &item);
                grpc_client_tx.send(item).await.unwrap();
                break;
            }
            // then connect
            let mut client = TransformerClient::connect("http://[::1]:50051")
                .await
                .unwrap();
            // establish the stream
            let response: tonic::Response<_> = client
                .transform_stream(ReceiverStream::new(client_rx))
                .await
                .unwrap();
            let mut resp_stream = response.into_inner();
            // get the next available item
            loop {
                // always wait for a response first to maintain ordering
                match resp_stream.next().await {
                    None => break,
                    Some(Ok(response)) => {
                        output_tx
                            .send(response.into())
                            .await
                            .expect("couldn't send");
                    }
                    Some(Err(er)) => {
                        /*
                        match er {
                        };
                        */
                        panic!("Got a bad status: {}", er);
                    }
                }
                //tokio::time::sleep(std::time::Duration::from_millis(500 as u64)).await;
                // after getting something, check if we have incoming, then forward to client
                match source_rx.recv().await {
                    None => break,
                    Some(item) => {
                        // since we got one item, go ahead and send the next
                        grpc_client_tx.send(item).await.unwrap();
                    }
                }
            }
            ()
        }),
        output_rx,
    )
}

impl DataOutput<String> for GrpcTransformerClient<String> {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<String>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let source_name = String::from("GrpcTransformDataSource");
        let (tx, mut rx) = channel(1);
        let jh = tokio::spawn(async move {
            let mut lines_written = 0_usize;
            let (client_tx, client_rx): (Sender<TransformPayload>, _) = channel(1);
            let (transform_jh, mut transformed_rx) = create_stream(client_rx);
            let (result_output_tx, result_output_jh) = self.result_output.start_stream()?;
            loop {
                match rx.recv().await {
                    Some(DataOutputMessage::Data(item)) => {
                        let item: String = item;
                        //println!("--> GrpcTransformerClient got {}", &item);
                        client_tx
                            .send(TransformPayload {
                                string_content: Some(item),
                                ..Default::default()
                            })
                            .await
                            .unwrap();
                        // to preserve order, need to wait for result before sending more
                        while let Some(res) = transformed_rx.recv().await {
                            //println!(" <- GrpcTransformerClient Got Result: {:?}", &res);
                            match res {
                                Ok(TransformPayload {
                                    string_content: Some(content),
                                    ..
                                }) => result_output_tx.send(DataOutputMessage::new(content)).await?,
                                Err(grpc_ds_err) => return Err(grpc_ds_err.into()),
                                _ => return Err(DataStoreError::FatalIO("Got a non string response from the GRPC server which is not supported by this DataOutput<String>".into())),
                            };
                            break;
                        }
                        lines_written += 1;
                    }
                    Some(DataOutputMessage::NoMoreData) => {
                        break;
                    }
                    None => {
                        drop(client_tx);
                        break;
                    }
                }
            }
            transform_jh.await?;
            result_output_jh.await??;
            Ok(DataOutputDetails::Basic {
                lines_written,
            })
        });
        Ok((tx, jh))
    }
}
