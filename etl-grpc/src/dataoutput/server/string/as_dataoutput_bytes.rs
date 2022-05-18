/// Creates a server which accepts DataOutput messages and allows to specify the DataOutput to
/// write to by specifying a function which creates a DataOutput for each stream
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::*;
use etl_core::deps::bytes::Bytes;
use etl_core::joins::CreateDataOutputFn;

use crate::dataoutput::data_output_string_server::DataOutputString as GrpcDataStore;
use crate::dataoutput::{
    DataOutputResult,
    DataOutputResponse, DataOutputStats as GrpcDataOutputStats, DataOutputStringMessage,
};

use tokio_stream::StreamExt;
use tonic::{Code, Request, Response, Status, Streaming};

pub struct DefaultDataOutputServer {
    create_output: CreateDataOutputFn<'static, Bytes>,
}

impl DefaultDataOutputServer {
    pub fn new(create_output: CreateDataOutputFn<'static, Bytes>) -> Self {
        DefaultDataOutputServer { create_output }
    }
}

//type ResponseStream = Pin<Box<dyn Stream<Item = Result<DataOutputResponse, Status>> + Send>>;

#[tonic::async_trait]
impl GrpcDataStore for DefaultDataOutputServer {
    async fn send_text_lines(
        &self,
        req_stream: Request<Streaming<DataOutputStringMessage>>,
    ) -> DataOutputResult<DataOutputResponse> {
        let mut in_stream = req_stream.into_inner();
        let output = (self.create_output)().await.map_err(|e| {
            Status::new(
                Code::Internal,
                format!("Creating a DataOutput failed: {}", e),
            )
        })?;
        let stats = tokio::spawn(async move {
            match DataOutput::start_stream(output) {
                Ok((out_tx, out_jh)) => {
                    let mut lines_written = 0_u64;
                    while let Some(incoming_msg) = in_stream.next().await {
                        match incoming_msg {
                            Ok(DataOutputStringMessage { content }) => {
                                lines_written += 1;
                                out_tx
                                    .send(DataOutputMessage::new(Bytes::from(content)))
                                    .await
                                    .map_err(|e| DataStoreError::FatalIO(e.to_string()))?;
                            }
                            Err(er) => {
                                println!(" ERROR: {}", er.to_string());
                            }
                            //_ => panic!("not handled"),
                        }
                    }
                    drop(out_tx);
                    out_jh.await??;
                    Ok::<_, DataStoreError>(GrpcDataOutputStats {
                        name: String::from("grpc-test"),
                        lines_written,
                    })
                }
                Err(er) => Err(DataStoreError::from(er)),
            }
        })
        .await;
        Ok(Response::new(DataOutputResponse {
            stats: Some(stats.expect("could not join").expect("got error")),
            error: None,
            error_message: None,
        }))
    }
}
