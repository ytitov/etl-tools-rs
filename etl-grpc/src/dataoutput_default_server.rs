use dataoutput::data_output_server::DataOutput;
use dataoutput::{DataOutputResponse, DataOutputStringMessage};

use futures::Stream;
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};

pub mod dataoutput {
    tonic::include_proto!("dataoutput");
}

#[derive(Debug, Default)]
pub struct DefaultDataOutputServer {}

type DataOutputResult<T> = Result<Response<T>, Status>;
//type ResponseStream = Pin<Box<dyn Stream<Item = Result<DataOutputResponse, Status>> + Send>>;

#[tonic::async_trait]
impl DataOutput for DefaultDataOutputServer {
    /*
     * for returning a stream, this is what is needed for a data source
    type StringDataOutputStream = ResponseStream;
    async fn string_data_output(
        &self,
        req_stream: Request<Streaming<DataOutputStringMessage>>,
    ) -> DataOutputResult<Self::StringDataOutputStream> {
        unimplemented!("todo")
    }
    */
    async fn string_data_output(
        &self,
        req_stream: Request<Streaming<DataOutputStringMessage>>,
    ) -> DataOutputResult<DataOutputResponse> {
        unimplemented!("todo")
    }
}
