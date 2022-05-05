// example: https://github.com/hyperium/tonic/blob/master/examples/src/streaming/server.rs
use datasource::data_source_server::DataSource;
use datasource::{StartDataSource, DataSourceResponse, DataSourceStringMessage};

use futures::Stream;
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};

pub mod datasource {
    tonic::include_proto!("datasource");
}

#[derive(Debug, Default)]
pub struct DefaultDataSourceServer {}

type DataSourceResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<DataSourceStringMessage, Status>> + Send>>;

#[tonic::async_trait]
impl DataSource for DefaultDataSourceServer {
     // for returning a stream, this is what is needed for a data source
    type StartStringDataSourceStream = ResponseStream;
    async fn start_string_data_source(
        &self,
        req: Request<StartDataSource>,
    ) -> DataSourceResult<Self::StartStringDataSourceStream> {
        unimplemented!("todo")
    }
}
