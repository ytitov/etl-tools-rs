tonic::include_proto!("dataoutput");
use tonic::{Response, Status};
use tokio::task::JoinHandle;

pub type DataOutputResult<T> = Result<Response<T>, Status>;
pub type ServerJoinHandle = JoinHandle<Result<(), Box<dyn std::error::Error + Send>>>;

pub mod server;
