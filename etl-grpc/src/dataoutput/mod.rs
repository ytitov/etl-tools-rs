tonic::include_proto!("dataoutput");
pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("dataoutput_descriptor");
use tokio::task::JoinHandle;
use tonic::{Response, Status};

pub type DataOutputResult<T> = Result<Response<T>, Status>;
pub type ServerJoinHandle = JoinHandle<Result<(), Box<dyn std::error::Error + Send>>>;

pub mod server;
