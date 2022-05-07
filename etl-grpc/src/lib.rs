pub mod datastore {
    use tonic::{Response, Status};
    use tokio::task::JoinHandle;

    pub type DataOutputResult<T> = Result<Response<T>, Status>;
    pub type DataSourceResult<T> = Result<Response<T>, Status>;
    pub type ServerJoinHandle = JoinHandle<Result<(), Box<dyn std::error::Error + Send>>>;
    tonic::include_proto!("datastore");
}

pub mod dataoutput_default_server;
pub mod datasource_server;

pub mod log_util {
    use simple_logger::SimpleLogger;
    use log::LevelFilter;

    pub fn new_info() {
        SimpleLogger::new().with_level(LevelFilter::Info).env().init().unwrap();
    }
    pub fn new_debug() {
        SimpleLogger::new().with_level(LevelFilter::Debug).env().init().unwrap();
    }
}

