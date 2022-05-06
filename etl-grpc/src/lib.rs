pub mod dataoutput {
    tonic::include_proto!("dataoutput");
}

pub mod dataoutput_default_server;
pub mod datasource_default_server;

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

