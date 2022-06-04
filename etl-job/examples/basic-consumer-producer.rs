use etl_core::{
    //datastore::{fs::LocalFs, mock::MockDataOutput},
    //decoder::json::JsonDecoder,
    deps::log,
    deps::tokio,
    task::transform_stream::*,
};
use etl_job::job::JobRunnerConfig;
use etl_job::job_manager::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        //.with_level(LevelFilter::Info)
        // specific logging only for etl_core to be info
        //.with_module_level("grpc_simple_store_server", LevelFilter::Info)
        //.with_module_level("etl_grpc::simplestore::observer::server", LevelFilter::Info)
        .env()
        .init()
        .unwrap();
    let mut job_manager = JobManagerBuilder::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager")
    .into_job_manager();

    let input = r#"hi
    these
    are a bunch of 
    lines"#
        .to_string();
    let t = TransformStream::new(input, Vec::new(), |s| {
        //Ok(99)
        Ok(format!("adding: {}", s))
    });
    job_manager.run_task(t)?;
    job_manager.start().await?;
    Ok(())
}
