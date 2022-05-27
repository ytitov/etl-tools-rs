use etl_core::datastore::mock::MockDataOutput;
use etl_job::job::JobRunner;
use etl_job::job::JobRunnerConfig;
use etl_job::job_manager::*;
use etl_grpc::simplestore::client::GrpcSimpleStoreClient;
//use etl_core::transformer::Transformer;

static MSG: &str = r#"
grpc hello
grpc my
grpc name
grpc is
grpc mr bananas
grpc 1
grpc 2
grpc 3
grpc rocks

previous string is empty"#;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    SimpleLogger::new()
        .with_level(LevelFilter::Error)
        // specific logging only for etl_core to be info
        .with_module_level("test_grpc_simple_store_job", LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        state_storage: Box::new(GrpcSimpleStoreClient::connect("http://[::]:50051").await?),
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();
    let jr = JobRunner::create(
        "some-instance-id",
        "test_grpc_transformer",
        &jm_handle,
        JobRunnerConfig {
            ..Default::default()
        },
    )
    .await
    .expect("Error creating JobRunner");

    let jr = jr
        .run_stream::<String>(
            "some-test-strings".into(),
            Box::new(String::from(MSG)),
            Box::new(MockDataOutput::default()),
        )
        .await
        .expect("Error running run_data_output");

    jr.complete().await?;

    Ok(())
}
