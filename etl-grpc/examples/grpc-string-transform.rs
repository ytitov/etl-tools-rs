use etl_core::datastore::mock::MockDataOutput;
use etl_core::datastore::DataSource;
use etl_grpc::transformer::client::*;
use etl_job::job::JobRunner;
use etl_job::job::JobRunnerConfig;
use etl_job::job_manager::*;
use etl_grpc::transformer::GrpcStringTransform;
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
    //let mut client = TransformerClient::connect("http://[::1]:50051").await?;
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    //SimpleLogger::new().with_level(LevelFilter::Info).env().init().unwrap();
    SimpleLogger::new()
        .with_level(LevelFilter::Error)
        // specific logging only for etl_core to be info
        .with_module_level("etl_core", LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();
    let jr = JobRunner::create(
        "id",
        "test_grpc_transformer",
        &jm_handle,
        JobRunnerConfig {
            ..Default::default()
        },
    )
    .await
    .expect("Error creating JobRunner");

    let jr = jr
        .run_transform_stream::<String, String>(
            "transformed-ds-1".into(),
            GrpcStringTransform::new_transformer("http://[::1]:50051").await?,
            Box::new(String::from(MSG)),
            Box::new(MockDataOutput::default()),
        )
        .await
        .expect("Error running run_data_output");

    jr.complete().await?;

    Ok(())
}
