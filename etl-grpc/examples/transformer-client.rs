use etl_core::datastore::mock::MockDataOutput;
use etl_core::datastore::DataSource;
use etl_grpc::transformer::client::*;
use etl_job::job::JobRunner;
use etl_job::job::JobRunnerConfig;
use etl_job::job_manager::*;
//use etl_core::transformer::Transformer;

static MSG: &str = r#"hello
my
name
is
mr bananas
1
2
3
rocks

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

    let output = GrpcTransformerClient::<String> {
        result_output: Box::new(MockDataOutput::default()),
    };
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();
    let jr = JobRunner::create(
        "test_simple_pipeline_id",
        "test_simple_pipeline",
        &jm_handle,
        JobRunnerConfig {
            ..Default::default()
        },
    )
    .await
    .expect("Error creating JobRunner");

    let jr = jr
        .run_stream::<String>(
            "transformed-ds-1",
            Box::new(String::from(MSG)),
            Box::new(output),
        )
        .await
        .expect("Error running run_data_output");

    jr.complete().await?;
    /*
    let request = tonic::Request::new(TransformPayload {
        string_content: Some("Tonic".into()),
        ..Default::default()
    });
    */

    //let g = GrpcTransformerClient { grpc_client: client };

    //let t_ds = Box<dyn DataSource<(I, TransformerResultTx<O>)>>::from_datasource(Box::new(String::from("lots a lines") as Box<dyn DataSource<String>>));
    //Box::new(g).create();

    /*
    let response = client.transform(request).await?;

    println!("RESPONSE={:?}", response);
    */

    Ok(())
}
