use clap::Clap;
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use fs::*;
use mock::*;
use serde::{Deserialize, Serialize};

#[derive(Clap, Debug)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Args {
    #[clap(short, long, about = "folder where to store state")]
    pub store_state: String,
    //#[clap(short, long, about = "Path to the log file", default_value = "./")]
    //pub log: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestSourceData {
    name: Option<String>,
    todo: Vec<String>,
    id: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestOutputData {
    resource_type: Option<String>,
    index: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct TestJobState {
    pub key: String,
}

pub struct TestTransformer;
#[async_trait]
impl TransformHandler<TestSourceData, TestOutputData> for TestTransformer {
    async fn transform_item(
        &self,
        ji: JobItemInfo,
        item: TestSourceData,
    ) -> anyhow::Result<Option<TransformOutput<TestOutputData>>> {
        use std::time::Duration;
        tokio::time::sleep(Duration::from_secs(1_u64)).await;
        Ok(Some(TransformOutput::Item(TestOutputData {
            resource_type: item.name,
            index: Some(ji.index),
        })))
    }
}

fn create_mock_data_source() -> MockJsonDataSource {
    MockJsonDataSource {
        lines: vec![
            serde_json::to_string(&TestSourceData {
                name: Some(String::from("Bob")),
                todo: Vec::new(),
                id: String::from("bob1"),
            })
            .unwrap(),
            "1 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            "2 this is a malformed json".to_string(),
            serde_json::to_string(&TestSourceData {
                name: Some(String::from("Angela")),
                todo: vec![String::from("paint the barn")],
                id: String::from("ang23"),
            })
            .unwrap(),
            serde_json::to_string(&TestSourceData {
                name: Some(String::from("Martin")),
                todo: vec![String::from("code something up")],
                id: String::from("mrt1"),
            })
            .unwrap(),
        ],
        ..Default::default()
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })?;
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::new(
        "test",
        "simple_pipeline",
        jm_channel.clone(),
        JobRunnerConfig {
            ds: Box::new(LocalFsDataSource {
                read_content: ReadContentOptions::Json,
                write_content: WriteContentOptions::Json,
                home: args.store_state,
                ..Default::default()
            }),
            ..Default::default()
        },
    );

    // transform mock data source using TestTransformer and
    // create a new data source
    let transformed_ds = jr.as_datasource(
        Box::new(create_mock_data_source()),
        Box::new(TestTransformer {}),
    )?;

    // write the transformed_ds into a mock json output
    let jr = jr
        .run_data_output(
            "transformed-ds-1",
            Box::new(transformed_ds) as Box<dyn DataSource<TestOutputData> + Send + Sync>,
            Box::new(MockJsonDataOutput::default()),
            jm_channel.clone(),
        )
        .await?;

    // can process streams sequentually if necessary (when the following stream depends on the
    // previous as an example).  Parallel work should be defined as a separate JobRunner.
    let transformed_ds = jr.as_datasource(
        Box::new(create_mock_data_source()),
        Box::new(TestTransformer {}),
    )?;
    let jr = jr
        .run_data_output(
            "transformed-ds-2",
            Box::new(transformed_ds) as Box<dyn DataSource<TestOutputData> + Send + Sync>,
            Box::new(MockJsonDataOutput::default()),
            jm_channel.clone(),
        )
        .await?;

    // run something afterwards (could be some mysql query)
    use command::SimpleCommand;
    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_01", || {
            Box::pin(async move {
                println!("Running do_stuff_01");
                Ok(())
            })
        }))
        .await?;
    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_02", || {
            Box::pin(async move {
                println!("Running do_stuff_02");
                Ok(())
            })
        }))
        .await?;
    jr.complete()?;
    jm_handle.await?;
    Ok(())
}
