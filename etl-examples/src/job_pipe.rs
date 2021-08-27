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
struct Info {
    resource_type: Option<String>,
    index: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct TestJobState {
    pub key: String,
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
    let job_manager = JobManager::new(JobManagerConfig::default())?;
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::new(
        "test",
        "job_pipe",
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

    jr.run_data_output::<TestSourceData>(
        "stream1",
        Box::new(create_mock_data_source()),
        Box::new(MockJsonDataOutput::default()),
        jm_channel,
    )
    .await?
    .complete()?;

    jm_handle.await?;

    Ok(())
}
