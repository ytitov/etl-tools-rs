use clap::Clap;
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
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
/// Starting data
struct TestSourceData {
    name: Option<String>,
    todo: Vec<String>,
    id: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// Output data
struct Info {
    resource_type: Option<String>,
    index: Option<usize>,
}

struct TestJob {
    pub target_csv: DataOutputTask<Info>,
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

#[async_trait]
impl StreamHandler<Info> for TestJob {
    async fn shutdown(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        // need to await on the output streams so they can complete
        // their writes, else job will shutdown before that can happen
        data_output_shutdown(jr, self.target_csv).await?;
        Ok(())
    }

    async fn process_item(
        &self,
        jinfo: JobItemInfo,
        _item: Info,
        _: &JobRunner,
    ) -> anyhow::Result<()> {
        let TestJob {
            target_csv: (target_csv_tx, _),
        } = &self;
        target_csv_tx
            .send(DataOutputMessage::new(Info {
                resource_type: Some("what".to_string()),
                index: Some(jinfo.index),
            }))
            .await?;
        Ok(())
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    //let args: Args = Args::parse();
    let job_manager = JobManager::new(JobManagerConfig::default())?;
    let (jm_handle, jm_channel) = job_manager.start();
    let testjob = TestJob {
        target_csv: MockJsonDataOutput::default()
            .start_stream(jm_channel.clone())
            .await?,
    };

    use etl_core::transformer::*;
    let transformer = Transformer {
        input: Box::new(create_mock_data_source()),
        map: |item: TestSourceData| {
            println!("Transform func got: {:?}", &item);
            Ok(Info {
                index: Some(0),
                resource_type: Some(String::from("blah")),
            })
        },
    };

    let jr = JobRunner::new(
        "test",
        "transform",
        jm_channel,
        JobRunnerConfig {
            ..Default::default()
        },
    );

    jr.run(Box::new(transformer), Box::new(testjob))
        .await?
        .complete()?;

    jm_handle.await?;

    Ok(())
}
