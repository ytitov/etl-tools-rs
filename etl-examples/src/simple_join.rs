/// This example demonstrates how to do a join on two DataSources.  Please note that this
/// functionality isn't meant to replace more performant methods of doing this.  This example
/// performs a typical "left" join.  It also demonstrates how to control error tolerance and
/// inserts some bad data into both the left and the right datasets.
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
    #[clap(
        short,
        long,
        default_value = "20",
        about = "maximum number of errors to allow"
    )]
    pub max_errors: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct LeftTestData {
    name: Option<String>,
    todo: Vec<String>,
    id: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct RightTestData {
    id: String,
    descr: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestOutputData {
    resource_type: Option<String>,
    index: Option<usize>,
}

fn create_mock_data_source_left() -> MockJsonDataSource {
    MockJsonDataSource {
        lines: vec![
            serde_json::to_string(&LeftTestData {
                name: Some(String::from("Bob")),
                todo: Vec::new(),
                id: String::from("bob1"),
            })
            .unwrap(),
            serde_json::to_string(&LeftTestData {
                name: Some(String::from("Bob")),
                todo: Vec::new(),
                id: String::from("bob2"),
            })
            .unwrap(),
            "1 left malformed json".to_string(),
            "2 left malformed json".to_string(),
            serde_json::to_string(&LeftTestData {
                name: Some(String::from("Angela")),
                todo: vec![String::from("paint the barn")],
                id: String::from("ang23"),
            })
            .unwrap(),
            serde_json::to_string(&LeftTestData {
                name: Some(String::from("Martin")),
                todo: vec![String::from("code something up")],
                id: String::from("mrt1"),
            })
            .unwrap(),
        ],
        ..Default::default()
    }
}

fn create_mock_data_source_right() -> MockJsonDataSource {
    MockJsonDataSource {
        lines: vec![
            serde_json::to_string(&RightTestData {
                id: String::from("bob1"),
                descr: String::from("Things are great"),
            })
            .unwrap(),
            "1 right malformed json".to_string(),
            "2 right malformed json".to_string(),
            "3 right malformed json".to_string(),
            serde_json::to_string(&RightTestData {
                id: String::from("bob10"),
                descr: String::from("should not match"),
            })
            .unwrap(),
            serde_json::to_string(&RightTestData {
                id: String::from("bob99"),
                descr: String::from("should not match either"),
            })
            .unwrap(),
            serde_json::to_string(&RightTestData {
                id: String::from("bob1"),
                descr: String::from("This is another match for bob1"),
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
        max_errors: args.max_errors,
        ..Default::default()
    })?;
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::new(
        "test",
        "simple_pipeline_join",
        jm_channel.clone(),
        JobRunnerConfig {
            ..Default::default()
        },
    );

    use etl_core::joins::LeftJoin;
    let join = LeftJoin::<'_, LeftTestData, RightTestData> {
        create_right_ds: Box::new(|| {
            Box::pin(async {
                Ok(Box::new(create_mock_data_source_right())
                    as Box<dyn DataSource<RightTestData>>)
            })
        }),
        left_ds: Box::new(create_mock_data_source_left()),
        // how many lines to keep in memory
        left_buf_len: 2,
        is_match: Box::new(|left, right| left.id == right.id),
    };

    // write the transformed_ds into a mock json output
    jr.run_stream(
        "test-join",
        Box::new(join)
            as Box<dyn DataSource<(LeftTestData, Option<RightTestData>)> + Send + Sync>,
        Box::new(MockJsonDataOutput::default()),
        jm_channel,
    )
    .await?
    .complete()?;

    jm_handle.await?;

    Ok(())
}
