use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use mock::*;
use serde::{Deserialize, Serialize};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_pipeline() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::new(
        "test_simple_pipeline_id",
        "test_simple_pipeline",
        jm_channel.clone(),
        JobRunnerConfig {
            /*
            ds: Box::new(LocalFsDataSource {
                read_content: ReadContentOptions::Json,
                write_content: WriteContentOptions::Json,
                home: args.store_state,
                ..Default::default()
            }),
            */
            ..Default::default()
        },
    );
    // transform mock data source using TestTransformer and
    // create a new data source
    let transformed_ds = jr
        .as_datasource(
            Box::new(create_mock_data_source()),
            Box::new(TestTransformer {}),
        )
        .expect("Error creating transformed_ds");
    let jr = jr
        .run_stream(
            "transformed-ds-1",
            Box::new(transformed_ds) as Box<dyn DataSource<TestOutputData> + Send + Sync>,
            Box::new(MockJsonDataOutput::default()),
            jm_channel.clone(),
        )
        .await
        .expect("Error running run_data_output");
    let job_state = jr.complete().expect("Error completing job");
    let ds_state = job_state.streams.states.get("transformed-ds-1").unwrap();
    let n = ds_state.get_total_lines();
    assert_eq!(n, 5);
    jm_handle
        .await
        .expect("Error awaiting on job manager handle");
}

pub struct TestTransformer;
#[async_trait]
impl TransformHandler<TestSourceData, TestOutputData> for TestTransformer {
    async fn transform_item(
        &self,
        ji: JobItemInfo,
        item: TestSourceData,
    ) -> anyhow::Result<Option<TransformOutput<TestOutputData>>> {
        //use std::time::Duration;
        //tokio::time::sleep(Duration::from_secs(1_u64)).await;
        Ok(Some(TransformOutput::Item(TestOutputData {
            resource_type: item.name,
            index: Some(ji.index),
        })))
    }
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
