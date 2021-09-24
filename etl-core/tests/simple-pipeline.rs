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
        .run_stream::<TestOutputData>(
            "transformed-ds-1",
            //Box::new(transformed_ds) as Box<dyn DataSource<TestOutputData>>,
            Box::new(transformed_ds),
            Box::new(MockJsonDataOutput::default()),
        )
        .await
        .expect("Error running run_data_output");
    let job_state = jr.complete().await.expect("Error completing job");
    use state::*;
    jm_handle
        .await
        .expect("Error awaiting on job manager handle");
    if let Some(cmd_status) = job_state.step_history.get("transformed-ds-1") {
        if let JobStepDetails {
            step:
                JobStepStatus::Stream(StepStreamStatus::Complete {
                    total_lines_scanned,
                    num_errors,
                    ..
                }),
            step_index,
            ..
        } = cmd_status
        {
            assert_eq!(0, *step_index);
            assert_eq!(3, *total_lines_scanned);
            assert_eq!(2, *num_errors);
        } else {
            panic!("transformed-ds-1 is not showing as completed");
        }
    } else {
        panic!("Expected a step with name transformed-ds-1");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_pipeline_max_error_with_failure() {
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
            max_errors: 2, // do a hard fail at error 2
            ..Default::default()
        },
    );
    // transform mock data source using TestTransformer and
    // create a new data source
    let transformed_ds = jr
        .as_datasource(
            Box::new(create_mock_data_source_many_errors()),
            Box::new(TestTransformer {}),
        )
        .expect("Error creating transformed_ds");
    let jr = jr
        .run_stream(
            "transformed-ds-1",
            Box::new(transformed_ds) as Box<dyn DataSource<TestOutputData>>,
            Box::new(MockJsonDataOutput::default()),
        )
        .await
        .unwrap_err();
    assert_eq!(JobRunnerError::TooManyErrors, jr);
    //let job_result = jr.complete().expect("Error completing job");
    //use state::*;
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

fn create_mock_data_source_many_errors() -> MockJsonDataSource {
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
