use etl_core::datastore::*;
use etl_core::preamble::*;
use etl_core::job::stream::*;
use mock::*;
use serde::{Deserialize, Serialize};

/// create the following tests
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn run_two_jobs_fail_one() {
    let job_manager = JobManager::new(JobManagerConfig {
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();

    let jr_ok = create_jr(jm_channel.clone(), 100).await;
    let jr_ok_job_state = jr_ok
        .run_stream::<TestSourceData>(
            "transformed-ds-1a",
            Box::new(create_mock_data_source()),
            Box::new(MockJsonDataOutput::default()),
        )
        .await
        .expect("Should not have failed")
        .complete()
        .await
        .expect("Should not have failed");

    let jr_not_ok = create_jr(jm_channel.clone(), 1).await;
    let jr_not_ok_err = jr_not_ok
        .run_stream::<TestSourceData>(
            "transformed-ds-1b",
            Box::new(create_mock_data_source()),
            Box::new(MockJsonDataOutput::default()),
        )
        .await
        .unwrap_err();
    assert_eq!(JobRunnerError::TooManyErrors, jr_not_ok_err);

    use etl_core::job::state::*;
    if let Some(cmd_status) = jr_ok_job_state.step_history.get("transformed-ds-1a") {
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
    jm_handle.await.expect("Fatal: error awaiting on jm handle");
}

async fn create_jr(jm_channel: JobManagerChannel, max_errors: usize) -> JobRunner {
    JobRunner::create(
        "test_simple_pipeline_id",
        format!("test_simple_pipeline_{}", max_errors),
        jm_channel.clone(),
        JobRunnerConfig {
            max_errors,
            ..Default::default()
        },
    ).await.expect("Error creating JobRunner")
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestSourceData {
    name: Option<String>,
    todo: Vec<String>,
    id: String,
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
