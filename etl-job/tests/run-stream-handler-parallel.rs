use etl_core::datastore::*;
use etl_job::job::stream::*;
use etl_job::job_manager::*;
use etl_job::job::*;
use etl_job::job::error::*;
use etl_job::job::state::*;
use mock::*;
use etl_core::deps::serde::{Deserialize, Serialize};
use etl_core::deps::tokio;
use etl_core::deps::*;

/// This tests running two jobs in parallel.  I have not come up with a good use case for this
/// because to run things in parallel you can simply use the DataOutputTask trait.  Leaving this
/// for now, but if it is not used sometime soon, should plan to take this idea out
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn run_two_jobs_fail_one() {
    let job_manager = JobManager::new(JobManagerConfig {
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();

    // very important to initialize jobs before calling complete
    // otherwise the JobManager will exit before the second job can start
    let jr_ok = create_jr(&jm_handle, 100).await;
    let jr_not_ok = create_jr(&jm_handle, 1).await;
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

    let jr_not_ok_err = jr_not_ok
        .run_stream::<TestSourceData>(
            "transformed-ds-1b",
            Box::new(create_mock_data_source()),
            Box::new(MockJsonDataOutput::default()),
        )
        .await
        .unwrap_err();
    assert_eq!(JobRunnerError::TooManyErrors, jr_not_ok_err);

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
    jm_handle.shutdown().await.expect("Fatal: error awaiting on jm handle");
}

async fn create_jr(jm_handle: &JobManagerHandle, max_errors: usize) -> JobRunner {
    JobRunner::create(
        "test_simple_pipeline_id",
        format!("test_simple_pipeline_{}", max_errors),
        &jm_handle,
        JobRunnerConfig {
            max_errors,
            ..Default::default()
        },
    ).await.expect("Error creating JobRunner")
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(crate = "serde", rename_all = "camelCase")]
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
