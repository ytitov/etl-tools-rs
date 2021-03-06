use etl_job::job::*;
use etl_job::job::error::*;
use etl_job::job_manager::*;
use etl_core::deps::anyhow;
use etl_core::deps::tokio;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
/// Runs three commands, with stop_on_error set to false and checks that the second command results
/// with an error
async fn test_job_command_with_error() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();

    let jr = JobRunner::create(
        "test",
        "test_bad_command",
        &jm_handle,
        JobRunnerConfig {
            stop_on_error: false,
            ..Default::default()
        },
    ).await.expect("Error creating JobRunner");
    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_and_not_fail_1", |_| {
            Box::pin(async move { Ok(()) })
        }))
        .await
        .expect("Failed run_cmd");

    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_and_fail_2", |_| {
            Box::pin(async move { Err(anyhow::anyhow!("Sorry I failed :(")) })
        }))
        .await
        .expect("Failed run_cmd");

    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_and_not_fail_3", |_| {
            Box::pin(async move { Ok(()) })
        }))
        .await
        .expect("Failed run_cmd");
    let job_state = jr.complete().await.expect("Error completing job");
    jm_handle.shutdown().await.expect("Failed waiting on handle");
    use command::*;
    use state::*;
    // Make sure that the second command resulted in an error and verify the step index
    if let Some(cmd_status) = job_state.step_history.get("do_stuff_and_fail_2") {
        if let JobStepDetails {
            step: JobStepStatus::Command(StepCommandStatus::Error { .. }),
            step_index,
            ..
        } = cmd_status
        {
            // expect to be an error
            assert_eq!(1, *step_index);
        } else {
            assert!(false);
        }
    } else {
        // did not find, must fail
        assert!(false);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
/// Runs three commands, with stop_on_error set to true and checks that the second command results
/// with an error, also ensures that the step history does not contain the 3rd command
async fn test_job_command_with_error_2() {
    use command::*;
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();

    let jr = JobRunner::create(
        "test",
        "test_bad_command",
        &jm_handle,
        JobRunnerConfig {
            stop_on_error: true,
            ..Default::default()
        },
    ).await.expect("Error creating JobRunner");
    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_and_not_fail_1", |_| {
            Box::pin(async move { Ok(()) })
        }))
        .await
        .expect("Failed run_cmd");

    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_and_fail_2", |_| {
            Box::pin(async move { Err(anyhow::anyhow!("Sorry I failed :(")) })
        }))
        .await
        .expect("Failed run_cmd");

    let jr = jr
        .run_cmd(SimpleCommand::new("do_stuff_and_not_fail_3", |_| {
            Box::pin(async move { Ok(()) })
        }))
        .await
        .unwrap_err();
    if let JobRunnerError::JobStepError { .. } = jr {
    } else {
        panic!("Should have gotten a JobRunnerError::JobStepError due to stop_on_error: true option");
    }
    jm_handle.shutdown().await.expect("Failed waiting on handle");
}
