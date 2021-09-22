use etl_core::datastore::*;
use etl_core::decoder::csv::*;
use etl_core::decoder::json::*;
use etl_core::job::state::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use fs::LocalFs;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct TestCsv {
    index: String,
    words: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_basic_fs_json_decoder() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::new(
        "decoder_fs",
        "decoder_fs_test",
        jm_channel.clone(),
        JobRunnerConfig {
            ..Default::default()
        },
    );
    let job_state = jr
        .run_stream::<TestCsv>(
            "basic json fs",
            JsonDecoder::new(
                // generate a pretty boring csv stream
                Box::new(LocalFs {
                    home: String::from("tests/test_data"),
                    files: vec!["10_lines.ndjson".to_string()],
                }),
            )
            .await,
            Box::new(mock::MockJsonDataOutput::default()),
        )
        .await
        .expect("Failed run_stream")
        .complete()
        .expect("Fail completing");
    if let Some(cmd_status) = job_state.step_history.get("basic json fs") {
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
            assert_eq!(9, *total_lines_scanned);
            assert_eq!(1, *num_errors);
        } else {
            panic!("move to db is not showing as completed");
        }
    } else {
        panic!("expected step name of `move to db` but did not find one");
    }
    jm_handle.await.expect("failure waiting for jm");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_basic_fs_csv_decoder() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::new(
        "decoder_fs",
        "decoder_fs_test",
        jm_channel.clone(),
        JobRunnerConfig {
            ..Default::default()
        },
    );
    use fs::LocalFs;
    let job_state = jr
        .run_stream::<TestCsv>(
            "basic csv fs",
            CsvDecoder::new(
                CsvReadOptions::default(),
                // generate a pretty boring csv stream
                Box::new(LocalFs {
                    home: String::from("tests/test_data"),
                    files: vec!["14_good_lines.csv".to_string()],
                }),
            )
            .await,
            Box::new(mock::MockJsonDataOutput::default()),
        )
        .await
        .expect("Failed run_stream")
        .complete()
        .expect("Fail completing");
    use etl_core::job::state::*;
    if let Some(cmd_status) = job_state.step_history.get("basic csv fs") {
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
            assert_eq!(14, *total_lines_scanned);
            assert_eq!(0, *num_errors);
        } else {
            panic!("move to db is not showing as completed");
        }
    } else {
        panic!("expected step name of `move to db` but did not find one");
    }
    jm_handle.await.expect("failure waiting for jm");
}
