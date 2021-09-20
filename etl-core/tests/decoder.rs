use enumerate::EnumerateStreamAsync;
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job::state::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct TestCsv {
    index: String,
    words: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_basic_csv_decoder() {
    use etl_core::decoder::csv::*;
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
    jr.run_stream::<TestCsv>(
        "basic csv",
        CsvDecoder::new(
            CsvReadOptions::default(),
            // generate a pretty boring csv stream
            Box::new(EnumerateStreamAsync::with_max(
                "create some byte lines",
                10,
                (),
                |_, idx| {
                    Box::pin(async move {
                        if idx == 0 {
                            Ok(String::from("index,words"))
                        } else {
                            if idx == 7 {
                                Ok(format!("{},stuff,\"should error\"", idx))
                            } else {
                                Ok(format!("{},stuff", idx))
                            }
                        }
                    })
                },
            )),
        )
        .await,
        Box::new(mock::MockJsonDataOutput::default()),
    )
    .await
    .expect("Failed run_stream")
    .complete()
    .expect("Fail completing");
    jm_handle.await.expect("failure waiting for jm");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_basic_json_decoder() {
    use etl_core::decoder::json::*;
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
    let job_state = jr.run_stream::<TestCsv>(
        "basic json",
        JsonDecoder::new(
            // generate a pretty boring csv stream
            Box::new(EnumerateStreamAsync::with_max(
                "create test json",
                10,
                (),
                |_, idx| {
                    Box::pin(async move {
                        if idx == 7 {
                            Ok(serde_json::to_string(&TestCsv {
                                index: format!("{}", idx),
                                words: String::from("words"),
                            }).expect("fail serializing"))
                        } else {
                            Ok(serde_json::to_string(&TestCsv {
                                index: format!("{}", idx),
                                words: String::from("nice words"),
                            }).expect("fail serializing"))
                        }
                    })
                },
            )),
        )
        .await,
        Box::new(mock::MockJsonDataOutput::default()),
    )
    .await
    .expect("Failed run_stream")
    .complete()
    .expect("Fail completing");
    if let Some(cmd_status) = job_state.step_history.get("basic json") {
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
            assert_eq!(10, *total_lines_scanned);
            assert_eq!(0, *num_errors);
        } else {
            panic!("move to db is not showing as completed");
        }
    } else {
        panic!("expected step name of `move to db` but did not find one");
    }
    jm_handle.await.expect("failure waiting for jm");
}

