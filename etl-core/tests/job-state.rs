use bytes::Bytes;
use enumerate::EnumerateStreamAsync;
use etl_core::datastore::*;
use etl_core::decoder::csv::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct State {
    offset: usize,
}

impl Default for State {
    fn default() -> Self {
        State { offset: 1000 }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct TestCsv {
    index: String,
    words: String,
}

/// test loading and saving state with default trait
/// it would be good to test with file system so we could have an existing state
/// also to test with bad serialization situations
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::create(
        "test_simple_state",
        "test_simple_state",
        jm_channel.clone(),
        JobRunnerConfig {
            ..Default::default()
        },
    ).await.expect("Error creating JobRunner");

    let mut jr = jr
        .run_stream::<TestCsv>(
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
                                Ok(Bytes::from("index,words"))
                            } else {
                                if idx == 7 {
                                    Ok(Bytes::from(format!("{},stuff,\"should error\"", idx)))
                                } else {
                                    Ok(Bytes::from(format!("{},stuff", idx)))
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
        .expect("Failed run_stream");

    jr.set_state("offset", &State { offset: 3 });
    let saved = jr
        .get_state::<State>("offset")
        .expect("Error with get_state");
    assert_eq!(saved.offset, 3);
    let job_state = jr.complete().await.expect("Fail completing");
    if let Ok(Some(saved)) = job_state.get::<State>("offset") {
        assert_eq!(saved.offset, 3);
    } else {
        panic!("Expected State struct to be in final state and did not find one");
    }
    jm_handle.await.expect("failure waiting for jm");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_existing() {
    use mock::MockJsonDataSource;
    use state::JobState;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use command::SimpleCommand;

    let mut hs_files = HashMap::new();
    let mut job_state = JobState::new("test_simple_state", "test_simple_state");
    let job_state_file_name = JobState::gen_name("test_simple_state", "test_simple_state");
    job_state.set("offset", &State { offset: 10 }).expect("Could not set value");
    hs_files.insert(
        String::from(&job_state_file_name),
        serde_json::to_string(&job_state).expect("Fatal error serializing"),
    );
    let files = Arc::new(Mutex::new(RefCell::new(hs_files)));
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let jr = JobRunner::create(
        "test_simple_state",
        "test_simple_state",
        jm_channel.clone(),
        JobRunnerConfig {
            ds: Box::new(MockJsonDataSource {
                lines: Vec::new(),
                files,
            }),
            ..Default::default()
        },
    ).await.expect("Error creating JobRunner");
    let mut jr = jr.run_cmd(SimpleCommand::new("dummy command", |_| {
        Box::pin(async {Ok(())})
    })).await.expect("Issues running command");

    let saved = jr
        .get_state_or_default::<State>("offset")
        .expect("Error with get_state");
    assert_eq!(saved.offset, 10);
    let job_state = jr.complete().await.expect("Fail completing");
    if let Ok(Some(saved)) = job_state.get::<State>("offset") {
        assert_eq!(saved.offset, 10);
    } else {
        panic!("Expected State struct to be in final state and did not find one");
    }
    jm_handle.await.expect("failure waiting for jm");
}
