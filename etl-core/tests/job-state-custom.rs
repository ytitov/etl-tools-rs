//use bytes::Bytes;
//use enumerate::EnumerateStreamAsync;
use etl_core::datastore::*;
//use etl_core::decoder::csv::*;
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

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct TestState {
    number: usize
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
    let mut jr = JobRunner::new(
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
    );
    match jr.get_state_or_default::<TestState>("test-state") {
       Ok(_) => {},
       Err(e) => {
           panic!("Should gotten a default value for test-state: {}", e);
       },
    }

    let mut jr = jr.run_cmd(SimpleCommand::new("dummy command", || {
        Box::pin(async {Ok(())})
    })).await.expect("Issues running command");

    let saved = jr
        .get_state_or_default::<State>("offset")
        .expect("Error with get_state");
    assert_eq!(saved.offset, 1000);
    let job_state = jr.complete().await.expect("Fail completing");
    if let Ok(Some(saved)) = job_state.get::<State>("offset") {
        assert_eq!(saved.offset, 1000);
    } else {
        panic!("Expected State struct to be in final state and did not find one");
    }
    match job_state.get::<TestState>("test-state").expect("could not read state back out") {
        Some(_) => {}
        None => panic!("Expected to get back the state 'test-state' and did not get it"),
    };
    jm_handle.await.expect("failure waiting for jm");
}
