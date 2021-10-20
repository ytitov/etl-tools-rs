use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use etl_core::splitter::*;
use mock::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// TODO:
// STATUS: when reaching max_errors on one pipeline, the second pipeline shuts down gracefully but
// does not receive that information.  The issue with that is, it saves a successful state.
// As a result, the split probably needs to be moved into the JobRunner, and some sort of
// dependency connections should be added
// - since the job runner shuts things down, no way for the copied data sources to know
// - in the end StreamHandler trait is best to handle this use case and this may be overkill and
// not really providing any new features.  Mainly because, all of the extra boiler plate needed
// with having to spawn separate tasks, then dealing with the join handle
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_splitter() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let jm_handle = job_manager.start();
    let (s_handle, mut datasources) =
        split_datasources::<TestSourceData>(Box::new(create_mock_data_source_many_errors()), 2)
            .await;

    let ds1 = datasources.remove(0);
    let ds2 = datasources.remove(0);
    let jr1 = JobRunner::create(
        "jr1_id",
        "Copy1",
        &jm_handle,
        JobRunnerConfig {
            max_errors: 2, // do a hard fail at error 2
            ..Default::default()
        },
    )
    .await
    .expect("Error creating JobRunner");
    let _ = jr1
        .run_stream::<TestSourceData>(
            "stream-jr1-ds",
            ds1,
            Box::new(MockJsonDataOutput {
                name: String::from("fast-ds"),
                sleep_duration: Duration::from_secs(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    //.expect("error processing stream 1")
    //.complete()
    //.expect("error completeing");

    JobRunner::create(
        "jr2_id",
        "Copy2",
        &jm_handle,
        JobRunnerConfig {
            //max_errors: 2, // do a hard fail at error 2
            ..Default::default()
        },
    )
    .await
    .expect("Error creating JobRunner")
    .run_stream::<TestSourceData>(
        "stream-jr2-ds",
        ds2,
        Box::new(MockJsonDataOutput {
            name: String::from("slow-ds"),
            sleep_duration: Duration::from_secs(2),
            ..Default::default()
        }),
    )
    .await
    .expect("error processing stream 2")
    .complete()
    .await
    .expect("error completeing");
    match s_handle.await {
        Ok(_) => {} //panic!("s_handle should return an error"),
        Err(_) => {}
    };
    jm_handle.shutdown().await.expect("error waiting on handle");
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestSourceData {
    name: Option<String>,
    todo: Vec<String>,
    id: String,
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
            "3 this is a malformed json".to_string(),
            "4 this is a malformed json".to_string(),
            "5 this is a malformed json".to_string(),
            serde_json::to_string(&TestSourceData {
                name: Some(String::from("Sandy")),
                todo: vec![String::from("sweep under rug")],
                id: String::from("s300"),
            })
            .unwrap(),
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
