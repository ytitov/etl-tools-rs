use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use etl_core::splitter::*;
use mock::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// TODO: finish this test
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_splitter() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let jr1 = JobRunner::new(
        "jr1_id",
        "Copy1",
        jm_channel.clone(),
        JobRunnerConfig {
            max_errors: 2, // do a hard fail at error 2
            ..Default::default()
        },
    );
    let jr2 = JobRunner::new(
        "jr2_id",
        "Copy2",
        jm_channel.clone(),
        JobRunnerConfig {
            //max_errors: 2, // do a hard fail at error 2
            ..Default::default()
        },
    );
    let (s_handle, mut datasources) =
        split_datasources::<TestSourceData>(Box::new(create_mock_data_source_many_errors()), 2)
            .await;

    let ds1 = datasources.remove(0);
    let ds2 = datasources.remove(0);
    tokio::spawn(async move {
        let jr1_err = jr1.run_stream::<TestSourceData>(
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
    });

    tokio::spawn(async move {
        jr2.run_stream::<TestSourceData>(
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
        .expect("error completeing");
    });
    //s_handle.await.expect("error waiting on split handle");
    jm_handle.await.expect("error waiting on handle");
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
