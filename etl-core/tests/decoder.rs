use bytes::Bytes;
use enumerate::EnumerateStreamAsync;
use etl_core::datastore::*;
use etl_core::decoder::csv::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use serde::{Deserialize, Serialize};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_basic_csv_decoder() {
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
    .expect("Failed run_stream")
    .complete()
    .expect("Fail completing");
    jm_handle.await.expect("failure waiting for jm");
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct TestCsv {
    index: String,
    words: String,
}
