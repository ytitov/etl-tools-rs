/// create the following tests
/// TODO: 1. test with errors inside the process element function itself
/// TODO: 2. test max_errors
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use mock::mock_csv::*;
use mock::MockJsonDataOutput;
use serde::{Deserialize, Serialize};
use state::*;

/// test when every single line generated a serialization error
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn basic_stream_handler_all_errors() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let testjob = TestJob {
        output: MockJsonDataOutput::default()
            .start_stream(jm_channel.clone())
            .await
            .expect("Could not create mock json data output"),
    };
    let jr = JobRunner::new(
        "test",
        "custom_job_state",
        jm_channel,
        JobRunnerConfig {
            ..Default::default()
        },
    );

    let job_state = jr
        .run_stream_handler(
            Box::new(create_csv_datasource_all_bad_lines()),
            Box::new(testjob),
        )
        .await
        .expect("Error running stream")
        .complete()
        .expect("Job completed with an error");
    jm_handle.await;
    if let Some(cmd_status) = job_state.step_history.get("TestJob") {
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
            assert_eq!(0, *total_lines_scanned);
            assert_eq!(3, *num_errors);
        } else {
            panic!("transformed-ds-1 is not showing as completed");
        }
    } else {
        panic!("Expected a step with name transformed-ds-1");
    }
}

fn create_csv_datasource_all_bad_lines() -> MockCsvDataSource {
    MockCsvDataSource {
        lines: vec![
            "id,name,notes".to_string(),
            "4f,\"McDondald, John\",needs to register".to_string(),
            "10q,\"Tim Dawes\",\"testing complete\"".to_string(),
            "11r,\"Tim Jones\",".to_string(),
        ],
        csv_options: CsvReadOptions {
            escape: Some(b'\\'),
            //quote: b'"',
            ..Default::default()
        },
        ..Default::default()
    }
}
struct TestJob {
    pub output: DataOutputTask<TestOutputData>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestSourceData {
    id: String,
    name: String,
    // force a failure on all serialization
    blah: String,
    notes: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct TestOutputData {
    id: String,
    name: String,
}

#[async_trait]
impl StreamHandler<TestSourceData> for TestJob {
    fn name(&self) -> &'static str {
        "TestJob"
    }

    async fn init(&mut self, _: &JobRunner) -> anyhow::Result<JobRunnerAction> {
        Ok(JobRunnerAction::Start)
    }
    async fn process_item(
        &self,
        jinfo: JobItemInfo,
        item: TestSourceData,
        _: &JobRunner,
    ) -> anyhow::Result<()> {
        let TestJob {
            output: (output_tx, _),
        } = &self;
        println!("{:?}", &item);
        let TestSourceData { id, name, .. } = item;
        output_tx
            .send(DataOutputMessage::new(TestOutputData { id, name }))
            .await?;
        Ok(())
    }

    async fn shutdown(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        Ok(())
    }
}
