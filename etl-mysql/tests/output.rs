use command::*;
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use etl_mysql::datastore::*;
use mock::*;
use serde::{Deserialize, Serialize};
use etl_core::job::state::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_mysql_output() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let job_state = JobRunner::new(
        "mysql_output_id",
        "mysql_output",
        jm_channel.clone(),
        JobRunnerConfig {
            ..Default::default()
        },
    )
    .run_cmd(SimpleCommand::new("create table Info", || {
        Box::pin(async {
            let pool = create_pool("admin", "admin", "localhost", "3306").await?;
            sqlx::query(CREATE_TABLE).execute(&pool).await?;
            sqlx::query("truncate table `default`.`Info`")
                .execute(&pool)
                .await?;
            Ok(())
        })
    }))
    .await
    .expect("Error running command")
    .run_stream::<TestOutputData>(
        "move to db",
        Box::new(create_mock_data_source()),
        Box::new(MySqlDataOutput {
            on_put_num_rows_max: 20,
            //on_put_num_rows: 10,
            on_put_num_rows: 1,
            table_name: "Info".to_owned(),
            db_name: "default".to_owned(),
            pool: MySqlDataOutputPool::CreatePool {
                max_connections: 1,
                user: "admin".to_string(),
                pw: "admin".to_string(),
                host: "localhost".to_string(),
                port: "3306".to_string(),
            },
        }),
    )
    .await
    .expect("Error running the stream")
    .complete()
    .expect("Error completing job");

    if let Some(cmd_status) = job_state.step_history.get("move to db") {
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
            assert_eq!(1, *step_index);
            assert_eq!(3, *total_lines_scanned);
            assert_eq!(1, *num_errors);
        } else {
            panic!("move to db is not showing as completed");
        }
    } else {
        panic!("expected step name of `move to db` but did not find one");
    }

    jm_handle
        .await
        .expect("Error awaiting on job manager handle");
}

async fn create_pool(user: &str, pw: &str, host: &str, port: &str) -> anyhow::Result<MySqlPool> {
    Ok(MySqlPoolOptions::new()
        .max_connections(1)
        // 3 hours timeout
        .connect_timeout(std::time::Duration::from_secs(60_u64))
        //.min_connections(1)
        .idle_timeout(Some(std::time::Duration::from_secs(60 * 10)))
        .max_lifetime(Some(std::time::Duration::from_secs(60 * 60 * 2)))
        .after_connect(|_conn| {
            Box::pin(async move {
                println!("MySql connection established");
                Ok(())
            })
        })
        .connect_lazy(&format!(
            "mysql://{}:{}@{}:{}/{}",
            user, pw, host, port, "default",
        ))?)
}

static CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS `default`.`Info` (
	`index` INTEGER NULL,
	`resourceType` varchar(100) NULL
)";

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct TestOutputData {
    resource_type: Option<String>,
    index: Option<usize>,
}

fn create_mock_data_source() -> MockJsonDataSource {
    MockJsonDataSource {
        lines: vec![
            serde_json::to_string(&TestOutputData {
                resource_type: Some(String::from("Bob")),
                index: Some(100),
            })
            .unwrap(),
            "1 this is a malformed json".to_string(),
            //"2 this is a malformed json".to_string(),
            serde_json::to_string(&TestOutputData {
                resource_type: Some(String::from("Bus")),
                index: Some(900),
            })
            .unwrap(),
            serde_json::to_string(&TestOutputData {
                resource_type: Some(String::from("Pencil")),
                index: Some(303),
            })
            .unwrap(),
        ],
        ..Default::default()
    }
}
