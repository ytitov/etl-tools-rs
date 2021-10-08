use command::*;
use etl_core::datastore::enumerate::EnumerateStreamAsync;
use etl_core::job::state::*;
use etl_core::job::*;
use etl_core::job::stream::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use etl_mysql::datastore::*;
use serde::{Deserialize, Serialize};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_mysql_output() {
    let job_manager = JobManager::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager");
    let (jm_handle, jm_channel) = job_manager.start();
    let job_state = JobRunner::create(
        "mysql_output_id",
        "mysql_output",
        jm_channel.clone(),
        JobRunnerConfig {
            ..Default::default()
        },
    ).await.expect("Fatal error could not create JobRunner")
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
        /*
        Box::new(EnumerateStream {
            max: Some(100),
            name: String::from("MakeSomeThings"),
            pause: None,
            //pause: Some(std::time::Duration::from_secs(1)),
            //state: create_pool("admin", "admin", "localhost", "3306").await.expect("could not make pool"),
            state: (),
            create: |_, idx| {
                Ok(TestOutputData {
                    resource_type: Some(String::from("Bob")),
                    index: Some(idx),
                })
            },
        }),
        */
        /*
        Box::new(EnumerateStreamAsync {
            max: Some(100),
            name: String::from("MakeSomeThingsAsync"),
            pause: None,
            state: (),
            create: Box::new(|_, idx| {
                Box::pin(async move {
                    Ok(TestOutputData {
                        resource_type: Some(String::from("Bob")),
                        index: Some(idx),
                    })
                })
            }),
        }),
        */
        Box::new(EnumerateStreamAsync::with_max(
            "MakeSomeThingsAsync",
            100,
            (),
            |_, idx| {
                Box::pin(async move {
                    Ok(TestOutputData {
                        resource_type: Some(String::from("Bob")),
                        index: Some(idx),
                    })
                })
            },
        )),
        Box::new(MySqlDataOutput {
            on_put_num_rows_max: 20,
            on_put_num_rows: 3,
            //on_put_num_rows: 1,
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
    .complete().await
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
            assert_eq!(100, *total_lines_scanned);
            assert_eq!(0, *num_errors);
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

