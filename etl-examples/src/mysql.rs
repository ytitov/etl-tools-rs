use clap::Clap;
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use etl_core::utils;
use etl_mysql::datastore::*;
use etl_s3::datastore::*;
use serde::{Deserialize, Serialize};

use rusoto_core::region::Region;
//use sqlx::mysql::MySqlPoolOptions;

#[derive(Clap, Debug)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Args {
    #[clap(short, long, about = "Path to the input config file")]
    pub config: String,
    //#[clap(short, long, about = "Path to the log file", default_value = "./")]
    //pub log: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Config {
    pub credentials: String,
    pub s3_bucket: String,
    pub s3_keys: Vec<String>,
    //pub s3_output_json: String,
    pub max_connections: u8,
    pub user: String,
    pub pw: String,
    pub host: String,
    pub port: String,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct Info {
    resource_type: Option<String>,
    index: Option<usize>,
}

struct TestJob {
    //pub target_json: DataOutputTask<serde_json::Value>,
    pub target_mysql: DataOutputTask<Info>,
    pub pool: MySqlPool,
}

static CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS `default`.Info (
	`index` INTEGER NULL,
	`resourceType` varchar(100) NULL
)";

#[async_trait]
impl StreamHandler<serde_json::Value> for TestJob {
    // This is an optional function
    async fn init(&mut self, _: &JobRunner) -> anyhow::Result<JobRunnerAction> {
        sqlx::query(CREATE_TABLE).execute(&self.pool).await?;
        Ok(JobRunnerAction::Start)
    }

    async fn shutdown(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        data_output_shutdown(jr, self.target_mysql).await?;
        //data_output_shutdown(jr, self.target_json).await?;
        Ok(())
    }

    async fn process_item(
        &self,
        jinfo: JobItemInfo,
        // TODO: actually use this instead of just having a generic value
        _item: serde_json::Value,
        _: &JobRunner,
    ) -> anyhow::Result<()> {
        //println!("TestJob --> {:?}", &item);
        let TestJob {
            target_mysql: (target_mysql_tx, _),
            ..
        } = &self;
        target_mysql_tx
            .send(DataOutputMessage::new(Info {
                resource_type: Some("what".to_string()),
                index: Some(jinfo.index),
            }))
            .await?;
        Ok(())
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let Config {
        credentials,
        s3_bucket,
        s3_keys,
        //s3_output_json,
        max_connections,
        user,
        pw,
        host,
        port,
    } = utils::load_toml(&args.config, true)?;

    let job_manager = JobManager::new(JobManagerConfig::default())?;
    let (jm_handle, jm_channel) = job_manager.start();

    let pool = MySqlPoolOptions::new()
        .max_connections(max_connections as u32)
        // 3 hours timeout
        .connect_timeout(std::time::Duration::from_secs(60_u64 * 60_u64 * 3_u64))
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
        ))?;

    let testjob = TestJob {
        pool: pool.clone(),
        /*
        target_json: S3JsonDataOutput {
            credentials: credentials.clone(),
            s3_bucket: s3_bucket.clone(),
            s3_key: s3_output_json,
            region: Region::UsEast1,
        }
        .start_stream(&job_manager)
        .await?,
        */
        target_mysql: MySqlDataOutput {
            on_put_num_rows_max: 20,
            on_put_num_rows: 10,
            table_name: "Info".to_owned(),
            db_name: "default".to_owned(),
            pool: MySqlDataOutputPool::Pool(pool),
            /*
             * can also ask for a pool to be created
            pool: MySqlDataOutputPool::CreatePool {
                max_connections,
                user,
                pw,
                host,
                port,
            },
            */
        }
        .start_stream(jm_channel.clone())
        .await?,
    };
    let fs_ds = S3DataSource {
        read_content: ReadContentOptions::Json,
        credentials,
        s3_bucket,
        s3_keys,
        region: Region::UsEast1,
    };
    let job = JobRunner::new("test", "mysql", jm_channel, JobRunnerConfig::default());
    job.run::<serde_json::Value>(Box::new(fs_ds), Box::new(testjob))
        .await?
        .complete()?;

    jm_handle.await?;

    Ok(())
}
