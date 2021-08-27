use clap::Clap;
use etl_core::datastore::*;
use etl_core::job::*;
use etl_core::job_manager::*;
use etl_core::preamble::*;
use etl_core::utils;
use etl_s3::datastore::*;
use serde::{Deserialize, Serialize};

use rusoto_core::region::Region;

#[derive(Clap, Debug)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Args {
    #[clap(short, long, about = "Path to the input config file")]
    pub config: String,
    //#[clap(short, long, about = "Path to the log file", default_value = "./")]
    //pub log: String,
}

/// The configuration file to get some details for this job
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Config {
    pub credentials: String,
    pub s3_bucket: String,
    pub s3_keys: Vec<String>,
    pub s3_output_json: String,
    pub s3_output_csv: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// One of the outputs
struct Info {
    resource_type: Option<String>,
    index: Option<usize>,
}

struct TestJob {
    pub target_json: DataOutputTask<serde_json::Value>,
    pub target_csv: DataOutputTask<Info>,
}

#[async_trait]
impl StreamHandler<serde_json::Value> for TestJob {
    async fn shutdown(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        // need to await on the output streams so they can complete
        // their writes, else job will shutdown before that can happen
        data_output_shutdown(jr, self.target_csv).await?;
        data_output_shutdown(jr, self.target_json).await?;
        Ok(())
    }

    async fn process_item(
        &self,
        jinfo: JobItemInfo,
        item: serde_json::Value,
        _: &JobRunner,
    ) -> anyhow::Result<()> {
        let TestJob {
            target_json: (target_json_tx, _),
            target_csv: (target_csv_tx, _),
        } = &self;
        target_json_tx
            .send(DataOutputMessage::new(item.clone()))
            .await?;
        target_csv_tx
            .send(DataOutputMessage::new(Info {
                resource_type: Some("what".to_string()),
                index: Some(jinfo.index),
            }))
            .await?;
        Ok(())
    }
}

struct TestJob2 {
    pub target: DataOutputTask<Info>,
}

#[async_trait]
impl StreamHandler<Info> for TestJob2 {
    async fn shutdown(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        data_output_shutdown(jr, self.target).await?;
        Ok(())
    }

    async fn process_item(
        &self,
        _jinfo: JobItemInfo,
        item: Info,
        _: &JobRunner,
    ) -> anyhow::Result<()> {
        let TestJob2 {
            target: (target_tx, _),
        } = &self;
        target_tx.send(DataOutputMessage::new(item)).await?;
        Ok(())
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let Config {
        ref credentials,
        ref s3_bucket,
        s3_keys,
        s3_output_json,
        ref s3_output_csv,
    } = utils::load_toml(&args.config, true)?;
    let job_manager = JobManager::new(JobManagerConfig::default())?;
    let (jm_handle, jm_channel) = job_manager.start();
    let testjob = TestJob {
        target_json: S3DataOutput {
            credentials: credentials.clone(),
            s3_bucket: s3_bucket.clone(),
            s3_key: s3_output_json,
            region: Region::UsEast1,
            write_content: WriteContentOptions::Json,
        }
        .start_stream(jm_channel.clone())
        .await?,
        target_csv: S3DataOutput {
            credentials: credentials.clone(),
            s3_bucket: s3_bucket.clone(),
            s3_key: s3_output_csv.to_owned(),
            region: Region::UsEast1,
            write_content: WriteContentOptions::Csv(CsvWriteOptions::default()),
        }
        .start_stream(jm_channel.clone())
        .await?,
    };
    let s3_json_ds = S3DataSource {
        read_content: ReadContentOptions::Csv(CsvReadOptions::default()),
        credentials: credentials.to_owned(),
        s3_bucket: s3_bucket.to_owned(),
        s3_keys,
        region: Region::UsEast1,
    };

    let jr = JobRunner::new("jr1", "s3", jm_channel.clone(), JobRunnerConfig::default());
    jr.run(Box::new(s3_json_ds), Box::new(testjob)).await?;

    // this is the resulting file from the previous job.
    let s3_csv_ds = S3DataSource {
        read_content: ReadContentOptions::Csv(CsvReadOptions::default()),
        credentials: credentials.to_owned(),
        s3_bucket: s3_bucket.to_owned(),
        s3_keys: vec![s3_output_csv.to_owned()],
        region: Region::UsEast1,
    };

    let testjob2 = TestJob2 {
        target: mock::MockJsonDataOutput {
            name: "Job2".to_owned(),
        }
        .start_stream(jm_channel.clone())
        .await?,
    };
    let jr = JobRunner::new("jr2", "s3", jm_channel, JobRunnerConfig::default());
    jr.run(Box::new(s3_csv_ds), Box::new(testjob2))
        .await?
        .complete()?;

    jm_handle.await?;

    Ok(())
}
