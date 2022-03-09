/// Create pipelines linking [crate::datastore::DataSource]s together; Provides state management based on a job id,
/// responsible for generating and running pipelines, including any run-time configurations
pub mod job;
/// and records each step.  A successful job with the same job id will not run more than once
pub mod job_manager;
pub mod transform_store;


use etl_core::deps::*;
use etl_core::datastore::{DataOutputTask, DataOutputStats};
use crate::job::JobRunner;
use std::fmt::Debug;

/// the purpose of this is to drop the sender, then wait on the receiver to finish.
/// the drop signals that it should finish, and waiting on the JoinHandle is to allow
/// it to finish its own work.  Of course if there are copies of the sender
/// floating around somewhere, this may cause some issues.  This is only used in the
/// job handler, so might be a good idea to just add it to the trait
/// --
/// Update: I am thinking this should not really fail, but still report the errors;
/// possibly take a pointer to job manager?
pub async fn data_output_shutdown<T>(
    jr: &JobRunner,
    (c, jh): DataOutputTask<T>,
) -> anyhow::Result<DataOutputStats>
where
    T: Debug + Send + Sync,
{
    drop(c);
    match jh.await {
        Ok(Ok(stats)) => Ok(stats),
        Ok(Err(e)) => {
            let msg = format!("Waiting for task to finish resulted in an error: {}", e);
            jr.log_err("JobManager", None, msg.clone()).await;
            Err(anyhow::anyhow!(msg))
        }
        Err(e) => Err(anyhow::anyhow!(
            "FATAL waiting on JoinHandle for DbOutputTask due to: {}",
            e
        )),
    }
}
