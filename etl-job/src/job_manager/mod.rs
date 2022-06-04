use crate::job::{error::JobRunnerError, state::JobState, JobRunner, JobRunnerConfig};
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::mock::MockJsonDataSource;
use etl_core::datastore::{TaskJoinHandle, OutputTask};
use etl_core::deps::log;
use etl_core::deps::{
    anyhow,
    bytes::Bytes,
    serde::{self, Deserialize, Serialize},
    tokio,
    tokio::sync::{
        mpsc,
        mpsc::{Receiver, Sender, UnboundedSender},
        oneshot,
    },
    tokio::task::JoinHandle,
};
use etl_core::keystore::simple::{
    mpsc::{MpscSimpleStoreRx, MpscSimpleStoreRxHandle, MpscSimpleStoreTx},
    SimpleStore,
};
use etl_core::utils::log::{log_err, log_info, tx_to_csv_output, tx_to_stdout_output, LogMessage};
use std::collections::HashMap;

/// messages that JobManager uses
pub mod message;

use message::*;

pub type JobManagerTx = Sender<Message>;
pub type JobManagerRx = Receiver<Message>;

#[derive(Debug, Clone)]
pub struct JobDetails {
    pub id: String,
    pub name: String,
}

pub struct JobManagerConfig {
    /// When max_errors is reached, a shutdown message is sent, which stops all
    /// of the jobs currently running. There is no guarantee that this message will arrive at a
    /// specific time, so max_errors inside the JobRunner is the local allowable errors.
    pub max_errors: usize,
    /// If set to None, the output will go to stdout
    pub log_path: Option<String>,
    pub log_name_prefix: String,
    pub storage: Box<dyn SimpleStore<Bytes>>,
    pub instance_id: String,
}

impl Default for JobManagerConfig {
    fn default() -> Self {
        JobManagerConfig {
            max_errors: 1000,
            log_path: None,
            log_name_prefix: "job_manager".to_string(),
            instance_id: "default".into(),
            storage: Box::new(MockJsonDataSource::default()),
        }
    }
}

// TODO:  All job runners should be instantiated with JobManager and nothing else
// * JobManager will use the MpscSimpleStore* and pass simple stores for every new job
// * check for name collisions and throw errors if more than one job has the same name
// * hard to justify why JobRunner exists, except maybe as a helper.
pub struct JobManagerBuilder {
    /// senders to job runners identified by their name
    /// currently only get the TooManyErrors message to let them know they need to terminate
    to_job_runner_tx: HashMap<String, Sender<Message>>,
    num_log_errors: usize,
    logger_tx: UnboundedSender<LogMessage>,
    num_tasks_started: usize,
    num_tasks_finished: usize,
    num_jobs_running: usize,
    storage: MpscSimpleStoreTx<Bytes>,
    storage_handle: MpscSimpleStoreRxHandle,
    max_errors: usize,
    instance_id: String,
}

pub struct JobManagerChannel {
    pub rx: JobManagerRx,
    pub tx: JobManagerTx,
}

impl JobManagerChannel {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(16);
        JobManagerChannel { rx, tx }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "serde")]
/// the resulting stats about the job
pub struct JobManagerOutput {
    pub num_errors: usize,
}

pub struct JobManagerHandle {
    join_handle: JoinHandle<anyhow::Result<JobManagerOutput>>,
    job_manager_tx: JobManagerTx,
}

pub struct JobManager {
    instance_id: String,
    task_join_handles: Vec<TaskJoinHandle>,
    job_manager_handle: JobManagerHandle,
    job_runner_join_handles: Vec<JoinHandle<anyhow::Result<JobState>>>,
    job_runners: HashMap<String, JobRunner>,
}

use std::future::Future;
use std::pin::Pin;
impl JobManager {
    pub async fn create_job<N: ToString>(
        &mut self,
        name: N,
        config: JobRunnerConfig,
    ) -> Result<JobRunner, Box<dyn std::error::Error>> {
        let name_str = name.to_string();
        let jr = JobRunner::create(
            &self.instance_id,
            &name_str,
            &self.job_manager_handle,
            config,
        )
        .await?;
        Ok(jr)
    }

    pub async fn run_job<N: ToString, F>(
        &mut self,
        name: N,
        config: JobRunnerConfig,
        func: F,
    ) -> anyhow::Result<()>
    where
        F: 'static
            + Fn(
                JobRunner,
            )
                -> Pin<Box<dyn Future<Output = Result<JobRunner, JobRunnerError>> + Send + Sync>>
            + Send
            + Sync,
    {
        let name_str = name.to_string();
        let instance_id = self.instance_id.clone();
        let job_manager_channel = self
            .job_manager_handle
            .connect(instance_id.clone(), name_str.clone())
            .await?;
        let jh = tokio::spawn(async move {
            let jr: JobRunner = JobRunner::create_with_channel(
                &instance_id,
                &name_str,
                job_manager_channel,
                config,
            )?;
            let jr: JobRunner = (func)(jr).await?;
            let s = jr.complete().await?;
            Ok(s)
        });
        self.job_runner_join_handles.push(jh);
        Ok(())
    }

    pub fn run_task<T>(&mut self, task: T) -> Result<(), JobRunnerError>
    where
        T: OutputTask,
    {
        match Box::new(task).create() {
            Ok(jh) => {
                self.task_join_handles.push(jh);
                Ok(())
            }
            Err(e) => Err(JobRunnerError::GenericError {
                message: format!("Failed running an DataOutput task due to: {}", e),
            }),
        }
    }

    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        for (_jr_name, jr) in self.job_runners {
            jr.complete().await?;
        }
        for jh in self.job_runner_join_handles {
            jh.await??;
        }
        for jh in self.task_join_handles {
            jh.await??;
        }
        self.job_manager_handle.shutdown().await?;
        Ok(())
    }
}

impl JobManagerHandle {
    pub fn get_jm_tx(&self) -> JobManagerTx {
        self.job_manager_tx.clone()
    }

    pub fn connect<A, B>(
        &self,
        name: A,
        instance_id: B,
    ) -> impl Future<Output = anyhow::Result<JobManagerChannel>> + '_
    where
        A: ToString,
        B: ToString,
    {
        // this will recieve the reciever from the JobManager so the JobRunner can send
        // messages
        let (oneshot_tx, oneshot_rx): (
            oneshot::Sender<JobManagerRx>,
            oneshot::Receiver<JobManagerRx>,
        ) = oneshot::channel();
        let tx = self.job_manager_tx.clone();
        let id = instance_id.to_string();
        let name = name.to_string();
        Box::pin(async {
            self.job_manager_tx
                .send(Message::broadcast_job_start(name, id, oneshot_tx))
                .await
                .expect("Could not get a response from JobManager");
            let job_manager_rx = oneshot_rx.await.unwrap();
            Ok(JobManagerChannel {
                tx,
                rx: job_manager_rx,
            })
        })
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        println!("Shutting down JobManager");
        match self
            .job_manager_tx
            .send(Message::ToJobManager(NotifyJobManager::ShutdownJobManager))
            .await
        {
            Ok(_) => {
                log::info!("Sent ShutdownJobManager");
                drop(self.job_manager_tx);
            }
            Err(e) => {
                println!("There was an error during shutdown of JobManager: {}", e);
            }
        };
        log::info!("Waiting on join_handle");
        self.join_handle.await??;
        Ok(())
    }
}

impl JobManagerBuilder {
    pub fn new(config: JobManagerConfig) -> anyhow::Result<Self> {
        let JobManagerConfig {
            ref log_path,
            ref log_name_prefix,
            ..
        } = config;
        let logger_tx = match log_path {
            Some(log_path) => {
                std::fs::create_dir_all(log_path)?;
                tx_to_csv_output(&format!("{}/{}", log_path, log_name_prefix), true)?
            }
            None => tx_to_stdout_output()?,
        };
        let (storage_handle, storage) = MpscSimpleStoreRx::create(config.storage);
        //let (from_job_runner_tx, from_job_runners_rx) = mpsc::channel(16);
        Ok(JobManagerBuilder {
            instance_id: config.instance_id,
            to_job_runner_tx: HashMap::new(),
            num_log_errors: 0,
            logger_tx,
            num_tasks_started: 0,
            num_tasks_finished: 0,
            num_jobs_running: 0,
            storage_handle,
            storage,
            max_errors: config.max_errors,
        })
    }

    fn notify_job_manager_job_operation(
        to_job_runner_tx: &mut HashMap<String, Sender<Message>>,
        num_jobs_running: &mut usize,
        num_tasks_started: &mut usize,
        num_tasks_finished: &mut usize,
        details: JobRunnerDetails,
        operation: JobRunnerOperation,
    ) -> Result<(), DataStoreError> {
        let JobRunnerDetails { id, name } = details;
        let sender_name = format!("{}-{}", &name, &id);
        use JobRunnerOperation::*;
        match operation {
            JobStart { reply_rx } => {
                let (tx, rx): (JobManagerTx, JobManagerRx) = mpsc::channel(1);
                reply_rx.send(rx).expect(
                    "Fatal error replying with a JobManagerRx to a job, this should never happen",
                );
                to_job_runner_tx.insert(sender_name, tx);
                *num_jobs_running += 1;
            }
            JobFinish => {
                *num_jobs_running -= 1;
                let _ = to_job_runner_tx.remove(&sender_name);
            }
            TaskStart => {
                *num_tasks_started += 1;
            }
            TaskFinish => {
                *num_tasks_finished += 1;
            }
        }
        Ok(())
    }

    async fn notify_job_manager_job_request(
        state_ds: &Box<dyn SimpleStore<Bytes>>,
        details: JobRunnerDetails,
        request: JobRunnerRequest,
    ) -> Result<(), DataStoreError> {
        let JobRunnerDetails { id, name } = details;
        let sep = state_ds.path_sep();
        let job_path = format!(
            "{sep}jobs{sep}{name}{sep}{id}.state",
            sep = sep,
            name = &name,
            id = &id
        );
        use JobRunnerRequest::*;
        match request {
            LoadState { reply_tx } => {
                let b = state_ds.load(&job_path).await;
                reply_tx
                    .send(b)
                    .map_err(|_e| DataStoreError::FatalIO("Failed to load state".into()))?;
            }
            SaveState { payload, reply_tx } => {
                let w = state_ds.write(&job_path, payload).await;
                reply_tx
                    .send(w)
                    .map_err(|_e| DataStoreError::FatalIO("Failed to save state".into()))?;
            }
        }
        Ok(())
    }

    //pub fn start(mut self) -> JoinHandle<()> {
    //pub fn start(mut self) -> JobManagerHandle {
    pub fn into_job_manager(mut self) -> JobManager {
        let (job_manager_tx, job_manager_rx) = mpsc::channel(16);
        let state_datastore = Box::new(self.storage) as Box<dyn SimpleStore<Bytes>>;
        let storage_handle = self.storage_handle;
        let instance_id = self.instance_id;
        let jh: JoinHandle<anyhow::Result<JobManagerOutput>> = tokio::spawn(async move {
            let mut from_jobs_rx = job_manager_rx;
            loop {
                match from_jobs_rx.recv().await {
                    Some(Message::ToJobManager(m)) => {
                        use NotifyJobManager::*;
                        match m {
                            LogInfo { sender, message } => {
                                log::info!("{}: {} ", sender, message);
                            }
                            LogError { sender, message } => {
                                log_err(&self.logger_tx, format!("{}: {} ", sender, message));
                                self.num_log_errors += 1;

                                if self.num_log_errors >= self.max_errors {
                                    log_err(
                                        &self.logger_tx,
                                        "Reached too many global errors, shutting down",
                                    );
                                    for (_, tx) in &self.to_job_runner_tx {
                                        tx.send(Message::ToJobRunner(
                                            NotifyJobRunner::TooManyErrors,
                                        ))
                                        .await
                                        .expect("Could not notify job_runner_tx");
                                    }
                                }
                            }
                            // needed so the job manager can finish writing the log
                            ShutdownJobManager => {
                                log::info!("Got ShutdownJobManager message");
                                break;
                            }
                            JobOperation { details, operation } => {
                                Self::notify_job_manager_job_operation(
                                    &mut self.to_job_runner_tx,
                                    &mut self.num_jobs_running,
                                    &mut self.num_tasks_started,
                                    &mut self.num_tasks_finished,
                                    details,
                                    operation,
                                )?;
                                if self.num_jobs_running == 0 {
                                    log::info!("JobManager: no more running jobs, shutting down");
                                    // TODO: this method still leaves some messages
                                    // in the queue
                                    break;
                                }
                            }
                            JobRequest { details, request } => {
                                Self::notify_job_manager_job_request(
                                    &state_datastore,
                                    details,
                                    request,
                                )
                                .await?;
                            }
                        }
                    }
                    None => {
                        println!("JobManager exiting got a none");
                        break;
                    }
                    Some(Message::ToJobRunner(m)) => {
                        log::info!("ToJobRunner: {:?}", m);
                    }
                }
            }
            let o = JobManagerOutput {
                num_errors: self.num_log_errors,
            };
            log_info(&self.logger_tx, &format!("JobManager: {:?}", &o));
            drop(state_datastore); // so the storage won't wait forever
            storage_handle.await??;
            Ok(o)
        });
        JobManager {
            job_runners: HashMap::new(),
            task_join_handles: Vec::new(),
            instance_id,
            job_runner_join_handles: Vec::new(),
            job_manager_handle: JobManagerHandle {
                join_handle: jh,
                job_manager_tx,
            },
        }
    }
}
