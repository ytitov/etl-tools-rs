use crate::job::JobRunner;
use etl_core::datastore::error::DataStoreError;
use etl_core::datastore::simple::QueryableStore;
use etl_core::deps::bytes::Bytes;
use etl_core::deps::{
    anyhow,
    serde::{self, Deserialize, Serialize},
    tokio,
    tokio::sync::{
        mpsc,
        mpsc::{Receiver, Sender, UnboundedSender},
        oneshot,
    },
    tokio::task::JoinHandle,
};
use etl_core::utils::log::{log_err, log_info, tx_to_csv_output, tx_to_stdout_output, LogMessage};
use std::collections::HashMap;

pub type JobManagerTx = Sender<Message>;
pub type JobManagerRx = Receiver<Message>;

#[derive(Debug)]
pub enum NotifyJobRunner {
    JobCanRun { to_job_manager_tx: JobManagerTx },
    TooManyErrors,
}

#[derive(Debug)]
pub struct JobRunnerDetails {
    pub id: String,
    pub name: String,
}

#[derive(Debug)]
pub enum JobRunnerOperation {
    JobStart {
        reply_rx: oneshot::Sender<JobManagerRx>,
    },
    JobFinish,
    TaskStart,
    TaskFinish,
}

#[derive(Debug)]
pub enum JobRunnerRequest {
    LoadState {
        reply_tx: oneshot::Sender<Result<Bytes, DataStoreError>>,
    },
    SaveState {
        payload: Bytes,
        reply_tx: oneshot::Sender<Result<(), DataStoreError>>,
    },
}

#[derive(Debug)]
pub enum NotifyJobManager {
    JobOperation {
        details: JobRunnerDetails,
        operation: JobRunnerOperation,
    },
    JobRequest {
        details: JobRunnerDetails,
        request: JobRunnerRequest,
    },
    LogInfo {
        sender: String,
        message: String,
    },
    LogError {
        sender: String,
        message: String,
    },
    ShutdownJobManager,
}

#[derive(Debug, Clone)]
pub struct JobDetails {
    pub id: String,
    pub name: String,
}

#[derive(Debug)]
pub enum Message {
    ToJobManager(NotifyJobManager),
    ToJobRunner(NotifyJobRunner),
}

pub struct JobManagerConfig {
    /// When max_errors is reached, a shutdown message is sent, which stops all
    /// of the jobs currently running. There is no guarantee that this message will arrive at a
    /// specific time, so max_errors inside the JobRunner is the local allowable errors.
    pub max_errors: usize,
    /// If set to None, the output will go to stdout
    pub log_path: Option<String>,
    pub log_name_prefix: String,
    pub state_storage: Box<dyn QueryableStore<Bytes>>,
}

impl Default for JobManagerConfig {
    fn default() -> Self {
        use etl_core::datastore::mock::MockJsonDataSource;
        JobManagerConfig {
            max_errors: 1000,
            log_path: None,
            log_name_prefix: "job_manager".to_string(),
            state_storage: Box::new(MockJsonDataSource::default()),
        }
    }
}

pub struct JobManager {
    /// senders to job runners identified by their name
    /// currently only get the TooManyErrors message to let them know they need to terminate
    to_job_runner_tx: HashMap<String, Sender<Message>>,
    /// receives messages from JobRunner and also DataOutput's
    from_job_runner_channel: Option<Receiver<Message>>,
    num_log_errors: usize,
    logger_tx: UnboundedSender<LogMessage>,
    num_tasks_started: usize,
    num_tasks_finished: usize,
    num_jobs_running: usize,
    config: JobManagerConfig,
}

pub struct JobManagerChannel {
    pub rx: JobManagerRx,
    pub tx: JobManagerTx,
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

impl JobManagerHandle {
    pub fn get_jm_tx(&self) -> JobManagerTx {
        self.job_manager_tx.clone()
    }

    pub async fn connect<A, B>(&self, id: A, name: B) -> anyhow::Result<JobManagerChannel>
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
        self.job_manager_tx
            .send(Message::broadcast_job_start(id, name, oneshot_tx))
            .await?;
        let job_manager_rx = oneshot_rx.await?;
        Ok(JobManagerChannel {
            tx: self.job_manager_tx.clone(),
            rx: job_manager_rx,
        })
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        println!("Shutting down JobManager");
        match self
            .job_manager_tx
            .send(Message::ToJobManager(NotifyJobManager::ShutdownJobManager))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                println!("There was an error during shutdown of JobManager: {}", e);
            }
        };
        self.join_handle.await??;
        Ok(())
    }
}

impl JobManager {
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
        Ok(JobManager {
            to_job_runner_tx: HashMap::new(),
            from_job_runner_channel: None,
            num_log_errors: 0,
            logger_tx,
            num_tasks_started: 0,
            num_tasks_finished: 0,
            num_jobs_running: 0,
            config,
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
        /*
        log_info(
            &self.logger_tx,
            format!("JobManager: starting {}-{}", &name, &id),
        );
        */
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
        state_ds: &Box<dyn QueryableStore<Bytes>>,
        details: JobRunnerDetails,
        request: JobRunnerRequest,
    ) -> Result<(), DataStoreError> {
        let JobRunnerDetails { id, name } = details;
        let sep = state_ds.path_sep();
        let job_path = format!("{}{}{}{}.state", sep, &name, sep, &id);
        /*
        log_info(
            &self.logger_tx,
            format!("JobManager: starting {}-{}", &name, &id),
        );
        */
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
    pub fn start(mut self) -> JobManagerHandle {
        let (job_manager_tx, job_manager_rx) = mpsc::channel(16);
        self.from_job_runner_channel = Some(job_manager_rx);
        let state_datastore = self.config.state_storage;
        let jh: JoinHandle<anyhow::Result<JobManagerOutput>> = tokio::spawn(async move {
            loop {
                if let Some(from_jobs_rx) = &mut self.from_job_runner_channel {
                    match from_jobs_rx.recv().await {
                        Some(Message::ToJobManager(m)) => {
                            use NotifyJobManager::*;
                            match m {
                                LogInfo { sender, message } => {
                                    log_info(&self.logger_tx, format!("{}: {} ", sender, message));
                                }
                                LogError { sender, message } => {
                                    log_err(&self.logger_tx, format!("{}: {} ", sender, message));
                                    self.num_log_errors += 1;

                                    if self.num_log_errors >= self.config.max_errors {
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
                                        log_info(
                                            &self.logger_tx,
                                            "JobManager: no more running jobs, shutting down",
                                        );
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
                            log_info(&self.logger_tx, &format!("ToJobRunner: {:?}", m));
                        }
                    }
                } else {
                    use std::time::Duration;
                    println!("WARNING: JobManager is waiting for jobs to start, will not exit if none start");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            let o = JobManagerOutput {
                num_errors: self.num_log_errors,
            };
            log_info(&self.logger_tx, &format!("JobManager: {:?}", &o));
            Ok(o)
        });
        JobManagerHandle {
            join_handle: jh,
            job_manager_tx,
        }
    }
}

impl Message {
    pub fn load_job_state(
        job: &JobRunner,
    ) -> (Self, oneshot::Receiver<Result<Bytes, DataStoreError>>) {
        let (reply_tx, reply_rx) = oneshot::channel();
        (
            Message::ToJobManager(NotifyJobManager::JobRequest {
                request: JobRunnerRequest::LoadState { reply_tx },
                details: JobRunnerDetails {
                    id: job.id().into(),
                    name: job.name().into(),
                },
            }),
            reply_rx,
        )
    }

    pub fn save_job_state(
        job: &JobRunner,
        payload: Bytes,
    ) -> (Self, oneshot::Receiver<Result<(), DataStoreError>>) {
        let (reply_tx, reply_rx) = oneshot::channel();
        (
            Message::ToJobManager(NotifyJobManager::JobRequest {
                request: JobRunnerRequest::SaveState { payload, reply_tx },
                details: JobRunnerDetails {
                    id: job.id().into(),
                    name: job.name().into(),
                },
            }),
            reply_rx,
        )
    }

    pub fn log_info<A, B>(sender: A, message: B) -> Self
    where
        A: Into<String>,
        B: Into<String>,
    {
        Message::ToJobManager(NotifyJobManager::LogInfo {
            sender: sender.into(),
            message: message.into(),
        })
    }

    pub fn log_err<A, B>(sender: A, message: B) -> Self
    where
        A: Into<String>,
        B: Into<String>,
    {
        Message::ToJobManager(NotifyJobManager::LogError {
            sender: sender.into(),
            message: message.into(),
        })
    }

    pub fn broadcast_task_start(job: &JobRunner) -> Self {
        Message::ToJobManager(NotifyJobManager::JobOperation {
            details: JobRunnerDetails {
                id: job.id().to_string(),
                name: job.name().to_string(),
            },
            operation: JobRunnerOperation::TaskStart,
        })
    }

    pub fn broadcast_task_end(job: &JobRunner) -> Self {
        Message::ToJobManager(NotifyJobManager::JobOperation {
            details: JobRunnerDetails {
                id: job.id().to_string(),
                name: job.name().to_string(),
            },
            operation: JobRunnerOperation::TaskFinish,
        })
    }

    pub fn broadcast_job_start<A, B>(
        id: A,
        name: B,
        reply_rx: oneshot::Sender<JobManagerRx>,
    ) -> Self
    where
        A: ToString,
        B: ToString,
    {
        Message::ToJobManager(NotifyJobManager::JobOperation {
            details: JobRunnerDetails {
                id: id.to_string(),
                name: name.to_string(),
            },
            operation: JobRunnerOperation::JobStart { reply_rx },
        })
    }

    pub fn broadcast_job_end(job: &JobRunner) -> Self {
        Message::ToJobManager(NotifyJobManager::JobOperation {
            details: JobRunnerDetails {
                id: job.id().to_string(),
                name: job.name().to_string(),
            },
            operation: JobRunnerOperation::JobFinish,
        })
    }
}
