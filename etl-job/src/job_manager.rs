use crate::job::JobRunner;
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
pub enum NotifyJobManager {
    LogInfo {
        sender: String,
        message: String,
    },
    LogError {
        sender: String,
        message: String,
    },
    TaskStarted {
        sender: String,
    },
    TaskFinished {
        sender: String,
    },
    JobStarted {
        sender_name: String,
        reply_rx: oneshot::Sender<JobManagerRx>,
    },
    JobFinished {
        sender: SenderDetails,
    },
    ShutdownJobManager,
}

#[derive(Debug, Clone)]
pub struct SenderDetails {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum NotifyJob {
    Scale {
        started: usize,
        finished: usize,
        running: usize,
    },
}

#[derive(Debug, Clone)]
pub enum NotifyDataSource {
    TooManyErrors,
}

#[derive(Debug)]
pub enum Message {
    ToJobManager(NotifyJobManager),
    ToJobRunner(NotifyJobRunner),
    ToDataSource(NotifyDataSource),
    ToJob(NotifyJob),
}

// Users the re-exported crate from etl-core
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "serde")]
pub struct JobManagerConfig {
    /// When max_errors is reached, a shutdown message is sent, which stops all
    /// of the jobs currently running. There is no guarantee that this message will arrive at a
    /// specific time, so max_errors inside the JobRunner is the local allowable errors.
    pub max_errors: usize,
    /// If set to None, the output will go to stdout
    pub log_path: Option<String>,
    pub log_name_prefix: String,
}

impl Default for JobManagerConfig {
    fn default() -> Self {
        JobManagerConfig {
            max_errors: 1000,
            log_path: None,
            log_name_prefix: "job_manager".to_string(),
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

    pub async fn connect<N: Into<String>>(&self, name: N) -> anyhow::Result<JobManagerChannel> {
        // this will recieve the reciever from the JobManager so the JobRunner can send
        // messages
        let (oneshot_tx, oneshot_rx): (
            oneshot::Sender<JobManagerRx>,
            oneshot::Receiver<JobManagerRx>,
        ) = oneshot::channel();
        self.job_manager_tx
            .send(Message::broadcast_job_start(name, oneshot_tx))
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

    //pub fn start(mut self) -> JoinHandle<()> {
    pub fn start(mut self) -> JobManagerHandle {
        let (job_manager_tx, job_manager_rx) = mpsc::channel(16);
        self.from_job_runner_channel = Some(job_manager_rx);
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
                                TaskStarted { sender } => {
                                    self.num_tasks_started += 1;
                                    let s = format!(
                                        "Started {} tasks: {}/{}",
                                        sender, self.num_tasks_finished, self.num_tasks_started
                                    );

                                    /*
                                    self.tx
                                        .send(Message::ToJob(NotifyJob::Scale {
                                            started: self.num_tasks_started,
                                            finished: self.num_tasks_finished,
                                            running: self.num_tasks_started
                                                - self.num_tasks_finished,
                                        }))
                                        .expect("Fatal");
                                    */
                                    log_info(&self.logger_tx, s);
                                }
                                TaskFinished { sender } => {
                                    self.num_tasks_finished += 1;
                                    /*
                                    self.tx
                                        .send(Message::ToJob(NotifyJob::Scale {
                                            started: self.num_tasks_started,
                                            finished: self.num_tasks_finished,
                                            running: self.num_tasks_started
                                                - self.num_tasks_finished,
                                        }))
                                        .expect("Fatal");
                                    */
                                    let s = format!(
                                        "Finished {} tasks: {}/{}",
                                        sender, self.num_tasks_finished, self.num_tasks_started
                                    );
                                    log_info(&self.logger_tx, s);
                                }
                                // needed so the job manager can finish writing the log
                                ShutdownJobManager => {
                                    break;
                                }
                                JobStarted {
                                    sender_name,
                                    reply_rx,
                                } => {
                                    log_info(
                                        &self.logger_tx,
                                        format!("JobManager: starting {}", &sender_name),
                                    );
                                    let (tx, rx): (JobManagerTx, JobManagerRx) = mpsc::channel(1);
                                    reply_rx
                                        .send(rx)
                                        .expect("Fatal error replying with a JobManagerRx to a job, this should never happen");
                                    self.to_job_runner_tx.insert(sender_name, tx);
                                    self.num_jobs_running += 1;
                                }
                                JobFinished { sender } => {
                                    self.num_jobs_running -= 1;
                                    log_info(
                                        &self.logger_tx,
                                        format!("JobManager: finished {}", &sender.name),
                                    );
                                    let _ = self.to_job_runner_tx.remove(&sender.name);
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
                            }
                        }
                        None => {
                            println!("JobManager exiting got a none");
                            break;
                        }
                        // ignore these here, as they are meant for Jobs, just log them
                        Some(Message::ToJob(m)) => {
                            log_info(&self.logger_tx, format!("ToJob: {:?}", &m));
                        }
                        Some(Message::ToJobRunner(m)) => {
                            log_info(&self.logger_tx, &format!("ToJobRunner: {:?}", m));
                        }
                        Some(Message::ToDataSource(m)) => {
                            log_info(&self.logger_tx, &format!("ToDataSource: {:?}", m));
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

    pub fn broadcast_task_start<A>(sender: A) -> Self
    where
        A: Into<String>,
    {
        Message::ToJobManager(NotifyJobManager::TaskStarted {
            sender: sender.into(),
        })
    }

    pub fn broadcast_task_end<A>(sender: A) -> Self
    where
        A: Into<String>,
    {
        Message::ToJobManager(NotifyJobManager::TaskFinished {
            sender: sender.into(),
        })
    }

    pub fn broadcast_job_start<A>(sender: A, reply_rx: oneshot::Sender<JobManagerRx>) -> Self
    where
        A: Into<String>,
    {
        Message::ToJobManager(NotifyJobManager::JobStarted {
            sender_name: sender.into(),
            reply_rx,
        })
    }

    pub fn broadcast_job_end(job: &JobRunner) -> Self {
        Message::ToJobManager(NotifyJobManager::JobFinished {
            sender: SenderDetails {
                id: String::from(job.id()),
                name: String::from(job.name()),
            },
        })
    }
}
