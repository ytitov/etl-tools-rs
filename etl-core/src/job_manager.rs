use crate::job::JobRunner;
use serde::{Deserialize, Serialize};
use tokio::sync::{
    oneshot,
    //mpsc,
    mpsc::{Receiver, Sender},
    //broadcast,
    //broadcast::{error::RecvError, Receiver, Sender},
};
pub type JobManagerTx = Sender<Message>;
pub type JobManagerRx = Receiver<Message>;
use crate::utils::log::{log_err, log_info, LogMessage};
use crate::utils::*;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub enum NotifyJobRunner {
    JobCanRun { to_job_manager_tx: JobManagerTx },
    TooManyErrors,
}

#[derive(Debug)]
pub enum NotifyJobManager {
    LogInfo { sender: String, message: String },
    LogError { sender: String, message: String },
    TaskStarted { sender: String },
    TaskFinished { sender: String },
    JobStarted { sender: String },
    JobFinished { sender: SenderDetails },
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    to_job_runner_tx: HashMap<String, Sender<Message>>,
    from_job_runner_channel: Option<(Sender<Message>, Receiver<Message>)>,
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

/*
impl JobManagerChannel {
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.tx.send(Message::shutdown_job_manager())?;
        match self.join_handle {
            Some(jh) => jh.await?,
            _ => println!("Channel already shutdown"),
        }
        Ok(())
    }
}
*/

/*
impl Clone for JobManagerChannel {
    fn clone(&self) -> Self {
        JobManagerChannel {
            tx: self.tx.clone(),
        }
    }
}
*/

impl JobManagerChannel {
    pub fn shutdown_job_manager(self) -> anyhow::Result<()> {
        panic!("shutdown_job_manager is unimplemented");
        //self.tx.send(Message::shutdown_job_manager())?;
        //Ok(())
    }
}

impl JobManager {
    pub fn new(config: JobManagerConfig) -> anyhow::Result<Self> {
        //let (tx, rx) = broadcast::channel(16);
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

    pub fn start(mut self) -> JoinHandle<()> {
        let jh = tokio::spawn(async move {
            loop {
                if let Some((_, from_jobs_rx)) = &mut self.from_job_runner_channel {
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
                                        /*
                                        self.tx
                                            .send(Message::ToJobRunner(
                                                NotifyJobRunner::TooManyErrors,
                                            ))
                                            .expect("Fatal");
                                        */
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
                                JobStarted { sender, .. } => {
                                    //self.from_job_runner_rx.insert(sender.name, rx);
                                    if let Some((tx, rx)) = &self.from_job_runner_channel {
                                        //let (rx, tx) = mpsc::channel(1);
                                    }
                                    self.num_jobs_running += 1;
                                }
                                JobFinished { .. } => {
                                    self.num_jobs_running -= 1;
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
                        /*
                        Err(RecvError::Lagged(num)) => {
                            println!("JobManager RecvError::Lagged by {} messages", num);
                        }
                        Err(RecvError::Closed) => {
                            println!("JobManager RecvError::Closed");
                            break;
                        }
                        */
                    }
                }
            }
        });
        jh
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

    pub fn broadcast_job_start<A>(sender: A) -> Self
    where
        A: Into<String>,
    {
        Message::ToJobManager(NotifyJobManager::JobStarted {
            sender: sender.into()
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

    pub fn shutdown_job_manager() -> Self {
        Message::ToJobManager(NotifyJobManager::ShutdownJobManager)
    }
}
