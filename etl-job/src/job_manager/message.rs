use super::*;

#[derive(Debug)]
pub enum Message {
    ToJobManager(NotifyJobManager),
    ToJobRunner(NotifyJobRunner),
}

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
        name: B,
        id: A,
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
