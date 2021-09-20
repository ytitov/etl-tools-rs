use super::*;
use bytes::Bytes;

pub type BytesSourceRx = Receiver<Result<BytesSourceMessage, DataStoreError>>;
pub type BytesSourceTx = Sender<BytesOutputMessage>;
pub type BytesSourceTask = (
    BytesSourceRx,
    JoinHandle<Result<BytesSourceStats, DataStoreError>>,
);

pub type BytesOutputJoinHandle = JoinHandle<anyhow::Result<()>>;

pub struct BytesSourceStats {
    pub lines_scanned: usize,
}

pub type BytesOutputTask = (Sender<BytesOutputMessage>, JoinHandle<anyhow::Result<()>>);

#[derive(Debug)]
pub enum BytesSourceMessage {
    Data { source: String, content: Bytes },
}

impl BytesSourceMessage {
    pub fn new<S: Into<String>, T: Into<Bytes>>(s: S, data: T) -> Self {
        BytesSourceMessage::Data {
            source: s.into(),
            content: data.into(),
        }
    }
}

#[derive(Debug)]
pub enum BytesOutputMessage {
    Data(Bytes),
    NoMoreData,
}

impl BytesOutputMessage {
    pub fn new<T: Into<Bytes>>(data: T) -> Self {
        BytesOutputMessage::Data(data.into())
    }
    pub fn no_more_data() -> Self {
        BytesOutputMessage::NoMoreData
    }
}

#[async_trait]
/// This is a simple store that acts like a key-val storage.  It is not streamted
/// so is not meant for big files.  Primarily created for the JobRunner to
/// store the state of the running job somewhere
pub trait SimpleStore<T: DeserializeOwned + Debug + 'static + Send>: Sync + Send {
    async fn read_file_str(&self, _: &str) -> Result<String, DataStoreError> {
        panic!("This SimpleStore does not support load operation");
    }

    async fn load(&self, _: &str) -> Result<T, DataStoreError> {
        panic!("This SimpleStore does not support load operation");
    }

    async fn write(&self, _: &str, _: T) -> Result<(), DataStoreError> {
        panic!("This SimpleStore does not support write operation");
    }
}

use crate::job_manager::JobManagerRx;

pub trait BytesSource: Sync + Send {
    fn name(&self) -> String;

    fn start_stream(self: Box<Self>) -> Result<BytesSourceTask, DataStoreError>;

    /// TODO: this is not integrated yet because this doesn't get the JobManagerChannel because I'm
    /// not completely convinced this is necessary.  After all, JobRunner can close the rx end of
    /// the channel provided by the data source
    fn process_job_manager_rx(&self, rx: &mut JobManagerRx) -> Result<(), DataStoreError> {
        loop {
            if let Ok(message) = rx.try_recv() {
                //use crate::job_manager::Message::*;
                //use crate::job_manager::NotifyBytesSource;
                // TODO: handle this
                match message {
                    // this is a global message which means need to shutdown and stop
                    // what we are doing
                    /*
                    ToBytesSource(NotifyBytesSource::TooManyErrors) => {
                        return Err(DataStoreError::TooManyErrors);
                    }
                    */
                    _ => {}
                }
            } else {
                // end of available messages
                break;
            }
        }
        Ok(())
    }
    /*
    fn boxed(self: Box<Self>) -> Box<dyn BytesSource<T> + Send + Sync> {
        Box::new(self)
    }
    */
}

/// Helps with creating BytesOutput's during run-time.
#[async_trait]
pub trait CreateBytesOutput<'de, C, T>: Sync + Send
where
    C: Deserializer<'de>,
    T: Serialize + Debug + 'static + Send,
{
    async fn create_data_output(_: C) -> anyhow::Result<Box<dyn BytesOutput>>;
}

/// Helps with creating BytesSource's during run-time.
#[async_trait]
pub trait CreateBytesSource<'de, C, T>: Sync + Send
where
    C: Deserializer<'de>,
    T: DeserializeOwned + Debug + 'static + Send,
{
    async fn create_data_source(_: C) -> anyhow::Result<Box<dyn BytesSource>>;
}

#[async_trait]
pub trait BytesOutput: Sync + Send {
    async fn start_stream(&mut self, _: JobManagerChannel) -> anyhow::Result<BytesOutputTask> {
        unimplemented!();
    }

    async fn shutdown(self: Box<Self>, _: &JobRunner) {
        println!("Called shutdown on self");
    }

    async fn data_output_shutdown(
        self: Box<Self>,
        dot: BytesOutputTask,
        jr: &JobRunner,
    ) -> anyhow::Result<()> {
        let (c, jh) = dot;
        drop(c);
        match jh.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                let msg = format!("Waiting for task to finish resulted in an error: {}", e);
                jr.log_err("JobManager", None, msg);
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!(
                "FATAL waiting on JoinHandle for DbOutputTask due to: {}",
                e
            )),
        }
    }
}

/// the purpose of this is to drop the sender, then wait on the receiver to finish.
/// the drop signals that it should finish, and waiting on the JoinHandle is to allow
/// it to finish its own work.  Of course if there are copies of the sender
/// floating around somewhere, this may cause some issues.  This is only used in the
/// job handler, so might be a good idea to just add it to the trait
/// --
/// Update: I am thinking this should not really fail, but still report the errors;
/// possibly take a pointer to job manager?
pub async fn data_output_shutdown<T>(jr: &JobRunner, (c, jh): BytesOutputTask) -> anyhow::Result<()>
where
    T: Serialize + Debug + Send + Sync,
{
    drop(c);
    match jh.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            let msg = format!("Waiting for task to finish resulted in an error: {}", e);
            jr.log_err("JobManager", None, msg);
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!(
            "FATAL waiting on JoinHandle for DbOutputTask due to: {}",
            e
        )),
    }
}

pub mod error {
    use thiserror::Error;
    #[derive(Error, Debug, Clone)]
    pub enum DataStoreError {
        #[error(
            "There was a problem deserializing: `{message:?}`, The string: `{attempted_string:?}`"
        )]
        Deserialize {
            message: String,
            attempted_string: String,
        },
        #[error("Could not decode utf8: `{0}`")]
        FatalUtf8(#[from] std::str::Utf8Error),
        #[error("There was a fatal problem: `{0}`")]
        //FatalIO(#[from] std::io::Error),
        FatalIO(String),
        #[error("Error: `{0}`")]
        //Generic(#[from] anyhow::Error),
        Generic(String),
        #[error("SendError from `{from:?}` to `{to:?}` reason: `{reason:?}`")]
        SendError {
            from: String,
            to: String,
            reason: String,
        },
        #[error("Error streaming lines from: `{key:?}`, error: `{error:?}`")]
        StreamingLines { key: String, error: String },
        #[error("Key or path `{key:?}` was not found.  Reason: `{error:?}`")]
        NotExist { key: String, error: String },
        #[error("Error returned from transform_item `{job_name:?}`.  Reason: `{error:?}`")]
        TransformerError { job_name: String, error: String },
        #[error("JoinError: `{0}`")]
        //JoinError(#[from] tokio::task::JoinError),
        JoinError(String),
        #[error("Shutting down.  JobManager sent a global TooManyErrors message.")]
        TooManyErrors,
    }

    impl DataStoreError {
        pub fn send_error<A: Into<String>, B: Into<String>, C: std::fmt::Display>(
            from: A,
            to: B,
            reason: C,
        ) -> Self {
            DataStoreError::SendError {
                from: from.into(),
                to: to.into(),
                reason: reason.to_string(),
            }
        }
    }

    use tokio::task::JoinError;
    impl From<JoinError> for DataStoreError {
        fn from(er: JoinError) -> Self {
            DataStoreError::JoinError(er.to_string())
        }
    }

    impl From<std::io::Error> for DataStoreError {
        fn from(er: std::io::Error) -> Self {
            DataStoreError::FatalIO(er.to_string())
        }
    }

    impl From<anyhow::Error> for DataStoreError {
        fn from(er: anyhow::Error) -> Self {
            DataStoreError::Generic(er.to_string())
        }
    }
}
