use super::*;
use bytes::Bytes;
use serde::de::DeserializeOwned;

// TODO: this all can be gotten rid of by simply removing serde traits from the DataSource and
// adding an impl of DataSource<T: Bytes>.  did not realize this initially for some reason
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

pub trait BytesSource: Sync + Send {
    fn name(&self) -> String;

    fn start_stream(self: Box<Self>) -> Result<BytesSourceTask, DataStoreError>;

    /*
    /// TODO: this is not integrated yet because this doesn't get the JobManagerChannel because I'm
    /// not completely convinced this is necessary.  After all, JobRunner can close the rx end of
    /// the channel provided by the data source
    fn process_job_manager_rx(&self, rx: &mut JobManagerRx) -> Result<(), DataStoreError> {
        loop {
            if let Ok(message) = rx.try_recv() {
                use crate::job_manager::Message::*;
                use crate::job_manager::NotifyDataSource;
                match message {
                    // this is a global message which means need to shutdown and stop
                    // what we are doing
                    ToDataSource(NotifyDataSource::TooManyErrors) => {
                        return Err(DataStoreError::TooManyErrors);
                    }
                    _ => {}
                }
            } else {
                // end of available messages
                break;
            }
        }
        Ok(())
    }
    */
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
                jr.log_err("JobManager", None, msg).await;
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!(
                "FATAL waiting on JoinHandle for DbOutputTask due to: {}",
                e
            )),
        }
    }
}

