use self::error::*;
use crate::job_manager::JobManagerTx;
use crate::preamble::*;
use async_trait::async_trait;
use serde::de::Deserializer;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

pub mod bytes_source;
/// creates generated data sources
pub mod enumerate;
/// Local file system data stores
pub mod fs;
/// various data stores used for testing
pub mod mock;
pub mod transform_store;

//pub type DataOutputItemResult = Result<String, Box<dyn std::error::Error + Send>>;

pub type DataOutputItemResult<T> = Result<T, DataStoreError>;

pub type DataSourceRx<T> = Receiver<Result<DataSourceMessage<T>, DataStoreError>>;
pub type DataSourceTask<T> = (
    DataSourceRx<T>,
    JoinHandle<Result<DataSourceStats, DataStoreError>>,
);
pub type DataOutputTx<T> = Sender<DataOutputMessage<T>>;
pub type DataOutputTask<T> = (DataOutputTx<T>, JoinHandle<anyhow::Result<DataOutputStats>>);

pub type DataOutputJoinHandle = JoinHandle<anyhow::Result<DataOutputStats>>;

pub struct DataSourceStats {
    pub lines_scanned: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataOutputStats {
    pub name: String,
    pub lines_written: usize,
}

#[derive(Debug)]
pub enum DataSourceMessage<T: Send> {
    Data { source: String, content: T },
}

impl<T: Send> DataSourceMessage<T> {
    pub fn new<S: Into<String>>(s: S, data: T) -> Self {
        DataSourceMessage::Data {
            source: s.into(),
            content: data,
        }
    }
}

#[derive(Debug)]
pub enum DataOutputMessage<T: Debug + Send + Sync> {
    Data(T),
    NoMoreData,
}

impl<T: Debug + Send + Sync> DataOutputMessage<T> {
    pub fn new(data: T) -> Self {
        DataOutputMessage::Data(data)
    }
    pub fn no_more_data() -> Self {
        DataOutputMessage::NoMoreData
    }
}

#[async_trait]
/// This is a simple store that acts like a key-val storage.  It is not streamted
/// so is not meant for big files.  Primarily created for the JobRunner to
/// store the state of the running job somewhere
pub trait SimpleStore<T: Debug + 'static + Send>: Sync + Send {
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

//use crate::job_manager::JobManagerRx;
pub struct DynDataSource<T> {
    pub ds: Box<dyn DataSource<T>>,
}

impl<T: Debug + Send + 'static> DynDataSource<T> {
    pub fn new<C: DataSource<T> + 'static>(t: C) -> Self {
        DynDataSource { ds: Box::new(t) }
    }
}

pub trait DataSource<T: Debug + 'static + Send>: Sync + Send {
    fn name(&self) -> String;

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError>;

    /*
    /// TODO: this is not integrated yet because this doesn't get the JobManagerChannel because I'm
    /// not completely convinced this is necessary.  After all, JobRunner can close the rx end of
    /// the channel provided by the data source
    fn process_job_manager_rx(
        &self,
        rx: &mut JobManagerRx,
    ) -> Result<(), DataStoreError> {
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
    fn boxed(self: Box<Self>) -> Box<dyn DataSource<T> + Send + Sync> {
        Box::new(self)
    }
    */
}

impl<T: 'static + Debug + Send> DataSource<T> for DataSourceTask<T> {
    fn name(&self) -> String {
        String::from("DataSourceTask")
    }
    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        Ok(*self)
    }
}

/// Helps with creating DataOutput's during run-time.
#[async_trait]
pub trait CreateDataOutput<'de, C, T>: Sync + Send
where
    C: Deserializer<'de>,
    T: Debug + 'static + Send,
{
    async fn create_data_output(_: C) -> anyhow::Result<Box<dyn DataOutput<T>>>;
}

/// Helps with creating DataSource's during run-time.
#[async_trait]
pub trait CreateDataSource<'de, C, T>: Sync + Send
where
    C: Deserializer<'de>,
    T: Debug + 'static + Send,
{
    async fn create_data_source(_: C) -> anyhow::Result<Box<dyn DataSource<T>>>;
}

#[async_trait]
pub trait DataOutput<T: Debug + 'static + Sync + Send>: Sync + Send {
    async fn start_stream(self: Box<Self>, _: JobManagerTx) -> anyhow::Result<DataOutputTask<T>> {
        unimplemented!();
    }

    async fn shutdown(self: Box<Self>, _: &JobRunner) {
        println!("Called shutdown on self");
    }

    async fn data_output_shutdown(
        self: Box<Self>,
        dot: DataOutputTask<T>,
        jr: &JobRunner,
    ) -> anyhow::Result<DataOutputStats> {
        let (c, jh) = dot;
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
}

#[async_trait]
impl<T: Debug + 'static + Sync + Send> DataSource<T> for DataSourceRx<T> {
    fn name(&self) -> String {
        String::from("DataSourceRx")
    }

    fn start_stream(mut self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        use tokio::task::JoinHandle;
        let (tx, rx) = channel(1);
        let name = self.name();
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                match self.recv().await {
                    Some(Ok(DataSourceMessage::Data {source, content})) => {
                        lines_scanned += 1;
                        tx.send(Ok(DataSourceMessage::new(&name, content)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &source, e))?;
                    }
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    None => break,
                }
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
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

#[derive(Debug, Clone)]
pub enum LineType {
    Csv,
    Json,
    Text,
}

use csv;
#[derive(Debug, Clone)]
pub struct CsvReadOptions {
    pub delimiter: u8,     // b','
    pub has_headers: bool, // true
    /// number of fields can change
    pub flexible: bool, // false, if num fields changes
    pub terminator: csv::Terminator, // csv::Terminator::CLRF
    /// quote character to use
    pub quote: u8, // b'"'
    /// escape character
    pub escape: Option<u8>, //None
    /// enable/disable double quotes are escapes
    pub double_quote: bool, //true
    /// enable/disable quoting
    pub quoting: bool, //true
    pub comment: Option<u8>, //None
}

#[derive(Debug, Clone)]
pub struct CsvWriteOptions {
    pub delimiter: u8,               // b','
    pub has_headers: bool,           // true
    pub terminator: csv::Terminator, // csv::Terminator::CLRF
    pub quote_style: csv::QuoteStyle,
    /// quote character to use
    pub quote: u8, // b'"'
    /// escape character
    pub escape: u8, // default to b'\'
    /// enable/disable double quotes are escapes
    pub double_quote: bool, //true
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        CsvReadOptions {
            delimiter: b',',
            has_headers: true,
            flexible: false,
            terminator: csv::Terminator::CRLF,
            quote: b'"',
            escape: None,
            double_quote: true,
            quoting: true,
            comment: None,
        }
    }
}

impl Default for CsvWriteOptions {
    fn default() -> Self {
        CsvWriteOptions {
            delimiter: b',',
            has_headers: true,
            terminator: csv::Terminator::CRLF,
            quote_style: csv::QuoteStyle::Necessary,
            quote: b'"',
            escape: b'\\',
            double_quote: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReadContentOptions {
    Csv(CsvReadOptions),
    Json,
    Text,
}
#[derive(Debug, Clone)]
pub enum WriteContentOptions {
    Csv(CsvWriteOptions),
    Json,
    Text,
}
