use self::error::*;
//use crate::job_manager::JobManagerTx;
//use crate::preamble::*;
use async_trait::async_trait;
use serde::de::Deserializer;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

//pub mod bytes_source;
/// creates generated data sources
pub mod enumerate;
/// Local file system data stores
pub mod fs;
/// various data stores used for testing
pub mod mock;
pub mod sources;
/// Traits that define non-streaming loading and saving of data
pub mod simple;
pub mod error;

//pub type DataOutputItemResult = Result<String, Box<dyn std::error::Error + Send>>;

pub type DataOutputItemResult<T> = Result<T, DataStoreError>;

pub type DataSourceRx<T> = Receiver<Result<DataSourceMessage<T>, DataStoreError>>;
pub type DataSourceTx<T> = Sender<DataSourceMessage<T>>;
pub type DataSourceTask<T> = (
    DataSourceRx<T>,
    JoinHandle<Result<DataSourceStats, DataStoreError>>,
);
pub type DataOutputTx<T> = Sender<DataOutputMessage<T>>;
pub type DataOutputRx<T> = Receiver<DataOutputMessage<T>>;
pub type DataOutputTask<T> = (DataOutputTx<T>, JoinHandle<anyhow::Result<DataOutputStats>>);

pub type DataOutputJoinHandle = JoinHandle<anyhow::Result<DataOutputStats>>;
pub type DataSourceJoinHandle = JoinHandle<Result<DataSourceStats, DataStoreError>>;

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

pub trait OutputTask: Sync + Send {
    fn create(self: Box<Self>) -> Result<DataOutputJoinHandle, DataStoreError>;
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
    async fn start_stream(self: Box<Self>) -> anyhow::Result<DataOutputTask<T>>;

    async fn data_output_shutdown(
        self: Box<Self>,
        dot: DataOutputTask<T>,
    ) -> anyhow::Result<DataOutputStats> {
        let (c, jh) = dot;
        drop(c);
        match jh.await {
            Ok(Ok(stats)) => Ok(stats),
            Ok(Err(e)) => {
                log::error!("Waiting for task to finish resulted in an error: {}", e);
                Err(anyhow::anyhow!(e))
            }
            Err(e) => {
                log::error!("FATAL waiting on JoinHandle for DbOutputTask due to: {}", e);
                Err(anyhow::anyhow!(
                "FATAL waiting on JoinHandle for DbOutputTask due to: {}",
                e
            ))
            },
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

#[async_trait]
impl<T: Debug + 'static + Sync + Send> DataSource<T> for Receiver<DataSourceMessage<T>> {
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
                    Some(DataSourceMessage::Data {source, content}) => {
                        lines_scanned += 1;
                        tx.send(Ok(DataSourceMessage::new(&name, content)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &source, e))?;
                    }
                    None => break,
                }
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
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
