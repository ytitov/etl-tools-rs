use self::error::*;
use async_trait::async_trait;
use serde::de::Deserializer;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fmt::Debug;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::{Receiver as OneShotRx, Sender as OneShotTx};
use tokio::task::JoinHandle;

//pub mod bytes_source;
/// creates generated data sources
pub mod enumerate;
pub mod error;
/// Local file system data stores
pub mod fs;
/// various data stores used for testing
pub mod mock;
pub mod sources;
pub mod outputs;
/// wrapper for values for cases when multiple values are returned
pub mod wrapper;

pub type CallbackRx<T> = OneShotRx<Result<T, DataStoreError>>;
pub type CallbackTx<T> = OneShotTx<Result<T, DataStoreError>>;
pub type CallbackChannel<T> = (CallbackTx<T>, CallbackRx<T>);

//pub type DataOutputItemResult = Result<String, Box<dyn std::error::Error + Send>>;

pub type DataOutputItemResult<T> = Result<T, DataStoreError>;

pub type DataSourceRx<T> = Receiver<Result<DataSourceMessage<T>, DataStoreError>>;
pub type DataSourceTx<T> = Sender<DataSourceMessage<T>>;
pub type DataSourceTask<T> = (DataSourceRx<T>, DataSourceJoinHandle);
pub type DataOutputTx<T> = Sender<DataOutputMessage<T>>;
pub type DataOutputRx<T> = Receiver<DataOutputMessage<T>>;
pub type DataOutputTask<T> = (DataOutputTx<T>, DataOutputJoinHandle);

pub type DataOutputJoinHandle = JoinHandle<Result<DataOutputDetails, DataStoreError>>;
pub type DataSourceJoinHandle = JoinHandle<Result<DataSourceDetails, DataStoreError>>;
pub type TaskJoinHandle = JoinHandle<Result<TaskOutputDetails, DataStoreError>>;
pub type BoxedDataSource<'a, T> = Box<dyn DataSource<'a, T>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceDetails {
    Empty,
    Basic {
        lines_scanned: usize,
    },
    WithJson {
        lines_scanned: usize,
        data: JsonValue,
    },
}

impl From<()> for DataSourceDetails {
    fn from(_: ()) -> Self {
        DataSourceDetails::Empty
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataOutputDetails {
    Empty,
    Basic {
        lines_written: usize,
    },
    WithJson {
        lines_written: usize,
        data: JsonValue,
    },
}

impl From<()> for DataOutputDetails {
    fn from(_: ()) -> Self {
        DataOutputDetails::Empty
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TaskOutputDetails {
    Empty,
    WithJson { data: JsonValue },
}

impl From<()> for TaskOutputDetails {
    fn from(_: ()) -> Self {
        TaskOutputDetails::Empty
    }
}

impl From<JsonValue> for TaskOutputDetails {
    fn from(data: JsonValue) -> Self {
        TaskOutputDetails::WithJson { data }
    }
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
pub enum DataOutputMessage<T: Send> {
    Data(T),
    NoMoreData,
}

impl<T: Send + Sync> DataOutputMessage<T> {
    pub fn new(data: T) -> Self {
        DataOutputMessage::Data(data)
    }
    pub fn no_more_data() -> Self {
        DataOutputMessage::NoMoreData
    }
}

pub trait OutputTask: Sync + Send {
    fn create(self: Box<Self>) -> Result<TaskJoinHandle, DataStoreError>;
}

pub trait DataSource<'a, T: Send>: 'a + Sync + Send {
    fn name(&self) -> String;

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError>;
}

impl<'a, T: 'a + Send> DataSource<'a, T> for DataSourceTask<T> {
    fn name(&self) -> String {
        String::from("DataSourceTask")
    }
    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        Ok(*self)
    }
}

/// Helps with creating DataOutput's during run-time.
#[async_trait]
pub trait CreateDataOutput<'de, 'a, C, T>: Sync + Send
where
    C: Deserializer<'de>,
    T: 'static + Send,
{
    async fn create_data_output(_: C) -> anyhow::Result<Box<dyn DataOutput<'a, T>>>;
}

/// Helps with creating DataSource's during run-time.
#[async_trait]
pub trait CreateDataSource<'de, C, T>: Sync + Send
where
    C: Deserializer<'de>,
    T: Send,
{
    async fn create_data_source<'a>(_: C) -> anyhow::Result<Box<dyn DataSource<'a, T>>>;
}

pub trait DataOutput<'a, T: Send>: 'a + Sync + Send {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError>;

    /*
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
    */
}

#[async_trait]
impl<T: Debug + 'static + Sync + Send> DataSource<'_, T> for DataSourceRx<T> {
    fn name(&self) -> String {
        String::from("DataSourceRx")
    }

    fn start_stream(mut self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let name = self.name();
        let jh: DataSourceJoinHandle = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                match self.recv().await {
                    Some(Ok(DataSourceMessage::Data { source, content })) => {
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
            Ok(DataSourceDetails::Basic { lines_scanned })
        });
        Ok((rx, jh))
    }
}

#[async_trait]
impl<T> DataSource<'_, T> for Receiver<DataSourceMessage<T>>
where
    T: Send + Sync + Debug + 'static,
{
    fn name(&self) -> String {
        String::from("DataSourceRx")
    }

    fn start_stream(mut self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let name = self.name();
        let jh: DataSourceJoinHandle = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                match self.recv().await {
                    Some(DataSourceMessage::Data { source, content }) => {
                        lines_scanned += 1;
                        tx.send(Ok(DataSourceMessage::new(&name, content)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &source, e))?;
                    }
                    None => break,
                }
            }
            Ok(DataSourceDetails::Basic { lines_scanned })
        });
        Ok((rx, jh))
    }
}

pub struct DynDataSource<'a, T> {
    pub ds: Box<dyn DataSource<'a, T>>,
}

impl<'a, T: Debug + Send + 'static> DynDataSource<'a, T> {
    pub fn new<C: DataSource<'a, T> + 'static>(t: C) -> Self {
        DynDataSource { ds: Box::new(t) }
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
