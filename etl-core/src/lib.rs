//! This library provides a foundation to construct pipelines using several basic principles.
//! There are stream producers and consumers, which must implement the traits [crate::datastore::DataSource]
//! and [crate::datastore::DataOutput].  etl_job::job::JobRunner is used to construct pipelines 
//! using these elements.  It also provides some simple state management which is loaded and
//! saved using the [crate::datastore::simple::SimpleStore] trait which is designed for loading whole
//! files
//! It is important to note that the purpose of this library is to provide a framework to manage
//! and organize pipelines.  It is not meant to replace data anylitical tools or databases.  The
//! main use case for the author is performing 100 G JSON data merges and mapping them to a schema
//! located on an MySql database.

pub mod datastore;
/// Rework of the datastore module instead to focus on the async Stream
pub mod streams;
/// Used by the [crate::datastore::DataSource<Bytes>] to help decode various streams.  So
/// anything that implements a BytesSource target, can select which decoder they want to use.  For
/// example either [crate::decoder::csv::CsvDecoder] or [crate::decoder::json::JsonDecoder]
pub mod decoder;
pub mod encoder;
pub mod utils;
pub mod task;
/// Adds facilities to implement queues
pub mod queue;
pub mod preamble {
    //pub use crate::datastore::transform_store::TransformDataSource;
}
/// deps which are re-exported (and used in the core)
pub mod deps {
    pub use anyhow;
    pub use async_trait::async_trait;
    pub use serde;
    pub use serde_json;
    pub use thiserror;
    pub use tokio;
    pub use log;
    pub use bytes;
    pub use futures_core;
    pub use chrono;
}
/// Perform joins between two [crate::datastore::DataSource]s
pub mod joins;
/// for splitting streams into many identical DataSources
pub mod splitter;
pub mod transformer;
/// take in a stream and output same items but as batches
pub mod batch;
/// key-value type storage.  Provides a simple interface over any storage using strings as keys
pub mod keystore;
