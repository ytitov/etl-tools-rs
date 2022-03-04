//! This library provides a foundation to construct pipelines using several basic principles.
//! There are stream producers and consumers, which must implement the traits [crate::datastore::DataSource]
//! and [crate::datastore::DataOutput].  [crate::job::JobRunner] is used to construct pipelines 
//! using these elements.  It also provides some simple state management which is loaded and
//! saved using the [crate::datastore::SimpleStore] trait which is designed for loading whole
//! files
//! It is important to note that the purpose of this library is to provide a framework to manage
//! and organize pipelines.  It is not meant to replace data anylitical tools or databases.  The
//! main use case for the author is performing 100 G JSON data merges and mapping them to a schema
//! located on an MySql database.

pub mod datastore;
/// Used by the [crate::datastore::DataSource<Bytes>] to help decode various streams.  So
/// anything that implements a BytesSource target, can select which decoder they want to use.  For
/// example either [crate::decoder::csv::CsvDecoder] or [crate::decoder::json::JsonDecoder]
pub mod decoder;
pub mod encoder;
pub mod utils;
/// Adds facilities to implement queues
pub mod queue;
pub mod preamble {
    pub use crate::datastore::transform_store::TransformDataSource;
    pub use crate::job::error::*;
    pub use crate::job::{handler::*, JobRunner, JobRunnerConfig};
    pub use crate::job_manager::{
        JobManager, JobManagerChannel, JobManagerConfig, JobManagerHandle, Message,
    };
}
/// deps which are re-exported (and used in the core)
pub mod deps {
    pub use anyhow;
    pub use async_trait::async_trait;
    pub use serde;
    pub use thiserror;
    pub use tokio;
    pub use log;
    pub use bytes;
    pub use futures_core;
}
/// responsible for generating and running pipelines, including any run-time configurations
pub mod job;
/// Create pipelines linking [crate::datastore::DataSource]s together; Provides state management based on a job id,
/// and records each step.  A successful job with the same job id will not run more than once
pub mod job_manager;
/// Perform joins between two [crate::datastore::DataSource]s
pub mod joins;
/// for splitting streams into many identical DataSources
pub mod splitter;
pub mod transformer;
/// take in a stream and output same items but as batches
pub mod batch;
/// accept a DataSource and execute an async function with a custom state
pub mod apply;
