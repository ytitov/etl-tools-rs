pub mod datastore;
/// Used by the [crate::datastore::bytes_source::BytesSource] to help decode various streams.  So
/// anything that implements a BytesSource target, can select which decoder they want to use.  For
/// example either [crate::decoder::csv::CsvDecoder] or [crate::decoder::json::JsonDecoder]
pub mod decoder;
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
