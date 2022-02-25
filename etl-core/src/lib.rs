pub mod datastore;
pub mod decoder;
pub mod utils;
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
pub mod job;
/// Create pipelines linking [crate::datastore::DataSource]s together; Provides state management based on a job id,
/// and records each step.  A successful job with the same job id will not run more than once
pub mod job_manager;
/// Perform joins between two [crate::datastore::DataSource]s
pub mod joins;
/// for splitting streams into many identical DataSources
pub mod splitter;
pub mod transformer;
