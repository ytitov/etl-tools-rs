pub mod datastore;
pub mod decoder;
pub mod utils;
pub mod preamble {
    pub use crate::datastore::job_runner::JRDataSource;
    pub use crate::job::error::*;
    pub use crate::job::{handler::*, JobRunner, JobRunnerConfig};
    pub use crate::job_manager::{
        JobManager, JobManagerChannel, JobManagerConfig, JobManagerHandle, Message,
    };
    pub use anyhow;
    pub use async_trait::async_trait;
    pub use serde;
    pub use serde_json;
    pub use thiserror;
    pub use tokio;
}
pub mod job;
pub mod job_manager;
pub mod joins;
/// for splitting streams into many identical DataSources
pub mod splitter;
pub mod transformer;
