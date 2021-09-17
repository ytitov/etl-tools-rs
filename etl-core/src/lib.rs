pub mod datastore;
pub mod utils;
pub mod decoder;
pub mod preamble {
    pub use crate::datastore::job_runner::JRDataSource;
    pub use crate::job::error::*;
    pub use crate::job::{handler::*, JobRunner, JobRunnerConfig};
    pub use crate::job_manager::{JobManager, JobManagerChannel, JobManagerConfig, Message};
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
pub mod transformer;
/// for splitting streams into many identical DataSources
pub mod splitter;
