use super::*;
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
