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
    //TODO: this should be DecodeError
    #[error("Could not decode utf8: `{0}`")]
    FatalUtf8(#[from] std::str::Utf8Error),
    #[error("Connection/Transport problem due to: `{0}`")]
    FatalIO(String),
    #[error("Transport failed due to `{message}` caused by: `{caused_by:?}`")]
    Transport {
        message: String,
        //can't use this because of clone
        //caused_by: Box<dyn std::error::Error + Send + Sync + 'static>,
        caused_by: String,
    },
    #[error("SendError from `{from:?}` to `{to:?}` reason: `{reason:?}`")]
    SendError {
        from: String,
        to: String,
        reason: String,
    },
    //TODO; also IO error
    #[error("Error streaming lines from: `{key:?}`, error: `{error:?}`")]
    StreamingLines { key: String, error: String },
    #[error("Key or path `{key:?}` was not found.  Reason: `{error:?}`")]
    NotExist { key: String, error: String },
    // TODO: should be removed since this has nothing to do with data storage
    #[error("Error returned from transform_item `{job_name:?}`.  Reason: `{error:?}`")]
    TransformerError { job_name: String, error: String },
    // TODO: id say this is more of an FatalIO
    #[error("JoinError: `{0}`")]
    JoinError(String),
    #[error("Shutting down.  JobManager sent a global TooManyErrors message.")]
    TooManyErrors,
    #[error("Error: `{0}`")]
    Generic(String),
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

    pub fn transport<M,E>(m: M, e: E) -> Self
        where
            M: ToString,
            E: std::error::Error
    {
        DataStoreError::Transport { message: m.to_string(), caused_by: e.to_string() }
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

impl From<&str> for DataStoreError {
    fn from(er: &str) -> Self {
        DataStoreError::Generic(er.to_string())
    }
}

/*
 * this might be a bad idea
impl From<serde_json::Error> for DataStoreError {
    fn from(er: serde_json::Error) -> Self {
        DataStoreError::FatalIO(er.to_string())
    }
}
*/

type DataOutputMessageSendError<T> = tokio::sync::mpsc::error::SendError<DataOutputMessage<T>>;
impl<T> From<DataOutputMessageSendError<T>> for DataStoreError 
where T: Debug + Sync + Send
{
    fn from(er: DataOutputMessageSendError<T>) -> Self {
        DataStoreError::SendError {
            from: "Unknown".into(),
            to: "mpsc::Sender".into(),
            reason: format!("SendError.  Content: {:?}", er.0)
        }
    }
}
