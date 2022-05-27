use crate::proto::etl_grpc::basetypes::simplestore_error::{
    GrpcSimpleStoreError, IoError, NotExistError,
};
use etl_core::datastore::error::DataStoreError;

impl From<DataStoreError> for GrpcSimpleStoreError {
    fn from(ds: DataStoreError) -> Self {
        let defaults = GrpcSimpleStoreError::default();
        match ds {
            DataStoreError::NotExist { key, error } => GrpcSimpleStoreError {
                error,
                not_exist: Some(NotExistError { key: key.clone() }),
                ..defaults
            },
            others => GrpcSimpleStoreError {
                error: others.to_string(),
                fatal_io: Some(IoError {}),
                ..defaults
            },
        }
    }
}

impl Into<DataStoreError> for GrpcSimpleStoreError {
    fn into(self) -> DataStoreError {
        let defaults = GrpcSimpleStoreError::default();
        match self {
            GrpcSimpleStoreError {
                error,
                not_exist: Some(NotExistError { key }),
                ..
            } => DataStoreError::NotExist { key, error },
            GrpcSimpleStoreError {
                error,
                fatal_io: Some(IoError {}),
                ..
            } => DataStoreError::FatalIO(error),
            _ => DataStoreError::FatalIO(
                "Got a fatal error from grpc SimpleStore but it was not structured properly".into(),
            ),
        }
    }
}
