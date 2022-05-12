use etl_core::datastore::error::DataStoreError;
pub mod proto {
    tonic::include_proto!("datastore");
}

pub mod error {
    use super::*;
    impl From<proto::DataStoreError> for DataStoreError {
        fn from(p_err: proto::DataStoreError) -> DataStoreError {
            use proto::DataStoreError as ProtoErr;
            use proto::DeserializeError;
            use proto::FatalIoError;
            use proto::NotExistError;
            use proto::SendError;
            use proto::TooManyErrorsError;
            match p_err {
                ProtoErr {
                    fatal_io: Some(FatalIoError {}),
                    error,
                    ..
                } => DataStoreError::FatalIO(error),
                ProtoErr {
                    deserialize: Some(DeserializeError { attempted_string }),
                    error,
                    ..
                } => DataStoreError::Deserialize {
                    message: error,
                    attempted_string,
                },
                ProtoErr {
                    not_exist: Some(NotExistError { key }),
                    error,
                    ..
                } => DataStoreError::NotExist { key, error },
                ProtoErr {
                    too_many_errors: Some(TooManyErrorsError {}),
                    ..
                } => DataStoreError::TooManyErrors,
                ProtoErr {
                    send: Some(SendError { from_name, to_name }),
                    error,
                    ..
                } => DataStoreError::SendError {
                    from: from_name,
                    to: to_name,
                    reason: error,
                },
                ProtoErr { error, .. } => DataStoreError::Generic(error),
            }
        }
    }
    impl From<DataStoreError> for proto::DataStoreError {
        fn from(err: DataStoreError) -> Self {
            use proto::DataStoreError as PErr;
            use proto::DecodeError as DecodeE;
            use proto::DeserializeError as DE;
            use proto::FatalIoError as FIO;
            use proto::SendError as SE;
            use DataStoreError::*;
            match err {
                Deserialize {
                    message,
                    attempted_string,
                } => PErr {
                    error: message,
                    deserialize: Some(DE { attempted_string }),
                    ..Default::default()
                },
                FatalUtf8(error) => PErr {
                    error: error.to_string(),
                    decode: Some(DecodeE {
                        attempted_string: "".to_string(),
                    }),
                    ..Default::default()
                },
                FatalIO(error) => PErr {
                    error,
                    fatal_io: Some(FIO {}),
                    ..Default::default()
                },
                SendError { from, to, reason } => PErr {
                    error: reason,
                    send: Some(SE {
                        from_name: from,
                        to_name: to,
                    }),
                    ..Default::default()
                },
                Generic(error) => PErr { error, ..Default::default() },
                e => PErr { error: e.to_string(), ..Default::default() },
            }
        }
    }
}
