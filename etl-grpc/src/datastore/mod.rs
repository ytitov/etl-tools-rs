use etl_core::datastore::error::DataStoreError;

pub mod error {
    use super::*;
    use crate::proto::etl_grpc::basetypes::ds_error as proto;
    //use etl_core::datastore::error::DataStoreError;
    impl From<proto::GrpcDataStoreError> for DataStoreError {
        fn from(p_err: proto::GrpcDataStoreError) -> DataStoreError {
            use proto::GrpcDataStoreError as ProtoErr;
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
    impl From<DataStoreError> for proto::GrpcDataStoreError {
        fn from(err: DataStoreError) -> Self {
            use proto::GrpcDataStoreError as PErr;
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
