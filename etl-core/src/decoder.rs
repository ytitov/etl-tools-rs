/// DecodeStream trait is designed to be used with any BytesSource to decode a CSV. When adding new
/// kinds of DataSources (like S3) one needs to simply implement a stream which produces Bytes
use crate::datastore::error::*;
use crate::datastore::*;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

pub mod csv;
pub mod json;
pub mod string;

pub trait DecodeStream<T: Debug + 'static + Send>: Sync + Send {
    fn decode_source(self, source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<T>>;
}

/// Helper wrapper for specific decoders to return so you do not have to construct them manually
pub struct DecodedSource<T: Debug + 'static + Send + Send> {
    source_name: String,
    ds_task_result: Result<DataSourceTask<T>, DataStoreError>,
}

impl<T: Debug + Send + Sync + 'static> DataSource<T> for DecodedSource<T> {
    fn name(&self) -> String {
        format!("Decoded-{}", &self.source_name)
    }
    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        self.ds_task_result
    }
}
