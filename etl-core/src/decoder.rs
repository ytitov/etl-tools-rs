/// DecodeStream trait is designed to be used with any BytesSource to decode a CSV. When adding new
/// kinds of DataSources (like S3) one needs to simply implement a stream which produces Bytes
use crate::datastore::error::*;
use crate::datastore::*;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use crate::datastore::bytes_source::*;

pub mod csv;
pub mod json;


#[async_trait]
//pub trait DecodeStream<T: DeserializeOwned + Debug + 'static + Send>: Sync + Send {
pub trait DecodeStream<T: Debug + 'static + Send>: Sync + Send {
    async fn decode_source(
        self: Box<Self>,
        source: Box<dyn BytesSource>,
    ) -> Box<dyn DataSource<T>>;
}

pub struct DecodedSource<T: DeserializeOwned + Debug + 'static + Send + Send> {
    source_name: String,
    ds_task_result: Result<DataSourceTask<T>, DataStoreError>,
}

impl<T: DeserializeOwned + Debug + Send + Sync + 'static> DataSource<T>
    for DecodedSource<T>
{
    fn name(&self) -> String {
        format!("DecodedSource-{}", &self.source_name)
    }
    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        self.ds_task_result
    }
}
