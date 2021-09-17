/// DecodeStream trait is designed to be used with any DataSource<Bytes> to decode a CSV.  This is
/// useful in cases where a new DataSource is implemented without having to reimplement repetitive
/// decoders.
use crate::datastore::error::*;
use crate::datastore::*;
use async_trait::async_trait;
use bytes::Bytes;
use serde::de::DeserializeOwned;
//use serde::Serialize;
use std::fmt::Debug;

pub mod csv;

#[async_trait]
pub trait DecodeStream<T: DeserializeOwned + Debug + 'static + Send>: Sync + Send {
    async fn decode_source(
        self: Box<Self>,
        source: Box<dyn DataSource<Bytes>>,
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
