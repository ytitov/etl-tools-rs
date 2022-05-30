use async_trait::async_trait;
use crate::datastore::error::DataStoreError;

#[async_trait]
pub trait Transformer<'a, I: 'a + Send, O: 'a + Send>: Sync + Send {
    async fn transform(&mut self, _: I) -> Result<O, DataStoreError>;
}
