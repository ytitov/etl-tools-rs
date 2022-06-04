use crate::datastore::error::DataStoreError;
use async_trait::async_trait;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

#[async_trait]
pub trait Transformer<'a, I: 'a + Send, O: 'a + Send>: Sync + Send {
    async fn transform(&mut self, _: I) -> Result<O, DataStoreError>;
}

#[async_trait]
impl<'a, I: 'a + Send, O: 'a + Send> Transformer<'a, I, O>
    for Box<dyn Fn(I) -> Result<O, DataStoreError> + Sync + Send>
{
    async fn transform(&mut self, input: I) -> Result<O, DataStoreError> {
        (self)(input)
    }
}

pub trait TransformerFut<'a, I: 'a + Send, O: 'a + Send>: Sync + Send {
    fn transform(
        &mut self,
        _: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>>;
}

impl<'a, I, O> TransformerFut<'a, I, O>
    for Box<dyn Fn(I) -> Result<O, DataStoreError> + Sync + Send>
where
    I: 'a + Send + Sync,
    O: 'a + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>> {
        let result = (self)(input);
        Box::pin(async { result })
    }
}

pub struct TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError>,
{
    f: F,
    p: PhantomData<(I, O)>,
}

impl<I, O, F> TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError>,
{
    pub fn new(f: F) -> Self {
        Self { p: PhantomData, f }
    }
}

impl<'a, F, I, O> TransformerFut<'a, I, O> for TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError> + Send + Sync,
    I: 'a + Send + Sync,
    O: 'a + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + Send + Sync + 'a>> {
        let result = (self.f)(input);
        Box::pin(async { result })
    }
}
