use futures::future::BoxFuture;
use crate::datastore::{
    error::DataStoreError, DataSource, DataSourceDetails, DataSourceJoinHandle, DataSourceMessage,
    DataSourceTask,
};
use async_trait::async_trait;
//use std::future::Future;
use std::marker::PhantomData;
//use std::pin::Pin;

// the idea here is that transformer lives at least as long as the input
pub struct TransformSource<'a, I, O> {
    input: Box<dyn DataSource<'a, I>>,
    transformer: Box<dyn TransformerFut<I, O> + 'a>,
}

impl<'a, I, O> TransformSource<'a, I, O>
where
    I: Send + Sync,
    O: Send + Sync,
{
    pub fn new<DS, TR>(i: DS, t: TR) -> Self
    where
        DS: DataSource<'a, I>,
        TR: TransformerFut<I, O> + 'a,
    {
        Self {
            input: Box::new(i),
            transformer: Box::new(t),
        }
    }
}

impl<'a, I, O> DataSource<'a, O> for TransformSource<'static, I, O>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn name(&self) -> String {
        "TransformSource".into()
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let (mut input_rx, input_jh) = self.input.start_stream()?;
        //let t: Box<dyn for<'b> TransformerFut<'b, I, O> + 'a>  = self.transformer.into();
        let mut t = self.transformer;
        let jh: DataSourceJoinHandle = tokio::spawn(async move {
            loop {
                match input_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data { content, source })) => {
                        match t.transform(content).await {
                            Ok(result) => {
                                tx.send(Ok(DataSourceMessage::Data {
                                    content: result,
                                    source,
                                }))
                                .await
                                .map_err(|er| DataStoreError::FatalIO(er.to_string()))?;
                            }
                            Err(er) => {
                                tx.send(Err(er))
                                    .await
                                    .map_err(|er| DataStoreError::FatalIO(er.to_string()))?;
                            }
                        };
                    }
                    Some(Err(er)) => {
                        log::error!("TransformSource got an error from source: {}", er);
                        tx.send(Err(er))
                            .await
                            .map_err(|er| DataStoreError::FatalIO(er.to_string()))?;
                    }
                    None => break,
                }
            }
            drop(input_rx);
            input_jh.await??;
            /*
            tx.send(Ok(DataSourceMessage::new("String", line.to_string())))
                .await
                .map_err(|er| DataStoreError::send_error("String", "", er))?;
            */
            Ok(DataSourceDetails::Empty)
        });
        Ok((rx, jh))
    }
}

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

pub trait TransformerBuilder<'a, I, O>: 'a + Send + Sync {
    fn build(&self) -> Box<dyn for<'b> TransformerFut<I, O>> {
        panic!("HAHA you need to implement this fool");
    }
}

pub trait TransformerFut<I: Send, O: Send>: Sync + Send {
    /*
    fn transform(
        &mut self,
        _: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>>;
    */
    fn transform(
        &mut self,
        _: I,
    ) -> BoxFuture<'_, Result<O, DataStoreError>>;
}

pub struct TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError>,
{
    f: F,
    p: PhantomData<(I, O)>,
}

impl<I, O, F> Clone for TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError> + Clone + Send + Sync,
{
    fn clone(&self) -> Self {
        Self { f: Clone::clone(&self.f), p: self.p.clone() }
    }
}

// could be better done with an Arc possibly?
impl<I, O, F> TransformerBuilder<'static, I, O> for TransformFunc<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, DataStoreError> + 'static + Clone + Send + Sync,
    Self: TransformerFut<I, O>,
{
    //fn build(&self) -> Self {
    //fn build(&self) -> Box<dyn for<'b> TransformerFut<'b, I, O>> {
    fn build(&self) -> Box<dyn TransformerFut<I, O>> {
        Box::new(self.clone()) as Box<dyn TransformerFut<I, O>>
    }
}

/*
impl<'a, I, O> TransformerBuilder<'a, I, O> for dyn Fn(I) -> Result<O, DataStoreError>
where
    I: Sync + Send + 'static,
    O: Sync + Send + 'static,
    Self: Send + Sync + Clone + 'static,

{
    fn build(&self) -> Box<dyn for<'b> TransformerFut<'b, I, O>> {
        //Box::new(Clone::clone(self))// as Box<dyn for<'b> TransformerFut<'b, I, O>>
        Box::new(TransformFunc::new(&self)) as Box<dyn for<'b> TransformerFut<'b, I, O> + 'static>
    }
}
*/

impl<I, O, F> TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError>,
{
    pub fn new(f: F) -> Self {
        Self { p: PhantomData, f }
    }
}

impl<'a, F, I, O> TransformerFut<I, O> for TransformFunc<I, O, F>
where
    F: Fn(I) -> Result<O, DataStoreError> + 'a + Send + Sync,
    I: 'a + Send + Sync,
    O: 'a + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    //) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + Send + Sync + 'a>> {
    ) -> BoxFuture<'_, Result<O, DataStoreError>> {
        let result = (self.f)(input);
        Box::pin(async { result })
    }
}

pub struct TransformFuncIdx<I, O, F>
where
    F: Fn(usize, I) -> Result<O, DataStoreError>,
{
    f: F,
    idx: usize,
    p: PhantomData<(I, O)>,
}

impl<I, O, F> TransformFuncIdx<I, O, F>
where
    F: Fn(usize, I) -> Result<O, DataStoreError>,
{
    pub fn new(f: F) -> Self {
        Self {
            idx: 0,
            p: PhantomData,
            f,
        }
    }
}

impl<'a, F, I, O> TransformerFut<I, O> for TransformFuncIdx<I, O, F>
where
    F: Fn(usize, I) -> Result<O, DataStoreError> + 'a + Send + Sync,
    I: 'a + Send + Sync,
    O: 'a + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    //) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + Send + Sync + 'a>> {
    ) -> BoxFuture<'_, Result<O, DataStoreError>> {
        let result = (self.f)(self.idx, input);
        self.idx += 1;
        Box::pin(async { result })
    }
}

impl<'a, F, I, O> TransformerFut<I, O> for F
where
    F: Fn(I) -> Result<O, DataStoreError> + 'a + Send + Sync,
    I: 'a + Send + Sync,
    O: 'static + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    //) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + Send + Sync + 'a>> {
    ) -> BoxFuture<'_, Result<O, DataStoreError>> {
        let result = (self)(input);
        Box::pin(async { result })
    }
}
