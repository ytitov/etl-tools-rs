use crate::datastore::{
    error::DataStoreError,
    DataSource, DataSourceDetails, DataSourceJoinHandle, DataSourceMessage, DataSourceTask,
};
use async_trait::async_trait;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

pub struct TransformSource<'a, I, O> {
    input: Box<dyn DataSource<'a, I>>,
    transformer: Box<dyn TransformerFut<'static, I, O>>,
}

impl<'a, 'b, I, O> TransformSource<'a, I, O>
where
    I: Send + Sync,
    O: Send + Sync,
{
    pub fn new<DS, TR>(i: DS, t: TR) -> Self
    where
        DS: DataSource<'a, I>,
        TR: TransformerFut<'static, I, O>,
    {
        Self {
            input: Box::new(i),
            transformer: Box::new(t),
        }
    }
}

impl<'a, I, O> DataSource<'a, O> for TransformSource<'a, I, O>
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

pub trait TransformerFut<'a, I: Send, O: Send>: 'a + Sync + Send {
    fn transform(
        &mut self,
        _: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>>;
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
    F: Fn(I) -> Result<O, DataStoreError> + 'a + Send + Sync,
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
        Self { idx: 0, p: PhantomData, f }
    }
}

impl<'a, F, I, O> TransformerFut<'a, I, O> for TransformFuncIdx<I, O, F>
where
    F: Fn(usize, I) -> Result<O, DataStoreError> + 'a + Send + Sync,
    I: 'a + Send + Sync,
    O: 'a + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + Send + Sync + 'a>> {
        let result = (self.f)(self.idx, input);
        self.idx += 1;
        Box::pin(async { result })
    }
}

impl<'a, F, I, O> TransformerFut<'a, I, O> for F 
where
    F: Fn(I) -> Result<O, DataStoreError> + 'a + Send + Sync,
    I: 'a + Send + Sync,
    O: 'a + Send + Sync,
{
    fn transform(
        &mut self,
        input: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + Send + Sync + 'a>> {
        let result = (self)(input);
        Box::pin(async { result })
    }
}
