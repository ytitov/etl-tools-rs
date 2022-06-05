use crate::datastore::error::*;
use crate::datastore::*;
//use crate::transformer::TransformerFut;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
pub struct TransformStream<'a, I, O> {
    input: Box<dyn Producer<'a, I>>,
    output: Box<dyn Consumer<'a, O>>,
    transformer: Box<dyn Tr<'a, I, O>>,
}

impl<'a, I, O> TransformStream<'a, I, O>
where
    I: Send + Sync,
    O: Send + Sync,
{
    pub fn new<DS, DO, TR>(i: DS, o: DO, t: TR) -> Self
    where
        DS: Producer<'a, I>,
        DO: Consumer<'a, O>,
        TR: Tr<'a, I, O>,
    {
        Self {
            input: Box::new(i),
            output: Box::new(o),
            transformer: Box::new(t),
        }
    }
}

pub trait Task<'a>: Sync + Send {
    fn create(self: Box<Self>) -> Result<TaskJoinHandle, DataStoreError>;
}

impl<I, O> OutputTask for TransformStream<'static, I, O> 
where
    I: Send + Sync + 'static,
    O: Debug + Send + Sync + 'static,
{
    fn create(self: Box<Self>) -> Result<TaskJoinHandle, DataStoreError> {
        let input = self.input;
        let output = self.output;
        let mut tr = self.transformer;
        Ok(tokio::spawn(async move {
            let (mut in_rx, in_jh) = input.into_producer()?;
            let (out_tx, out_jh) = output.into_consumer()?;
            while let Some(item_result) = in_rx.recv().await {
                match item_result {
                    Ok(DataSourceMessage::Data {source: _, content: item}) => {
                        match tr.transform(item).await {
                            Ok(result) => {
                                out_tx.send(DataOutputMessage::new(result)).await?;
                            },
                            Err(er) => {
                                log::error!("{}", er);
                            },
                        };
                    },
                    Err(er) => {
                        log::error!("{}", er);
                    }
                };
            }
            drop(out_tx); // graceful close
            in_jh.await??;
            out_jh.await??;
            Ok(TaskOutputDetails::Empty)
        }))
    }
}

pub trait Producer<'a, T: Send>: 'a + Sync + Send {
    fn into_producer(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError>;
}

impl Producer<'_, String> for String {
    fn into_producer(self: Box<Self>) -> Result<DataSourceTask<String>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let jh: DataSourceJoinHandle = tokio::spawn(async move {
            for line in self.lines() {
                tx.send(Ok(DataSourceMessage::new(
                    "String",
                    line.to_string(),
                )))
                .await
                .map_err(|er| DataStoreError::send_error("String", "", er))?;
            }
            Ok(DataSourceDetails::Empty)
        });
        Ok((rx, jh))
    }
}

pub trait Consumer<'a, T: Send>: 'a + Sync + Send {
    fn into_consumer(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError>;
}

impl<T> Consumer<'_, T> for Vec<T> 
where
    T: Send + Sync + Debug + 'static
{
    fn into_consumer(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, mut rx) = channel(1);
        let jh: DataOutputJoinHandle = tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                log::info!("Got item: {:?}", item);
            }
            Ok(DataOutputDetails::Empty)
        });
        Ok((tx, jh))
    }
}

// 'a tells how long the trait object shall live
pub trait Tr<'a, I: Send, O: Send>: 'a + Sync + Send {
    fn transform(
        &mut self,
        _: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>>;
}

// allows for normal closures to be used since transformer is allowed to be async
impl<'a, F, I, O> Tr<'a, I, O> for F 
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

pub fn demo() {
}
