use super::*;
use crate::transformer::TransformerFut;
use tokio::sync::mpsc::channel;

pub struct TransformProducer<'a, I, O> {
    producer: Box<dyn Producer<'a, I> + Send>,
    //transformer: Box<dyn for<'tr> TransformerFut<'tr, I, O> + 'a>,
    transformer: Box<dyn TransformerFut<I, O>>,
}

impl<'a, I, O> TransformProducer<'a, I, O>
where
    I: Send + Sync,
    O: Send + Sync,
{
    pub fn new<P, TR>(i: P, t: TR) -> Self
    where
        P: Producer<'a, I> + Send,
        //TR: for <'tr> TransformerFut<'tr, I, O> + 'a,
        TR: 'static + TransformerFut<I, O>,
    {
        Self {
            producer: Box::new(i),
            transformer: Box::new(t),
        }
    }
}

impl<'a, I, O> Producer<'a, Result<O, BoxDynError>> for TransformProducer<'static, I, O>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static + Debug,
{
    fn start_producer(
        self: Box<Self>,
        tx: Sender<Result<O, BoxDynError>>,
    ) -> ProducerResultFut<'a, DataSourceDetails> {
        let (producer_tx, mut producer_rx): (Sender<I>, _) = channel(1);
        let producer = self.producer;
        let mut t = self.transformer;
        let jh: JoinHandle<Result<_, BoxDynError>> = tokio::spawn(async move {
            match Box::new(producer).start_producer(producer_tx).await {
                Ok(d) => Ok(d),
                Err(other) => Err(other as Box<dyn Error + Send + Sync>),
            }
        });
        Box::pin(async move {
            loop {
                match producer_rx.recv().await {
                    Some(item) => {
                        match t.transform(item).await {
                            Ok(result) => {
                                tx.send(Ok(result)).await?;
                            }
                            Err(er) => {
                                //tx.send(Err(Box::new(er) as BoxDynError)).await?;
                                tx.send(Err(er)).await?;
                            }
                        };
                    }
                    None => break,
                }
            }
            match jh.await {
                Ok(Ok(_details)) => {
                    // _details
                    return Ok(_details);
                }
                Ok(Err(err)) => {
                    return Err(err as BoxDynError);
                }
                Err(_join_err) => {
                    panic!("run_data_stream encountered a JoinError");
                }
            }
        })
    }
}

pub struct TransformConsumer<'a, I, O, Data> {
    consumer: Box<dyn Consumer<'a, O, Data> + Send + Sync>,
    transformer: Box<dyn TransformerFut<I, O>>,
}

impl<'a, I, O, Data> TransformConsumer<'a, I, O, Data>
where
    I: Send + Sync,
    O: Send + Sync,
{
    pub fn new<C, TR>(i: C, t: TR) -> Self
    where
        C: Consumer<'a, O, Data> + Send + Sync,
        TR: 'static + TransformerFut<I, O>,
    {
        Self {
            consumer: Box::new(i),
            transformer: Box::new(t),
        }
    }
}

impl<'a, I, O, Data> Consumer<'a, I, Data> for TransformConsumer<'static, I, O, Data>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static + Debug,
    Data: 'static + Send,
{
    fn start_consumer(
        self: Box<Self>,
        mut rx: Receiver<I>,
    ) -> ConsumerResultFut<'a, ConsumerResult<Data>> {
        let (consumer_tx, consumer_rx): (Sender<O>, _) = channel(1);
        let consumer = self.consumer;
        let mut t = self.transformer;
        let jh: JoinHandle<Result<_, BoxDynError>> = tokio::spawn(async move {
            match Box::new(consumer).start_consumer(consumer_rx).await {
                Ok(d) => Ok(d),
                Err(other) => Err(other as Box<dyn Error + Send + Sync>),
            }
        });
        Box::pin(async move {
            loop {
                match rx.recv().await {
                    Some(item) => {
                        match t.transform(item).await {
                            Ok(result) => {
                                consumer_tx.send(result).await?;
                            }
                            Err(er) => {
                                return Err(er);
                                //tx.send(Err(Box::new(er) as BoxDynError)).await?;
                                //tx.send(Err(er)).await?;
                            }
                        };
                    }
                    None => break,
                }
            }
            match jh.await {
                Ok(Ok(_details)) => {
                    // _details
                    return Ok(_details);
                }
                Ok(Err(err)) => {
                    return Err(err as BoxDynError);
                }
                Err(_join_err) => {
                    panic!("run_data_stream encountered a JoinError");
                }
            }
        })
    }
}
