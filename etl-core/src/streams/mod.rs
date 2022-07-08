/// ProducerResultFut and ConsumerResultFut errors are consider to be fatal, and will stop the
/// whole process.  Any "acceptable" errors must be sent through so they can be reported and
/// handled by the appropriate streams like an error queue.
use crate::datastore::*;
use futures::future::BoxFuture;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use log;
use std::error::Error;
use std::fmt::Debug;
use std::pin::Pin;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

pub mod split;
pub mod transformer;

pub type BoxDynError = Box<dyn Error + 'static + Send + Sync>;

pub type ProducerResultFut<'a, O> = BoxFuture<'a, Result<O, Box<dyn Error + 'a + Send + Sync>>>;
pub type ConsumerResultFut<'a, O> = BoxFuture<'a, Result<O, Box<dyn Error + 'a + Send + Sync>>>;

pub trait Producer<'dp, T: Send>: 'dp {
    /// the given sender gets the items produced
    fn start_producer(self: Box<Self>, _: Sender<T>) -> ProducerResultFut<'dp, DataSourceDetails>;
}

#[derive(Debug)]
pub struct ConsumerResultDetails {
    pub num_errors: usize,
    pub num_read: usize,
}

#[derive(Debug)]
pub enum ConsumerResult<D> {
    Details(ConsumerResultDetails),
    WithData {
        data: D,
        details: ConsumerResultDetails,
    },
}

pub trait Consumer<'dp, T: Send, D>: 'dp {
    /// the given receiver sends the items to be consumed
    fn start_consumer(self: Box<Self>, _: Receiver<T>)
        -> ConsumerResultFut<'dp, ConsumerResult<D>>;
}

pub enum StreamMessage<T> {
    Item(T),
    /// Stream is completed but there was an error that was a fatal error
    CompletedError(Box<dyn Error + Sync + Send>),
    /// When converting from a Producer there tokio::spawn is called and if there is a
    /// JoinError which is caused by a panic, this is returned
    JoinError {
        error: JoinError,
        message: String,
    },
}
pub fn producer_stream<I, T>(producer: I) -> impl Stream<Item = StreamMessage<T>>
where
    T: 'static + Sync + Send,
    I: Producer<'static, T> + Sync + Send,
{
    use async_stream::stream;
    use tokio::sync::mpsc::channel;
    let (tx, rx): (Sender<T>, _) = channel(1);
    let jh: JoinHandle<Result<(), BoxDynError>> = tokio::spawn(async move {
        match Box::new(producer).start_producer(tx).await {
            Ok(_) => Ok(()),
            Err(other) => Err(other as Box<dyn Error + Send + Sync>),
        }
    });
    stream! {
        pin_mut!(rx);
        pin_mut!(jh);
        loop {
            match rx.recv().await {
                Some(item) => yield StreamMessage::Item(item),
                None => break,
            }
        }
        match jh.await {
            Ok(Ok(_)) => {
                log::info!("ProducerStream finished successfully");
            },
            Ok(Err(s)) => yield StreamMessage::CompletedError(s),
            Err(je) => {
                log::error!("FAILED joining on producer: {}", &je);
                yield StreamMessage::JoinError { error: je, message: "Failed joining on producer JoinHandle".into() };
            }
        };
        drop(rx);
    }
}

fn crazy_stream<T: Clone>(max: usize, item: T) -> impl Stream<Item = T> {
    use async_stream::stream;
    let count = 0_usize;
    stream! {
        pin_mut!(count);
        loop {
            if (*count >= max) {
                break;
            }
            *count += 1;
            yield item.clone()
        }
    }
}

pub struct ProducerStreamBuilder<'dp, T> {
    stream: Pin<Box<dyn Stream<Item = StreamMessage<T>> + 'dp + Send + Sync>>,
}

impl<'dp, T> ProducerStreamBuilder<'dp, T>
where
    T: Sync + Send,
{
    pub fn from_async_stream<Q>(stream: Q) -> Self
    where
        Q: 'dp + Stream<Item = T> + Unpin + Sync + Send,
    {
        let s = stream.map(|m| StreamMessage::Item(m));
        Self {
            stream: Box::pin(s),
        }
    }

    pub fn producer_to_stream<I>(d: I) -> impl Stream<Item = StreamMessage<T>>
    where
        T: 'static + Sync + Send,
        I: Producer<'static, T> + Sync + Send,
    {
        producer_stream(d)
    }

    pub fn into_stream(self) -> impl Stream<Item = StreamMessage<T>> + 'dp
    where
        T: 'dp,
    {
        self.stream
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl<'dp, T> Producer<'dp, T> for ProducerStreamBuilder<'dp, T>
where
    T: 'dp + Sync + Send + Debug,
{
    fn start_producer(self: Box<Self>, tx: Sender<T>) -> ProducerResultFut<'dp, DataSourceDetails> {
        let mut stream = self.stream;
        Box::pin(async move {
            while let Some(item) = StreamExt::next(&mut stream.as_mut()).await {
                match item {
                    StreamMessage::Item(item) => match tx.send(item).await {
                        Ok(_) => {}
                        Err(send_err) => {
                            return Err(Box::new(send_err) as Box<dyn Error + Send + Sync>);
                        }
                    },
                    StreamMessage::CompletedError(er) => {
                        log::error!("Stream completed with error: {}", er);
                        return Err(er);
                    }
                    StreamMessage::JoinError { error: er, message } => {
                        log::error!("Fatal JoinError: {}", &message);
                        return Err(Box::new(er));
                    }
                }
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

/*
// can't do both, this conflicts with the Stream implementation
impl<'dp, 'item: 'dp, T, Q> Producer<'dp, 'item, T> for Q
where
    T: 'item + Sync + Send + Debug,
    Q: 'dp + std::iter::Iterator<Item = T>,
{
    fn to_sender(self: Box<Self>, tx: Sender<T>) -> BoxFutResult<'dp, DataSourceDetails> {
        //let lines_scanned = 0;
        let itr = self.into_iter();
        Box::pin(async move {
            for item in itr {
                tx.send(item).await?;
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}
*/

impl<'dp, T, Q> Producer<'dp, T> for Q
where
    T: 'dp + Sync + Send + Debug,
    Q: 'dp + futures_core::stream::Stream<Item = T> + Unpin + Sync + Send,
{
    fn start_producer(self: Box<Self>, tx: Sender<T>) -> ProducerResultFut<'dp, DataSourceDetails> {
        //let lines_scanned = 0;
        //let itr = self.into_iter();
        let mut stream = self;
        Box::pin(async move {
            while let Some(item) = StreamExt::next(stream.as_mut()).await {
                match tx.send(item).await {
                    Ok(_) => {}
                    Err(send_err) => {
                        return Err(Box::new(send_err) as Box<dyn Error + Send + Sync>);
                    }
                }
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

impl<'dc, T> Consumer<'dc, T, Vec<T>> for Vec<T>
where
    T: 'dc + Sync + Send + Debug,
{
    fn start_consumer(
        self: Box<Self>,
        mut rx: Receiver<T>,
    ) -> ConsumerResultFut<'dc, ConsumerResult<Vec<T>>> {
        Box::pin(async move {
            let mut items = Vec::new();
            let num_errors = 0;
            let mut num_read = 0;
            while let Some(i) = rx.recv().await {
                log::info!("Vec<T> - {:?}", &i);
                num_read += 1;
                items.push(i);
            }
            Ok(ConsumerResult::WithData {
                data: items,
                details: ConsumerResultDetails {
                    num_errors,
                    num_read,
                },
            })
        })
    }
}

pub async fn run_data_stream<'a, T, I, O, DATA>(
    input: I,
    output: O,
) -> JoinHandle<Result<ConsumerResult<DATA>, BoxDynError>>
where
    T: 'static + Sync + Send + Debug,
    I: Producer<'a, T>,
    DATA: 'static + Send,
    O: Consumer<'static, T, DATA> + Send + Sync,
{
    use tokio::sync::mpsc::channel;
    //use tokio::sync::mpsc::error::SendError;
    //use tokio::task::JoinError;
    let (tx, rx): (Sender<T>, _) = channel(1);
    let jh = tokio::spawn(async move {
        match Box::new(output).start_consumer(rx).await {
            Err(e) => Err(e),
            Ok(r) => Ok(r),
        }
    });
    Box::new(input).start_producer(tx).await.unwrap();
    /*
    match jh.await {
        Ok(Ok(_details)) => _details,
        Ok(Err(_other_fatal_error)) => {
            panic!("run_data_stream encountered an error");
        }
        Err(_join_err) => {
            panic!("run_data_stream encountered a JoinError");
        }
    }
    */
    jh
}

pub async fn test_run_data_stream() {
    run_data_stream(tokio_stream::iter(vec![1, 2, 3, 4]), Vec::new()).await;
}

pub async fn test_run_data_stream2() {
    let s = crazy_stream(10, String::from("blah"));
    pin_mut!(s);
    run_data_stream(s, Vec::new()).await;
}

pub async fn blah() {
    let v = vec![1, 2, 3, 4, 5];
    use tokio::sync::mpsc::channel;
    let (tx, rx) = channel(1);
    let jh = tokio::spawn(async move {
        Box::new(Vec::new())
            .start_consumer(rx)
            .await
            .expect("failed");
    });
    Box::new(tokio_stream::iter(v))
        .start_producer(tx)
        .await
        .unwrap();
    jh.await.unwrap();
}

// can't manage to create a stream
pub async fn demo_stream() {
    use bytes::Bytes;
    use tokio::io::AsyncBufReadExt;
    use tokio_stream::wrappers::LinesStream;
    //use std::io::BufRead;
    let c = Bytes::from(String::from("hi\nline1\nlinefinal"));
    //let v = AsyncBufReadExt::lines(c);
    use tokio::sync::mpsc::channel;
    let (tx, rx) = channel(1);
    let jh = tokio::spawn(async move {
        Box::new(Vec::new())
            .start_consumer(rx)
            .await
            .expect("failed");
    });
    (Box::new(LinesStream::new(c.lines())))
        .start_producer(tx)
        .await
        .unwrap();
    jh.await.unwrap();
}

// demonstrates using the wrapper type, since implementing on a generic limits to basically one
// implementation (can't have multiple implementations on a generic types even with different where
// clauses
pub async fn demo_stream_wrapper() {
    use bytes::Bytes;
    use tokio::io::AsyncBufReadExt;
    use tokio_stream::wrappers::LinesStream;
    //use std::io::BufRead;
    let c = Bytes::from(String::from("hi\nline1\nlinefinal"));
    //let v = AsyncBufReadExt::lines(c);
    use tokio::sync::mpsc::channel;
    let (tx, rx) = channel(1);
    let jh = tokio::spawn(async move {
        Box::new(Vec::new())
            .start_consumer(rx)
            .await
            .expect("failed");
    });
    ProducerStreamBuilder::from_async_stream(LinesStream::new(c.lines()))
        .boxed()
        .start_producer(tx)
        .await
        .unwrap();
    jh.await.unwrap();
}
