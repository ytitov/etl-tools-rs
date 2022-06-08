/// ProducerResultFut and ConsumerResultFut errors are consider to be fatal, and will stop the
/// whole process.  Any "acceptable" errors must be sent through so they can be reported and
/// handled by the appropriate streams like an error queue.
use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

pub type BoxFut<'a, O> =
    Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>>;

pub type ProducerResultFut<'a, O> =
    Pin<Box<dyn Future<Output = Result<O, Box<dyn Error + 'a>>> + 'a + Send + Sync>>;

pub type ConsumerResultFut<'a, O> =
    Pin<Box<dyn Future<Output = Result<O, Box<dyn Error + Send + Sync>>> + 'a + Send + Sync>>;

pub trait DataProducer<'dp, T: Send>: 'dp {
    /// the given sender gets the items produced
    fn start_producer(self: Box<Self>, _: Sender<T>) -> ProducerResultFut<'dp, DataSourceDetails>;
}

pub trait DataConsumer<'dp, T: Send>: 'dp {
    /// the given sender gets the items produced
    fn start_consumer(self: Box<Self>, _: Receiver<T>)
        -> ConsumerResultFut<'dp, DataSourceDetails>;
}

pub fn producer_stream<I, T>(producer: I) -> impl Stream<Item = T>
where
    T: 'static + Sync + Send,
    I: DataProducer<'static, T> + Sync + Send,
{
    use async_stream::stream;
    use tokio::sync::mpsc::channel;
    let (tx, rx): (Sender<T>, _) = channel(1);
    let jh: JoinHandle<()> = tokio::spawn(async move {
        match Box::new(producer).start_producer(tx).await {
            Err(_e) => (),
            Ok(_) => (),
        }
    });
    stream! {
        pin_mut!(rx);
        pin_mut!(jh);
        loop {
            match rx.recv().await {
                Some(item) => yield item,
                None => break,
            }
        }
        jh.await.unwrap();
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
    stream: Pin<Box<dyn Stream<Item = T> + 'dp + Send + Sync>>,
}

impl<'dp, T> ProducerStreamBuilder<'dp, T>
where
    T: Sync + Send,
{
    pub fn from_async_stream<Q>(stream: Q) -> Self
    where
        Q: 'dp + Stream<Item = T> + Unpin + Sync + Send,
    {
        Self {
            stream: Box::pin(stream),
        }
    }

    pub fn from_producer<I>(d: I) -> Self
    where
        T: 'static + Sync + Send,
        I: DataProducer<'static, T> + Sync + Send,
    {
        Self {
            stream: Box::pin(producer_stream(d)),
        }
    }

    pub fn producer_to_stream<I>(d: I) -> impl Stream<Item = T>
    where
        T: 'static + Sync + Send,
        I: DataProducer<'static, T> + Sync + Send,
    {
        producer_stream(d)
    }

    // can't get around from having to return JoinHandle
    // not sure if I like this, it ends up requiring quite a few extra bounds to error
    // would likely nneed to actually implement the Stream trait that would await on the join
    // handle in the end
    /*
    pub fn from_data_producer<I>(
        producer: I,
    ) -> (
        JoinHandle<Result<(), Box<dyn Error + Send>>>,
        Self,
    )
    where
        T: 'static + Sync + Send + Debug,
        I: DataProducer<'static, T> + Sync + Send,
    {
        use tokio::sync::mpsc::channel;
        use tokio_stream::wrappers::ReceiverStream;
        let (tx, rx): (Sender<T>, _) = channel(1);
        let jh = tokio::spawn(async move {
            match Box::new(producer).start_producer(tx).await {
                Err(e) => Err(e),
                Ok(_) => Ok(()),
            }
        });
        (
            jh,
            Self {
                stream: Box::pin(ReceiverStream::new(rx)),
            },
        )
    }
    */

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl<'dp, T> DataProducer<'dp, T> for ProducerStreamBuilder<'dp, T>
where
    T: 'dp + Sync + Send + Debug,
{
    fn start_producer(self: Box<Self>, tx: Sender<T>) -> ProducerResultFut<'dp, DataSourceDetails> {
        let mut stream = self.stream;
        Box::pin(async move {
            while let Some(item) = StreamExt::next(&mut stream.as_mut()).await {
                match tx.send(item).await {
                    Ok(_) => {}
                    Err(send_err) => {
                        return Err(Box::new(send_err) as Box<dyn Error>);
                    }
                }
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

/*
// can't do both, this conflicts with the Stream implementation
impl<'dp, 'item: 'dp, T, Q> DataProducer<'dp, 'item, T> for Q
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

impl<'dp, T, Q> DataProducer<'dp, T> for Q
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
                        return Err(Box::new(send_err) as Box<dyn Error>);
                    }
                }
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

impl<'dc, T> DataConsumer<'dc, T> for Vec<T>
where
    T: 'dc + Sync + Send + Debug,
{
    fn start_consumer(
        self: Box<Self>,
        mut rx: Receiver<T>,
    ) -> ConsumerResultFut<'dc, DataSourceDetails> {
        Box::pin(async move {
            let mut items = Vec::new();
            while let Some(i) = rx.recv().await {
                items.push(i);
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

pub async fn run_data_stream<'a, T, I, O>(input: I, output: O) -> ()
where
    T: 'static + Sync + Send + Debug,
    I: DataProducer<'a, T>,
    O: DataConsumer<'a, T> + 'static + Send + Sync,
{
    use tokio::sync::mpsc::channel;
    //use tokio::sync::mpsc::error::SendError;
    //use tokio::task::JoinError;
    let (tx, rx): (Sender<T>, _) = channel(1);
    let jh = tokio::spawn(async move {
        match Box::new(output).start_consumer(rx).await {
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    });
    Box::new(input).start_producer(tx).await.unwrap();
    match jh.await {
        Ok(Ok(_details)) => {}
        Ok(Err(_other_fatal_error)) => {}
        Err(_join_err) => {}
    };
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
