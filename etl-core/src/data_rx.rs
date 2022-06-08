use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use futures_core::stream::Stream;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

pub type BoxFut<'a, O> =
    Pin<Box<dyn Future<Output = Result<O, DataStoreError>> + 'a + Send + Sync>>;

pub type BoxFutResult<'a, O> =
    Pin<Box<dyn Future<Output = Result<O, Box<dyn std::error::Error + 'a>>> + 'a + Send + Sync>>;

pub struct DPStream<'dp, T> {
    stream: Pin<Box<dyn Stream<Item = T> + 'dp + Send + Sync>>,
}

/// 'dp is the lifetime of the data producer, and 'item is each element that it produces.  The
/// idea is that the 'item may live longer than the producer, since once it stops producing it
/// should be able to go away freely.
pub trait DataProducer<'dp, 'item: 'dp, T: 'item + Send>: 'dp {
    /// the given sender gets the items produced
    fn start_producer(
        self: Box<Self>,
        _: Sender<T>,
        //) -> BoxFut<'dp, DataSourceDetails>;
    ) -> BoxFutResult<'dp, DataSourceDetails>;
}

pub trait DataConsumer<'dp, 'item: 'dp, T: 'item + Send>: 'dp {
    /// the given sender gets the items produced
    fn start_consumer(self: Box<Self>, _: Receiver<T>) -> BoxFutResult<'dp, DataSourceDetails>;
}

pub trait Named {
    fn name(&self) -> String;
}

impl<'dp, 'item: 'dp, T> DPStream<'dp, T>
where
    T: 'item + Sync + Send + Debug,
{
    pub fn from_async_stream<Q>(stream: Q) -> Self
    where
        Q: 'dp + futures_core::stream::Stream<Item = T> + Unpin + Sync + Send,
    {
        Self {
            stream: Box::pin(stream),
        }
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl<'dp, 'item: 'dp, T> DataProducer<'dp, 'item, T> for DPStream<'dp, T>
where
    T: 'item + Sync + Send + Debug,
{
    fn start_producer(self: Box<Self>, tx: Sender<T>) -> BoxFutResult<'dp, DataSourceDetails> {
        //let lines_scanned = 0;
        //let itr = self.into_iter();
        let mut stream = self.stream;
        Box::pin(async move {
            while let Some(item) = StreamExt::next(&mut stream.as_mut()).await {
                match tx.send(item).await {
                    Ok(_) => {}
                    Err(_val) => {}
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

impl<'dp, 'item: 'dp, T, Q> DataProducer<'dp, 'item, T> for Q
where
    T: 'item + Sync + Send + Debug,
    Q: 'dp + futures_core::stream::Stream<Item = T> + Unpin + Sync + Send,
{
    fn start_producer(self: Box<Self>, tx: Sender<T>) -> BoxFutResult<'dp, DataSourceDetails> {
        //let lines_scanned = 0;
        //let itr = self.into_iter();
        let mut stream = self;
        Box::pin(async move {
            while let Some(item) = StreamExt::next(stream.as_mut()).await {
                match tx.send(item).await {
                    Ok(_) => {}
                    Err(_val) => {}
                }
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

impl<'dc, 'item: 'dc, T> DataConsumer<'dc, 'item, T> for Vec<T>
where
    T: 'item + Sync + Send + Debug,
{
    fn start_consumer(
        self: Box<Self>,
        mut rx: Receiver<T>,
    ) -> BoxFutResult<'dc, DataSourceDetails> {
        Box::pin(async move {
            let mut items = Vec::new();
            while let Some(i) = rx.recv().await {
                items.push(i);
            }
            Ok(DataSourceDetails::Empty)
        })
    }
}

// TODO: need to chance the return type to Future, it can't figure out lifetimes.  Thats why it is
// asking for a 'static lifetime
pub async fn run_data_stream<'a, T, I, O>(input: I, output: O) -> ()
where
    T: 'static + Sync + Send + Debug,
    I: DataProducer<'a, 'a, T>,
    O: DataConsumer<'a, 'a, T> + 'static + Send + Sync,
{
    use tokio::sync::mpsc::channel;
    let (tx, rx): (Sender<T>, _) = channel(1);
    let jh = tokio::spawn(async move {
        Box::new(output).start_consumer(rx).await.expect("failed");
    });
    Box::new(input).start_producer(tx).await.unwrap();
    jh.await.unwrap();
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
    DPStream::from_async_stream(LinesStream::new(c.lines()))
        .boxed()
        .start_producer(tx)
        .await
        .unwrap();
    jh.await.unwrap();
}
