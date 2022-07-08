use super::*;
use crate::transformer::TransformerFut;
use tokio::sync::mpsc::channel;

pub enum SelectStream<L, R> {
    Left(L),
    Right(R),
}

pub struct SplitStreams<'a, I, L, R, LDATA, RDATA> {
    //producer: Box<dyn Producer<'a, I> + Send>,
    consumer_left: Box<dyn Consumer<'a, L, LDATA> + Send + Sync>,
    consumer_right: Box<dyn Consumer<'a, R, RDATA> + Send + Sync>,
    transformer: Box<dyn TransformerFut<I, SelectStream<L, R>>>,
}

impl<'a, I, L, R, LDATA, RDATA> SplitStreams<'a, I, L, R, LDATA, RDATA>
where
    I: 'a + Sync + Send + Debug,
    L: 'a + Sync + Send + Debug,
    R: 'a + Sync + Send + Debug,
    LDATA: 'a + Send + Sync,
    RDATA: 'a + Send + Sync,
{
    pub fn new<LC, RC, TR>(left: LC, right: RC, t: TR) -> Self
    where
        LC: Consumer<'a, L, LDATA> + Send + Sync,
        RC: Consumer<'a, R, RDATA> + Send + Sync,
        TR: TransformerFut<I, SelectStream<L, R>> + 'static,
    {
        Self {
            consumer_left: Box::new(left),
            consumer_right: Box::new(right),
            transformer: Box::new(t),
        }
    }
}

impl<I, L, R, LDATA, RDATA> Consumer<'static, I, (ConsumerResult<LDATA>, ConsumerResult<RDATA>)>
    for SplitStreams<'static, I, L, R, LDATA, RDATA>
where
    I: 'static + Sync + Send + Debug,
    L: 'static + Sync + Send + Debug,
    R: 'static + Sync + Send + Debug,
    LDATA: 'static + Send + Sync,
    RDATA: 'static + Send + Sync,
{
    fn start_consumer(
        self: Box<Self>,
        mut rx: Receiver<I>,
    ) -> ConsumerResultFut<'static, ConsumerResult<(ConsumerResult<LDATA>, ConsumerResult<RDATA>)>>
    {
        let (left_tx, left_rx): (Sender<L>, _) = channel(1);
        let (right_tx, right_rx): (Sender<R>, _) = channel(1);
        let mut t = self.transformer;
        let left_consumer = self.consumer_left;
        let left_jh = tokio::spawn(async move {
            match Box::new(left_consumer).start_consumer(left_rx).await {
                Ok(r) => Ok(r),
                Err(e) => Err(e as Box<dyn Error + Send + Sync>),
            }
        });
        let right_consumer = self.consumer_right;
        let right_jh = tokio::spawn(async move {
            match Box::new(right_consumer).start_consumer(right_rx).await {
                Ok(r) => Ok(r),
                Err(e) => Err(e as Box<dyn Error + Send + Sync>),
            }
        });
        Box::pin(async move {
            let num_errors = 0;
            let mut num_read = 0;
            while let Some(item) = rx.recv().await {
                num_read += 1;
                match t.transform(item).await {
                    Ok(SelectStream::Left(l_item)) => {
                        left_tx.send(l_item).await?;
                    }
                    Ok(SelectStream::Right(r_item)) => {
                        right_tx.send(r_item).await?;
                    }
                    Err(_er) => {
                        return Err(_er);
                    }
                };
            }
            drop(left_tx);
            drop(right_tx);
            let left_data = left_jh.await??;
            let right_data = right_jh.await??;
            Ok(ConsumerResult::WithData {
                data: (left_data, right_data),
                details: ConsumerResultDetails {
                    num_errors,
                    num_read,
                },
            })
        })
    }
}
