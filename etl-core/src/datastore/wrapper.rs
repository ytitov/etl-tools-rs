use super::*;
use tokio::sync::mpsc::channel;

pub enum MessageWrapper<O>
where
    O: Send + Sync,
{
    Skip,
    Item(O),
    List(Vec<O>),
}

pub struct Unwrap<'a, O>
where
    O: Send + Sync,
{
    output: Box<dyn DataOutput<'a, O>>,
}

impl<'a, O> Unwrap<'a, O>
where
    O: Send + Sync,
{
    pub fn unwrap_dataoutput<DO>(out: DO) -> Self
    where
        DO: DataOutput<'a, O>,
    {
        Self {
            output: Box::new(out),
        }
    }
}

impl<'a, O> DataOutput<'a, MessageWrapper<O>> for Unwrap<'a, O>
where
    O: Send + Sync + 'static,
{
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<MessageWrapper<O>>, DataStoreError> {
        let (out_tx, bytes_out_jh) = self.output.start_stream()?;
        let (tx, mut rx): (DataOutputTx<MessageWrapper<O>>, _) = channel(1);
        let jh: DataOutputJoinHandle = tokio::spawn(async move {
            let mut lines_written = 0_usize;
            loop {
                match rx.recv().await {
                    Some(DataOutputMessage::Data(MessageWrapper::Skip)) => {
                    }
                    Some(DataOutputMessage::Data(MessageWrapper::Item(data))) => {
                        lines_written += 1;
                        out_tx.send(DataOutputMessage::new(data)).await?;
                    }
                    Some(DataOutputMessage::Data(MessageWrapper::List(elems))) => {
                        for data in elems {
                            out_tx.send(DataOutputMessage::new(data)).await?;
                            lines_written += 1;
                        }
                    }
                    None => break,
                    _ => break,
                };
            }
            drop(out_tx);
            bytes_out_jh.await??;
            Ok(DataOutputDetails::Basic { lines_written })
        });
        Ok((tx, jh))
    }
}
