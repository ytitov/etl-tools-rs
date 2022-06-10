use crate::datastore::error::*;
use crate::datastore::*;
use crate::transformer::*;
pub struct TransformStream<'a, I, O> {
    input: Box<dyn DataSource<'a, I>>,
    output: Box<dyn DataOutput<'a, O>>,
    transformer: Box<dyn TransformerFut<I, O>>,
}

impl<'a, I, O> TransformStream<'a, I, O>
where
    I: Send + Sync,
    O: Send + Sync,
{
    pub fn new<DS, DO, TR>(i: DS, o: DO, t: TR) -> Self
    where
        DS: DataSource<'a, I>,
        DO: DataOutput<'a, O>,
        TR: 'static + TransformerFut<I, O>,
    {
        Self {
            input: Box::new(i),
            output: Box::new(o),
            transformer: Box::new(t),
        }
    }
}

pub trait Task<'a>: 'a + Sync + Send {
    fn create(self: Box<Self>) -> Result<TaskJoinHandle, DataStoreError>;
}

impl<I, O> OutputTask for TransformStream<'static, I, O> 
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn create(self: Box<Self>) -> Result<TaskJoinHandle, DataStoreError> {
        let input = self.input;
        let output = self.output;
        let mut tr = self.transformer;
        Ok(tokio::spawn(async move {
            let (mut in_rx, in_jh) = input.start_stream()?;
            let (out_tx, out_jh) = output.start_stream()?;
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
            let input_result = in_jh.await??;
            let output_result = out_jh.await??;
            use serde_json::json;
            Ok(TaskOutputDetails::WithJson { data: json!({
                "input": input_result,
                "output": output_result,
            })})
        }))
    }
}

