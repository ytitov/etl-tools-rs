use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use std::fmt::Debug;

pub struct Pipeline<I> {
    pub input: Box<dyn DataSource<I>>,
    pub output: Box<dyn DataOutput<I>>,
}
impl<I: 'static + Debug + Send + Sync> Pipeline<I> {
    pub async fn run(self) -> anyhow::Result<DataOutputStats> {
        let Self {
            input,
            output,
        } = self;
        //let source_name = format!("Pipeline-{}", input.name());
        let (mut input_rx, input_jh) = input.start_stream()?;
        let (output_tx, output_jh) = output.start_stream().await?;
        //let mut lines_scanned = 0_usize;
        //let mut lines_written = 0_usize;
        //let mut num_errors = 0_usize;
        loop {
            match input_rx.recv().await {
                Some(Ok(DataSourceMessage::Data {
                    source: _,
                    content: input_item,
                })) => {
                    //lines_scanned += 1;
                    output_tx.send(DataOutputMessage::new(input_item)).await?;
                    //lines_written += 1;
                }
                Some(Err(val)) => {
                    //lines_scanned += 1;
                    log::error!("Pipeline: {}", val);
                    //num_errors += 1;
                }
                None => break,
            };
        }
        drop(input_rx);
        input_jh.await??;
        drop(output_tx);
        Ok(output_jh.await??)
    }
}

impl<I: 'static + Debug + Send + Sync> OutputTask for Pipeline<I> {
    fn create(self: Box<Self>) -> Result<DataOutputJoinHandle, DataStoreError> {
        Ok(tokio::spawn(async move { Ok(self.run().await?) }))
    }
}
