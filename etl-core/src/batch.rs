use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

pub struct Batcher<I> {
    pub input: Box<dyn DataSource<I>>,
    pub new_batch: fn(&'_ I, &'_ Vec<I>) -> bool,
}

#[async_trait]
impl<I: Debug + Send + Sync + 'static> DataSource<Vec<I>> for Batcher<I> {
    fn name(&self) -> String {
        format!("Batcher-{}", self.input.name())
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<Vec<I>>, DataStoreError> {
        let name = self.name();
        use tokio::sync::mpsc::channel;
        let (tx, rx): (
            _,
            Receiver<Result<DataSourceMessage<Vec<I>>, DataStoreError>>,
        ) = channel(1);
        let (mut input_rx, _) = self.input.start_stream()?;
        let new_batch_func = self.new_batch;
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            let mut batch_vec: Vec<I> = Vec::new();
            let mut source: String = name.clone();
            loop {
                match input_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data {
                        source: s,
                        content: input_item,
                    })) => match new_batch_func(&input_item, &batch_vec) {
                        true => {
                            source = s;
                            lines_scanned += 1;
                            if batch_vec.len() > 0 {
                                tx.send(Ok(DataSourceMessage::new(&source, batch_vec)))
                                    .await
                                    .map_err(|e| DataStoreError::send_error(&name, &source, e))?;
                                batch_vec = Vec::new();
                            }
                            batch_vec.push(input_item);
                        }
                        false => {
                            batch_vec.push(input_item);
                        }
                    },
                    Some(Err(er)) => println!("ERROR: {}", er),
                    None => break,
                };
            }
            if batch_vec.len() > 0 {
                tx.send(Ok(DataSourceMessage::new(&source, batch_vec)))
                    .await
                    .map_err(|e| DataStoreError::send_error(&name, &source, e))?;
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
