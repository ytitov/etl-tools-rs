use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use crate::preamble::*;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

pub struct Transformer<I, O> {
    pub input: Box<dyn DataSource<I>>,
    //pub output: Box<dyn DataSource<O>>,
    //pub map: Box<dyn Fn(I) -> DataOutputItemResult<O> + Send + Sync + 'static>,
    pub map: fn(I) -> DataOutputItemResult<O>,
}
/*
 * started doing some work on an async version but decided to stop and finish
 * this at another time.  The simple mapper should suffice because can do these
 * final modifications at the job handler trait
pub struct Transformer<I, O> {
    pub input: Box<dyn DataSource<I>>,
    //pub output: Box<dyn DataSource<O>>,
    //pub map: Box<dyn Fn(I) -> DataOutputItemResult<O> + Send + Sync + 'static>,
    //pub map: fn(I) -> DataOutputItemResult<O>,
    pub map: Box<dyn MapItem<I, O> + Send + Sync>,
}

#[async_trait]
pub trait MapItem<I: 'static + Send, O: Send>: Sync + Send {
    async fn map(&self, _: I) -> DataOutputItemResult<O> {
        unimplemented!();
    }
}

#[async_trait]
impl<I: 'static + Send, O: Send> MapItem<I, O>
    for Box<dyn Fn(I) -> DataOutputItemResult<O> + Send + Sync>
{
    async fn map(&self, i: I) -> DataOutputItemResult<O> {
        (self)(i)
    }
}
*/

#[async_trait]
impl<
        I: DeserializeOwned + Debug + Send + Sync + 'static,
        O: DeserializeOwned + Debug + Send + Sync + 'static,
    > DataSource<O> for Transformer<I, O>
{
    async fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx): (_, Receiver<Result<DataSourceMessage<O>, DataStoreError>>) =
            channel(1);
        let (mut input_rx, _) = self.input.start_stream().await?;
        let map_func = self.map;
        let name = String::from("Transformer");
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
            tokio::spawn(async move {
                let mut lines_scanned = 0_usize;
                loop {
                    match input_rx.recv().await {
                        Some(Ok(DataSourceMessage::Data {
                            source,
                            content: input_item,
                        })) => match map_func(input_item) {
                            Ok(output_item) => {
                                lines_scanned += 1;
                                tx.send(Ok(DataSourceMessage::new(&source, output_item)))
                                    .await
                                    .map_err(|e| {
                                        DataStoreError::send_error(&name, &source, e)
                                    })?;
                            }
                            Err(val) => {
                                match tx
                                    .send(Err(DataStoreError::Deserialize {
                                        message: val.to_string(),
                                        attempted_string: format!("{:?}", val),
                                    }))
                                    .await
                                {
                                    Ok(_) => {
                                        lines_scanned += 1;
                                    }
                                    Err(e) => {
                                        return Err(DataStoreError::send_error(
                                            &name, "", e,
                                        ));
                                    }
                                }
                            }
                        },
                        Some(Err(er)) => println!("ERROR: {}", er),
                        None => break,
                    };
                }
                Ok(DataSourceStats { lines_scanned })
            });
        Ok((rx, jh))
    }
}
