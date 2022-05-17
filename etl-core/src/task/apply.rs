use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use futures_core::future::BoxFuture;
use std::fmt::Debug;

/// Apply an async function to every element of a DataSource.
pub struct Apply<S, I> {
    pub state: S,
    pub input: Box<dyn DataSource<I>>,
    pub apply_fn:
        Box<dyn Fn(&'_ S, I) -> BoxFuture<'_, anyhow::Result<()>> + 'static + Send + Sync>,
}

impl<S: 'static + Send + Sync, I: 'static + Debug + Send> Apply<S, I> {
    pub async fn run(self) -> anyhow::Result<DataOutputStats> {
        let source_name = format!("Apply-{}", self.input.name());
        let (mut rx, jh) = self.input.start_stream()?;
        let apply_fn = self.apply_fn;
        let state = self.state;
        let s = state;
        let mut lines_written = 0_usize;
        loop {
            match rx.recv().await {
                Some(Ok(DataSourceMessage::Data { source: _, content })) => {
                    match apply_fn(&s, content).await {
                        Ok(_) => {
                            lines_written += 1;
                        }
                        Err(e) => {
                            log::error!("Apply apply_fn returned an error: {}", &e);
                            return Err(e);
                        }
                    }
                }
                Some(Err(e)) => {
                    log::error!("DataSource sent an error: {}", e);
                }
                None => {
                    log::info!("got a none");
                    break;
                }
            };
        }
        drop(rx);
        jh.await??;
        Ok(DataOutputStats {name: source_name, lines_written, key: None })
    }
}
impl<S: 'static + Send + Sync, I: 'static + Debug + Send> OutputTask for Apply<S, I> {
    fn create(self: Box<Self>) -> Result<DataOutputJoinHandle, DataStoreError> 
    {
        Ok(tokio::spawn(async move {
            Ok(self.run().await?)
        }))
    }
}
