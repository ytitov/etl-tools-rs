use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use futures_core::future::BoxFuture;

/// Apply an async function to every element of a DataSource.
pub struct Apply<'a, S, I> {
    pub state: S,
    pub input: Box<dyn DataSource<'a, I>>,
    pub apply_fn:
        Box<dyn Fn(&'_ S, I) -> BoxFuture<'_, anyhow::Result<()>> + 'a + Send + Sync>,
}

impl<'a, S, I> Apply<'a, S, I> 
where
    S: 'static + Send + Sync,
    I: 'static + Send,
{
    pub async fn run(self) -> Result<TaskOutputDetails, DataStoreError> {
        //let source_name = format!("Apply-{}", self.input.name());
        let (mut rx, jh) = self.input.start_stream()?;
        let apply_fn = self.apply_fn;
        let state = self.state;
        let s = state;
        //let mut lines_written = 0_usize;
        loop {
            match rx.recv().await {
                Some(Ok(DataSourceMessage::Data { source: _, content })) => {
                    match apply_fn(&s, content).await {
                        Ok(_) => {
                            //lines_written += 1;
                        }
                        Err(e) => {
                            log::error!("Apply: {}", &e);
                            return Err(e.into());
                        }
                    }
                }
                _ => {
                    break;
                }
            };
        }
        jh.await??;
        Ok(().into())
    }
}
impl<S, I> OutputTask for Apply<'static, S, I> 
where
    S: 'static + Send + Sync,
    I: 'static + Send,
{
    fn create(self: Box<Self>) -> Result<TaskJoinHandle, DataStoreError> 
    {
        Ok(tokio::spawn(async move {
            Ok(self.run().await?)
        }))
    }
}
