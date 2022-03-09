use super::*;
use crate::job::*;
use etl_core::datastore::*;
use etl_core::datastore::error::*;
use etl_core::deps::tokio::task::JoinHandle;
use etl_core::deps::serde::{Serialize, de::DeserializeOwned};
use crate::transform_store::handler::TransformHandler;

/// Allows processing data using the TransformHandler trait to process and
/// filter a stream, and output that as a new DataSource which can
/// then be forwarded to a DataOutput, or continue as input for
/// more transformations
pub struct TransformDataSource<I, O: Serialize + Debug + Send + Sync + 'static> {
    pub input_ds: Box<dyn DataSource<I>>, // recv message
    pub transformer: Box<dyn TransformHandler<I, O>>,
    pub job_name: String,
}

impl<I, O> TransformDataSource<I, O>
where
    I: Debug + Send + Sync,
    O: Serialize + Debug + Send + Sync,
{
    pub fn new(
        name: &str,
        ds: Box<dyn DataSource<I>>,
        transformer: Box<dyn TransformHandler<I, O>>,
    ) -> Self {
        TransformDataSource {
            job_name: name.to_string(),
            transformer,
            input_ds: ds,
        }
    }
}

impl<
        I: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
        O: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
    > DataSource<O> for TransformDataSource<I, O>
{
    fn name(&self) -> String {
        format!("TransformDataSource-{}", &self.job_name)
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (mut source_rx, source_stream_jh) = self.input_ds.start_stream()?;
        let (tx, rx) = channel(1);
        let transformer = self.transformer;
        let job_name = self.job_name;
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                use crate::job::handler::TransformOutput::*;
                match source_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data {
                        source,
                        content: item,
                    })) => {
                        lines_scanned += 1;
                        match transformer
                            .transform_item(JobItemInfo::new((lines_scanned, &job_name)), item)
                            .await
                        {
                            Ok(Some(Item(item_out))) => tx
                                .send(Ok(DataSourceMessage::new(&job_name, item_out)))
                                .await
                                .map_err(|e| DataStoreError::send_error(&job_name, &source, e))?,
                            Ok(Some(List(_vec))) => {
                                panic!("Processing list not implemented")
                            }
                            Ok(None) => {}
                            Err(er) => {
                                tx.send(Err(DataStoreError::TransformerError {
                                    job_name: job_name.to_owned(),
                                    error: er.to_string(),
                                }))
                                .await
                                .map_err(|e| DataStoreError::send_error(&job_name, &source, e))?;
                            }
                        };
                    }
                    Some(Err(val)) => {
                        tx.send(Err(DataStoreError::Deserialize {
                            message: val.to_string(),
                            attempted_string: "Unknown".to_string(),
                        }))
                        .await
                        .map_err(|e| DataStoreError::send_error(&job_name, "", e))?;
                    }
                    None => break,
                };
            }

            source_stream_jh.await??;

            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
