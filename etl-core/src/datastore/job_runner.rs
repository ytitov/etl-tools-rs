use super::*;
use crate::job::*;
use tokio::task::JoinHandle;

pub struct JRDataSource<I, O: Serialize + Debug + Send + Sync + 'static> {
    pub input_ds: Box<dyn DataSource<I>>, // recv message
    pub transformer: Box<dyn TransformHandler<I, O>>,
    pub job_name: String,
}

impl<
        I: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
        O: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
    > DataSource<O> for JRDataSource<I, O>
{
    fn name(&self) -> String {
        format!("JobRunner-{}", &self.job_name)
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (mut source_rx, source_stream_jh) = self.input_ds.start_stream()?;
        let (tx, rx) = channel(1);
        let transformer = self.transformer;
        let job_name = format!("JRDataSource job name: {}", &self.job_name);
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
                                .send(Ok(DataSourceMessage::new(&source, item_out)))
                                .await
                                .map_err(|e| DataStoreError::send_error(&job_name, "", e))?,
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
                                .map_err(|e| DataStoreError::send_error(&job_name, "", e))?;
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
