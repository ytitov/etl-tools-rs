use crate::datastore::error::DataStoreError;
use crate::datastore::*;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

pub struct DuplicateDataSource<I: Serialize + DeserializeOwned + Debug + Send + Sync> {
    pub rx: DataSourceRx<I>,
    pub name: String,
}

#[async_trait]
impl<I: Serialize + DeserializeOwned + Debug + Send + Sync + 'static> DataSource<I>
    for DuplicateDataSource<I>
{
    fn name(&self) -> String {
        format!("{}-DuplicatedDataSource", &self.name)
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<I>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (fw_tx, fw_rx): (_, Receiver<Result<DataSourceMessage<I>, DataStoreError>>) =
            channel(1);
        let mut input_rx = self.rx;
        let name = self.name;
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                match input_rx.recv().await {
                    /*
                    Some(OkDataSourceMessage::Data(data)) => {
                        lines_scanned += 1;
                        // TODO: send with fw_tx
                        fw_tx
                            .send(Ok(DataSourceMessage::new(&name, data)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                    }
                    //Some(Err(er)) => println!("ERROR: {}", er),
                    Some(_) => break,
                    */
                    Some(Ok(DataSourceMessage::Data {
                        // we know the source is Splitter
                        source: _,
                        content,
                    })) => {
                        fw_tx
                            .send(Ok(DataSourceMessage::new(&name, content)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                    }
                    Some(Err(err)) => {
                        fw_tx
                            .send(Err(err))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                    }
                    None => break,
                };
                lines_scanned += 1;
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((fw_rx, jh))
    }
}

pub async fn split_datasources<I>(
    data_source: Box<dyn DataSource<I>>,
    n: u16,
) -> (
    JoinHandle<Result<DataSourceStats, DataStoreError>>,
    Vec<Box<DuplicateDataSource<I>>>,
)
where
    I: Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
{
    if n == 0 {
        panic!("Can't split into zero streams");
    }
    // not sure if need to wait on the handle, but I believe so
    // the handle will need to be awaited after the duplicate streams are read
    // the JobRunner may have to be involved somehow when splitting
    use tokio::sync::mpsc::channel;
    let mut outputs_tx = Vec::new();
    //let mut outputs_rx = Vec::new();
    let mut dup_data_sources: Vec<Box<DuplicateDataSource<I>>> = Vec::new();
    for num in 0..n {
        // TODO: since all will be streaming at different speeds, good idea to add a parameter to
        // configure the size of the buffer
        let (tx, rx): (_, DataSourceRx<I>) = channel(1);
        outputs_tx.push(tx);
        dup_data_sources.push(Box::new(DuplicateDataSource {
            name: format!("{}_{}", data_source.name(), num),
            rx,
        }));
    }
    let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
        let (mut input_rx, input_jh) = data_source.start_stream()?;
        let mut lines_scanned = 0_usize;
        loop {
            match input_rx.recv().await {
                Some(Ok(DataSourceMessage::Data {
                    source,
                    content: input_item,
                })) => {
                    lines_scanned += 1;
                    //let mut fx_tx_idx = 0;
                    for fw_tx in &outputs_tx {
                        //println!("SENDING TO: {}", fx_tx_idx);
                        //fx_tx_idx += 1;
                        fw_tx
                            .send(Ok(DataSourceMessage::new("Splitter", input_item.clone())))
                            .await
                            .map_err(|e| {
                                DataStoreError::send_error("DuplicateDataSource", &source, e)
                            })?;
                    }
                }
                Some(Err(err)) => {
                    lines_scanned += 1;
                    for fw_tx in &outputs_tx {
                        fw_tx
                            .send(Err(err.clone()))
                            .await
                            .map_err(|e| DataStoreError::send_error("Splitter", "", e))?;
                    }
                }
                None => break,
            };
        }
        input_jh.await??;
        Ok(DataSourceStats { lines_scanned })
    });
    (jh, dup_data_sources)
}
