use super::*;

impl<T> DataOutput<'_, T> for Vec<T> 
where
    T: Send + Sync + Debug + 'static
{
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, mut rx) = channel(1);
        let jh: DataOutputJoinHandle = tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                log::info!("Got item: {:?}", item);
            }
            Ok(DataOutputDetails::Empty)
        });
        Ok((tx, jh))
    }
}
