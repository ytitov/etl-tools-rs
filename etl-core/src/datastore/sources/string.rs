use crate::datastore::*;
use bytes::Bytes;
use tokio::sync::mpsc::channel;

impl DataSource<Bytes> for String {
    fn name(&self) -> String {
        format!("String")
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<Bytes>, DataStoreError> {
        let (tx, rx) = channel(1);
        let name = self.name();
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            for line in self.lines() {
                lines_scanned += 1;
                tx.send(Ok(DataSourceMessage::new(
                    &name,
                    Bytes::from(line.to_owned()),
                )))
                .await
                .map_err(|er| DataStoreError::send_error(&name, "", er))?;
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
