use crate::datastore::error::*;
use crate::datastore::*;
use futures_core::future::BoxFuture;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::Sender;

// move these into datastore module
pub type BoxedDataSourceResult<T> = anyhow::Result<Box<dyn DataSource<T>>>;
pub type CreateDataSourceFn<'a, R> =
    Box<dyn Fn() -> BoxFuture<'a, BoxedDataSourceResult<R>> + 'static + Send + Sync>;
pub type BoxedDataOutputResult<T> = anyhow::Result<Box<dyn DataOutput<T>>>;
pub type CreateDataOutputFn<'a, R> =
    Box<dyn Fn() -> BoxFuture<'a, BoxedDataOutputResult<R>> + 'static + Send + Sync>;

//NOTE: caching could be added here, though wrapping this with a DataOutput inside
//JobRunner would accomplish this.
pub struct LeftJoin<'a, L, R> {
    pub create_right_ds: CreateDataSourceFn<'a, R>,
    pub left_ds: Box<dyn DataSource<L>>,
    /// Number of rows from the left dataset to hold in memory.  Higher number means less scans
    /// on the right table
    pub left_buf_len: usize,
    //NOTE: possibly add an async version of this.  Again, enriching the data or doing some lookups
    //should probably happen in other steps, like in the StreamHandler
    pub is_match: Box<dyn Fn(&L, &R) -> bool + Send + Sync>,
}

async fn fill_left_stack<L, R>(
    v: &mut Vec<L>,
    left_rx: &mut DataSourceRx<L>,
    fw_tx: &Sender<Result<DataSourceMessage<(L, Option<R>)>, DataStoreError>>,
) -> Result<usize, DataStoreError>
where
    L: DeserializeOwned + Send + Debug,
    R: DeserializeOwned + Send + Debug,
{
    let mut num_read = 0;
    while v.capacity() > v.len() {
        match left_rx.recv().await {
            Some(Ok(DataSourceMessage::Data { source: _, content })) => {
                num_read += 1;
                v.push(content);
            }
            Some(Err(er)) => {
                fw_tx
                    .send(Err(er.into()))
                    .await
                    .map_err(|e| DataStoreError::send_error("LeftJoin", "LeftSide", e))?;
                // so it continues reading; a real zero means it is done
                num_read += 1;
            }
            None => {
                return Ok(num_read);
            }
        }
    }
    Ok(num_read)
}

async fn forward_matches<L, R>(
    fw_tx: &Sender<Result<DataSourceMessage<(L, Option<R>)>, DataStoreError>>,
    v: &Vec<L>,
    create_right_ds: &CreateDataSourceFn<'_, R>,
    is_match: &Box<dyn Fn(&L, &R) -> bool + Send + Sync>,
) -> Result<usize, DataStoreError>
where
    L: DeserializeOwned + Send + Debug + Clone,
    R: DeserializeOwned + Send + Debug + Clone + 'static,
{
    let mut num_read = 0;
    let mut left_match_counts: HashMap<usize, usize> = HashMap::with_capacity(v.len());
    for (idx, _) in v.iter().enumerate() {
        left_match_counts.insert(idx, 0);
    }
    match create_right_ds().await {
        Err(e) => {
            return Err(e.into());
        }
        Ok(right_ds) => {
            let (mut right_rx, _) = right_ds.start_stream()?;
            loop {
                match right_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data {
                        source,
                        content: _c,
                    })) => {
                        num_read += 1;
                        for (idx, elem) in v.iter().enumerate() {
                            if is_match(elem, &_c) {
                                let r = left_match_counts
                                    .get_mut(&idx)
                                    .expect("This should not panic");
                                *r += 1;
                                fw_tx
                                    .send(Ok(DataSourceMessage::new(
                                        "LeftJoin::Matches",
                                        (elem.clone(), Some(_c.clone())),
                                    )))
                                    .await
                                    .map_err(|e| {
                                        DataStoreError::send_error(
                                            "Join LeftJoin",
                                            &source,
                                            e,
                                        )
                                    })?;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        fw_tx.send(Err(e.into())).await.map_err(|e| {
                            DataStoreError::send_error("LeftJoin", "RightSide", e)
                        })?;
                    }
                    None => {
                        break;
                    }
                }
            }
            // send the non-matches
            for (idx, elem) in v.iter().enumerate() {
                if let Some(count) = left_match_counts.get(&idx) {
                    if *count == 0 {
                        fw_tx
                            .send(Ok(DataSourceMessage::new(
                                "LeftJoin::NonMatches",
                                (elem.clone(), None),
                            )))
                            .await
                            .map_err(|e| {
                                DataStoreError::send_error("Join LeftJoin", "", e)
                            })?;
                    }
                }
            }
        }
    }
    Ok(num_read)
}

impl<L, R> DataSource<(L, Option<R>)> for LeftJoin<'static, L, R>
where
    R: DeserializeOwned + Debug + Clone + 'static + Send + Sync,
    L: DeserializeOwned + Debug + Clone + 'static + Send + Sync,
{
    fn name(&self) -> String {
        format!("LeftJoin-{}", self.left_ds.name())
    }

    fn start_stream(
        self: Box<Self>,
    ) -> Result<DataSourceTask<(L, Option<R>)>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let (mut left_rx, _) = self.left_ds.start_stream()?;

        let max_left_len = self.left_buf_len;
        let matching_func = self.is_match;
        let create_right_ds = self.create_right_ds;
        let jh: DataSourceJoinHandle =
            tokio::spawn(async move {
                let mut lines_scanned = 0;
                let mut left_stack: Vec<L> = Vec::with_capacity(max_left_len);
                loop {
                    match fill_left_stack(&mut left_stack, &mut left_rx, &tx).await? {
                        num_read if num_read == 0 => {
                            break;
                        }
                        num_read => {
                            forward_matches(
                                &tx,
                                &left_stack,
                                &create_right_ds,
                                &matching_func,
                            )
                            .await?;
                            lines_scanned += num_read;
                            left_stack.clear();
                        }
                    }
                }
                Ok(DataSourceDetails::Basic { lines_scanned })
            });
        Ok((rx, jh))
    }
}
