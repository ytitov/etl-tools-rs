use crate::datastore::*;
use async_trait::async_trait;
use futures_core::future::BoxFuture;
use std::time::Duration;

pub struct EnumerateStream<S, O> {
    pub name: String,
    /// generates maximum elements, otherwise it is unlimited
    pub max: Option<usize>,
    pub pause: Option<Duration>,
    pub state: S,
    pub create: fn(&'_ S, usize) -> DataOutputItemResult<O>,
}

#[async_trait]
impl<S: Send + Sync + 'static, O: DeserializeOwned + Debug + Send + Sync + 'static> DataSource<O>
    for EnumerateStream<S, O>
{
    fn name(&self) -> String {
        format!("{}", &self.name)
    }

    async fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx): (_, Receiver<Result<DataSourceMessage<O>, DataStoreError>>) = channel(1);
        let create_func = self.create;
        let name = self.name;
        let maybe_pause = self.pause;
        let maybe_max = self.max;
        let state = self.state;
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                if let Some(max) = &maybe_max {
                    if lines_scanned >= *max {
                        break;
                    }
                }
                match create_func(&state, lines_scanned) {
                    Ok(output_item) => {
                        tx.send(Ok(DataSourceMessage::new(&name, output_item)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &name, e))?;
                    }
                    Err(err) => {
                        tx.send(Err(DataStoreError::Generic(err.to_string())))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &name, e))?;
                    }
                }
                if let Some(pause) = &maybe_pause {
                    tokio::time::sleep(*pause).await;
                }
                lines_scanned += 1;
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}

pub type BoxedEnumeratedItemResult<S, O> =
    Box<dyn Fn(&'_ S, usize) -> BoxFuture<'_, DataOutputItemResult<O>> + Send + Sync>;

/// Same as EnumerateStream but meant for making async calls
pub struct EnumerateStreamAsync<S, O> {
    pub name: String,
    /// generates maximum elements, otherwise it is unlimited
    pub max: Option<usize>,
    pub pause: Option<Duration>,
    /// Used for things like a connection pool to make db requests, see mysql test for an example
    pub state: S,
    pub create: BoxedEnumeratedItemResult<S, O>,
}

impl<S,O> EnumerateStreamAsync<S,O> {
    pub fn with_max<N, F>(name: N, max: usize, state: S, create_func: F) -> Self
        where 
            N: Into<String>,
            F: Fn(&'_ S, usize) -> BoxFuture<'_, DataOutputItemResult<O>> + Send + Sync + 'static
    {
        EnumerateStreamAsync {
            name: name.into(),
            max: Some(max),
            pause: None,
            state,
            create: Box::new(create_func),
        }
    }
}

#[async_trait]
impl<S: Send + Sync + 'static, O: DeserializeOwned + Debug + Send + Sync + 'static> DataSource<O>
    for EnumerateStreamAsync<S, O>
{
    fn name(&self) -> String {
        format!("{}", &self.name)
    }

    async fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        use tokio::sync::mpsc::channel;
        let (tx, rx): (_, Receiver<Result<DataSourceMessage<O>, DataStoreError>>) = channel(1);
        let create_func = self.create;
        let name = self.name;
        let maybe_pause = self.pause;
        let maybe_max = self.max;
        let state = self.state;
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> = tokio::spawn(async move {
            let mut lines_scanned = 0_usize;
            loop {
                if let Some(max) = &maybe_max {
                    if lines_scanned >= *max {
                        break;
                    }
                }
                match create_func(&state, lines_scanned).await {
                    Ok(output_item) => {
                        tx.send(Ok(DataSourceMessage::new(&name, output_item)))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &name, e))?;
                    }
                    Err(err) => {
                        tx.send(Err(DataStoreError::Generic(err.to_string())))
                            .await
                            .map_err(|e| DataStoreError::send_error(&name, &name, e))?;
                    }
                }
                if let Some(pause) = &maybe_pause {
                    tokio::time::sleep(*pause).await;
                }
                lines_scanned += 1;
            }
            Ok(DataSourceStats { lines_scanned })
        });
        Ok((rx, jh))
    }
}
