use super::*;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Sender as OneShotTx;
use tokio::task::JoinHandle;
type LoadMessage<T> = (String, OneShotTx<Result<T, DataStoreError>>);
type WriteMessage<T> = (String, T, OneShotTx<Result<(), DataStoreError>>);
enum Operation<T> {
    Load(LoadMessage<T>),
    Write(WriteMessage<T>),
}

#[derive(Clone)]
pub struct MpscSimpleStoreTx<T> {
    storage_tx: Sender<Operation<T>>,
}

pub struct MpscSimpleStoreRx<T> {
    p: std::marker::PhantomData<T>,
}

pub type MpscSimpleStoreRxHandle = JoinHandle<Result<(), DataStoreError>>;
impl<T> MpscSimpleStoreRx<T>
where
    T: 'static + Send,
{
    pub fn create(
        storage: Box<dyn SimpleStore<T>>,
    ) -> (MpscSimpleStoreRxHandle, MpscSimpleStoreTx<T>) {
        use tokio::sync::mpsc;
        let (storage_tx, mut storage_rx): (Sender<Operation<T>>, _) = mpsc::channel(10);
        let jh: MpscSimpleStoreRxHandle = tokio::spawn(async move {
            loop {
                match storage_rx.recv().await {
                    Some(Operation::Load((key, reply_tx))) => {
                        match reply_tx.send(storage.load(&key).await) {
                            Ok(_) => {}
                            Err(_er) => {
                                return Err(DataStoreError::FatalIO(
                                    "Could not return reply to the SimpleStoreTx.".into(),
                                ));
                            }
                        };
                    }
                    Some(Operation::Write((key, item, reply_tx))) => {
                        match reply_tx.send(storage.write(&key, item).await) {
                            Ok(_) => {}
                            Err(_er) => {
                                return Err(DataStoreError::FatalIO(
                                    "Could not return reply to the SimpleStoreTx.".into(),
                                ));
                            }
                        };
                    }
                    None => break,
                }
            }
            Ok(())
        });
        (jh, MpscSimpleStoreTx { storage_tx })
    }
}

#[async_trait]
impl<T> SimpleStore<T> for MpscSimpleStoreTx<T>
where
    T: 'static + Send,
{
    async fn load(&self, path: &str) -> Result<T, DataStoreError> {
        use tokio::sync::oneshot;
        let (oneshot_tx, oneshot_rx): (OneShotTx<Result<T, DataStoreError>>, _) =
            oneshot::channel();
        match self
            .storage_tx
            .send(Operation::Load((path.to_string(), oneshot_tx)))
            .await
        {
            Ok(_) => match oneshot_rx.await {
                Ok(result) => result,
                Err(er) => Err(DataStoreError::FatalIO(er.to_string())),
            },
            Err(er) => Err(DataStoreError::FatalIO(er.to_string())),
        }
    }
    async fn write(&self, path: &str, item: T) -> Result<(), DataStoreError> {
        use tokio::sync::oneshot;
        let (oneshot_tx, oneshot_rx): (OneShotTx<Result<(), DataStoreError>>, _) =
            oneshot::channel();
        match self
            .storage_tx
            .send(Operation::Write((path.to_string(), item, oneshot_tx)))
            .await
        {
            Ok(_) => match oneshot_rx.await {
                Ok(result) => result,
                Err(er) => Err(DataStoreError::FatalIO(er.to_string())),
            },
            Err(er) => Err(DataStoreError::FatalIO(er.to_string())),
        }
    }
}
