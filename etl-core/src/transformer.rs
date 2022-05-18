use crate::datastore::error::DataStoreError;
use crate::datastore::BoxedDataSource;
use crate::datastore::DataSource;
use crate::datastore::{DataOutputStats, DataSourceMessage};
use std::fmt::Debug;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver as OneShotRx, Sender as OneShotTx};
use tokio::task::JoinHandle;

pub type TransformerItemRequest<T> = OneShotTx<T>;
pub type TransformerResultRx<T> = OneShotRx<Result<T, DataStoreError>>;
pub type TransformerResultTx<T> = OneShotTx<Result<T, DataStoreError>>;
pub type TransformerResultChannel<T> = (TransformerResultTx<T>, TransformerResultRx<T>);
//pub type TransformerResultRx<T> = Receiver<TransformerItemRequest<DataSourceMessage<T>>>;
//pub type TransformerResultTx<T> = Sender<TransformerItemReply<DataSourceMessage<T>>>;
pub type TransformerItemRequestChannel<I> = (
    Sender<TransformerItemRequest<I>>,
    Receiver<TransformerItemRequest<I>>,
);
pub type TransformerTask<T> = (
    TransformerResultRx<T>,
    JoinHandle<Result<DataOutputStats, DataStoreError>>,
);

pub struct TransformerPipeline<I, O> {
    input: Box<dyn DataSource<I>>,
    output: Box<dyn DataOutput<O>>,
}

impl<I, O> TransformerPipeline<I, O>
where
    I: Debug + 'static + Send,
    O: Debug + 'static + Send + Sync,
{
    /*
    pub fn with_dataoutput(
        input: Box<dyn DataSource<I>>,
        output: Box<dyn DataOutput<O>>,
    ) -> DataSourceTask<(I, OneShotTx<O>)> {
        use tokio::sync::mpsc;
        let (mut source_rx, jh) = input.start_stream().unwrap();
        let (mapped_ds_tx, mapped_ds_jh) = output.start_stream().unwrap();
        let (transform_tx, transform_rx) = mpsc::channel(1);
        let jh = tokio::spawn(async move {
            let lines_scanned = 0_usize;
            loop {
                match source_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data { source, content })) => {
                        let (c_tx, c_rx) = oneshot::channel();
                        transform_tx
                            .send(Ok(DataSourceMessage::new(&source, (content, c_tx))))
                            .await
                            .unwrap();
                        // what do we do here??
                        // need to send it somewhere
                        match c_rx.await {
                            Ok(result_item) => {
                                mapped_ds_tx
                                    .send(DataOutputMessage::new(result_item))
                                    .await
                                    .unwrap();
                            }
                            Err(er) => {
                                panic!("transformer failed due to: {}", er);
                            }
                        };
                    }
                    Some(Err(err)) => {
                        // not sure what to do here yet
                        //let s: String = err;
                    }
                    None => break,
                }
            }
            jh.await??;
            mapped_ds_jh.await??;
            Ok::<_, DataStoreError>(DataSourceStats { lines_scanned })
        });
        (transform_rx, jh)
    }
    */
}

// no need for all this, can just create a struct that does the splitting and has all the logic
pub trait Transformable<I, O>: DataSource<(I, TransformerResultTx<O>)> + Sync + Send
where
    I: 'static + Send + Debug,
    O: 'static + Send + Debug,
{
}

pub trait TransformSource<I, O>: Transformable<I, O>
where
    I: 'static + Send + Debug,
    O: 'static + Sync + Send + Debug,
{
    fn transform_source(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
        unimplemented!();
    }
}

impl<I: Debug + 'static + Send + Sync, O: Debug + 'static + Send + Sync>
    DataSource<(I, TransformerResultTx<O>)> for TransformerPipeline<I, O>
{
    fn name(&self) -> String {
        "TransformerPipeline".into()
    }

    fn start_stream(
        self: Box<Self>,
    ) -> Result<DataSourceTask<(I, TransformerResultTx<O>)>, DataStoreError> {
        let input: Box<dyn DataSource<I>> = self.input;
        let output: Box<dyn DataOutput<O>> = self.output;
        use tokio::sync::mpsc;
        let (mut source_rx, jh) = input.start_stream().unwrap();
        let (datastore_output_tx, datastore_output_ds_jh) = output.start_stream().unwrap();
        let (transform_output_tx, transform_output_rx): (_, _) = mpsc::channel(1);
        let (transform_input_tx, transform_input_rx): (
            Sender<DataSourceMessage<(I, TransformerResultRx<O>)>>,
            Receiver<DataSourceMessage<(I, TransformerResultRx<O>)>>,
        ) = mpsc::channel(1);
        let task_jh = tokio::spawn(async move {
            let lines_scanned = 0_usize;
            loop {
                match source_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data { source, content })) => {
                        let (c_tx, c_rx): (_, OneShotRx<Result<O, DataStoreError>>) =
                            oneshot::channel();
                        /*
                        transform_input_tx
                            .send(DataSourceMessage::new(&source, (content, c_tx)))
                            .await
                            .unwrap();
                        */
                        // what do we do here??
                        // need to send it somewhere
                        match c_rx.await {
                            Ok(Ok(mapped_value)) => {
                                //let s: String = mapped_value;
                                datastore_output_tx
                                    .send(DataOutputMessage::new(mapped_value))
                                    .await
                                    .unwrap();
                            }
                            Ok(Err(datastore_err)) => {
                                /*
                                mapped_ds_tx
                                    .send(DataOutputMessage::new(result_item))
                                    .await
                                    .unwrap();
                                */
                            }
                            Err(er) => {
                                //let e: DataStoreError = er;
                                panic!("transformer failed due to: {}", er);
                            }
                        };
                    }
                    Some(Err(err)) => {
                        // not sure what to do here yet
                        //let s: String = err;
                    }
                    None => break,
                }
            }
            jh.await??;
            //mapped_ds_jh.await??;
            Ok::<_, DataStoreError>(DataSourceStats { lines_scanned })
        });
        Ok((transform_output_rx, task_jh))
    }
}

use crate::datastore::DataOutput;
pub type DataOutputTransformer<I, O> = dyn DataOutput<(I, TransformerResultTx<O>)>;
pub type DataSourceTransformer<I, O> = dyn DataSource<(I, TransformerResultTx<O>)>;

//impl<I: Debug + 'static + Send, O: Debug + 'static + Send> dyn DataSource<(I, TransformerResultTx<O>)>
use crate::datastore::DataOutputTask;
impl<I: Debug + 'static + Send, O: Debug + 'static + Send + Sync> DataOutput<O>
    for dyn DataSource<(I, TransformerResultTx<O>)>
{
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<O>, DataStoreError> {
        unimplemented!();
    }

    /*
    fn into_ds(self: Box<Self>) -> String {
        use crate::datastore::DataSourceRx;
        let (out_rx, jh): (DataSourceRx<I>, _) = self.start_stream().unwrap();
        String::from("DataSourceTransformer")
    }
    */
}

use crate::datastore::DataOutputMessage;
use crate::datastore::DataSourceStats;
use crate::datastore::DataSourceTask;
impl<I: Debug + 'static + Send, O: Debug + 'static + Send + Sync>
    dyn DataSource<(I, TransformerResultTx<O>)>
{
    /// convert a DataSource<I> into DataSource<(I, OneShotTx<O>)> for the purposes of using
    /// Transformer trait on any DataSource
    fn from_datasource(
        input: Box<dyn DataSource<I>>,
        output: Box<dyn DataOutput<O>>,
    ) -> DataSourceTask<(I, OneShotTx<O>)> {
        use tokio::sync::mpsc;
        let (mut source_rx, jh) = input.start_stream().unwrap();
        let (mut mapped_ds_tx, mapped_ds_jh) = output.start_stream().unwrap();
        let (transform_tx, transform_rx) = mpsc::channel(1);
        let jh = tokio::spawn(async move {
            let lines_scanned = 0_usize;
            loop {
                match source_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data { source, content })) => {
                        let (c_tx, c_rx) = oneshot::channel();
                        transform_tx
                            .send(Ok(DataSourceMessage::new(&source, (content, c_tx))))
                            .await
                            .unwrap();
                        // what do we do here??
                        // need to send it somewhere
                        match c_rx.await {
                            Ok(result_item) => {
                                mapped_ds_tx
                                    .send(DataOutputMessage::new(result_item))
                                    .await
                                    .unwrap();
                            }
                            Err(er) => {
                                panic!("transformer failed due to: {}", er);
                            }
                        };
                    }
                    Some(Err(err)) => {
                        // not sure what to do here yet
                        //let s: String = err;
                    }
                    None => break,
                }
            }
            mapped_ds_jh.await??;
            Ok::<_, DataStoreError>(DataSourceStats { lines_scanned })
        });
        (transform_rx, jh)
    }
    /*
    fn into_ds(self: Box<Self>) -> String {
        use crate::datastore::DataSourceRx;
        let (out_rx, jh): (DataSourceRx<I>, _) = self.start_stream().unwrap();
        String::from("DataSourceTransformer")
    }
    */
}

/*
impl<I: Debug + Send + 'static, O: 'static + Debug + Send> DataSource<I>
    for BoxedDataSource<(I, TransformerItemReply<O>)>
{
    fn name(&self) -> String {
        String::from(DataSource::<I>::name(self))
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<I>>
}
*/

pub mod local {
    use crate::datastore::error::DataStoreError;
    use crate::datastore::*;
    use std::fmt::Debug;
    use tokio::sync::mpsc::Receiver;
    use tokio::task::JoinHandle;
    pub struct Transformer<I, O> {
        pub input: Box<dyn DataSource<I>>,
        pub map: fn(I) -> DataOutputItemResult<Option<O>>,
    }

    impl<I: Debug + Send + Sync + 'static, O: Debug + Send + Sync + 'static> DataSource<O>
        for Transformer<I, O>
    {
        fn name(&self) -> String {
            format!("Transformer-{}", self.input.name())
        }

        fn start_stream(self: Box<Self>) -> Result<DataSourceTask<O>, DataStoreError> {
            use tokio::sync::mpsc::channel;
            let (tx, rx): (_, Receiver<Result<DataSourceMessage<O>, DataStoreError>>) = channel(1);
            let (mut input_rx, _) = self.input.start_stream()?;
            let map_func = self.map;
            let name = String::from("Transformer");
            let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
                tokio::spawn(async move {
                    let mut lines_scanned = 0_usize;
                    loop {
                        match input_rx.recv().await {
                            Some(Ok(DataSourceMessage::Data {
                                source,
                                content: input_item,
                            })) => match map_func(input_item) {
                                Ok(Some(output_item)) => {
                                    lines_scanned += 1;
                                    tx.send(Ok(DataSourceMessage::new(&source, output_item)))
                                        .await
                                        .map_err(|e| {
                                            DataStoreError::send_error(&name, &source, e)
                                        })?;
                                }
                                Ok(None) => {}
                                Err(val) => {
                                    match tx
                                        .send(Err(DataStoreError::Deserialize {
                                            message: val.to_string(),
                                            attempted_string: format!("{:?}", val),
                                        }))
                                        .await
                                    {
                                        Ok(_) => {
                                            lines_scanned += 1;
                                        }
                                        Err(e) => {
                                            return Err(DataStoreError::send_error(&name, "", e));
                                        }
                                    }
                                }
                            },
                            Some(Err(er)) => println!("ERROR: {}", er),
                            None => break,
                        };
                    }
                    Ok(DataSourceStats { lines_scanned })
                });
            Ok((rx, jh))
        }
    }
}
