use crate::datastore::error::*;
use crate::datastore::*;
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use tokio::sync::mpsc::channel;

pub mod json {
    use super::*;
    pub struct JsonEncoder<'a, I> {
        m: std::marker::PhantomData<I>,
        bytes_output: Box<dyn DataOutput<'a, Bytes>>,
    }

    impl<'a, I> JsonEncoder<'a, I>
    where
        I: Serialize + Send + Sync,
    {
        pub fn into_bytes_output<DO>(out: DO) -> Self
        where
            DO: DataOutput<'a, Bytes>,
        {
            Self {
                m: std::marker::PhantomData,
                bytes_output: Box::new(out),
            }
        }
    }

    /*
    pub struct TransformConsumer<'a, I, O, Data> {
        consumer: Box<dyn Consumer<'a, O, Data> + Send>,
        transformer: Box<dyn TransformerFut<I, O>>,
    }
        */
    use crate::streams::transformer::TransformConsumer;
    use crate::streams::*;
    use crate::transformer::TransformFunc;

    /// Accepts any Consumer which can receive Bytes, then uses the Serialize trait to convert the
    /// incoming data to Bytes
    pub fn as_json_bytes_consumer<'a, I, C, Data>(
        bytes_consumer: C,
    ) -> TransformConsumer<'a, I, Bytes, Data>
    where
        C: Consumer<'a, Bytes, Data> + Send + Sync,
        I: 'static + Send + Sync + Serialize,
    {
        TransformConsumer::new(
            bytes_consumer,
            TransformFunc::new(|incoming: I| Ok(Bytes::from(serde_json::to_vec(&incoming)?))),
        )
    }

    //impl<T: Serialize + Debug + Send + Sync + 'static> DataOutput<'_, T> for MockDataOutput {
    impl<'a, I> DataOutput<'a, I> for JsonEncoder<'a, I>
    where
        I: Serialize + Send + Sync + 'static,
    {
        fn start_stream(self: Box<Self>) -> Result<DataOutputTask<I>, DataStoreError> {
            let (bytes_out_tx, bytes_out_jh) = self.bytes_output.start_stream()?;
            let (tx, mut rx): (DataOutputTx<I>, _) = channel(1);
            let jh: DataOutputJoinHandle = tokio::spawn(async move {
                let mut lines_written = 0_usize;
                loop {
                    match rx.recv().await {
                        Some(DataOutputMessage::Data(data)) => {
                            match serde_json::to_string(&data) {
                                Ok(data_str) => {
                                    bytes_out_tx
                                        .send(DataOutputMessage::new(Bytes::from(data_str)))
                                        .await?;
                                    lines_written += 1;
                                }
                                Err(er) => {
                                    return Err(DataStoreError::FatalIO(format!(
                                        "Could not serialize this item: {}",
                                        er
                                    )));
                                }
                            };
                        }
                        None => break,
                        _ => break,
                    };
                }
                drop(bytes_out_tx);
                bytes_out_jh.await??;
                Ok(DataOutputDetails::Basic { lines_written })
            });
            Ok((tx, jh))
        }
    }
}

//TODO: this can be removed
pub trait EncodeStream<'a, I, O>: Sync + Send
where
    I: 'static + Send,
    O: 'static + Send,
{
    fn encode_source(
        self: Box<Self>,
        source: Box<dyn DataSource<'a, I>>,
    ) -> Box<dyn DataSource<'a, O>>;
}

/// helper wrapper that specific  encoders can return for convenience
pub struct EncodedSource<T: 'static + Send + Sync> {
    source_name: String,
    ds_task_result: Result<DataSourceTask<T>, DataStoreError>,
}

impl<T> DataSource<'_, T> for EncodedSource<T>
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> String {
        format!("{}-EncodedSource", &self.source_name)
    }
    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        self.ds_task_result
    }
}

/// A convenience wrapper which takes in any decoder which will decode to Bytes and accepts any
/// DataOutput which will accepts bytes and wraps that all into a DataOutput
pub struct EncodedOutput<'a, I: 'static + Send + Sync> {
    /// takes any I and converts to Bytes.  For example csv encoder will implement EncodeStream
    pub encoder: Box<dyn EncodeStream<'a, I, Bytes>>,
    /// handles writing the result of the encoder
    pub output: Box<dyn DataOutput<'a, Bytes>>,
}

/*
use std::fmt::Debug;
impl<'a, T: Debug + Serialize + 'static + Sync + Send> DataOutput<T> for EncodedOutput<'a, T> {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError> {
        // create a datasource for the encoder because it is needed to create the encoder
        // data sent to the data output will be forwarded here
        let (data_source_tx, data_source_rx): (_, DataSourceRx<T>) = channel(1);

        let encoder = self.encoder;
        let encoded_datasource =
            encoder.encode_source(Box::new(data_source_rx) as Box<dyn DataSource<'a, T>>);
        let given_output_jh: DataOutputJoinHandle = tokio::spawn(async move {
            // forward encoded elements to the output dataoutput
            let (mut encoded_datasource_rx, encoded_datasource_jh) =
                encoded_datasource.start_stream()?;
            let (final_data_output_tx, final_data_output_jh) = self.output.start_stream()?;
            loop {
                match encoded_datasource_rx.recv().await {
                    Some(Ok(DataSourceMessage::Data { source, content })) => {
                        final_data_output_tx
                            .send(DataOutputMessage::Data(content))
                            .await
                            .map_err(|e| {
                                DataStoreError::send_error(
                                    &source,
                                    "EncodedOutput:FinalizedOutput",
                                    e,
                                )
                            })?;
                    }
                    Some(Err(er)) => return Err(er),
                    None => {
                        break;
                    }
                }
            }
            drop(final_data_output_tx);
            encoded_datasource_jh.await??;
            Ok(final_data_output_jh.await?.map_err(|er| {
                DataStoreError::FatalIO(format!("Error inside the DataOutput: {}", er.to_string()))
            })?)
        });

        // this part receives the dataoutput messages and forwards them to the datasource
        // which can be given to the encoder, but running into problem due to encode_source method
        // wants to consume the encoder but we are in a &mut environment here
        // pass the input_rx stream into the encoder
        let (input_tx, mut input_rx) = channel(1);
        let output_name = String::from("EncodedOutput");
        let jh: DataOutputJoinHandle = tokio::spawn(async move {
            loop {
                match input_rx.recv().await {
                    Some(DataOutputMessage::Data(data)) => {
                        data_source_tx
                            .send(Ok(DataSourceMessage::new(&output_name, data)))
                            .await
                            .map_err(|e| {
                                DataStoreError::send_error(&output_name, "encoded-output", e)
                            })?;
                    }
                    Some(DataOutputMessage::NoMoreData) => break,
                    None => break,
                }
            }
            drop(data_source_tx);
            //encoded_datasource_jh.await??;
            // return the output stats from the passed in output as opposed to creating them in this task, because ultimately we likely care about the passed in output and not this one
            Ok(given_output_jh.await??)
        });
        Ok((input_tx, jh))
    }
}
*/

pub mod csv_encoder {
    use super::*;
    use crate::datastore::CsvWriteOptions;

    pub struct CsvStringEncoder {
        csv_write_options: CsvWriteOptions,
    }

    impl Default for CsvStringEncoder {
        fn default() -> Self {
            CsvStringEncoder {
                csv_write_options: CsvWriteOptions::default(),
            }
        }
    }

    /*
    #[async_trait]
    impl<'a, I: 'static + Serialize + Send> EncodeStream<'a, I, Bytes> for CsvStringEncoder {
        async fn encode_source(
            self: Box<Self>,
            source: Box<dyn for<'i> DataSource<'i, I>>,
        ) -> Box<dyn DataSource<'a, Bytes>> {
            use csv::WriterBuilder;
            let (tx, rx) = channel(1);
            let source_name = source.name();
            match source.start_stream() {
                Ok((mut source_rx, source_stream_jh)) => {
                    let CsvWriteOptions {
                        delimiter,
                        has_headers,
                        terminator,
                        quote_style,
                        quote,
                        escape,
                        double_quote,
                    } = self.csv_write_options;
                    let jh: DataSourceJoinHandle = tokio::spawn(async move {
                        let mut lines_scanned = 0_usize;
                        loop {
                            match source_rx.recv().await {
                                Some(Ok(DataSourceMessage::Data { source, content })) => {
                                    let send_header = match lines_scanned {
                                        0 => has_headers,
                                        _ => false,
                                    };
                                    let mut wrt = WriterBuilder::new()
                                        .has_headers(send_header)
                                        .delimiter(delimiter)
                                        .terminator(terminator)
                                        .quote_style(quote_style)
                                        .quote(quote)
                                        .escape(escape)
                                        .double_quote(double_quote)
                                        .from_writer(vec![]);
                                    wrt.serialize(content)
                                        .map_err(|er| DataStoreError::FatalIO(er.to_string()))?;
                                    let b = Bytes::from(
                                        String::from_utf8(wrt.into_inner().map_err(|er| {
                                            DataStoreError::FatalIO(er.to_string())
                                        })?)
                                        .map_err(|e| DataStoreError::FatalIO(e.to_string()))?,
                                    );
                                    tx.send(Ok(DataSourceMessage::new(&source, b)))
                                        .await
                                        .map_err(|e| {
                                            DataStoreError::send_error(&source, "CsvDecoder", e)
                                        })?;
                                    lines_scanned += 1;
                                }
                                Some(Err(e)) => {
                                    return Err(e);
                                }
                                None => {
                                    break;
                                }
                            }
                        }

                        source_stream_jh.await??;
                        Ok(DataSourceDetails::Basic { lines_scanned })
                    });
                    return Box::new(EncodedSource {
                        source_name: source_name.clone(),
                        ds_task_result: Ok((rx, jh)),
                    });
                }
                Err(er) => {
                    return Box::new(EncodedSource {
                        source_name,
                        ds_task_result: Err(er),
                    });
                }
            }
        }
    }
    */
}
