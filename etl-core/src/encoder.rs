use crate::datastore::error::*;
use crate::datastore::*;
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::fmt::Debug;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

#[async_trait]
pub trait EncodeStream<I: Debug + 'static + Send, O: Debug + 'static + Send>: Sync + Send {
    async fn encode_source(
        self: Box<Self>,
        source: Box<dyn DataSource<I>>,
    ) -> Box<dyn DataSource<O>>;
}

/// helper wrapper that specific  encoders can return for convenience
pub struct EncodedSource<T: Debug + 'static + Send + Sync> {
    source_name: String,
    ds_task_result: Result<DataSourceTask<T>, DataStoreError>,
}

impl<T: Debug + Send + Sync + 'static> DataSource<T> for EncodedSource<T> {
    fn name(&self) -> String {
        format!("{}-EncodedSource", &self.source_name)
    }
    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        self.ds_task_result
    }
}

/// A convenience wrapper which takes in any decoder which will decode to Bytes and accepts any
/// DataOutput which will accepts bytes and wraps that all into a DataOutput
pub struct EncodedOutput<I: Debug + 'static + Send + Sync> {
    /// takes any I and converts to Bytes.  For example csv encoder will implement EncodeStream
    pub encoder: Box<dyn EncodeStream<I, Bytes>>,
    /// handles writing the result of the encoder
    pub output: Box<dyn DataOutput<Bytes>>,
}

impl<T: Serialize + Debug + 'static + Sync + Send> DataOutput<T> for EncodedOutput<T> {
    fn start_stream(self: Box<Self>) -> Result<DataOutputTask<T>, DataStoreError> {
        // create a datasource for the encoder because it is needed to create the encoder
        // data sent to the data output will be forwarded here
        let (data_source_tx, data_source_rx): (_, DataSourceRx<T>) = channel(1);

        let given_output_jh: JoinHandle<Result<DataOutputStats, DataStoreError>> =
            tokio::spawn(async move {
                let encoded_datasource = self
                    .encoder
                    .encode_source(Box::new(data_source_rx) as Box<dyn DataSource<T>>)
                    .await;

                // forward encoded elements to the output dataoutput
                let (mut encoded_datasource_rx, encoded_datasource_jh) =
                    encoded_datasource.start_stream()?;
                let (final_data_output_tx, final_data_output_jh) =
                    self.output.start_stream()?;
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
                    DataStoreError::FatalIO(format!(
                        "Error inside the DataOutput: {}",
                        er.to_string()
                    ))
                })?)
            });

        // this part receives the dataoutput messages and forwards them to the datasource
        // which can be given to the encoder, but running into problem due to encode_source method
        // wants to consume the encoder but we are in a &mut environment here
        // pass the input_rx stream into the encoder
        let (input_tx, mut input_rx) = channel(1);
        let output_name = String::from("EncodedOutput");
        let jh: JoinHandle<Result<DataOutputStats, DataStoreError>> = tokio::spawn(async move {
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

    #[async_trait]
    impl<I: Serialize + Debug + 'static + Send> EncodeStream<I, Bytes> for CsvStringEncoder {
        async fn encode_source(
            self: Box<Self>,
            source: Box<dyn DataSource<I>>,
        ) -> Box<dyn DataSource<Bytes>> {
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
                    let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
                        tokio::spawn(async move {
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
                                        wrt.serialize(content).map_err(|er| {
                                            DataStoreError::FatalIO(er.to_string())
                                        })?;
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
                            Ok(DataSourceStats { lines_scanned })
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
}
