use super::*;
use crate::transformer::{TransformFuncIdx, TransformSource};
use ::csv::ReaderBuilder;
use std::sync::Arc;
use std::sync::Mutex;

pub struct CsvDecoder {
    pub csv_options: CsvReadOptions,
}

impl CsvDecoder {
    pub fn new<T>(
        csv_options: CsvReadOptions,
        source: Box<dyn DataSource<Bytes>>,
    ) -> Box<dyn DataSource<T>>
    where
        T: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        DecodeStream::decode_source(CsvDecoder { csv_options }, source)
    }

    // this is most deffinitely awkward...
    pub fn decode_bytes_source<'a, DS, O>(
        opts: CsvReadOptions,
        source: DS,
    ) -> TransformSource<'a, Bytes, O>
    where
        for<'_a> DS: DataSource<'_a, Bytes>,
        O: Send + Sync + DeserializeOwned + 'static,
    {
        let o = Arc::new(opts);
        let headers = Arc::new(Mutex::new("".to_string()));
        TransformSource::new(
            source,
            TransformFuncIdx::new(move |idx, content: Bytes| {
                let headers = &headers;
                let opts = &o;
                let has_headers = opts.has_headers;
                let delimiter = opts.delimiter;
                let flexible = opts.flexible;
                let terminator = opts.terminator;
                let quote = opts.quote;
                let escape = opts.escape;
                let double_quote = opts.double_quote;
                let quoting = opts.quoting;
                let comment = opts.comment;
                let line = std::string::String::from_utf8_lossy(&*content).to_string();
                if idx == 0 && has_headers {
                    let headers_str = std::string::String::from_utf8_lossy(&*content).to_string();
                    let mut h = headers
                        .lock()
                        .map_err(|e| DataStoreError::FatalIO(e.to_string()))?;
                    h.clear();
                    h.push_str(&headers_str);
                    drop(h);
                }
                let data;
                match has_headers {
                    true => {
                        let h = headers
                            .lock()
                            .map_err(|e| DataStoreError::FatalIO(e.to_string()))?;
                        data = format!("{}\n{}", h, line);
                        drop(h);
                    }
                    false => {
                        data = line.to_string();
                    }
                };
                let rdr = ReaderBuilder::new()
                    .delimiter(delimiter)
                    .has_headers(has_headers)
                    .flexible(flexible)
                    .terminator(terminator)
                    .quote(quote)
                    .escape(escape)
                    .double_quote(double_quote)
                    .quoting(quoting)
                    .comment(comment)
                    .from_reader(data.as_bytes());
                let mut iter = rdr.into_deserialize::<O>();
                match iter.next() {
                    Some(result) => match result {
                        Ok(item) => {
                            return Ok(item);
                        }
                        Err(er) => {
                            return Err(DataStoreError::Deserialize {
                                message: er.to_string(),
                                attempted_string: line.to_string(),
                            });
                        }
                    },
                    None => {
                        return Err(DataStoreError::Deserialize {
                            message: "Could not pull out a CSV object from the line".into(),
                            attempted_string: "".into(),
                        });
                    }
                }
            }),
        )
    }
}

impl<T: DeserializeOwned + Debug + 'static + Send + Sync> DecodeStream<T> for CsvDecoder {
    fn decode_source(self, source: Box<dyn DataSource<Bytes>>) -> Box<dyn DataSource<T>> {
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);

        let source_name = source.name();
        match source.start_stream() {
            Ok((mut source_rx, source_stream_jh)) => {
                let CsvReadOptions {
                    delimiter,
                    has_headers,
                    flexible,
                    terminator,
                    quote,
                    escape,
                    double_quote,
                    quoting,
                    comment,
                } = self.csv_options;
                let jh: DataSourceJoinHandle = tokio::spawn(async move {
                    let mut headers_str = String::from("");
                    let mut lines_scanned = 0_usize;
                    loop {
                        match source_rx.recv().await {
                            Some(Ok(DataSourceMessage::Data { source, content })) => {
                                if has_headers == true && lines_scanned == 0 {
                                    headers_str =
                                        std::string::String::from_utf8_lossy(&*content).to_string();
                                } else {
                                    let line = std::string::String::from_utf8_lossy(&*content);
                                    let data = match has_headers {
                                        true => format!("{}\n{}", headers_str, line),
                                        false => line.to_string(),
                                    };
                                    let rdr = ReaderBuilder::new()
                                        .delimiter(delimiter)
                                        .has_headers(has_headers)
                                        .flexible(flexible)
                                        .terminator(terminator)
                                        .quote(quote)
                                        .escape(escape)
                                        .double_quote(double_quote)
                                        .quoting(quoting)
                                        .comment(comment)
                                        .from_reader(data.as_bytes());
                                    let mut iter = rdr.into_deserialize::<T>();
                                    match iter.next() {
                                        Some(result) => match result {
                                            Ok(item) => {
                                                tx.send(Ok(DataSourceMessage::new(&source, item)))
                                                    .await
                                                    .map_err(|e| {
                                                        DataStoreError::send_error(
                                                            &source,
                                                            "CsvDecoder",
                                                            e,
                                                        )
                                                    })?;
                                            }
                                            Err(er) => {
                                                match tx
                                                    .send(Err(DataStoreError::Deserialize {
                                                        message: er.to_string(),
                                                        attempted_string: line.to_string(),
                                                    }))
                                                    .await
                                                {
                                                    Ok(_) => {
                                                        //sent_count += 1;
                                                    }
                                                    Err(e) => {
                                                        return Err(DataStoreError::send_error(
                                                            &source, "", e,
                                                        ));
                                                    }
                                                }
                                            }
                                        },
                                        None => {
                                            break;
                                        }
                                    }
                                }
                                lines_scanned += 1;
                            }
                            Some(Err(e)) => {
                                println!("An error happened in CsvDecoder: {}", e);
                                // TODO: this error does not seem to stop the pipeline
                                return Err(e);
                            }
                            None => {
                                break;
                            }
                        };
                    }
                    source_stream_jh.await??;
                    Ok(DataSourceDetails::Basic { lines_scanned })
                });
                return Box::new(DecodedSource {
                    source_name,
                    ds_task_result: Ok((rx, jh)),
                });
            }
            Err(er) => {
                return Box::new(DecodedSource {
                    source_name,
                    ds_task_result: Err(er),
                });
            }
        }
    }
}
