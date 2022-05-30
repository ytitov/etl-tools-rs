use super::*;
use csv::ReaderBuilder;

/// Designed as a "null" datastore target.  Items received are not written anywhere
pub struct MockCsvDataSource {
    pub name: String,
    pub lines: Vec<String>,
    pub csv_options: CsvReadOptions,
}

impl Default for MockCsvDataSource {
    fn default() -> Self {
        MockCsvDataSource {
            name: String::from("MockCsvDataSource"),
            lines: Vec::new(),
            csv_options: CsvReadOptions::default(),
        }
    }
}

impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static>
    DataSource<T> for MockCsvDataSource
{
    fn name(&self) -> String {
        format!("MockCsvDataSource-{}", &self.name)
    }

    fn start_stream(self: Box<Self>) -> Result<DataSourceTask<T>, DataStoreError> {
        use tokio::sync::mpsc::channel;
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
        // could make this configurable
        let (tx, rx) = channel(1);
        let name = String::from("MockJsonDataSource");
        let jh: DataSourceJoinHandle =
            tokio::spawn(async move {
                let mut count = 0;
                let lines = self.lines;
                let mut headers_str = String::new();
                let mut lines_scanned = 0_usize;
                for line in lines {
                    if count == 0 {
                        headers_str = line.clone();
                        count += 1;
                    } else {
                        let data = format!("{}\n{}", headers_str, line);
                        //let rdr = Reader::from_reader(data.as_bytes());
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
                            Some(Ok(item)) => {
                                tx.send(Ok(DataSourceMessage::new(
                                    "MockJsonDataSource",
                                    item,
                                )))
                                .await
                                .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                                lines_scanned += 1;
                            }
                            Some(Err(er)) => {
                                tx.send(Err(DataStoreError::Deserialize {
                                    message: er.to_string(),
                                    attempted_string: line.to_string(),
                                }))
                                .await
                                .map_err(|e| DataStoreError::send_error(&name, "", e))?;
                                lines_scanned += 1;
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
                Ok(DataSourceDetails::Basic { lines_scanned })
            });
        Ok((rx, jh))
    }
}
