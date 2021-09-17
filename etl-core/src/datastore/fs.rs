use super::*;
use csv::ReaderBuilder;
use std::path::Path;

pub struct LocalFsDataSource {
    pub read_content: ReadContentOptions,
    pub write_content: WriteContentOptions,
    pub files: Vec<String>,
    pub home: String,
}

impl Default for LocalFsDataSource {
    fn default() -> Self {
        LocalFsDataSource {
            read_content: ReadContentOptions::Json,
            write_content: WriteContentOptions::Json,
            files: Vec::new(),
            home: "./".to_string(),
        }
    }
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static>
    DataSource<T> for LocalFsDataSource
{

    fn name(&self) -> String {
        format!("LocalFsDataSource-{}", &self.home)
    }

    fn start_stream(
        mut self: Box<Self>,
    ) -> Result<DataSourceTask<T>, DataStoreError> {
        use ReadContentOptions::*;
        let name = String::from("LocalFsDataSource");
        match self.read_content.clone() {
            Json => self.start_stream_json::<T>(name),
            Csv(options) => self.start_stream_csv::<T>(options, name),
            Text => {
                unimplemented!()
            }
        }
    }
}

#[async_trait]
impl<T: Serialize + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static>
    SimpleStore<T> for LocalFsDataSource
{
    /*
    async fn read_file_str(&self, path: &str) -> Result<String, DataStoreError> {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;
        let mut file = File::open(Path::new(&self.home).join(path)).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        let s = std::string::String::from_utf8_lossy(&contents);
        Ok(s.into_owned())
    }
    */
    async fn load(&self, path: &str) -> Result<T, DataStoreError> {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;
        let p = Path::new(&self.home).join(path);
        if false == p.exists() {
            return Err(DataStoreError::NotExist {
                key: format!("{:?}", p),
                error: "Path does not exist".to_string(),
            });
        }
        let mut file = File::open(p).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        let s = std::string::String::from_utf8_lossy(&contents);
        match serde_json::from_str::<T>(&s) {
            Ok::<T, _>(obj) => Ok(obj),
            Err(e) => Err(DataStoreError::Deserialize {
                attempted_string: format!("Could not deserialize {}", path),
                message: e.to_string(),
            }),
        }
    }

    async fn write(&self, path: &str, item: T) -> Result<(), DataStoreError> {
        use tokio::fs::OpenOptions;
        use tokio::io::AsyncWriteExt;
        tokio::fs::create_dir_all(Path::new(&self.home)).await?;
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(Path::new(&self.home).join(path))
            .await?;
        match serde_json::to_string_pretty(&item) {
            Ok(content) => match file.write_all(content.as_bytes()).await {
                Ok(()) => Ok(()),
            Err(err) => Err(DataStoreError::FatalIO(err.to_string())),
            },
            Err(err) => Err(DataStoreError::FatalIO(err.to_string())),
        }
    }
}

impl LocalFsDataSource {
    pub fn load_toml<T>(p: &str, autocreate: bool) -> anyhow::Result<T>
    where
        T: Serialize + DeserializeOwned + Default + Debug,
    {
        use anyhow::anyhow;
        use std::fs::File;
        use std::io::prelude::*;
        match File::open(Path::new(&p)) {
            Ok(mut file) => {
                let mut cont = String::new();
                file.read_to_string(&mut cont)?;
                match toml::from_str(&cont) {
                    Ok(cfg) => Ok(cfg),
                    Err(err) => Err(anyhow!("There is an error in your config: {}", err)),
                }
            }
            Err(err) => {
                if autocreate == true {
                    let cfg = T::default();
                    println!("Creating default config: {:?}", &cfg);
                    let mut f = File::create(&p)?;
                    f.write_all(toml::Value::try_from(&cfg)?.to_string().as_bytes())?;
                    Ok(cfg)
                } else {
                    Err(anyhow!("Error opening Configuration file: {}", err))
                }
            }
        }
    }

    fn start_stream_json<T>(
        &mut self,
        name: String,
    ) -> Result<DataSourceTask<T>, DataStoreError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        use tokio::fs::File;
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let files = self.files.clone();
        let home = self.home.clone();
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
            tokio::spawn(async move {
                let mut lines_scanned = 0_usize;
                for fname in files {
                    let file = File::open(Path::new(&home).join(&fname)).await?;
                    // 68 mb
                    let r = BufReader::with_capacity(1 << 26, file);
                    let mut lines = r.lines();
                    loop {
                        if let Some(line) = lines.next_line().await? {
                            match serde_json::from_str::<T>(&line) {
                                Ok(r) => {
                                    lines_scanned += 1;
                                    if let Err(e) = tx
                                        .send(Ok(DataSourceMessage::new(&fname, r)))
                                        .await
                                    {
                                        return Err(DataStoreError::send_error(
                                            name, &fname, e,
                                        ));
                                    }
                                }
                                Err(val) => {
                                    match tx
                                        .send(Err(DataStoreError::Deserialize {
                                            message: val.to_string(),
                                            attempted_string: line.to_string(),
                                        }))
                                        .await
                                    {
                                        Ok(_) => {
                                            //sent_count += 1;
                                        }
                                        Err(e) => {
                                            return Err(DataStoreError::send_error(
                                                name, "", e,
                                            ));
                                        }
                                    }
                                }
                            };
                        } else {
                            break;
                        }
                    }
                }
                Ok(DataSourceStats { lines_scanned })
            });
        Ok((rx, jh))
    }

    fn start_stream_csv<T>(
        &mut self,
        csv_options: CsvReadOptions,
        name: String,
    ) -> Result<DataSourceTask<T>, DataStoreError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        use tokio::fs::File;
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::sync::mpsc::channel;
        let (tx, rx) = channel(1);
        let files = self.files.clone();
        let home = self.home.clone();
        //println!("Options: {:?}", &csv_options);
        //panic!("exiting");
        let mut headers_str = String::new();
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
        } = csv_options;
        let jh: JoinHandle<Result<DataSourceStats, DataStoreError>> =
            tokio::spawn(async move {
                let mut lines_scanned = 0_usize;
                for fname in files {
                    let mut count = 0_usize;
                    let file = File::open(Path::new(&home).join(&fname)).await?;
                    // 68 mb
                    let r = BufReader::with_capacity(1 << 26, file);
                    let mut lines = r.lines();
                    loop {
                        if let Some(line) = lines.next_line().await? {
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
                                    Some(result) => match result {
                                        Ok(item) => {
                                            tx.send(Ok(DataSourceMessage::new(
                                                &fname, item,
                                            )))
                                            .await
                                            .map_err(|e| {
                                                DataStoreError::send_error(&name, &fname, e)
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
                                                    return Err(
                                                        DataStoreError::send_error(
                                                            name, "", e,
                                                        ),
                                                    );
                                                }
                                            }
                                        }
                                    },
                                    None => {
                                        break;
                                    }
                                }
                                lines_scanned += 1;
                            }
                        } else {
                            break;
                        }
                    }
                }
                Ok(DataSourceStats { lines_scanned })
            });
        Ok((rx, jh))
    }
}
