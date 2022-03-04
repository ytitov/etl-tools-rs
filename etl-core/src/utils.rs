use csv::WriterBuilder;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::Path;
use std::thread;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use toml;

pub mod log;

pub fn load_toml<T>(p: &str, autocreate: bool) -> anyhow::Result<T>
where
    T: DeserializeOwned + Serialize + Default,
{
    use anyhow::anyhow;
    use std::fs::File;
    use std::io::prelude::*;
    match File::open(&p) {
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
                let mut f = File::create(&p)?;
                f.write_all(toml::Value::try_from(&cfg)?.to_string().as_bytes())?;
                Ok(cfg)
            } else {
                Err(anyhow!("Error opening Configuration file: {}", err))
            }
        }
    }
}

#[derive(Debug)]
pub struct CsvMessage<T: Serialize + Send + Sync> {
    pub content: T,
}

pub fn tx_to_stdout_output<T: Serialize + std::fmt::Debug + Send + Sync + 'static>(
) -> anyhow::Result<UnboundedSender<CsvMessage<T>>> {
    let (tx, mut rx): (
        UnboundedSender<CsvMessage<T>>,
        UnboundedReceiver<CsvMessage<T>>,
    ) = unbounded_channel();
    use ::log;

    thread::spawn(move || {
        println!("tx_to_stdout_output logger started");
        loop {
            match rx.blocking_recv() {
                Some(data) => {
                    log::debug!("{:?}", data.content);
                }
                None => {
                    log::debug!("Closing rx_to_stdout_output");
                    break;
                }
            }
        }
    });

    Ok(tx)
}

pub fn tx_to_csv_output<T: Serialize + std::fmt::Debug + Send + Sync + 'static>(
    file_name: &str,
    add_date: bool,
) -> anyhow::Result<UnboundedSender<CsvMessage<T>>> {
    let (tx, mut rx): (
        UnboundedSender<CsvMessage<T>>,
        UnboundedReceiver<CsvMessage<T>>,
    ) = unbounded_channel();

    //let mut idx = series.clone();
    //let mut p = Path::new(base_folder).join(format!("{}.{}.csv", file_name, idx));
    //let s = format!("{}.csv", file_name);
    let s = {
        use chrono::Utc;
        if add_date {
            format!("{}.{}.csv", file_name, Utc::now().date())
        } else {
            format!("{}.csv", file_name)
        }
    };
    println!("Creating log file {}", &s);
    let p = Path::new(&s);
    /*
    while Path::new(&p).exists() {
        idx += 1;
        //p = Path::new(base_folder).join(format!("{}.{}.csv", file_name, idx));
        s = format!("{}.{}.csv", file_name, idx);
        p = Path::new(&s);
    }
    */
    let csv_file = std::fs::OpenOptions::new()
        .truncate(false)
        .append(true)
        .create(true)
        .open(p)?;
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::NonNumeric)
        .double_quote(false)
        .escape(b'\\')
        // 67 mb per file
        .buffer_capacity(1 << 26)
        .from_writer(csv_file);

    thread::spawn(move || {
        println!("logger started");
        loop {
            match rx.blocking_recv() {
                Some(data) => {
                    //println!("----> got some date {:?}", &data);
                    if let Err(e) = wtr.serialize(&data.content) {
                        println!("ERROR writing to file {:?}", &e);
                    }
                    if let Ok(_) = wtr.flush() {
                    } else {
                        println!("error writing");
                    }
                }
                None => {
                    println!("no more data");
                    break;
                }
            }
        }
    });

    Ok(tx)
}

//use lazy_static::lazy_static;
pub type KeyValue = (String, String);
use anyhow;
//use regex::Regex;
//use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde_json::Value as JsonValue;


/// converts T into a serde_json::Value then walks the fields and returns
/// key value pairs as a vec of key values as strings
pub fn key_values<'de, T>(v: T) -> anyhow::Result<Vec<KeyValue>>
where
    T: Serialize,
{
    /*
    lazy_static! {
        // need to match `\"NUMBER.NUMBERS\"` and can't figure out a way to escape
        // the backslash, but this is close enough
        //TODO: add processing numbers like 9.61735854E8 and convert to decimal
        static ref RE: Regex = Regex::new(r#"^.\\"-?[0-9]*\.?[0-9]*\\".$"#).unwrap();
    }
    */
    let mut vec = vec![];
    let v = serde_json::to_value(v)?;
    match v {
        JsonValue::Object(obj) => {
            for (key, val) in obj {
                let /*mut*/ s_val = format!("{}", val).trim().to_owned();
                /*
                if RE.is_match(&s_val) {
                    s_val = s_val.replace("\\\"", "");
                }
                */

                vec.push((key, s_val));
            }
        }
        _ => panic!("Got something that does not convert to an object"),
    };
    Ok(vec)
}
