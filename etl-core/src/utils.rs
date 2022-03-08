use anyhow;
use serde_json::Value as JsonValue;
use csv::WriterBuilder;
//use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::Path;
use std::thread;
//use toml;

pub mod log;

/*
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
*/


//use lazy_static::lazy_static;
pub type KeyValue = (String, String);
//use regex::Regex;
//use serde::de::{self, Deserialize, Deserializer, Visitor};


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
