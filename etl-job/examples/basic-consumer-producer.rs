use etl_core::{
    //datastore::{fs::LocalFs, mock::MockDataOutput},
    //decoder::json::JsonDecoder,
    deps::log,
    deps::serde,
    deps::serde::Deserialize,
    deps::tokio,
    task::transform_stream::*,
    transformer::TransformFuncIdx,
    transformer::TransformerFut,
};
use etl_job::job::JobRunnerConfig;
use etl_job::job_manager::*;

#[derive(Debug, Deserialize)]
#[serde(crate = "serde")]
struct Thing {
    pub name: String,
}
#[derive(Debug, Deserialize)]
#[serde(crate = "serde")]
struct CsvRow {
    pub name: String,
    pub identifier: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        //.with_level(LevelFilter::Info)
        // specific logging only for etl_core to be info
        //.with_module_level("grpc_simple_store_server", LevelFilter::Info)
        //.with_module_level("etl_grpc::simplestore::observer::server", LevelFilter::Info)
        .env()
        .init()
        .unwrap();
    let mut job_manager = JobManagerBuilder::new(JobManagerConfig {
        max_errors: 100,
        ..Default::default()
    })
    .expect("Could not initialize job_manager")
    .into_job_manager();

    let csv_sample = r#"name,identifier
boba feata,af-222
donald duck,"234324""#
        .to_string();

    let input1 = r#"
    {"name": "boy"}
    {"name": "thing"}
    needs to cause an error"#
        .to_string();

    let input2 = r#"hi
    these
    {"name": "thing"}
    are a bunch of 
    lines"#
        .to_string();

    use etl_core::decoder::csv::CsvDecoder;
    use etl_core::datastore::CsvReadOptions;
    let t1 = TransformStream::new(
        CsvDecoder::from_bytes_source(CsvReadOptions::default(), csv_sample),
        Vec::new(),
        |s: CsvRow| {
            log::info!("got s {:?}", &s);
            Ok(format!("adding: {:?}", s))
        },
    );

    let t2 = TransformStream::new(
        input1,
        Vec::new(),
        TransformFuncIdx::new(|idx, s: String| Ok(format!("[{}] -- {}", idx, s))),
    );

    use etl_core::decoder::json::JsonDecoder;
    let t3 = TransformStream::new(
        JsonDecoder::from_bytes_source(input2),
        Vec::new(),
        |s: Thing| {
            //Ok(99)
            log::info!("got s {:?}", &s);
            Ok(format!("adding: {:?}", s))
        },
    );

    job_manager.run_task(t1)?;
    job_manager.run_task(t2)?;
    job_manager.run_task(t3)?;
    job_manager.start().await?;
    Ok(())
}
