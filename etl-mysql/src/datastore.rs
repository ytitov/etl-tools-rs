use etl_core::datastore::*;
use etl_core::deps::anyhow;
use etl_core::deps::async_trait;
use etl_core::deps::tokio;
use etl_core::deps::tokio::sync::mpsc::channel;
use etl_core::deps::tokio::task::JoinHandle;
use etl_core::job_manager::JobManagerTx;
use etl_core::preamble::*;
use etl_core::utils;
use serde::de;
use serde::Deserialize;
use serde::Serialize;
pub use sqlx;
pub use sqlx::mysql::MySqlPoolOptions;
pub use sqlx::MySqlPool;
use std::fmt::Debug;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct MySqlDataOutput {
    // TODO: scaling may never need to happen
    // remove it
    pub on_put_num_rows_max: usize,
    pub on_put_num_rows: usize,
    pub table_name: String,
    pub db_name: String,
    pub pool: MySqlDataOutputPool,
}

#[derive(Clone)]
/// can either pass the pool or ask for one to be created
pub enum MySqlDataOutputPool {
    Pool(MySqlPool),
    CreatePool {
        max_connections: u8,
        user: String,
        pw: String,
        host: String,
        port: String,
    },
}

impl Default for MySqlDataOutputPool {
    fn default() -> Self {
        MySqlDataOutputPool::CreatePool {
            max_connections: 2,
            user: "master".to_owned(),
            pw: "password".to_owned(),
            host: "localhost".to_owned(),
            port: "3306".to_owned(),
        }
    }
}

#[async_trait]
impl<T: Serialize + std::fmt::Debug + Send + Sync + 'static> DataOutput<T> for MySqlDataOutput {
    async fn start_stream(&mut self, mut jm_tx: JobManagerTx) -> anyhow::Result<DataOutputTask<T>> {
        let (tx, mut rx): (DataOutputTx<T>, _) = channel(self.on_put_num_rows * 2);

        let table_name = self.table_name.clone();
        let on_put_num_rows_orig = *&self.on_put_num_rows;
        let on_put_num_rows_max = *&self.on_put_num_rows_max;
        let db_name = self.db_name.clone();
        let pool_config = self.pool.clone();
        let join_handle: JoinHandle<anyhow::Result<DataOutputStats>> = tokio::spawn(async move {
            let mut time_started = std::time::Instant::now();
            let mut value_rows: Vec<String> = Vec::with_capacity(on_put_num_rows_max);
            let pool = match pool_config {
                MySqlDataOutputPool::Pool(p) => p.clone(),
                MySqlDataOutputPool::CreatePool {
                    max_connections,
                    user,
                    pw,
                    host,
                    port,
                } => {
                    MySqlPoolOptions::new()
                        // TODO: pass these params into config
                        .max_connections(max_connections as u32)
                        // 3 hours timeout
                        //.connect_timeout(std::time::Duration::from_secs(60))
                        //.min_connections(1)
                        //.idle_timeout(Some(std::time::Duration::from_secs(60 * 10)))
                        //.max_lifetime(Some(std::time::Duration::from_secs(60 * 60 * 2)))
                        .after_connect(|_conn| {
                            Box::pin(async move {
                                println!("MySql connection established");
                                Ok(())
                            })
                        })
                        /*
                        // connect lazy appears to be massively slowing things down
                        .connect_lazy(&format!(
                            "mysql://{}:{}@{}:{}/{}",
                            user, pw, host, port, &db_name
                        ))?,
                        */
                        .connect(&format!(
                            "mysql://{}:{}@{}:{}/{}",
                            user, pw, host, port, &db_name
                        ))
                        .await?
                }
            };
            let mut columns: Vec<String> = vec![];
            let mut num_bytes = 0_usize;
            let mut total_inserted = 0_usize;
            let /*mut*/ on_put_num_rows = on_put_num_rows_orig;
            let mut source_finished_sending = false;
            loop {
                if num_bytes >= 4_000_000 {
                    println!(" for table {} Packet exceeded 4mb consider reducing max commit size or setting the server max_allowed_packet", &table_name);
                }
                // 1. loop until we have enough rows
                loop {
                    if value_rows.len() < on_put_num_rows && num_bytes < 4_000_000 {
                        match rx.recv().await {
                            Some(DataOutputMessage::Data(data)) => {
                                let mut vals = vec![];
                                if let Ok(keyvals) = utils::key_values(&data) {
                                    columns.clear();
                                    for (key, val) in keyvals {
                                        columns.push(key);
                                        vals.push(val.clone());
                                    }
                                    let v = vals.join(",");
                                    num_bytes += std::mem::size_of_val(&v);
                                    value_rows.push(v);
                                }
                            }
                            Some(DataOutputMessage::NoMoreData) => {
                                break;
                            }
                            None => {
                                source_finished_sending = true;
                                // get out because source is done
                                break;
                            }
                        }
                    } else {
                        // break out so we can write the rows to db
                        break;
                    }
                }
                // 2. if there are rows, send them to db
                if (value_rows.len() >= on_put_num_rows || source_finished_sending)
                    && value_rows.len() > 0
                {
                    match exec_rows_mysql(
                        &mut jm_tx,
                        &pool,
                        &db_name,
                        &table_name,
                        &columns,
                        &value_rows,
                        0,
                    )
                    .await
                    {
                        Ok(r) => {
                            total_inserted += r.inserted;
                            if time_started.elapsed().as_secs() >= 60 {
                                time_started = std::time::Instant::now();
                                let m = format!(
                                    "{} exec_rows_mysql inserted {} and took: {} ms",
                                    &table_name,
                                    &total_inserted,
                                    r.duration.as_millis()
                                );
                                jm_tx.send(Message::log_info("mysql-datastore", m)).await?;
                            }
                        }
                        Err(e) => {
                            let m = format!(
                                "Error inserting rows into {}: {} will try to insert one by one",
                                &table_name, e
                            );
                            jm_tx.send(Message::log_err("mysql-datastore", m)).await?;
                            match exec_rows_mysql(
                                &mut jm_tx,
                                &pool,
                                &db_name,
                                &table_name,
                                &columns,
                                &value_rows,
                                1,
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(er) => {
                                    let m = format!(
                                        "Error inserting rows into {}: {} ",
                                        &table_name, er
                                    );
                                    jm_tx.send(Message::log_err("mysql-datastore", m)).await?;
                                }
                            }
                        }
                    };
                    value_rows.clear();
                    num_bytes = 0;
                }
                if source_finished_sending {
                    break;
                }
            } // end loop
              // finish the inserts if there are any more rows left
            let msg = format!(
                "Finished receiving for {}.  Inserting remaining {} records into db to finish up",
                &table_name,
                value_rows.len()
            );
            jm_tx
                .send(Message::log_info("mysql-datastore", msg))
                .await?;
            drop(pool);
            let m = format!("{} inserted {} entries", table_name, total_inserted);
            jm_tx.send(Message::log_info("mysql-datastore", m)).await?;
            Ok(DataOutputStats {
                name: format!("{}.{}", db_name, table_name),
                lines_written: total_inserted,
            })
        });
        Ok((tx, join_handle))
    }

    /*
    async fn shutdown(self: Box<Self>, _: &JobRunner) {
    }
    */
}

#[derive(Debug)]
/// output from exec_rows_mysql
pub struct ExecRowsOutput {
    pub inserted: usize,
    pub duration: Duration,
}

pub async fn exec_rows_mysql(
    jm_tx: &mut JobManagerTx,
    pool: &sqlx::MySqlPool,
    db_name: &str,
    table_name: &str,
    columns: &Vec<String>,
    value_rows: &Vec<String>,
    step: usize,
) -> Result<ExecRowsOutput, sqlx::Error> {
    let mut value_rows_buf: Vec<&String> = Vec::with_capacity(step);
    let mut count_ok = 0_usize;
    let instance = Instant::now();
    if step > 0 {
        for ref value_row in value_rows {
            value_rows_buf.push(&value_row);
            if value_rows_buf.len() == step {
                let query = format!(
                    "INSERT INTO `{}`.`{}` ({}) \nVALUES \n{}",
                    db_name,
                    table_name,
                    columns
                        .iter()
                        .map(|s| format!("`{}`", s))
                        .collect::<Vec<String>>()
                        .join(","),
                    value_rows_buf
                        .iter()
                        .map(|s| format!("({})", s))
                        .collect::<Vec<String>>()
                        .join(",")
                );
                //println!("QUERY: {}", &query);
                match sqlx::query(&query).execute(pool).await {
                    Ok(_) => {
                        count_ok += value_rows_buf.len(); //println!(" ** OK ** ");
                    }
                    Err(sqlx::Error::Io(e)) => {
                        println!("Error::Io {}", e);
                    }
                    Err(e) => {
                        let m = format!("MYSQL `{}` ERROR: {}", table_name, e);
                        println!("QUERY: {}", &query);
                        let _ = jm_tx.send(Message::log_info("mysql-datastore", m)).await;
                    }
                }
                value_rows_buf.clear();
            }
        }
    } else {
        // either gets the rest of them or all of them
        value_rows_buf = value_rows.iter().map(|s| s).collect();
        let query = format!(
            "INSERT INTO `{}`.`{}` ({}) \nVALUES \n{}",
            db_name,
            table_name,
            columns
                .iter()
                .map(|s| format!("`{}`", s))
                .collect::<Vec<String>>()
                .join(","),
            value_rows_buf
                .iter()
                .map(|s| format!("({})", s))
                .collect::<Vec<String>>()
                .join(",")
        );
        match sqlx::query(&query).execute(pool).await {
            Ok(_) => {
                count_ok += value_rows_buf.len(); //println!(" ** OK ** ");
            }
            Err(sqlx::Error::Io(e)) => {
                // TODO: need to figure out a way to handle these errors
                // do we just stop on the first error or allow several errors to accumulate via the
                // job manager, just like in data sources. menono, though writing is probably a bit
                // more important.  right now, for example getting error that table does not exist
                // but the JobRunner thinks records have been inserted (they're under threshold of
                // the batch size)
                println!(
                    "MYSQL IO `{}` ERROR: {}.  Did not insert {} records",
                    table_name,
                    e,
                    value_rows_buf.len()
                );
            }
            Err(e) => {
                println!(
                    "MYSQL `{}` ERROR: {}.  Did not insert {} records",
                    table_name,
                    e,
                    value_rows_buf.len()
                );
                return Err(e);
            }
        }
    }
    Ok(ExecRowsOutput {
        inserted: count_ok,
        duration: instance.elapsed(),
    })
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateMySqlDataOutput {
    #[serde(default = "CreateMySqlDataOutput::def_on_put_num_rows_max")]
    pub on_put_num_rows_max: usize,
    #[serde(default = "CreateMySqlDataOutput::def_on_put_num_rows")]
    pub on_put_num_rows: usize,
    #[serde(default = "CreateMySqlDataOutput::def_max_connections")]
    pub max_connections: u32,
    #[serde(default = "CreateMySqlDataOutput::def_timeout_sec_connect")]
    pub timeout_sec_connect: u64,
    #[serde(default = "CreateMySqlDataOutput::def_timeout_sec_idle")]
    pub timeout_sec_idle: u64,
    #[serde(default = "CreateMySqlDataOutput::def_max_lifetime_sec")]
    pub max_lifetime_sec: u64,
    pub table_name: String,
    pub db_name: String,
    pub user: String,
    pub pw: String,
    pub host: String,
    #[serde(default = "CreateMySqlDataOutput::def_port")]
    pub port: String,
}
impl CreateMySqlDataOutput {
    pub fn def_on_put_num_rows_max() -> usize {
        50
    }
    pub fn def_on_put_num_rows() -> usize {
        60
    }
    pub fn def_max_connections() -> u32 {
        1
    }
    /// 3 hours default
    pub fn def_timeout_sec_connect() -> u64 {
        //60_u64 * 60_u64 * 3_u64
        60_u64
    }
    /// 1 hr default
    pub fn def_timeout_sec_idle() -> u64 {
        60 * 10
    }
    pub fn def_max_lifetime_sec() -> u64 {
        60_u64 * 60_u64 * 3_u64
    }
    pub fn def_port() -> String {
        String::from("3306")
    }
}
#[async_trait]
impl<
        C: 'static + Send + for<'de> de::Deserializer<'de>,
        T: 'static + Sync + Send + Debug + Serialize,
    > CreateDataOutput<'_, C, T> for MySqlDataOutput
{
    async fn create_data_output(cfg: C) -> anyhow::Result<Box<dyn DataOutput<T>>> {
        match de::Deserialize::deserialize(cfg) {
            Ok::<CreateMySqlDataOutput, _>(s) => {
                let CreateMySqlDataOutput {
                    on_put_num_rows,
                    on_put_num_rows_max,
                    max_connections,
                    timeout_sec_connect,
                    timeout_sec_idle,
                    max_lifetime_sec,
                    table_name,
                    db_name,
                    user,
                    pw,
                    host,
                    port,
                } = s;
                let pool = MySqlPoolOptions::new()
                    .max_connections(max_connections as u32)
                    .connect_timeout(std::time::Duration::from_secs(timeout_sec_connect))
                    .idle_timeout(Some(std::time::Duration::from_secs(timeout_sec_idle)))
                    .max_lifetime(Some(std::time::Duration::from_secs(max_lifetime_sec)))
                    .after_connect(|_conn| {
                        Box::pin(async move {
                            //println!("MySql connection established");
                            Ok(())
                        })
                    })
                    .connect_lazy(&format!(
                        "mysql://{}:{}@{}:{}/{}",
                        user, pw, host, port, &db_name
                    ))?;
                return Ok(Box::new(MySqlDataOutput {
                    on_put_num_rows_max,
                    on_put_num_rows,
                    table_name,
                    db_name,
                    pool: MySqlDataOutputPool::Pool(pool),
                }));
            }
            Err(e) => {
                println!("ERROR running deserialize: {}", e);
            }
        };
        unimplemented!();
    }
}
