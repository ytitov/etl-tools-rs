use super::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use chrono::Utc;
use crate::deps::log;
pub type LogMessage = CsvMessage<LogEntry>;

#[derive(Debug, Serialize)]
pub struct LogEntry {
    pub ts: String,
    pub entry_type: LogEntryType,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub enum LogEntryType {
    Info,
    Error,
}

pub fn log_info<T>(tx: &UnboundedSender<CsvMessage<LogEntry>>, msg: T)
where
    T: Into<String>,
{
    match tx.send(CsvMessage {
        content: LogEntry {
            ts: format!("{}", Utc::now().format("%D %T")),
            message: msg.into(),
            entry_type: LogEntryType::Info,
        },
    }) {
        Err(e) => log::error!("Error logging: {}", e),
        _ => {}
    }
}

pub fn log_err<T>(tx: &UnboundedSender<CsvMessage<LogEntry>>, msg: T)
where
    T: Into<String>,
{
    match tx.send(CsvMessage {
        content: LogEntry {
            ts: format!("{}", Utc::now().format("%D %T")),
            message: msg.into(),
            entry_type: LogEntryType::Error,
        },
    }) {
        Err(e) => log::error!("Error logging: {}", e),
        _ => {}
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

    thread::spawn(move || {
        log::info!("tx_to_stdout_output logger started");
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

    let s = {
        if add_date {
            format!("{}.{}.csv", file_name, Utc::now().date())
        } else {
            format!("{}.csv", file_name)
        }
    };
    log::info!("Creating log file {}", &s);
    let p = Path::new(&s);
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
        log::info!("logger started");
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
