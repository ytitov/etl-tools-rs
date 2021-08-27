use super::*;
use chrono::Utc;
use tokio::sync::mpsc::UnboundedSender;
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
        Err(e) => println!("Error logging: {}", e),
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
        Err(e) => println!("Error logging: {}", e),
        _ => {}
    }
}
