use super::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "status")]
pub enum StepStreamStatus {
    New,
    Complete {
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
        total_lines_scanned: usize,
        #[serde(default)]
        total_lines_written: usize,
        num_errors: usize,
        files: HashMap<String, FileStatus>,
    },
    InProgress {
        started: DateTime<Utc>,
        total_lines_scanned: usize,
        num_errors: usize,
        files: HashMap<String, FileStatus>,
    },
    Error {
        message: String,
        datetime: DateTime<Utc>,
        num_errors: usize,
        last_index: usize,
        files: HashMap<String, FileStatus>,
    },
}

impl StepStreamStatus {
    pub fn started_on(&self) -> DateTime<Utc> {
        match self {
            StepStreamStatus::New => Utc::now(),
            StepStreamStatus::InProgress { started, .. } => started.clone(),
            StepStreamStatus::Complete { started, .. } => started.clone(),
            StepStreamStatus::Error { .. } => Utc::now(),
        }
    }

    pub fn new_in_progress() -> Self {
        StepStreamStatus::InProgress {
            started: Utc::now(),
            total_lines_scanned: 0,
            num_errors: 0,
            files: HashMap::new(),
        }
    }

    pub fn in_progress(&mut self) {
        *self = StepStreamStatus::new_in_progress();
    }

    pub fn complete(&mut self, stats: DataOutputStats) {
        match self {
            StepStreamStatus::InProgress {
                ref started,
                ref total_lines_scanned,
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StepStreamStatus::Complete {
                    started: started.to_owned(),
                    finished: Utc::now(),
                    total_lines_scanned: *total_lines_scanned,
                    total_lines_written: stats.lines_written,
                    num_errors: *num_errors,
                    files: files.clone(),
                }
            }
            _ => panic!("Can't set lines scanned on this StepStreamStatus."),
        }
    }

    pub fn set_total_lines(&mut self, count: usize) {
        match self {
            StepStreamStatus::New => {
                *self = StepStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: count,
                    num_errors: 0,
                    files: HashMap::new(),
                };
            }
            StepStreamStatus::InProgress {
                ref mut total_lines_scanned,
                ..
            } => {
                *total_lines_scanned = count;
            }
            StepStreamStatus::Complete {
                ref mut total_lines_scanned,
                ..
            } => {
                *total_lines_scanned = count;
            }
            StepStreamStatus::Error { .. } => {
                *self = StepStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: count,
                    num_errors: 0,
                    files: HashMap::new(),
                };
            }
        };
    }

    pub fn get_total_lines(&self) -> usize {
        match self {
            StepStreamStatus::New => 0_usize,
            StepStreamStatus::InProgress {
                ref total_lines_scanned,
                ..
            } => *total_lines_scanned,
            StepStreamStatus::Complete {
                ref total_lines_scanned,
                ..
            } => *total_lines_scanned,
            StepStreamStatus::Error { ref last_index, .. } => *last_index,
        }
    }
    pub fn incr_line(&mut self, f_name: &str) {
        match self {
            StepStreamStatus::New => {
                let mut files = HashMap::new();
                files.insert(
                    f_name.to_owned(),
                    FileStatus::Info {
                        started: Utc::now(),
                        num_ok: 1,
                    },
                );
                *self = StepStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 1,
                    num_errors: 0,
                    files,
                };
            }
            StepStreamStatus::InProgress {
                ref mut total_lines_scanned,
                ref mut files,
                ..
            } => {
                *total_lines_scanned += 1;
                FileStatus::incr_file_line(files, f_name);
            }
            StepStreamStatus::Complete {
                ref mut total_lines_scanned,
                ref mut files,
                ..
            } => {
                *total_lines_scanned += 1;
                FileStatus::incr_file_line(files, f_name);
            }
            StepStreamStatus::Error { .. } => {
                let mut files: HashMap<String, FileStatus> = HashMap::new();
                FileStatus::incr_file_line(&mut files, f_name);
                *self = StepStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 1,
                    num_errors: 0,
                    files,
                };
            }
        };
    }

    pub fn incr_error(&mut self) {
        match self {
            StepStreamStatus::New => {
                let files = HashMap::new();
                *self = StepStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 0,
                    num_errors: 1,
                    files,
                };
            }
            StepStreamStatus::InProgress {
                ref mut num_errors, ..
            } => {
                *num_errors += 1;
            }
            StepStreamStatus::Complete {
                ref mut num_errors, ..
            } => {
                *num_errors += 1;
            }
            StepStreamStatus::Error { .. } => {
                let files: HashMap<String, FileStatus> = HashMap::new();
                *self = StepStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 0,
                    num_errors: 1,
                    files,
                };
            }
        };
    }

    pub fn set_error<T: Into<String>>(&mut self, msg: T, last_index: usize) {
        match self {
            StepStreamStatus::New => {
                *self = StepStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: 1,
                    files: HashMap::new(),
                };
            }
            StepStreamStatus::InProgress {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StepStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
            StepStreamStatus::Complete {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StepStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
            StepStreamStatus::Error {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StepStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
        }
    }
}

impl Default for StepStreamStatus {
    fn default() -> Self {
        StepStreamStatus::New
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "state")]
pub enum FileStatus {
    Info {
        started: DateTime<Utc>,
        num_ok: usize,
    },
    Error {
        message: String,
        datetime: DateTime<Utc>,
        index: usize,
    },
}

impl FileStatus {
    pub fn incr_num_ok(&mut self) {
        match self {
            FileStatus::Info { ref mut num_ok, .. } => {
                *num_ok += 1;
            }
            FileStatus::Error { .. } => {
                panic!("Tried to increment a FileStatus that already has an error");
            }
        };
    }

    pub fn incr_file_line(files: &mut HashMap<String, FileStatus>, f_name: &str) {
        match files.get_mut(f_name) {
            None => {
                files.insert(
                    f_name.to_owned(),
                    FileStatus::Info {
                        started: Utc::now(),
                        num_ok: 1,
                    },
                );
            }
            Some(file) => {
                file.incr_num_ok();
            }
        };
    }
}

