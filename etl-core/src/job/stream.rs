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

impl Default for StepStreamStatus {
    fn default() -> Self {
        StepStreamStatus::New
    }
}
