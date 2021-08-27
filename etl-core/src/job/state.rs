use crate::job::*;
use anyhow;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "state")]
pub enum StreamStatus {
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

impl Default for StreamStatus {
    fn default() -> Self {
        StreamStatus::New
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

pub const JOB_STATE_EXT: &'static str = "job.json";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobStreamsState {
    pub states: HashMap<String, StreamStatus>,
}

impl Default for JobStreamsState {
    fn default() -> Self {
        JobStreamsState {
            states: HashMap::new(),
        }
    }
}

impl JobStreamsState {
    pub fn complete<N: Into<String>>(&mut self, name: N) -> anyhow::Result<()> {
        match self.states.get_mut(&name.into()) {
            Some(js) => {
                js.complete();
                Ok(())
            }
            None => Err(anyhow::anyhow!(
                "Tried completing a stream which was never started"
            )),
        }
    }

    pub fn in_progress<N: Into<String>>(&mut self, name: N) -> anyhow::Result<()> {
        self.states
            .insert(name.into(), StreamStatus::new_in_progress());
        Ok(())
    }

    pub fn incr_num_ok<N: Into<String>>(
        &mut self,
        name: N,
        source: &str,
    ) -> anyhow::Result<()> {
        let name = name.into();
        match self.states.get_mut(&name) {
            Some(js) => {
                js.incr_line(source);
                Ok(())
            }
            None => {
                self.in_progress(name.clone())?;
                // TODO: should probably get rid of recursion.. makes me nervious
                self.incr_num_ok(name, source)?;
                Ok(())
            }
        }
    }

    pub fn incr_error<N: Into<String>>(&mut self, name: N) -> anyhow::Result<()> {
        let name = name.into();
        match self.states.get_mut(&name) {
            Some(js) => {
                js.incr_error();
                Ok(())
            }
            None => {
                self.in_progress(name.clone())?;
                // TODO: should probably get rid of recursion.. makes me nervious
                self.incr_error(name)?;
                Ok(())
            }
        }
    }

    pub fn set_total_lines<N: Into<String>>(
        &mut self,
        name: N,
        num_lines: usize,
    ) -> anyhow::Result<()> {
        match self.states.get_mut(&name.into()) {
            Some(js) => {
                js.set_total_lines(num_lines);
                Ok(())
            }
            None => Err(anyhow::anyhow!(
                "Tried increment a line on a stream which never started"
            )),
        }
    }

    pub fn set_error<N: Into<String>, M: Into<String>>(
        &mut self,
        name: N,
        message: M,
        lines_scanned: usize,
    ) -> anyhow::Result<()> {
        match self.states.get_mut(&name.into()) {
            Some(js) => {
                js.set_error(message, lines_scanned);
                Ok(())
            }
            None => Err(anyhow::anyhow!(
                "Tried increment a line on a stream which never started"
            )),
        }
    }

    pub fn is_complete<N: Into<String>>(&mut self, name: N) -> anyhow::Result<bool> {
        match self.states.get(&name.into()) {
            Some(js) => match js {
                StreamStatus::Complete { .. } => Ok(true),
                _ => Ok(false),
            },
            None => Err(anyhow::anyhow!(
                "Tried completing a stream which was never started"
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobState {
    pub commands: HashMap<String, JobCommandStatus>,
    pub settings: HashMap<String, JsonValue>,
    #[serde(default)]
    //pub streams: StreamStatus,
    pub streams: JobStreamsState,
    pub name: String,
    pub id: String,
}

impl JobState {
    pub fn new<A: Into<String>, B: Into<String>>(name: A, id: B) -> Self {
        JobState {
            commands: HashMap::new(),
            settings: HashMap::new(),
            streams: JobStreamsState::default(),
            id: id.into(),
            name: name.into(),
        }
    }

    pub fn set<K: Into<String>, V: Serialize>(
        &mut self,
        key: K,
        val: &V,
    ) -> anyhow::Result<()> {
        let v = serde_json::to_value(val)?;
        self.settings.insert(key.into(), v);
        Ok(())
    }

    pub fn get<V: DeserializeOwned>(&mut self, key: &str) -> anyhow::Result<Option<V>> {
        if let Some(val) = self.settings.get(key) {
            Ok(Some(serde_json::from_value(val.to_owned())?))
        } else {
            Ok(None)
        }
    }

    pub fn gen_name<A: Into<String>, B: Into<String>>(id: A, name: B) -> String {
        let name = format!(
            "{id}.{name}.{ext}",
            name = name.into(),
            id = id.into(),
            ext = JOB_STATE_EXT
        );
        name
    }
}

impl StreamStatus {
    pub fn new_in_progress() -> Self {
        StreamStatus::InProgress {
            started: Utc::now(),
            total_lines_scanned: 0,
            num_errors: 0,
            files: HashMap::new(),
        }
    }

    pub fn in_progress(&mut self) {
        *self = StreamStatus::new_in_progress();
    }

    pub fn complete(&mut self) {
        match self {
            StreamStatus::InProgress {
                ref started,
                ref total_lines_scanned,
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StreamStatus::Complete {
                    started: started.to_owned(),
                    finished: Utc::now(),
                    total_lines_scanned: *total_lines_scanned,
                    num_errors: *num_errors,
                    files: files.clone(),
                }
            }
            _ => panic!("Can't set lines scanned on this StreamStatus."),
        }
    }

    pub fn set_total_lines(&mut self, count: usize) {
        match self {
            StreamStatus::New => {
                *self = StreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: count,
                    num_errors: 0,
                    files: HashMap::new(),
                };
            }
            StreamStatus::InProgress {
                ref mut total_lines_scanned,
                ..
            } => {
                *total_lines_scanned = count;
            }
            StreamStatus::Complete {
                ref mut total_lines_scanned,
                ..
            } => {
                *total_lines_scanned = count;
            }
            StreamStatus::Error { .. } => {
                *self = StreamStatus::InProgress {
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
            StreamStatus::New => 0_usize,
            StreamStatus::InProgress {
                ref total_lines_scanned,
                ..
            } => *total_lines_scanned,
            StreamStatus::Complete {
                ref total_lines_scanned,
                ..
            } => *total_lines_scanned,
            StreamStatus::Error { ref last_index, .. } => *last_index,
        }
    }
    pub fn incr_line(&mut self, f_name: &str) {
        match self {
            StreamStatus::New => {
                let mut files = HashMap::new();
                files.insert(
                    f_name.to_owned(),
                    FileStatus::Info {
                        started: Utc::now(),
                        num_ok: 1,
                    },
                );
                *self = StreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 1,
                    num_errors: 0,
                    files,
                };
            }
            StreamStatus::InProgress {
                ref mut total_lines_scanned,
                ref mut files,
                ..
            } => {
                *total_lines_scanned += 1;
                FileStatus::incr_file_line(files, f_name);
            }
            StreamStatus::Complete {
                ref mut total_lines_scanned,
                ref mut files,
                ..
            } => {
                *total_lines_scanned += 1;
                FileStatus::incr_file_line(files, f_name);
            }
            StreamStatus::Error { .. } => {
                let mut files: HashMap<String, FileStatus> = HashMap::new();
                FileStatus::incr_file_line(&mut files, f_name);
                *self = StreamStatus::InProgress {
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
            StreamStatus::New => {
                let files = HashMap::new();
                *self = StreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 0,
                    num_errors: 1,
                    files,
                };
            }
            StreamStatus::InProgress {
                ref mut num_errors, ..
            } => {
                *num_errors += 1;
            }
            StreamStatus::Complete {
                ref mut num_errors, ..
            } => {
                *num_errors += 1;
            }
            StreamStatus::Error { .. } => {
                let files: HashMap<String, FileStatus> = HashMap::new();
                *self = StreamStatus::InProgress {
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
            StreamStatus::New => {
                *self = StreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: 1,
                    files: HashMap::new(),
                };
            }
            StreamStatus::InProgress {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
            StreamStatus::Complete {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
            StreamStatus::Error {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = StreamStatus::Error {
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
