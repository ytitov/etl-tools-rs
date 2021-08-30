use crate::job::*;
use anyhow;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "status")]
pub enum JobStreamStatus {
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

impl Default for JobStreamStatus {
    fn default() -> Self {
        JobStreamStatus::New
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
    pub states: HashMap<String, JobStreamStatus>,
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
            .insert(name.into(), JobStreamStatus::new_in_progress());
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
                JobStreamStatus::Complete { .. } => Ok(true),
                _ => Ok(false),
            },
            None => Err(anyhow::anyhow!(
                "Tried completing a stream which was never started"
            )),
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "state")]
pub enum RunStatus {
    InProgress,
    FatalError {
        step_index: isize,
        step_name: String,
        message: String,
    },
    Completed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "step_type")]
pub enum JobStepStatus {
    Stream(JobStreamStatus),
    Command(JobCommandStatus),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobStepDetails {
    pub step: JobStepStatus,
    pub name: String,
    pub step_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobState {
    commands: HashMap<String, JobCommandStatus>,
    pub settings: HashMap<String, JsonValue>,
    #[serde(default)]
    pub streams: JobStreamsState,
    name: String,
    id: String,
    cur_step_index: isize,
    run_status: RunStatus,
    pub step_history: HashMap<String, JobStepDetails>,
}

impl JobState {
    pub fn new<A: Into<String>, B: Into<String>>(name: A, id: B) -> Self {
        JobState {
            commands: HashMap::new(),
            settings: HashMap::new(),
            streams: JobStreamsState::default(),
            id: id.into(),
            name: name.into(),
            cur_step_index: -1,
            run_status: RunStatus::InProgress,
            step_history: HashMap::new(),
        }
    }

    pub fn get_command(&self, cmd_name: &str) -> Option<&'_ JobCommandStatus> {
        self.commands.get(cmd_name)
    }

    fn add_command<C: Into<String>>(&mut self, cmd_name: C, cmd: JobCommandStatus) {
        self.commands.insert(cmd_name.into(), cmd);
    }

    pub fn id(&self) -> &'_ str {
        &self.id
    }

    pub fn name(&self) -> &'_ str {
        &self.name
    }

    pub fn reset_step_index(&mut self) {
        self.cur_step_index = -1;
    }

    fn set_fatal_error<N: Into<String>, M: Into<String>>(&mut self, name: N, message: M) {
        self.run_status = RunStatus::FatalError {
            step_index: self.cur_step_index,
            step_name: name.into(),
            message: message.into(),
        };
    }

    pub fn start_new_cmd<N: Into<String>>(
        &mut self,
        name: N,
        jrc: &JobRunnerConfig,
    ) -> anyhow::Result<()> {
        let started = Utc::now();
        match &self.run_status {
            RunStatus::InProgress => {
                self.add_command(name, JobCommandStatus::InProgress { started });
            }
            RunStatus::Completed => {}
            RunStatus::FatalError {
                step_index: _,
                step_name: _,
                message: _,
            } => {
                if jrc.stop_on_error {
                    return Err(anyhow::anyhow!("Can't start the new job because stop_on_error flag is set to true"));
                } else {
                    self.add_command(name, JobCommandStatus::InProgress { started });
                }
            }
        }
        self.cur_step_index += 1;
        Ok(())
    }

    pub fn cmd_ok<N: Into<String>>(
        &mut self,
        name: N,
        _: &JobRunnerConfig,
    ) -> anyhow::Result<()> {
        let n = name.into();
        if let Some(cmd) = self.commands.get_mut(&n) {
            match cmd {
                JobCommandStatus::InProgress { started } => {
                    *cmd = JobCommandStatus::Complete {
                        started: *started,
                        finished: Utc::now(),
                    };
                    self.step_history.insert(
                        n.clone(),
                        JobStepDetails {
                            name: n,
                            step_index: self.cur_step_index as usize,
                            step: JobStepStatus::Command(cmd.clone()),
                        },
                    );
                    return Ok(());
                }
                _ => (),
            };
        }
        Err(anyhow::anyhow!(
            "Fatal error: the command was never started"
        ))
    }

    pub fn cmd_not_ok<N: Into<String>, M: Into<String>>(
        &mut self,
        name: N,
        m: M,
    ) -> anyhow::Result<()> {
        let n = name.into();
        let m = m.into();
        self.set_fatal_error(&n, &m);
        if let Some(cmd) = self.commands.get_mut(&n) {
            match cmd {
                JobCommandStatus::InProgress { started } => {
                    *cmd = JobCommandStatus::Error {
                        started: *started,
                        datetime: Utc::now(),
                        message: m,
                    };
                    self.step_history.insert(
                        n.clone(),
                        JobStepDetails {
                            name: n,
                            step_index: self.cur_step_index as usize,
                            step: JobStepStatus::Command(cmd.clone()),
                        },
                    );
                    return Ok(());
                }
                _ => (),
            };
        }
        Err(anyhow::anyhow!(
            "Fatal error: the command was never started"
        ))
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

impl JobStreamStatus {
    pub fn new_in_progress() -> Self {
        JobStreamStatus::InProgress {
            started: Utc::now(),
            total_lines_scanned: 0,
            num_errors: 0,
            files: HashMap::new(),
        }
    }

    pub fn in_progress(&mut self) {
        *self = JobStreamStatus::new_in_progress();
    }

    pub fn complete(&mut self) {
        match self {
            JobStreamStatus::InProgress {
                ref started,
                ref total_lines_scanned,
                ref files,
                ref num_errors,
                ..
            } => {
                *self = JobStreamStatus::Complete {
                    started: started.to_owned(),
                    finished: Utc::now(),
                    total_lines_scanned: *total_lines_scanned,
                    num_errors: *num_errors,
                    files: files.clone(),
                }
            }
            _ => panic!("Can't set lines scanned on this JobStreamStatus."),
        }
    }

    pub fn set_total_lines(&mut self, count: usize) {
        match self {
            JobStreamStatus::New => {
                *self = JobStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: count,
                    num_errors: 0,
                    files: HashMap::new(),
                };
            }
            JobStreamStatus::InProgress {
                ref mut total_lines_scanned,
                ..
            } => {
                *total_lines_scanned = count;
            }
            JobStreamStatus::Complete {
                ref mut total_lines_scanned,
                ..
            } => {
                *total_lines_scanned = count;
            }
            JobStreamStatus::Error { .. } => {
                *self = JobStreamStatus::InProgress {
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
            JobStreamStatus::New => 0_usize,
            JobStreamStatus::InProgress {
                ref total_lines_scanned,
                ..
            } => *total_lines_scanned,
            JobStreamStatus::Complete {
                ref total_lines_scanned,
                ..
            } => *total_lines_scanned,
            JobStreamStatus::Error { ref last_index, .. } => *last_index,
        }
    }
    pub fn incr_line(&mut self, f_name: &str) {
        match self {
            JobStreamStatus::New => {
                let mut files = HashMap::new();
                files.insert(
                    f_name.to_owned(),
                    FileStatus::Info {
                        started: Utc::now(),
                        num_ok: 1,
                    },
                );
                *self = JobStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 1,
                    num_errors: 0,
                    files,
                };
            }
            JobStreamStatus::InProgress {
                ref mut total_lines_scanned,
                ref mut files,
                ..
            } => {
                *total_lines_scanned += 1;
                FileStatus::incr_file_line(files, f_name);
            }
            JobStreamStatus::Complete {
                ref mut total_lines_scanned,
                ref mut files,
                ..
            } => {
                *total_lines_scanned += 1;
                FileStatus::incr_file_line(files, f_name);
            }
            JobStreamStatus::Error { .. } => {
                let mut files: HashMap<String, FileStatus> = HashMap::new();
                FileStatus::incr_file_line(&mut files, f_name);
                *self = JobStreamStatus::InProgress {
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
            JobStreamStatus::New => {
                let files = HashMap::new();
                *self = JobStreamStatus::InProgress {
                    started: Utc::now(),
                    total_lines_scanned: 0,
                    num_errors: 1,
                    files,
                };
            }
            JobStreamStatus::InProgress {
                ref mut num_errors, ..
            } => {
                *num_errors += 1;
            }
            JobStreamStatus::Complete {
                ref mut num_errors, ..
            } => {
                *num_errors += 1;
            }
            JobStreamStatus::Error { .. } => {
                let files: HashMap<String, FileStatus> = HashMap::new();
                *self = JobStreamStatus::InProgress {
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
            JobStreamStatus::New => {
                *self = JobStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: 1,
                    files: HashMap::new(),
                };
            }
            JobStreamStatus::InProgress {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = JobStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
            JobStreamStatus::Complete {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = JobStreamStatus::Error {
                    message: msg.into(),
                    datetime: Utc::now(),
                    last_index,
                    num_errors: *num_errors,
                    files: files.clone(),
                };
            }
            JobStreamStatus::Error {
                ref files,
                ref num_errors,
                ..
            } => {
                *self = JobStreamStatus::Error {
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
