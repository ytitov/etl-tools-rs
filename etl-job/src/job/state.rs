use super::stream::*;
use crate::job::*;
use anyhow;
use etl_core::deps::chrono::Utc;
use serde::{self, de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

pub const JOB_STATE_EXT: &'static str = "job.json";

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "state", crate = "serde")]
pub enum RunStatus {
    InProgress,
    FatalError {
        step_index: usize,
        step_name: String,
        message: String,
    },
    Completed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "step_type", crate = "serde")]
pub enum JobStepStatus {
    Stream(StepStreamStatus),
    Command(StepCommandStatus),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "serde")]
pub struct JobStepDetails {
    pub step: JobStepStatus,
    pub name: String,
    pub step_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "serde")]
pub struct JobState {
    pub settings: HashMap<String, JsonValue>,
    name: String,
    id: String,
    /// used for sequentual steps.  Tasks such as DataOutputTask are not associated with an index
    /// and run in parallel.  Note that, tasks between these steps will not be executed until a
    /// particular step finishes.  In this way if ordering is necessary for a task, then interleave
    /// between steps
    #[serde(skip_serializing, default)]
    cur_step_index: usize,
    run_status: RunStatus,
    // TODO: if a job runner is issued multiple steps with the same name, it will still run then
    // because it expects them to be possibly there already (because JobState) gets loaded from a
    // file if it exists.  As a side effect if a programmer makes a mistake and that command is
    // already in there, it will overwrite previously ran command by the same name.  Not sure yet
    // how to fix this issue because ideally I want it to panic.  This means we need to have a
    // runtime tracker of what ran so far and not have it be serialized when the file is ran or
    // deserialized when things are written
    /// these are sequential steps in the pipeline
    pub step_history: HashMap<String, JobStepDetails>,
    #[serde(skip_deserializing, default)]
    pub caught_errors: Vec<JobRunnerError>,
    // TODO: need to save task results
}

impl JobState {
    pub fn new<A: Into<String>, B: Into<String>>(name: A, id: B) -> Self {
        JobState {
            //commands: HashMap::new(),
            settings: HashMap::new(),
            //streams: JobStreamsState::default(),
            id: id.into(),
            name: name.into(),
            cur_step_index: 0,
            run_status: RunStatus::InProgress,
            step_history: HashMap::new(),
            caught_errors: Vec::new(),
        }
    }

    pub fn get_cur_step_index(&self) -> usize {
        self.cur_step_index
    }

    pub fn set_cur_step_index(&mut self, idx: usize) {
        self.cur_step_index = idx;
    }

    pub fn get_command(&self, cmd_name: &str) -> Option<(usize, StepCommandStatus)> {
        match self.step_history.get(cmd_name) {
            Some(JobStepDetails {
                step_index,
                step: JobStepStatus::Command(cmd),
                ..
            }) => {
                if *step_index == self.cur_step_index {
                    Some((*step_index, cmd.clone()))
                } else {
                    // in this case another command/stream may have been added, therefore
                    // invalidating the rest of the steps
                    None
                }
            }
            Some(_) => panic!("Unexpectedly got a stream instead of a command"),
            None => None,
        }
    }

    pub fn get_stream(&self, cmd_name: &str) -> Option<(usize, StepStreamStatus)> {
        match self.step_history.get(cmd_name) {
            Some(JobStepDetails {
                step_index,
                step: JobStepStatus::Stream(s),
                ..
            }) => {
                if *step_index == self.cur_step_index {
                    Some((*step_index, s.clone()))
                } else {
                    None
                }
            }
            Some(_) => panic!("Unexpectedly got a stream instead of a command"),
            None => None,
        }
    }

    fn add_command<C: Into<String>>(&mut self, cmd_name: C, cmd: StepCommandStatus) {
        let n = cmd_name.into();
        self.step_history.insert(
            n.clone(),
            JobStepDetails {
                name: n,
                step_index: self.cur_step_index as usize,
                step: JobStepStatus::Command(cmd.clone()),
            },
        );
    }

    fn add_stream<C: Into<String>>(&mut self, name: C, stream: StepStreamStatus) {
        let n = name.into();
        self.step_history.insert(
            n.clone(),
            JobStepDetails {
                name: n,
                step_index: self.cur_step_index as usize,
                step: JobStepStatus::Stream(stream.clone()),
            },
        );
    }

    pub fn id(&self) -> &'_ str {
        &self.id
    }

    pub fn name(&self) -> &'_ str {
        &self.name
    }

    fn set_fatal_error<N: Into<String>, M: Into<String>>(&mut self, name: N, message: M) {
        self.run_status = RunStatus::FatalError {
            step_index: self.cur_step_index,
            step_name: name.into(),
            message: message.into(),
        };
    }

    pub fn set_run_status_complete(&mut self) -> Result<(), JobRunnerError> {
        // in anticipation of future errors, leaving this as a result
        self.run_status = RunStatus::Completed;
        Ok(())
    }

    pub fn start_new_cmd<N: Into<String> + Clone>(
        &mut self,
        name: N,
        jrc: &JobRunnerConfig,
    ) -> Result<(usize, StepCommandStatus), JobRunnerError> {
        let started = Utc::now();
        let n = name.clone();

        match self.get_command(&name.clone().into()) {
            Some((_, StepCommandStatus::Complete { .. })) => {
                // do nothing
            }
            _ => match &self.run_status {
                RunStatus::InProgress => {
                    self.add_command(name, StepCommandStatus::InProgress { started });
                }
                RunStatus::Completed => {
                    self.add_command(name, StepCommandStatus::InProgress { started });
                }
                RunStatus::FatalError {
                    step_index: _,
                    step_name: _,
                    message: _,
                } => {
                    if jrc.stop_on_error {
                        return Err(JobRunnerError::JobStepError {
                            step_index: self.cur_step_index,
                            name: name.clone().into(),
                            message: String::from(
                                "Can't start the new command because stop_on_error flag is set to true",
                            ),
                        });
                    } else {
                        self.add_command(name, StepCommandStatus::InProgress { started });
                    }
                }
            },
        };
        let c = self.get_command(&n.into()).unwrap();
        self.cur_step_index += 1;
        Ok(c)
    }
    pub fn start_new_stream<N: Into<String> + Clone>(
        &mut self,
        name: N,
        jrc: &JobRunnerConfig,
    ) -> Result<(usize, StepStreamStatus), JobRunnerError> {
        let n = name.clone();
        match self.get_stream(&name.clone().into()) {
            Some((_, StepStreamStatus::Complete { .. })) => {
                // do nothing
            }
            _ => match &self.run_status {
                RunStatus::InProgress => {
                    self.add_stream(name, StepStreamStatus::new_in_progress());
                }
                RunStatus::Completed => {
                    self.add_stream(name, StepStreamStatus::new_in_progress());
                }
                RunStatus::FatalError {
                    step_index: _,
                    step_name: _,
                    message: _,
                } => {
                    if jrc.stop_on_error {
                        return Err(JobRunnerError::JobStepError {
                            step_index: self.cur_step_index,
                            name: name.clone().into(),
                            message: String::from(
                                "Can't start the new stream because stop_on_error flag is set to true",
                            ),
                        });
                    } else {
                        self.add_stream(name, StepStreamStatus::new_in_progress());
                    }
                }
            },
        }
        let s = self
            .get_stream(&n.into())
            .expect("Getting stream failed inside start_new_stream");
        self.cur_step_index += 1;
        Ok(s)
    }

    pub fn cmd_ok<N: Into<String>>(&mut self, name: N, _: &JobRunnerConfig) -> anyhow::Result<()> {
        let n = name.into();
        match self.step_history.get_mut(&n) {
            Some(JobStepDetails {
                step: JobStepStatus::Command(ref mut cmd),
                ..
            }) => {
                let started = cmd.started_on();
                *cmd = StepCommandStatus::Complete {
                    started,
                    finished: Utc::now(),
                };
            }
            Some(_) => panic!("Unexpectedly got a stream instead of a command"),
            None => {
                panic!("Tried to complete a command which was never started");
            }
        };
        Ok(())
    }

    pub fn stream_ok<N: Into<String>>(
        &mut self,
        name: N,
        _: &JobRunnerConfig,
        stats: Vec<DataOutputDetails>,
    ) -> anyhow::Result<()> {
        let n = name.into();
        match self.step_history.get_mut(&n) {
            Some(JobStepDetails {
                name: _,
                step_index: _,
                step: JobStepStatus::Stream(ref mut st),
            }) => {
                st.complete(stats);
            }
            Some(_) => panic!("Unexpectedly got a stream instead of a command"),
            None => {
                panic!("Tried to complete a command which was never started");
            }
        };
        Ok(())
    }

    pub fn stream_not_ok<N: Into<String>, M: Into<String>>(
        &mut self,
        name: N,
        m: M,
        lines_scanned: usize,
    ) -> anyhow::Result<()> {
        let name = name.into();
        match self.step_history.get_mut(&name) {
            Some(JobStepDetails {
                step: JobStepStatus::Stream(s),
                ..
            }) => {
                s.set_error(m, lines_scanned);
                Ok(())
            }
            _ => {
                panic!("Attempted to stream_incr_count_ok on a non existant stream");
            }
        }
    }

    pub fn stream_incr_count_ok<N: Into<String>>(
        &mut self,
        name: N,
        source: &str,
    ) -> anyhow::Result<()> {
        let name = name.into();
        match self.step_history.get_mut(&name) {
            Some(JobStepDetails {
                step: JobStepStatus::Stream(cmd),
                ..
            }) => {
                cmd.incr_line(source);
                Ok(())
            }
            _ => {
                panic!("Attempted to stream_incr_count_ok on a non existant stream");
            }
        }
    }

    pub fn stream_incr_count_err<N: Into<String>>(&mut self, name: N) -> anyhow::Result<()> {
        let name = name.into();
        match self.step_history.get_mut(&name) {
            Some(JobStepDetails {
                step: JobStepStatus::Stream(cmd),
                ..
            }) => {
                cmd.incr_error();
                Ok(())
            }
            _ => {
                panic!("Attempted to stream_incr_count_ok on a non existant stream");
            }
        }
    }

    pub fn cmd_not_ok<N: Into<String>, M: Into<String>>(
        &mut self,
        name: N,
        m: M,
    ) -> anyhow::Result<()> {
        let n = name.into();
        let message = m.into();
        self.set_fatal_error(&n, &message);
        match self.step_history.get_mut(&n) {
            Some(JobStepDetails {
                step: JobStepStatus::Command(ref mut cmd),
                ..
            }) => {
                let started = cmd.started_on();
                *cmd = StepCommandStatus::Error {
                    started,
                    message,
                    datetime: Utc::now(),
                };
                Ok(())
            }
            Some(_) => panic!("Unexpectedly got a stream instead of a command"),
            None => {
                panic!("Unexpected cmd_not_ok for a command which did not run");
            }
        }
    }

    pub fn set<K: Into<String>, V: Serialize>(&mut self, key: K, val: &V) -> anyhow::Result<()> {
        let v = serde_json::to_value(val)?;
        self.settings.insert(key.into(), v);
        Ok(())
    }

    pub fn get<V: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<V>> {
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
