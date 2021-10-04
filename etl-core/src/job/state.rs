use super::stream::*;
use crate::job::*;
use anyhow;
use chrono::Utc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

pub const JOB_STATE_EXT: &'static str = "job.json";

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "state")]
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
#[serde(tag = "step_type")]
pub enum JobStepStatus {
    Stream(StepStreamStatus),
    Command(StepCommandStatus),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobStepDetails {
    pub step: JobStepStatus,
    pub name: String,
    pub step_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
// TODO: when JobRunner completes it doesn't set the run_status to complete
pub struct JobState {
    pub settings: HashMap<String, JsonValue>,
    name: String,
    id: String,
    cur_step_index: usize,
    run_status: RunStatus,
    pub step_history: HashMap<String, JobStepDetails>,
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
        }
    }

    pub fn get_command(&self, cmd_name: &str) -> Option<&'_ StepCommandStatus> {
        match self.step_history.get(cmd_name) {
            Some(JobStepDetails {
                step: JobStepStatus::Command(cmd),
                ..
            }) => Some(cmd),
            Some(_) => panic!("Unexpectedly got a stream instead of a command"),
            None => None,
        }
    }

    pub fn get_stream(&self, cmd_name: &str) -> Option<&'_ StepStreamStatus> {
        match self.step_history.get(cmd_name) {
            Some(JobStepDetails {
                step: JobStepStatus::Stream(s),
                ..
            }) => Some(s),
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

    pub fn reset_step_index(&mut self) {
        self.cur_step_index = 0;
    }

    fn set_fatal_error<N: Into<String>, M: Into<String>>(&mut self, name: N, message: M) {
        self.run_status = RunStatus::FatalError {
            step_index: self.cur_step_index,
            step_name: name.into(),
            message: message.into(),
        };
    }

    //TODO: streamline start_* and make it use an enum.  There are issues with steps numbering
    //and it looks like it is only incrementing when steps run, but not when they do not run which
    //will mess up the counter if some run and some don't (when resuming)
    pub fn start_new_cmd<N: Into<String>>(
        &mut self,
        name: N,
        jrc: &JobRunnerConfig,
    ) -> anyhow::Result<()> {
        let started = Utc::now();
        match &self.run_status {
            RunStatus::InProgress => {
                self.add_command(name, StepCommandStatus::InProgress { started });
            }
            RunStatus::Completed => {
                // TODO: this should possibly panic
            }
            RunStatus::FatalError {
                step_index: _,
                step_name: _,
                message: _,
            } => {
                if jrc.stop_on_error {
                    return Err(anyhow::anyhow!(
                        "Can't start the new job because stop_on_error flag is set to true"
                    ));
                } else {
                    self.add_command(name, StepCommandStatus::InProgress { started });
                }
            }
        }
        self.cur_step_index += 1;
        Ok(())
    }
    pub fn start_new_stream<N: Into<String>>(
        &mut self,
        name: N,
        jrc: &JobRunnerConfig,
    ) -> anyhow::Result<()> {
        match &self.run_status {
            RunStatus::InProgress => {
                self.add_stream(name, StepStreamStatus::new_in_progress());
            }
            RunStatus::Completed => {}
            RunStatus::FatalError {
                step_index: _,
                step_name: _,
                message: _,
            } => {
                if jrc.stop_on_error {
                    return Err(anyhow::anyhow!(
                        "Can't start the new job because stop_on_error flag is set to true"
                    ));
                } else {
                    self.add_stream(name, StepStreamStatus::new_in_progress());
                }
            }
        }
        self.cur_step_index += 1;
        Ok(())
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
    ) -> anyhow::Result<()> {
        let n = name.into();
        match self.step_history.get_mut(&n) {
            Some(JobStepDetails {
                name: _,
                step_index: _,
                step: JobStepStatus::Stream(ref mut st),
            }) => {
                st.complete();
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
            },
            _ => {
                panic!("Attempted to stream_incr_count_ok on a non existant stream");
            }
        }
    }

    pub fn stream_incr_count_ok<N: Into<String>>(&mut self, name: N, source: &str) -> anyhow::Result<()> {
        let name = name.into();
        match self.step_history.get_mut(&name) {
            Some(JobStepDetails {
                step: JobStepStatus::Stream(cmd),
                ..
            }) => {
                cmd.incr_line(source);
                Ok(())
            },
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
            },
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
