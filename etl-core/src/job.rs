use super::datastore::error::*;
use super::datastore::job_runner::*;
use super::datastore::*;
use crate::job_manager::*;
use async_trait::async_trait;
use mock::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use state::*;
use std::fmt::Debug;

pub mod command;
pub mod handler;
use command::*;
use handler::*;
pub mod state;

type JsonValue = serde_json::Value;
use self::error::*;

/// JobRunner is used to declare pipelines which are designed to run step-by-step.  To execute
/// multiple pipelines in parallel, use separate JobRunner for each pipeline.
pub struct JobRunner {
    config: JobRunnerConfig,
    job_manager_tx: JobManagerTx,
    job_manager_rx: JobManagerRx,
    num_process_item_errors: usize,
    num_processed_items: usize,
    /// helps to await on DataOutput instances to complete writing
    data_output_handles: Vec<DataOutputJoinHandle>,
    job_state: JobState,
}

pub struct JobRunnerConfig {
    //pub max_errors: usize,
    /// The SimpleStore used by JobRunner to store state.  The default setting uses the
    /// MockJsonDataSource which does not persist, thus all pipelines will run every time.
    pub ds: Box<dyn SimpleStore<serde_json::Value>>,
}

impl Default for JobRunnerConfig {
    fn default() -> Self {
        JobRunnerConfig {
            //max_errors: 1000,
            ds: Box::new(MockJsonDataSource::default()),
        }
    }
}

impl JobRunner {
    /// `id` is an enumerated name like an extract id
    /// `name` is the name of the actual job, like 'extract-patient-data'
    pub fn new<A, B>(
        id: A,
        name: B,
        job_manager_channel: JobManagerChannel,
        config: JobRunnerConfig,
    ) -> Self
    where
        A: Into<String>,
        B: Into<String>,
    {
        let jr = JobRunner {
            job_manager_rx: job_manager_channel.rx,
            job_manager_tx: job_manager_channel.tx,
            num_process_item_errors: 0,
            num_processed_items: 0,
            config,
            data_output_handles: Vec::new(),
            job_state: JobState::new(id, name),
        };
        jr.register()
            .expect("There was an error registering the job");
        jr
    }

    async fn load_job_state(&self) -> Result<JobState, DataStoreError> {
        let path = JobState::gen_name(&self.job_state.id, &self.job_state.name);
        match self.config.ds.load(&path).await {
            Ok(json) => match serde_json::from_value(json) {
                Ok(job_state) => Ok(job_state),
                Err(e) => {
                    println!("------> Fix or remove the file: {}", &path);
                    Err(DataStoreError::Deserialize {
                        attempted_string: self.job_state.name.to_owned(),
                        message: e.to_string(),
                    })
                }
            },
            Err(DataStoreError::NotExist { .. }) => {
                Ok(JobState::new(&self.job_state.name, &self.job_state.id))
            }
            Err(others) => Err(others),
        }
    }

    async fn save_job_state(&self) -> anyhow::Result<()> {
        let path = JobState::gen_name(&self.job_state.id, &self.job_state.name);
        self.config
            .ds
            .write(&path, serde_json::to_value(self.job_state.clone()).unwrap())
            .await?;
        Ok(())
    }

    pub fn await_data_output(&mut self, d: DataOutputJoinHandle) {
        self.data_output_handles.push(d);
    }

    pub fn log_info<A, B>(&self, name: A, msg: B)
    where
        A: Into<String>,
        B: Into<String>,
    {
        match self
            .job_manager_tx
            .send(Message::log_info(name, msg.into()))
        {
            Ok(_) => {}
            Err(er) => println!("Could not send to job_manager {}", er),
        };
    }

    pub fn log_err<A, B>(&self, name: A, item_info: Option<&JobItemInfo>, msg: B)
    where
        A: Into<String>,
        B: Into<String>,
    {
        let message = match item_info {
            Some(item_info) => {
                format!("{} {} {}", item_info.index, item_info.path, msg.into())
            }
            None => format!("{}", msg.into()),
        };
        match self
            .job_manager_tx
            .send(Message::log_err(name.into(), message))
        {
            Ok(_) => {}
            Err(er) => println!("Could not send to job_manager {}", er),
        };
    }

    fn _log_if_err<T>(&self, name: &str, item_info: Result<(), T>)
    where
        T: std::fmt::Display,
    {
        if let Err(e) = item_info {
            let message = format!("{}", e);
            self.log_err(name, None, message);
        }
    }

    /// process messages from the JobManager
    fn process_job_manager_rx(&mut self) -> Result<(), JobRunnerError> {
        loop {
            if let Ok(message) = self.job_manager_rx.try_recv() {
                use crate::job_manager::Message::*;
                match message {
                    // message broadcasted from JobManager when there are too many errors and its
                    // better to stop the job completely.  max errors are defined in the
                    // configuration file
                    ToJobRunner(NotifyJobRunner::TooManyErrors) => {
                        return Err(JobRunnerError::TooManyErrors);
                    }
                    _ => {}
                }
            } else {
                // end of available messages
                break;
            }
        }
        Ok(())
    }

    /// Notify JobManager that this job was added
    fn register(&self) -> Result<(), JobRunnerError> {
        match self
            .job_manager_tx
            .send(Message::broadcast_job_start(&self.job_state.name))
        {
            Ok(_) => Ok(()),
            Err(er) => Err(JobRunnerError::CompleteError(er.to_string())),
        }
    }

    /// must be run once the JobRunner is done otherwise JobManager will not exit
    pub fn complete(self) -> Result<JobState, JobRunnerError> {
        match self
            .job_manager_tx
            .send(Message::broadcast_job_end(&self.job_state.name))
        {
            Ok(_) => Ok(self.job_state),
            Err(er) => Err(JobRunnerError::CompleteError(er.to_string())),
        }
    }

    /// Same as run except this takes a DataSource and a DataOutput and
    /// moves the data from one to the other.
    pub async fn run_data_output<T>(
        mut self,
        stream_name: &str,
        input: Box<dyn DataSource<T>>,
        mut output: Box<dyn DataOutput<T>>,
        jm: JobManagerChannel,
    ) -> anyhow::Result<Self>
    where
        T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
    {
        self.job_state = self.load_job_state().await?;

        match &self.job_state.streams.is_complete(stream_name) {
            Ok(true) => {
                self.log_info(stream_name, "Stream already completed, skipping");
                return Ok(self);
            }
            Ok(false) => {
                self.job_state.streams.in_progress(stream_name)?;
            }
            _ => (),
        };

        //self.log_info(&self.job_state.name, "Job started");
        let input_name = input.name().to_string();
        // no need to wait on input JoinHandle
        let (mut input_rx, _) = input.start_stream().await?;
        let (output_tx, output_jh) = output.start_stream(jm).await?;
        self.save_job_state().await?;
        let mut lines_scanned = 0_usize;
        loop {
            let info = JobItemInfo::new((lines_scanned, &self.job_state.name));
            match input_rx.recv().await {
                Some(Ok(DataSourceMessage::Data {
                    source,
                    content: input_item,
                })) => {
                    lines_scanned += 1;
                    self.job_state.streams.incr_num_ok(stream_name, &source)?;
                    output_tx.send(DataOutputMessage::new(input_item)).await?;
                }
                Some(Err(val)) => {
                    lines_scanned += 1;
                    self.job_state.streams.incr_error(stream_name)?;
                    self.log_err(&input_name, Some(&info), val.to_string());
                }
                None => break,
            };
            match self.process_job_manager_rx() {
                Err(JobRunnerError::TooManyErrors) => {
                    self.job_state.streams.set_error(
                        stream_name,
                        "JobMonitor has reached maximum errors",
                        lines_scanned,
                    )?;
                    self.save_job_state().await?;
                    return Err(anyhow::anyhow!("{}", JobRunnerError::TooManyErrors));
                }
                Err(_) => {}
                Ok(()) => {}
            };
        }
        drop(input_rx);
        drop(output_tx);
        output_jh.await??;
        self.job_state
            .streams
            .set_total_lines(stream_name, lines_scanned)?;
        self.job_state.streams.complete(stream_name)?;
        self.save_job_state().await?;
        Ok(self)
    }

    /// Allows processing data using the TransformHandler trait to process and
    /// filter a stream, and output that as a new DataSource which can
    /// then be forwarded to a DataOutput, or continue as input for
    /// more transformations
    // TODO: this should be taken out and put into Transformer.  Doesn't make sense
    // semantically to be done by the JobRunner
    pub fn as_datasource<I, O>(
        &self,
        ds: Box<dyn DataSource<I> + Send + Sync>,
        transformer: Box<dyn TransformHandler<I, O> + Send + Sync>,
    ) -> anyhow::Result<JRDataSource<I, O>>
    where
        I: DeserializeOwned + Debug + Send + Sync,
        O: Serialize + Debug + Send + Sync,
    {
        Ok(JRDataSource {
            job_name: self.job_state.name.clone(),
            transformer,
            input_ds: ds,
        })
    }

    /// uses the StreamHandler trait to do the final custom transform.
    /// This method gives a lot of flexibility in what you can execute
    /// during the execute (like external apis).  During the init, and
    /// shutdown step give extra options.  See the relevant docs on that
    pub async fn run<I>(
        mut self,
        ds: Box<dyn DataSource<I> + Send + Sync>,
        mut job_handler: Box<dyn StreamHandler<I> + Send + Sync>,
    ) -> anyhow::Result<Self>
    where
        I: DeserializeOwned + Serialize + Debug + Send + Sync + 'static,
    {
        let (mut rx, source_stream_jh) = ds.start_stream().await?;
        let stream_name = job_handler.name();
        self.job_state = self.load_job_state().await?;

        match self.job_state.streams.is_complete(stream_name) {
            Ok(true) => {
                self.log_info(&self.job_state.name, "Job already completed, skipping");
                return Ok(self);
            }
            Ok(false) => {
                //self.log_info(&self.job_state.name, "Job is in error state, restarting");
                //return Err(anyhow::anyhow!("{}", JobRunnerError::LoadedStateWithError));
                self.job_state.streams.in_progress(stream_name)?;
            }
            _ => (),
        };

        self.log_info(&self.job_state.name, "Starting job");
        let jr_action = job_handler.init(&self).await?;
        let index_start = match &jr_action {
            JobRunnerAction::Start => 0,
            JobRunnerAction::Resume { index } => *index,
            JobRunnerAction::Skip => {
                self.job_state.streams.complete(stream_name)?;
                self.save_job_state().await?;
                self.log_info(
                    &self.job_state.name,
                    "StreamHandler requested the job to be skipped",
                );
                return Ok(self);
            }
        };

        let mut received_lines = 0_usize;

        self.save_job_state().await?;
        let mut lines_scanned = 0_usize;
        loop {
            match rx.recv().await {
                Some(Ok(DataSourceMessage::Data {
                    source,
                    content: line,
                })) => {
                    if received_lines >= index_start {
                        lines_scanned += 1;
                        self.job_state.streams.incr_num_ok(stream_name, &source)?;
                        match job_handler
                            .process_item(
                                JobItemInfo::new((
                                    self.num_processed_items,
                                    &self.job_state.name,
                                )),
                                line,
                                &self,
                            )
                            .await
                        {
                            Ok(()) => {
                                self.num_processed_items += 1;
                            }
                            Err(er) => {
                                self.log_err(
                                    &self.job_state.name,
                                    Some(&JobItemInfo::new((
                                        self.num_processed_items,
                                        &self.job_state.name,
                                    ))),
                                    er.to_string(),
                                );
                                self.num_process_item_errors += 1;
                            }
                        }
                    }
                    received_lines += 1;
                }
                Some(Err(er)) => {
                    self.num_process_item_errors += 1;
                    self.log_err(
                        &self.job_state.name,
                        Some(&JobItemInfo::new((
                            self.num_processed_items,
                            &self.job_state.name,
                        ))),
                        er.to_string(),
                    );
                }
                None => {
                    break;
                }
            };
            match self.process_job_manager_rx() {
                Err(JobRunnerError::TooManyErrors) => {
                    self.job_state.streams.set_error(
                        stream_name,
                        "JobMonitor has reached maximum errors",
                        lines_scanned,
                    )?;
                    self.save_job_state().await?;
                    return Err(anyhow::anyhow!("{}", JobRunnerError::TooManyErrors));
                }
                Err(_) => {}
                Ok(()) => {}
            };
        }
        // TODO: num_processed_lines should be local, remove self.num_processed_items
        self.job_state
            .streams
            .set_total_lines(stream_name, self.num_processed_items)?;
        self.job_state.streams.complete(stream_name)?;
        job_handler.shutdown(&mut self).await?;

        // only wait if everything is okay
        source_stream_jh.await??;
        for join_handle in self.data_output_handles {
            join_handle.await??;
        }
        self.data_output_handles = Vec::new();
        self.save_job_state().await?;
        Ok(self)
    }

    pub async fn run_cmd(mut self, job_cmd: Box<dyn JobCommand>) -> anyhow::Result<Self> {
        self.job_state = self.load_job_state().await?;
        let name = job_cmd.name();
        if let Some(JobCommandStatus::Complete { .. }) =
            self.job_state.commands.get(&name)
        {
            self.log_info(
                &self.job_state.name,
                format!("{} command previously ran, skipping", &name),
            );
        } else {
            use chrono::Utc;
            let started = Utc::now();
            job_cmd.run(&self).await?;
            let finished = Utc::now();
            self.job_state.commands.insert(
                name.clone(),
                JobCommandStatus::Complete { started, finished },
            );
            self.save_job_state().await?;
        }
        Ok(self)
    }
}

#[derive(Debug, Serialize)]
pub struct JobItemInfo {
    pub index: usize,
    /// filename or bucket key
    pub path: String,
}

impl JobItemInfo {
    pub fn new<T>((index, path): (usize, T)) -> Self
    where
        T: Into<String>,
    {
        JobItemInfo {
            index,
            path: path.into(),
        }
    }
}

pub enum JobRunnerAction {
    Start,
    Skip,
    /// it is up to the datastore to present files in a consistent order
    /// because the index is cumulative
    Resume {
        index: usize,
    },
}

pub mod error {
    use thiserror::Error;
    #[derive(Error, Debug)]
    pub enum JobRunnerError {
        #[error("Received TooManyErrors message from JobManager")]
        TooManyErrors,
        /// currently never returns this error because general use-cases call for
        /// running the job automatically.  May introduce a flag to stop the job in the
        /// future
        #[error("Loaded an existing state with error.  Remove the state to restart")]
        LoadedStateWithError,
        #[error("Job completed with an error: `{0}`")]
        CompleteError(String),
    }
}
