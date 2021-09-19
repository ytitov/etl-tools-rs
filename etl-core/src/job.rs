use super::datastore::error::*;
use super::datastore::job_runner::*;
use super::datastore::*;
use crate::job_manager::*;
use async_trait::async_trait;
use mock::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use state::*;
use std::fmt;
use std::fmt::Debug;

pub mod command;
pub mod handler;
pub mod stream;
pub mod stream_handler_builder;
use command::*;
use handler::*;
//use stream::*;
pub mod state;

type JsonValue = serde_json::Value;
use self::error::*;

/// JobRunner is used to declare pipelines which are designed to run step-by-step.  To execute
/// multiple pipelines in parallel, use separate JobRunner for each pipeline.
pub struct JobRunner {
    config: JobRunnerConfig,
    job_manager_channel: JobManagerChannel,
    num_process_item_errors: usize,
    num_processed_items: usize,
    /// helps to await on DataOutput instances to complete writing
    data_output_handles: Vec<DataOutputJoinHandle>,
    job_state: JobState,
    /// has the job started yet
    is_running: bool,
}

pub struct JobRunnerConfig {
    /// maximum number of errors to tolerate in this particular pipeline.  Different from
    /// JobManager max_errors which counts number of errors from all pipelines reporting
    pub max_errors: usize,
    /// Stop if any commands or streams result in an error.  Note that this does not count any
    /// serialization/deserialization errors which happen when processing individual data
    /// elements
    pub stop_on_error: bool,
    /// The SimpleStore used by JobRunner to store state.  The default setting uses the
    /// MockJsonDataSource which does not persist, thus all pipelines will run every time.
    pub ds: Box<dyn SimpleStore<serde_json::Value>>,
}

impl Default for JobRunnerConfig {
    fn default() -> Self {
        JobRunnerConfig {
            max_errors: 1000,
            stop_on_error: true,
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
            job_manager_channel,
            num_process_item_errors: 0,
            num_processed_items: 0,
            config,
            data_output_handles: Vec::new(),
            job_state: JobState::new(id, name),
            is_running: false,
        };
        jr.register()
            .expect("There was an error registering the job");
        jr
    }

    pub fn name(&self) -> &'_ str {
        self.job_state.name()
    }

    pub fn id(&self) -> &'_ str {
        self.job_state.id()
    }

    async fn load_job_state(&mut self) -> Result<JobState, DataStoreError> {
        let path = JobState::gen_name(self.job_state.id(), self.job_state.name());
        let mut job_state = match self.config.ds.load(&path).await {
            Ok(json) => match serde_json::from_value(json) {
                Ok(job_state) => Ok(job_state),
                Err(e) => {
                    println!("------> Fix or remove the file: {}", &path);
                    Err(DataStoreError::Deserialize {
                        attempted_string: self.job_state.name().to_owned(),
                        message: e.to_string(),
                    })
                }
            },
            Err(DataStoreError::NotExist { .. }) => {
                Ok(JobState::new(self.job_state.name(), self.job_state.id()))
            }
            Err(others) => Err(others),
        }?;
        if !self.is_running {
            self.is_running = true;
            job_state.reset_step_index();
        }
        Ok(job_state)
    }

    async fn save_job_state(&self) -> anyhow::Result<()> {
        let path = JobState::gen_name(self.job_state.id(), self.job_state.name());
        self.config
            .ds
            .write(&path, serde_json::to_value(self.job_state.clone()).unwrap())
            .await?;
        Ok(())
    }

    /// convenience method to await on any handles useful inside the StreamHandler
    pub fn await_data_output(&mut self, d: DataOutputJoinHandle) {
        self.data_output_handles.push(d);
    }

    pub fn log_info<A, B>(&self, name: A, msg: B)
    where
        A: Into<String>,
        B: Into<String>,
    {
        match self
            .job_manager_channel
            .tx
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
            .job_manager_channel
            .tx
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

    /// Checks if the job must exist by processing messages from JobManager and also checking
    /// whether the JobRunner has reached maximum number of errors allowed.  If it must exit, it
    /// will notify JobManager of the fact and return a JobRunnerError
    fn process_job_manager_rx(&mut self) -> Result<(), JobRunnerError> {
        loop {
            if let Ok(message) = self.job_manager_channel.rx.try_recv() {
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
        if self.config.max_errors <= self.num_process_item_errors {
            // notify JobManager that we are exiting
            self.un_register()?;
            return Err(JobRunnerError::TooManyErrors);
        }
        Ok(())
    }

    /// Notify JobManager that this job was added
    fn register(&self) -> Result<(), JobRunnerError> {
        match self
            .job_manager_channel
            .tx
            .send(Message::broadcast_job_start(self.job_state.name()))
        {
            Ok(_) => Ok(()),
            Err(er) => Err(JobRunnerError::CompleteError(er.to_string())),
        }
    }

    /// Notify JobManager that this job was added
    fn un_register(&self) -> Result<(), JobRunnerError> {
        match self
            .job_manager_channel
            .tx
            .send(Message::broadcast_job_end(&self))
        {
            Ok(_) => Ok(()),
            Err(er) => Err(JobRunnerError::CompleteError(er.to_string())),
        }
    }

    /// must be run once the JobRunner is done otherwise JobManager will not exit
    pub fn complete(self) -> Result<JobState, JobRunnerError> {
        match self
            .job_manager_channel
            .tx
            .send(Message::broadcast_job_end(&self))
        {
            Ok(_) => Ok(self.job_state),
            Err(er) => Err(JobRunnerError::CompleteError(er.to_string())),
        }
    }

    /// Same as run except this takes a DataSource and a DataOutput and
    /// moves the data from one to the other.
    pub async fn run_stream<T>(
        mut self,
        stream_name: &str,
        input: Box<dyn DataSource<T>>,
        mut output: Box<dyn DataOutput<T>>,
    ) -> Result<Self, JobRunnerError>
    where
        T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
    {
        self.job_state = self.load_job_state().await?;
        let name = stream_name.to_string();
        if let Err(_) = self.job_state.start_new_stream(&name, &self.config) {
            return Ok(self);
        }

        if let Some(StepStreamStatus::Complete { .. }) = self.job_state.get_stream(&name) {
            self.log_info(
                self.job_state.name(),
                format!("{} stream previously ran, skipping", &name),
            );
        } else {
            let input_name = input.name().to_string();
            // no need to wait on input JoinHandle
            let (mut input_rx, _) = input.start_stream()?;
            let (output_tx, output_jh) = output
                .start_stream(self.job_manager_channel.clone())
                .await?;
            self.save_job_state().await?;
            let mut lines_scanned = 0_usize;
            loop {
                let info = JobItemInfo::new((lines_scanned, self.job_state.name()));
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
                        self.num_process_item_errors += 1;
                    }
                    None => break,
                };
                match self.process_job_manager_rx() {
                    Err(JobRunnerError::TooManyErrors) => {
                        println!("handling TooManyErrors");
                        self.job_state.stream_not_ok(
                            name,
                            String::from("Reached too many errors"),
                            lines_scanned,
                        )?;
                        self.save_job_state().await?;
                        drop(input_rx);
                        drop(output_tx);
                        output_jh.await??;
                        return Err(JobRunnerError::TooManyErrors);
                    }
                    Err(e) => {
                        panic!("Received an unforseen error {}", e);
                    }
                    Ok(()) => {}
                };
            }
            self.job_state.stream_ok(name, &self.config)?;
            drop(input_rx);
            drop(output_tx);
            output_jh.await??;
            self.save_job_state().await?;
        }
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
        ds: Box<dyn DataSource<I>>,
        transformer: Box<dyn TransformHandler<I, O>>,
    ) -> anyhow::Result<JRDataSource<I, O>>
    where
        I: DeserializeOwned + Debug + Send + Sync,
        O: Serialize + Debug + Send + Sync,
    {
        Ok(JRDataSource {
            job_name: self.job_state.name().to_owned(),
            transformer,
            input_ds: ds,
        })
    }

    /// uses the StreamHandler trait to do the final custom transform.
    /// This method gives a lot of flexibility in what you can execute
    /// during the execute (like external apis).  During the init, and
    /// shutdown step give extra options.  See the relevant docs on that
    pub async fn run_stream_handler<I>(
        mut self,
        ds: Box<dyn DataSource<I>>,
        mut job_handler: Box<dyn StreamHandler<I>>,
    ) -> Result<Self, JobRunnerError>
    where
        I: DeserializeOwned + Serialize + Debug + Send + Sync + 'static,
    {
        let (mut rx, source_stream_jh) = ds.start_stream()?;
        let stream_name = job_handler.name();
        self.job_state = self.load_job_state().await?;

        if let Err(_) = self.job_state.start_new_stream(stream_name, &self.config) {
            return Ok(self);
        }
        match self.job_state.streams.is_complete(stream_name) {
            Ok(true) => {
                self.log_info(self.job_state.name(), "Job already completed, skipping");
                return Ok(self);
            }
            Ok(false) => {
                //self.log_info(&self.job_state.name, "Job is in error state, restarting");
                //return Err(anyhow::anyhow!("{}", JobRunnerError::LoadedStateWithError));
                self.job_state.streams.in_progress(stream_name)?;
            }
            _ => (),
        };

        let jr_action = job_handler.init(&self).await?;
        let index_start = match &jr_action {
            JobRunnerAction::Start => 0,
            JobRunnerAction::Resume { index } => *index,
            JobRunnerAction::Skip => {
                //self.job_state.streams.complete(stream_name)?;
                self.job_state.stream_ok(stream_name, &self.config)?;
                self.save_job_state().await?;
                self.log_info(
                    self.job_state.name(),
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
                        match job_handler
                            .process_item(
                                JobItemInfo::new((self.num_processed_items, self.job_state.name())),
                                line,
                                &self,
                            )
                            .await
                        {
                            Ok(()) => {
                                self.num_processed_items += 1;
                                self.job_state.streams.incr_num_ok(stream_name, &source)?;
                            }
                            Err(er) => {
                                self.log_err(
                                    self.job_state.name(),
                                    Some(&JobItemInfo::new((
                                        self.num_processed_items,
                                        self.job_state.name(),
                                    ))),
                                    er.to_string(),
                                );
                                self.job_state.streams.incr_error(stream_name)?;
                                self.num_process_item_errors += 1;
                            }
                        }
                    }
                    received_lines += 1;
                }
                Some(Err(er)) => {
                    self.log_err(
                        self.job_state.name(),
                        Some(&JobItemInfo::new((
                            self.num_processed_items,
                            self.job_state.name(),
                        ))),
                        er.to_string(),
                    );
                    self.job_state.streams.incr_error(stream_name)?;
                    self.num_process_item_errors += 1;
                }
                None => {
                    break;
                }
            };
            match self.process_job_manager_rx() {
                Err(JobRunnerError::TooManyErrors) => {
                    self.job_state.stream_not_ok(
                        stream_name,
                        "JobMonitor has reached maximum errors",
                        lines_scanned,
                    )?;
                    self.save_job_state().await?;
                    return Err(JobRunnerError::TooManyErrors);
                }
                Err(er) => {
                    panic!("Unhandled error: {}", er);
                }
                Ok(()) => {}
            };
        }
        // TODO: num_processed_lines should be local, remove self.num_processed_items
        self.job_state
            .streams
            .set_total_lines(stream_name, self.num_processed_items)?;
        self.job_state.stream_ok(stream_name, &self.config)?;
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

    pub async fn run_cmd(mut self, job_cmd: Box<dyn JobCommand>) -> Result<Self, JobRunnerError> {
        self.job_state = self.load_job_state().await?;
        let name = job_cmd.name();
        if let Err(_) = self.job_state.start_new_cmd(&name, &self.config) {
            return Ok(self);
        }
        if let Some(StepCommandStatus::Complete { .. }) = self.job_state.get_command(&name) {
            self.log_info(
                self.job_state.name(),
                format!("{} command previously ran, skipping", &name),
            );
        } else {
            match job_cmd.run(&self).await {
                Ok(()) => {
                    self.job_state.cmd_ok(name, &self.config)?;
                }
                Err(er) => {
                    self.job_state.cmd_not_ok(&name, er.to_string())?;
                    self.log_err(
                        self.job_state.name(),
                        None,
                        format!("{} command ran into an error: {}", name, er),
                    );
                }
            };
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

impl fmt::Debug for JobRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobRunner")
            .field("job_state", &self.job_state)
            .finish()
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
    /// These are all fatal errors which can stop the execution of a pipeline defined by the
    /// JobRunner
    #[derive(Error, Debug, PartialEq)]
    pub enum JobRunnerError {
        /// Triggered when some particular DataSource or DataOutput reach a maximum amount of
        /// errors
        #[error("Received TooManyErrors message from JobManager")]
        TooManyErrors,
        /// currently never returns this error because general use-cases call for
        /// running the job automatically.  May introduce a flag to stop the job in the
        /// future
        #[error("Loaded an existing state with error.  Remove the state to restart")]
        LoadedStateWithError,
        #[error("Job completed with an error: `{0}`")]
        CompleteError(String),
        #[error("Step {name} at index {step_index} in the job returned: {message} ")]
        JobStepError {
            step_index: isize,
            name: String,
            message: String,
        },
        #[error("Encounter a fatal error: {message} ")]
        GenericError { message: String },
    }

    impl From<anyhow::Error> for JobRunnerError {
        fn from(er: anyhow::Error) -> Self {
            JobRunnerError::GenericError {
                message: er.to_string(),
            }
        }
    }

    use crate::datastore::error::DataStoreError;
    impl From<DataStoreError> for JobRunnerError {
        fn from(er: DataStoreError) -> Self {
            JobRunnerError::GenericError {
                message: er.to_string(),
            }
        }
    }

    use tokio::task::JoinError;
    impl From<JoinError> for JobRunnerError {
        fn from(er: JoinError) -> Self {
            JobRunnerError::GenericError {
                message: er.to_string(),
            }
        }
    }

    use tokio::sync::mpsc::error::SendError;
    impl<T> From<SendError<T>> for JobRunnerError {
        fn from(er: SendError<T>) -> Self {
            JobRunnerError::GenericError {
                message: er.to_string(),
            }
        }
    }
}
