use crate::job_manager::*;
use etl_core::datastore::error::*;
use etl_core::datastore::simple::SimpleStore;
use etl_core::datastore::*;
use etl_core::deps::{
    anyhow, async_trait, serde, serde::de::DeserializeOwned, serde::Serialize, tokio,
};
use mock::*;
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
    data_output_handles: Vec<(String, DataOutputJoinHandle)>,
    job_state: JobState,
    /// has the job started yet
    is_running: bool,
    /// has the job state been updated and not saved
    job_state_updated: bool,
    /// the current step being run
    cur_step_index: usize,
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
    pub async fn create<A, B>(
        id: A,   // what the job is always called
        name: B, // instance id of the job
        job_manager_handle: &JobManagerHandle,
        config: JobRunnerConfig,
    ) -> anyhow::Result<Self>
    where
        A: Into<String>,
        B: Into<String>,
    {
        let name = name.into();
        let id = id.into();
        let mut jr = JobRunner {
            job_manager_channel: job_manager_handle.connect(id.clone()).await?,
            num_process_item_errors: 0,
            num_processed_items: 0,
            config,
            data_output_handles: Vec::new(),
            job_state: JobState::new(id, name),
            job_state_updated: false,
            is_running: false,
            cur_step_index: 0,
        };
        jr.job_state = jr.load_job_state().await?;
        /*
        jr.register().await
            .expect("There was an error registering the job");
        */
        Ok(jr)
    }

    pub fn get_job_manager_sender(&self) -> JobManagerTx {
        self.job_manager_channel.tx.clone()
    }

    pub fn name(&self) -> &'_ str {
        self.job_state.name()
    }

    pub fn id(&self) -> &'_ str {
        self.job_state.id()
    }

    async fn load_job_state(&mut self) -> Result<JobState, DataStoreError> {
        if self.job_state_updated {
            self.save_job_state().await?;
            self.job_state_updated = false;
        }
        let path = JobState::gen_name(self.job_state.id(), self.job_state.name());
        let old_step_index = self.job_state.get_cur_step_index();
        let mut job_state = match self.config.ds.load(&path).await {
            Ok(json) => match serde_json::from_value(json) {
                Ok(job_state) => Ok(job_state),
                Err(e) => {
                    self.log_err(
                        "JobRunner",
                        None,
                        format!("Fix or remove the file: {}", &path),
                    )
                    .await;
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
        }
        job_state.set_cur_step_index(old_step_index);
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
    pub fn await_data_output<N: ToString>(&mut self, name: N, d: DataOutputJoinHandle) {
        self.data_output_handles.push((name.to_string(), d));
    }

    pub async fn log_info<A, B>(&self, name: A, msg: B)
    where
        A: Into<String>,
        B: Into<String>,
    {
        match self
            .job_manager_channel
            .tx
            .send(Message::log_info(name, msg.into()))
            .await
        {
            Ok(_) => {}
            Err(er) => println!("Could not send to job_manager {}", er),
        };
    }

    pub async fn log_err<A, B>(&self, name: A, item_info: Option<&JobItemInfo>, msg: B)
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
            .await
        {
            Ok(_) => {}
            Err(er) => println!("Could not send to job_manager {}", er),
        };
    }

    pub fn set_state<K: Into<String>, V: Serialize>(&mut self, key: K, val: &V) {
        if let Ok(_) = self.job_state.set(key.into(), val) {
            self.job_state_updated = true;
        } else {
            println!("JobRunner::set_state: error saving state");
        }
    }

    pub fn get_state_or_default<V: DeserializeOwned + Serialize + Default>(
        &mut self,
        key: &str,
    ) -> anyhow::Result<V> {
        match self.job_state.get(key) {
            Ok(Some(val)) => Ok(val),
            Ok(None) => {
                self.job_state.set::<String, V>(key.into(), &V::default())?;
                self.job_state_updated = true;
                Ok(V::default())
            }
            Err(e) => panic!("Fatal error JobRunner::get_state for {} error: {}", key, e),
        }
    }

    pub fn get_state<V: DeserializeOwned + Default>(&self, key: &str) -> anyhow::Result<V> {
        match self.job_state.get(key) {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Err(anyhow::anyhow!("get_state requested a non-existant value")),
            Err(e) => panic!("Fatal error JobRunner::get_state for {} error: {}", key, e),
        }
    }

    /// Checks if the job must exist by processing messages from JobManager and also checking
    /// whether the JobRunner has reached maximum number of errors allowed.  If it must exit, it
    /// will notify JobManager of the fact and return a JobRunnerError
    async fn process_job_manager_rx(&mut self) -> Result<(), JobRunnerError> {
        use tokio::sync::mpsc::error::TryRecvError;
        loop {
            match self.job_manager_channel.rx.try_recv() {
                Ok(message) => {
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
                }
                // even though we're disconnected, allow the job to finish
                Err(TryRecvError::Disconnected) => {
                    break;
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
            }
        }
        if self.config.max_errors <= self.num_process_item_errors {
            // notify JobManager that we are exiting
            self.un_register().await?;
            return Err(JobRunnerError::TooManyErrors);
        }
        Ok(())
    }

    /// Notify JobManager that this job was added
    async fn un_register(&self) -> Result<(), JobRunnerError> {
        match self
            .job_manager_channel
            .tx
            .send(Message::broadcast_job_end(&self))
            .await
        {
            Ok(_) => Ok(()),
            Err(er) => Err(JobRunnerError::CompleteError(er.to_string())),
        }
    }

    /// must be run once the JobRunner is done otherwise JobManager will not exit
    pub async fn complete(mut self) -> Result<JobState, JobRunnerError> {
        let outputhandles = self.data_output_handles;
        self.data_output_handles = Vec::new();
        let mut output_stats = Vec::new();
        for (name, join_handle) in outputhandles {
            match join_handle.await {
                Err(join_handle_err) => {
                    self.job_state.caught_errors.push(join_handle_err.into());
                }
                Ok(Err(task_err)) => {
                    self.job_state.caught_errors.push(task_err.into());
                }
                Ok(Ok(data_output_stats)) => {
                    output_stats.push((name, data_output_stats));
                }
            }
        }
        //println!("OUTPUT STATS: {:#?}", output_stats);
        for (name, stats) in output_stats {
            self.job_state.stream_ok(name, &self.config, vec![stats])?;
        }
        if self.job_state.caught_errors.len() == 0 {
            self.job_state.set_run_status_complete()?;
        }
        self.save_job_state().await?;
        match self
            .job_manager_channel
            .tx
            .send(Message::broadcast_job_end(&self))
            .await
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
        output: Box<dyn DataOutput<T>>,
    ) -> Result<Self, JobRunnerError>
    where
        T: Debug + Send + Sync + 'static,
    {
        self.job_state = self.load_job_state().await?;
        let name = stream_name.to_string();
        use stream::StepStreamStatus;

        match self.job_state.start_new_stream(&name, &self.config) {
            Ok((_, StepStreamStatus::Complete { .. })) => {
                self.log_info(
                    self.job_state.name(),
                    format!("{} stream previously ran, skipping", &name),
                )
                .await;
            }
            Err(e) => {
                self.complete().await?;
                return Err(e);
            }
            Ok(_) => {
                let input_name = input.name().to_string();
                // no need to wait on input JoinHandle
                let (mut input_rx, _) = input.start_stream()?;
                let (output_tx, output_jh) = output.start_stream().await?;
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
                            self.job_state.stream_incr_count_ok(stream_name, &source)?;
                            output_tx.send(DataOutputMessage::new(input_item)).await?;
                        }
                        Some(Err(val)) => {
                            lines_scanned += 1;
                            self.job_state.stream_incr_count_err(stream_name)?;
                            self.log_err(&input_name, Some(&info), val.to_string())
                                .await;
                            self.num_process_item_errors += 1;
                        }
                        None => break,
                    };
                    match self.process_job_manager_rx().await {
                        Err(JobRunnerError::TooManyErrors) => {
                            self.job_state.stream_not_ok(
                                name,
                                String::from("Reached too many errors"),
                                lines_scanned,
                            )?;
                            self.save_job_state().await?;
                            drop(input_rx);
                            drop(output_tx);
                            // not using the number in error yet..
                            let _ = output_jh.await??;
                            return Err(JobRunnerError::TooManyErrors);
                        }
                        Err(e) => {
                            //panic!("Received an unforseen error {}", e);
                            self.job_state.stream_not_ok(
                                name,
                                format!("While processing job manager messages ran into {}", &e),
                                lines_scanned,
                            )?;
                            self.save_job_state().await?;
                            drop(input_rx);
                            drop(output_tx);
                            output_jh.await??;
                            return Err(JobRunnerError::GenericError {
                                message: e.to_string(),
                            });
                        }
                        Ok(()) => {}
                    };
                }
                self.cur_step_index += 1;
                drop(input_rx);
                drop(output_tx);
                let output_stats = output_jh.await??;
                self.job_state
                    .stream_ok(name, &self.config, vec![output_stats])?;
                self.save_job_state().await?;
            }
        }
        Ok(self)
    }

    pub async fn run_stream_handler_fn<I>(
        mut self,
        name: &str,
        ds: Box<dyn DataSource<I>>,
        create_sh: CreateStreamHandlerFn<'static, I>,
    ) -> Result<Self, JobRunnerError>
    where
        I: Debug + Send + Sync + 'static,
    {
        use stream::StepStreamStatus;
        if let Some((_i, StepStreamStatus::Complete { .. })) = self.job_state.get_stream(name) {
            Ok(self)
        } else {
            let sh = create_sh(&mut self).await?;
            Ok(self.run_stream_handler::<I, &str>(name, ds, sh).await?)
        }
    }

    /// runs an output in parallel
    pub fn run_output_task<N: ToString>(
        mut self,
        name: N,
        t: Box<dyn OutputTask>,
    ) -> Result<Self, JobRunnerError> {
        let n = name.to_string();
        self.job_state.start_new_stream(&n, &self.config)?;
        match t.create() {
            Ok(jh) => {
                // ensure that the job continues but in the end this task finishes
                self.cur_step_index += 1;
                self.await_data_output(&n, jh);
                Ok(self)
            }
            Err(e) => Err(JobRunnerError::GenericError {
                message: format!("Failed running an DataOutput task due to: {}", e),
            }),
        }
    }

    /// uses the StreamHandler trait to do the final custom transform.
    /// This method gives a lot of flexibility in what you can execute
    /// during the execute (like external apis).  During the init, and
    /// shutdown step give extra options.  See the relevant docs on that
    pub async fn run_stream_handler<I, S: Into<String>>(
        mut self,
        name: S,
        ds: Box<dyn DataSource<I>>,
        mut job_handler: Box<dyn StreamHandler<I>>,
    ) -> Result<Self, JobRunnerError>
    where
        I: Debug + Send + Sync + 'static,
    {
        use stream::StepStreamStatus;
        let (mut rx, source_stream_jh) = ds.start_stream()?;
        let stream_name = name.into();
        self.job_state = self.load_job_state().await?;

        match self.job_state.start_new_stream(&stream_name, &self.config) {
            Ok((_, StepStreamStatus::Complete { .. })) => {
                self.log_info(
                    self.job_state.name(),
                    format!("{} stream previously ran, skipping", &stream_name),
                )
                .await;
            }
            Err(e) => {
                return Err(e);
            }
            Ok(_) => {
                let jr_action = job_handler.init(&self).await?;
                let index_start = match &jr_action {
                    JobRunnerAction::Start => 0,
                    JobRunnerAction::Resume { index } => *index,
                    JobRunnerAction::Skip => {
                        //self.job_state.streams.complete(stream_name)?;
                        self.job_state
                            .stream_ok(stream_name, &self.config, Vec::new())?;
                        self.save_job_state().await?;
                        self.log_info(
                            self.job_state.name(),
                            "StreamHandler requested the job to be skipped",
                        )
                        .await;
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
                                        JobItemInfo::new((
                                            self.num_processed_items,
                                            self.job_state.name(),
                                        )),
                                        line,
                                        &self,
                                    )
                                    .await
                                {
                                    Ok(()) => {
                                        self.num_processed_items += 1;
                                        self.job_state
                                            .stream_incr_count_ok(&stream_name, &source)?;
                                    }
                                    Err(er) => {
                                        self.log_err(
                                            self.job_state.name(),
                                            Some(&JobItemInfo::new((
                                                self.num_processed_items,
                                                self.job_state.name(),
                                            ))),
                                            er.to_string(),
                                        )
                                        .await;
                                        self.job_state.stream_incr_count_err(&stream_name)?;
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
                            )
                            .await;
                            self.job_state.stream_incr_count_err(&stream_name)?;
                            self.num_process_item_errors += 1;
                        }
                        None => {
                            break;
                        }
                    };
                    match self.process_job_manager_rx().await {
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
                /*
                self.job_state
                    .streams
                    .set_total_lines(stream_name, self.num_processed_items)?;
                */
                self.cur_step_index += 1;
                job_handler.shutdown(&mut self).await?;

                // only wait if everything is okay
                source_stream_jh.await??;
                let mut output_stats = Vec::new();
                for (_name, join_handle) in self.data_output_handles {
                    let s = join_handle.await??;
                    output_stats.push(s);
                }
                self.job_state
                    .stream_ok(stream_name, &self.config, output_stats)?;
                self.data_output_handles = Vec::new();
                self.save_job_state().await?;
            }
        }
        Ok(self)
    }

    //TODO: it appears that after you press control-c to terminate program while it is running
    //the next time around it will think it already ran.. need to investigate
    pub async fn run_cmd(mut self, job_cmd: Box<dyn JobCommand>) -> Result<Self, JobRunnerError> {
        self.job_state = self.load_job_state().await?;

        let name = job_cmd.name();
        match self.job_state.start_new_cmd(&name, &self.config) {
            Ok((_, StepCommandStatus::Complete { .. })) => {
                self.log_info(
                    self.job_state.name(),
                    format!("{} command previously ran, skipping", &name),
                )
                .await;
            }
            Err(e) => {
                // complete job which closes some things off
                self.complete().await?;
                return Err(e);
            }
            Ok((_, _)) => {
                match job_cmd.run(&mut self).await {
                    Ok(()) => {
                        self.job_state.cmd_ok(name, &self.config)?;
                    }
                    Err(er) => {
                        self.job_state.cmd_not_ok(&name, er.to_string())?;
                        self.log_err(
                            self.job_state.name(),
                            None,
                            format!("{} command ran into an error: {}", name, er),
                        )
                        .await;
                    }
                };
                self.save_job_state().await?;
            }
        }
        self.cur_step_index += 1;
        Ok(self)
    }
}

#[derive(Debug, Serialize)]
#[serde(crate = "serde")]
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
    use super::*;
    use etl_core::deps::thiserror::{self, Error};
    /// These are all fatal errors which can stop the execution of a pipeline defined by the
    /// JobRunner
    #[derive(Serialize, Error, Clone, Debug, PartialEq)]
    #[serde(crate = "serde", tag = "error", content = "details")]
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
        #[error("Step {name} at index {step_index} in the job returned: {message}")]
        JobStepError {
            step_index: usize,
            name: String,
            message: String,
        },
        #[error("GenericError: {message}")]
        GenericError { message: String },
        /// Errors returned from DataSource or DataOutputs
        #[error("StreamError: {message}")]
        StreamError { message: String },
    }

    impl From<anyhow::Error> for JobRunnerError {
        fn from(er: anyhow::Error) -> Self {
            JobRunnerError::GenericError {
                message: er.to_string(),
            }
        }
    }

    use etl_core::datastore::error::DataStoreError;
    impl From<DataStoreError> for JobRunnerError {
        fn from(er: DataStoreError) -> Self {
            JobRunnerError::StreamError {
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
