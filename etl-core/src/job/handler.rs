use super::*;
use futures_core::future::BoxFuture;

pub type BoxedCreateStreamHandlerResult<T> = anyhow::Result<Box<dyn StreamHandler<T>>>;
pub type CreateStreamHandlerFn<'a, R> = Box<
    dyn Fn(&'_ mut JobRunner) -> BoxFuture<'a, BoxedCreateStreamHandlerResult<R>>
        + 'static
        + Send
        + Sync,
>;
pub type CreateStreamHandlerForEachFn<'a, T> = Box<
    dyn Fn(T) -> BoxFuture<'a, anyhow::Result<()>>
        + 'static
        + Send
        + Sync,
>;

#[async_trait]
/// Meant to be used for a variety situations like calling external apis, or
/// reacting to the output of the stream in some fashion where returning
/// a single element does not make sense
pub trait StreamHandler<T>: Sync + Send
where
    T: DeserializeOwned + Serialize + Debug + Send + Sync,
{
    /// Optionally let the job handler decide if it needs to be skipped or resume.
    /// By default it will always start
    async fn init(&mut self, _: &JobRunner) -> anyhow::Result<JobRunnerAction> {
        Ok(JobRunnerAction::Start)
    }

    async fn shutdown(self: Box<Self>, _: &mut JobRunner) -> anyhow::Result<()>;

    async fn process_item(&self, _: JobItemInfo, item: T, job: &JobRunner) -> anyhow::Result<()>;
}

pub enum TransformOutput<T>
where
    T: Serialize + Debug + Send + Sync,
{
    Item(T),
    /// if there are any sort of expanding
    List(Vec<T>),
}

#[async_trait]
/// for situations where using a simple function pointer won't work, here the mapping
/// function is async.  This trait is used to convert the JobRunner into a DataSource
/// so one could implement further pipelines with this
pub trait TransformHandler<I, O>: Sync + Send
where
    I: DeserializeOwned + Debug + Send + Sync,
    O: Serialize + Debug + Send + Sync,
{
    /// optionally override to help identifying in cases of errors or loggin
    fn name(&self) -> &str {
        return "TransformHandler";
    }

    /// return None if this particular item should be filtered out
    async fn transform_item(
        &self,
        _: JobItemInfo,
        item: I,
    ) -> anyhow::Result<Option<TransformOutput<O>>>;
}
