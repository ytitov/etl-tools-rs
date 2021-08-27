use super::*;
use chrono::{DateTime, Utc};
use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[async_trait]
/// Designed for situations where you need to execute a single action like sql queries, cleanup, or
/// any other side effect
pub trait JobCommand: Sync + Send {
    fn name(&self) -> String;
    async fn run(self: Box<Self>, job: &JobRunner) -> anyhow::Result<()>;
}

pub struct SimpleCommand<'a> {
    name: String,
    run_command:
        Box<dyn Fn() -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync>,
}

impl<'a> SimpleCommand<'a> {
    pub fn new<S, F>(name: S, callback: F) -> Box<dyn JobCommand + 'a>
    where
        S: Into<String>,
        F: Fn() -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync,
    {
        Box::new(SimpleCommand {
            name: name.into(),
            run_command: Box::new(callback),
        })
    }
}

#[async_trait]
impl<'a> JobCommand for SimpleCommand<'a> {
    async fn run(self: Box<Self>, _: &JobRunner) -> anyhow::Result<()> {
        (self.run_command)().await?;
        Ok(())
    }

    fn name(&self) -> String {
        self.name.clone()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "state")]
pub enum JobCommandStatus {
    Complete {
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
    },
    Error {
        started: DateTime<Utc>,
        message: String,
        datetime: DateTime<Utc>,
    },
}
