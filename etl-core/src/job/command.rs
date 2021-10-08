use super::*;
use chrono::{DateTime, Utc};
use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[async_trait]
/// Designed for situations where you need to execute a single action like sql queries, cleanup, or
/// any other side effect
pub trait JobCommand: Sync + Send {
    fn name(&self) -> String;
    async fn run(self: Box<Self>, job: &mut JobRunner) -> anyhow::Result<()>;
}

pub struct SimpleCommand<'a> {
    name: String,
    run_command:
        Box<dyn Fn(&'_ JobRunner) -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync>,
}

impl<'a> SimpleCommand<'a> {
    pub fn new<S, F>(name: S, callback: F) -> Box<dyn JobCommand + 'a>
    where
        S: Into<String>,
        F: Fn(&'_ JobRunner) -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync,
    {
        Box::new(SimpleCommand {
            name: name.into(),
            run_command: Box::new(callback),
        })
    }
}

#[async_trait]
impl<'a> JobCommand for SimpleCommand<'a> {
    async fn run(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        //let js = jr.get_state();
        (self.run_command)(jr).await?;
        Ok(())
    }

    fn name(&self) -> String {
        self.name.clone()
    }
}

/// for use when you want to provide some sharable resource into the command that you can't read
/// from job state (like creds, so they don't make it into the state)
pub struct SimpleCommandWith<'a, D: Send + Sync> {
    name: String,
    data: D,
    run_command:
        Box<dyn Fn(D, &'_ JobRunner) -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync>,
}

impl<'a, D: 'a + Send + Sync> SimpleCommandWith<'a, D> {
    pub fn new<S, F>(name: S, data: D, callback: F) -> Box<dyn JobCommand + 'a>
    where
        S: Into<String>,
        F: Fn(D, &'_ JobRunner) -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync,
    {
        Box::new(SimpleCommandWith {
            name: name.into(),
            data,
            run_command: Box::new(callback),
        })
    }
}

#[async_trait]
impl<'a, D: 'a + Send + Sync> JobCommand for SimpleCommandWith<'a, D> {
    async fn run(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        (self.run_command)(self.data, jr).await?;
        Ok(())
    }

    fn name(&self) -> String {
        self.name.clone()
    }
}

/*
pub struct Command<'a> {
    name: String,
    run_command:
        Box<dyn Fn() -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync>,
}

impl<'a> Command<'a> {
    pub fn new<S, F>(name: S, callback: F) -> Box<dyn JobCommand + 'a>
    where
        S: Into<String>,
        F: Fn() -> BoxFuture<'a, anyhow::Result<()>> + 'static + Send + Sync,
    {
        Box::new(Command {
            name: name.into(),
            run_command: Box::new(callback),
        })
    }
}

#[async_trait]
impl<'a> JobCommand for Command<'a> {
    async fn run(self: Box<Self>, _: &JobRunner) -> anyhow::Result<()> {
        (self.run_command)().await?;
        Ok(())
    }

    fn name(&self) -> String {
        self.name.clone()
    }
}
*/

/*
use core::future::Future;
use core::pin::Pin;
#[async_trait]
impl<'a> JobCommand for Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Sync + 'static>> {
    async fn run(self: Box<Self>, _: &JobRunner) -> anyhow::Result<()> {
        self.await?;
        Ok(())
    }

    fn name(&self) -> String {
       String::from("BoxFuture") 
    }
}
*/

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "status")]
pub enum StepCommandStatus {
    InProgress {
        started: DateTime<Utc>,
    },
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

impl StepCommandStatus {
    pub fn started_on(&self) -> DateTime<Utc> {
        match self {
            StepCommandStatus::InProgress { ref started, .. } => started.clone(),
            StepCommandStatus::Complete { ref started, .. } => started.clone(),
            StepCommandStatus::Error { ref started, .. } => started.clone(),
        }
    }
}
