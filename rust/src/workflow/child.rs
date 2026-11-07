//! Children (I-11): runs started from a workflow, whose result the parent may await, and the
//! messages a workflow sends.

use std::future::IntoFuture;
use std::marker::PhantomData;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::Context;
use super::scheduler::Answer;
use super::{AsWorkflow, decode, input_object, json};
use crate::error::{Error, Result};
use crate::names::{check_key, check_name};
use crate::proto::boundary::v1::{self as b, call::Call, called::Called};

/// A run started by this one, whose result is an `O`.
pub struct Child<O> {
    ctx: Context,
    chain: String,
    output: PhantomData<fn() -> O>,
}

impl<O> Clone for Child<O> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            chain: self.chain.clone(),
            output: PhantomData,
        }
    }
}

impl Context {
    /// Starts a child run of `workflow`, under `.id(..)`, or an id the core derives from this
    /// run's.
    pub fn start<W: AsWorkflow + ?Sized>(
        &self,
        workflow: &W,
        input: impl Serialize,
    ) -> StartChild<W::Output> {
        let workflow = workflow.workflow();
        StartChild {
            ctx: self.clone(),
            workflow: workflow.name().to_owned(),
            version: workflow.version(),
            input: input_object(input),
            id: None,
            output: PhantomData,
        }
    }

    /// Sends a message to the last run of the chain `run`, delivered once per `.id(..)`, which
    /// the core derives from this call when none is given.
    pub fn send(&self, run: &str, name: &str, value: impl Serialize) -> SendFromWorkflow {
        SendFromWorkflow {
            ctx: self.clone(),
            run: run.to_owned(),
            name: name.to_owned(),
            value: serde_json::to_vec(&value).map_err(Error::from),
            id: None,
        }
    }
}

/// A start of a child run, awaited to start it.
#[must_use = "a start does nothing until awaited"]
pub struct StartChild<O> {
    ctx: Context,
    workflow: String,
    version: Option<u32>,
    input: Result<Vec<u8>>,
    id: Option<String>,
    output: PhantomData<fn() -> O>,
}

impl<O> StartChild<O> {
    /// Starts the child under this id instead of one derived from the parent's.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_owned());
        self
    }
}

impl<O: 'static> IntoFuture for StartChild<O> {
    type Output = Result<Child<O>>;
    type IntoFuture = BoxFuture<'static, Result<Child<O>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if let Some(id) = &self.id {
                check_key("run id", id)?;
            }
            let start = b::call::Start {
                workflow: self.workflow,
                version: self.version,
                input: Some(json(self.input?)?),
                id: self.id,
            };
            match self.ctx.call(Call::Start(start)).await? {
                Answer::Called(Called::Run(chain)) => Ok(Child {
                    ctx: self.ctx,
                    chain,
                    output: PhantomData,
                }),
                _ => Err(Error::Protocol("start answered other than a run".into())),
            }
        })
    }
}

/// A message a workflow sends, awaited to send it.
#[must_use = "a message is not sent until awaited"]
pub struct SendFromWorkflow {
    ctx: Context,
    run: String,
    name: String,
    value: Result<Vec<u8>>,
    id: Option<String>,
}

impl SendFromWorkflow {
    /// Delivers the message once whatever the number of sends with this id.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_owned());
        self
    }
}

impl IntoFuture for SendFromWorkflow {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'static, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            check_key("run id", &self.run)?;
            check_name("message name", &self.name)?;
            if let Some(id) = &self.id {
                check_key("message id", id)?;
            }
            let send = b::call::Send {
                run: self.run,
                name: self.name,
                value: Some(json(self.value?)?),
                id: self.id,
            };
            match self.ctx.call(Call::Send(send)).await? {
                Answer::Resolved(_) => Ok(()),
                Answer::Called(called) => Err(Error::Protocol(format!("send answered {called:?}"))),
            }
        })
    }
}

impl<O> Child<O> {
    /// The child's chain: the id a client gives `Substantial::run`.
    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// Whether the child's chain ended, as this replay has seen so far: for a `wait_until`.
    pub fn done(&self) -> bool {
        self.ctx.shared.has_ended(&self.chain)
    }

    /// The value the child's chain ended with; `RunFailed` if it failed or was cancelled.
    pub async fn result(&self) -> Result<O>
    where
        O: DeserializeOwned,
    {
        decode(
            self.ctx
                .resolved(Call::ChildResult(self.chain.clone()))
                .await?,
        )
    }

    /// Sends the child a message, as `Context::send`.
    pub fn send(&self, name: &str, value: impl Serialize) -> SendFromWorkflow {
        self.ctx.send(&self.chain, name, value)
    }
}
