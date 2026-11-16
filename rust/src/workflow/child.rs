//! Children (I-11): runs started from a workflow, whose result the parent may await, and the
//! messages a workflow sends.

use std::future::IntoFuture;
use std::marker::PhantomData;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::Context;
use super::scheduler::Answer;
use super::{AsWorkflow, decode, input_object, json};
use crate::client::RunStatus;
use crate::cost::Budget;
use crate::error::{Error, Result};
use crate::names::{check_key, check_name};
use crate::proto::boundary::v1::{self as b, call::Call, called::Called};
use crate::time::millis;

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
            budget: workflow.budget().cloned(),
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

    /// The value the chain `chain` ended with, a child of this run or not; `RunFailed` if it
    /// failed or was cancelled, `RunNotFound` if there is none, with `.timeout(..)`,
    /// `WaitTimeout` once it passed first.
    pub fn result<T: DeserializeOwned>(&self, chain: &str) -> ChainResult<T> {
        ChainResult {
            ctx: self.clone(),
            chain: chain.to_owned(),
            timeout: None,
            output: PhantomData,
        }
    }
}

/// The end of a chain, awaited to wait for it.
#[must_use = "a result does nothing until awaited"]
pub struct ChainResult<T> {
    ctx: Context,
    chain: String,
    timeout: Option<Duration>,
    output: PhantomData<fn() -> T>,
}

impl<T> ChainResult<T> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<T: DeserializeOwned + 'static> IntoFuture for ChainResult<T> {
    type Output = Result<T>;
    type IntoFuture = BoxFuture<'static, Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            check_key("run id", &self.chain)?;
            let call = b::call::ChainResult {
                chain: self.chain.clone(),
                timeout_ms: self.timeout.map(millis),
            };
            let resolved = self.ctx.resolved(Call::ChainResult(call)).await;
            failure_read(&self.ctx, &self.chain, &resolved);
            decode(resolved?)
        })
    }
}

/// Tells the clock when a result of `chain` gave the code its failure.
fn failure_read<T>(ctx: &Context, chain: &str, resolved: &Result<T>) {
    if let Err(Error::RunFailed {
        status: RunStatus::Failed,
        ..
    }) = resolved
    {
        ctx.shared.clock().failure_read(chain);
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
    budget: Option<Budget>,
    output: PhantomData<fn() -> O>,
}

impl<O> StartChild<O> {
    /// Starts the child under this id instead of one derived from the parent's.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_owned());
        self
    }

    /// The child's budget, instead of its workflow's.
    pub fn budget(mut self, budget: impl Into<Budget>) -> Self {
        self.budget = Some(budget.into());
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
                budget: self.budget.as_ref().map(Budget::to_proto).transpose()?,
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
            if self.name.starts_with("substantial.") {
                return Err(Error::InvalidArgument(format!(
                    "message name {:?} starts with substantial., which is reserved",
                    self.name
                )));
            }
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
        let resolved = self
            .ctx
            .resolved(Call::ChildResult(self.chain.clone()))
            .await;
        failure_read(&self.ctx, &self.chain, &resolved);
        decode(resolved?)
    }

    /// Sends the child a message, as `Context::send`.
    pub fn send(&self, name: &str, value: impl Serialize) -> SendFromWorkflow {
        self.ctx.send(&self.chain, name, value)
    }

    /// Cancels the child as a client's `Run::cancel` does; returns once the outcome is recorded,
    /// before the child ends: `result()` then fails with `RunFailed`, cancelled.
    pub async fn cancel(&self, reason: &str) -> Result<()> {
        let cancel = b::call::Cancel {
            run: self.chain.clone(),
            reason: reason.to_owned(),
        };
        match self.ctx.call(Call::Cancel(cancel)).await? {
            Answer::Resolved(_) => Ok(()),
            Answer::Called(called) => Err(Error::Protocol(format!("cancel answered {called:?}"))),
        }
    }
}
