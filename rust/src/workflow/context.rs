use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::log::Log;
use super::scheduler::{Answer, CallFuture, Shared};
use super::{decode, json};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, call::Call, called::Called};
use crate::proto::layout::v1::{self as layout, Time, Value};
use crate::time::millis;

/// What workflow code calls; one per replay, cheap to clone.
#[derive(Clone)]
pub struct Context {
    /// The run's logger, muted while a replay walks the recorded history.
    pub log: Log,
    pub(super) shared: Arc<Shared>,
    /// The saga the steps called through this context belong to.
    pub(super) saga: Option<u64>,
}

impl Context {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self::in_saga(shared, None)
    }

    pub(super) fn in_saga(shared: Arc<Shared>, saga: Option<u64>) -> Self {
        Self {
            log: Log::new(shared.clone()),
            shared,
            saga,
        }
    }

    pub(super) fn call(&self, call: Call) -> CallFuture {
        CallFuture::new(self.shared.clone(), call, None)
    }

    pub(super) async fn resolved(&self, call: Call) -> Result<Option<Value>> {
        value(self.call(call).await?)
    }

    /// The chain of the run: the id a client gives `Substantial::run`, the same through its
    /// continuations.
    pub fn chain(&self) -> &str {
        self.shared.chain()
    }

    /// The time, recorded.
    pub async fn now(&self) -> Result<DateTime<Utc>> {
        let text: String = decode(self.resolved(Call::Now(b::Empty {})).await?)?;
        DateTime::parse_from_rfc3339(&text)
            .map(|t| t.with_timezone(&Utc))
            .map_err(|e| Error::Protocol(format!("now {text:?}: {e}")))
    }

    pub async fn sleep(&self, d: Duration) -> Result<()> {
        self.resolved(Call::SleepMs(millis(d))).await.map(drop)
    }

    /// Sleeps until `t`; a time that has passed fires at once.
    pub async fn sleep_until(&self, t: DateTime<Utc>) -> Result<()> {
        let until = Time {
            ms: t.timestamp_millis(),
        };
        self.resolved(Call::SleepUntil(until)).await.map(drop)
    }

    /// The next message of `name` that no `receive` took yet.
    pub async fn receive<T: DeserializeOwned>(&self, name: &str) -> Result<T> {
        decode(self.resolved(Call::Receive(name.to_owned())).await?)
    }

    /// Makes `value` the last one published under `key` by the chain.
    pub fn publish(&self, key: &str, value: impl Serialize) -> Result<()> {
        let published = layout::Published {
            key: key.to_owned(),
            value: Some(json(serde_json::to_vec(&value)?)?),
        };
        done(self.shared.call(Call::Publish(published))?)
    }

    /// Ends the run cancelled, once returned as the workflow's error: `return Err(c.cancel(..).await)`.
    pub async fn cancel(&self, reason: &str) -> Error {
        Error::Cancelled(reason.to_owned())
    }

    /// Waits until `predicate` holds, evaluated after each message, child end and timer.
    pub fn wait_until(&self, predicate: impl Fn() -> bool + Send + 'static) -> WaitUntil {
        WaitUntil {
            ctx: self.clone(),
            predicate: Box::new(predicate),
            timeout: None,
        }
    }

    /// A float in [0, 1), recorded.
    pub async fn random(&self) -> Result<f64> {
        decode(self.resolved(Call::Random(b::Empty {})).await?)
    }

    /// A UUID, recorded.
    pub async fn uuid(&self) -> Result<String> {
        decode(self.resolved(Call::Uuid(b::Empty {})).await?)
    }
}

pub(super) fn value(answer: Answer) -> Result<Option<Value>> {
    match answer {
        Answer::Resolved(v) => Ok(v),
        Answer::Called(Called::Value(v)) => Ok(Some(v)),
        Answer::Called(Called::Done(_)) => Ok(None),
        Answer::Called(other) => Err(Error::Protocol(format!("unexpected answer {other:?}"))),
    }
}

fn done(called: Called) -> Result<()> {
    match called {
        Called::Done(_) => Ok(()),
        other => Err(Error::Protocol(format!("expected done, got {other:?}"))),
    }
}

/// A wait on a predicate, awaited to `true` once it holds or `false` when its timeout passes.
#[must_use = "a wait does nothing until awaited"]
pub struct WaitUntil {
    ctx: Context,
    predicate: Box<dyn Fn() -> bool + Send>,
    timeout: Option<Duration>,
}

impl WaitUntil {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl IntoFuture for WaitUntil {
    type Output = Result<bool>;
    type IntoFuture = BoxFuture<'static, Result<bool>>;

    fn into_future(self) -> Self::IntoFuture {
        let call = Call::Wait(b::call::Wait {
            timeout_ms: self.timeout.map(millis),
        });
        let shared = self.ctx.shared.clone();
        Box::pin(async move {
            let mut predicate = Some(self.predicate);
            let mut future = std::pin::pin!(CallFuture::new(shared.clone(), call, None));
            let answer = std::future::poll_fn(|cx| {
                let poll = future.as_mut().poll(cx);
                if let (Some(id), Some(p)) = (future.pending_call(), predicate.take()) {
                    shared.add_wait(id, p);
                }
                poll
            })
            .await?;
            decode(value(answer)?)
        })
    }
}
