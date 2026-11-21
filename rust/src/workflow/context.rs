use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::{DeserializeOwned, IgnoredAny};

use super::log::Log;
use super::scheduler::{Answer, CallFuture, Receives, Shared};
use super::{decode, json};
use crate::cost::Cost;
use crate::error::{Error, Result};
use crate::names::check_name;
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

    /// The next message of `name` that no `receive` took yet; with `.timeout(..)`, `WaitTimeout`
    /// once it passed with none.
    pub fn receive<T: DeserializeOwned>(&self, name: &str) -> Receive<T> {
        Receive {
            ctx: self.clone(),
            name: name.to_owned(),
            timeout: None,
            output: PhantomData,
        }
    }

    /// Makes `value` the last one published under `key` by the chain.
    pub fn publish(&self, key: &str, value: impl Serialize) -> Result<()> {
        // § h8wg39cct.c@75j6j
        let published = layout::Published {
            key: key.to_owned(),
            value: Some(json(super::argument_json(&value, "the value")?)?),
        };
        done(self.shared.call(Call::Publish(published))?)
    }

    /// Ends the run cancelled, once returned as the workflow's error: `return Err(c.cancel(..).await)`.
    pub async fn cancel(&self, reason: &str) -> Error {
        // § g039jvy6k.c@vfy5j
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

    /// The chain's cost so far, recorded.
    pub async fn cost(&self) -> Result<Cost> {
        decode(self.resolved(Call::Cost(b::Empty {})).await?)
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

/// A receive of a message, awaited to take it.
#[must_use = "a receive does nothing until awaited"]
pub struct Receive<T> {
    ctx: Context,
    name: String,
    timeout: Option<Duration>,
    output: PhantomData<fn() -> T>,
}

impl<T> Receive<T> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<T: DeserializeOwned + 'static> IntoFuture for Receive<T> {
    type Output = Result<T>;
    type IntoFuture = BoxFuture<'static, Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        let Receive {
            ctx, name, timeout, ..
        } = self;
        Box::pin(async move {
            check_name("message name", &name)?;
            let counts = ctx.shared.receives(&name);
            let _pending = Pending::new(&counts, &name, timeout.is_some())?;
            let Some(timeout) = timeout else {
                let received = ctx.call(Call::Receive(name));
                counts.lock().expect(POISONED).taken += 1;
                return decode(value(received.await?)?);
            };
            if !counts.lock().expect(POISONED).bound {
                let delivered = counts.clone();
                ctx.handle(&name, move |_: IgnoredAny| {
                    delivered.lock().expect(POISONED).delivered += 1;
                })?;
                counts.lock().expect(POISONED).bound = true;
            }
            // § p6tzbmjaq@e18c0
            let claimed = Arc::new(AtomicBool::new(false));
            // Claiming in the predicate keeps two receives woken by one message from both taking
            // it.
            let claim = {
                let claimed = claimed.clone();
                move || {
                    let mut counts = counts.lock().expect(POISONED);
                    if !claimed.load(Ordering::Relaxed) && counts.delivered > counts.taken {
                        counts.taken += 1;
                        claimed.store(true, Ordering::Relaxed);
                    }
                    claimed.load(Ordering::Relaxed)
                }
            };
            // § end p6tzbmjaq
            if !claim() {
                ctx.wait_until(claim).timeout(timeout).await?;
            }
            // § p6tzbmjaq@bk30n
            if !claimed.load(Ordering::Relaxed) {
                return Err(Error::WaitTimeout);
            }
            decode(ctx.resolved(Call::Receive(name)).await?)
            // § end p6tzbmjaq
        })
    }
}

const POISONED: &str = "the receive counts are never locked across a panic";

/// A receive pending from its first poll until it ends or is dropped.
struct Pending {
    counts: Arc<std::sync::Mutex<Receives>>,
    timed: bool,
}

impl Pending {
    fn new(counts: &Arc<std::sync::Mutex<Receives>>, name: &str, timed: bool) -> Result<Self> {
        // § p6tzbmjaq.a@8fww6
        let mut c = counts.lock().expect(POISONED);
        if (if timed { c.plain } else { c.timed }) > 0 {
            return Err(Error::InvalidArgument(format!(
                "a receive of {name:?} with a timeout and one without are pending at once: which \
                 one takes a message would depend on the order of the code's branches"
            )));
        }
        *(if timed { &mut c.timed } else { &mut c.plain }) += 1;
        Ok(Self {
            counts: counts.clone(),
            timed,
        })
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        let mut c = self.counts.lock().expect(POISONED);
        *(if self.timed {
            &mut c.timed
        } else {
            &mut c.plain
        }) -= 1;
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
