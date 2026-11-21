use std::fmt::Display;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::__bindings::substantial::guest::context as wit;
use crate::durations::whole_millis;
use crate::error::{Error, Result};
use crate::executor::Calling;

type LocalFuture<T> = Pin<Box<dyn Future<Output = T>>>;

/// What workflow code calls, `s::Context` as in the Rust host: each verb a call of the host,
/// recorded in the run's history.
#[derive(Clone)]
pub struct Context {
    /// The run's logger, muted while a replay walks the recorded history.
    pub log: Log,
    chain: Rc<str>,
}

impl Context {
    pub(crate) fn new() -> Self {
        Self {
            log: Log(()),
            chain: wit::chain().into(),
        }
    }

    /// The chain of the run: the id a client gives its start, the same through its
    /// continuations.
    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// The time, recorded.
    pub async fn now(&self) -> Result<DateTime<Utc>> {
        let text: String = answer(Calling::new(wit::now).await)?;
        DateTime::parse_from_rfc3339(&text)
            .map(|t| t.with_timezone(&Utc))
            .map_err(|e| Error::Json(format!("now {text:?}: {e}")))
    }

    pub async fn sleep(&self, d: Duration) -> Result<()> {
        let ms = whole_millis(d);
        answer(Calling::new(move || wit::sleep(ms)).await)
    }

    /// Sleeps until `t`; a time that has passed fires at once.
    pub async fn sleep_until(&self, t: DateTime<Utc>) -> Result<()> {
        let ms = t.timestamp_millis();
        answer(Calling::new(move || wit::sleep_until(ms)).await)
    }

    /// The next message of `name` that no `receive` took yet; with `.timeout(..)`, `WaitTimeout`
    /// once it passed with none.
    pub fn receive<T: DeserializeOwned>(&self, name: &str) -> Receive<T> {
        Receive {
            name: name.to_owned(),
            timeout: None,
            output: PhantomData,
        }
    }

    /// Sends the message `name` to the run `run`, a chain's id.
    pub fn send(&self, run: &str, name: &str, value: impl Serialize) -> SendFromWorkflow {
        SendFromWorkflow {
            run: run.to_owned(),
            name: name.to_owned(),
            value: serde_json::to_string(&value).map_err(Error::from),
            id: None,
        }
    }

    /// Makes `value` the last one published under `key` by the chain.
    pub fn publish(&self, key: &str, value: impl Serialize) -> Result<()> {
        let value = serde_json::to_string(&value)?;
        wit::publish(key, &value).map_err(Error::from)
    }

    /// Ends the run cancelled, once returned as the workflow's error: `return Err(c.cancel(..).await)`.
    pub async fn cancel(&self, reason: &str) -> Error {
        Error::Cancelled(reason.to_owned())
    }

    /// A float in [0, 1), recorded.
    pub async fn random(&self) -> Result<f64> {
        answer(Calling::new(wit::random).await)
    }

    /// A UUID, recorded.
    pub async fn uuid(&self) -> Result<String> {
        answer(Calling::new(wit::uuid).await)
    }

    #[doc(hidden)]
    pub fn __step<T>(&self, name: &'static str, args: Result<String>) -> Step<T> {
        Step {
            name,
            args,
            retry: None,
            timeout: None,
            output: PhantomData,
        }
    }
}

fn answer<T: DeserializeOwned>(ended: std::result::Result<String, wit::Raised>) -> Result<T> {
    Ok(serde_json::from_str(&ended?)?)
}

/// The logger of a run, `c.log`: lines the host writes once the replay is live.
#[derive(Clone)]
pub struct Log(());

macro_rules! level {
    ($name:ident, $level:ident) => {
        pub fn $name(&self, message: impl Display) {
            wit::log(wit::Level::$level, &message.to_string());
        }
    };
}

impl Log {
    level!(trace, Trace);
    level!(debug, Debug);
    level!(info, Info);
    level!(warn, Warn);
    level!(error, Error);
}

/// A receive of a message, awaited to take it.
#[must_use = "a receive does nothing until awaited"]
pub struct Receive<T> {
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
    type IntoFuture = LocalFuture<Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        let Receive { name, timeout, .. } = self;
        let timeout = timeout.map(whole_millis);
        Box::pin(async move { answer(Calling::new(move || wit::receive(&name, timeout)).await) })
    }
}

/// A send from a workflow, awaited to send it.
#[must_use = "a send does nothing until awaited"]
pub struct SendFromWorkflow {
    run: String,
    name: String,
    value: Result<String>,
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
    type IntoFuture = LocalFuture<Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let SendFromWorkflow {
                run,
                name,
                value: sent,
                id,
            } = self;
            let sent = sent?;
            answer(Calling::new(move || wit::send(&run, &name, &sent, id.as_deref())).await)
        })
    }
}

/// A step call, awaited to run it; `retry` and `timeout` set its options first.
#[must_use = "a step does nothing until awaited"]
pub struct Step<T> {
    name: &'static str,
    args: Result<String>,
    retry: Option<crate::Retry>,
    timeout: Option<Duration>,
    output: PhantomData<fn() -> T>,
}

impl<T> Step<T> {
    pub fn retry(mut self, retry: crate::Retry) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Per attempt, instead of the workflow's or the worker's `step_timeout`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<T: DeserializeOwned + 'static> IntoFuture for Step<T> {
    type Output = Result<T>;
    type IntoFuture = LocalFuture<Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        let options = wit::StepOptions {
            retry: self.retry.map(|r| wit::Retry {
                max_attempts: r.max_attempts,
                initial_ms: whole_millis(r.initial),
                max_ms: whole_millis(r.max),
                multiplier: r.multiplier,
            }),
            timeout_ms: self.timeout.map(whole_millis),
        };
        let (name, args) = (self.name, self.args);
        Box::pin(async move {
            let args = args?;
            answer(Calling::new(move || wit::step(name, &args, options)).await)
        })
    }
}
