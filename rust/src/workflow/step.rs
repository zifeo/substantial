//! Steps (I-6): the builder, its undo (I-10), and the step key.

use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::{Context, value};
use super::schedule::RESERVED_STEP;
use super::scheduler::{CallFuture, StepBody, StepSpec, Undo};
use super::{Permanent, Retry, decode};
use crate::error::{Error, Failure, Result};
use crate::names::check_step_name;
use crate::proto::boundary::v1::{self as b, call::Call};
use crate::proto::layout::v1::Amount;
use crate::time::millis;

/// The step attempt running: its step key, and what it charged so far.
#[derive(Clone)]
pub(crate) struct LiveAttempt {
    key: String,
    charges: Arc<Mutex<Vec<Amount>>>,
}

impl LiveAttempt {
    pub(crate) fn new(key: String) -> Self {
        Self {
            key,
            charges: Arc::default(),
        }
    }

    /// What the attempt charged, in call order.
    pub(crate) fn charges(&self) -> Vec<Amount> {
        self.charges.lock().expect("never poisoned").clone()
    }
}

tokio::task_local! {
    static ATTEMPT: LiveAttempt;
}

/// `<run>/<step>/<occurrence>`, the same for every attempt and replay of the step running.
///
/// # Panics
///
/// Outside a step attempt, a bug of the calling code.
pub fn step_key() -> String {
    ATTEMPT
        .try_with(|a| a.key.clone())
        .expect("step_key is called inside a step")
}

/// Adds `nano` nano-units of `currency` to the charges of the step attempt running, recorded with
/// its outcome and counted in the run's cost; an unknown currency fails the step at once with
/// `InvalidArgument`, once the attempt ended. `InvalidArgument` for `nano` over `i64::MAX`.
///
/// # Panics
///
/// Outside a step attempt, as `step_key`.
pub fn charge(currency: &str, nano: u64) -> Result<()> {
    let charges = ATTEMPT
        .try_with(|a| a.charges.clone())
        .expect("charge is called inside a step");
    // § cvnf47vy8.a@s1aej
    if nano > i64::MAX as u64 {
        return Err(Error::InvalidArgument(format!(
            "a charge of {nano} is over the limit of {}",
            i64::MAX
        )));
    }
    // § end cvnf47vy8.a
    // § cvnf47vy8@z24wj
    charges.lock().expect("never poisoned").push(Amount {
        currency: currency.to_owned(),
        nano,
    });
    Ok(())
}

/// Runs an attempt with its step key and its charges.
pub(crate) fn with_attempt<F: Future>(
    running: LiveAttempt,
    attempt: F,
) -> impl Future<Output = F::Output> {
    ATTEMPT.scope(running, attempt)
}

pub(super) fn failure(kind: &str, message: String, retryable: bool) -> Failure {
    Failure {
        kind: kind.into(),
        message,
        retryable,
        details: None,
    }
}

/// The outcome of an attempt as the run machine takes it: a `Permanent` error is not retryable.
async fn outcome<T, E>(
    attempt: impl Future<Output = std::result::Result<T, E>>,
) -> Result<Vec<u8>, Failure>
where
    T: Serialize,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    // § epxcjcz4s@zgp55
    match attempt.await {
        Ok(value) => super::to_json(&value).map_err(|e| failure("Json", e.to_string(), false)),
        Err(e) => {
            // § epxcjcz4s.a@paszp
            // An `anyhow::Error` turned into a box hides the error it wraps from `downcast_ref`.
            let wrapped = (&e as &dyn std::any::Any)
                .downcast_ref::<anyhow::Error>()
                .is_some_and(|e| e.is::<Permanent>());
            let e = e.into();
            let permanent = wrapped || e.downcast_ref::<Permanent>().is_some();
            Err(failure("Error", e.to_string(), !permanent))
        }
    }
}

impl Context {
    /// Runs `f` as the step `name`: recorded once it returns, its recorded value on a replay.
    ///
    /// `f` may run more than once (attempts, crashes), so it builds a fresh future each time. An
    /// attempt the worker abandons, past its timeout or its worker's shutdown grace, is dropped
    /// at its await: the code after it never runs.
    pub fn step<T, E, F, Fut>(&self, name: &str, f: F) -> Step<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
    {
        let body: StepBody = Arc::new(move || Box::pin(outcome(f())));
        Step {
            ctx: self.clone(),
            name: name.to_owned(),
            retry: None,
            timeout: None,
            body,
            undo: None,
            reserved: false,
            value: PhantomData,
        }
    }

    /// Runs the blocking `f` as the step `name`, on tokio's blocking threads. A thread cannot be
    /// stopped: an attempt abandoned, past its timeout or its worker's shutdown grace, runs to
    /// its end with its outcome dropped, maybe while the next one runs.
    pub fn step_blocking<T, E, F>(&self, name: &str, f: F) -> Step<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
        F: Fn() -> std::result::Result<T, E> + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        self.step(name, move || {
            let f = f.clone();
            async move {
                // § v4n6ja42e.b@hxb6g
                #[cfg(feature = "testing")]
                crate::testing::threaded();
                // The thread runs outside the attempt's task: it takes the attempt along, for
                // `step_key` and `charge`.
                let running = ATTEMPT.try_with(LiveAttempt::clone).ok();
                let blocking = move || match running {
                    Some(running) => ATTEMPT.sync_scope(running, || f()),
                    None => f(),
                };
                match tokio::task::spawn_blocking(blocking).await {
                    Ok(result) => result,
                    // A panic goes on in the attempt's task, as it would from an async step.
                    Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Err(e) => panic!("a blocking step attempt: {e}"),
                }
            }
        })
    }

    /// The step `name`, whose attempts run `body`, which gives its outcome as the run machine
    /// takes it: a Wasm workflow's step.
    #[cfg(feature = "wasm-runner")]
    pub(crate) fn step_of<T>(&self, name: &str, body: StepBody) -> Step<T> {
        Step {
            ctx: self.clone(),
            name: name.to_owned(),
            retry: None,
            timeout: None,
            body,
            undo: None,
            reserved: false,
            value: PhantomData,
        }
    }
}

/// A step call, awaited to run it; `retry`, `timeout` and `undo` set its options first.
#[must_use = "a step does nothing until awaited"]
pub struct Step<T> {
    ctx: Context,
    name: String,
    retry: Option<Retry>,
    timeout: Option<Duration>,
    body: StepBody,
    undo: Option<Undo>,
    /// Whether it may take the name `Context::schedule` records its fires under.
    reserved: bool,
    value: PhantomData<fn() -> T>,
}

impl<T> Step<T> {
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Per attempt, instead of the workflow's or the worker's `step_timeout`, larger or smaller:
    /// a late attempt counts as failed, a retryable `StepTimeout`, and its result is discarded.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub(super) fn reserved(mut self) -> Self {
        self.reserved = true;
        self
    }
}

impl<T: DeserializeOwned + Send + 'static> Step<T> {
    /// How a rollback or a saga undoes this step: `f` gets the step's value, and runs as the step
    /// `<name>.undo` with this step's retry.
    pub fn undo<E, F, Fut>(mut self, f: F) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send + 'static,
    {
        let f = Arc::new(f);
        self.undo = Some(Arc::new(move |v| {
            let f = f.clone();
            Arc::new(move || {
                let (f, v) = (f.clone(), v.clone());
                Box::pin(async move {
                    let value: T = decode(v).map_err(|e| failure("Json", e.to_string(), false))?;
                    outcome(f(value)).await
                })
            })
        }));
        self
    }
}

impl<T: DeserializeOwned + Send + 'static> IntoFuture for Step<T> {
    type Output = Result<T>;
    type IntoFuture = BoxFuture<'static, Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        // § vnykycs34.b@b7gk5
        if self.name == RESERVED_STEP && !self.reserved {
            return Box::pin(async {
                Err(Error::InvalidArgument(format!(
                    "the step name {RESERVED_STEP:?} is reserved for c.schedule()"
                )))
            });
        }
        // § end vnykycs34.b
        if let Err(e) = check_step_name(&self.name) {
            return Box::pin(async { Err(e) });
        }
        if let Some(r) = &self.retry
            && let Err(e) = r.check()
        {
            return Box::pin(async { Err(e) });
        }
        let retry = self.retry.map(|r| b::RetryPolicy {
            max_attempts: r.max_attempts,
            initial_ms: millis(r.initial),
            max_ms: millis(r.max),
            multiplier: r.multiplier,
        });
        // § aym52h8ch@c5z6g
        let timeout = millis(self.timeout.unwrap_or(self.ctx.shared.step_timeout()));
        // § end aym52h8ch
        let options = b::StepOptions {
            retry,
            timeout_ms: Some(timeout),
            undo: self.undo.is_some(),
            saga: self.ctx.saga,
            occurrence: None,
        };
        let call = Call::Step(b::call::Step {
            name: self.name,
            options: Some(options),
        });
        let spec = StepSpec {
            body: self.body,
            undo: self.undo.map(|u| (u, retry, timeout)),
        };
        // § vjy5p3exq@njmrs
        let future = CallFuture::new(self.ctx.shared.clone(), call, Some(spec));
        Box::pin(async move { decode(value(future.await?)?) })
        // § end vjy5p3exq
    }
}
