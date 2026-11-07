//! Steps (I-6): the builder, its undo (I-10), and the step key.

use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::{Context, value};
use super::scheduler::{CallFuture, StepBody, StepSpec, Undo};
use super::{Permanent, Retry, decode};
use crate::error::{Failure, Result};
use crate::proto::boundary::v1::{self as b, call::Call};
use crate::time::millis;

tokio::task_local! {
    static STEP_KEY: String;
}

/// `<run>/<step>/<occurrence>`, the same for every attempt and replay of the step running.
///
/// # Panics
///
/// Outside a step attempt, a bug of the calling code.
pub fn step_key() -> String {
    STEP_KEY
        .try_with(String::clone)
        .expect("step_key is called inside a step")
}

/// Runs an attempt with its step key.
pub(crate) fn with_step_key<F: Future>(key: String, attempt: F) -> impl Future<Output = F::Output> {
    STEP_KEY.scope(key, attempt)
}

pub(super) fn failure(kind: &str, message: String, retryable: bool) -> Failure {
    Failure {
        kind: kind.into(),
        message,
        retryable,
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
    match attempt.await {
        Ok(value) => serde_json::to_vec(&value).map_err(|e| failure("Json", e.to_string(), false)),
        Err(e) => {
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
                #[cfg(feature = "testing")]
                crate::testing::threaded();
                match tokio::task::spawn_blocking(move || f()).await {
                    Ok(result) => result,
                    // A panic goes on in the attempt's task, as it would from an async step.
                    Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Err(e) => panic!("a blocking step attempt: {e}"),
                }
            }
        })
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
        let retry = self.retry.map(|r| b::RetryPolicy {
            max_attempts: r.max_attempts,
            initial_ms: millis(r.initial),
            max_ms: millis(r.max),
            multiplier: r.multiplier,
        });
        let timeout = millis(self.timeout.unwrap_or(self.ctx.shared.step_timeout()));
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
        let future = CallFuture::new(self.ctx.shared.clone(), call, Some(spec));
        Box::pin(async move { decode(value(future.await?)?) })
    }
}
