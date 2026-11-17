//! `Context::poll`: a step tried again after a sleep until it returns a value or its timeout
//! passes.

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Retry;
use super::context::Context;
use crate::error::{Error, Result};

type Try<T> = Arc<dyn Fn(&Context) -> super::Step<Option<T>> + Send + Sync>;

impl Context {
    /// Runs `f` as the step `name` until it returns `Some`, sleeping `every` between two tries;
    /// `None` once `timeout`, counted from the first call, has passed. Each try is a step of its
    /// own, recorded: `Ok(None)` is "not ready", so a value that serializes to `null` never is.
    /// `every` and `timeout` are required.
    pub fn poll<T, E, F, Fut>(&self, name: &str, f: F) -> Poll<T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Option<T>, E>> + Send + 'static,
    {
        let (name, f) = (name.to_owned(), Arc::new(f));
        Poll {
            ctx: self.clone(),
            attempt: Arc::new(move |c: &Context| {
                let f = f.clone();
                c.step(&name, move || f())
            }),
            every: None,
            timeout: None,
            retry: None,
        }
    }
}

/// A poll, awaited to run it; `every` and `timeout` are required, `retry` is each try's.
#[must_use = "a poll does nothing until awaited"]
pub struct Poll<T> {
    ctx: Context,
    attempt: Try<T>,
    every: Option<Duration>,
    timeout: Option<Duration>,
    retry: Option<Retry>,
}

impl<T> Poll<T> {
    /// The sleep between two tries, positive.
    pub fn every(mut self, every: Duration) -> Self {
        self.every = Some(every);
        self
    }

    /// Counted from the first call: the poll returns `None` once it passed.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The retry of each try, as a step's.
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(retry);
        self
    }
}

impl<T: DeserializeOwned + Send + 'static> IntoFuture for Poll<T> {
    type Output = Result<Option<T>>;
    type IntoFuture = BoxFuture<'static, Result<Option<T>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let every = self.every.filter(|e| !e.is_zero()).ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "every is a positive duration, not {}",
                    shown(self.every)
                ))
            })?;
            let timeout = self.timeout.ok_or_else(|| {
                Error::InvalidArgument("timeout is a duration, not missing".into())
            })?;
            let c = self.ctx;
            let deadline = c.now().await? + timeout;
            loop {
                let mut attempt = (self.attempt)(&c);
                if let Some(retry) = self.retry {
                    attempt = attempt.retry(retry);
                }
                if let Some(value) = attempt.await? {
                    return Ok(Some(value));
                }
                let left = deadline - c.now().await?;
                // A negative left is not a std duration: the timeout passed.
                let Ok(left) = left.to_std() else {
                    return Ok(None);
                };
                if left.is_zero() {
                    return Ok(None);
                }
                c.sleep(every.min(left)).await?;
            }
        })
    }
}

fn shown(d: Option<Duration>) -> String {
    d.map_or_else(|| "missing".into(), |d| format!("{d:?}"))
}
