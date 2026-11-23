//! Workflows and the context their code calls (I-5 to I-14).

mod as_workflow;
mod canonical;
mod child;
mod context;
pub(crate) mod executor;
mod iterate;
mod log;
mod messages;
mod poll;
mod saga;
pub(crate) mod schedule;
pub(crate) mod scheduler;
mod step;
mod var;

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub use as_workflow::{AsWorkflow, IntoWorkflows};
pub(crate) use canonical::{argument_json, input_object, to_json};
pub use child::{ChainResult, Child, SendFromWorkflow, StartChild};
pub use context::{Context, Receive, WaitUntil};
pub use iterate::Iterate;
pub use log::Log;
pub use messages::Handlers;
pub use poll::Poll;
pub use schedule::{Cron, Schedule};
pub(crate) use step::{LiveAttempt, with_attempt};
pub use step::{Step, charge, heartbeat, step_key};
pub use var::Var;

use crate::cost::Budget;
use crate::error::{Error, Failure, Result};
use crate::proto::layout::v1::{self as layout, value};
use crate::settings::check_retention;

type Run = dyn Fn(Context, Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>>> + Send + Sync;

/// A workflow whose result reads as JSON, the form a worker serves.
pub(crate) type Untyped = Workflow<serde_json::Value>;

tokio::task_local! {
    /// The context of the code being polled, for the steps declared with `#[step]`.
    static CURRENT: Context;
}

/// The context of the workflow code being polled; `NotInWorkflow` outside one.
pub(crate) fn current() -> Result<Context> {
    CURRENT
        .try_with(Context::clone)
        .map_err(|_| Error::NotInWorkflow)
}

/// Polls `future` with `c` as the current context.
pub(crate) fn with_context<F: Future>(c: Context, future: F) -> impl Future<Output = F::Output> {
    CURRENT.scope(c, future)
}

/// A workflow: a name and an async function of a context and a JSON input, whose result is an `O`.
pub struct Workflow<O> {
    name: String,
    version: Option<u32>,
    step_timeout: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
    retention: Option<u32>,
    budget: Option<Budget>,
    run: Arc<Run>,
    output: PhantomData<fn() -> O>,
}

impl<O> Clone for Workflow<O> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            version: self.version,
            step_timeout: self.step_timeout,
            heartbeat_timeout: self.heartbeat_timeout,
            retention: self.retention,
            budget: self.budget.clone(),
            run: self.run.clone(),
            output: PhantomData,
        }
    }
}

impl<O: Serialize + Send + 'static> Workflow<O> {
    /// A workflow whose input deserializes into `I`.
    pub fn new<I, F, Fut>(name: &str, f: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        F: Fn(Context, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        let f = Arc::new(f);
        let run = move |c: Context, input: Vec<u8>| -> BoxFuture<'static, Result<Vec<u8>>> {
            let f = f.clone();
            Box::pin(async move {
                let input: I = serde_json::from_slice(&input)?;
                // § one dxgjekysz.a@7wyrn
                Ok(to_json(&f(c, input).await?)?)
            })
        };
        Self {
            name: name.to_owned(),
            version: None,
            step_timeout: None,
            heartbeat_timeout: None,
            retention: None,
            budget: None,
            run: Arc::new(run),
            output: PhantomData,
        }
    }

    /// A workflow that takes no input (it is given `{}`).
    pub fn without_input<F, Fut>(name: &str, f: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        Self::new(name, move |c, _: serde::de::IgnoredAny| f(c))
    }
}

impl<O> Workflow<O> {
    /// Serves only the runs started with this version, beside other versions of the same name.
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// The timeout of every attempt of its steps and undos that set none, instead of the
    /// worker's `step_timeout`.
    /// # Panics
    ///
    /// If `timeout` is zero, as a declaration error.
    pub fn with_step_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "workflow {:?}: step_timeout must be positive",
            self.name
        );
        self.step_timeout = Some(timeout);
        self
    }

    /// The longest an attempt of its steps and undos that set none may go without
    /// `s::heartbeat()`; none unless set.
    /// # Panics
    ///
    /// If `timeout` is zero, as a declaration error.
    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        assert!(
            !timeout.is_zero(),
            "workflow {:?}: heartbeat_timeout must be positive",
            self.name
        );
        self.heartbeat_timeout = Some(timeout);
        self
    }

    /// How many whole days its runs are kept after they end, instead of the worker's
    /// `retention`; under 1 or over 36,500 is a `ConfigError` when a worker starts with it.
    pub fn with_retention(mut self, days: u32) -> Self {
        self.retention = Some(days);
        self
    }

    /// The budget of its runs started without one of their own: `InvalidArgument` at the start
    /// for a malformed meter or limit.
    pub fn with_budget(mut self, budget: impl Into<Budget>) -> Self {
        self.budget = Some(budget.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> Option<u32> {
        self.version
    }

    pub fn step_timeout(&self) -> Option<Duration> {
        self.step_timeout
    }

    pub fn heartbeat_timeout(&self) -> Option<Duration> {
        self.heartbeat_timeout
    }

    pub fn retention(&self) -> Option<u32> {
        self.retention
    }

    pub fn budget(&self) -> Option<&Budget> {
        self.budget.as_ref()
    }

    /// The same workflow, its result read as JSON: workflows of different results in one `Vec`.
    pub fn untyped(&self) -> Untyped {
        Workflow {
            name: self.name.clone(),
            version: self.version,
            step_timeout: self.step_timeout,
            heartbeat_timeout: self.heartbeat_timeout,
            retention: self.retention,
            budget: self.budget.clone(),
            run: self.run.clone(),
            output: PhantomData,
        }
    }

    pub(crate) fn start(&self, c: Context, input: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>>> {
        Box::pin(with_context(c.clone(), (self.run)(c, input)))
    }
}

/// The workflows a worker serves, by name and version.
#[derive(Clone, Default)]
pub(crate) struct Workflows(Arc<HashMap<(String, Option<u32>), Untyped>>);

impl Workflows {
    pub(crate) fn new(workflows: Vec<Untyped>) -> Result<Self> {
        let mut map = HashMap::new();
        for w in workflows {
            let name = w.name.clone();
            crate::names::check_key("workflow name", &name)?;
            let version = w.version;
            if map.insert((name.clone(), version), w).is_some() {
                return Err(Error::ConfigError(format!(
                    "workflow {name:?} version {version:?} given twice"
                )));
            }
        }
        if map.is_empty() {
            return Err(Error::ConfigError(
                "a worker needs at least one workflow".into(),
            ));
        }
        // The core keeps one retention per name.
        let mut retentions: HashMap<&str, Option<u32>> = HashMap::new();
        for ((name, _), w) in &map {
            if let Some(r) = w.retention {
                check_retention(r, &format!("the retention of workflow {name:?}"))?;
            }
            // § one pnctq6tx8.a@nrpqy
            if *retentions.entry(name).or_insert(w.retention) != w.retention {
                return Err(Error::ConfigError(format!(
                    "the versions of workflow {name:?} give different retentions"
                )));
            }
        }
        Ok(Self(Arc::new(map)))
    }

    /// The retention the workflows of `name` give, the same for all its versions.
    pub(crate) fn retention(&self, name: &str) -> Option<u32> {
        self.0
            .iter()
            .find(|((n, _), _)| n == name)
            .and_then(|(_, w)| w.retention)
    }

    /// The retentions the workflows give, those that give one.
    pub(crate) fn retentions(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.values().filter_map(|w| w.retention)
    }

    // § one pjrbw31h4@srryr
    pub(crate) fn get(&self, name: &str, version: Option<u32>) -> Option<&Untyped> {
        self.0.get(&(name.to_owned(), version))
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = (&str, Option<u32>)> {
        self.0
            .keys()
            .map(|(name, version)| (name.as_str(), *version))
    }
}

/// Exponential backoff for a step: every field is required (I-6).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Retry {
    pub max_attempts: u32,
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    /// From the start of the step's first attempt, no attempt starts past it, and each runs at
    /// most until it; `None` for none.
    pub deadline: Option<Duration>,
}

impl Retry {
    /// The checks of the other hosts at declaration, made here at the call: the fields are public.
    pub(crate) fn check(&self) -> Result<()> {
        if self.max_attempts == 0 {
            return Err(Error::InvalidArgument(format!(
                "max_attempts must be a positive integer: {}",
                self.max_attempts
            )));
        }
        if self.max < self.initial {
            return Err(Error::InvalidArgument(format!(
                "invalid backoff from {:?} to {:?}",
                self.initial, self.max
            )));
        }
        if !self.multiplier.is_finite() || self.multiplier < 1.0 {
            return Err(Error::InvalidArgument(format!(
                "multiplier must be at least 1: {}",
                self.multiplier
            )));
        }
        if self.deadline.is_some_and(|d| d.is_zero()) {
            // § s0a5x1qed.a@g45bm
            return Err(Error::InvalidArgument(
                "a retry's deadline must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// An error that fails a step at once, attempts left or not.
#[derive(Debug)]
pub struct Permanent(pub Box<dyn std::error::Error + Send + Sync>);

impl Permanent {
    pub fn new(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(e.into())
    }
}

impl std::fmt::Display for Permanent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Permanent {}

/// The largest JSON encoding of a value the host hands to the core, above which it refuses it
/// with `ValueTooLarge`: much larger values trap the core.
// § one h8wg39cct@5xyr3
pub(crate) const MAX_VALUE: usize = 64 * 1024 * 1024;

/// A value encoded by the host, refused above `MAX_VALUE`.
pub(crate) fn json(bytes: Vec<u8>) -> Result<layout::Value> {
    // § one h8wg39cct@a9ba2
    if bytes.len() > MAX_VALUE {
        return Err(Error::ValueTooLarge {
            size: bytes.len(),
            limit: MAX_VALUE,
        });
    }
    // § one e1dc4dar4@0p3jv
    Ok(layout::Value {
        kind: Some(value::Kind::Json(bytes)),
    })
}

pub(crate) fn decode<T: DeserializeOwned>(v: Option<layout::Value>) -> Result<T> {
    match v.and_then(|v| v.kind) {
        Some(value::Kind::Json(bytes)) => Ok(serde_json::from_slice(&bytes)?),
        None => Ok(serde_json::from_slice(b"null")?),
        Some(other) => Err(Error::Protocol(format!(
            "values other than inline JSON are not supported yet: {other:?}"
        ))),
    }
}

/// The failure a workflow's error ends its run with.
pub(crate) fn failure_of(e: &Error) -> Failure {
    match e {
        Error::Failed(f) | Error::Fail(f) | Error::Code(f) | Error::StepFailed { error: f, .. } => {
            f.clone()
        }
        Error::RolledBack(reason) => Failure {
            kind: "RolledBack".into(),
            message: reason.clone(),
            retryable: false,
            details: None,
        },
        other => Failure {
            kind: error_kind(other).into(),
            message: other.to_string(),
            retryable: false,
            details: None,
        },
    }
}

fn error_kind(e: &Error) -> &'static str {
    match e {
        Error::Failed(_) => "Failed",
        Error::Fail(_) => "Fail",
        Error::Code(_) => "Error",
        Error::HistoryTooLarge(_) => "HistoryTooLarge",
        Error::StepFailed { .. } => "StepFailed",
        Error::Cancelled(_) => "Cancelled",
        Error::RolledBack(_) => "RolledBack",
        Error::UndoFailed { .. } => "UndoFailed",
        Error::ContinuedAsNew(_) => "ContinuedAsNew",
        Error::NonDeterminism(_) => "NonDeterminism",
        Error::StoreError(_) => "StoreError",
        Error::LayoutError(_) => "LayoutError",
        Error::ConfigError(_) => "ConfigError",
        Error::RunEnded { .. } => "RunEnded",
        Error::RunNotFound { .. } => "RunNotFound",
        Error::InvalidArgument(_) => "InvalidArgument",
        Error::RunExists { .. } => "RunExists",
        Error::RunFailed { .. } => "RunFailed",
        Error::BudgetExceeded { .. } => "BudgetExceeded",
        Error::WaitTimeout => "WaitTimeout",
        Error::Stuck(_) => "Stuck",
        Error::Unchecked(_) => "Unchecked",
        Error::ValueTooLarge { .. } => "ValueTooLarge",
        Error::NotInWorkflow => "NotInWorkflow",
        Error::NotCompressible(_) => "NotCompressible",
        Error::Json(_) => "Json",
        Error::Core { .. }
        | Error::Trap(_)
        | Error::Module(_)
        | Error::Protocol(_)
        | Error::Io(_) => "HostError",
    }
}
