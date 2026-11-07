//! Workflows and the context their code calls (I-5 to I-14).

mod as_workflow;
mod child;
mod context;
pub(crate) mod executor;
mod iterate;
mod log;
mod messages;
mod saga;
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
pub use child::{Child, SendFromWorkflow, StartChild};
pub use context::{Context, WaitUntil};
pub use log::Log;
pub use messages::Handlers;
pub(crate) use step::with_step_key;
pub use step::{Step, step_key};
pub use var::Var;

use crate::error::{Error, Failure, Result};
use crate::proto::layout::v1::{self as layout, value};

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
    run: Arc<Run>,
    output: PhantomData<fn() -> O>,
}

impl<O> Clone for Workflow<O> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            version: self.version,
            step_timeout: self.step_timeout,
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
                Ok(serde_json::to_vec(&f(c, input).await?)?)
            })
        };
        Self {
            name: name.to_owned(),
            version: None,
            step_timeout: None,
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> Option<u32> {
        self.version
    }

    pub fn step_timeout(&self) -> Option<Duration> {
        self.step_timeout
    }

    /// The same workflow, its result read as JSON: workflows of different results in one `Vec`.
    pub fn untyped(&self) -> Untyped {
        Workflow {
            name: self.name.clone(),
            version: self.version,
            step_timeout: self.step_timeout,
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
        Ok(Self(Arc::new(map)))
    }

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
pub(crate) const MAX_VALUE: usize = 64 * 1024 * 1024;

/// A value encoded by the host, refused above `MAX_VALUE`.
pub(crate) fn json(bytes: Vec<u8>) -> Result<layout::Value> {
    if bytes.len() > MAX_VALUE {
        return Err(Error::ValueTooLarge {
            size: bytes.len(),
            limit: MAX_VALUE,
        });
    }
    Ok(layout::Value {
        kind: Some(value::Kind::Json(bytes)),
    })
}

/// An input as JSON: an object, or `{}` for `()` and `None`.
pub(crate) fn input_object(input: impl Serialize) -> Result<Vec<u8>> {
    match serde_json::to_value(input)? {
        serde_json::Value::Null => Ok(b"{}".to_vec()),
        object @ serde_json::Value::Object(_) => Ok(serde_json::to_vec(&object)?),
        other => Err(Error::InvalidArgument(format!(
            "an input is a JSON object, not {other}"
        ))),
    }
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
        Error::Failed(f) | Error::StepFailed { error: f, .. } => f.clone(),
        Error::RolledBack(reason) => Failure {
            kind: "RolledBack".into(),
            message: reason.clone(),
            retryable: false,
        },
        other => Failure {
            kind: error_kind(other).into(),
            message: other.to_string(),
            retryable: false,
        },
    }
}

fn error_kind(e: &Error) -> &'static str {
    match e {
        Error::Failed(_) => "Failed",
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
        Error::WaitTimeout => "WaitTimeout",
        Error::ValueTooLarge { .. } => "ValueTooLarge",
        Error::NotInWorkflow => "NotInWorkflow",
        Error::Json(_) => "Json",
        Error::Core { .. }
        | Error::Trap(_)
        | Error::Module(_)
        | Error::Protocol(_)
        | Error::Io(_) => "HostError",
    }
}
