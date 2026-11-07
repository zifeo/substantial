//! Creating and opening a namespace (I-16), starting runs (I-17), and the workers serving it.

mod open;
mod run;

use std::future::IntoFuture;
use std::sync::{Arc, Mutex, Weak};

use futures::future::BoxFuture;
use serde::Serialize;
use tokio::sync::watch;

pub(crate) use open::Source;
#[cfg(feature = "testing")]
pub(crate) use open::attached;
pub use open::{Create, Open};
pub use run::{ResultWait, Run, RunStatus, SendMessage};

use crate::core::Core;
use crate::error::{Error, Result};
use crate::names::check_key;
use crate::plan;
use crate::proto::boundary::v1::{self as b, plan_input::Plan, plan_step::Step, started};
use crate::proto::layout::v1::Time;
use crate::settings::{self, Preset, Settings};
use crate::store::Store;
use crate::worker::{OnDeath, Wakes, Worker};
use crate::workflow::{AsWorkflow, IntoWorkflows, Workflow, input_object, json};

/// An opened namespace: the store, its partitions, and the workers of this process serving it.
#[derive(Clone)]
pub struct Substantial(Arc<Inner>);

struct Inner {
    core: Core,
    store: Arc<dyn Store>,
    partitions: u32,
    workers: Mutex<Vec<Weak<dyn Wakes>>>,
    /// The error of the last worker of this handle that died, which fails the waits in progress.
    died: Died,
    /// The test environment this handle belongs to, whose clock and ids it takes.
    #[cfg(feature = "testing")]
    env: Option<crate::testing::Attached>,
}

pub(crate) type Died = Arc<watch::Sender<Option<Arc<Error>>>>;

impl Substantial {
    /// Creates the namespace a URL names, or opens it if it exists with the same `partitions`,
    /// which are required.
    pub fn create(url: impl AsRef<str>) -> Create {
        Create::new(Source::Url(url.as_ref().to_owned()))
    }

    /// Opens the namespace a URL names; one that does not exist is a `LayoutError`.
    pub fn open(url: impl AsRef<str>) -> Open {
        Open::new(Source::Url(url.as_ref().to_owned()))
    }

    pub fn partitions(&self) -> u32 {
        self.0.partitions
    }

    /// A worker serving `workflows` (one, a tuple, or a `Vec`) on every partition; its preset is
    /// required.
    pub fn worker(&self, workflows: impl IntoWorkflows) -> WorkerBuilder {
        WorkerBuilder {
            sub: self.clone(),
            workflows: workflows.into_workflows(),
            preset: None,
            overrides: Vec::new(),
        }
    }

    /// Starts a run of `workflow`, idempotently under `.id(..)`, a ULID when none is given.
    pub fn start<W: AsWorkflow>(&self, workflow: W, input: impl Serialize) -> StartRun<W::Output> {
        StartRun {
            sub: self.clone(),
            workflow: workflow.workflow(),
            input: input_object(input),
            id: None,
        }
    }

    /// A handle on an existing chain of `workflow`.
    pub fn run<W: AsWorkflow>(&self, _workflow: W, chain: &str) -> Run<W::Output> {
        Run::new(self.clone(), chain.to_owned())
    }

    fn now(&self) -> Time {
        #[cfg(feature = "testing")]
        if let Some(env) = &self.0.env {
            return env.now();
        }
        crate::time::now()
    }

    fn ulid(&self, ms: i64) -> String {
        #[cfg(feature = "testing")]
        if let Some(env) = &self.0.env {
            return env.ulid(ms);
        }
        plan::ulid(ms)
    }

    #[cfg(feature = "testing")]
    pub(crate) fn env(&self) -> Option<&crate::testing::Attached> {
        self.0.env.as_ref()
    }

    async fn plan(&self, plan: Plan) -> Result<Step> {
        plan::run(&self.0.core, &*self.0.store, plan)
            .await
            .map_err(plan::caller_invalid)
    }

    /// Wakes the workers of this process, which then list at once instead of after `max_idle`.
    fn wake_workers(&self) {
        let workers = self.0.workers.lock().expect("never poisoned");
        for worker in workers.iter().filter_map(Weak::upgrade) {
            worker.wake();
        }
    }
}

fn datetime(t: &Time) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(t.ms).expect("a time in chrono's range")
}

/// A start of a run, awaited to start it.
#[must_use = "a start does nothing until awaited"]
pub struct StartRun<O> {
    sub: Substantial,
    workflow: Workflow<O>,
    input: Result<Vec<u8>>,
    id: Option<String>,
}

impl<O> StartRun<O> {
    /// Starts the run under this id: a start with the same workflow, version and input returns
    /// the same run, other ones are `RunExists`.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_owned());
        self
    }
}

impl<O: 'static> IntoFuture for StartRun<O> {
    type Output = Result<Run<O>>;
    type IntoFuture = BoxFuture<'static, Result<Run<O>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let input = self.input?;
            check_key("workflow name", self.workflow.name())?;
            if let Some(id) = &self.id {
                check_key("run id", id)?;
            }
            let now = self.sub.now();
            let id = self.id.unwrap_or_else(|| self.sub.ulid(now.ms));
            let start = b::StartPlan {
                prefix: String::new(),
                partitions: self.sub.0.partitions,
                workflow: self.workflow.name().to_owned(),
                version: self.workflow.version(),
                input: Some(json(input)?),
                id: id.clone(),
                now: Some(now),
            };
            match self.sub.plan(Plan::Start(start)).await? {
                Step::Started(b::Started {
                    started: Some(outcome),
                }) => match outcome {
                    started::Started::Created(_) | started::Started::Existing(_) => {}
                    started::Started::RunExists(taken) => {
                        let until = taken.collected_until.map(|t| datetime(&t));
                        return Err(Error::RunExists {
                            id,
                            collected: until.is_some(),
                            until,
                        });
                    }
                    started::Started::Failed(f) => return Err(Error::StoreError(f.message)),
                },
                other => return Err(plan::unexpected("start", other)),
            }
            self.sub.wake_workers();
            Ok(Run::new(self.sub, id))
        })
    }
}

/// A worker to start: its preset is required, and `set` overrides one of its settings.
#[must_use = "a worker does nothing until started or served"]
pub struct WorkerBuilder {
    sub: Substantial,
    workflows: Vec<Workflow<serde_json::Value>>,
    preset: Option<Preset>,
    overrides: Vec<Override>,
}

struct Override {
    name: String,
    value: String,
    /// Set by the tests, through `__internal::set`: any setting, not only the public ones.
    internal: bool,
}

impl WorkerBuilder {
    pub fn preset(mut self, preset: Preset) -> Self {
        self.preset = Some(preset);
        self
    }

    /// Overrides the setting `name` of the preset: `concurrency`, `max_idle`, `shutdown_grace`,
    /// `claim_ttl`, `continue_as_new_after`, `step_timeout` or `retention`. Durations take a unit
    /// (`100ms`, `5s`, `10min`, `1h`, `7d`); `continue_as_new_after` is `segments,size`
    /// (`1000,8MiB`), or `0` to continue at every boundary; `retention` a duration of at least
    /// `1h`, or `forever` (`s::FOREVER`). Any other name, or a value that does not parse, is a `ConfigError` when
    /// the worker starts.
    pub fn set(self, name: &str, value: impl ToString) -> Self {
        self.with(name, value, false)
    }

    pub(crate) fn with(mut self, name: &str, value: impl ToString, internal: bool) -> Self {
        self.overrides.push(Override {
            name: name.to_owned(),
            value: value.to_string(),
            internal,
        });
        self
    }

    fn settings(&self) -> Result<Settings> {
        let preset = self.preset.ok_or_else(|| {
            Error::ConfigError("a worker needs a preset, Script or Service".into())
        })?;
        let unknown: Vec<&str> = self
            .overrides
            .iter()
            .filter(|o| !o.internal && !settings::PUBLIC.contains(&o.name.as_str()))
            .map(|o| o.name.as_str())
            .collect();
        if !unknown.is_empty() {
            return Err(settings::unknown(&unknown));
        }
        let mut settings = Settings::preset(preset, self.sub.0.store.capabilities().wait);
        for o in &self.overrides {
            settings.apply(&o.name, &o.value)?;
        }
        settings.validate()?;
        Ok(settings)
    }

    /// Starts the worker on the current tokio runtime; `shutdown` stops it.
    pub fn start(self) -> Result<Worker> {
        let settings = self.settings()?;
        let (sub, workflows) = (self.sub, self.workflows);
        let inner = &sub.0;
        #[cfg(feature = "testing")]
        let worker = match &inner.env {
            Some(env) => env.worker(workflows, settings)?,
            None => real(&sub, workflows, settings)?,
        };
        #[cfg(not(feature = "testing"))]
        let worker = real(&sub, workflows, settings)?;
        let mut workers = inner.workers.lock().expect("never poisoned");
        workers.retain(|w| w.strong_count() > 0);
        workers.push(Arc::downgrade(&worker.wakes()));
        Ok(worker)
    }

    /// Runs the worker until SIGTERM or Ctrl-C, then shuts it down.
    pub async fn serve(self) -> Result<()> {
        let signals = crate::worker::Signals::listen()?;
        self.start()?.serve(signals).await
    }
}

fn real(
    sub: &Substantial,
    workflows: Vec<Workflow<serde_json::Value>>,
    settings: Settings,
) -> Result<Worker> {
    let inner = &sub.0;
    let died = inner.died.clone();
    let died: OnDeath = Arc::new(move |e: &Error| {
        died.send_replace(Some(Arc::new(e.again())));
    });
    Worker::start(
        inner.store.clone(),
        workflows,
        inner.partitions,
        settings,
        died,
    )
}
