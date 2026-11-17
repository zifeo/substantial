//! Creating and opening a namespace (I-16), starting runs (I-17), and the workers serving it.

mod open;
mod run;
mod runs;

use std::future::IntoFuture;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex, Weak};

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use serde::Serialize;
use tokio::sync::watch;

pub(crate) use open::Source;
#[cfg(feature = "testing")]
pub(crate) use open::attached;
pub use open::{Create, Open};
pub use run::{HistoryEvent, ResultWait, RetryRun, Run, RunStatus, SendMessage};
pub use runs::{RunInfo, WaitingOn};

use crate::clock::Clock;
use crate::core::Core;
use crate::cost::Budget;
use crate::error::{Error, Result};
use crate::names::check_key;
use crate::observe::Metering;
use crate::plan;
use crate::proto::boundary::v1::{self as b, plan_input::Plan, plan_step::Step, started};
use crate::proto::layout::v1::Time;
use crate::settings::{self, Preset, Settings};
use crate::store::Store;
use crate::worker::{OnDeath, Wakes, Worker};
use crate::workflow::schedule::{self, Cron};
use crate::workflow::{AsWorkflow, IntoWorkflows, Workflow, input_object, json};

/// An opened namespace: the store, and the workers of this process serving it.
#[derive(Clone)]
pub struct Substantial(Arc<Inner>);

struct Inner {
    core: Core,
    store: Arc<dyn Store>,
    workers: Mutex<Vec<Weak<dyn Wakes>>>,
    /// The error of the last worker of this handle that died, which fails the waits in progress.
    died: Died,
    /// Its prices and observer, which its workers take.
    metering: Metering,
    /// The test environment this handle belongs to, whose clock and ids it takes.
    #[cfg(feature = "testing")]
    env: Option<crate::testing::Attached>,
}

pub(crate) type Died = Arc<watch::Sender<Option<Arc<Error>>>>;

impl Substantial {
    /// Creates the namespace a URL names, or opens it if it exists.
    pub fn create(url: impl AsRef<str>) -> Create {
        Create::new(Source::Url(url.as_ref().to_owned()))
    }

    /// Opens the namespace a URL names; one that does not exist is a `LayoutError`.
    pub fn open(url: impl AsRef<str>) -> Open {
        Open::new(Source::Url(url.as_ref().to_owned()))
    }

    /// A worker serving `workflows` (one, a tuple, or a `Vec`); its preset is required.
    pub fn worker(&self, workflows: impl IntoWorkflows) -> WorkerBuilder {
        WorkerBuilder {
            sub: self.clone(),
            workflows: workflows.into_workflows(),
            preset: None,
            overrides: Vec::new(),
            shift: None,
        }
    }

    /// Starts a run of `workflow`, idempotently under `.id(..)`, a ULID when none is given.
    pub fn start<W: AsWorkflow>(&self, workflow: W, input: impl Serialize) -> StartRun<W::Output> {
        let workflow = workflow.workflow();
        StartRun {
            sub: self.clone(),
            budget: workflow.budget().cloned(),
            workflow,
            input: input_object(input),
            id: None,
        }
    }

    /// The live runs of `workflow`, all its versions, each once: pending, waiting on a timer or
    /// a retry backoff, overdue, or idle until a message comes. Listed a page at a time as the
    /// stream is polled; dropping the stream stops the listing.
    pub fn runs<W: AsWorkflow>(&self, workflow: W) -> BoxStream<'static, Result<RunInfo>> {
        let listing = runs::Listing::new(self.clone(), workflow.workflow().name().to_owned());
        stream::try_unfold(listing, runs::Listing::next).boxed()
    }

    /// A handle on an existing chain of `workflow`.
    pub fn run<W: AsWorkflow>(&self, _workflow: W, chain: &str) -> Run<W::Output> {
        Run::new(self.clone(), chain.to_owned())
    }

    /// Configures the schedule of the run `id` of `workflow`, whose code loops on
    /// `Context::schedule`: starts it if needed and sends it this config, applied only if
    /// `revision` is higher than its own. `None` disables it. `.backfill(n)` is how many fires
    /// missed while no worker ran fire when one does, the most recent ones, 5 unless given. A
    /// revision already applied with the same config does nothing; a higher one applied, or the
    /// same with another config, is a `ConfigError`.
    pub fn schedule<W: AsWorkflow>(
        &self,
        workflow: W,
        id: &str,
        cron: Option<Cron>,
        revision: u64,
    ) -> ScheduleRun<W::Output> {
        ScheduleRun {
            sub: self.clone(),
            workflow: workflow.workflow(),
            id: id.to_owned(),
            cron,
            revision,
            backfill: 5,
        }
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

    /// In a test environment, `InvalidArgument` for a call from another tokio runtime than its own.
    fn check_runtime(&self) -> Result<()> {
        #[cfg(feature = "testing")]
        if let Some(env) = &self.0.env {
            return env.check_runtime();
        }
        Ok(())
    }

    async fn plan(&self, plan: Plan) -> Result<Step> {
        self.check_runtime()?;
        plan::run(&self.0.core, &*self.0.store, plan)
            .await
            .map_err(plan::caller_invalid)
    }

    /// Wakes the workers of this process serving `workflow`, which then list its due entries at
    /// once instead of after `max_idle`.
    fn wake_workers(&self, workflow: &str) {
        let workers = self.0.workers.lock().expect("never poisoned");
        for worker in workers.iter().filter_map(Weak::upgrade) {
            worker.wake_workflow(workflow);
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
    budget: Option<Budget>,
}

impl<O> StartRun<O> {
    /// Starts the run under this id: a start with the same workflow, version, input and budget
    /// returns the same run, other ones are `RunExists`.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_owned());
        self
    }

    /// The run's budget, instead of its workflow's.
    pub fn budget(mut self, budget: impl Into<Budget>) -> Self {
        self.budget = Some(budget.into());
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
            let budget = self.budget.as_ref().map(Budget::to_proto).transpose()?;
            let now = self.sub.now();
            let id = self.id.unwrap_or_else(|| self.sub.ulid(now.ms));
            let start = b::StartPlan {
                prefix: String::new(),
                workflow: self.workflow.name().to_owned(),
                version: self.workflow.version(),
                input: Some(json(input)?),
                id: id.clone(),
                now: Some(now),
                budget,
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
            self.sub.wake_workers(self.workflow.name());
            Ok(Run::new(self.sub, id))
        })
    }
}

/// A schedule's config, awaited to apply it; returns the handle on its run.
#[must_use = "a schedule is not configured until awaited"]
pub struct ScheduleRun<O> {
    sub: Substantial,
    workflow: Workflow<O>,
    id: String,
    cron: Option<Cron>,
    revision: u64,
    backfill: u64,
}

impl<O> ScheduleRun<O> {
    /// How many fires missed while no worker ran fire when one does, the most recent ones.
    pub fn backfill(mut self, backfill: u64) -> Self {
        self.backfill = backfill;
        self
    }
}

impl<O: 'static> IntoFuture for ScheduleRun<O> {
    type Output = Result<Run<O>>;
    type IntoFuture = BoxFuture<'static, Result<Run<O>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (id, revision) = (&self.id, self.revision);
            schedule::check_config(revision, self.backfill)?;
            let wanted = schedule::config(revision, self.cron.as_ref(), self.backfill);
            let run = self.sub.run(&self.workflow, id);
            let published: Option<serde_json::Value> = run.read(schedule::MESSAGE).await?;
            if let Some(published) = published {
                let applied: serde_json::Map<_, _> = ["revision", "cron", "tz", "backfill"]
                    .into_iter()
                    .map(|k| (k.to_owned(), published.get(k).cloned().unwrap_or_default()))
                    .collect();
                let applied = serde_json::Value::Object(applied);
                let was = applied["revision"].as_u64().ok_or_else(|| {
                    Error::Protocol(format!("schedule {id}: a published revision {applied}"))
                })?;
                if was > revision {
                    return Err(Error::ConfigError(format!(
                        "schedule {id}: revision {was} is applied already, higher than {revision}"
                    )));
                }
                if was == revision {
                    if applied == wanted {
                        return Ok(run);
                    }
                    return Err(Error::ConfigError(format!(
                        "schedule {id}: revision {revision} is applied with another config, \
                         {applied}, not {wanted}"
                    )));
                }
            }
            let run = self.sub.start(&self.workflow, ()).id(id).await?;
            run.send(schedule::MESSAGE, wanted)
                .id(&format!("schedule-{revision}"))
                .await?;
            Ok(run)
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
    /// Set by the tests, through `__internal::shifted`: how far ahead of the real one its clock is.
    pub(crate) shift: Option<Arc<AtomicI64>>,
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
    /// `1h`. Any other name, or a value that does not parse, is a `ConfigError` when the worker
    /// starts.
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
        let (sub, workflows, shift) = (self.sub, self.workflows, self.shift);
        let inner = &sub.0;
        #[cfg(feature = "testing")]
        let worker = match &inner.env {
            Some(env) => env.worker(workflows, settings)?,
            None => real(&sub, workflows, settings, shift)?,
        };
        #[cfg(not(feature = "testing"))]
        let worker = real(&sub, workflows, settings, shift)?;
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

    /// Starts the worker and drains it, as `Worker::drain`.
    pub async fn drain(self, within: std::time::Duration) -> Result<()> {
        self.start()?.drain(within).await
    }
}

fn real(
    sub: &Substantial,
    workflows: Vec<Workflow<serde_json::Value>>,
    settings: Settings,
    shift: Option<Arc<AtomicI64>>,
) -> Result<Worker> {
    let inner = &sub.0;
    let died = inner.died.clone();
    let died: OnDeath = Arc::new(move |e: &Error| {
        died.send_replace(Some(Arc::new(e.again())));
    });
    let metering = inner.metering.clone();
    let clock = match shift {
        None => Clock::Real,
        Some(ms) => Clock::Shifted(ms),
    };
    Worker::start(
        inner.store.clone(),
        workflows,
        settings,
        died,
        metering,
        clock,
    )
}
