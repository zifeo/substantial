//! A `Substantial` attached to an environment, and what a run it waits on is stuck on.

use std::sync::{Arc, Mutex, MutexGuard};

use chrono::DateTime;
use prost::Message;

use super::store::FaultStore;
use super::workers::EnvWorker;
use super::world::{World, due_ms};
use crate::client::{RunStatus, Substantial, attached};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::plan_input::Plan;
use crate::proto::layout::v1::{self as layout, Time};
use crate::settings::Settings;
use crate::worker::{Kind, Worker};
use crate::workflow::{Untyped, Workflows};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("never panics while locked")
}

pub(super) fn decode<M: Message + Default>(world: &World, key: &str) -> Result<M> {
    let bytes = world
        .object(key)
        .ok_or_else(|| Error::Protocol(format!("no {key}")))?;
    M::decode(bytes).map_err(|e| Error::Protocol(format!("{key}: {e}")))
}

pub(super) fn start(world: &World, run: &str) -> Result<layout::Start> {
    decode(world, &format!("runs/{run}/start"))
}

/// A `Substantial` of an environment: its clock, its ids and its workers are the environment's.
#[derive(Clone)]
pub(crate) struct Attached {
    world: Arc<World>,
    /// Whether it is the environment itself, through which the test reads failures.
    env: bool,
}

impl Attached {
    pub(super) fn new(world: &Arc<World>, env: bool) -> Self {
        Self {
            world: world.clone(),
            env,
        }
    }

    pub(super) async fn open(self, plan: Plan) -> Result<Substantial> {
        let store = Arc::new(FaultStore::new(self.world.clone(), None));
        attached(store, plan, Some(self)).await
    }

    pub(crate) fn now(&self) -> Time {
        Time {
            ms: self.world.time.now(),
        }
    }

    pub(crate) fn ulid(&self, ms: i64) -> String {
        self.world.ulids.next(ms)
    }

    pub(crate) fn worker(&self, workflows: Vec<Untyped>, settings: Settings) -> Result<Worker> {
        settings.validate()?;
        let worker = EnvWorker::start(
            &self.world,
            Workflows::new(workflows, settings.retention_min)?,
            settings,
        )?;
        Ok(Worker(Kind::Env(worker)))
    }

    pub(crate) async fn settle(&self) -> Result<()> {
        self.world.settle().await
    }

    /// Marks the failure of `chain` read when the environment itself read it.
    pub(crate) fn read(&self, chain: &str, status: RunStatus) {
        if self.env && status == RunStatus::Failed {
            lock(&self.world.checked).insert(chain.to_owned());
        }
    }

    /// What `run` of `chain` waits on, for `Stuck`.
    pub(crate) fn stuck(&self, chain: &str, run: &str) -> Error {
        match waits_on(&self.world, run) {
            Ok(what) => Error::Stuck(format!("run {chain} has not ended: it waits on {what}")),
            Err(e) => e,
        }
    }
}

fn serves(world: &World, start: &layout::Start) -> bool {
    lock(&world.workers)
        .iter()
        .any(|w| w.serves(&start.workflow, start.version))
}

fn named(start: &layout::Start) -> String {
    match start.version {
        Some(v) => format!("{} version {v}", start.workflow),
        None => start.workflow.clone(),
    }
}

fn waits_on(world: &World, run: &str) -> Result<String> {
    let begun = start(world, run)?;
    let mut what = Vec::new();
    if !serves(world, &begun) {
        what.push(format!(
            "workflow {}, which no worker serves",
            named(&begun)
        ));
    }
    for hold in lock(&world.holds).iter() {
        let held = lock(&hold.held).iter().filter(|h| h.run == run).count();
        what.extend((0..held).map(|_| format!("step {}, held", hold.step)));
    }
    for key in world.keys(&format!("queues/{}/", begun.workflow)) {
        let entry = key.rsplit('/').next().unwrap_or_default();
        if entry.split('~').nth(1) == Some(run) {
            let at = DateTime::from_timestamp_millis(due_ms(entry)).expect("a time in range");
            what.push(format!("a timer due at {}", at.to_rfc3339()));
        }
    }
    if what.is_empty() {
        what.push("a message, or the end of a run it started".into());
    }
    let mut unserved = Vec::new();
    for other in pending(world).iter().filter(|other| *other != run) {
        let begun = start(world, other)?;
        if !serves(world, &begun) {
            unserved.push(format!("{other} ({})", named(&begun)));
        }
    }
    if !unserved.is_empty() {
        what.push(format!(
            "while runs of workflows no worker serves wait: {}",
            unserved.join(", ")
        ));
    }
    Ok(what.join("; "))
}

/// The runs started and not ended.
fn pending(world: &World) -> Vec<String> {
    let keys = world.keys("runs/");
    let ended = |run: &str| keys.contains(&format!("runs/{run}/result"));
    (keys.iter())
        .filter_map(|k| k.strip_prefix("runs/")?.strip_suffix("/start"))
        .filter(|run| !run.contains('/') && !ended(run))
        .map(str::to_owned)
        .collect()
}
