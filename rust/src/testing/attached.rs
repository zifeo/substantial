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

/// The head of a run, its chain's through the pointer of a later run.
pub(super) fn head(world: &World, id: &str) -> Result<layout::Head> {
    let found: layout::Run = decode(world, &format!("runs/{id}"))?;
    match found.kind {
        Some(layout::run::Kind::Head(head)) => Ok(head),
        Some(layout::run::Kind::Pointer(p)) => {
            match decode::<layout::Run>(world, &format!("runs/{}", p.chain))?.kind {
                Some(layout::run::Kind::Head(head)) => Ok(head),
                _ => Err(Error::Protocol(format!("runs/{}: no head", p.chain))),
            }
        }
        None => Err(Error::Protocol(format!(
            "runs/{id}: neither head nor pointer"
        ))),
    }
}

/// Whether a head's run ended: its last segment holds an end.
pub(super) fn ended(head: &layout::Head) -> bool {
    use layout::event::Kind;
    head.segments.last().is_some_and(|s| {
        s.events.iter().any(|e| {
            matches!(
                e.kind,
                Some(
                    Kind::Completed(_) | Kind::Failed(_) | Kind::Cancelled(_) | Kind::Continued(_)
                )
            )
        })
    })
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
        self.world.check_runtime()?;
        let store = Arc::new(FaultStore::new(self.world.clone(), None));
        let metering = self.world.metering.clone();
        // § one bsw9tpftr@bkw67
        attached(store, plan, metering, Some(self)).await
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
        self.world.check_runtime()?;
        settings.validate()?;
        let worker = EnvWorker::start(&self.world, Workflows::new(workflows)?, settings)?;
        Ok(Worker(Kind::Env(worker)))
    }

    pub(crate) async fn settle(&self) -> Result<()> {
        self.world.settle().await
    }

    pub(crate) fn check_runtime(&self) -> Result<()> {
        self.world.check_runtime()
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
    let current = head(world, run)?;
    let begun = current.start.clone().unwrap_or_default();
    let mut what = Vec::new();
    if !serves(world, &begun) {
        what.push(format!(
            "workflow {}, which no worker serves",
            named(&begun)
        ));
    }
    if let Some(block) = current.blocked {
        let error = block.error.unwrap_or_default();
        what.push(format!(
            "a fix of its code, blocked by {}: {}",
            error.r#type, error.message
        ));
    }
    for hold in lock(&world.holds).iter() {
        let held = lock(&hold.held).iter().filter(|h| h.run == run).count();
        what.extend((0..held).map(|_| format!("step {}, held", hold.step)));
    }
    for key in world.keys(&format!("queues/{}/due/", begun.workflow)) {
        let entry = key.rsplit('/').next().unwrap_or_default();
        let mut parts = entry.split('~').skip(1);
        let (chain, tag) = (parts.next(), parts.next().unwrap_or_default());
        // A message's entry, at the epoch, is no timer.
        if chain == Some(begun.chain.as_str()) && tag.starts_with('s') {
            let at = DateTime::from_timestamp_millis(due_ms(entry)).expect("a time in range");
            what.push(format!("a timer due at {}", at.to_rfc3339()));
        }
    }
    if what.is_empty() {
        what.push("a message, or the end of a run it started".into());
    }
    let mut unserved = Vec::new();
    for (other, begun) in pending(world)?.iter().filter(|(other, _)| other != run) {
        if !serves(world, begun) {
            unserved.push(format!("{other} ({})", named(begun)));
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

/// The current runs of the chains not ended, with their start.
fn pending(world: &World) -> Result<Vec<(String, layout::Start)>> {
    let mut found = Vec::new();
    for key in world.keys("runs/") {
        let run: layout::Run = decode(world, &key)?;
        if let Some(layout::run::Kind::Head(head)) = run.kind {
            let start = head.start.clone().unwrap_or_default();
            if key == format!("runs/{}", start.chain) && !ended(&head) {
                found.push((head.run, start));
            }
        }
    }
    Ok(found)
}
