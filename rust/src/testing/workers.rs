//! The workers of an environment: the clock of each of their incarnations on the virtual time,
//! `crash`, `pause` and `resume`, and the crashes Chaos draws.

use std::collections::{BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::task::AbortHandle;

use super::hold::{Held, Holding};
use super::store::FaultStore;
use super::world::World;
use crate::clock::{Clock, Ready, Task};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::outcome::Outcome;
use crate::proto::layout::v1::Time;
use crate::settings::Settings;
use crate::time::millis;
use crate::worker::{Agent, Launch, Running, Wakes};
use crate::workflow::Workflows;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("never panics while locked")
}

/// The clock of one incarnation of a worker: a crash ends it, a Chaos replacement gets a new one.
pub(crate) struct Incarnation {
    pub(super) world: Arc<World>,
    worker: Weak<EnvWorker>,
    pub(super) id: String,
    /// The worker's, shared by its incarnations.
    paused: Arc<AtomicBool>,
    dead: AtomicBool,
    tasks: Mutex<Vec<(u64, AbortHandle)>>,
    /// The agent loop's task and its inbox, for `World::skip_scans`.
    pub(super) agent: OnceLock<(u64, Arc<Agent>)>,
    /// How many times it tried each operation on each key, for Chaos.
    tries: Mutex<HashMap<(String, String), u32>>,
    /// Its last scan, for `World::skip_scans`.
    pub(super) scan: Mutex<Scan>,
}

/// The instant of a worker's last scan, the store's writes then, and the keys its listings drew
/// Chaos faults for.
#[derive(Default)]
pub(super) struct Scan {
    pub(super) at: Option<i64>,
    pub(super) seen: u64,
    pub(super) keys: BTreeSet<String>,
}

impl Incarnation {
    pub(super) fn paused(&self) -> bool {
        self.dead.load(Ordering::SeqCst) || self.paused.load(Ordering::SeqCst)
    }

    pub(super) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    pub(super) fn worker(&self) -> Option<Arc<EnvWorker>> {
        self.worker.upgrade()
    }

    pub(crate) fn now(&self) -> Time {
        Time {
            ms: self.world.time.now(),
        }
    }

    pub(crate) async fn wait(&self, signals: &[Arc<dyn Ready>], at: Option<i64>) {
        self.world.time.wait(signals, at).await;
    }

    pub(super) async fn passing(&self) {
        self.world.time.passing(self).await;
    }

    pub(crate) fn spawn(
        self: &Arc<Self>,
        name: String,
        future: BoxFuture<'static, ()>,
    ) -> AbortHandle {
        // Locked first, so that the tasks this one spawns on another thread come after it.
        let mut tasks = lock(&self.tasks);
        let (id, abort) = self.world.time.spawn(self, name, future);
        if self.is_dead() {
            abort.abort();
            self.world.time.forget(&[id]);
        } else {
            tasks.retain(|(_, t)| !t.is_finished());
            tasks.push((id, abort.clone()));
        }
        abort
    }

    pub(crate) fn replayed(&self, run: &str) {
        self.world.time.replayed(run);
    }

    pub(crate) fn failure_read(&self, chain: &str) {
        lock(&self.world.checked).insert(chain.to_owned());
    }

    /// Kills the incarnation: its tasks are aborted, and with them the step bodies they await,
    /// never its claims.
    pub(super) fn crash(&self) {
        let tasks = {
            let mut tasks = lock(&self.tasks);
            self.dead.store(true, Ordering::SeqCst);
            std::mem::take(&mut *tasks)
        };
        let ids: Vec<u64> = tasks.iter().map(|(id, _)| *id).collect();
        self.world.time.forget(&ids);
        tasks.iter().for_each(|(_, t)| t.abort());
    }

    /// How many tries of `kind` on `key` it made, plus `more`, which it counts as made.
    pub(super) fn tries(&self, kind: &str, key: &str, more: u32) -> u32 {
        let mut tries = lock(&self.tries);
        let n = tries.entry((kind.to_owned(), key.to_owned())).or_default();
        *n += more;
        *n
    }

    /// Records a listing of due entries, drawn as `key`, made when the store had seen `writes`
    /// writes.
    pub(super) fn listed(&self, key: &str, writes: u64) {
        let now = self.world.time.now();
        let mut scan = lock(&self.scan);
        if scan.at != Some(now) {
            *scan = Scan {
                at: Some(now),
                seen: writes,
                keys: BTreeSet::from([key.to_owned()]),
            };
        } else {
            scan.seen = scan.seen.min(writes);
            scan.keys.insert(key.to_owned());
        }
    }

    /// One attempt: its body runs as a task of its own, aborted once abandoned, by its timeout or
    /// its worker's crash or shutdown, as production drops it, unless it runs a thread, which
    /// `settle` waits for. Chaos may crash the worker once it returned, then a hold may keep its
    /// outcome until released.
    pub(crate) async fn attempt(
        self: &Arc<Self>,
        name: &str,
        key: &str,
        body: BoxFuture<'static, Outcome>,
        timeout: Option<Duration>,
    ) -> Option<Outcome> {
        let world = &self.world;
        let deadline = timeout.map(|t| world.time.now() + millis(t) as i64);
        let (body, threaded) = world
            .time
            .body(format!("step {key}"), AssertUnwindSafe(body).catch_unwind());
        let _abandoned = Abandoned(&body, threaded);
        self.wait(&[body.done()], deadline).await;
        if !body.is_done() {
            return None;
        }
        // On another thread, a task of a crashed incarnation may run on to its next await.
        if self.is_dead() {
            return std::future::pending().await;
        }
        if world.draw("crash", key, self).is_some() {
            world.faulted(|f| f.crashed += 1);
            if let Some(worker) = self.worker() {
                worker.replace(self);
            }
            // Aborted with this incarnation at this await.
            return std::future::pending().await;
        }
        if let Some(hold) = world.hold_of(name) {
            let run = key.split('/').next().unwrap_or_default().to_owned();
            let held = Arc::new(Held {
                inc: self.clone(),
                released: Arc::default(),
                run,
            });
            let _holding = Holding(hold.clone(), held.clone());
            lock(&hold.held).push(held.clone());
            let released: Arc<dyn Ready> = held.released.clone();
            self.wait(std::slice::from_ref(&released), deadline).await;
            if !held.released.is_set() {
                return None;
            }
        }
        match body.take().expect("a body that returned has its value") {
            Ok(outcome) => Some(outcome),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

/// Interrupts a body left before it returned, unless it runs a thread.
struct Abandoned<'a, T: Send + 'static>(&'a Task<T>, Arc<AtomicBool>);

impl<T: Send + 'static> Drop for Abandoned<'_, T> {
    fn drop(&mut self) {
        if !self.0.is_done() && !self.1.load(Ordering::SeqCst) {
            self.0.abort();
        }
    }
}

/// The current incarnation of a worker and its agent loop.
pub(super) struct Current {
    pub(super) inc: Arc<Incarnation>,
    running: Running,
}

/// A worker of an environment, which Chaos may replace by a fresh one of the same workflows and
/// settings.
pub(crate) struct EnvWorker {
    world: Arc<World>,
    place: usize,
    replaced: AtomicU32,
    paused: Arc<AtomicBool>,
    crashed: AtomicBool,
    workflows: Workflows,
    pub(super) settings: Settings,
    partitions: u32,
    current: Mutex<Option<Arc<Current>>>,
}

impl EnvWorker {
    pub(super) fn start(
        world: &Arc<World>,
        workflows: Workflows,
        settings: Settings,
    ) -> Result<Arc<Self>> {
        let mut workers = lock(&world.workers);
        let worker = Arc::new(Self {
            world: world.clone(),
            place: workers.len(),
            replaced: AtomicU32::new(0),
            paused: Arc::default(),
            crashed: AtomicBool::new(false),
            workflows,
            settings,
            partitions: world.partitions,
            current: Mutex::new(None),
        });
        *lock(&worker.current) = Some(worker.launch()?);
        workers.push(worker.clone());
        Ok(worker)
    }

    fn launch(self: &Arc<Self>) -> Result<Arc<Current>> {
        let world = &self.world;
        let id = world.worker_id(self.place, self.replaced.load(Ordering::SeqCst));
        let inc = Arc::new(Incarnation {
            world: world.clone(),
            worker: Arc::downgrade(self),
            id: id.clone(),
            paused: self.paused.clone(),
            dead: AtomicBool::new(false),
            tasks: Mutex::default(),
            agent: OnceLock::new(),
            tries: Mutex::default(),
            scan: Mutex::default(),
        });
        let died = world.clone();
        let running = Launch {
            store: Arc::new(FaultStore::new(world.clone(), Some(inc.clone()))),
            workflows: &self.workflows,
            partitions: self.partitions,
            settings: &self.settings,
            died: Arc::new(move |e: &Error| died.died(e)),
            clock: Clock::Virtual(inc.clone()),
            id,
            seed: world.core_seed.to_vec(),
        }
        .start()?;
        let agent = lock(&inc.tasks)
            .first()
            .map(|(id, _)| *id)
            .expect("the agent loop is its first task");
        let _ = inc.agent.set((agent, running.agent.clone()));
        Ok(Arc::new(Current { inc, running }))
    }

    pub(super) fn current(&self) -> Arc<Current> {
        lock(&self.current)
            .clone()
            .expect("a worker starts on creation")
    }

    pub(super) fn place(&self) -> (usize, u32, String) {
        let id = self.current().inc.id.clone();
        (self.place, self.replaced.load(Ordering::SeqCst), id)
    }

    /// A fresh worker of the same workflows and settings in place of `inc`, crashed, unless it
    /// was replaced already.
    fn replace(self: &Arc<Self>, inc: &Arc<Incarnation>) {
        let mut current = lock(&self.current);
        if !current.as_ref().is_some_and(|c| Arc::ptr_eq(&c.inc, inc)) {
            return;
        }
        self.replaced.fetch_add(1, Ordering::SeqCst);
        // Launched before the crash, so that a settle on another thread never sees neither.
        match self.launch() {
            Ok(launched) => *current = Some(launched),
            Err(e) => self.world.died(&e),
        }
        inc.crash();
    }

    pub(super) fn served(&self) -> impl Iterator<Item = &str> {
        self.workflows.keys().map(|(name, _)| name)
    }

    pub(super) fn serves(&self, workflow: &str, version: Option<u32>) -> bool {
        self.is_alive() && self.workflows.get(workflow, version).is_some()
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.crashed.load(Ordering::SeqCst) && self.current().running.is_alive()
    }

    pub(crate) fn error(&self) -> Option<Error> {
        self.current().running.error()
    }

    /// Kills the worker at once, as a SIGKILL would: its claims stay until they expire, and its
    /// steps in flight are abandoned, their bodies dropped at their await, those in a thread
    /// (`step_blocking`) left to run to their end.
    pub(crate) fn crash(&self) {
        self.crashed.store(true, Ordering::SeqCst);
        self.current().inc.crash();
    }

    pub(crate) fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub(crate) fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.world.time.kick();
    }

    /// Settles, then leaves as a SIGTERM does with no grace: steps that returned are recorded,
    /// held ones abandoned, and the runs it leaves have their claims deleted, for another worker.
    pub(crate) async fn shutdown(&self) -> Result<()> {
        if self.crashed.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.world.settle().await?;
        let current = self.current();
        if current.running.task.is_finished() {
            return current.running.ended();
        }
        current.running.agent.shutdown(Duration::ZERO);
        self.world.settle().await?;
        if !current.running.task.is_finished() {
            return Err(Error::Stuck(format!(
                "worker {} could not leave: it is paused",
                current.inc.id
            )));
        }
        current.running.ended()
    }
}

impl Wakes for EnvWorker {
    fn wake(&self) {
        self.current().running.agent.wake();
    }
}
