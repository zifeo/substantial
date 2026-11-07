//! The virtual time of an environment: its instant, the deadlines its tasks wait on, the gate that
//! `settle` and `advance` open, and how they know that nothing more can happen.
//!
//! Every task of a worker (its loop, its store operations, its replays and their attempts) is
//! spawned here and waits only through `wait`, so the environment knows at each moment which ones
//! are parked and on what. Nothing can happen without the clock moving once every such task is
//! parked on signals not ready and a deadline not come, and no step body runs.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use super::park::{TASK, Tracked, Wait};
use super::workers::Incarnation;
use crate::clock::{Ready, Task};
use crate::error::{Error, Result};

tokio::task_local! {
    /// Set in the step bodies, which a settle there would wait on; `true` once the body runs a
    /// thread.
    static BODY: Arc<AtomicBool>;
}

/// Marks the step body running as one in a thread, which cannot be interrupted.
pub(crate) fn threaded() {
    let _ = BODY.try_with(|t| t.store(true, Ordering::SeqCst));
}

/// Refuses a settle inside a task of the environment or a step body it started.
fn outside() -> Result<()> {
    if TASK.try_with(|_| ()).is_ok() || BODY.try_with(|_| ()).is_ok() {
        return Err(Error::Stuck(
            "a step cannot wait for the environment to settle (result, settle or advance), \
             which waits for the step itself: start a child with c.start and await its result, \
             or pass the value in"
                .into(),
        ));
    }
    Ok(())
}

/// Replays of one run in one settle past which runs are taken to wake each other forever.
const REPLAY_LIMIT: u32 = 100;

pub(super) struct State {
    pub(super) ms: i64,
    pub(super) open: bool,
    next: u64,
    pub(super) tasks: BTreeMap<u64, Tracked>,
    /// Step bodies running, with the real time they started at.
    bodies: BTreeMap<u64, (String, Instant)>,
    replayed: HashMap<String, u32>,
    /// How long a step body may run before `settle` gives up on it.
    pub(super) step_limit: Duration,
}

impl State {
    fn quiet(&self) -> bool {
        self.bodies.is_empty() && self.tasks.values().all(|t| self.parked(t))
    }

    fn parked(&self, t: &Tracked) -> bool {
        t.waiter
            .as_ref()
            .is_some_and(|w| !w.ready(&t.owner, self.ms))
    }

    /// What keeps a settle from ending.
    fn active(&self) -> String {
        let bodies = self.bodies.values().map(|(name, _)| name.as_str());
        let tasks = self.tasks.values().filter(|t| !self.parked(t));
        let mut names: Vec<&str> = bodies.chain(tasks.map(|t| t.name.as_str())).collect();
        names.sort_unstable();
        names.join(", ")
    }

    fn kick(&self) {
        for w in self.tasks.values().filter_map(|t| t.waiter.as_ref()) {
            w.waker.wake_by_ref();
        }
    }

    fn check(&self) -> Result<()> {
        let late: Vec<&str> = (self.bodies.values())
            .filter(|(_, started)| started.elapsed() >= self.step_limit)
            .map(|(name, _)| name.as_str())
            .collect();
        if !late.is_empty() {
            return Err(Error::Stuck(format!(
                "a step body has not returned after {:?}: {}; still active: {}",
                self.step_limit,
                late.join(", "),
                self.active()
            )));
        }
        match self.replayed.iter().max_by_key(|(_, n)| **n) {
            Some((run, n)) if *n > REPLAY_LIMIT => Err(Error::Stuck(format!(
                "run {run} was replayed more than {REPLAY_LIMIT} times without the clock \
                 moving: runs keep waking each other"
            ))),
            _ => Ok(()),
        }
    }

    /// The real time left to the oldest step body running.
    fn budget(&self) -> Option<Duration> {
        let oldest = self.bodies.values().map(|(_, started)| *started).min()?;
        Some(self.step_limit.saturating_sub(oldest.elapsed()))
    }
}

pub(crate) struct Time {
    state: Mutex<State>,
    /// Raised by every park, task end and body end.
    changed: Notify,
    settling: tokio::sync::Mutex<()>,
}

impl Time {
    pub(super) fn new(ms: i64) -> Self {
        let state = State {
            ms,
            open: false,
            next: 0,
            tasks: BTreeMap::new(),
            bodies: BTreeMap::new(),
            replayed: HashMap::new(),
            step_limit: Duration::from_secs(10),
        };
        Self {
            state: Mutex::new(state),
            changed: Notify::new(),
            settling: tokio::sync::Mutex::new(()),
        }
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("never panics while locked")
    }

    pub(super) fn now(&self) -> i64 {
        self.lock().ms
    }

    /// Parks the task running until one of `signals` is ready or the clock reaches `at`, while
    /// the gate is open and its owner not paused.
    pub(super) async fn wait(&self, signals: &[Arc<dyn Ready>], at: Option<i64>) {
        let id = TASK
            .try_with(|id| *id)
            .expect("a virtual wait runs in a task of the environment");
        Wait {
            time: self,
            id,
            parked: false,
            signals: signals.to_vec(),
            at,
        }
        .await
    }

    /// Returns once the gate is open and the owner not paused.
    pub(super) async fn passing(&self, owner: &Incarnation) {
        let blocked = {
            let st = self.lock();
            !st.open || owner.paused()
        };
        if blocked {
            // A flag of its own: a set flag is never woken again, so the wakers registered on a
            // shared one would stay there forever.
            let open = crate::clock::Flag::default();
            open.set();
            self.wait(&[Arc::new(open)], None).await;
        }
    }

    /// A task of `owner`, which starts once the gate is open.
    pub(super) fn spawn(
        self: &Arc<Self>,
        owner: &Arc<Incarnation>,
        name: String,
        future: BoxFuture<'static, ()>,
    ) -> (u64, AbortHandle) {
        let id = {
            let mut st = self.lock();
            let id = st.next;
            st.next += 1;
            let tracked = Tracked {
                owner: owner.clone(),
                name,
                waiter: None,
            };
            st.tasks.insert(id, tracked);
            id
        };
        let (time, gated) = (self.clone(), owner.clone());
        let task = TASK.scope(id, async move {
            let _ended = Ended(time.clone(), id, false);
            time.passing(&gated).await;
            future.await;
        });
        (id, tokio::spawn(task).abort_handle())
    }

    /// Stops tracking tasks of a crashed worker, which are aborted.
    pub(super) fn forget(&self, ids: &[u64]) {
        let mut st = self.lock();
        for id in ids {
            st.tasks.remove(id);
        }
        drop(st);
        self.changed.notify_one();
    }

    /// A step body, waited for by `settle` until it returns or is aborted, and whether it runs a
    /// thread, which an abort would not stop.
    pub(super) fn body<T: Send + 'static>(
        self: &Arc<Self>,
        name: String,
        future: impl Future<Output = T> + Send + 'static,
    ) -> (Task<T>, Arc<AtomicBool>) {
        let id = {
            let mut st = self.lock();
            let id = st.next;
            st.next += 1;
            st.bodies.insert(id, (name, Instant::now()));
            id
        };
        let time = self.clone();
        // Its end is told once its value is in, so that its waiter is ready by then, or once it
        // is aborted, which drops `ended`.
        let end = Ended(time, id, true);
        let ended = move |_: &T| drop(end);
        let threaded = Arc::new(AtomicBool::new(false));
        let task = Task::new(BODY.scope(threaded.clone(), future), ended, |f| {
            tokio::spawn(f).abort_handle()
        });
        (task, threaded)
    }

    pub(super) fn bodies_running(&self) -> bool {
        !self.lock().bodies.is_empty()
    }

    pub(super) async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Has every parked task check again whether it may go on.
    pub(super) fn kick(&self) {
        self.lock().kick();
        self.changed.notify_one();
    }

    pub(super) fn notify(&self) {
        self.changed.notify_one();
    }

    pub(super) fn replayed(&self, run: &str) {
        *self.lock().replayed.entry(run.to_owned()).or_default() += 1;
    }

    pub(super) async fn settle(&self) -> Result<()> {
        outside()?;
        let _settling = self.settling.lock().await;
        self.settled().await
    }

    /// Settles, then moves to each deadline up to `by` later, settling there; `skip` first moves
    /// past what cannot find anything up to the instant given.
    pub(super) async fn advance(
        &self,
        by: i64,
        mut skip: impl FnMut(&mut State, i64),
    ) -> Result<()> {
        outside()?;
        let _settling = self.settling.lock().await;
        let target = self.now() + by;
        self.settled().await?;
        loop {
            {
                let mut st = self.lock();
                if st.ms >= target {
                    return Ok(());
                }
                skip(&mut st, target);
                let deadlines = (st.tasks.values())
                    .filter(|t| !t.owner.paused())
                    .filter_map(|t| t.waiter.as_ref().and_then(|w| w.at));
                let next = deadlines.fold(target, i64::min);
                if next <= st.ms {
                    return Err(Error::Protocol(format!(
                        "a deadline at {next} ms is not after now, {} ms",
                        st.ms
                    )));
                }
                st.ms = next;
            }
            self.settled().await?;
        }
    }

    async fn settled(&self) -> Result<()> {
        {
            let mut st = self.lock();
            // Replays are counted per settle: the test's own calls between settles wake runs too.
            st.replayed.clear();
            st.open = true;
            st.kick();
        }
        let _closed = Closed(self);
        loop {
            let budget = {
                let st = self.lock();
                if st.quiet() {
                    return Ok(());
                }
                st.budget()
            };
            let changed = self.changed.notified();
            match budget {
                Some(budget) => drop(tokio::time::timeout(budget, changed).await),
                None => changed.await,
            }
            self.lock().check()?;
        }
    }
}

/// Closes the gate when a settle ends, however it ends.
struct Closed<'a>(&'a Time);

impl Drop for Closed<'_> {
    fn drop(&mut self) {
        self.0.lock().open = false;
    }
}

/// The end of a task (`false`) or a body (`true`), however it ends.
struct Ended(Arc<Time>, u64, bool);

impl Drop for Ended {
    fn drop(&mut self) {
        let mut st = self.0.lock();
        if self.2 {
            st.bodies.remove(&self.1);
        } else {
            st.tasks.remove(&self.1);
        }
        drop(st);
        self.0.changed.notify_one();
    }
}
