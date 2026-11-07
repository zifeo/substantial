//! The agent loop of a worker: the agent machine of I-23 driven against a store, and how it
//! starts.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::FutureExt;

use super::Wakes;
use super::replay::ToReplay;
use super::replays::Replays;
use crate::clock::{Clock, Inbox, Ready, Task, Tasks};
use crate::core::{AgentMachine, Core};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, agent_command::Command, op_result, replay_end};
use crate::settings::Settings;
use crate::store::{self, Store};
use crate::time::millis;
use crate::workflow::Workflows;

/// What the agent loop of a worker waits on: store answers, replays, and the process.
pub(crate) enum Event {
    Feed(u64, op_result::Result),
    Commit(String, b::Commit),
    Ended(String, replay_end::End),
    Wake,
    /// Starts no new replay, and stops those still running after this grace.
    Shutdown(Duration),
    /// A store operation panicked, a bug that kills the worker.
    Panicked(String),
}

/// What a worker's death is told to: the waits in progress of its `Substantial`.
pub(crate) type OnDeath = Arc<dyn Fn(&Error) + Send + Sync>;

/// The side of a running agent loop that the process talks to.
#[derive(Default)]
pub(crate) struct Agent {
    events: Arc<Inbox<Event>>,
    wake_pending: AtomicBool,
}

impl Agent {
    /// Wakes the agent at once, as after a message sent from this process.
    pub(crate) fn wake(&self) {
        if !self.wake_pending.swap(true, Ordering::SeqCst) {
            self.events.push(Event::Wake);
        }
    }

    pub(crate) fn shutdown(&self, grace: Duration) {
        self.events.push(Event::Shutdown(grace));
    }

    #[cfg(feature = "testing")]
    pub(crate) fn idle(&self) -> bool {
        self.events.is_empty()
    }
}

impl Wakes for Agent {
    fn wake(&self) {
        Agent::wake(self);
    }
}

/// An agent loop on its task.
pub(crate) struct Running {
    pub(crate) agent: Arc<Agent>,
    pub(crate) task: Task<Result<()>>,
    /// The worker's `shutdown_grace`.
    pub(super) grace: Duration,
}

impl Running {
    pub(crate) async fn shutdown(&self, grace: Duration) -> Result<()> {
        self.agent.shutdown(grace);
        self.task.returned().await;
        self.ended()
    }

    pub(crate) fn ended(&self) -> Result<()> {
        self.task.with(|ended| match ended {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(e.again()),
            None => Err(Error::Protocol("the worker task did not return".into())),
        })
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    pub(crate) fn error(&self) -> Option<Error> {
        self.task
            .with(|ended| ended.and_then(|e| e.as_ref().err()).map(Error::again))
    }
}

/// A dropped worker leaves as on `shutdown`: its loop runs on a task of its own, which nothing
/// else would stop.
impl Drop for Running {
    fn drop(&mut self) {
        if !self.task.is_finished() {
            self.agent.shutdown(self.grace);
        }
    }
}

/// What starts an agent loop.
pub(crate) struct Launch<'a> {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) workflows: &'a Workflows,
    pub(crate) partitions: u32,
    pub(crate) settings: &'a Settings,
    pub(crate) died: OnDeath,
    pub(crate) clock: Clock,
    pub(crate) id: String,
    /// The core's seed, each replay's derived from it.
    pub(crate) seed: Vec<u8>,
}

impl Launch<'_> {
    /// Starts the agent loop on a task of the clock, on the current tokio runtime; the store is
    /// scoped to the namespace, so the key prefix is empty.
    pub(crate) fn start(self) -> Result<Running> {
        let config = agent_config(
            &self.id,
            self.workflows,
            self.partitions,
            self.store.capabilities().wait,
            self.settings,
            self.seed,
        );
        let core = Core::new()?;
        let machine = AgentMachine::new(&core, &config)?;
        let agent = Arc::new(Agent::default());
        let replays = Replays::new(
            core,
            self.store.clone(),
            self.workflows.clone(),
            self.settings.step_timeout,
            self.clock.clone(),
            agent.events.clone(),
        );
        let looped = AgentLoop {
            machine,
            store: self.store,
            replays,
            clock: self.clock.clone(),
            agent: agent.clone(),
        };
        let (id, died) = (self.id.clone(), self.died);
        let looped = async move {
            AssertUnwindSafe(looped.run())
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| Err(Error::Protocol(panicked(&*panic))))
        };
        // Told once the task has its value, so that the waits it fails see a worker not alive.
        let ended = move |ended: &Result<()>| {
            if let Err(e) = ended {
                tracing::error!(worker = id, "worker died: {e}");
                died(e);
            }
        };
        let task = self
            .clock
            .spawn_then(format_args!("worker {}", self.id), looped, ended);
        let grace = self.settings.shutdown_grace;
        Ok(Running { agent, task, grace })
    }
}

pub(super) fn panicked(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic without a message");
    format!("the worker panicked: {message}")
}

/// How many times the core reissues an operation that still failed after the adapter's own
/// retries, which `store_retry` sets at open (I-26).
const OP_RETRIES: u32 = 5;

/// The 256 bits the core seeds its random and uuid from, from the OS CSPRNG.
pub(super) fn os_seed() -> Vec<u8> {
    use rand::TryRng;
    let mut seed = vec![0; 32];
    rand::rngs::SysRng
        .try_fill_bytes(&mut seed)
        .expect("the OS CSPRNG");
    seed
}

fn agent_config(
    id: &str,
    workflows: &Workflows,
    partitions: u32,
    store_waits: bool,
    s: &Settings,
    seed: Vec<u8>,
) -> b::AgentConfig {
    let mut served: BTreeMap<&str, b::Served> = BTreeMap::new();
    for (name, version) in workflows.keys() {
        let s = served.entry(name).or_insert_with(|| b::Served {
            name: name.to_owned(),
            versions: Vec::new(),
            unversioned: Some(false),
            retention: workflows.retention(name).map(|r| r.proto()),
        });
        match version {
            Some(v) => s.versions.push(v),
            None => s.unversioned = Some(true),
        }
    }
    let served = served
        .into_values()
        .map(|mut s| {
            s.versions.sort_unstable();
            s
        })
        .collect();
    b::AgentConfig {
        prefix: String::new(),
        partitions,
        worker: id.to_owned(),
        workflows: served,
        concurrency: s.concurrency,
        claim_ttl_ms: millis(s.claim_ttl),
        claim_skew_ms: millis(s.claim_skew),
        max_idle_ms: millis(s.max_idle),
        due_page: s.due_page,
        due_pages: s.due_pages,
        snapshot_every: s.snapshot_every,
        op_retries: OP_RETRIES,
        seed,
        store_waits,
        wait_seconds: s.wait.as_secs() as u32,
        tombstone_ms: millis(s.tombstone),
        retention_min_ms: millis(s.retention_min),
        recheck_ms: millis(s.recheck),
        settings: Some(b::RunSettings {
            commit_every_ms: millis(s.commit_every),
            continue_as_new_after_segments: s.continue_as_new_after.segments,
            continue_as_new_after_bytes: s.continue_as_new_after.bytes,
            dedup_window_ms: millis(s.dedup_window),
            retention: Some(s.retention.proto()),
        }),
    }
}

struct AgentLoop {
    machine: AgentMachine,
    store: Arc<dyn Store>,
    replays: Replays,
    clock: Clock,
    agent: Arc<Agent>,
}

impl AgentLoop {
    async fn run(mut self) -> Result<()> {
        // Aborted when the loop ends, as their answers have no one to go to.
        let mut ops = Tasks::default();
        let events = self.agent.events.clone();
        let signal: Arc<dyn Ready> = events.clone();
        let (mut grace, mut grace_ends): (i64, Option<i64>) = (0, None);
        loop {
            match self.machine.next(self.clock.now())? {
                Command::Store(b::agent_command::Store { id, op }) => {
                    let (store, events) = (self.store.clone(), events.clone());
                    let op =
                        op.ok_or_else(|| Error::Protocol("store command without op".into()))?;
                    ops.push(self.clock.spawn(format_args!("store op"), async move {
                        let performed = AssertUnwindSafe(store::perform(&*store, op));
                        events.push(match performed.catch_unwind().await {
                            Ok(result) => Event::Feed(id, result),
                            Err(panic) => Event::Panicked(panicked(&*panic)),
                        });
                    }));
                }
                Command::Replay(b::agent_command::Replay { run, input }) => {
                    let input = input
                        .ok_or_else(|| Error::Protocol(format!("replay of {run} without input")))?;
                    self.replays.spawn(run, input);
                }
                Command::Committed(c) => self.replays.tell(
                    &c.run,
                    ToReplay::Committed {
                        n: c.n,
                        won: c.won,
                        inbox: c.inbox,
                    },
                ),
                Command::StopReplay(run) => self.replays.tell(&run, ToReplay::Stop),
                Command::WaitUntil(mut at) => {
                    let past_grace = grace_ends.filter(|_| !self.replays.is_empty());
                    if let Some(ends) = past_grace {
                        let left = (ends - self.clock.elapsed()).max(0);
                        at.ms = at.ms.min(self.clock.now().ms + left);
                    }
                    if events.is_empty() {
                        self.clock
                            .wait(std::slice::from_ref(&signal), Some(at))
                            .await;
                    }
                    let Some(event) = events.pop() else {
                        if past_grace.is_some_and(|ends| self.clock.elapsed() >= ends) {
                            self.replays.stop_all();
                            grace_ends = Some(self.clock.elapsed() + grace);
                        }
                        continue;
                    };
                    match event {
                        Event::Feed(id, result) => self.machine.feed(id, result)?,
                        Event::Commit(run, commit) => self.machine.commit(run, commit)?,
                        Event::Ended(run, end) => {
                            self.replays.ended(&run);
                            self.machine.replay_ended(run, end)?;
                        }
                        Event::Wake => {
                            self.agent.wake_pending.store(false, Ordering::SeqCst);
                            self.machine.wake()?;
                        }
                        Event::Shutdown(g) if grace_ends.is_none() => {
                            grace = i64::try_from(millis(g)).expect("a grace fits in i64 ms");
                            grace_ends = Some(self.clock.elapsed() + grace);
                            self.machine.shutdown()?;
                        }
                        Event::Shutdown(_) => {}
                        Event::Panicked(message) => return Err(Error::Protocol(message)),
                    }
                }
                Command::Stop(_) => return Ok(()),
            }
        }
    }
}
