//! The agent loop of a worker: the agent machine of I-23 driven against a store, and how it
//! starts.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::FutureExt;

use super::Wakes;
use super::replay::{Host, ToReplay};
use super::replays::Replays;
use crate::clock::{Clock, Inbox, Ready, Task, Tasks};
use crate::core::{AgentMachine, Core};
use crate::cost::Prices;
use crate::error::{Error, Result};
use crate::observe::{self, Metering, Observed};
use crate::proto::boundary::v1::{self as b, ErrorKind, agent_command::Command, replay_end};
use crate::proto::layout::v1 as layout;
use crate::settings::{Settings, retention_proto};
use crate::store::{self, Store};
use crate::time::millis;
use crate::workflow::Workflows;

/// What the agent loop of a worker waits on: store answers, replays, and the process.
pub(crate) enum Event {
    Feed(u64, b::OpResult),
    Commit(String, Box<b::Commit>),
    Ended(String, replay_end::End),
    /// A full scan.
    Wake,
    /// A scan of this workflow's due entries alone.
    WakeWorkflow(String),
    /// Starts no new replay, and stops those still running after this grace.
    Shutdown(Duration),
    /// Serves the entries due by this time, in ms, then stops.
    Drain(i64),
    /// A store operation or an observer panicked, a bug that kills the worker.
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
    /// Wakes the agent at once for a full scan.
    pub(crate) fn wake(&self) {
        if !self.wake_pending.swap(true, Ordering::SeqCst) {
            self.events.push(Event::Wake);
        }
    }

    /// Wakes the agent at once on `workflow`, as after a start or a send from this process.
    pub(crate) fn wake_workflow(&self, workflow: &str) {
        self.events.push(Event::WakeWorkflow(workflow.to_owned()));
    }

    pub(crate) fn shutdown(&self, grace: Duration) {
        self.events.push(Event::Shutdown(grace));
    }

    pub(crate) fn drain(&self, until: i64) {
        self.events.push(Event::Drain(until));
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

    fn wake_workflow(&self, workflow: &str) {
        Agent::wake_workflow(self, workflow);
    }
}

/// An agent loop on its task.
pub(crate) struct Running {
    pub(crate) agent: Arc<Agent>,
    pub(crate) task: Task<Result<()>>,
    /// The worker's `shutdown_grace`.
    pub(super) grace: Duration,
    clock: Clock,
}

impl Running {
    pub(crate) async fn shutdown(&self, grace: Duration) -> Result<()> {
        self.agent.shutdown(grace);
        self.task.returned().await;
        self.ended()
    }

    /// Asks the agent to drain within `within` of now, without waiting for it.
    pub(crate) fn drain(&self, within: Duration) -> i64 {
        // § cjtegwkna@g8870
        let until = self.clock.now().ms + i64::try_from(millis(within)).expect("ms fit in i64");
        self.agent.drain(until);
        until
    }

    pub(crate) fn ended(&self) -> Result<()> {
        self.task.with(|ended| match ended {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(e.again()),
            None => Err(Error::Protocol("the worker task did not return".into())),
        })
    }

    pub(crate) fn is_alive(&self) -> bool {
        // § pgv9r1j12.a@gspcj
        !self.task.is_finished()
    }

    pub(crate) fn error(&self) -> Option<Error> {
        // § pgv9r1j12.a@np81j
        self.task
            .with(|ended| ended.and_then(|e| e.as_ref().err()).map(Error::again))
    }
}

/// A dropped worker leaves as on `shutdown`: its loop runs on a task of its own, which nothing
/// else would stop.
impl Drop for Running {
    fn drop(&mut self) {
        // § dh7y75pvn.c@e9b9j
        if !self.task.is_finished() {
            self.agent.shutdown(self.grace);
        }
    }
}

/// What starts an agent loop.
pub(crate) struct Launch<'a> {
    pub(crate) store: Arc<dyn Store>,
    pub(crate) workflows: &'a Workflows,
    pub(crate) settings: &'a Settings,
    pub(crate) died: OnDeath,
    pub(crate) clock: Clock,
    pub(crate) id: String,
    /// The core's seed, each replay's derived from it.
    pub(crate) seed: Vec<u8>,
    /// The `Substantial`'s prices and observer; `store` is observed already.
    pub(crate) metering: Metering,
}

impl Launch<'_> {
    /// Starts the agent loop on a task of the clock, on the current tokio runtime; the store is
    /// scoped to the namespace, so the key prefix is empty.
    pub(crate) fn start(self) -> Result<Running> {
        let config = agent_config(
            &self.id,
            self.workflows,
            self.store.capabilities().wait,
            self.settings,
            self.seed,
            self.metering.prices.as_ref(),
        );
        let core = Core::new()?;
        // The config is the worker's: one the core refuses, such as prices in a currency outside
        // ISO 4217, is a ConfigError where the worker starts.
        let machine = AgentMachine::new(&core, &config).map_err(|e| match e {
            Error::Core {
                kind: ErrorKind::Invalid,
                message,
            } => Error::ConfigError(message),
            other => other,
        })?;
        let agent = Arc::new(Agent::default());
        let host = Host {
            core,
            store: self.store.clone(),
            clock: self.clock.clone(),
            metering: self.metering.clone(),
        };
        let replays = Replays::new(
            host,
            self.workflows.clone(),
            self.settings.step_timeout,
            agent.events.clone(),
        );
        let looped = AgentLoop {
            machine,
            store: self.store,
            replays,
            clock: self.clock.clone(),
            agent: agent.clone(),
            metering: self.metering,
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
            // § pgv9r1j12@kxt4m
            if let Err(e) = ended {
                tracing::error!(worker = id, "worker died: {e}");
                died(e);
            }
        };
        let task = self
            .clock
            .spawn_then(format_args!("worker {}", self.id), looped, ended);
        Ok(Running {
            agent,
            task,
            grace: self.settings.shutdown_grace,
            clock: self.clock,
        })
    }
}

pub(crate) fn panicked(panic: &(dyn std::any::Any + Send)) -> String {
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
    // § gpzydmca3.a@nf5pn
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
    store_waits: bool,
    s: &Settings,
    seed: Vec<u8>,
    prices: Option<&Prices>,
) -> b::AgentConfig {
    let mut served: BTreeMap<&str, b::Served> = BTreeMap::new();
    for (name, version) in workflows.keys() {
        let s = served.entry(name).or_insert_with(|| b::Served {
            name: name.to_owned(),
            versions: Vec::new(),
            unversioned: Some(false),
            retention: workflows.retention(name).map(retention_proto),
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
            retention: Some(retention_proto(s.retention)),
            prices: prices.map(Prices::to_proto),
        }),
    }
}

struct AgentLoop {
    machine: AgentMachine,
    store: Arc<dyn Store>,
    replays: Replays,
    clock: Clock,
    agent: Arc<Agent>,
    metering: Metering,
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
                    // § pgv9r1j12.b@5p1g1
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
                Command::Replay(b::agent_command::Replay { run, input, due_at }) => {
                    let input = input
                        .ok_or_else(|| Error::Protocol(format!("replay of {run} without input")))?;
                    self.replays.spawn(run, input, due_at);
                }
                Command::Committed(c) => self.replays.tell(
                    &c.run,
                    ToReplay::Committed {
                        n: c.n,
                        won: c.won,
                        inbox: c.inbox,
                        cost: c.cost,
                    },
                ),
                Command::Scanned(s) => {
                    let scan = Observed::Scan(observe::Scan {
                        workflow: s.workflow,
                        due: s.due,
                    });
                    self.metering.tell(|| scan).map_err(Error::Protocol)?;
                }
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
                            // § dh7y75pvn.b@j5tr9
                            self.replays.stop_all();
                            grace_ends = Some(self.clock.elapsed() + grace);
                        }
                        continue;
                    };
                    match event {
                        Event::Feed(id, result) => self.machine.feed(id, result)?,
                        Event::Commit(run, commit) => self.machine.commit(run, *commit)?,
                        Event::Ended(run, end) => {
                            self.replays.ended(&run);
                            self.machine.replay_ended(run, end)?;
                        }
                        Event::Wake => {
                            self.agent.wake_pending.store(false, Ordering::SeqCst);
                            self.machine.wake()?;
                        }
                        Event::WakeWorkflow(workflow) => self.machine.wake_workflow(workflow)?,
                        Event::Shutdown(g) if grace_ends.is_none() => {
                            // § dh7y75pvn@kb3x4
                            grace = i64::try_from(millis(g)).expect("a grace fits in i64 ms");
                            grace_ends = Some(self.clock.elapsed() + grace);
                            self.machine.shutdown()?;
                        }
                        Event::Shutdown(_) => {}
                        Event::Drain(until) => self.machine.drain(layout::Time { ms: until })?,
                        Event::Panicked(message) => return Err(Error::Protocol(message)),
                    }
                }
                Command::Stop(_) => return Ok(()),
            }
        }
    }
}
