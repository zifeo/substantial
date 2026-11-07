//! A worker: the agent machine of I-23 driven against a store, and the replays it asks for.

mod replay;
mod replays;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use replay::{ReplayEvent, ToReplay};
use replays::Replays;

use crate::client::Died;
use crate::core::{AgentMachine, Core};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, agent_command::Command};
use crate::settings::Settings;
use crate::store::{self, Store};
use crate::time::{millis, until};
use crate::workflow::scheduler::Clock;
use crate::workflow::{Untyped, Workflows};

/// A running worker. Its agent machine and every run machine of its replays share one instance
/// of the core; the agent loop runs on a task of its own, each replay on another.
pub struct Worker {
    wake: Arc<Notify>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl Worker {
    /// Starts a worker on the current tokio runtime, on a namespace of `partitions` (the store is
    /// scoped to the namespace, so the key prefix is empty).
    pub(crate) fn start(
        store: Arc<dyn Store>,
        workflows: Vec<Untyped>,
        partitions: u32,
        settings: Settings,
        died: Died,
    ) -> Result<Self> {
        settings.validate()?;
        let workflows = Workflows::new(workflows)?;
        let id = hex::encode(rand::random::<[u8; 8]>());
        let core = Core::new()?;
        let agent = AgentMachine::new(
            &core,
            &agent_config(
                &id,
                &workflows,
                partitions,
                store.capabilities().wait,
                &settings,
            ),
        )?;
        let clock: Clock = Arc::new(crate::time::now);
        let wake = Arc::new(Notify::new());
        let (shutdown, shutdown_rx) = oneshot::channel();
        let replays = Replays::new(
            core,
            store.clone(),
            workflows,
            settings.step_timeout,
            clock.clone(),
        );
        let looped = AgentLoop {
            agent,
            store,
            replays,
            clock,
            wake: wake.clone(),
            grace: settings.shutdown_grace,
        };
        let task = tokio::spawn(async move {
            let ended = AssertUnwindSafe(looped.run(shutdown_rx))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| Err(Error::Protocol(panicked(&*panic))));
            if let Err(e) = &ended {
                tracing::error!(worker = id, "worker died: {e}");
                died.send_replace(Some(Arc::new(e.again())));
            }
            ended
        });
        Ok(Self {
            wake,
            shutdown: Some(shutdown),
            task,
        })
    }

    pub(crate) fn wake_handle(&self) -> &Arc<Notify> {
        &self.wake
    }

    /// Wakes the agent at once, as after a message sent from this process.
    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Runs until a signal of `signals`, then shuts down; a worker that stops on its own first
    /// returns its error.
    pub(crate) async fn serve(mut self, mut signals: Signals) -> Result<()> {
        tokio::select! {
            ended = &mut self.task => {
                ended.map_err(|e| Error::Protocol(format!("worker task: {e}")))??;
                return Err(Error::Protocol("the worker stopped without a signal".into()));
            }
            received = signals.recv() => received?,
        }
        self.shutdown().await
    }

    /// Starts no new replay and lets the steps in flight end within `shutdown_grace`.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        (&mut self.task)
            .await
            .map_err(|e| Error::Protocol(format!("worker task: {e}")))?
    }
}

fn panicked(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic without a message");
    format!("the worker panicked: {message}")
}

/// The signals that stop a served worker: SIGTERM, and Ctrl-C.
pub(crate) struct Signals {
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
}

impl Signals {
    /// Listens from now on, so that a signal sent once this returns is never missed.
    pub(crate) fn listen() -> Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            term: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) -> Result<()> {
        #[cfg(unix)]
        tokio::select! {
            _ = self.term.recv() => Ok(()),
            interrupted = tokio::signal::ctrl_c() => Ok(interrupted?),
        }
        #[cfg(not(unix))]
        Ok(tokio::signal::ctrl_c().await?)
    }
}

/// How many times the core reissues an operation that still failed after the adapter's own
/// retries, which `store_retry` sets at open (I-26).
const OP_RETRIES: u32 = 5;

/// The 256 bits the core seeds its random and uuid from, from the OS CSPRNG.
fn os_seed() -> Vec<u8> {
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
) -> b::AgentConfig {
    let mut served: BTreeMap<&str, b::Served> = BTreeMap::new();
    for (name, version) in workflows.keys() {
        let s = served.entry(name).or_insert_with(|| b::Served {
            name: name.to_owned(),
            versions: Vec::new(),
            unversioned: Some(false),
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
        seed: os_seed(),
        store_waits,
        wait_seconds: s.wait.as_secs() as u32,
        settings: Some(b::RunSettings {
            commit_every_ms: millis(s.commit_every),
            continue_as_new_after_segments: s.continue_as_new_after.segments,
            continue_as_new_after_bytes: s.continue_as_new_after.bytes,
            dedup_window_ms: millis(s.dedup_window),
        }),
    }
}

struct AgentLoop {
    agent: AgentMachine,
    store: Arc<dyn Store>,
    replays: Replays,
    clock: Clock,
    wake: Arc<Notify>,
    grace: std::time::Duration,
}

impl AgentLoop {
    async fn run(mut self, mut shutdown: oneshot::Receiver<()>) -> Result<()> {
        let mut ops = FuturesUnordered::new();
        let mut grace_ends: Option<Instant> = None;
        loop {
            match self.agent.next((self.clock)())? {
                Command::Store(b::agent_command::Store { id, op }) => {
                    let store = self.store.clone();
                    let op =
                        op.ok_or_else(|| Error::Protocol("store command without op".into()))?;
                    ops.push(async move { (id, store::perform(&*store, op).await) });
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
                Command::WaitUntil(at) => {
                    let deadline = Instant::now() + until(&at, &(self.clock)());
                    let past_grace = grace_ends.filter(|_| !self.replays.is_empty());
                    tokio::select! {
                        Some((id, result)) = ops.next() => self.agent.feed(id, result)?,
                        event = self.replays.next() => match event {
                            ReplayEvent::Commit(run, commit) => self.agent.commit(run, commit)?,
                            ReplayEvent::Ended(run, end) => self.agent.replay_ended(run, end)?,
                        },
                        _ = self.wake.notified() => self.agent.wake()?,
                        _ = &mut shutdown, if grace_ends.is_none() => {
                            grace_ends = Some(Instant::now() + self.grace);
                            self.agent.shutdown()?;
                        }
                        _ = tokio::time::sleep_until(past_grace.unwrap_or(deadline)), if past_grace.is_some() => {
                            self.replays.stop_all();
                            grace_ends = Some(Instant::now() + self.grace);
                        }
                        _ = tokio::time::sleep_until(deadline) => {}
                    }
                }
                Command::Stop(_) => return Ok(()),
            }
        }
    }
}
