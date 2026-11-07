//! A worker: the agent machine of I-23 driven against a store, and the replays it asks for.

mod agent;
pub(crate) mod replay;
mod replays;
#[cfg(test)]
mod tests;

use std::sync::Arc;

#[cfg(feature = "testing")]
pub(crate) use agent::Agent;
use agent::os_seed;
pub(crate) use agent::{Event, Launch, OnDeath, Running};

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::settings::Settings;
use crate::store::Store;
use crate::workflow::{Untyped, Workflows};

/// Something a start, send or cancel of this process wakes: a worker.
pub(crate) trait Wakes: Send + Sync {
    fn wake(&self);
}

/// A running worker. Its agent machine and every run machine of its replays share one instance
/// of the core; the agent loop runs on a task of its own, each replay on another.
pub struct Worker(pub(crate) Kind);

pub(crate) enum Kind {
    Real(Running),
    #[cfg(feature = "testing")]
    Env(Arc<crate::testing::EnvWorker>),
}

impl Worker {
    /// Starts a worker on the current tokio runtime, on a namespace of `partitions`.
    pub(crate) fn start(
        store: Arc<dyn Store>,
        workflows: Vec<Untyped>,
        partitions: u32,
        settings: Settings,
        died: OnDeath,
    ) -> Result<Self> {
        settings.validate()?;
        let workflows = Workflows::new(workflows, settings.retention_min)?;
        let running = Launch {
            store,
            workflows: &workflows,
            partitions,
            settings: &settings,
            died,
            clock: Clock::Real,
            id: hex::encode(rand::random::<[u8; 8]>()),
            seed: os_seed(),
        }
        .start()?;
        Ok(Self(Kind::Real(running)))
    }

    /// What a start, send or cancel of this process wakes.
    pub(crate) fn wakes(&self) -> Arc<dyn Wakes> {
        match &self.0 {
            Kind::Real(r) => r.agent.clone(),
            #[cfg(feature = "testing")]
            Kind::Env(w) => w.clone(),
        }
    }

    /// Wakes the agent at once, as after a message sent from this process.
    pub(crate) fn wake(&self) {
        self.wakes().wake();
    }

    /// Runs until a signal of `signals`, then shuts down; a worker that stops on its own first
    /// returns its error.
    pub(crate) async fn serve(self, mut signals: Signals) -> Result<()> {
        #[cfg_attr(
            not(feature = "testing"),
            allow(clippy::infallible_destructuring_match)
        )]
        let running = match &self.0 {
            Kind::Real(r) => r,
            #[cfg(feature = "testing")]
            Kind::Env(_) => {
                return Err(Error::ConfigError(
                    "a worker of a test environment is started, not served".into(),
                ));
            }
        };
        tokio::select! {
            () = running.task.returned() => {
                running.ended()?;
                return Err(Error::Protocol("the worker stopped without a signal".into()));
            }
            received = signals.recv() => received?,
        }
        self.shutdown().await
    }

    /// Starts no new replay and lets the steps in flight end within `shutdown_grace`.
    pub async fn shutdown(&self) -> Result<()> {
        match &self.0 {
            Kind::Real(r) => r.shutdown(r.grace).await,
            #[cfg(feature = "testing")]
            Kind::Env(w) => w.shutdown().await,
        }
    }

    /// Whether its loop runs: started, and neither left nor dead.
    pub fn is_alive(&self) -> bool {
        match &self.0 {
            Kind::Real(r) => r.is_alive(),
            #[cfg(feature = "testing")]
            Kind::Env(w) => w.is_alive(),
        }
    }

    /// The error that killed its loop, else `None`: what a readiness probe reads.
    pub fn error(&self) -> Option<Error> {
        match &self.0 {
            Kind::Real(r) => r.error(),
            #[cfg(feature = "testing")]
            Kind::Env(w) => w.error(),
        }
    }

    #[cfg(feature = "testing")]
    fn env(&self) -> &crate::testing::EnvWorker {
        match &self.0 {
            Kind::Env(w) => w,
            Kind::Real(_) => panic!("only a worker of a testing::Env crashes, pauses and resumes"),
        }
    }

    /// Kills the worker at once, as a SIGKILL would: its claims stay until they expire, and its
    /// steps in flight are abandoned, their bodies dropped at their await, those in a thread
    /// (`step_blocking`) left to run to their end.
    ///
    /// # Panics
    ///
    /// For a worker outside a `testing::Env`, as `pause` and `resume`.
    #[cfg(feature = "testing")]
    pub fn crash(&self) {
        self.env().crash();
    }

    /// Freezes the worker, as a long garbage collection or a network partition would: it renews
    /// no claim and reaches no store, while its steps' bodies go on to their end.
    #[cfg(feature = "testing")]
    pub fn pause(&self) {
        self.env().pause();
    }

    /// Lets a paused worker go on from where it was.
    #[cfg(feature = "testing")]
    pub fn resume(&self) {
        self.env().resume();
    }
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
