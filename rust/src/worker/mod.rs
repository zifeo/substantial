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
pub(crate) use agent::{Event, Launch, OnDeath, Running, panicked};

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::observe::Metering;
use crate::settings::Settings;
use crate::store::Store;
use crate::workflow::{Untyped, Workflows};

/// Something a start, send or cancel of this process wakes: a worker.
pub(crate) trait Wakes: Send + Sync {
    /// A full scan.
    fn wake(&self);
    /// A scan of the due entries of `workflow` alone, which a start or send created one under.
    fn wake_workflow(&self, workflow: &str);
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
    /// Starts a worker on the current tokio runtime.
    pub(crate) fn start(
        store: Arc<dyn Store>,
        workflows: Vec<Untyped>,
        settings: Settings,
        died: OnDeath,
        metering: Metering,
        clock: Clock,
    ) -> Result<Self> {
        settings.validate()?;
        let workflows = Workflows::new(workflows, settings.retention_min)?;
        let running = Launch {
            store,
            workflows: &workflows,
            settings: &settings,
            died,
            clock,
            id: hex::encode(rand::random::<[u8; 8]>()),
            seed: os_seed(),
            metering,
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

    /// Wakes the agent at once for a full scan.
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
        // § dh7y75pvn.a@yde29
        tokio::select! {
            () = running.task.returned() => {
                running.ended()?;
                return Err(Error::Protocol("the worker stopped without a signal".into()));
            }
            received = signals.recv() => received?,
        }
        self.shutdown().await
        // § end dh7y75pvn.a
    }

    /// Starts no new replay and lets the steps in flight end within `shutdown_grace`.
    pub async fn shutdown(&self) -> Result<()> {
        match &self.0 {
            Kind::Real(r) => r.shutdown(r.grace).await,
            #[cfg(feature = "testing")]
            Kind::Env(w) => w.shutdown().await,
        }
    }

    /// Serves every run due now or within `within`, those its own work makes due, and those of a
    /// crashed worker whose claim expires by then, then stops as `shutdown` does once nothing is
    /// due by then and nothing is in flight; the worker's error if it died.
    ///
    /// # Panics
    ///
    /// For a worker of a `testing::Env` that crashed.
    pub async fn drain(&self, within: std::time::Duration) -> Result<()> {
        match &self.0 {
            Kind::Real(r) => {
                // § cjtegwkna.a@2fzqk
                r.drain(within);
                r.task.returned().await;
                r.ended()
            }
            #[cfg(feature = "testing")]
            Kind::Env(w) => w.drain(within).await,
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
    // § one k6m3smdpk@kt775
    term: tokio::signal::unix::Signal,
}

impl Signals {
    /// Listens from now on, so that a signal sent once this returns is never missed.
    pub(crate) fn listen() -> Result<Self> {
        Ok(Self {
            term: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) -> Result<()> {
        // § dh7y75pvn.a@vqqs2
        tokio::select! {
            _ = self.term.recv() => Ok(()),
            interrupted = tokio::signal::ctrl_c() => Ok(interrupted?),
        }
    }
}
