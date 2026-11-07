use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::replay::{Host, ReplayEvent, ToReplay, replay};
use crate::core::Core;
use crate::proto::boundary::v1::{self as b, replay_end};
use crate::store::Store;
use crate::workflow::Workflows;
use crate::workflow::scheduler::Clock;

/// The replays of a worker, each a task of its own talking to the agent loop through channels.
pub(crate) struct Replays {
    host: Host,
    workflows: Workflows,
    /// The worker's `step_timeout`, for the workflows that set none.
    step_timeout: Duration,
    running: HashMap<String, mpsc::UnboundedSender<ToReplay>>,
    events_tx: mpsc::UnboundedSender<ReplayEvent>,
    events_rx: mpsc::UnboundedReceiver<ReplayEvent>,
}

impl Replays {
    pub(crate) fn new(
        core: Core,
        store: Arc<dyn Store>,
        workflows: Workflows,
        step_timeout: Duration,
        clock: Clock,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        Self {
            host: Host { core, store, clock },
            workflows,
            step_timeout,
            running: HashMap::new(),
            events_tx,
            events_rx,
        }
    }

    pub(crate) fn spawn(&mut self, run: String, input: b::RunInput) {
        let (name, version) = input
            .start
            .as_ref()
            .map(|s| (s.workflow.as_str(), s.version))
            .unwrap_or_default();
        let Some(workflow) = self.workflows.get(name, version).cloned() else {
            tracing::error!(
                run,
                workflow = name,
                ?version,
                "replay of a workflow this worker does not serve"
            );
            let _ = self.events_tx.send(ReplayEvent::Ended(
                run,
                replay_end::End::Dropped(b::Empty {}),
            ));
            return;
        };
        let (tx, rx) = mpsc::unbounded_channel();
        if self.running.insert(run.clone(), tx).is_some() {
            tracing::error!(run, "a second replay of a run in replay");
        }
        let events = self.events_tx.clone();
        let step_timeout = workflow.step_timeout().unwrap_or(self.step_timeout);
        tokio::spawn(replay(
            self.host.clone(),
            workflow,
            step_timeout,
            input,
            events,
            rx,
        ));
    }

    pub(crate) fn tell(&self, run: &str, message: ToReplay) {
        match self.running.get(run) {
            Some(tx) => {
                let _ = tx.send(message);
            }
            None => tracing::warn!(run, ?message, "no replay of this run"),
        }
    }

    pub(crate) fn stop_all(&self) {
        for tx in self.running.values() {
            let _ = tx.send(ToReplay::Stop);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    pub(crate) async fn next(&mut self) -> ReplayEvent {
        let event = self
            .events_rx
            .recv()
            .await
            .expect("the sender lives in self");
        if let ReplayEvent::Ended(run, _) = &event {
            self.running.remove(run);
        }
        event
    }
}
