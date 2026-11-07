use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::Event;
use super::replay::{Host, ToReplay, replay};
use crate::clock::{Clock, Inbox, Task};
use crate::core::Core;
use crate::proto::boundary::v1::{self as b, replay_end};
use crate::store::Store;
use crate::workflow::Workflows;

/// The replays of a worker, each a task of its own talking to the agent loop through inboxes;
/// those still running when it is dropped are aborted.
pub(crate) struct Replays {
    host: Host,
    workflows: Workflows,
    /// The worker's `step_timeout`, for the workflows that set none.
    step_timeout: Duration,
    running: HashMap<String, (Arc<Inbox<ToReplay>>, Task<()>)>,
    events: Arc<Inbox<Event>>,
}

impl Replays {
    pub(crate) fn new(
        core: Core,
        store: Arc<dyn Store>,
        workflows: Workflows,
        step_timeout: Duration,
        clock: Clock,
        events: Arc<Inbox<Event>>,
    ) -> Self {
        Self {
            host: Host { core, store, clock },
            workflows,
            step_timeout,
            running: HashMap::new(),
            events,
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
            self.events
                .push(Event::Ended(run, replay_end::End::Dropped(b::Empty {})));
            return;
        };
        let inbox = Arc::new(Inbox::default());
        let step_timeout = workflow.step_timeout().unwrap_or(self.step_timeout);
        let clock = &self.host.clock;
        clock.replayed(&run);
        let task = clock.spawn(
            format_args!("replay of {run}"),
            replay(
                self.host.clone(),
                workflow,
                step_timeout,
                input,
                self.events.clone(),
                inbox.clone(),
            ),
        );
        if let Some((inbox, _)) = self.running.insert(run.clone(), (inbox, task)) {
            tracing::error!(run, "a second replay of a run in replay");
            inbox.push(ToReplay::Stop);
        }
    }

    pub(crate) fn tell(&self, run: &str, message: ToReplay) {
        match self.running.get(run) {
            Some((inbox, _)) => inbox.push(message),
            None => tracing::warn!(run, ?message, "no replay of this run"),
        }
    }

    pub(crate) fn stop_all(&self) {
        for (inbox, _) in self.running.values() {
            inbox.push(ToReplay::Stop);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    pub(crate) fn ended(&mut self, run: &str) {
        self.running.remove(run);
    }
}

impl Drop for Replays {
    fn drop(&mut self) {
        for (_, task) in self.running.values() {
            task.abort();
        }
    }
}
