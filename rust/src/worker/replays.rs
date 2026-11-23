use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::Event;
use super::replay::{Host, ToReplay, replay};
use crate::clock::{Inbox, Task};
use crate::proto::boundary::v1::{self as b, replay_end};
use crate::proto::layout::v1::Time;
use crate::workflow::Workflows;

/// The replays of a worker, each a task of its own talking to the agent loop through inboxes;
/// those still running when it is dropped are aborted.
pub(crate) struct Replays {
    host: Host,
    workflows: Workflows,
    /// The worker's `step_timeout`, for the workflows that set none.
    step_timeout: Duration,
    /// By chain, as the agent names a replay.
    running: HashMap<String, (Arc<Inbox<ToReplay>>, Task<()>)>,
    events: Arc<Inbox<Event>>,
}

impl Replays {
    pub(crate) fn new(
        host: Host,
        workflows: Workflows,
        step_timeout: Duration,
        events: Arc<Inbox<Event>>,
    ) -> Self {
        Self {
            host,
            workflows,
            step_timeout,
            running: HashMap::new(),
            events,
        }
    }

    /// A replay of `chain`, whose current run is `input.run`, which logs and observers name.
    pub(crate) fn spawn(&mut self, chain: String, input: b::RunInput, due_at: Option<Time>) {
        let run = input.run.clone();
        let (name, version) = input
            .start
            .as_ref()
            .map(|s| (s.workflow.as_str(), s.version))
            .unwrap_or_default();
        // § pjrbw31h4@65kr3
        let Some(workflow) = self.workflows.get(name, version).cloned() else {
            tracing::error!(
                run,
                workflow = name,
                ?version,
                "replay of a workflow this worker does not serve"
            );
            self.events
                .push(Event::Ended(chain, replay_end::End::Dropped(b::Empty {})));
            return;
        };
        // § end pjrbw31h4
        let inbox = Arc::new(Inbox::default());
        // § aym52h8ch@ykf3t
        let step_timeout = workflow.step_timeout().unwrap_or(self.step_timeout);
        // § end aym52h8ch
        let clock = &self.host.clock;
        clock.replayed(&run);
        let task = clock.spawn(
            format_args!("replay of {run}"),
            replay(
                self.host.clone(),
                chain.clone(),
                workflow,
                step_timeout,
                input,
                due_at,
                self.events.clone(),
                inbox.clone(),
            ),
        );
        if let Some((inbox, _)) = self.running.insert(chain, (inbox, task)) {
            tracing::error!(run, "a second replay of a chain in replay");
            inbox.push(ToReplay::Stop);
        }
    }

    pub(crate) fn tell(&self, chain: &str, message: ToReplay) {
        match self.running.get(chain) {
            Some((inbox, _)) => inbox.push(message),
            None => tracing::warn!(chain, ?message, "no replay of this chain"),
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

    pub(crate) fn ended(&mut self, chain: &str) {
        self.running.remove(chain);
    }
}

impl Drop for Replays {
    fn drop(&mut self) {
        for (_, task) in self.running.values() {
            task.abort();
        }
    }
}
