use std::collections::VecDeque;

use super::{Substantial, datetime};
use crate::core::PlanMachine;
use crate::error::{Error, Failure, Result};
use crate::names::check_key;
use crate::plan;
use crate::proto::boundary::v1::{self as b, plan_input::Plan, plan_step::Step, runs};

/// What a live run waits on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitingOn {
    /// Started, never replayed yet.
    Start,
    /// A timer, or a step's retry backoff.
    Time,
    /// A message reached it and is due, consumed at its next wakeup.
    Inbox,
    /// Idle until a message comes, with no timer.
    Message,
    /// Blocked by an error of its code, tried again at `due_at` (`RunInfo::error`).
    Blocked,
}

impl std::fmt::Display for WaitingOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            WaitingOn::Start => "start",
            WaitingOn::Time => "time",
            WaitingOn::Inbox => "inbox",
            WaitingOn::Message => "message",
            WaitingOn::Blocked => "blocked",
        })
    }
}

impl TryFrom<b::Waiting> for WaitingOn {
    type Error = Error;

    fn try_from(w: b::Waiting) -> Result<Self> {
        Ok(match w {
            b::Waiting::Start => WaitingOn::Start,
            b::Waiting::Time => WaitingOn::Time,
            b::Waiting::Inbox => WaitingOn::Inbox,
            b::Waiting::Message => WaitingOn::Message,
            b::Waiting::Blocked => WaitingOn::Blocked,
            b::Waiting::Unspecified => {
                return Err(Error::Protocol("unspecified waiting".into()));
            }
        })
    }
}

/// A live run of a workflow, as `Substantial::runs` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunInfo {
    pub chain: String,
    pub run: String,
    pub version: Option<u32>,
    /// When it is due next; `None` when it waits on messages alone.
    pub due_at: Option<chrono::DateTime<chrono::Utc>>,
    pub waiting_on: WaitingOn,
    /// For a blocked run, the error of its latest block.
    pub error: Option<Failure>,
}

impl TryFrom<b::RunInfo> for RunInfo {
    type Error = Error;

    fn try_from(info: b::RunInfo) -> Result<Self> {
        let waiting_on = info.waiting().try_into()?;
        let error = match info.blocked.and_then(|b| b.error) {
            Some(error) => Some(Failure::try_from(error)?),
            None => None,
        };
        Ok(RunInfo {
            waiting_on,
            error,
            chain: info.chain,
            run: info.run,
            version: info.version,
            due_at: info.due_at.map(|t| datetime(&t)),
        })
    }
}

/// The runs plan of one listing, driven a page at a time as the stream is polled.
pub(super) struct Listing {
    sub: Substantial,
    workflow: String,
    machine: Option<PlanMachine>,
    page: VecDeque<b::RunInfo>,
}

impl Listing {
    pub(super) fn new(sub: Substantial, workflow: String) -> Self {
        Self {
            sub,
            workflow,
            machine: None,
            page: VecDeque::new(),
        }
    }

    pub(super) async fn next(mut self) -> Result<Option<(RunInfo, Self)>> {
        loop {
            if let Some(info) = self.page.pop_front() {
                return Ok(Some((info.try_into()?, self)));
            }
            let machine = match &self.machine {
                Some(machine) => machine,
                None => {
                    check_key("workflow name", &self.workflow)?;
                    let plan = Plan::Runs(b::RunsPlan {
                        prefix: String::new(),
                        workflow: self.workflow.clone(),
                    });
                    let input = b::PlanInput { plan: Some(plan) };
                    let machine = PlanMachine::new(&self.sub.0.core, &input);
                    self.machine.insert(machine.map_err(plan::caller_invalid)?)
                }
            };
            self.sub.check_runtime()?;
            let outcome = plan::outcome(machine, &*self.sub.0.store).await;
            match outcome.map_err(plan::caller_invalid)? {
                Step::Runs(b::Runs { runs: Some(runs) }) => match runs {
                    runs::Runs::Page(page) => self.page = page.runs.into(),
                    runs::Runs::End(_) => return Ok(None),
                    runs::Runs::Failed(f) => return Err(Error::StoreError(f.message)),
                },
                other => return Err(plan::unexpected("runs", other)),
            }
        }
    }
}
