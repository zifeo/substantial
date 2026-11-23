use prost::Message;

use super::module::{Core, Kind};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{
    self as b, agent_request, agent_response, plan_request, plan_response, run_request,
    run_response,
};
use crate::proto::layout::v1::{Amount, Cost, Time};

/// A machine of the core, dropped with `sub_drop` when this handle goes.
struct Handle {
    core: Core,
    id: u32,
}

impl Handle {
    /// A machine of `input`; an input the core refuses fails here, not at the first call.
    fn new(core: &Core, kind: Kind, input: &impl Message) -> Result<Self> {
        let id = core.new_machine(kind, &input.encode_to_vec())?;
        let handle = Self {
            core: core.clone(),
            id,
        };
        if let Some(bytes) = core.refused(id)? {
            // Every response has its CoreError as field 1: any of them decodes it.
            let refused = b::AgentResponse::decode(bytes.as_slice())
                .map_err(|e| Error::Protocol(format!("undecodable refusal: {e}")))?;
            return match refused.response {
                Some(agent_response::Response::Error(e)) => Err(e.into()),
                other => Err(unexpected("a refusal", other)),
            };
        }
        Ok(handle)
    }

    fn call<R: Message + Default>(&self, request: &impl Message) -> Result<R> {
        let bytes = self.core.call(self.id, &request.encode_to_vec())?;
        R::decode(bytes.as_slice())
            .map_err(|e| Error::Protocol(format!("undecodable response: {e}")))
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.core.drop_machine(self.id);
    }
}

fn unexpected(what: &str, got: impl std::fmt::Debug) -> Error {
    // § b80k2s0e4.a@zrjhr
    Error::Protocol(format!("expected {what}, got {got:?}"))
}

/// The run machine of one replay.
pub struct RunMachine(Handle);

impl RunMachine {
    pub fn new(core: &Core, input: &b::RunInput) -> Result<Self> {
        Handle::new(core, Kind::Run, input).map(Self)
    }

    fn request(&self, request: run_request::Request) -> Result<run_response::Response> {
        let response: b::RunResponse = self.0.call(&b::RunRequest {
            request: Some(request),
        })?;
        match response.response {
            Some(run_response::Response::Error(e)) => Err(e.into()),
            Some(r) => Ok(r),
            None => Err(Error::Protocol("empty run response".into())),
        }
    }

    fn done(&self, request: run_request::Request) -> Result<()> {
        match self.request(request)? {
            run_response::Response::Done(_) => Ok(()),
            other => Err(unexpected("done", other)),
        }
    }

    pub fn call(&self, call: b::call::Call, now: Time) -> Result<b::Called> {
        let request = run_request::MakeCall {
            call: Some(b::Call { call: Some(call) }),
            now: Some(now),
        };
        match self.request(run_request::Request::Call(request))? {
            run_response::Response::Called(called) => Ok(called),
            other => Err(unexpected("called", other)),
        }
    }

    pub fn step_ended(
        &self,
        call: u64,
        attempt: u32,
        outcome: b::outcome::Outcome,
        charges: Vec<Amount>,
        now: Time,
    ) -> Result<()> {
        let outcome = Some(b::Outcome {
            outcome: Some(outcome),
        });
        self.done(run_request::Request::StepEnded(run_request::StepEnded {
            call,
            attempt,
            outcome,
            now: Some(now),
            charges,
        }))
    }

    pub fn satisfied(&self, call: u64) -> Result<()> {
        self.done(run_request::Request::Satisfied(call))
    }

    pub fn arrived(&self, messages: Vec<b::InboxMessage>, cost: Cost) -> Result<()> {
        self.done(run_request::Request::Arrived(run_request::Arrived {
            messages,
            cost: Some(cost),
        }))
    }

    /// The answer to `Next::Claim` once the agent holds the run's claim.
    pub fn claimed(&self) -> Result<()> {
        self.done(run_request::Request::Claimed(b::Empty {}))
    }

    /// The answer to `Next::Store`, an operation of a workflow's send or result.
    pub fn stored(&self, result: b::OpResult) -> Result<()> {
        self.done(run_request::Request::Stored(result))
    }

    pub fn next(&self, now: Time) -> Result<b::Next> {
        match self.request(run_request::Request::Next(now))? {
            run_response::Response::Next(next) => Ok(next),
            other => Err(unexpected("next", other)),
        }
    }
}

/// The agent machine of one worker.
pub struct AgentMachine(Handle);

impl AgentMachine {
    pub fn new(core: &Core, config: &b::AgentConfig) -> Result<Self> {
        Handle::new(core, Kind::Agent, config).map(Self)
    }

    fn request(&self, request: agent_request::Request) -> Result<agent_response::Response> {
        let response: b::AgentResponse = self.0.call(&b::AgentRequest {
            request: Some(request),
        })?;
        match response.response {
            Some(agent_response::Response::Error(e)) => Err(e.into()),
            Some(r) => Ok(r),
            None => Err(Error::Protocol("empty agent response".into())),
        }
    }

    fn done(&self, request: agent_request::Request) -> Result<()> {
        match self.request(request)? {
            agent_response::Response::Done(_) => Ok(()),
            other => Err(unexpected("done", other)),
        }
    }

    pub fn next(&self, now: Time) -> Result<b::agent_command::Command> {
        match self.request(agent_request::Request::Next(now))? {
            agent_response::Response::Command(b::AgentCommand { command: Some(c) }) => Ok(c),
            other => Err(unexpected("command", other)),
        }
    }

    pub fn feed(&self, id: u64, result: b::OpResult) -> Result<()> {
        self.done(agent_request::Request::Feed(agent_request::Feed {
            id,
            result: Some(result),
        }))
    }

    pub fn commit(&self, run: String, commit: b::Commit) -> Result<()> {
        self.done(agent_request::Request::Commit(agent_request::RunCommit {
            run,
            commit: Some(commit),
        }))
    }

    pub fn claim(&self, run: String) -> Result<()> {
        self.done(agent_request::Request::Claim(run))
    }

    pub fn replay_ended(&self, run: String, end: b::replay_end::End) -> Result<()> {
        let end = Some(b::ReplayEnd { end: Some(end) });
        self.done(agent_request::Request::ReplayEnded(
            agent_request::ReplayEnded { run, end },
        ))
    }

    pub fn wake(&self) -> Result<()> {
        self.done(agent_request::Request::Wake(b::Empty {}))
    }

    pub fn wake_workflow(&self, workflow: String) -> Result<()> {
        self.done(agent_request::Request::WakeWorkflow(workflow))
    }

    pub fn shutdown(&self) -> Result<()> {
        self.done(agent_request::Request::Shutdown(b::Empty {}))
    }

    pub fn drain(&self, until: Time) -> Result<()> {
        self.done(agent_request::Request::Drain(until))
    }
}

/// The plan machine of one client action.
pub struct PlanMachine(Handle);

impl PlanMachine {
    pub fn new(core: &Core, input: &b::PlanInput) -> Result<Self> {
        Handle::new(core, Kind::Plan, input).map(Self)
    }

    fn request(&self, request: plan_request::Request) -> Result<plan_response::Response> {
        let response: b::PlanResponse = self.0.call(&b::PlanRequest {
            request: Some(request),
        })?;
        match response.response {
            Some(plan_response::Response::Error(e)) => Err(e.into()),
            Some(r) => Ok(r),
            None => Err(Error::Protocol("empty plan response".into())),
        }
    }

    pub fn next(&self) -> Result<b::plan_step::Step> {
        match self.request(plan_request::Request::Next(b::Empty {}))? {
            plan_response::Response::Step(b::PlanStep { step: Some(step) }) => Ok(step),
            other => Err(unexpected("step", other)),
        }
    }

    pub fn feed(&self, result: b::OpResult) -> Result<()> {
        match self.request(plan_request::Request::Feed(result))? {
            plan_response::Response::Done(_) => Ok(()),
            other => Err(unexpected("done", other)),
        }
    }
}
