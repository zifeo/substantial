//! One replay: a run machine driven by the workflow's code (I-23, I-24).

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;

use super::Event;
use super::agent::panicked;
use crate::clock::{Clock, Inbox, Ready, Tasks};
use crate::core::{Core, RunMachine};
use crate::error::{Continuation, Error, Failure, Result, step_failed_end};
use crate::proto::boundary::v1::{
    self as b, end, next::Next, op_result, outcome::Outcome, replay_end,
};
use crate::proto::layout::v1::{self as layout, event, value};
use crate::store::{self, Store};
use crate::workflow::executor::Executor;
use crate::workflow::scheduler::Shared;
use crate::workflow::{Context, Untyped, failure_of, json, with_step_key};

/// What the agent loop tells a replay.
#[derive(Debug)]
pub(crate) enum ToReplay {
    Committed {
        n: u64,
        won: bool,
        inbox: Vec<b::InboxMessage>,
    },
    Stop,
}

/// What every replay of a worker shares.
#[derive(Clone)]
pub(crate) struct Host {
    pub(crate) core: Core,
    /// The namespace's store, which a workflow's send reads its chain from.
    pub(crate) store: Arc<dyn Store>,
    pub(crate) clock: Clock,
}

/// Runs one replay to its end, which it always reports, `Dropped` when it could not go on.
/// `step_timeout` is the workflow's, else the worker's.
pub(crate) async fn replay(
    host: Host,
    workflow: Untyped,
    step_timeout: Duration,
    input: b::RunInput,
    events: Arc<Inbox<Event>>,
    inbox: Arc<Inbox<ToReplay>>,
) {
    let run = input.run.clone();
    let end = match drive(&host, &workflow, step_timeout, input, &events, &inbox).await {
        Ok(end) => end,
        Err(e) => {
            tracing::error!(run, error = %e, "replay dropped");
            replay_end::End::Dropped(b::Empty {})
        }
    };
    events.push(Event::Ended(run, end));
}

/// An attempt's call, attempt and outcome, or the panic of its step.
type Attempt = std::result::Result<(u64, u32, Outcome), String>;
type WorkflowFuture = BoxFuture<'static, Result<Vec<u8>>>;

async fn drive(
    host: &Host,
    workflow: &Untyped,
    step_timeout: Duration,
    input: b::RunInput,
    events: &Inbox<Event>,
    inbox: &Arc<Inbox<ToReplay>>,
) -> Result<replay_end::End> {
    let dropped = Ok(replay_end::End::Dropped(b::Empty {}));
    let run = input.run.clone();
    let start = input
        .start
        .clone()
        .ok_or_else(|| Error::Protocol(format!("replay of {run} without start")))?;
    let input_json = input_bytes(start.input)?;
    let chain = start.chain;
    let shared = Shared::new(
        RunMachine::new(&host.core, &input)?,
        host.clock.clone(),
        run.clone(),
        chain.clone(),
        input_json.clone(),
        !input.history.is_empty(),
        step_timeout,
    );
    let mut code: Option<WorkflowFuture> =
        Some(workflow.start(Context::new(shared.clone()), input_json));
    let executor = Executor::new();
    // Aborted when dropped, which drops the outcomes of the attempts in flight.
    let mut attempts = Tasks::default();
    let attempted: Arc<Inbox<Attempt>> = Arc::default();
    let told: Arc<dyn Ready> = inbox.clone();
    loop {
        fail_code(&shared, &mut code)?;
        if let Some(future) = code.as_mut()
            && let Poll::Ready(result) = executor.run(future.as_mut())
        {
            code = None;
            if shared.fatal().is_none() && !shared.diverged() {
                end_run(&shared, result)?;
            }
        }
        if shared.diverged() {
            // The core recorded the failure, which `next` commits. Dropping the future and the
            // attempts in flight stops the code where it is.
            code = None;
            attempts = Tasks::default();
            while attempted.pop().is_some() {}
        }
        fail_code(&shared, &mut code)?;
        if let Some(e) = shared.fatal() {
            return Err(Error::Protocol(format!("a call of the code failed: {e}")));
        }
        match inbox.pop() {
            None => {}
            Some(ToReplay::Stop) => return dropped,
            Some(other) => return Err(Error::Protocol(format!("{other:?} without a commit"))),
        }
        shared.evaluate_waits()?;
        while let Some(done) = attempted.pop() {
            let (call, attempt, outcome) =
                done.map_err(|e| Error::Protocol(format!("step task: {e}")))?;
            shared.step_ended(call, attempt, outcome)?;
        }
        match shared.next()? {
            Next::Resolve(r) => shared.resolve(r)?,
            Next::Deliver(m) => shared.deliver(&m),
            Next::ChildEnded(child) => shared.child_ended(child),
            Next::Execute(e) => {
                let (body, name, key) = shared.step_body(e.call).ok_or_else(|| {
                    Error::Protocol(format!("execute of call {} without a step", e.call))
                })?;
                let timeout = e.timeout_ms.map(Duration::from_millis);
                let (clock, done, label) = (host.clock.clone(), attempted.clone(), key.clone());
                let task = host
                    .clock
                    .spawn(format_args!("attempt of {label}"), async move {
                        let attempt = with_step_key(key.clone(), body()).map(outcome);
                        let attempt = clock.attempt(&name, &key, Box::pin(attempt), timeout);
                        let ended = match AssertUnwindSafe(attempt).catch_unwind().await {
                            Ok(Some(outcome)) => Ok((e.call, e.attempt, outcome)),
                            Ok(None) => Ok((e.call, e.attempt, timed_out(timeout))),
                            Err(panic) => Err(panicked(&*panic)),
                        };
                        done.push(ended);
                    });
                attempts.push(task);
            }
            Next::Commit(commit) => {
                let n = commit.segment.as_ref().map(|s| s.n);
                let ended = commit.segment.as_ref().and_then(failed_end);
                events.push(Event::Commit(run.clone(), commit));
                host.clock.wait(std::slice::from_ref(&told), None).await;
                match inbox.pop() {
                    Some(ToReplay::Committed {
                        n: acked,
                        won: true,
                        inbox,
                    }) => {
                        if n.is_some_and(|n| n != acked) {
                            return Err(Error::Protocol(format!(
                                "commit of segment {n:?} acknowledged as {acked}"
                            )));
                        }
                        if !inbox.is_empty() {
                            shared.arrived(inbox)?;
                        }
                        if let Some(ended) = ended {
                            tracing::warn!("run {chain} {ended}");
                        }
                    }
                    Some(ToReplay::Committed { won: false, .. } | ToReplay::Stop) | None => {
                        return dropped;
                    }
                }
            }
            Next::WaitSteps(wait) => {
                if attempts.is_empty() && attempted.is_empty() {
                    return Err(Error::Protocol("wait_steps with no step in flight".into()));
                }
                let signals = [attempted.clone() as Arc<dyn Ready>, told.clone()];
                host.clock.wait(&signals, wait.until).await;
            }
            Next::Idle(idle) => return Ok(replay_end::End::Idle(idle)),
            Next::Store(op) => {
                let store = host.store.clone();
                let read = host.clock.spawn(format_args!("read of {run}"), async move {
                    store::perform(&*store, op).await
                });
                host.clock.wait(&[read.done(), told.clone()], None).await;
                match inbox.pop() {
                    None => {}
                    Some(ToReplay::Stop) => {
                        read.abort();
                        return dropped;
                    }
                    Some(other) => {
                        return Err(Error::Protocol(format!(
                            "{other:?} while reading for a send"
                        )));
                    }
                }
                let result = read
                    .take()
                    .ok_or_else(|| Error::Protocol("a read without its result".into()))?;
                if let op_result::Result::Failed(f) = result {
                    return Err(Error::StoreError(f.message));
                }
                shared.stored(result)?;
            }
        }
    }
}

/// The outcome of an attempt as the run machine takes it; a value over the limit fails it.
fn outcome(result: Result<Vec<u8>, Failure>) -> Outcome {
    match result.and_then(|bytes| json(bytes).map_err(too_large)) {
        Ok(value) => Outcome::Value(value),
        Err(f) => Outcome::Error(f.into()),
    }
}

fn timed_out(timeout: Option<Duration>) -> Outcome {
    outcome(Err(Failure {
        kind: "StepTimeout".into(),
        message: format!("attempt past {:?}", timeout.unwrap_or_default()),
        retryable: true,
    }))
}

/// Ends the run with a failure the code could not see, a handler's, dropping the code.
fn fail_code(shared: &Shared, code: &mut Option<WorkflowFuture>) -> Result<()> {
    if let Some(failure) = shared.take_code_failure()
        && code.take().is_some()
    {
        end_run(shared, Err(Error::Failed(failure)))?;
    }
    Ok(())
}

fn input_bytes(input: Option<layout::Value>) -> Result<Vec<u8>> {
    match input.and_then(|v| v.kind) {
        None => Ok(b"{}".to_vec()),
        Some(value::Kind::Json(bytes)) => Ok(bytes),
        Some(other) => Err(Error::Protocol(format!(
            "input other than inline JSON is not supported yet: {other:?}"
        ))),
    }
}

/// How a segment ends its run, if it ends it failed or cancelled: `failed: <kind>: <message>`
/// or `cancelled: <reason>`.
fn failed_end(segment: &layout::Segment) -> Option<String> {
    segment.events.iter().find_map(|e| match &e.kind {
        Some(event::Kind::Failed(f)) => {
            let error = f.error.clone().map(Failure::from)?;
            Some(format!("failed: {error}"))
        }
        Some(event::Kind::Cancelled(c)) => Some(format!("cancelled: {}", c.reason)),
        _ => None,
    })
}

/// A step value over the limit fails the step at once: another attempt would return it again.
fn too_large(e: Error) -> Failure {
    Failure {
        kind: "ValueTooLarge".into(),
        message: e.to_string(),
        retryable: false,
    }
}

/// Records the end of the code: its value, its failure, its cancellation, or its continuation;
/// a value over the limit fails the run instead.
fn end_run(shared: &Shared, result: Result<Vec<u8>>) -> Result<()> {
    let end = match result.and_then(completed) {
        Ok(end) => end,
        Err(Error::Cancelled(reason)) => end::End::Cancelled(layout::Cancelled { reason }),
        Err(Error::ContinuedAsNew(c)) => continued(c).unwrap_or_else(|e| failed(&e)),
        Err(e) => failed(&e),
    };
    shared.call(b::call::Call::End(b::End { end: Some(end) }))?;
    Ok(())
}

fn completed(bytes: Vec<u8>) -> Result<end::End> {
    Ok(end::End::Completed(layout::Completed {
        value: Some(json(bytes)?),
    }))
}

fn continued(c: Continuation) -> Result<end::End> {
    Ok(end::End::Continued(end::Continued {
        workflow: c.workflow,
        input: Some(json(c.input)?),
        state: c.state.map(json).transpose()?,
        version: c.version,
    }))
}

fn failed(e: &Error) -> end::End {
    let error = match e {
        Error::StepFailed { error, cause } => step_failed_end(error, cause),
        other => failure_of(other).into(),
    };
    end::End::Failed(layout::Failed { error: Some(error) })
}
