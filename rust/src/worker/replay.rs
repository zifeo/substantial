//! One replay: a run machine driven by the workflow's code (I-23, I-24).

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;

use super::Event;
use super::agent::panicked;
use crate::clock::{Clock, Heartbeat, Inbox, Late, Ready, Tasks};
use crate::core::{Core, RunMachine};
use crate::error::{Continuation, Error, Failure, NEW_VERSION, Result, step_failed_end};
use crate::observe::{self, Metering, Observed};
use crate::proto::boundary::v1::{
    self as b, CommitKind, end, next::Next, op_result, outcome::Outcome, replay_end,
};
use crate::proto::layout::v1::{self as layout, event, value};
use crate::store::{self, Store};
use crate::workflow::executor::Executor;
use crate::workflow::scheduler::Shared;
use crate::workflow::{Context, LiveAttempt, Untyped, failure_of, json, with_attempt};

/// What the agent loop tells a replay.
#[derive(Debug)]
pub(crate) enum ToReplay {
    Committed {
        n: u64,
        won: bool,
        inbox: Vec<b::InboxMessage>,
        cost: Option<Box<layout::Cost>>,
    },
    Claimed,
    Stop,
}

/// What every replay of a worker shares.
#[derive(Clone)]
pub(crate) struct Host {
    pub(crate) core: Core,
    /// The namespace's store, which a workflow's send and result read a chain from.
    pub(crate) store: Arc<dyn Store>,
    pub(crate) clock: Clock,
    pub(crate) metering: Metering,
}

impl Host {
    /// Tells the observer; one that panics kills the worker, as a bug of the worker would.
    fn tell(&self, events: &Inbox<Event>, event: impl FnOnce() -> Observed) {
        if let Err(panic) = self.metering.tell(event) {
            events.push(Event::Panicked(panic));
        }
    }
}

/// Runs one replay of `chain` to its end, which it always reports, `Dropped` when it could not go
/// on. `step_timeout` is the workflow's, else the worker's; `due_at` the earliest due entry it
/// serves. Its requests to the agent name the chain, its logs the run.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay(
    host: Host,
    chain: String,
    workflow: Untyped,
    step_timeout: Duration,
    input: b::RunInput,
    due_at: Option<layout::Time>,
    events: Arc<Inbox<Event>>,
    inbox: Arc<Inbox<ToReplay>>,
) {
    let run = input.run.clone();
    let (started, now) = (host.clock.seconds(), host.clock.now());
    let mut committed = None;
    let driven = drive(
        &host,
        &chain,
        &workflow,
        step_timeout,
        input,
        &events,
        &inbox,
        &mut committed,
    );
    let end = match driven.await {
        Ok(end) => end,
        Err(e) => {
            tracing::error!(run, error = %e, "replay dropped");
            replay_end::End::Dropped(b::Empty {})
        }
    };
    let ended = match (&end, committed) {
        (replay_end::End::Dropped(_), _) => "dropped",
        (replay_end::End::Blocked(_), _) => "blocked",
        (_, Some(CommitKind::Suspend)) => "suspended",
        (_, Some(CommitKind::End)) => "ended",
        _ => "idle",
    };
    host.tell(&events, || {
        Observed::Replay(observe::Replay {
            workflow: workflow.name().to_owned(),
            delay: due_at.map(|due| (now.ms - due.ms) as f64 / 1000.0),
            seconds: host.clock.seconds() - started,
            end: ended,
        })
    });
    events.push(Event::Ended(chain, end));
}

/// An attempt's call, attempt, outcome and charges, or the panic of its step.
type Attempt = std::result::Result<(u64, u32, Outcome, Vec<layout::Amount>), String>;
type WorkflowFuture = BoxFuture<'static, Result<Vec<u8>>>;

/// Tells the observer of a step attempt once it ended: `abandoned` when dropped before.
struct Attempted {
    host: Host,
    events: Arc<Inbox<Event>>,
    workflow: String,
    step: String,
    attempt: u32,
    started: f64,
    outcome: &'static str,
}

impl Attempted {
    fn end(mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

// § one yp7fv5sa7.a@5n973
impl Drop for Attempted {
    fn drop(&mut self) {
        self.host.tell(&self.events, || {
            Observed::Attempt(observe::Attempt {
                workflow: std::mem::take(&mut self.workflow),
                step: std::mem::take(&mut self.step),
                attempt: self.attempt,
                seconds: self.host.clock.seconds() - self.started,
                outcome: self.outcome,
            })
        });
    }
}

/// How an attempt ended, as its observer is told.
fn attempt_outcome(outcome: &Outcome, attempt: u32, max_attempts: u32) -> &'static str {
    match outcome {
        Outcome::Value(_) => "ok",
        Outcome::Error(f) if f.r#type == "StepTimeout" => "timeout",
        Outcome::Error(f) if f.retryable && attempt < max_attempts => "retry",
        Outcome::Error(_) => "failed",
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    host: &Host,
    replayed: &str,
    workflow: &Untyped,
    step_timeout: Duration,
    input: b::RunInput,
    events: &Arc<Inbox<Event>>,
    inbox: &Arc<Inbox<ToReplay>>,
    committed: &mut Option<CommitKind>,
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
        &input,
        chain.clone(),
        input_json.clone(),
        step_timeout,
        workflow.heartbeat_timeout(),
    );
    // § g4kv2kj51@ztcvz
    let mut code: Option<WorkflowFuture> =
        Some(workflow.start(Context::new(shared.clone()), input_json));
    // § end g4kv2kj51
    let executor = Executor::new();
    // Aborted when dropped, which drops the outcomes of the attempts in flight.
    // § one v4n6ja42e.a@w3fh5
    let mut attempts = Tasks::default();
    let attempted: Arc<Inbox<Attempt>> = Arc::default();
    let told: Arc<dyn Ready> = inbox.clone();
    loop {
        // § k3zma9hjs@hr40c
        if let Some(error) = code_failure(&shared, &mut code) {
            return Ok(block(host, events, workflow, &shared, error).await);
        }
        if let Some(future) = code.as_mut()
            && let Poll::Ready(result) = executor.run(future.as_mut())
        {
            code = None;
            if shared.fatal().is_none()
                && !shared.diverged()
                && !shared.core_ended()
                && let Err(error) = end_run(&shared, result)?
            {
                return Ok(block(host, events, workflow, &shared, error).await);
            }
        }
        if shared.diverged() || shared.core_ended() {
            // Nothing more of the code is recorded: the core blocks a divergence at `next`, and
            // commits the end it made itself. Dropping the future and the attempts in flight
            // stops the code where it is.
            code = None;
            // § v4n6ja42e.a@39sfn
            attempts = Tasks::default();
            while attempted.pop().is_some() {}
            // § end v4n6ja42e.a
        }
        if let Some(error) = code_failure(&shared, &mut code) {
            return Ok(block(host, events, workflow, &shared, error).await);
        }
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
            let (call, attempt, outcome, charges) =
                done.map_err(|e| Error::Protocol(format!("step task: {e}")))?;
            shared.step_ended(call, attempt, outcome, charges)?;
        }
        let next = shared.next();
        // § awhhtbr5n.b@vt4n7
        if host.clock.checks_only() {
            let diverged = shared.divergence().or_else(|| match &next {
                Err(Error::NonDeterminism(m)) => Some(m.clone()),
                Ok(Next::Blocked(f)) => Some(f.message.clone()),
                _ => None,
            });
            let replaying = matches!(
                next,
                Ok(Next::Resolve(_) | Next::Deliver(_) | Next::ChildEnded(_)) | Err(_)
            );
            if diverged.is_some() || !replaying {
                host.clock.checked(&run, diverged);
                // Parked for good, so nothing past the history runs or is written: the tester
                // crashes the worker once it read the outcome.
                host.clock.wait(&[], None).await;
            }
        }
        // § end awhhtbr5n.b
        match next? {
            Next::Resolve(r) => shared.resolve(r)?,
            Next::Deliver(m) => shared.deliver(&m),
            Next::ChildEnded(child) => shared.child_ended(child),
            Next::Execute(e) => {
                let step = shared.step_body(e.call).ok_or_else(|| {
                    Error::Protocol(format!("execute of call {} without a step", e.call))
                })?;
                let timeout = e.timeout_ms.map(Duration::from_millis);
                let (clock, done) = (host.clock.clone(), attempted.clone());
                // § yp7fv5sa7.a@yprtf
                let observed = Attempted {
                    host: host.clone(),
                    events: events.clone(),
                    workflow: workflow.name().to_owned(),
                    step: step.name.clone(),
                    attempt: e.attempt,
                    started: host.clock.seconds(),
                    outcome: "abandoned",
                };
                // § end yp7fv5sa7.a
                let label = step.key.clone();
                let task = host
                    .clock
                    .spawn(format_args!("attempt of {label}"), async move {
                        let running = LiveAttempt::new(step.key.clone(), clock.clone());
                        let heartbeat = step.heartbeat.map(|timeout| Heartbeat {
                            timeout,
                            beat: running.beats(),
                        });
                        let body = with_attempt(running.clone(), (step.body)()).map(outcome);
                        let body = Box::pin(body);
                        let attempt =
                            clock.attempt(&step.name, &step.key, body, timeout, heartbeat);
                        let ended = match AssertUnwindSafe(attempt).catch_unwind().await {
                            Ok(Ok(outcome)) => outcome,
                            // § one v4n6ja42e@19j1s
                            Ok(Err(Late::Timeout)) => timed_out(timeout),
                            Ok(Err(Late::Silent)) => silent(step.heartbeat),
                            Err(panic) => return done.push(Err(panicked(&*panic))),
                        };
                        observed.end(attempt_outcome(&ended, e.attempt, step.max_attempts));
                        // § cvnf47vy8@zfjq5
                        done.push(Ok((e.call, e.attempt, ended, running.charges())));
                    });
                attempts.push(task);
            }
            Next::Commit(commit) => {
                let n = commit.segment.as_ref().map(|s| s.n);
                let ended = commit.segment.as_ref().and_then(failed_end);
                let kind = commit.kind();
                let near = commit.near_limits.clone();
                events.push(Event::Commit(replayed.to_owned(), Box::new(commit)));
                host.clock.wait(std::slice::from_ref(&told), None).await;
                match inbox.pop() {
                    Some(ToReplay::Committed {
                        n: acked,
                        won: true,
                        inbox,
                        cost,
                    }) => {
                        if n.is_some_and(|n| n != acked) {
                            return Err(Error::Protocol(format!(
                                "commit of segment {n:?} acknowledged as {acked}"
                            )));
                        }
                        *committed = Some(kind);
                        match cost {
                            Some(cost) => shared.arrived(inbox, *cost)?,
                            None if inbox.is_empty() => {}
                            None => {
                                return Err(Error::Protocol(
                                    "messages arrived without the chain's cost".into(),
                                ));
                            }
                        }
                        if let Some(ended) = ended {
                            tracing::warn!("run {run} {ended}");
                        }
                        for limit in near {
                            near_limit(host, events, workflow, &shared, limit);
                        }
                    }
                    Some(ToReplay::Committed { won: false, .. } | ToReplay::Stop) | None => {
                        return dropped;
                    }
                    Some(ToReplay::Claimed) => {
                        return Err(Error::Protocol("claimed during a commit".into()));
                    }
                }
            }
            Next::Claim(_) => {
                events.push(Event::Claim(replayed.to_owned()));
                host.clock.wait(std::slice::from_ref(&told), None).await;
                match inbox.pop() {
                    Some(ToReplay::Claimed) => shared.claimed()?,
                    Some(ToReplay::Stop) | None => return dropped,
                    Some(other) => {
                        return Err(Error::Protocol(format!("{other:?} while claiming")));
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
            // § one d9818h4m4@yj88p
            Next::Idle(idle) => return Ok(replay_end::End::Idle(idle)),
            Next::Blocked(error) => {
                let error = error.try_into()?;
                return Ok(block(host, events, workflow, &shared, error).await);
            }
            Next::Store(op) => {
                let (store, events) = (host.store.clone(), events.clone());
                let read = host.clock.spawn(format_args!("read of {run}"), async move {
                    let performed = AssertUnwindSafe(store::perform(&*store, op));
                    let read = performed.catch_unwind().await;
                    // A panic there, an observer's, is a bug that kills the worker.
                    read.map_err(|panic| events.push(Event::Panicked(panicked(&*panic))))
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
                            "{other:?} during a store operation of a send or a result"
                        )));
                    }
                }
                let Ok(result) = read
                    .take()
                    .ok_or_else(|| Error::Protocol("a read without its result".into()))?
                else {
                    return dropped;
                };
                if let Some(op_result::Result::Failed(f)) = result.result {
                    return Err(Error::StoreError(f.message));
                }
                shared.stored(result)?;
            }
        }
    }
}

/// The outcome of an attempt as the run machine takes it; a value over the limit fails it.
fn outcome(result: Result<Vec<u8>, Failure>) -> Outcome {
    // § h8wg39cct.b@gns87
    match result.and_then(|bytes| json(bytes).map_err(too_large)) {
        Ok(value) => Outcome::Value(value),
        Err(f) => Outcome::Error(f.into()),
    }
}

fn silent(heartbeat: Option<Duration>) -> Outcome {
    // § vvp37rp40@g2ajg
    outcome(Err(Failure {
        kind: "StepTimeout".into(),
        message: format!("no heartbeat within {:?}", heartbeat.unwrap_or_default()),
        retryable: true,
        details: None,
    }))
}

fn timed_out(timeout: Option<Duration>) -> Outcome {
    // § aym52h8ch@7stkz
    outcome(Err(Failure {
        kind: "StepTimeout".into(),
        message: format!("attempt past {:?}", timeout.unwrap_or_default()),
        retryable: true,
        details: None,
    }))
}

/// A failure the code could not see, a handler's, which blocks the run: the code is dropped.
fn code_failure(shared: &Shared, code: &mut Option<WorkflowFuture>) -> Option<Failure> {
    let failure = shared.take_code_failure()?;
    code.take().map(|_| failure)
}

/// Stops the code where it is and reports the replay blocked with `error`: the run records
/// nothing more, and is tried again after a backoff.
async fn block(
    host: &Host,
    events: &Inbox<Event>,
    workflow: &Untyped,
    shared: &Shared,
    mut error: Failure,
) -> replay_end::End {
    // § dxgjekysz.e@nd7wa
    if error.kind == "NonDeterminism" && !error.message.ends_with(NEW_VERSION) {
        error.message = format!("{}; {NEW_VERSION}", error.message);
    }
    // § awhhtbr5n.b@khdxb
    if host.clock.checks_only() {
        host.clock.checked(shared.run(), Some(error.to_string()));
        host.clock.wait(&[], None).await;
    }
    // § end awhhtbr5n.b
    // § yp7fv5sa7.c@mc85g
    tracing::warn!("run {} blocked: {error}", shared.run());
    let (workflow, run) = (workflow.name().to_owned(), shared.run().to_owned());
    let end = replay_end::End::Blocked(error.clone().into());
    host.tell(events, || {
        Observed::Blocked(observe::Blocked {
            workflow,
            run,
            error,
        })
    });
    // § end yp7fv5sa7.c
    end
}

/// Warns that a commit took the run past the warning share of `limit`.
fn near_limit(
    host: &Host,
    events: &Inbox<Event>,
    workflow: &Untyped,
    shared: &Shared,
    limit: b::LimitUse,
) {
    // § yp7fv5sa7.c@zgk53
    tracing::warn!(
        "run {} near its {} limit: {} of {}",
        shared.run(),
        limit.limit,
        limit.used,
        limit.max
    );
    host.tell(events, || {
        Observed::NearLimit(observe::NearLimit {
            workflow: workflow.name().to_owned(),
            run: shared.run().to_owned(),
            limit: limit.limit,
            used: limit.used,
            max: limit.max,
        })
    });
}

fn input_bytes(input: Option<layout::Value>) -> Result<Vec<u8>> {
    // § e1dc4dar4.a@c7gxc
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
            let error = f.error.as_ref()?;
            Some(format!("failed: {}: {}", error.r#type, error.message))
        }
        Some(event::Kind::Cancelled(c)) => Some(format!("cancelled: {}", c.reason)),
        _ => None,
    })
}

/// A step value over the limit fails the step at once: another attempt would return it again.
fn too_large(e: Error) -> Failure {
    // § h8wg39cct.b@335x1
    Failure {
        kind: "ValueTooLarge".into(),
        message: e.to_string(),
        retryable: false,
        details: None,
    }
}

/// Records the end of the code: its value, its cancellation, its continuation, or the failure of
/// an error that ends a run; any other error, a value over the limit included, is the failure that
/// blocks the run, recording nothing.
fn end_run(shared: &Shared, result: Result<Vec<u8>>) -> Result<Result<(), Failure>> {
    // § dxgjekysz@99zw7
    let end = match result {
        Ok(bytes) => match completed(bytes) {
            Ok(end) => end,
            Err(e) => return Ok(Err(failure_of(&e))),
        },
        Err(Error::Cancelled(reason)) => end::End::Cancelled(layout::Cancelled { reason }),
        Err(Error::ContinuedAsNew(c)) => match continued(c) {
            Ok(end) => end,
            Err(e) => return Ok(Err(failure_of(&e))),
        },
        Err(e) if ends(&e) => failed(shared, &e),
        Err(e) => return Ok(Err(failure_of(&e))),
    };
    shared.call(b::call::Call::End(b::End { end: Some(end) }))?;
    Ok(Ok(()))
}

/// Whether an error that leaves the code ends its run failed: a recorded outcome of a verb, the
/// same at every replay, or the code's own `Fail`.
fn ends(e: &Error) -> bool {
    // § dxgjekysz.d@x5917
    matches!(
        e,
        Error::Fail(_)
            | Error::Failed(_)
            | Error::StepFailed { .. }
            | Error::BudgetExceeded { .. }
            | Error::RolledBack(_)
            | Error::UndoFailed { .. }
            | Error::HistoryTooLarge(_)
            | Error::RunFailed { .. }
            | Error::WaitTimeout
            | Error::RunNotFound { .. }
            | Error::RunEnded { .. }
    )
}

fn completed(bytes: Vec<u8>) -> Result<end::End> {
    // § h8wg39cct.e@pwgt7
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
        highest: c.highest,
    }))
}

fn failed(shared: &Shared, e: &Error) -> end::End {
    // § r3sd261h4.b@pv6jz
    let error = match e {
        Error::StepFailed { error, cause } => match shared.step_failure(error, cause) {
            Some(details) => layout::Failure {
                details: Some(details),
                ..error.clone().into()
            },
            None => step_failed_end(error, cause),
        },
        other => failure_of(other).into(),
    };
    end::End::Failed(layout::Failed { error: Some(error) })
}
