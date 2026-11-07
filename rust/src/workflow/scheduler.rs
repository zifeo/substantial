//! The Rust scheduler of I-24: the workflow future is polled by the host, which makes the code's
//! calls in order, runs it to quiescence, and only then asks the run machine for `next`.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};

use futures::future::BoxFuture;

use crate::clock::Clock;
use crate::core::RunMachine;
use crate::error::{Error, Failure, NEW_VERSION, Result};
use crate::proto::boundary::v1::{self as b, ErrorKind, called::Called, next::Next, resolution};
use crate::proto::layout::v1::{self as layout, Time};

/// One attempt of a step: its JSON value, or its failure.
pub(crate) type StepBody =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Vec<u8>, Failure>> + Send + Sync>;
/// An undo, given the value of the step it undoes, as the body of its step.
pub(crate) type Undo = Arc<dyn Fn(Option<layout::Value>) -> StepBody + Send + Sync>;
/// An undo with its step's retry and timeout in milliseconds.
pub(crate) type UndoSpec = (Undo, Option<b::RetryPolicy>, u64);
/// What a step call runs: its attempts, and its undo.
pub(crate) struct StepSpec {
    pub(crate) body: StepBody,
    pub(crate) undo: Option<UndoSpec>,
}
pub(crate) type Handler = Box<dyn FnMut(&b::Message) -> Result<(), Failure> + Send>;
type Predicate = Box<dyn Fn() -> bool + Send>;

#[derive(Default)]
struct Slot {
    result: Option<Result<Option<layout::Value>>>,
    waker: Option<Waker>,
}

struct State {
    machine: RunMachine,
    slots: HashMap<u64, Slot>,
    /// The steps pending, with their name and step key.
    steps: HashMap<u64, (StepBody, String, String)>,
    /// The undos of every step call of the replay, by call.
    undos: HashMap<u64, UndoSpec>,
    /// The children whose end the core consumed, by run.
    ended: HashSet<String>,
    handlers: HashMap<String, Vec<Handler>>,
    waits: Vec<(u64, Predicate)>,
    /// A core error met by a call of the code: the replay cannot go on.
    fatal: Option<String>,
    /// A call the history does not answer: the core recorded the run's failure, so the code stops
    /// and every later call fails the same way.
    diverged: Option<String>,
    /// A failure outside the code's reach, a handler's: the run fails with it.
    code_failure: Option<Failure>,
}

/// What the code and the replay driver share for one replay.
pub(crate) struct Shared {
    state: Mutex<State>,
    clock: Clock,
    run: String,
    chain: String,
    input: Vec<u8>,
    /// The timeout of the steps and undos that set none: the workflow's, else the worker's.
    step_timeout: std::time::Duration,
    /// Whether the replay walked the whole recorded history, so that the code's logs are written
    /// (I-14): set from each answer of the run machine.
    live: AtomicBool,
}

impl Shared {
    pub(crate) fn new(
        machine: RunMachine,
        clock: Clock,
        run: String,
        chain: String,
        input: Vec<u8>,
        recorded: bool,
        step_timeout: std::time::Duration,
    ) -> Arc<Self> {
        let state = State {
            machine,
            slots: HashMap::new(),
            steps: HashMap::new(),
            undos: HashMap::new(),
            ended: HashSet::new(),
            handlers: HashMap::new(),
            waits: Vec::new(),
            fatal: None,
            diverged: None,
            code_failure: None,
        };
        Arc::new(Self {
            state: Mutex::new(state),
            clock,
            run,
            chain,
            input,
            step_timeout,
            live: AtomicBool::new(!recorded),
        })
    }

    pub(crate) fn run(&self) -> &str {
        &self.run
    }

    pub(crate) fn chain(&self) -> &str {
        &self.chain
    }

    pub(crate) fn step_timeout(&self) -> std::time::Duration {
        self.step_timeout
    }

    pub(crate) fn live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }

    /// The run's input, which a continuation of its loop starts the next run with.
    pub(crate) fn input(&self) -> Vec<u8> {
        self.input.clone()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("the scheduler never panics while locked")
    }

    pub(crate) fn now(&self) -> Time {
        self.clock.now()
    }

    pub(crate) fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Makes a call of the code. `INVALID` for a call carrying the code's own names or values is
    /// the code's argument and goes back to it; any other core error is fatal to the replay.
    pub(crate) fn call(&self, call: b::call::Call) -> Result<Called> {
        self.call_full(call).map(|(called, _)| called)
    }

    /// As `call`, with the occurrence the core gives a step call.
    fn call_full(&self, call: b::call::Call) -> Result<(Called, Option<u64>)> {
        use b::call::Call;
        let named = matches!(
            call,
            Call::Step(_)
                | Call::Receive(_)
                | Call::Bind(_)
                | Call::Start(_)
                | Call::ChildResult(_)
                | Call::Send(_)
                | Call::Publish(_)
                | Call::Iterate(_)
        );
        let step = matches!(call, Call::Step(_));
        let mut state = self.lock();
        if let Some(message) = &state.diverged {
            return Err(Error::NonDeterminism(message.clone()));
        }
        let answer =
            state
                .machine
                .call(call, self.now())
                .and_then(|a| match (a.called, a.occurrence) {
                    (None, _) => Err(Error::Protocol("an empty answer".into())),
                    (Some(_), None) if step => {
                        Err(Error::Protocol("a step without occurrence".into()))
                    }
                    (Some(called), occurrence) => Ok((called, occurrence, a.live)),
                });
        match answer {
            Ok((called, occurrence, live)) => {
                self.live.store(live, Ordering::Relaxed);
                Ok((called, occurrence))
            }
            Err(Error::Core {
                kind: ErrorKind::Invalid,
                message,
            }) if named => Err(Error::InvalidArgument(message)),
            Err(e @ Error::ValueTooLarge { .. }) if named => Err(e),
            Err(Error::NonDeterminism(message)) => {
                let message = format!("{message}; {NEW_VERSION}");
                tracing::warn!(run = self.run, "{message}");
                state.diverged = Some(message.clone());
                state.waits.clear();
                Err(Error::NonDeterminism(message))
            }
            Err(e) => {
                state.fatal.get_or_insert_with(|| e.to_string());
                Err(e)
            }
        }
    }

    /// Makes a step call: a pending one keeps its body, its undo and its step key.
    fn call_step(&self, step: b::call::Step, spec: StepSpec) -> Result<Called> {
        let name = step.name.clone();
        let (called, occurrence) = self.call_full(b::call::Call::Step(step))?;
        let occurrence = occurrence.expect("call_full checks that a step has its occurrence");
        if let Called::Pending(id) = called {
            let key = format!("{}/{name}/{occurrence}", self.run);
            let mut state = self.lock();
            state.steps.insert(id, (spec.body, name, key));
            if let Some(undo) = spec.undo {
                state.undos.insert(id, undo);
            }
        }
        Ok(called)
    }

    pub(crate) fn child_ended(&self, run: String) {
        self.lock().ended.insert(run);
    }

    pub(crate) fn has_ended(&self, run: &str) -> bool {
        self.lock().ended.contains(run)
    }

    pub(crate) fn undo(&self, call: u64) -> Option<UndoSpec> {
        self.lock().undos.get(&call).cloned()
    }

    pub(crate) fn fatal(&self) -> Option<String> {
        self.lock().fatal.clone()
    }

    /// Whether a call of the code diverged from the history, which ended the run (I-5).
    pub(crate) fn diverged(&self) -> bool {
        self.lock().diverged.is_some()
    }

    pub(crate) fn fail_code(&self, f: Failure) {
        self.lock().code_failure.get_or_insert(f);
    }

    pub(crate) fn take_code_failure(&self) -> Option<Failure> {
        self.lock().code_failure.take()
    }

    pub(crate) fn add_handler(&self, name: String, handler: Handler) {
        self.lock().handlers.entry(name).or_default().push(handler);
    }

    pub(crate) fn add_wait(&self, call: u64, predicate: Predicate) {
        self.lock().waits.push((call, predicate));
    }

    pub(crate) fn step_body(&self, call: u64) -> Option<(StepBody, String, String)> {
        self.lock().steps.get(&call).cloned()
    }

    /// Hands a message to the bound handlers of its name, in the order they were bound.
    ///
    /// Handlers run unlocked, so that one may call the context.
    /// A handler that cannot take the message fails the run.
    pub(crate) fn deliver(&self, message: &b::Message) {
        let mut handlers = self
            .lock()
            .handlers
            .remove(&message.name)
            .unwrap_or_default();
        let result = handlers.iter_mut().try_for_each(|h| h(message));
        let mut state = self.lock();
        let bound_meanwhile = state.handlers.remove(&message.name).unwrap_or_default();
        handlers.extend(bound_meanwhile);
        state.handlers.insert(message.name.clone(), handlers);
        if let Err(f) = result {
            state.code_failure.get_or_insert(f);
        }
    }

    /// Tells the run machine of every wait whose predicate now holds.
    pub(crate) fn evaluate_waits(&self) -> Result<()> {
        let waits = std::mem::take(&mut self.lock().waits);
        let (held, pending): (Vec<_>, Vec<_>) = waits.into_iter().partition(|(_, p)| p());
        let mut state = self.lock();
        state.waits.extend(pending);
        held.into_iter()
            .try_for_each(|(call, _)| state.machine.satisfied(call))
    }

    pub(crate) fn resolve(&self, r: b::next::Resolve) -> Result<()> {
        let result = match r.resolution.and_then(|r| r.resolution) {
            Some(resolution::Resolution::Value(v)) => Ok(Some(v)),
            Some(resolution::Resolution::Error(f)) => Err(Error::from(f)),
            Some(resolution::Resolution::Cancelled(reason)) => Err(Error::Cancelled(reason)),
            None => {
                return Err(Error::Protocol(format!(
                    "empty resolution of call {}",
                    r.call
                )));
            }
        };
        let mut state = self.lock();
        state.steps.remove(&r.call);
        state.waits.retain(|(call, _)| *call != r.call);
        let slot = state
            .slots
            .get_mut(&r.call)
            .ok_or_else(|| Error::Protocol(format!("resolve of unknown call {}", r.call)))?;
        slot.result = Some(result);
        if let Some(waker) = slot.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    pub(crate) fn step_ended(
        &self,
        call: u64,
        attempt: u32,
        outcome: b::outcome::Outcome,
    ) -> Result<()> {
        self.lock()
            .machine
            .step_ended(call, attempt, outcome, self.now())
    }

    pub(crate) fn stored(&self, result: b::op_result::Result) -> Result<()> {
        self.lock().machine.stored(result)
    }

    pub(crate) fn arrived(&self, messages: Vec<b::InboxMessage>) -> Result<()> {
        self.lock().machine.arrived(messages)
    }

    pub(crate) fn next(&self) -> Result<Next> {
        let next = self.lock().machine.next(self.now())?;
        self.live.store(next.live, Ordering::Relaxed);
        next.next
            .ok_or_else(|| Error::Protocol("empty next".into()))
    }
}

/// A call of the code, made when first polled: a pending call waits for its `resolve`.
pub(crate) struct CallFuture {
    shared: Arc<Shared>,
    call: Option<b::call::Call>,
    step: Option<StepSpec>,
    pending: Option<u64>,
}

pub(crate) enum Answer {
    /// The call was answered at once.
    Called(Called),
    /// The call was pending, then resolved with this value.
    Resolved(Option<layout::Value>),
}

impl CallFuture {
    pub(crate) fn new(shared: Arc<Shared>, call: b::call::Call, step: Option<StepSpec>) -> Self {
        Self {
            shared,
            call: Some(call),
            step,
            pending: None,
        }
    }

    pub(crate) fn pending_call(&self) -> Option<u64> {
        self.pending
    }
}

impl Future for CallFuture {
    type Output = Result<Answer>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if let Some(call) = self.call.take() {
            let called = match (call, self.step.take()) {
                (b::call::Call::Step(step), Some(spec)) => self.shared.call_step(step, spec)?,
                (call, None) => self.shared.call(call)?,
                (call, Some(_)) => {
                    return Poll::Ready(Err(Error::Protocol(format!("a body for {call:?}"))));
                }
            };
            match called {
                Called::Pending(id) => {
                    let slot = Slot {
                        result: None,
                        waker: Some(cx.waker().clone()),
                    };
                    self.shared.lock().slots.insert(id, slot);
                    self.pending = Some(id);
                    return Poll::Pending;
                }
                called => return Poll::Ready(Ok(Answer::Called(called))),
            }
        }
        let id = self
            .pending
            .expect("a call future polled after it completed");
        let mut state = self.shared.lock();
        let slot = state.slots.get_mut(&id).expect("a pending call has a slot");
        match slot.result.take() {
            Some(result) => {
                state.slots.remove(&id);
                Poll::Ready(result.map(Answer::Resolved))
            }
            None => {
                slot.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
