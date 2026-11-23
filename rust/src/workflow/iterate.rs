//! Loops and compression (I-12): in Rust, what the loop needs is in its state.

use std::future::{Future, IntoFuture};
use std::ops::ControlFlow;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::context::Context;
use super::{AsWorkflow, decode, input_object, json};
use crate::error::{Continuation, Error, Result};
use crate::proto::boundary::v1::{self as b, call::Call, called::Called};

impl Context {
    /// Runs `body` from `init`, or from the state a previous run of the chain carried, until it
    /// breaks with the last state. At each boundary past the threshold, the run continues as new
    /// with the chain's input and the state; the error it returns ends the run, so propagate it.
    /// `.upgrade()` also continues at a boundary once the worker serves a higher version of the
    /// workflow than the run's, into the highest one.
    pub fn iterate<S, F, Fut>(&self, init: S, body: F) -> Iterate<'_, S, F>
    where
        S: Serialize + DeserializeOwned,
        F: FnMut(Context, S) -> Fut,
        Fut: Future<Output = Result<ControlFlow<S, S>>>,
    {
        Iterate {
            c: self,
            init,
            body,
            upgrade: false,
        }
    }

    /// The state a loop starts from: `init`, or the one a previous run of the chain carried.
    pub(super) fn iteration<S: Serialize + DeserializeOwned>(&self, init: S) -> Result<S> {
        let initial = json(super::to_json(&init)?)?;
        match self.shared.call(Call::Iterate(initial))? {
            Called::Value(v) => decode(Some(v)),
            other => Err(Error::Protocol(format!("iterate answered {other:?}"))),
        }
    }

    /// A loop's boundary: past the threshold, or with `upgrade` once the worker serves a higher
    /// version, the error that continues the run as new with the chain's input and `state`.
    pub(super) fn boundary<S: Serialize>(&self, state: &S, upgrade: bool) -> Result<()> {
        // § hy3f1z5jj@jg53p
        if self.past_threshold(upgrade)? {
            // § hy3f1z5jj.b@96z2c
            return Err(Error::ContinuedAsNew(Continuation {
                workflow: None,
                version: None,
                input: self.shared.input(),
                // § hy3f1z5jj.c@sf3c0
                state: Some(super::to_json(state).map_err(|e| {
                    Error::NotCompressible(format!(
                        "the state of iterate does not serialize to JSON: {e}"
                    ))
                })?),
                // § end hy3f1z5jj.c
                highest: upgrade,
            }));
        }
        Ok(())
    }

    /// Whether the history passed the worker's `continue_as_new_after`, or with `upgrade` the
    /// worker serves a higher version of the workflow than the run's.
    fn past_threshold(&self, upgrade: bool) -> Result<bool> {
        let call = Call::ShouldContinueAsNew(b::call::ShouldContinue { highest: upgrade });
        match self.shared.call(call)? {
            Called::Value(v) => decode(Some(v)),
            other => Err(Error::Protocol(format!(
                "should_continue_as_new answered {other:?}"
            ))),
        }
    }

    /// Ends the run and starts the next of its chain with `input`, once returned as the
    /// workflow's error: `return Err(c.continue_as_new(..).await)`.
    /// The run keeps its version.
    pub async fn continue_as_new(&self, input: impl Serialize) -> Error {
        continuation(None, None, input)
    }

    /// As `continue_as_new`, the next run being of `workflow`, with its version or without one.
    pub async fn continue_as_new_to<W: AsWorkflow + ?Sized>(
        &self,
        workflow: &W,
        input: impl Serialize,
    ) -> Error {
        let workflow = workflow.workflow();
        continuation(Some(workflow.name().to_owned()), workflow.version(), input)
    }
}

fn continuation(workflow: Option<String>, version: Option<u32>, input: impl Serialize) -> Error {
    // § aaev3rs1q.b@0as6p
    match input_object(input) {
        // § one gjnefe4w6@a90rc
        Ok(input) => Error::ContinuedAsNew(Continuation {
            workflow,
            version,
            input,
            state: None,
            highest: false,
        }),
        Err(e) => e,
    }
}

/// A loop of `Context::iterate`, run when awaited.
#[must_use = "a loop does nothing until awaited"]
pub struct Iterate<'a, S, F> {
    c: &'a Context,
    init: S,
    body: F,
    upgrade: bool,
}

impl<S, F> Iterate<'_, S, F> {
    /// Continues at a boundary into the highest version of the workflow the worker serves, once
    /// it is higher than the run's.
    pub fn upgrade(mut self) -> Self {
        self.upgrade = true;
        self
    }
}

impl<'a, S, F, Fut> IntoFuture for Iterate<'a, S, F>
where
    S: Serialize + DeserializeOwned + Send + 'a,
    F: FnMut(Context, S) -> Fut + Send + 'a,
    Fut: Future<Output = Result<ControlFlow<S, S>>> + Send + 'a,
{
    type Output = Result<S>;
    type IntoFuture = BoxFuture<'a, Result<S>>;

    fn into_future(self) -> Self::IntoFuture {
        let Iterate {
            c,
            init,
            mut body,
            upgrade,
        } = self;
        Box::pin(async move {
            let mut state = c.iteration(init)?;
            loop {
                state = match body(c.clone(), state).await? {
                    ControlFlow::Break(last) => return Ok(last),
                    ControlFlow::Continue(next) => next,
                };
                c.boundary(&state, upgrade)?;
            }
        })
    }
}
