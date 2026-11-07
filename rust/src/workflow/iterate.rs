//! Loops and compression (I-12): in Rust, what the loop needs is in its state.

use std::future::Future;
use std::ops::ControlFlow;

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
    pub async fn iterate<S, F, Fut>(&self, init: S, mut body: F) -> Result<S>
    where
        S: Serialize + DeserializeOwned,
        F: FnMut(Context, S) -> Fut,
        Fut: Future<Output = Result<ControlFlow<S, S>>>,
    {
        let initial = json(serde_json::to_vec(&init)?)?;
        let mut state: S = match self.shared.call(Call::Iterate(initial))? {
            Called::Value(v) => decode(Some(v))?,
            other => return Err(Error::Protocol(format!("iterate answered {other:?}"))),
        };
        loop {
            state = match body(self.clone(), state).await? {
                ControlFlow::Break(last) => return Ok(last),
                ControlFlow::Continue(next) => next,
            };
            if self.past_threshold()? {
                return Err(Error::ContinuedAsNew(Continuation {
                    workflow: None,
                    version: None,
                    input: self.shared.input(),
                    state: Some(serde_json::to_vec(&state)?),
                }));
            }
        }
    }

    /// Whether the history passed the worker's `continue_as_new_after`.
    fn past_threshold(&self) -> Result<bool> {
        match self.shared.call(Call::ShouldContinueAsNew(b::Empty {}))? {
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
    match input_object(input) {
        Ok(input) => Error::ContinuedAsNew(Continuation {
            workflow,
            version,
            input,
            state: None,
        }),
        Err(e) => e,
    }
}
