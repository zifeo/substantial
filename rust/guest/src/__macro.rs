//! What `#[workflow]`, `#[step]` and `export!` generate refers to; not an API.

use std::future::Future;
use std::pin::Pin;
use std::sync::Once;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub use crate::__bindings::exports::substantial::guest::workflow::{End, StepError};
use crate::__bindings::substantial::guest::panics;
use crate::context::WorkflowContext;
use crate::error::{Error, Permanent, Result};
use crate::executor::block_on;

pub use serde_json;

/// The value type of a function's `Result`, the output of its step or workflow.
pub trait Returns {
    type Value;
}

impl<T, E> Returns for std::result::Result<T, E> {
    type Value = T;
}

/// A workflow, as `#[workflow]` declares it: what the export `run` runs.
pub trait Workflow {
    fn run(&self, input: String) -> std::result::Result<String, End>;
}

/// The context of the workflow code being run, for the steps declared with `#[step]`.
pub fn current_context() -> Result<WorkflowContext> {
    Ok(WorkflowContext::new())
}

/// Hands the host a panic's message before the guest traps, for the failure of its trap.
fn report_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| std::panic::set_hook(Box::new(|info| panics::panicked(&info.to_string()))));
}

/// Runs a workflow taking an input that deserializes into `I`.
pub fn run_workflow<I, O, F, Fut>(input: String, f: F) -> std::result::Result<String, End>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(WorkflowContext, I) -> Fut,
    Fut: Future<Output = Result<O>>,
{
    report_panics();
    let ran = serde_json::from_str::<I>(&input)
        .map_err(Error::from)
        .and_then(|input| block_on(f(WorkflowContext::new(), input)))
        .and_then(|output| Ok(serde_json::to_string(&output)?));
    ran.map_err(Error::end)
}

/// Runs a workflow that takes no input.
pub fn run_without_input<O, F, Fut>(input: String, f: F) -> std::result::Result<String, End>
where
    O: Serialize,
    F: FnOnce(WorkflowContext) -> Fut,
    Fut: Future<Output = Result<O>>,
{
    run_workflow(input, |c, _: serde::de::IgnoredAny| f(c))
}

/// The JSON of a step's arguments, an array of them in order.
pub fn encode_arguments<T: Serialize>(args: &T) -> Result<String> {
    Ok(serde_json::to_string(args)?)
}

pub type Attempt = Pin<Box<dyn Future<Output = std::result::Result<String, StepError>>>>;

/// A step of the guest, as `#[step]` declares it: its name and what an attempt runs.
pub struct StepEntry {
    pub name: &'static str,
    pub attempt: fn(String) -> Attempt,
}

/// The arguments of an attempt; ones that do not decode fail it at once.
pub fn decode_arguments<T: DeserializeOwned>(args: &str) -> std::result::Result<T, StepError> {
    serde_json::from_str(args).map_err(|e| StepError {
        message: format!("the step's arguments do not decode: {e}"),
        permanent: true,
    })
}

/// The outcome of an attempt as the host takes it: a `Permanent` error is not retryable.
pub fn step_outcome<T, E>(
    result: std::result::Result<T, E>,
) -> std::result::Result<String, StepError>
where
    T: Serialize,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match result {
        Ok(value) => serde_json::to_string(&value).map_err(|e| StepError {
            message: format!("the step's value does not encode: {e}"),
            permanent: true,
        }),
        Err(e) => {
            let e = e.into();
            Err(StepError {
                message: e.to_string(),
                permanent: e.downcast_ref::<Permanent>().is_some(),
            })
        }
    }
}

/// Runs one attempt of the step `name` of `steps`.
pub fn run_step(
    steps: &[StepEntry],
    name: &str,
    args: String,
) -> std::result::Result<String, StepError> {
    report_panics();
    // § sfd6dmd7t.a@2cd65
    let Some(entry) = steps.iter().find(|s| s.name == name) else {
        return Err(StepError {
            message: format!("this guest exports no step {name:?}: list it in export!"),
            permanent: true,
        });
    };
    // § end sfd6dmd7t.a
    block_on((entry.attempt)(args))
}
