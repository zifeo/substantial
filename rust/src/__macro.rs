pub use serde_json;

use crate::error::Result;
use crate::workflow::Context;

/// The context of the workflow code polling the calling step.
pub fn context() -> Result<Context> {
    crate::workflow::current()
}

/// The value type of a function's `Result`, the output of its step or workflow.
pub trait Returns {
    type Value;
}

impl<T, E> Returns for std::result::Result<T, E> {
    type Value = T;
}
