//! The Wasm workflow side of Substantial, which the crate `substantial` re-exports for wasm32: a
//! workflow and its steps, written as for the Rust host, compiled to a WebAssembly component that
//! a worker of the Rust host loads at run time.
//!
//! ```
//! # extern crate substantial_wasm_sdk as substantial;
//! use substantial as s;
//!
//! #[derive(serde::Deserialize)]
//! struct Input {
//!     name: String,
//! }
//!
//! #[s::step]
//! async fn greet(name: &str) -> Result<String, std::io::Error> {
//!     Ok(format!("hello {name}"))
//! }
//!
//! #[s::workflow]
//! async fn hello(c: s::Context, input: Input) -> s::Result<String> {
//!     c.sleep(s::seconds(1)).await?;
//!     greet(&input.name).await
//! }
//!
//! s::export!(hello, steps = [greet]);
//! # fn main() {}
//! ```
//!
//! Built with `cargo build --target wasm32-unknown-unknown`, then made a component with
//! `wasm-tools component new`.

extern crate self as substantial;

mod context;
mod durations;
mod error;
mod executor;
mod json;

#[doc(hidden)]
pub mod __macro;

#[doc(hidden)]
pub mod __bindings {
    wit_bindgen::generate!({
        world: "module",
        path: "wit",
        pub_export_macro: true,
        export_macro_name: "__export_module",
        default_bindings_module: "::substantial::__bindings",
        runtime_path: "::substantial::__wit_bindgen::rt",
    });
}

#[doc(hidden)]
pub use wit_bindgen as __wit_bindgen;

// § one .:c7aw13gws@z2hp8
pub use context::{Context, Log, Receive, SendFromWorkflow, Step, step_key};
pub use error::{Error, Failure, Permanent, Raised, Result};
// § one .:c7aw13gws@vrm5w
pub use durations::{days, hours, minutes, seconds};
pub use substantial_macros::{wasm_export as export, wasm_step as step, wasm_workflow as workflow};

/// Exponential backoff for a step: every field is required, as in the Rust host.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Retry {
    pub max_attempts: u32,
    pub initial: std::time::Duration,
    pub max: std::time::Duration,
    pub multiplier: f64,
    /// The native host's total deadline of the step's attempts, which a Wasm workflow's step
    /// does not have yet: `Some` raises `InvalidArgument` at the step's call.
    pub deadline: Option<std::time::Duration>,
}
