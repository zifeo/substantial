//! The guest SDK of Substantial: a workflow and its steps, written as for the Rust host, compiled
//! to a WebAssembly component that a worker of the Rust host loads at run time.
//!
//! ```ignore
//! use substantial_guest as s;
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
//! ```
//!
//! Built with `cargo build --target wasm32-unknown-unknown`, then made a component with
//! `wasm-tools component new`.

extern crate self as substantial_guest;

mod context;
mod durations;
mod error;
mod executor;

#[doc(hidden)]
pub mod __macro;

#[doc(hidden)]
pub mod __bindings {
    wit_bindgen::generate!({
        world: "guest",
        path: "wit",
        pub_export_macro: true,
        export_macro_name: "__export_guest",
        default_bindings_module: "::substantial_guest::__bindings",
        runtime_path: "::substantial_guest::__wit_bindgen::rt",
    });
}

#[doc(hidden)]
pub use wit_bindgen as __wit_bindgen;

// § one b4j0mc19d@q3kd3
pub use context::{Log, Receive, SendFromWorkflow, Step, WorkflowContext as Context};
pub use error::{Error, Failure, Permanent, Raised, Result};
// § one b4j0mc19d@sc2mj
pub use durations::{
    duration_days as days, duration_hours as hours, duration_minutes as minutes,
    duration_seconds as seconds,
};
pub use substantial_macros::{
    guest_export as export, guest_step as step, guest_workflow as workflow,
};

/// Exponential backoff for a step: every field is required, as in the Rust host.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Retry {
    pub max_attempts: u32,
    pub initial: std::time::Duration,
    pub max: std::time::Duration,
    pub multiplier: f64,
}
