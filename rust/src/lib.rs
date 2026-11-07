//! The Rust host of Substantial: loads the core and drives it against a store.
//!
//! Threading: the core module is compiled once per process; each [`Worker`] instantiates it once,
//! and its agent machine and the run machines of all its replays share that instance behind a
//! mutex, held only for the length of one synchronous boundary call. The agent loop is one tokio
//! task; each replay is another, which polls the workflow future itself (I-24) and spawns step
//! attempts as tasks of their own on the runtime.

mod client;
mod core;
mod error;
mod names;
mod plan;
mod proto;
mod settings;
mod store;
mod time;
mod worker;
mod workflow;

pub use client::{
    Create, Open, ResultWait, Run, RunStatus, SendMessage, StartRun, Substantial, WorkerBuilder,
};
pub use error::{Continuation, Error, Failure, Result};
pub use settings::Preset;
pub use store::{Credentials, Keys};
pub use substantial_macros::{step, workflow};
pub use time::{days, hours, minutes, seconds};
pub use worker::Worker;
pub use workflow::{
    AsWorkflow, Child, Context, Handlers, IntoWorkflows, Log, Permanent, Retry, SendFromWorkflow,
    StartChild, Step, Var, WaitUntil, Workflow, step_key,
};

/// What the code `#[workflow]` and `#[step]` generate refers to; not an API.
#[doc(hidden)]
pub mod __macro;

/// What the tests reach beyond the API: store objects, every setting, waking a worker. Not an API.
#[doc(hidden)]
pub mod __internal {
    use std::sync::Arc;

    pub use crate::settings::{STORE_RETRY, StoreRetry};
    pub use crate::store::{
        Capabilities, Create, FsStore, ListWait, Listing, MemoryStore, S3Location, S3Store, Store,
        StoreFailure, StoreFuture, open,
    };

    use crate::client::{self, Source};
    use crate::{Worker, WorkerBuilder};

    /// `Substantial::create` on a store object.
    pub fn create(store: Arc<dyn Store>) -> client::Create {
        client::Create::new(Source::Store(store))
    }

    /// `Substantial::open` on a store object.
    pub fn open_store(store: Arc<dyn Store>) -> client::Open {
        client::Open::new(Source::Store(store))
    }

    /// `WorkerBuilder::set`, for any setting.
    pub fn set(builder: WorkerBuilder, name: &str, value: impl ToString) -> WorkerBuilder {
        builder.with(name, value, true)
    }

    /// Wakes the worker at once, as a start or a send from this process does.
    pub fn wake(worker: &Worker) {
        worker.wake();
    }
}
