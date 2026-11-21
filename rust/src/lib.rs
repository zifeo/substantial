//! Substantial, brokerless durable execution. Natively, the Rust host: it loads the core and
//! drives it against a store. Built for wasm32, the API of a Wasm workflow, which a worker with the
//! feature `wasm-runner` loads: the same macros and verbs, recorded by the worker that runs it.
//!
//! Natively, threading: the core module is compiled once per process; each [`Worker`] instantiates it once,
//! and its agent machine and the run machines of all its replays share that instance behind a
//! mutex, held only for the length of one synchronous boundary call. The agent loop is one tokio
//! task; each replay is another, which polls the workflow future itself (I-24) and spawns step
//! attempts as tasks of their own on the runtime.

#[cfg(not(target_family = "wasm"))]
mod client;
#[cfg(not(target_family = "wasm"))]
mod clock;
#[cfg(not(target_family = "wasm"))]
mod core;
#[cfg(not(target_family = "wasm"))]
mod cost;
#[cfg(not(target_family = "wasm"))]
mod error;
#[cfg(not(target_family = "wasm"))]
mod names;
#[cfg(not(target_family = "wasm"))]
pub mod observe;
#[cfg(not(target_family = "wasm"))]
mod plan;
#[cfg(not(target_family = "wasm"))]
mod proto;
#[cfg(not(target_family = "wasm"))]
mod settings;
#[cfg(not(target_family = "wasm"))]
mod store;
#[cfg(all(not(target_family = "wasm"), feature = "testing"))]
pub mod testing;
#[cfg(not(target_family = "wasm"))]
mod time;
#[cfg(all(not(target_family = "wasm"), feature = "wasm-runner"))]
// § one s9ynfgdqm.c@bbk2h
pub mod wasm;
#[cfg(not(target_family = "wasm"))]
mod worker;
#[cfg(not(target_family = "wasm"))]
mod workflow;

#[cfg(not(target_family = "wasm"))]
pub use client::{
    Create, HistoryEvent, Open, ResultWait, RetryRun, Run, RunInfo, RunStatus, ScheduleRun,
    SendMessage, StartRun, Substantial, WaitingOn, WorkerBuilder,
};
#[cfg(not(target_family = "wasm"))]
pub use cost::{Amount, Budget, Cost, Meters, Prices};
#[cfg(not(target_family = "wasm"))]
pub use error::{Continuation, Error, Failure, Result};
#[cfg(not(target_family = "wasm"))]
pub use observe::Observed;
#[cfg(not(target_family = "wasm"))]
pub use settings::Preset;
#[cfg(not(target_family = "wasm"))]
pub use store::{Credentials, Keys, Password};
#[cfg(not(target_family = "wasm"))]
// § one s9ynfgdqm@z6bqs
pub use substantial_macros::{export, step, workflow};
#[cfg(not(target_family = "wasm"))]
pub use time::{days, hours, minutes, seconds};
#[cfg(not(target_family = "wasm"))]
pub use worker::Worker;
#[cfg(not(target_family = "wasm"))]
pub use workflow::{
    AsWorkflow, ChainResult, Child, Context, Cron, Handlers, IntoWorkflows, Log, Permanent, Poll,
    Receive, Retry, SendFromWorkflow, StartChild, Step, Var, WaitUntil, Workflow, charge, step_key,
};

/// What the code `#[workflow]` and `#[step]` generate refers to; not an API.
#[doc(hidden)]
#[cfg(not(target_family = "wasm"))]
pub mod __macro;

/// What the tests reach beyond the API: store objects, every setting, waking a worker. Not an API.
#[doc(hidden)]
#[cfg(not(target_family = "wasm"))]
pub mod __internal {
    use std::sync::Arc;

    pub use crate::settings::{STORE_RETRY, StoreRetry};
    #[cfg(feature = "redis")]
    pub use crate::store::RedisStore;
    pub use crate::store::{
        Capabilities, Create, FsStore, ListWait, Listing, MemoryStore, RedisLocation, S3Location,
        S3Store, Store, StoreFailure, StoreFuture, Topology, open,
    };

    pub use crate::proto::layout::v1 as layout;
    pub use crate::workflow::schedule::{Plan, Skipped, iso, next_fire, parse, plan};

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

    /// A `result` wait that follows its chain from the first run again every `refollow`.
    pub fn refollow<T>(
        wait: crate::ResultWait<T>,
        refollow: std::time::Duration,
    ) -> crate::ResultWait<T> {
        wait.refollow(refollow)
    }

    /// Wakes the worker at once for a full scan of every workflow it serves.
    pub fn wake(worker: &Worker) {
        worker.wake();
    }

    /// A worker whose clock is the real one ahead by the milliseconds `ms` holds, which the test
    /// may change while it runs.
    pub fn shifted(
        mut builder: WorkerBuilder,
        ms: Arc<std::sync::atomic::AtomicI64>,
    ) -> WorkerBuilder {
        builder.shift = Some(ms);
        builder
    }
}

#[cfg(target_family = "wasm")]
// § one s9ynfgdqm.a@jspdm
pub use substantial_wasm_sdk::*;
