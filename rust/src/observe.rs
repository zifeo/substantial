//! What a `Substantial` tells its observer: store operations, step attempts, replays, scans, and
//! runs blocked, near a history limit or left for a clock behind their history.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::cost::{Amount, Prices};
use crate::error::{Failure, Result};
use crate::store::attempts::measured;
use crate::store::{
    Capabilities, Cas, Create, ListWait, Listing, Object, Store, StoreFailure, StoreFuture,
};

/// An event of a `Substantial`, given to the function of `.observe(..)`.
#[derive(Clone, Debug, PartialEq)]
pub enum Observed {
    StoreOp(StoreOp),
    Attempt(Attempt),
    Replay(Replay),
    Scan(Scan),
    Blocked(Blocked),
    NearLimit(NearLimit),
    Skewed(Skewed),
}

/// A store operation of a client or a worker, once it answered.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreOp {
    /// `create`, `get`, `list` or `delete`.
    pub op: &'static str,
    pub seconds: f64,
    /// The requests the adapter sent, its retries included.
    pub attempts: u32,
    /// `created`, `exists`, `found`, `missing`, `listed`, `deleted` or `failed`.
    pub outcome: &'static str,
    /// `attempts` requests at the price of the operation's kind; `None` without prices.
    pub cost: Option<Amount>,
}

/// A step attempt a worker ran, once it ended.
#[derive(Clone, Debug, PartialEq)]
pub struct Attempt {
    pub workflow: String,
    pub step: String,
    pub attempt: u32,
    pub seconds: f64,
    /// `ok`, `retry` (a retryable error with attempts left), `failed` (no attempt left, or a
    /// permanent error), `timeout` or `abandoned` (interrupted, its outcome not recorded).
    pub outcome: &'static str,
}

/// A replay a worker ran, once it ended.
#[derive(Clone, Debug, PartialEq)]
pub struct Replay {
    pub workflow: String,
    /// How late it started after the earliest due entry it serves; `None` when it serves none.
    pub delay: Option<f64>,
    pub seconds: f64,
    /// `suspended`, `ended`, `idle` (nothing new) or `dropped` (a lost race, a stop, a shutdown).
    pub end: &'static str,
}

/// A scan listed the due entries of a workflow, `due` of them due.
#[derive(Clone, Debug, PartialEq)]
pub struct Scan {
    pub workflow: String,
    pub due: u32,
}

/// A replay blocked: its code raised an error that is no end, or diverged from the history. The
/// run is tried again after a backoff.
#[derive(Clone, Debug, PartialEq)]
pub struct Blocked {
    pub workflow: String,
    pub run: String,
    pub error: Failure,
}

/// A commit took a run past the warning share of one of its history limits: `limit` is
/// `history_bytes`, `history_events`, `replay_inbox` or `seen`.
#[derive(Clone, Debug, PartialEq)]
pub struct NearLimit {
    pub workflow: String,
    pub run: String,
    pub limit: String,
    pub used: u64,
    pub max: u64,
}

/// A worker left a chain alone, `run` the chain and `current` its current run: its clock, `now`, is
/// behind the time of the run's last segment by more than `claim_skew`.
#[derive(Clone, Debug, PartialEq)]
pub struct Skewed {
    pub run: String,
    pub current: String,
    pub segment_at: chrono::DateTime<chrono::Utc>,
    pub now: chrono::DateTime<chrono::Utc>,
}

pub(crate) type Observe = Arc<dyn Fn(&Observed) + Send + Sync>;

/// The prices and the observer of a `Substantial`, which its workers take.
#[derive(Clone, Default)]
pub(crate) struct Metering {
    pub(crate) prices: Option<Prices>,
    pub(crate) observe: Option<Observe>,
}

impl Metering {
    /// `store`, its operations observed when there is an observer.
    pub(crate) fn wrap(&self, store: Arc<dyn Store>) -> Arc<dyn Store> {
        match &self.observe {
            Some(observe) => Arc::new(Observing {
                inner: store,
                observe: observe.clone(),
                prices: self.prices.clone(),
            }),
            None => store,
        }
    }

    /// Tells the observer, if any; a panic of the observer is returned as its message.
    pub(crate) fn tell(&self, event: impl FnOnce() -> Observed) -> Result<(), String> {
        let Some(observe) = &self.observe else {
            return Ok(());
        };
        let event = event();
        std::panic::catch_unwind(AssertUnwindSafe(|| observe(&event)))
            .map_err(|panic| crate::worker::panicked(&*panic))
    }
}

/// A store whose every operation is told to an observer once it answered.
struct Observing {
    inner: Arc<dyn Store>,
    observe: Observe,
    prices: Option<Prices>,
}

impl Observing {
    async fn observed<T>(
        &self,
        op: &'static str,
        performed: StoreFuture<'_, T>,
        outcome: fn(&T) -> &'static str,
    ) -> Result<T, StoreFailure> {
        let started = Instant::now();
        let (result, attempts) = measured(performed).await;
        let event = StoreOp {
            op,
            seconds: started.elapsed().as_secs_f64(),
            attempts,
            outcome: result.as_ref().map_or("failed", outcome),
            // § one vg9yp9xsf@hdx5e
            cost: self.prices.as_ref().map(|p| p.of(op, attempts)),
        };
        // § yp7fv5sa7.a@8mn6n
        (self.observe)(&Observed::StoreOp(event));
        result
    }
}

impl Store for Observing {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn open(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.open()
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Create> {
        let created = self.inner.create(key, bytes, expiry);
        Box::pin(self.observed("create", created, |c| match c {
            Create::Created { .. } => "created",
            Create::Exists => "exists",
        }))
    }

    fn cas<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        etag: &'a str,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Cas> {
        let written = self.inner.cas(key, bytes, etag, expiry);
        Box::pin(self.observed("cas", written, |c| match c {
            Cas::Written { .. } => "written",
            Cas::Mismatch => "mismatch",
        }))
    }

    fn check_expiry<'a>(&'a self, days: &'a [u32]) -> BoxFuture<'a, Result<()>> {
        self.inner.check_expiry(days)
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
        Box::pin(self.observed("get", self.inner.get(key), |g| match g {
            Some(_) => "found",
            None => "missing",
        }))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        let listed = self.inner.list(prefix, start_after, max_keys, wait);
        Box::pin(self.observed("list", listed, |_| "listed"))
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(self.observed("delete", self.inner.delete(key), |_| "deleted"))
    }
}
