//! The store of an environment: its namespace in memory, seen by each worker and client through
//! the faults the test asks for, an outage or Chaos.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;

use super::unit;
use super::workers::Incarnation;
use super::world::World;
use crate::settings::STORE_RETRY;
use crate::store::retry::{self, Fault};
use crate::store::{Capabilities, Create, ListWait, Listing, Store, StoreFailure, StoreFuture};

/// Faults drawn from `seed`: each worker store operation fails with probability `rate`, half of
/// the failed creates applied anyway with their answer lost, and each step attempt that returns
/// crashes its worker with probability `rate`, a fresh worker taking its place.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Chaos {
    pub seed: u64,
    /// From 0 to 1.
    pub rate: f64,
}

/// What Chaos injected: store operations `failed` and not applied, creates applied with their
/// answer lost (`ambiguous`), and workers `crashed` after a step returned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Faults {
    pub failed: u32,
    pub ambiguous: u32,
    pub crashed: u32,
}

/// The namespace as a worker incarnation (`inc`) or a client (`None`) sees it.
pub(super) struct FaultStore {
    world: Arc<World>,
    inc: Option<Arc<Incarnation>>,
}

impl FaultStore {
    pub(super) fn new(world: Arc<World>, inc: Option<Arc<Incarnation>>) -> Self {
        Self { world, inc }
    }

    /// A worker's backoff waits in virtual time; a client's takes none.
    async fn sleep(&self, d: Duration) {
        if let Some(inc) = &self.inc {
            let at = self.world.time.now() + d.as_nanos().div_ceil(1_000_000) as i64;
            inc.wait(&[], Some(at)).await;
        }
    }

    /// One try of `apply`, which Chaos may fault; `created` tells whether a create applied.
    async fn attempt<T, F, Fut>(
        &self,
        kind: &str,
        key: &str,
        apply: F,
        created: fn(&T) -> bool,
    ) -> Result<T, Fault>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, StoreFailure>>,
    {
        let world = &self.world;
        if let Some(inc) = &self.inc {
            // A paused worker does not reach the store.
            inc.passing().await;
        }
        if world.time.now() < world.outage_until.load(Ordering::SeqCst) {
            return Err(Fault::Transient("the store is out".into()));
        }
        if let Some(inc) = &self.inc
            && let Some(n) = world.draw(kind, key, inc)
        {
            let applied =
                kind == "create" && unit(world.seed, &[&"applied", &key, &inc.id, &n]) < 0.5;
            // A create that found its key applied nothing: it counts as failed.
            if applied && created(&apply().await.map_err(|e| Fault::Permanent(e.message))?) {
                world.faulted(|f| f.ambiguous += 1);
                return Err(Fault::Ambiguous(
                    "chaos: a create applied, its answer lost".into(),
                ));
            }
            world.faulted(|f| f.failed += 1);
            return Err(Fault::Transient(format!("chaos: a {kind} failed")));
        }
        apply().await.map_err(|e| Fault::Permanent(e.message))
    }

    async fn retry<T, F, Fut>(&self, kind: &str, key: &str, apply: F) -> Result<T, StoreFailure>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, StoreFailure>>,
    {
        let op = || self.attempt(kind, key, &apply, |_| false);
        retry::with_retry(STORE_RETRY, op, |d| self.sleep(d)).await
    }
}

impl Store for FaultStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: false }
    }

    fn open(&self) -> futures::future::BoxFuture<'_, crate::error::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            let memory = &self.world.memory;
            let create = || {
                self.attempt(
                    "create",
                    key,
                    || async {
                        let created = memory.create(key, bytes.clone()).await?;
                        if created == Create::Created {
                            self.world.writes.fetch_add(1, Ordering::SeqCst);
                            self.world.written(key, &bytes);
                        }
                        Ok(created)
                    },
                    |created| *created == Create::Created,
                )
            };
            let get = || self.attempt("get", key, || memory.get(key), |_| false);
            retry::create(STORE_RETRY, &bytes, create, get, |d| self.sleep(d)).await
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
        Box::pin(self.retry("get", key, move || self.world.memory.get(key)))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        Box::pin(async move {
            crate::store::refuse_wait(&wait)?;
            let writes = self.world.writes.load(Ordering::SeqCst);
            let key = match start_after {
                Some(after) => format!("{prefix}>{after}"),
                None => prefix.to_owned(),
            };
            let memory = &self.world.memory;
            let listing = self
                .retry("list", &key, || {
                    memory.list(prefix, start_after, max_keys, None)
                })
                .await?;
            if let Some(inc) = &self.inc
                && prefix.starts_with("queues/")
            {
                inc.listed(&key, writes);
            }
            Ok(listing)
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(self.retry("delete", key, move || async move {
            self.world.memory.delete(key).await?;
            self.world.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
    }
}
