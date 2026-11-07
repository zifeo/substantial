//! The Redis adapter, over the `redis` crate, which the `redis` feature brings.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ::redis::Value;
use ::redis::aio::MultiplexedConnection;
use bytes::Bytes;
use futures::future::BoxFuture;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use super::RedisLocation;
use super::Topology;
use super::connect::{
    ATTEMPT_TIMEOUT, CREATE, Connector, Count, DELETE, Failure, Primary, RECOUNT_EVERY, WAITAOF_MS,
    answered, refused,
};
use super::notify::Notifications;
use crate::error::{Error, Result};
use crate::settings::StoreRetry;
use crate::store::credentials::Credentials;
use crate::store::retry::{self, Fault};
use crate::store::{Capabilities, Create, ListWait, Listing, Store, StoreFuture};

/// The longest a waiting listing waits.
const MAX_WAIT_SECONDS: u32 = 20;

/// `redis://`, `redis+sentinel://` and `redis+cluster://`: the namespace `<ns>` is the hash tag
/// of its keys, so they live on one slot. A key is the string `{<ns>}:o:<key>` and a member of
/// the sorted set `{<ns>}:i`, its index, listed by lex range; every create and delete publishes
/// the key on `{<ns>}:c`, which waiting listings subscribe to.
///
/// A create is the script `create.lua`, then `WAITAOF` on the same connection: acknowledged once
/// fsynced on the primary and on `replicas`. The client's own retries are off: `store_retry` is
/// the only retry.
pub struct RedisStore {
    shared: Arc<Shared>,
}

pub(super) struct Shared {
    pub(super) connector: Connector,
    pub(super) notifications: Notifications,
    pub(super) retry: StoreRetry,
    object: String,
    index: String,
    fence: String,
    channel: String,
    publish: &'static str,
    current: Mutex<Option<Arc<Primary>>>,
    connecting: tokio::sync::Mutex<()>,
    waitaof_ms: AtomicU64,
}

/// How an attempt failed: a fault, or a server the adapter cannot use.
enum Classified {
    Fault(Fault),
    Config(String),
}

impl Shared {
    pub(super) async fn primary(&self) -> Result<Arc<Primary>, Failure> {
        if let Some(primary) = self.current() {
            return Ok(primary);
        }
        let _connecting = self.connecting.lock().await;
        if let Some(primary) = self.current() {
            return Ok(primary);
        }
        let primary = Arc::new(self.connector.connect().await?);
        *self.current.lock().expect("never poisoned") = Some(primary.clone());
        Ok(primary)
    }

    fn current(&self) -> Option<Arc<Primary>> {
        self.current.lock().expect("never poisoned").clone()
    }

    /// Forgets a primary after a failure on it, so the next attempt finds it again.
    pub(super) fn lost(&self, primary: &Arc<Primary>) {
        let mut current = self.current.lock().expect("never poisoned");
        if current.as_ref().is_some_and(|c| Arc::ptr_eq(c, primary)) {
            *current = None;
        }
    }

    /// A failure without an answer is `Ambiguous` for a create and `Transient` otherwise, an
    /// error answer `Transient`, a refused password `Permanent`.
    fn classify(
        &self,
        what: &str,
        create: bool,
        primary: Option<&Arc<Primary>>,
        failure: Failure,
    ) -> Classified {
        let e = match failure {
            Failure::Fault(f) => return Classified::Fault(f),
            Failure::Config(m) => return Classified::Config(m),
            Failure::Provider(m) => {
                return Classified::Fault(Fault::Permanent(format!("{what}: {m}")));
            }
            Failure::Redis(e) => e,
        };
        let text = format!("{what}: {e}");
        if refused(&e) {
            return Classified::Fault(Fault::Permanent(text));
        }
        if let Some(primary) = primary {
            self.lost(primary);
        }
        if answered(&e) || primary.is_none() || !create {
            Classified::Fault(Fault::Transient(text))
        } else {
            Classified::Fault(Fault::Ambiguous(text))
        }
    }

    async fn attempt_classified<T, F, Fut>(
        &self,
        what: &str,
        create: bool,
        op: F,
    ) -> Result<T, Classified>
    where
        F: FnOnce(Arc<Primary>) -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        let primary = match tokio::time::timeout(ATTEMPT_TIMEOUT, self.primary()).await {
            Ok(Ok(primary)) => primary,
            Ok(Err(e)) => return Err(self.classify(what, create, None, e)),
            Err(_) => {
                return Err(Classified::Fault(Fault::Transient(format!(
                    "{what}: no primary within {} s",
                    ATTEMPT_TIMEOUT.as_secs()
                ))));
            }
        };
        match tokio::time::timeout(ATTEMPT_TIMEOUT, op(primary.clone())).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(self.classify(what, create, Some(&primary), e)),
            Err(_) => {
                self.lost(&primary);
                let text = format!("{what}: no answer within {} s", ATTEMPT_TIMEOUT.as_secs());
                Err(Classified::Fault(if create {
                    Fault::Ambiguous(text)
                } else {
                    Fault::Transient(text)
                }))
            }
        }
    }

    /// One attempt of an operation on the primary.
    async fn attempt<T, F, Fut>(&self, what: &str, create: bool, op: F) -> Result<T, Fault>
    where
        F: FnOnce(Arc<Primary>) -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        self.attempt_classified(what, create, op)
            .await
            .map_err(|e| match e {
                Classified::Fault(f) => f,
                Classified::Config(m) => Fault::Permanent(m),
            })
    }

    /// `WAITAOF` on the connection of the last write: one short of the primary and its replicas
    /// leaves that write, and those before it, not durable yet.
    ///
    /// A separate round trip: with the write in the same pipeline, replicas acknowledge their
    /// fsync only at their next second.
    async fn durable(
        &self,
        primary: &Primary,
        conn: &mut MultiplexedConnection,
        what: &str,
        created: bool,
    ) -> Result<(), Failure> {
        let fault = |m: String| -> Failure {
            if created {
                Fault::Ambiguous(m).into()
            } else {
                Fault::Transient(m).into()
            }
        };
        let replicas = match self.replicas(primary, conn).await {
            Err(Failure::Fault(Fault::Transient(e))) => return Err(fault(format!("{what}: {e}"))),
            other => other?,
        };
        let waited: Result<Vec<i64>, _> = ::redis::cmd("WAITAOF")
            .arg(1)
            .arg(replicas)
            .arg(self.waitaof_ms.load(Ordering::Relaxed))
            .query_async(conn)
            .await;
        let answer = match waited {
            Ok(counts) => {
                if let [local, copies] = counts.as_slice()
                    && *local >= 1
                    && *copies >= i64::from(replicas)
                {
                    return Ok(());
                }
                let counts: Vec<String> = counts.iter().map(i64::to_string).collect();
                format!("[{}]", counts.join(", "))
            }
            Err(e) if answered(&e) => e.to_string(),
            Err(e) => return Err(e.into()),
        };
        if self.connector.location.replicas.is_none() {
            self.count(primary, conn).await?;
        }
        Err(fault(format!(
            "{what}: WAITAOF 1 {replicas} answered {answer}"
        )))
    }

    /// The replicas a create waits for, counted again every `RECOUNT_EVERY` for `all`.
    ///
    /// On Sentinel and Cluster, a primary with no replica online acknowledges nothing: just
    /// promoted, its replicas still syncing, it could itself be replaced by one of them, which
    /// would lack what it acknowledged alone.
    async fn replicas(
        &self,
        primary: &Primary,
        conn: &mut MultiplexedConnection,
    ) -> Result<u32, Failure> {
        if let Some(n) = self.connector.location.replicas {
            return Ok(n);
        }
        let counted = *primary.count.lock().expect("never poisoned");
        let replicas = if counted.at.elapsed() >= RECOUNT_EVERY {
            self.count(primary, conn).await?
        } else {
            counted.replicas
        };
        if replicas == 0 && self.connector.location.topology != Topology::Standalone {
            return Err(Fault::Transient(format!("{} has no replica online", primary.at)).into());
        }
        Ok(replicas)
    }

    async fn count(
        &self,
        primary: &Primary,
        conn: &mut MultiplexedConnection,
    ) -> Result<u32, Failure> {
        let info: String = ::redis::cmd("INFO")
            .arg("replication")
            .query_async(conn)
            .await?;
        let replicas = self.connector.replicas(&::redis::InfoDict::new(&info));
        *primary.count.lock().expect("never poisoned") = Count {
            replicas,
            at: Instant::now(),
        };
        Ok(replicas)
    }

    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.object)
    }

    async fn put(&self, primary: Arc<Primary>, key: &str, data: &Bytes) -> Result<Create, Failure> {
        let mut lease = primary.lease(&self.connector).await?;
        self.replicas(&primary, &mut lease.conn).await?;
        let created: i64 = ::redis::cmd("EVALSHA")
            .arg(&CREATE.sha)
            .arg(3)
            .arg(self.key(key))
            .arg(&self.index)
            .arg(&self.fence)
            .arg(key)
            .arg(data.as_ref())
            .arg(&self.channel)
            .arg(self.publish)
            .query_async(&mut lease.conn)
            .await?;
        let outcome = if created == 1 {
            Create::Created
        } else {
            Create::Exists
        };
        let what = format!("create {key}");
        self.durable(&primary, &mut lease.conn, &what, outcome == Create::Created)
            .await?;
        lease.keep();
        Ok(outcome)
    }

    /// A read answered once what it saw is durable: a fence written after it on this connection,
    /// then fsynced, covers it, as the AOF and the replication stream are one ordered log. A key
    /// once read never disappears in a failover, nor does one once missing come back, as history
    /// keys are deleted too, by collections. `resolving` marks the get that resolves an ambiguous
    /// create, ambiguous again when the fence falls short.
    async fn read(
        &self,
        primary: &Primary,
        what: &str,
        command: ::redis::Cmd,
        resolving: bool,
    ) -> Result<Value, Failure> {
        let mut lease = primary.lease(&self.connector).await?;
        let (found, _): (Value, i64) = ::redis::pipe()
            .add_command(command)
            .cmd("INCR")
            .arg(&self.fence)
            .query_async(&mut lease.conn)
            .await?;
        self.durable(primary, &mut lease.conn, what, resolving)
            .await?;
        lease.keep();
        Ok(found)
    }

    async fn get(
        &self,
        primary: Arc<Primary>,
        key: &str,
        resolving: bool,
    ) -> Result<Option<Bytes>, Failure> {
        let what = format!("get {key}");
        let command = ::redis::cmd("GET").arg(self.key(key)).to_owned();
        let found = self.read(&primary, &what, command, resolving).await?;
        let found: Option<Vec<u8>> = ::redis::from_redis_value(found)?;
        Ok(found.map(Bytes::from))
    }

    async fn page(
        &self,
        primary: Arc<Primary>,
        prefix: &str,
        after: Option<&str>,
        count: u64,
    ) -> Result<Vec<String>, Failure> {
        let mut lower = format!("[{prefix}").into_bytes();
        if let Some(after) = after
            && after >= prefix
        {
            lower = format!("({after}").into_bytes();
        }
        // UTF-8 has no byte 0xff, so every key under the prefix sorts before this bound.
        let mut upper = format!("({prefix}").into_bytes();
        upper.push(0xff);
        let command = ::redis::cmd("ZRANGE")
            .arg(&self.index)
            .arg(lower)
            .arg(upper)
            .arg("BYLEX")
            .arg("LIMIT")
            .arg(0)
            .arg(count)
            .to_owned();
        let what = format!("list {prefix}");
        let members = self.read(&primary, &what, command, false).await?;
        Ok(::redis::from_redis_value(members)?)
    }

    async fn remove(&self, primary: Arc<Primary>, key: &str) -> Result<(), Failure> {
        let mut lease = primary.lease(&self.connector).await?;
        let _: Value = ::redis::cmd("EVALSHA")
            .arg(&DELETE.sha)
            .arg(3)
            .arg(self.key(key))
            .arg(&self.index)
            .arg(&self.fence)
            .arg(key)
            .arg(&self.channel)
            .arg(self.publish)
            .query_async(&mut lease.conn)
            .await?;
        let what = format!("delete {key}");
        self.durable(&primary, &mut lease.conn, &what, false)
            .await?;
        lease.keep();
        Ok(())
    }
}

/// The etag of a waiting listing's page: its keys, and whether more follow.
fn etag(keys: &[String], more: bool) -> String {
    let mut digest = Sha256::new();
    for key in keys {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key.as_bytes());
    }
    digest.update([u8::from(more)]);
    hex::encode(digest.finalize())[..16].to_owned()
}

impl RedisStore {
    pub fn open(
        location: RedisLocation,
        credentials: &Credentials,
        retry: StoreRetry,
    ) -> Result<Self> {
        retry.validate()?;
        let auth = credentials.redis()?;
        let tag = format!("{{{}}}", location.namespace);
        let cluster = location.topology == Topology::Cluster;
        let channel = format!("{tag}:c");
        let shared = Shared {
            notifications: Notifications::new(channel.clone(), cluster),
            connector: Connector::new(location, auth),
            retry,
            object: format!("{tag}:o:"),
            index: format!("{tag}:i"),
            fence: format!("{tag}:f"),
            channel,
            publish: if cluster { "SPUBLISH" } else { "PUBLISH" },
            current: Mutex::default(),
            connecting: tokio::sync::Mutex::default(),
            waitaof_ms: AtomicU64::new(WAITAOF_MS),
        };
        Ok(Self {
            shared: Arc::new(shared),
        })
    }

    /// The `host:port` of the primary the store is connected to, if it is; for the tests.
    #[doc(hidden)]
    pub fn primary_address(&self) -> Option<String> {
        self.shared.current().map(|p| p.at.clone())
    }

    /// Takes the primary connected to for one with no replica online, as one just promoted; for
    /// the tests.
    #[doc(hidden)]
    pub fn assume_no_replica_online(&self) {
        if let Some(primary) = self.shared.current() {
            primary.count.lock().expect("never poisoned").replicas = 0;
        }
    }

    /// How long a create or a read waits for its fsyncs, `WAITAOF_MS` unless set; for the tests.
    #[doc(hidden)]
    pub fn set_waitaof_ms(&self, ms: u64) {
        self.shared.waitaof_ms.store(ms, Ordering::Relaxed);
    }

    async fn list_once(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
        wait: Option<&ListWait>,
    ) -> Result<Listing, Fault> {
        let shared = &self.shared;
        let what = format!("list {prefix}");
        let Some(wait) = wait else {
            let page = |p| shared.page(p, prefix, start_after, u64::from(max_keys));
            let keys = shared.attempt(&what, false, page).await?;
            return Ok(Listing { keys, etag: None });
        };
        if wait.seconds > MAX_WAIT_SECONDS {
            return Err(Fault::Permanent(format!(
                "a waiting listing waits 0 to {MAX_WAIT_SECONDS} s, not {}",
                wait.seconds
            )));
        }
        let deadline = Instant::now() + std::time::Duration::from_secs(wait.seconds.into());
        loop {
            // Watching before listing: a key landing after the page wakes the wait.
            let watch = match wait.seconds {
                0 => None,
                _ => Some(shared.notifications.watch(shared, prefix).await?),
            };
            let page = |p| shared.page(p, prefix, start_after, u64::from(max_keys) + 1);
            let mut keys = shared.attempt(&what, false, page).await?;
            let more = keys.len() > max_keys as usize;
            keys.truncate(max_keys as usize);
            let etag = etag(&keys, more);
            let listing = Listing {
                keys,
                etag: Some(etag),
            };
            let Some(watch) = watch else {
                return Ok(listing);
            };
            if listing.etag != wait.etag
                || tokio::time::timeout_at(deadline, watch.woken())
                    .await
                    .is_err()
            {
                return Ok(listing);
            }
        }
    }
}

impl Drop for RedisStore {
    fn drop(&mut self) {
        self.shared.notifications.stop();
    }
}

impl Store for RedisStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: true }
    }

    fn open(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let shared = &self.shared;
            let config = Mutex::new(None);
            let connected = || async {
                let attempt = shared.attempt_classified("connect", false, |_| async { Ok(()) });
                match attempt.await {
                    Ok(()) => Ok(()),
                    Err(Classified::Fault(f)) => Err(f),
                    Err(Classified::Config(m)) => {
                        *config.lock().expect("never poisoned") = Some(m.clone());
                        Err(Fault::Permanent(m))
                    }
                }
            };
            let outcome = retry::with_retry(shared.retry, connected, tokio::time::sleep).await;
            if let Some(m) = config.into_inner().expect("never poisoned") {
                return Err(Error::ConfigError(m));
            }
            outcome.map_err(|e| Error::StoreError(e.message))
        })
    }

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            let shared = &self.shared;
            let (create, get) = (format!("create {key}"), format!("get {key}"));
            retry::create(
                shared.retry,
                &bytes,
                || shared.attempt(&create, true, |p| shared.put(p, key, &bytes)),
                || shared.attempt(&get, false, |p| shared.get(p, key, true)),
                tokio::time::sleep,
            )
            .await
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let shared = &self.shared;
            let what = format!("get {key}");
            let attempt = || shared.attempt(&what, false, |p| shared.get(p, key, false));
            retry::with_retry(shared.retry, attempt, tokio::time::sleep).await
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        Box::pin(async move {
            let attempt = || self.list_once(prefix, start_after, max_keys, wait.as_ref());
            retry::with_retry(self.shared.retry, attempt, tokio::time::sleep).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let shared = &self.shared;
            let what = format!("delete {key}");
            let attempt = || shared.attempt(&what, false, |p| shared.remove(p, key));
            retry::with_retry(shared.retry, attempt, tokio::time::sleep).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etags_tell_pages_apart() {
        let page = |keys: &[&str]| keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
        assert_eq!(etag(&page(&["a"]), false).len(), 16);
        assert_ne!(etag(&page(&["a"]), false), etag(&page(&["a"]), true));
        assert_ne!(etag(&page(&["ab"]), false), etag(&page(&["a", "b"]), false));
        assert_eq!(etag(&page(&[]), false), etag(&page(&[]), false));
    }

    #[test]
    fn scripts_ship_with_their_hashes() {
        assert!(CREATE.text.contains("SET"));
        assert_eq!(DELETE.sha.len(), 40);
    }
}
