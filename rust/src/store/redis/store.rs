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
    ATTEMPT_TIMEOUT, CAS, CREATE, Connector, Count, DELETE, Failure, Primary, RECOUNT_EVERY,
    WAITAOF_MS, answered, refused,
};
use super::notify::Notifications;
use crate::error::{Error, Result};
use crate::settings::StoreRetry;
use crate::store::credentials::Credentials;
use crate::store::retry::{self, Fault};
use crate::store::{
    Capabilities, Cas, Create, DAY_MS, ListWait, Listing, Object, Store, StoreFailure, StoreFuture,
    check_days,
};

/// The longest a waiting listing waits.
const MAX_WAIT_SECONDS: u32 = 20;

/// `redis://`, `redis+sentinel://` and `redis+cluster://`: the namespace `<ns>` is the hash tag
/// of its keys, so they live on one slot. A key is the string `{<ns>}:o:<key>` and a member of
/// the sorted set `{<ns>}:i`, its index, listed by lex range, its ETag the field of the key in the
/// hash `{<ns>}:t`; every create, cas and delete publishes the key on `{<ns>}:c`, which waiting
/// listings subscribe to.
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
    expiries: String,
    etags: String,
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
        // § htbc4k715@2jtmw
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
        // § dfd6vt90h.e@pva14
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
        // § end dfd6vt90h.e
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
            // § htbc4k715.c@kwsp3
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

    async fn put(
        &self,
        primary: Arc<Primary>,
        key: &str,
        data: &Bytes,
        expiry: Option<u32>,
    ) -> Result<Create, Failure> {
        let mut lease = primary.lease(&self.connector).await?;
        self.replicas(&primary, &mut lease.conn).await?;
        let created: i64 = ::redis::cmd("EVALSHA")
            .arg(&CREATE.sha)
            .arg(5)
            .arg(self.key(key))
            .arg(&self.index)
            .arg(&self.fence)
            .arg(&self.expiries)
            .arg(&self.etags)
            .arg(key)
            .arg(data.as_ref())
            .arg(&self.channel)
            .arg(self.publish)
            .arg(expiry_ms(expiry))
            .query_async(&mut lease.conn)
            .await?;
        let outcome = if created == 1 {
            Create::Created {
                etag: sha1_etag(data),
            }
        } else {
            Create::Exists
        };
        let what = format!("create {key}");
        let landed = matches!(outcome, Create::Created { .. });
        self.durable(&primary, &mut lease.conn, &what, landed)
            .await?;
        lease.keep();
        Ok(outcome)
    }

    async fn put_if_match(
        &self,
        primary: Arc<Primary>,
        key: &str,
        data: &Bytes,
        etag: &str,
        expiry: Option<u32>,
    ) -> Result<Cas, Failure> {
        // § rz3se15bd@z744z
        let mut lease = primary.lease(&self.connector).await?;
        self.replicas(&primary, &mut lease.conn).await?;
        let written: i64 = ::redis::cmd("EVALSHA")
            .arg(&CAS.sha)
            .arg(5)
            .arg(self.key(key))
            .arg(&self.index)
            .arg(&self.fence)
            .arg(&self.expiries)
            .arg(&self.etags)
            .arg(key)
            .arg(data.as_ref())
            .arg(etag)
            .arg(expiry_ms(expiry))
            .arg(&self.channel)
            .arg(self.publish)
            .query_async(&mut lease.conn)
            .await?;
        let outcome = match written {
            1 => Cas::Written {
                etag: sha1_etag(data),
            },
            _ => Cas::Mismatch,
        };
        let what = format!("cas {key}");
        // § one htbc4k715.g@fd2q9
        let written = matches!(outcome, Cas::Written { .. });
        self.durable(&primary, &mut lease.conn, &what, written)
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
        // § htbc4k715.a@b7qf9
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
    ) -> Result<Option<Object>, Failure> {
        let what = format!("get {key}");
        let command = ::redis::cmd("GET").arg(self.key(key)).to_owned();
        let found = self.read(&primary, &what, command, resolving).await?;
        let found: Option<Vec<u8>> = ::redis::from_redis_value(found)?;
        Ok(found.map(|bytes| Object {
            etag: sha1_etag(&bytes),
            bytes: Bytes::from(bytes),
        }))
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

    /// The ETags of `keys` the scripts keep, none for a key written before they kept them.
    async fn etags_of(
        &self,
        primary: Arc<Primary>,
        keys: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, Failure> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut lease = primary.lease(&self.connector).await?;
        let etags: Vec<Option<Vec<u8>>> = ::redis::cmd("HMGET")
            .arg(&self.etags)
            .arg(keys)
            .query_async(&mut lease.conn)
            .await?;
        lease.keep();
        Ok(etags)
    }

    async fn remove(&self, primary: Arc<Primary>, key: &str) -> Result<(), Failure> {
        // § htbc4k715.a@zy849
        let mut lease = primary.lease(&self.connector).await?;
        let _: Value = ::redis::cmd("EVALSHA")
            .arg(&DELETE.sha)
            .arg(5)
            .arg(self.key(key))
            .arg(&self.index)
            .arg(&self.fence)
            .arg(&self.expiries)
            .arg(&self.etags)
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

/// The expiry of a write in ms for the scripts, 0 for none.
fn expiry_ms(expiry: Option<u32>) -> u64 {
    // § hxvcx7f2a@r18ph
    expiry.map_or(0, |d| u64::from(d) * DAY_MS)
}

/// The ETag of a value: the digest the compare-and-set script computes with `redis.sha1hex`.
fn sha1_etag(bytes: &[u8]) -> String {
    use sha1::Digest;
    hex::encode(sha1::Sha1::digest(bytes))
}

/// The etag of a waiting listing's page: its keys, their ETags, and whether more follow, so a page
/// changes when a key under it is added, removed or rewritten; as every host computes it.
fn etag(keys: &[String], etags: &[Option<Vec<u8>>], more: bool) -> String {
    let mut digest = Sha256::new();
    for (key, etag) in keys.iter().zip(etags) {
        for part in [key.as_bytes(), etag.as_deref().unwrap_or_default()] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
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
        // § htbc4k715.d@b4fg8
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
            expiries: format!("{tag}:e"),
            etags: format!("{tag}:t"),
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
            let etags = |p| shared.etags_of(p, &keys);
            let etags = shared.attempt(&what, false, etags).await?;
            let etag = etag(&keys, &etags, more);
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

    fn create<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            check_days(expiry).map_err(StoreFailure::new)?;
            let shared = &self.shared;
            let (create, get) = (format!("create {key}"), format!("get {key}"));
            let read = || shared.attempt(&get, false, |p| shared.get(p, key, true));
            retry::create(
                shared.retry,
                &bytes,
                || shared.attempt(&create, true, |p| shared.put(p, key, &bytes, expiry)),
                read,
                tokio::time::sleep,
            )
            .await
        })
    }

    fn cas<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        etag: &'a str,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Cas> {
        Box::pin(async move {
            check_days(expiry).map_err(StoreFailure::new)?;
            let shared = &self.shared;
            let (cas, get) = (format!("cas {key}"), format!("get {key}"));
            retry::cas(
                shared.retry,
                &bytes,
                etag,
                || {
                    shared.attempt(&cas, true, |p| {
                        shared.put_if_match(p, key, &bytes, etag, expiry)
                    })
                },
                || shared.attempt(&get, false, |p| shared.get(p, key, true)),
                tokio::time::sleep,
            )
            .await
        })
    }

    fn check_expiry<'a>(&'a self, days: &'a [u32]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { crate::store::check_all_days(days) })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
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
        let none = |n: usize| vec![None; n];
        let tag = |t: &str| vec![Some(t.as_bytes().to_vec())];
        assert_eq!(etag(&page(&["a"]), &none(1), false).len(), 16);
        assert_ne!(
            etag(&page(&["a"]), &none(1), false),
            etag(&page(&["a"]), &none(1), true)
        );
        assert_ne!(
            etag(&page(&["ab"]), &none(1), false),
            etag(&page(&["a", "b"]), &none(2), false)
        );
        assert_ne!(
            etag(&page(&["a"]), &tag("1"), false),
            etag(&page(&["a"]), &tag("2"), false)
        );
        assert_eq!(etag(&page(&[]), &[], false), etag(&page(&[]), &[], false));
    }

    /// The bytes Python's `_page_etag` hashes, so that every host's listing etags agree.
    #[test]
    fn etags_are_the_hosts_shared_digest() {
        let keys = ["runs/a".to_owned(), "runs/b".to_owned()];
        let etags = [Some(b"0123".to_vec()), None];
        let mut bytes = Vec::new();
        for (key, etag) in [("runs/a", &b"0123"[..]), ("runs/b", &b""[..])] {
            for part in [key.as_bytes(), etag] {
                bytes.extend((part.len() as u64).to_be_bytes());
                bytes.extend(part);
            }
        }
        bytes.push(1);
        let expected = &hex::encode(Sha256::digest(&bytes))[..16];
        assert_eq!(etag(&keys, &etags, true), expected);
        // Python's `_page_etag(["runs/a", "runs/b"], [b"0123", None], True)`.
        assert_eq!(expected, "ea4628c48d64bf4d");
    }

    #[test]
    fn scripts_ship_with_their_hashes() {
        assert!(CREATE.text.contains("SET") && CAS.text.contains("sha1hex"));
        assert_eq!(DELETE.sha.len(), 40);
    }
}
