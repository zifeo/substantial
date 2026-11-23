//! Stores and settings shared by the integration tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use substantial::__internal::{
    self as internal, Capabilities, Cas, Create, FsStore, ListWait, Listing, MemoryStore, Object,
    Store, StoreFailure, StoreFuture, layout,
};
use substantial::{Credentials, IntoWorkflows, Preset, Substantial, WorkerBuilder};
use tokio::sync::watch;
use tokio::time::Instant;

/// A store around another that records what it is asked, and can fail or freeze on demand.
pub struct Recording {
    inner: Arc<dyn Store>,
    pub listed: Mutex<Vec<String>>,
    pub created: Mutex<Vec<(String, Create)>>,
    pub cased: Mutex<Vec<(String, Cas)>>,
    /// Every create and cas asked, with the expiry it carries.
    pub writes: Mutex<Vec<(String, Option<u32>)>>,
    /// The days of every expiry check asked.
    pub checked: Mutex<Vec<Vec<u32>>>,
    pub failing: AtomicBool,
    /// Whether every write of a segment fails, the other operations going through.
    pub failing_segments: AtomicBool,
    /// The writes of a segment `failing_segments` failed.
    pub failed_segments: AtomicU32,
    /// When set, the first write of a segment freezes every operation until this sees `true`
    /// or its sender is dropped.
    pub freeze_on_segment: Mutex<Option<watch::Receiver<bool>>>,
    frozen: Mutex<Option<watch::Receiver<bool>>>,
    /// When set, a delete of this key waits until its receiver sees `true` or its sender is
    /// dropped.
    pub hold_delete: Mutex<Option<(String, watch::Receiver<bool>)>>,
}

impl Recording {
    pub fn new(inner: Arc<dyn Store>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            listed: Mutex::default(),
            created: Mutex::default(),
            cased: Mutex::default(),
            writes: Mutex::default(),
            checked: Mutex::default(),
            failing: AtomicBool::new(false),
            failing_segments: AtomicBool::new(false),
            failed_segments: AtomicU32::new(0),
            freeze_on_segment: Mutex::default(),
            frozen: Mutex::default(),
            hold_delete: Mutex::default(),
        })
    }

    pub fn listed(&self) -> Vec<String> {
        self.listed.lock().unwrap().clone()
    }

    pub fn created(&self) -> Vec<(String, Create)> {
        self.created.lock().unwrap().clone()
    }

    pub fn cased(&self) -> Vec<(String, Cas)> {
        self.cased.lock().unwrap().clone()
    }

    pub fn writes(&self) -> Vec<(String, Option<u32>)> {
        self.writes.lock().unwrap().clone()
    }

    /// Whether a cas of `bytes` on `key` writes a segment: a chain's head holding more segments
    /// than the one the store holds, a lease or a clearing write adding none.
    async fn writes_segment(&self, key: &str, bytes: &[u8]) -> Result<bool, StoreFailure> {
        if !key
            .strip_prefix("runs/")
            .is_some_and(|id| !id.contains('/'))
        {
            return Ok(false);
        }
        let segments = |bytes: &[u8]| match layout::Run::decode(bytes).map(|r| r.kind) {
            Ok(Some(layout::run::Kind::Head(head))) => head.segments.len(),
            _ => 0,
        };
        let stored = self.inner.get(key).await?;
        Ok(segments(bytes) > stored.map_or(0, |o| segments(&o.bytes)))
    }

    /// Freezes every operation from now on when a freeze is set, and fails when segments fail.
    async fn segment(&self) -> Result<(), StoreFailure> {
        if let Some(freeze) = self.freeze_on_segment.lock().unwrap().take() {
            *self.frozen.lock().unwrap() = Some(freeze);
        }
        self.gate().await?;
        if self.failing_segments.load(Ordering::SeqCst) {
            self.failed_segments.fetch_add(1, Ordering::SeqCst);
            return Err(StoreFailure::new("injected"));
        }
        Ok(())
    }

    async fn gate(&self) -> Result<(), StoreFailure> {
        let frozen = self.frozen.lock().unwrap().clone();
        if let Some(mut frozen) = frozen {
            let _ = frozen.wait_for(|thawed| *thawed).await;
        }
        if self.failing.load(Ordering::SeqCst) {
            return Err(StoreFailure::new("injected"));
        }
        Ok(())
    }
}

impl Store for Recording {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn open(&self) -> futures::future::BoxFuture<'_, substantial::Result<()>> {
        self.inner.open()
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            self.writes.lock().unwrap().push((key.to_owned(), expiry));
            self.gate().await?;
            let outcome = self.inner.create(key, bytes, expiry).await?;
            self.created
                .lock()
                .unwrap()
                .push((key.to_owned(), outcome.clone()));
            Ok(outcome)
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
            self.writes.lock().unwrap().push((key.to_owned(), expiry));
            if self.writes_segment(key, &bytes).await? {
                self.segment().await?;
            } else {
                self.gate().await?;
            }
            let outcome = self.inner.cas(key, bytes, etag, expiry).await?;
            self.cased
                .lock()
                .unwrap()
                .push((key.to_owned(), outcome.clone()));
            Ok(outcome)
        })
    }

    fn check_expiry<'a>(
        &'a self,
        days: &'a [u32],
    ) -> futures::future::BoxFuture<'a, substantial::Result<()>> {
        self.checked.lock().unwrap().push(days.to_vec());
        self.inner.check_expiry(days)
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
        Box::pin(async move {
            self.gate().await?;
            self.inner.get(key).await
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
            self.listed.lock().unwrap().push(prefix.to_owned());
            self.gate().await?;
            self.inner.list(prefix, start_after, max_keys, wait).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let held = self.hold_delete.lock().unwrap().clone();
            if let Some((held, mut released)) = held
                && held == key
            {
                let _ = released.wait_for(|released| *released).await;
            }
            self.gate().await?;
            self.inner.delete(key).await
        })
    }
}

/// A create's outcome without the ETag it carries.
pub fn outcome(created: &Create) -> &'static str {
    match created {
        Create::Created { .. } => "created",
        Create::Exists => "exists",
    }
}

/// `outcome` of a create's answer.
pub fn kind(created: Result<Create, StoreFailure>) -> Result<&'static str, StoreFailure> {
    created.map(|c| outcome(&c))
}

/// An operation as S3 bills it: a create is a conditional `PutObject`, a cas a `PutObject` with
/// `If-Match`, a listing a `ListObjectsV2`, waiting or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Put,
    Cas,
    Get,
    List,
    /// A listing held until its page changes, sent only to a store whose listings wait.
    Wait,
    Delete,
}

#[derive(Clone, Debug)]
pub struct Op {
    pub kind: Kind,
    pub key: String,
    /// Of a create or a cas.
    pub bytes: usize,
    pub started: Instant,
    pub ended: Instant,
}

/// A store around another that logs every operation with its kind, key and timing, each one
/// held for `latency` first so that operations in flight together overlap.
pub struct Counting {
    inner: Arc<dyn Store>,
    latency: Duration,
    ops: Mutex<Vec<Op>>,
    /// The expiry of each key's last create or cas.
    expiries: Mutex<HashMap<String, Option<u32>>>,
}

impl Counting {
    pub fn new(inner: Arc<dyn Store>, latency: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            latency,
            ops: Mutex::default(),
            expiries: Mutex::default(),
        })
    }

    /// The expiry the last write of `key` carried.
    pub fn expiry(&self, key: &str) -> Option<u32> {
        *self
            .expiries
            .lock()
            .unwrap()
            .get(key)
            .expect("a key written")
    }

    /// The operations ended so far: a mark to read the ones after.
    pub fn mark(&self) -> usize {
        self.ops.lock().unwrap().len()
    }

    pub fn since(&self, mark: usize) -> Vec<Op> {
        self.ops.lock().unwrap()[mark..].to_vec()
    }

    async fn timed<T>(
        &self,
        kind: Kind,
        key: &str,
        bytes: usize,
        op: impl Future<Output = Result<T, StoreFailure>>,
    ) -> Result<T, StoreFailure> {
        let started = Instant::now();
        tokio::time::sleep(self.latency).await;
        let out = op.await;
        let ended = Instant::now();
        let key = key.to_owned();
        self.ops.lock().unwrap().push(Op {
            kind,
            key,
            bytes,
            started,
            ended,
        });
        out
    }
}

impl Store for Counting {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn open(&self) -> futures::future::BoxFuture<'_, substantial::Result<()>> {
        self.inner.open()
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Create> {
        let len = bytes.len();
        self.expiries.lock().unwrap().insert(key.to_owned(), expiry);
        let created = self.inner.create(key, bytes, expiry);
        Box::pin(self.timed(Kind::Put, key, len, created))
    }

    fn cas<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        etag: &'a str,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Cas> {
        let len = bytes.len();
        self.expiries.lock().unwrap().insert(key.to_owned(), expiry);
        let written = self.inner.cas(key, bytes, etag, expiry);
        Box::pin(self.timed(Kind::Cas, key, len, written))
    }

    fn check_expiry<'a>(
        &'a self,
        days: &'a [u32],
    ) -> futures::future::BoxFuture<'a, substantial::Result<()>> {
        self.inner.check_expiry(days)
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
        Box::pin(self.timed(Kind::Get, key, 0, self.inner.get(key)))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        // A wait of 0 s answers at once: a plain listing that also returns the page's etag.
        let kind = match &wait {
            Some(w) if w.seconds > 0 => Kind::Wait,
            _ => Kind::List,
        };
        let listed = self.inner.list(prefix, start_after, max_keys, wait);
        Box::pin(self.timed(kind, prefix, 0, listed))
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(self.timed(Kind::Delete, key, 0, self.inner.delete(key)))
    }
}

/// Operations by kind, and the round trips on their critical path: `put` counts the creates,
/// `cas` the compare-and-sets, both of S3's PUT class.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub put: usize,
    pub cas: usize,
    pub get: usize,
    pub list: usize,
    pub wait: usize,
    pub delete: usize,
    pub trips: usize,
}

impl Tally {
    /// Round trips are the most operations no two of which were in flight at once: each takes
    /// at least the store's latency, so operations sent together overlap and count once, and
    /// operations sent one after the other count each.
    pub fn of(ops: &[Op]) -> Self {
        let mut t = Tally::default();
        for op in ops {
            match op.kind {
                Kind::Put => t.put += 1,
                Kind::Cas => t.cas += 1,
                Kind::Get => t.get += 1,
                Kind::List => t.list += 1,
                Kind::Wait => t.wait += 1,
                Kind::Delete => t.delete += 1,
            }
        }
        let mut by_end: Vec<&Op> = ops.iter().collect();
        by_end.sort_by_key(|op| op.ended);
        let mut last: Option<Instant> = None;
        for op in by_end {
            if last.is_none_or(|end| op.started >= end) {
                t.trips += 1;
                last = Some(op.ended);
            }
        }
        t
    }

    pub fn ops(&self) -> usize {
        self.put + self.cas + self.get + self.list + self.wait + self.delete
    }
}

/// A fresh namespace in each adapter of this host, the temporary directory kept alive with it.
pub struct Namespace {
    pub name: &'static str,
    pub store: Arc<dyn Store>,
    _dir: Option<tempfile::TempDir>,
    _failover: Option<FailoverLock>,
}

/// The lock every test process of the machine takes, whatever its host, as Python's
/// `failover_exclusion` does: shared by a test that uses the Redis stores, exclusive by one that
/// stops their containers or changes a server's configuration. The kernel releases it if the
/// process dies.
pub struct FailoverLock(#[allow(dead_code)] std::fs::File);

/// Takes the lock, waiting for it: call it first in a test, and keep it to the test's end.
pub fn failover_lock(exclusive: bool) -> FailoverLock {
    let path = std::env::temp_dir().join("substantial-tests-failover.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let locked = if exclusive {
        file.lock()
    } else {
        file.lock_shared()
    };
    locked.unwrap_or_else(|e| panic!("lock {}: {e}", path.display()));
    FailoverLock(file)
}

pub fn namespaces() -> Vec<Namespace> {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let ns = format!(
        "e2e-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let dir = tempfile::tempdir().unwrap();
    let fs = FsStore::open(dir.path().join("ns"), retry()).unwrap();
    let namespaces = vec![
        Namespace {
            name: "memory",
            store: Arc::new(MemoryStore::open(&ns).unwrap()),
            _dir: None,
            _failover: None,
        },
        Namespace {
            name: "file",
            store: Arc::new(fs),
            _dir: Some(dir),
            _failover: None,
        },
        Namespace {
            name: "s3",
            store: internal::open(&s3_url("s3"), &s3_credentials(), retry()).unwrap(),
            _dir: None,
            _failover: None,
        },
    ];
    #[cfg(feature = "redis")]
    let namespaces = (namespaces.into_iter())
        .chain(REDIS_URLS.map(|(name, _)| Namespace {
            name,
            store: internal::open(&redis_url(name), &Credentials::None, retry()).unwrap(),
            _dir: None,
            _failover: Some(failover_lock(false)),
        }))
        .collect();
    namespaces
}

/// What starts the stores of the tests, named by every failure to reach one.
pub const UP: &str = "docker compose -f dev/compose.yaml up -d --wait";

/// A setting of the test stores: the environment's, else the one of `dev/stores.env`.
pub fn setting(name: &str) -> String {
    static FILE: OnceLock<HashMap<String, String>> = OnceLock::new();
    match std::env::var(name) {
        Ok(value) => return value,
        Err(std::env::VarError::NotPresent) => {}
        Err(e) => panic!("{name}: {e}"),
    }
    let file = FILE.get_or_init(|| {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../dev/stores.env");
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
        (text.lines().enumerate())
            .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
            .map(|(n, line)| {
                let (name, value) = line
                    .split_once('=')
                    .unwrap_or_else(|| panic!("{path}:{} is not NAME=value: {line:?}", n + 1));
                (name.to_owned(), value.to_owned())
            })
            .collect()
    });
    file.get(name)
        .unwrap_or_else(|| panic!("{name} is set neither in the environment nor in dev/stores.env"))
        .clone()
}

/// The URL `variable` names, once each of its hosts accepts a connection.
fn reached(variable: &str) -> String {
    let url = setting(variable);
    assert!(!url.is_empty(), "{variable} is empty: run `{UP}`");
    let hosts = url.split_once("://").map_or("", |(_, rest)| rest);
    for address in hosts.split('/').next().unwrap_or("").split(',') {
        let reached = (address.to_socket_addrs().ok().into_iter().flatten())
            .any(|at| TcpStream::connect_timeout(&at, Duration::from_secs(2)).is_ok());
        assert!(
            reached,
            "{variable} names {address}, which does not answer: run `{UP}`"
        );
    }
    url
}

/// The keys of the S3 store of the tests, given explicitly so that no test changes the process's
/// environment.
pub fn s3_credentials() -> Credentials {
    Credentials::S3 {
        access_key_id: setting("AWS_ACCESS_KEY_ID"),
        secret_access_key: setting("AWS_SECRET_ACCESS_KEY"),
        session_token: None,
    }
}

/// The variable naming the namespace of each Redis topology.
pub const REDIS_URLS: [(&str, &str); 3] = [
    ("redis", "SUBSTANTIAL_REDIS_URL"),
    ("redis-sentinel", "SUBSTANTIAL_REDIS_SENTINEL_URL"),
    ("redis-cluster", "SUBSTANTIAL_REDIS_CLUSTER_URL"),
];

/// A fresh namespace under the one the variable of `topology` names.
pub fn redis_url(topology: &str) -> String {
    let (_, variable) = REDIS_URLS
        .iter()
        .find(|(name, _)| *name == topology)
        .unwrap_or_else(|| panic!("no Redis topology {topology}"));
    let url = reached(variable);
    let (path, query) = url.split_once('?').unwrap_or((&url, ""));
    let unique = hex::encode(rand::random::<[u8; 8]>());
    format!("{path}/{unique}?{query}")
}

pub const S3_URL: &str = "SUBSTANTIAL_S3_URL";
/// The versioned bucket, which RustFS serves short of the store contract: only to check that.
pub const S3_VERSIONED_URL: &str = "SUBSTANTIAL_S3_VERSIONED_URL";

/// A fresh namespace under the one `SUBSTANTIAL_S3_URL` names, as a URL of `scheme` (`s3` or
/// `h3`, its `+http` kept), whose keys are `s3_credentials`.
pub fn s3_url(scheme: &str) -> String {
    s3_url_of(S3_URL, scheme)
}

/// As `s3_url`, under the namespace `variable` names.
pub fn s3_url_of(variable: &str, scheme: &str) -> String {
    let url = reached(variable);
    let (given, rest) = url
        .split_once("://")
        .unwrap_or_else(|| panic!("{variable} is not a URL: {url:?}"));
    let plain = match given {
        "s3" => "",
        "s3+http" => "+http",
        _ => panic!("{variable} is not an s3:// or s3+http:// URL: {url:?}"),
    };
    let (path, query) = rest
        .split_once('?')
        .unwrap_or_else(|| panic!("{variable} has no query: {url:?}"));
    let unique = hex::encode(rand::random::<[u8; 8]>());
    format!("{scheme}{plain}://{path}/{unique}?{query}")
}

pub fn retry() -> internal::StoreRetry {
    internal::STORE_RETRY
}

/// The settings a worker refuses, set through the internal path.
pub trait Hidden {
    fn hidden(self, name: &str, value: impl ToString) -> Self;
}

impl Hidden for WorkerBuilder {
    fn hidden(self, name: &str, value: impl ToString) -> Self {
        internal::set(self, name, value)
    }
}

/// A worker of the script preset, listing often so that tests need not wait for `max_idle`.
pub fn script(sub: &Substantial, workflows: impl IntoWorkflows) -> WorkerBuilder {
    sub.worker(workflows)
        .preset(Preset::Script)
        .set("max_idle", "50ms")
}

/// Waits for `check` to hold, polling, at most five seconds.
pub async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until the chain's last run reads blocked, then the error `runs` lists it with: the
/// block creates its hint before its queue's marker, so `runs` may list the run a moment later.
pub async fn blocked<W: substantial::AsWorkflow>(
    sub: &Substantial,
    workflow: W,
    chain: &str,
) -> substantial::Failure {
    use futures::TryStreamExt;
    let run = sub.run(&workflow, chain);
    let deadline = Instant::now() + LONG;
    loop {
        if run.status().await.unwrap() == substantial::RunStatus::Blocked {
            let listed: Vec<substantial::RunInfo> =
                sub.runs(&workflow).try_collect().await.unwrap();
            let info = listed
                .into_iter()
                .find(|i| i.chain == chain && i.waiting_on == substantial::WaitingOn::Blocked);
            if let Some(info) = info {
                assert!(info.due_at.is_some(), "a blocked run is tried again");
                return info.error.expect("a blocked run is listed with its error");
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {chain} to block"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// A guard against hangs, not a bound.
pub const LONG: Duration = Duration::from_secs(30);

/// Pushes `interrupted` when dropped: held across an `.await`, it tells a future dropped there;
/// forgotten once the code after it ran.
pub struct Interrupted(pub Arc<Mutex<Vec<&'static str>>>);

impl Drop for Interrupted {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("interrupted");
    }
}
