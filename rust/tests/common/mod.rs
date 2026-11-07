//! Stores and settings shared by the integration tests.

#![allow(dead_code)]

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use substantial::__internal::{
    self as internal, Capabilities, Create, FsStore, ListWait, Listing, MemoryStore, Store,
    StoreFailure, StoreFuture,
};
use substantial::{Credentials, IntoWorkflows, Preset, Substantial, WorkerBuilder};
use tokio::time::Instant;

/// A store around another that records what it is asked, and can fail or freeze on demand.
pub struct Recording {
    inner: Arc<dyn Store>,
    pub listed: Mutex<Vec<String>>,
    pub created: Mutex<Vec<(String, Create)>>,
    pub failing: AtomicBool,
    /// When set, the first create of a segment freezes every operation for this long.
    pub freeze_on_segment: Mutex<Option<Duration>>,
    frozen_until: Mutex<Option<Instant>>,
}

impl Recording {
    pub fn new(inner: Arc<dyn Store>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            listed: Mutex::default(),
            created: Mutex::default(),
            failing: AtomicBool::new(false),
            freeze_on_segment: Mutex::default(),
            frozen_until: Mutex::default(),
        })
    }

    pub fn listed(&self) -> Vec<String> {
        self.listed.lock().unwrap().clone()
    }

    pub fn created(&self) -> Vec<(String, Create)> {
        self.created.lock().unwrap().clone()
    }

    async fn gate(&self) -> Result<(), StoreFailure> {
        let until = *self.frozen_until.lock().unwrap();
        if let Some(until) = until {
            tokio::time::sleep_until(until).await;
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

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            if key.contains("/log/")
                && let Some(freeze) = self.freeze_on_segment.lock().unwrap().take()
            {
                *self.frozen_until.lock().unwrap() = Some(Instant::now() + freeze);
            }
            self.gate().await?;
            let outcome = self.inner.create(key, bytes).await?;
            self.created.lock().unwrap().push((key.to_owned(), outcome));
            Ok(outcome)
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
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
            self.gate().await?;
            self.inner.delete(key).await
        })
    }
}

/// An operation as S3 bills it: a create is a conditional `PutObject`, a listing a
/// `ListObjectsV2`, waiting or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Put,
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
    /// Of a create.
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
}

impl Counting {
    pub fn new(inner: Arc<dyn Store>, latency: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            latency,
            ops: Mutex::default(),
        })
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

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        let len = bytes.len();
        Box::pin(self.timed(Kind::Put, key, len, self.inner.create(key, bytes)))
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
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

/// Operations by kind, and the round trips on their critical path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub put: usize,
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
        self.put + self.get + self.list + self.wait + self.delete
    }
}

/// A fresh namespace in each adapter of this host, the temporary directory kept alive with it.
pub struct Namespace {
    pub name: &'static str,
    pub store: Arc<dyn Store>,
    _dir: Option<tempfile::TempDir>,
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
    let mut namespaces = vec![
        Namespace {
            name: "memory",
            store: Arc::new(MemoryStore::open(&ns).unwrap()),
            _dir: None,
        },
        Namespace {
            name: "file",
            store: Arc::new(fs),
            _dir: Some(dir),
        },
    ];
    if let Some(url) = s3_url("s3") {
        namespaces.push(Namespace {
            name: "s3",
            store: internal::open(&url, &Credentials::Env, retry()).unwrap(),
            _dir: None,
        });
    }
    namespaces
}

pub const S3_URL: &str = "SUBSTANTIAL_S3_URL";

/// A fresh namespace under the one `SUBSTANTIAL_S3_URL` names, as a URL of `scheme` (`s3` or
/// `h3`, its `+http` kept); `None`, said on stderr, when the variable is not set. Its
/// credentials are `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. See `dev/rustfs.compose.yaml`.
pub fn s3_url(scheme: &str) -> Option<String> {
    let url = match std::env::var(S3_URL) {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent) => {
            eprintln!("skipping the S3 store: {S3_URL} is not set");
            return None;
        }
        Err(e) => panic!("{S3_URL}: {e}"),
    };
    let (given, rest) = url
        .split_once("://")
        .unwrap_or_else(|| panic!("{S3_URL} is not a URL: {url:?}"));
    let plain = match given {
        "s3" => "",
        "s3+http" => "+http",
        _ => panic!("{S3_URL} is not an s3:// or s3+http:// URL: {url:?}"),
    };
    let (path, query) = rest
        .split_once('?')
        .unwrap_or_else(|| panic!("{S3_URL} has no query: {url:?}"));
    let unique = hex::encode(rand::random::<[u8; 8]>());
    Some(format!("{scheme}{plain}://{path}/{unique}?{query}"))
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

pub const LONG: Duration = Duration::from_secs(10);
