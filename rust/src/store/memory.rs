use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use futures::future::BoxFuture;

use super::{
    Capabilities, Cas, Create, DAY_MS, ListWait, Listing, Object, Store, StoreFailure, StoreFuture,
    check_days, digest_etag,
};
use crate::error::Result;

/// A value and the instant, in ms since the epoch, it expires at.
#[derive(Clone)]
struct Entry {
    bytes: Bytes,
    expires: Option<i64>,
}

type Objects = Arc<Mutex<BTreeMap<String, Entry>>>;

/// Milliseconds since the epoch: the wall clock, or a test's.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

pub(crate) fn wall_clock() -> Clock {
    Arc::new(|| crate::time::now().ms)
}

fn namespaces() -> std::sync::MutexGuard<'static, HashMap<String, Objects>> {
    static NAMESPACES: OnceLock<Mutex<HashMap<String, Objects>>> = OnceLock::new();
    NAMESPACES
        .get_or_init(Default::default)
        .lock()
        .expect("registry")
}

/// `memory://<ns>`: a namespace held by the process, for tests. Every store opened on the same
/// `<ns>` in a process shares its objects.
#[derive(Clone)]
pub struct MemoryStore {
    objects: Objects,
    clock: Clock,
}

impl MemoryStore {
    pub fn open(ns: &str) -> Result<Self> {
        Self::with_clock(ns, wall_clock())
    }

    /// The namespace `ns`, its expiries judged by `clock`, in ms since the epoch.
    #[doc(hidden)]
    pub fn with_clock(ns: &str, clock: Clock) -> Result<Self> {
        // § e9ern38bz.d@33y7d
        let objects = namespaces().entry(ns.to_owned()).or_default().clone();
        Ok(Self { objects, clock })
    }

    /// Whether the process holds the namespace `ns`.
    pub fn exists(ns: &str) -> bool {
        namespaces().contains_key(ns)
    }

    /// Forgets the namespace `ns`: a store opened on it after starts empty.
    #[cfg(feature = "testing")]
    pub(crate) fn forget(ns: &str) {
        namespaces().remove(ns);
    }

    /// The keys under `prefix` that did not expire, in order, as a test sees what the store keeps.
    #[cfg(feature = "testing")]
    pub(crate) fn keys(&self, prefix: &str) -> Vec<String> {
        let now = (self.clock)();
        let objects = self.objects();
        let under = objects.range::<str, _>((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Unbounded,
        ));
        under
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter(|(_, e)| e.expires.is_none_or(|at| now < at))
            .map(|(k, _)| k.clone())
            .collect()
    }

    #[cfg(feature = "testing")]
    pub(crate) fn object(&self, key: &str) -> Option<Bytes> {
        let now = (self.clock)();
        let objects = self.objects();
        live(&objects, key, now).map(|e| e.bytes.clone())
    }

    fn objects(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Entry>> {
        self.objects
            .lock()
            .expect("a memory store never panics while locked")
    }

    fn entry(&self, bytes: Bytes, expiry: Option<u32>) -> Entry {
        // § r4rsq6spn.a@vpmv6
        let expires = expiry.map(|d| (self.clock)() + (u64::from(d) * DAY_MS) as i64);
        Entry { bytes, expires }
    }
}

/// The entry of `key` unless it expired by `now`: exact, as a store that expires keys itself.
fn live<'a>(objects: &'a BTreeMap<String, Entry>, key: &str, now: i64) -> Option<&'a Entry> {
    // § r4rsq6spn.b@266dj
    objects
        .get(key)
        .filter(|e| e.expires.is_none_or(|at| now < at))
}

impl Store for MemoryStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: false }
    }

    fn open(&self) -> futures::future::BoxFuture<'_, crate::error::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            check_days(expiry).map_err(StoreFailure::new)?;
            let entry = self.entry(bytes, expiry);
            let now = (self.clock)();
            // § dfd6vt90h@zhden
            let mut objects = self.objects();
            if live(&objects, key, now).is_some() {
                return Ok(Create::Exists);
            }
            let etag = digest_etag(&entry.bytes);
            objects.insert(key.to_owned(), entry);
            Ok(Create::Created { etag })
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
            let entry = self.entry(bytes, expiry);
            let now = (self.clock)();
            // § kkad6aqxp@v0je5
            let mut objects = self.objects();
            match live(&objects, key, now) {
                Some(current) if digest_etag(&current.bytes) == etag => {
                    let etag = digest_etag(&entry.bytes);
                    objects.insert(key.to_owned(), entry);
                    Ok(Cas::Written { etag })
                }
                // § one kkad6aqxp.a@3pb7j
                _ => Ok(Cas::Mismatch),
            }
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
        Box::pin(async move {
            let now = (self.clock)();
            let objects = self.objects();
            Ok(live(&objects, key, now).map(|e| Object {
                etag: digest_etag(&e.bytes),
                bytes: e.bytes.clone(),
            }))
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
            super::refuse_wait(&wait)?;
            let objects = self.objects();
            let lower = match start_after {
                Some(s) if s > prefix => s,
                _ => prefix,
            };
            let under = objects
                .range::<str, _>((std::ops::Bound::Included(lower), std::ops::Bound::Unbounded))
                .map(|(k, _)| k)
                .take_while(|k| k.starts_with(prefix));
            let keys = super::in_range(under, prefix, start_after, max_keys);
            Ok(Listing { keys, etag: None })
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.objects().remove(key);
            Ok::<_, StoreFailure>(())
        })
    }

    fn check_expiry<'a>(&'a self, days: &'a [u32]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { super::check_all_days(days) })
    }
}
