use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;

use super::{Capabilities, Create, ListWait, Listing, Store, StoreFailure, StoreFuture};
use crate::error::Result;

type Objects = Arc<Mutex<BTreeMap<String, Bytes>>>;

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
}

impl MemoryStore {
    pub fn open(ns: &str) -> Result<Self> {
        let objects = namespaces().entry(ns.to_owned()).or_default().clone();
        Ok(Self { objects })
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

    /// The keys under `prefix`, in order.
    #[cfg(feature = "testing")]
    pub(crate) fn keys(&self, prefix: &str) -> Vec<String> {
        let objects = self.objects();
        let under = objects.range::<str, _>((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Unbounded,
        ));
        under
            .map(|(k, _)| k)
            .take_while(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    #[cfg(feature = "testing")]
    pub(crate) fn object(&self, key: &str) -> Option<Bytes> {
        self.objects().get(key).cloned()
    }

    fn objects(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Bytes>> {
        self.objects
            .lock()
            .expect("a memory store never panics while locked")
    }
}

impl Store for MemoryStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: false }
    }

    fn open(&self) -> futures::future::BoxFuture<'_, crate::error::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            let mut objects = self.objects();
            if objects.contains_key(key) {
                return Ok(Create::Exists);
            }
            objects.insert(key.to_owned(), bytes);
            Ok(Create::Created)
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
        Box::pin(async move { Ok(self.objects().get(key).cloned()) })
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
}
