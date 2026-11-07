//! Store adapters and the store contract (I-26).

mod credentials;
mod fs;
pub(crate) mod h3;
mod memory;
mod names;
mod redis;
pub(crate) mod retry;
mod s3;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;

pub use credentials::{Credentials, Keys, Password};
pub use fs::FsStore;
pub use memory::MemoryStore;
#[cfg(feature = "redis")]
pub use redis::RedisStore;
pub use redis::{RedisLocation, Topology};
pub use s3::{S3Location, S3Store};

use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, op_result, store_op};
use crate::settings::StoreRetry;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capabilities {
    pub wait: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Create {
    Created,
    Exists,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Listing {
    pub keys: Vec<String>,
    pub etag: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListWait {
    pub etag: Option<String>,
    pub seconds: u32,
}

/// The store failed after the adapter's own retries.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("store failure: {message}")]
pub struct StoreFailure {
    pub message: String,
}

impl StoreFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreFailure>> + Send + 'a>>;

/// The four operations of the store contract, the only ones Substantial performs on a store.
///
/// The futures are boxed so that a store can be chosen at run time (`Arc<dyn Store>`), which
/// `async fn` in a trait does not allow.
pub trait Store: Send + Sync {
    fn capabilities(&self) -> Capabilities;
    /// Connects, failing with `ConfigError` on a store the adapter cannot use; the operations
    /// connect too when it was not called.
    fn open(&self) -> BoxFuture<'_, Result<()>>;
    /// Atomic and exclusive: of concurrent creates of a key, exactly one gets `Created`.
    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create>;
    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>>;
    /// Keys under `prefix` and after `start_after`, in byte order of the full key, at most `max_keys`.
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing>;
    /// Removes a key; deleting a missing key succeeds.
    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()>;
}

/// Opens the adapter a URL names (I-16); the scheme alone picks it, nothing is detected.
pub fn open(url: &str, credentials: &Credentials, retry: StoreRetry) -> Result<Arc<dyn Store>> {
    retry.validate()?;
    if let Some(ns) = url.strip_prefix("memory://") {
        refuse_query(url, ns)?;
        credentials.refuse(url)?;
        return Ok(Arc::new(MemoryStore::open(ns)?));
    }
    if let Some(rest) = url.strip_prefix("file:") {
        let path = rest.strip_prefix("//").unwrap_or(rest);
        refuse_query(url, path)?;
        if path.starts_with('~') {
            return Err(Error::ConfigError(format!(
                "{url:?}: give an absolute or relative path, ~ is not expanded"
            )));
        }
        credentials.refuse(url)?;
        return Ok(Arc::new(FsStore::open(path, retry)?));
    }
    let schemes = ["s3://", "s3+http://", "h3://", "h3+http://"];
    if schemes.iter().any(|scheme| url.starts_with(scheme)) {
        let location = S3Location::parse(url)?;
        return Ok(Arc::new(S3Store::open(location, credentials, retry)?));
    }
    if redis::SCHEMES
        .iter()
        .any(|(scheme, ..)| url.starts_with(&format!("{scheme}://")))
    {
        return redis::open(url, credentials, retry);
    }
    let planned = ["sqlite:", "postgres://", "mysql://"];
    if let Some(scheme) = planned.iter().find(|scheme| url.starts_with(**scheme)) {
        return Err(Error::ConfigError(format!(
            "{scheme} stores are not available in this host yet; {SUPPORTED}"
        )));
    }
    Err(Error::ConfigError(format!(
        "unknown scheme in {url:?}; {SUPPORTED}"
    )))
}

const SUPPORTED: &str = "the supported schemes are memory://, file:, s3://, s3+http://, h3://, \
     h3+http://, redis://, rediss://, redis+sentinel://, rediss+sentinel://, redis+cluster:// and \
     rediss+cluster://";

fn refuse_query(url: &str, rest: &str) -> Result<()> {
    if rest.contains('?') {
        return Err(Error::ConfigError(format!(
            "{url:?} takes no query parameters"
        )));
    }
    if rest.is_empty() {
        return Err(Error::ConfigError(format!("{url:?} names no namespace")));
    }
    Ok(())
}

/// Performs one `StoreOp` of the core and answers with its `OpResult`.
pub(crate) async fn perform(store: &dyn Store, op: b::StoreOp) -> op_result::Result {
    let failed =
        |e: StoreFailure| op_result::Result::Failed(b::StoreFailure { message: e.message });
    let empty = b::Empty {};
    match op.op {
        Some(store_op::Op::Create(c)) => match store.create(&c.key, c.bytes.into()).await {
            Ok(Create::Created) => op_result::Result::Created(empty),
            Ok(Create::Exists) => op_result::Result::Exists(empty),
            Err(e) => failed(e),
        },
        Some(store_op::Op::Get(key)) => match store.get(&key).await {
            Ok(Some(bytes)) => op_result::Result::Bytes(bytes.to_vec()),
            Ok(None) => op_result::Result::Missing(empty),
            Err(e) => failed(e),
        },
        Some(store_op::Op::List(l)) => {
            let wait = l.wait.map(|w| ListWait {
                etag: w.etag,
                seconds: w.seconds,
            });
            match store
                .list(&l.prefix, l.start_after.as_deref(), l.max_keys, wait)
                .await
            {
                Ok(listing) => op_result::Result::Listing(op_result::Listing {
                    keys: listing.keys,
                    etag: listing.etag,
                }),
                Err(e) => failed(e),
            }
        }
        Some(store_op::Op::Delete(key)) => match store.delete(&key).await {
            Ok(()) => op_result::Result::Deleted(empty),
            Err(e) => failed(e),
        },
        None => failed(StoreFailure::new("empty store op from the core")),
    }
}

/// Keeps the keys of a sorted run that lie under `prefix` and after `start_after`, up to `max_keys`.
pub(crate) fn in_range<'a>(
    keys: impl Iterator<Item = &'a String>,
    prefix: &str,
    start_after: Option<&str>,
    max_keys: u32,
) -> Vec<String> {
    keys.filter(|k| k.starts_with(prefix) && start_after.is_none_or(|s| k.as_str() > s))
        .take(max_keys as usize)
        .cloned()
        .collect()
}

pub(crate) fn refuse_wait(wait: &Option<ListWait>) -> Result<(), StoreFailure> {
    match wait {
        Some(_) => Err(StoreFailure::new("this store cannot wait on a listing")),
        None => Ok(()),
    }
}
