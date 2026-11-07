use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, IdentityCache, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    SharedCredentialsProvider,
};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

use super::credentials::{Credentials, Refresh, S3Auth, Signing};
use super::h3;
use super::retry::{self, Fault};
use super::{Capabilities, Create, ListWait, Listing, Store, StoreFuture};
use crate::error::{Error, Result};
use crate::settings::StoreRetry;

/// ListObjectsV2 returns at most this many keys a page.
pub(super) const PAGE: u32 = 1000;

/// `s3://<endpoint>/<bucket>/<ns>?region=<r>`: one object per key under the prefix `<ns>/`,
/// created with `If-None-Match: *`. `h3://` is the same adapter, its listings declared able to
/// wait; `s3+http://` and `h3+http://` reach the endpoint over plain HTTP.
#[derive(Clone, Debug)]
pub struct S3Store {
    client: Client,
    bucket: String,
    prefix: String,
    wait: bool,
    retry: StoreRetry,
    refresh: Option<Arc<Refresh>>,
}

/// The parts of an `s3://` or `h3://` URL, every one required (I-16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Location {
    /// `https://<host>[:<port>]`, or `http://` for the `+http` schemes.
    pub endpoint: String,
    pub bucket: String,
    pub namespace: String,
    pub region: String,
    pub wait: bool,
}

impl S3Location {
    pub fn parse(url: &str) -> Result<Self> {
        let invalid = |why: &str| Error::ConfigError(format!("{url:?}: {why}"));
        let parsed = url::Url::parse(url).map_err(|e| invalid(&e.to_string()))?;
        let (wait, protocol) = match parsed.scheme() {
            "s3" => (false, "https"),
            "s3+http" => (false, "http"),
            "h3" => (true, "https"),
            "h3+http" => (true, "http"),
            _ => return Err(invalid("not an s3:// or h3:// URL")),
        };
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(invalid("credentials go in `credentials`, not in the URL"));
        }
        if parsed.fragment().is_some() {
            return Err(invalid("a store URL takes no fragment"));
        }
        let host = match parsed.host_str() {
            Some(host) if !host.is_empty() => host,
            _ => return Err(invalid("names no endpoint")),
        };
        let endpoint = match parsed.port() {
            Some(port) => format!("{protocol}://{host}:{port}"),
            None => format!("{protocol}://{host}"),
        };
        let path = parsed.path().strip_prefix('/').unwrap_or_default();
        let (bucket, namespace) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() {
            return Err(invalid("names no bucket"));
        }
        if namespace.split('/').any(str::is_empty) {
            return Err(invalid(
                "names no namespace, or one with an empty component",
            ));
        }
        let mut query = BTreeMap::new();
        for (name, value) in parsed.query_pairs() {
            if query.insert(name.to_string(), value.to_string()).is_some() {
                return Err(invalid(&format!("{name} is given twice")));
            }
        }
        let region = query
            .remove("region")
            .filter(|r| !r.is_empty())
            .ok_or_else(|| invalid("the query parameter region is required"))?;
        if let Some(unknown) = query.keys().next() {
            return Err(invalid(&format!("unknown query parameter {unknown}")));
        }
        Ok(Self {
            endpoint,
            bucket: bucket.to_owned(),
            namespace: namespace.to_owned(),
            region,
            wait,
        })
    }
}

impl S3Store {
    pub fn open(
        location: S3Location,
        credentials: &Credentials,
        retry: StoreRetry,
    ) -> Result<Self> {
        retry.validate()?;
        let auth = credentials.s3()?;
        // A request that hangs must become an ambiguous outcome, not a stuck worker; retries are
        // `store_retry`'s alone. An attempt fits a waiting listing (20 s at most) with 40 s to spare.
        let timeouts = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .operation_attempt_timeout(Duration::from_secs(60))
            .build();
        let mut config = aws_sdk_s3::Config::builder();
        let refresh = match auth {
            S3Auth::Static(keys) => {
                config.set_credentials_provider(Some(SharedCredentialsProvider::new(keys)));
                None
            }
            S3Auth::Refreshed(refresh) => {
                let signing = SharedCredentialsProvider::new(Signing(refresh.clone()));
                config.set_credentials_provider(Some(signing));
                // The SDK's cache would keep keys past their refresh.
                config.set_identity_cache(IdentityCache::no_cache());
                Some(refresh)
            }
        };
        let config = config
            .behavior_version(BehaviorVersion::v2026_01_12())
            .endpoint_url(&location.endpoint)
            .region(Region::new(location.region))
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .timeout_config(timeouts)
            // S3-compatible stores that predate flexible checksums refuse their headers.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();
        Ok(Self {
            client: Client::from_conf(config),
            bucket: location.bucket,
            prefix: format!("{}/", location.namespace),
            wait: location.wait,
            retry,
            refresh,
        })
    }

    /// Refreshes the provider's keys if they near their expiry; a provider's error fails the
    /// request without retries.
    async fn fresh(&self) -> Result<(), Fault> {
        match &self.refresh {
            Some(refresh) => refresh.fresh().await.map_err(Fault::Permanent),
            None => Ok(()),
        }
    }

    fn full(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    async fn put(&self, key: &str, bytes: &Bytes) -> Result<Create, Fault> {
        self.fresh().await?;
        let sent = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full(key))
            .if_none_match("*")
            .body(ByteStream::from(bytes.clone()))
            .send()
            .await;
        let Err(e) = sent else {
            return Ok(Create::Created);
        };
        let message = describe("create", key, &e);
        match status(&e) {
            Some(412) => Ok(Create::Exists),
            // AWS: a concurrent conditional write of the key was in flight, this one did not happen.
            Some(409) | Some(429) => Err(Fault::Transient(message)),
            Some(s) if s >= 500 || s == 408 => Err(Fault::Ambiguous(message)),
            Some(_) => Err(Fault::Permanent(message)),
            None if matches!(e, SdkError::ConstructionFailure(_)) => Err(Fault::Permanent(message)),
            None => Err(Fault::Ambiguous(message)),
        }
    }

    async fn read(&self, key: &str) -> Result<Option<Bytes>, Fault> {
        self.fresh().await?;
        let sent = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full(key))
            .send()
            .await;
        match sent {
            Ok(out) => match out.body.collect().await {
                Ok(data) => Ok(Some(data.into_bytes())),
                Err(e) => Err(Fault::Transient(format!("get {key}: {e}"))),
            },
            Err(e) if e.code() == Some("NoSuchKey") => Ok(None),
            Err(e) => Err(fault("get", key, &e)),
        }
    }

    async fn list_once(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> Result<Listing, Fault> {
        self.fresh().await?;
        let mut keys = Vec::new();
        let mut after = start_after.map(|s| self.full(s));
        while (keys.len() as u32) < max_keys {
            let page = (max_keys - keys.len() as u32).min(PAGE);
            let out = self
                .page(prefix, after.take(), page)
                .send()
                .await
                .map_err(|e| fault("list", prefix, &e))?;
            self.keep(&out, &mut keys)?;
            match keys.last() {
                Some(last) if out.is_truncated() == Some(true) => after = Some(self.full(last)),
                _ => break,
            }
        }
        keys.truncate(max_keys as usize);
        Ok(Listing { keys, etag: None })
    }

    async fn list_waiting(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
        wait: &ListWait,
    ) -> Result<Listing, Fault> {
        self.fresh().await?;
        let page = self.page(prefix, start_after.map(|s| self.full(s)), max_keys);
        let (list, answer) = h3::wait(page.customize(), max_keys, wait)?;
        let out = list.send().await.map_err(|e| fault("list", prefix, &e))?;
        let mut keys = Vec::new();
        self.keep(&out, &mut keys)?;
        Ok(Listing {
            keys,
            etag: Some(answer.etag()?),
        })
    }

    fn page(
        &self,
        prefix: &str,
        after: Option<String>,
        max_keys: u32,
    ) -> ListObjectsV2FluentBuilder {
        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(self.full(prefix))
            .set_start_after(after)
            .max_keys(max_keys as i32)
    }

    fn keep(&self, out: &ListObjectsV2Output, keys: &mut Vec<String>) -> Result<(), Fault> {
        for object in out.contents() {
            let full = object.key().unwrap_or_default();
            let key = full.strip_prefix(&self.prefix).ok_or_else(|| {
                Fault::Permanent(format!("listed {full:?} outside the namespace"))
            })?;
            keys.push(key.to_owned());
        }
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<(), Fault> {
        self.fresh().await?;
        let sent = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.full(key))
            .send()
            .await;
        match sent {
            Ok(_) => Ok(()),
            Err(e) if e.code() == Some("NoSuchKey") => Ok(()),
            Err(e) => Err(fault("delete", key, &e)),
        }
    }
}

fn status<E>(e: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>) -> Option<u16> {
    e.raw_response().map(|r| r.status().as_u16())
}

/// An S3 error answer as its status, `Code` and `Message`; any other failure with its causes.
fn describe<E: std::error::Error + ProvideErrorMetadata + 'static>(
    what: &str,
    key: &str,
    e: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
) -> String {
    match (status(e), e.code(), e.message()) {
        (Some(s), Some(code), Some(message)) => format!("{what} {key}: HTTP {s} {code}: {message}"),
        (Some(s), Some(code), None) => format!("{what} {key}: HTTP {s} {code}"),
        _ => format!("{what} {key}: {}", DisplayErrorContext(e)),
    }
}

/// Classifies the failure of an operation other than a create, which none makes ambiguous.
fn fault<E: std::error::Error + ProvideErrorMetadata + 'static>(
    what: &str,
    key: &str,
    e: &SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
) -> Fault {
    let message = describe(what, key, e);
    match (status(e), e) {
        (None, SdkError::ConstructionFailure(_)) => Fault::Permanent(message),
        (None, _) => Fault::Transient(message),
        (Some(s), _) if s >= 500 || s == 408 || s == 409 || s == 429 => Fault::Transient(message),
        (Some(_), _) => Fault::Permanent(message),
    }
}

impl Store for S3Store {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: self.wait }
    }

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            let attempt = || self.put(key, &bytes);
            retry::create(self.retry, &bytes, attempt, || self.read(key)).await
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
        Box::pin(retry::with_retry(self.retry, move || self.read(key)))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        Box::pin(async move {
            match &wait {
                Some(wait) if self.wait => {
                    let attempt = || self.list_waiting(prefix, start_after, max_keys, wait);
                    retry::with_retry(self.retry, attempt).await
                }
                _ => {
                    super::refuse_wait(&wait)?;
                    let attempt = || self.list_once(prefix, start_after, max_keys);
                    retry::with_retry(self.retry, attempt).await
                }
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(retry::with_retry(self.retry, move || self.remove(key)))
    }
}
