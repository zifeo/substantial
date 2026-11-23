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
use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, Tag};
use bytes::Bytes;
use futures::future::BoxFuture;

use super::credentials::{Credentials, Refresh, S3Auth, Signing};
use super::h3;
use super::retry::{self, Fault};
use super::{
    Capabilities, Cas, Create, ListWait, Listing, Object, Store, StoreFailure, StoreFuture,
    check_days,
};
use crate::error::{Error, Result};
use crate::settings::StoreRetry;

/// ListObjectsV2 returns at most this many keys a page.
pub(super) const PAGE: u32 = 1000;
/// A request that hangs must become an ambiguous outcome, not a stuck worker. An attempt fits a
/// waiting listing (20 s at most) with 40 s to spare.
// § one gvqb04830.b@1twx3
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

/// The suffix of an S3 directory bucket's name.
const DIRECTORY_BUCKET: &str = "--x-s3";
const DIRECTORY_REFUSED: &str = "names an S3 directory bucket, whose listings are not in byte \
     order of the key as the store contract requires: use a general purpose bucket";

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
        // The raw authority: `Url` drops an empty userinfo, as in `s3://@host`.
        let authority = url.split_once("://").map_or("", |(_, rest)| {
            rest.split(['/', '?', '#']).next().unwrap_or_default()
        });
        if authority.contains('@') {
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
        // § h1b3afst6@q2tq4
        if bucket.ends_with(DIRECTORY_BUCKET) {
            return Err(invalid(DIRECTORY_REFUSED));
        }
        // § end h1b3afst6
        if namespace.split('/').any(str::is_empty) {
            return Err(invalid(
                "names no namespace, or one with an empty component",
            ));
        }
        // § e9ern38bz.b@9abwz
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
        if !valid_region(&region) {
            return Err(invalid(&format!(
                "the region {region:?} is not 1 to 63 lowercase letters, digits and hyphens, \
                 neither starting nor ending with a hyphen"
            )));
        }
        if let Some(unknown) = query.keys().next() {
            return Err(invalid(&format!("unknown query parameter {unknown}")));
        }
        // § end e9ern38bz.b
        Ok(Self {
            endpoint,
            bucket: bucket.to_owned(),
            namespace: namespace.to_owned(),
            region,
            wait,
        })
    }
}

/// A region as AWS names them, a DNS label of lowercase letters, digits and hyphens.
fn valid_region(region: &str) -> bool {
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
    (1..=63).contains(&region.len())
        && region.chars().all(allowed)
        && !region.starts_with('-')
        && !region.ends_with('-')
}

impl S3Store {
    pub fn open(
        location: S3Location,
        credentials: &Credentials,
        retry: StoreRetry,
    ) -> Result<Self> {
        retry.validate()?;
        let auth = credentials.s3()?;
        // Retries are `store_retry`'s alone.
        // § gvqb04830.b@q07wb
        let timeouts = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .operation_attempt_timeout(ATTEMPT_TIMEOUT)
            .build();
        // § end gvqb04830.b
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
        // No cap, as in every host: the SDK's default client, hyper's pool, opens a
        // connection per request in flight, so a waiting listing queues nothing behind it.
        // § x2qrbh9jk.a@gqj0w
        // Pinned, not the newest: a newer one needs a newer SDK than the floor this crate declares.
        #[allow(deprecated)]
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
        // § end x2qrbh9jk.a
    }

    /// Refreshes the provider's keys if they near their expiry; a provider's error fails the
    /// request without retries, and a provider that does not answer within the attempt timeout
    /// fails the attempt as transient.
    async fn fresh(&self) -> Result<(), Fault> {
        let Some(refresh) = &self.refresh else {
            return Ok(());
        };
        // Dropping a refresh that hangs releases its lock, so the next attempt calls the provider
        // again.
        match tokio::time::timeout(ATTEMPT_TIMEOUT, refresh.fresh()).await {
            Ok(fresh) => fresh.map_err(Fault::Permanent),
            Err(_) => Err(Fault::Transient(format!(
                "credentials provider: no keys within {} ms",
                ATTEMPT_TIMEOUT.as_millis()
            ))),
        }
    }

    fn full(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    async fn put(&self, key: &str, bytes: &Bytes, expiry: Option<u32>) -> Result<Create, Fault> {
        // § gvqb04830@a2x2r
        self.fresh().await?;
        let sent = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full(key))
            .if_none_match("*")
            .set_tagging(tagging(expiry))
            .body(ByteStream::from(bytes.clone()))
            .send()
            .await;
        let e = match sent {
            // An answer without its ETag is read back, as one lost.
            Ok(out) => {
                return match out.e_tag() {
                    Some(etag) => Ok(Create::Created {
                        etag: etag.to_owned(),
                    }),
                    None => Err(Fault::Ambiguous(format!(
                        "create {key}: created without an ETag in the answer"
                    ))),
                };
            }
            Err(e) => e,
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

    async fn put_if_match(
        &self,
        key: &str,
        bytes: &Bytes,
        etag: &str,
        expiry: Option<u32>,
    ) -> Result<Cas, Fault> {
        // § c4fyc3wyk@qsv0p
        self.fresh().await?;
        let sent = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full(key))
            .if_match(etag)
            .set_tagging(tagging(expiry))
            .body(ByteStream::from(bytes.clone()))
            .send()
            .await;
        let e = match sent {
            // § one c4fyc3wyk.b@wk49z
            Ok(out) => {
                return match out.e_tag() {
                    Some(etag) => Ok(Cas::Written {
                        etag: etag.to_owned(),
                    }),
                    None => Err(Fault::Ambiguous(format!(
                        "cas {key}: written without an ETag in the answer"
                    ))),
                };
            }
            Err(e) => e,
        };
        let message = describe("cas", key, &e);
        // § c4fyc3wyk.a@2qetg
        match status(&e) {
            Some(412) => Ok(Cas::Mismatch),
            Some(404) if e.code() == Some("NoSuchKey") => Ok(Cas::Mismatch),
            Some(409) | Some(429) => Err(Fault::Transient(message)),
            Some(s) if s >= 500 || s == 408 => Err(Fault::Ambiguous(message)),
            Some(_) => Err(Fault::Permanent(message)),
            None if matches!(e, SdkError::ConstructionFailure(_)) => Err(Fault::Permanent(message)),
            None => Err(Fault::Ambiguous(message)),
        }
        // § end c4fyc3wyk.a
    }

    /// The bucket's lifecycle rules, none when it has no configuration.
    async fn lifecycle(&self) -> Result<Vec<LifecycleRule>, Fault> {
        self.fresh().await?;
        let sent = self
            .client
            .get_bucket_lifecycle_configuration()
            .bucket(&self.bucket)
            .send()
            .await;
        match sent {
            Ok(out) => Ok(out.rules().to_vec()),
            Err(e) if e.code() == Some("NoSuchLifecycleConfiguration") => Ok(Vec::new()),
            // § one cfdckf61n.a@bzsay
            Err(e) if status(&e) == Some(403) => Err(Fault::Permanent(format!(
                "reading the lifecycle rules of the bucket {:?} needs the permission \
                 s3:GetLifecycleConfiguration: {}",
                self.bucket,
                describe("lifecycle of", &self.bucket, &e)
            ))),
            Err(e) => Err(fault("lifecycle of", &self.bucket, &e)),
        }
    }

    async fn read(&self, key: &str) -> Result<Option<Object>, Fault> {
        // § gvqb04830.a@jny3c
        self.fresh().await?;
        let sent = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full(key))
            .send()
            .await;
        match sent {
            Ok(out) => {
                let etag = out.e_tag().map(str::to_owned);
                let bytes = match out.body.collect().await {
                    Ok(data) => data.into_bytes(),
                    Err(e) => return Err(Fault::Transient(format!("get {key}: {e}"))),
                };
                match etag {
                    Some(etag) => Ok(Some(Object { bytes, etag })),
                    None => Err(Fault::Permanent(format!(
                        "get {key}: answered without an ETag"
                    ))),
                }
            }
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
        // § dfd6vt90h.c@zqqpa
        while (keys.len() as u32) < max_keys {
            let page = (max_keys - keys.len() as u32).min(PAGE);
            let out = self
                .page(prefix, after.take(), page)
                .send()
                .await
                .map_err(|e| fault("list", prefix, &e))?;
            self.keep(&out, &mut keys)?;
            match keys.last() {
                Some(last)
                    if out.is_truncated() == Some(true) && (keys.len() as u32) < max_keys =>
                {
                    // The retry layer counted the first page's request.
                    super::attempts::sent();
                    after = Some(self.full(last));
                }
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
        // § gvqb04830.d@ny3tr
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
            // § one dfd6vt90h.d@w2vp7
            Err(e) if e.code() == Some("NoSuchKey") => Ok(()),
            Err(e) => Err(fault("delete", key, &e)),
        }
    }
}

/// The tag of a write's expiry, which a lifecycle rule of the bucket expires.
fn tagging(expiry: Option<u32>) -> Option<String> {
    // § one c4fyc3wyk@vr1yy
    expiry.map(|d| format!("{RETENTION_TAG}={d}d"))
}

const RETENTION_TAG: &str = "substantial-retention";

/// Whether `rule` expires, after `days`, the keys under `prefix` tagged with that expiry.
fn expires(rule: &LifecycleRule, prefix: &str, days: u32) -> bool {
    // § cfdckf61n@3d4af
    let value = format!("{days}d");
    let tagged =
        |tags: &[Tag]| matches!(tags, [tag] if tag.key() == RETENTION_TAG && tag.value() == value);
    let filtered = match rule.filter() {
        Some(filter) => match (filter.and(), filter.tag()) {
            (Some(and), None) => {
                and.object_size_greater_than().is_none()
                    && and.object_size_less_than().is_none()
                    && prefix.starts_with(and.prefix().unwrap_or_default())
                    && tagged(and.tags())
            }
            (None, Some(tag)) => tagged(std::slice::from_ref(tag)),
            _ => false,
        },
        None => false,
    };
    filtered
        && rule.status() == &ExpirationStatus::Enabled
        && rule.expiration().and_then(|e| e.days()) == Some(days as i32)
}

/// Why a store would never expire the keys of `missing` expiries, with the rules to add as JSON.
fn missing_rules(bucket: &str, prefix: &str, missing: &[u32]) -> String {
    let rules: Vec<serde_json::Value> = missing
        .iter()
        .map(|d| {
            let tag = serde_json::json!({"Key": RETENTION_TAG, "Value": format!("{d}d")});
            serde_json::json!({
                "ID": format!("substantial-expire-{d}d"),
                "Status": "Enabled",
                "Filter": {"And": {"Prefix": prefix, "Tags": [tag]}},
                "Expiration": {"Days": d},
            })
        })
        .collect();
    let tags: Vec<String> = missing.iter().map(|d| format!("{d}d")).collect();
    format!(
        "the bucket {bucket:?} has no lifecycle rule expiring the keys under {prefix:?} tagged \
         {RETENTION_TAG}={} after as many days, so they would never expire: add these rules to \
         its lifecycle configuration, which needs the permission s3:PutLifecycleConfiguration: {}",
        tags.join(", "),
        serde_json::json!({ "Rules": rules })
    )
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

    fn open(&self) -> BoxFuture<'_, Result<()>> {
        // § x79k6r9e5@pnpr1
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
            let attempt = || self.put(key, &bytes, expiry);
            let read = || self.read(key);
            retry::create(self.retry, &bytes, attempt, read, tokio::time::sleep).await
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
            let attempt = || self.put_if_match(key, &bytes, etag, expiry);
            let read = || self.read(key);
            retry::cas(self.retry, &bytes, etag, attempt, read, tokio::time::sleep).await
        })
    }

    fn check_expiry<'a>(&'a self, days: &'a [u32]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            super::check_all_days(days)?;
            let attempt = || self.lifecycle();
            let rules = retry::with_retry(self.retry, attempt, tokio::time::sleep)
                .await
                .map_err(|e| Error::ConfigError(e.message))?;
            let mut missing: Vec<u32> = days
                .iter()
                .copied()
                .filter(|d| !rules.iter().any(|r| expires(r, &self.prefix, *d)))
                .collect();
            missing.sort_unstable();
            missing.dedup();
            if missing.is_empty() {
                return Ok(());
            }
            let why = missing_rules(&self.bucket, &self.prefix, &missing);
            Err(Error::ConfigError(why))
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
        Box::pin(retry::with_retry(
            self.retry,
            move || self.read(key),
            tokio::time::sleep,
        ))
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
                    retry::with_retry(self.retry, attempt, tokio::time::sleep).await
                }
                _ => {
                    super::refuse_wait(&wait)?;
                    let attempt = || self.list_once(prefix, start_after, max_keys);
                    retry::with_retry(self.retry, attempt, tokio::time::sleep).await
                }
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(retry::with_retry(
            self.retry,
            move || self.remove(key),
            tokio::time::sleep,
        ))
    }
}
