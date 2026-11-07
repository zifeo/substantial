use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use aws_credential_types::provider::{ProvideCredentials, error::CredentialsError, future};
use chrono::{DateTime, TimeDelta, Utc};
use futures::future::BoxFuture;

use crate::error::{Error, Result};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type ProviderFn = dyn Fn() -> BoxFuture<'static, Result<Keys, BoxError>> + Send + Sync;

/// How an adapter authenticates (I-16). Stores that take no credentials refuse any but `None`,
/// and stores that need them refuse `None`.
#[derive(Clone)]
pub enum Credentials {
    None,
    /// The store's standard variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for S3, and
    /// `AWS_SESSION_TOKEN` when it is set.
    Env,
    S3 {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    /// Keys from a function, made with [`Credentials::provider`].
    Provider(Provider),
}

/// The keys a credentials provider returns; without `expires_at`, they are never refreshed.
#[derive(Clone)]
pub struct Keys {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

/// A function returning [`Keys`], called at open and again before any request made less than
/// 5 minutes before the last keys' `expires_at`.
#[derive(Clone)]
pub struct Provider(Arc<ProviderFn>);

impl Credentials {
    /// Keys from `f`, an async function: called at open, and again before any request made less
    /// than 5 minutes before the `expires_at` of the keys it last returned. An error fails the
    /// request as the store's failure, and the next request calls `f` again.
    pub fn provider<F, Fut, E>(f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Keys, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        let f = move || -> BoxFuture<'static, Result<Keys, BoxError>> {
            let keys = f();
            Box::pin(async move { keys.await.map_err(Into::into) })
        };
        Credentials::Provider(Provider(Arc::new(f)))
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credentials::None => f.write_str("None"),
            Credentials::Env => f.write_str("Env"),
            Credentials::S3 { access_key_id, .. } => f
                .debug_struct("S3")
                .field("access_key_id", access_key_id)
                .finish_non_exhaustive(),
            Credentials::Provider(_) => f.write_str("Provider"),
        }
    }
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys")
            .field("access_key_id", &self.access_key_id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Provider")
    }
}

/// How an S3 store signs: keys given once, or keys a provider refreshes.
pub(crate) enum S3Auth {
    Static(aws_sdk_s3::config::Credentials),
    Refreshed(Arc<Refresh>),
}

fn sdk_keys(
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
) -> aws_sdk_s3::config::Credentials {
    aws_sdk_s3::config::Credentials::new(
        access_key_id,
        secret_access_key,
        session_token,
        None,
        "substantial",
    )
}

impl Credentials {
    pub(crate) fn refuse(&self, url: &str) -> Result<()> {
        match self {
            Credentials::None => Ok(()),
            _ => Err(Error::ConfigError(format!("{url:?} takes no credentials"))),
        }
    }

    pub(crate) fn s3(&self) -> Result<S3Auth> {
        match self {
            Credentials::None => Err(Error::ConfigError(
                "an S3 store needs credentials, Env, explicit or a provider".into(),
            )),
            Credentials::Env => {
                let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
                let required = |name: &str| {
                    var(name).ok_or_else(|| Error::ConfigError(format!("{name} is not set")))
                };
                Ok(S3Auth::Static(sdk_keys(
                    required("AWS_ACCESS_KEY_ID")?,
                    required("AWS_SECRET_ACCESS_KEY")?,
                    var("AWS_SESSION_TOKEN"),
                )))
            }
            Credentials::S3 {
                access_key_id,
                secret_access_key,
                session_token,
            } => Ok(S3Auth::Static(sdk_keys(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token.clone(),
            ))),
            Credentials::Provider(provider) => Ok(S3Auth::Refreshed(Arc::new(Refresh {
                provider: provider.clone(),
                refreshing: tokio::sync::Mutex::new(()),
                held: Mutex::new(None),
            }))),
        }
    }
}

/// Keys this close to their expiry are refreshed before a request.
const REFRESH_BEFORE: TimeDelta = TimeDelta::minutes(5);

/// The keys of a provider, refreshed before a request when they near their expiry.
pub(crate) struct Refresh {
    provider: Provider,
    /// Held while the provider is called, so that concurrent requests call it once.
    refreshing: tokio::sync::Mutex<()>,
    held: Mutex<Option<(aws_sdk_s3::config::Credentials, Option<DateTime<Utc>>)>>,
}

impl std::fmt::Debug for Refresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Refresh")
    }
}

impl Refresh {
    fn usable(&self) -> bool {
        match &*self.held.lock().expect("never poisoned") {
            None => false,
            Some((_, None)) => true,
            Some((_, Some(expires_at))) => {
                *expires_at - DateTime::<Utc>::from(SystemTime::now()) > REFRESH_BEFORE
            }
        }
    }

    /// Calls the provider unless the keys held are usable; its error is the request's.
    pub(crate) async fn fresh(&self) -> std::result::Result<(), String> {
        if self.usable() {
            return Ok(());
        }
        let _refreshing = self.refreshing.lock().await;
        if self.usable() {
            return Ok(());
        }
        let keys = (self.provider.0)()
            .await
            .map_err(|e| format!("credentials provider: {e}"))?;
        let sdk = sdk_keys(
            keys.access_key_id,
            keys.secret_access_key,
            keys.session_token,
        );
        *self.held.lock().expect("never poisoned") = Some((sdk, keys.expires_at));
        Ok(())
    }
}

/// What the SDK signs with: the keys `fresh` left, read at each request.
#[derive(Debug)]
pub(crate) struct Signing(pub(crate) Arc<Refresh>);

impl ProvideCredentials for Signing {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        let held = self.0.held.lock().expect("never poisoned");
        future::ProvideCredentials::ready(
            held.as_ref()
                .map(|(keys, _)| keys.clone())
                .ok_or_else(|| CredentialsError::not_loaded("no keys from the provider yet")),
        )
    }
}
