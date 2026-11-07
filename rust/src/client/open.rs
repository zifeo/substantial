//! The builders of `create` and `open` (I-16).

use std::future::IntoFuture;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use super::{Inner, Substantial};
use crate::core::Core;
use crate::error::{Error, Result};
use crate::plan;
use crate::proto::boundary::v1::{self as b, opened, plan_input::Plan, plan_step::Step};
use crate::settings::STORE_RETRY;
use crate::store::{self, Credentials, Store};

/// What a namespace is opened on: a URL, or a store object for the tests.
pub(crate) enum Source {
    Url(String),
    Store(Arc<dyn Store>),
}

/// How a URL is opened: credentials, none unless given.
struct Access {
    source: Source,
    credentials: Option<Credentials>,
}

impl Access {
    /// A store object comes configured: credentials given with it are refused.
    fn store(self) -> Result<Arc<dyn Store>> {
        match self.source {
            Source::Url(url) => {
                let credentials = self.credentials.unwrap_or(Credentials::None);
                store::open(&url, &credentials, STORE_RETRY)
            }
            Source::Store(store) => {
                if self.credentials.is_some() {
                    return Err(Error::ConfigError(
                        "a store object takes no credentials".into(),
                    ));
                }
                Ok(store)
            }
        }
    }
}

/// A creation of a namespace, awaited to create or open it; `partitions` is required.
#[must_use = "a creation does nothing until awaited"]
pub struct Create {
    access: Access,
    partitions: Option<u32>,
}

impl Create {
    pub(crate) fn new(source: Source) -> Self {
        Self {
            access: Access {
                source,
                credentials: None,
            },
            partitions: None,
        }
    }

    pub fn partitions(mut self, partitions: u32) -> Self {
        self.partitions = Some(partitions);
        self
    }

    /// How the store authenticates: `Credentials::Env`, explicit keys, or a provider; none unless
    /// given.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.access.credentials = Some(credentials);
        self
    }
}

impl IntoFuture for Create {
    type Output = Result<Substantial>;
    type IntoFuture = BoxFuture<'static, Result<Substantial>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let partitions = match self.partitions {
                None => {
                    return Err(Error::ConfigError(
                        "create needs partitions, a positive whole number".into(),
                    ));
                }
                Some(0) => {
                    return Err(Error::ConfigError(
                        "partitions must be a positive whole number, not 0".into(),
                    ));
                }
                Some(p) => p,
            };
            let plan = Plan::Create(b::CreatePlan {
                prefix: String::new(),
                partitions,
            });
            opened(self.access.store()?, plan).await
        })
    }
}

/// An opening of a namespace that exists, awaited to open it.
#[must_use = "an opening does nothing until awaited"]
pub struct Open {
    access: Access,
}

impl Open {
    pub(crate) fn new(source: Source) -> Self {
        Self {
            access: Access {
                source,
                credentials: None,
            },
        }
    }

    /// How the store authenticates: `Credentials::Env`, explicit keys, or a provider; none unless
    /// given.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.access.credentials = Some(credentials);
        self
    }
}

impl IntoFuture for Open {
    type Output = Result<Substantial>;
    type IntoFuture = BoxFuture<'static, Result<Substantial>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let plan = Plan::Open(b::OpenPlan {
                prefix: String::new(),
            });
            opened(self.access.store()?, plan).await
        })
    }
}

async fn opened(store: Arc<dyn Store>, plan: Plan) -> Result<Substantial> {
    attached(
        store,
        plan,
        #[cfg(feature = "testing")]
        None,
    )
    .await
}

/// Opens or creates a namespace, for a test environment when `env` is given.
pub(crate) async fn attached(
    store: Arc<dyn Store>,
    plan: Plan,
    #[cfg(feature = "testing")] env: Option<crate::testing::Attached>,
) -> Result<Substantial> {
    store.open().await?;
    let core = Core::new()?;
    let partitions = match plan::run(&core, &*store, plan).await? {
        Step::Opened(b::Opened {
            opened: Some(outcome),
        }) => match outcome {
            opened::Opened::Partitions(p) => p,
            opened::Opened::LayoutError(e) => return Err(Error::LayoutError(e)),
            opened::Opened::Failed(f) => return Err(Error::StoreError(f.message)),
        },
        other => return Err(plan::unexpected("open", other)),
    };
    let inner = Inner {
        core,
        store,
        partitions,
        workers: Mutex::default(),
        died: Arc::new(tokio::sync::watch::channel(None).0),
        #[cfg(feature = "testing")]
        env,
    };
    Ok(Substantial(Arc::new(inner)))
}
