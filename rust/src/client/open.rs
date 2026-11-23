//! The builders of `create` and `open` (I-16).

use std::future::IntoFuture;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use super::{Inner, Substantial};
use crate::core::Core;
use crate::cost::Prices;
use crate::error::{Error, Result};
use crate::observe::{Metering, Observed};
use crate::plan;
use crate::proto::boundary::v1::{self as b, opened, plan_input::Plan, plan_step::Step};
use crate::settings::STORE_RETRY;
use crate::store::{self, Credentials, Store};

/// What a namespace is opened on: a URL, or a store object for the tests.
pub(crate) enum Source {
    Url(String),
    Store(Arc<dyn Store>),
}

/// How a URL is opened: credentials, none unless given; the store's prices and an observer,
/// none unless given.
struct Access {
    source: Source,
    credentials: Option<Credentials>,
    metering: Metering,
}

impl Access {
    fn new(source: Source) -> Self {
        Self {
            source,
            credentials: None,
            metering: Metering::default(),
        }
    }

    /// A store object comes configured: credentials given with it are refused.
    fn store(self) -> Result<(Arc<dyn Store>, Metering)> {
        if let Some(prices) = &self.metering.prices {
            prices.validate()?;
        }
        let store = match self.source {
            Source::Url(url) => {
                let credentials = self.credentials.unwrap_or(Credentials::None);
                store::open(&url, &credentials, STORE_RETRY)?
            }
            Source::Store(store) => {
                if self.credentials.is_some() {
                    return Err(Error::ConfigError(
                        "a store object takes no credentials".into(),
                    ));
                }
                store
            }
        };
        Ok((store, self.metering))
    }
}

/// A creation of a namespace, awaited to create or open it.
#[must_use = "a creation does nothing until awaited"]
pub struct Create {
    access: Access,
}

impl Create {
    pub(crate) fn new(source: Source) -> Self {
        Self {
            access: Access::new(source),
        }
    }

    /// How the store authenticates: `Credentials::Env`, explicit keys, or a provider; none unless
    /// given.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.access.credentials = Some(credentials);
        self
    }

    /// The store's prices, which price the store requests of runs and the observer's store
    /// operations; without them, requests are counted and no money is.
    pub fn prices(mut self, prices: Prices) -> Self {
        self.access.metering.prices = Some(prices);
        self
    }

    /// Told every store operation, step attempt, replay and scan of this `Substantial` and its
    /// workers; a panic of `f` kills a worker, or panics the client call.
    pub fn observe(mut self, f: impl Fn(&Observed) + Send + Sync + 'static) -> Self {
        self.access.metering.observe = Some(Arc::new(f));
        self
    }
}

impl IntoFuture for Create {
    type Output = Result<Substantial>;
    type IntoFuture = BoxFuture<'static, Result<Substantial>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let plan = Plan::Create(b::CreatePlan {
                prefix: String::new(),
                probe: crate::plan::probe(),
            });
            opened(self.access, plan).await
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
            access: Access::new(source),
        }
    }

    /// How the store authenticates: `Credentials::Env`, explicit keys, or a provider; none unless
    /// given.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.access.credentials = Some(credentials);
        self
    }

    /// As `Create::prices`.
    pub fn prices(mut self, prices: Prices) -> Self {
        self.access.metering.prices = Some(prices);
        self
    }

    /// As `Create::observe`.
    pub fn observe(mut self, f: impl Fn(&Observed) + Send + Sync + 'static) -> Self {
        self.access.metering.observe = Some(Arc::new(f));
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
                probe: crate::plan::probe(),
            });
            opened(self.access, plan).await
        })
    }
}

async fn opened(access: Access, plan: Plan) -> Result<Substantial> {
    let (store, metering) = access.store()?;
    attached(
        store,
        plan,
        metering,
        #[cfg(feature = "testing")]
        None,
    )
    .await
}

/// Opens or creates a namespace, for a test environment when `env` is given.
pub(crate) async fn attached(
    store: Arc<dyn Store>,
    plan: Plan,
    metering: Metering,
    #[cfg(feature = "testing")] env: Option<crate::testing::Attached>,
) -> Result<Substantial> {
    let store = metering.wrap(store);
    store.open().await?;
    let core = Core::new()?;
    match plan::run(&core, &*store, plan).await? {
        Step::Opened(b::Opened {
            opened: Some(outcome),
        }) => match outcome {
            opened::Opened::Open(_) => {}
            opened::Opened::LayoutError(e) => return Err(Error::LayoutError(e)),
            opened::Opened::ConfigError(e) => return Err(Error::ConfigError(e)),
            // § one zemxe46k3.b@53t47
            opened::Opened::Failed(f) => return Err(Error::StoreError(f.message)),
        },
        other => return Err(plan::unexpected("open", other)),
    }
    let inner = Inner {
        core,
        store,
        workers: Mutex::default(),
        died: Arc::new(tokio::sync::watch::channel(None).0),
        metering,
        #[cfg(feature = "testing")]
        env,
    };
    Ok(Substantial(Arc::new(inner)))
}
