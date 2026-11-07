//! Redis store URLs; the adapter needs the `redis` feature.

#[cfg(feature = "redis")]
mod connect;
#[cfg(feature = "redis")]
mod notify;
#[cfg(feature = "redis")]
mod store;

use std::collections::BTreeMap;

#[cfg(feature = "redis")]
pub use store::RedisStore;

use crate::error::{Error, Result};

/// The schemes of the Redis store, with the topology and whether TLS is on.
pub(crate) const SCHEMES: [(&str, Topology, bool); 6] = [
    ("redis", Topology::Standalone, false),
    ("rediss", Topology::Standalone, true),
    ("redis+sentinel", Topology::Sentinel, false),
    ("rediss+sentinel", Topology::Sentinel, true),
    ("redis+cluster", Topology::Cluster, false),
    ("rediss+cluster", Topology::Cluster, true),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topology {
    Standalone,
    Sentinel,
    Cluster,
}

impl Topology {
    fn parameters(self) -> &'static [&'static str] {
        match self {
            Topology::Standalone => &["db", "replicas"],
            Topology::Sentinel => &["db", "master", "replicas"],
            Topology::Cluster => &["replicas"],
        }
    }

    #[cfg_attr(not(feature = "redis"), allow(dead_code))]
    fn name(self) -> &'static str {
        match self {
            Topology::Standalone => "standalone",
            Topology::Sentinel => "sentinel",
            Topology::Cluster => "cluster",
        }
    }
}

/// The parts of a Redis URL, every one required (I-16): the server, the sentinels or the cluster
/// nodes to start from, the namespace, and `replicas`, the replicas that fsync a create before it
/// is acknowledged, `None` for every replica connected to the primary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisLocation {
    pub topology: Topology,
    pub tls: bool,
    pub nodes: Vec<(String, u16)>,
    pub namespace: String,
    pub db: u32,
    pub replicas: Option<u32>,
    pub master: Option<String>,
}

impl RedisLocation {
    pub fn parse(url: &str) -> Result<Self> {
        let invalid = |why: &str| Error::ConfigError(format!("{url:?}: {why}"));
        let known = url.split_once("://").and_then(|(scheme, rest)| {
            let found = SCHEMES.iter().find(|(s, ..)| *s == scheme)?;
            Some((found, rest))
        });
        let Some((&(_, topology, tls), rest)) = known else {
            let names: Vec<_> = SCHEMES.iter().map(|(s, ..)| *s).collect();
            return Err(invalid(&format!(
                "not a Redis URL; the schemes are {}",
                names.join(", ")
            )));
        };
        if rest.contains('#') {
            return Err(invalid("a store URL takes no fragment"));
        }
        let (rest, query_text) = rest.split_once('?').unwrap_or((rest, ""));
        let (netloc, namespace) = rest.split_once('/').unwrap_or((rest, ""));
        if netloc.contains('@') {
            return Err(invalid("credentials go in `credentials`, not in the URL"));
        }
        let nodes = netloc
            .split(',')
            .map(|part| node(part).ok_or_else(|| invalid(&format!("{part:?} is not host:port"))))
            .collect::<Result<Vec<_>>>()?;
        if topology == Topology::Standalone && nodes.len() != 1 {
            return Err(invalid("names one server, host:port"));
        }
        if !valid_namespace(namespace) {
            return Err(invalid(
                "names no namespace, or one outside [A-Za-z0-9._-] and /",
            ));
        }
        let allowed = topology.parameters();
        let mut query = BTreeMap::new();
        for (name, value) in url::form_urlencoded::parse(query_text.as_bytes()) {
            if query.contains_key(name.as_ref()) {
                return Err(invalid(&format!("{name} is given twice")));
            }
            if !allowed.contains(&name.as_ref()) {
                return Err(invalid(&format!("unknown query parameter {name}")));
            }
            query.insert(name.into_owned(), value.into_owned());
        }
        if let Some(missing) = allowed.iter().find(|name| !query.contains_key(**name)) {
            return Err(invalid(&format!(
                "the query parameter {missing} is required"
            )));
        }
        let replicas = match query["replicas"].as_str() {
            "all" => None,
            // Sentinel and Cluster promote a replica on failover: a create one of them misses is
            // lost.
            _ if topology != Topology::Standalone => {
                return Err(invalid(
                    "replicas is all on Sentinel and Cluster, so a failover keeps every create",
                ));
            }
            n => Some(
                decimal(n)
                    .ok_or_else(|| invalid(&format!("replicas is a number or all, not {n:?}")))?,
            ),
        };
        let db = match query.get("db") {
            None => 0,
            Some(db) => decimal(db)
                .ok_or_else(|| invalid(&format!("db is a database number, not {db:?}")))?,
        };
        let master = query.remove("master");
        if master.as_deref() == Some("") {
            return Err(invalid("master names the primary the sentinels monitor"));
        }
        Ok(Self {
            topology,
            tls,
            nodes,
            namespace: namespace.to_owned(),
            db,
            replicas,
            master,
        })
    }
}

fn decimal(text: &str) -> Option<u32> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn node(part: &str) -> Option<(String, u16)> {
    let (host, port) = part.rsplit_once(':')?;
    let port = decimal(port).and_then(|p| u16::try_from(p).ok())?;
    if host.is_empty() || port == 0 {
        return None;
    }
    let host = match host.strip_prefix('[') {
        Some(inner) => inner.strip_suffix(']')?,
        None => host,
    };
    Some((host.to_owned(), port))
}

/// The namespace becomes the hash tag `{<ns>}` of every key, so it holds no brace.
fn valid_namespace(namespace: &str) -> bool {
    let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
    namespace
        .split('/')
        .all(|part| !part.is_empty() && part.chars().all(allowed))
}

/// Opens the adapter of a Redis URL, or names the feature it needs.
pub(crate) fn open(
    url: &str,
    credentials: &super::Credentials,
    retry: crate::settings::StoreRetry,
) -> Result<std::sync::Arc<dyn super::Store>> {
    let location = RedisLocation::parse(url)?;
    #[cfg(feature = "redis")]
    {
        Ok(std::sync::Arc::new(RedisStore::open(
            location,
            credentials,
            retry,
        )?))
    }
    #[cfg(not(feature = "redis"))]
    {
        let _ = (location, credentials, retry);
        Err(Error::ConfigError(format!(
            "{url:?} needs the redis feature of substantial, which \
             `cargo add substantial --features redis` enables"
        )))
    }
}
