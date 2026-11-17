//! Connections of the Redis adapter: its scripts, its password, where the primary of a namespace
//! is, and the checks at open.

use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use ::redis::aio::MultiplexedConnection;
use ::redis::{
    AsyncConnectionConfig, Client, ConnectionAddr, ErrorKind, InfoDict, IntoConnectionInfo,
    ProtocolVersion, PushInfo, RedisConnectionInfo, RedisError, ServerErrorKind, Value,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;

use super::{RedisLocation, Topology};
use crate::store::credentials::RedisAuth;
use crate::store::retry::Fault;

pub(super) const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a create waits for its AOF fsync on the primary and its replicas.
pub(super) const WAITAOF_MS: u64 = 5000;
/// How often `replicas=all` counts the replicas online again, so that one joining is waited for.
pub(super) const RECOUNT_EVERY: Duration = Duration::from_secs(10);

pub(super) struct Script {
    pub(super) text: &'static str,
    pub(super) sha: String,
}

impl Script {
    fn shipped(text: &'static str) -> Self {
        let sha = ::redis::Script::new(text).get_hash().to_owned();
        Script { text, sha }
    }
}

// `redis` is a symlink to the repository's single copy: cargo package follows it, so the
// published crate carries the scripts themselves.
pub(super) static CREATE: LazyLock<Script> = LazyLock::new(|| {
    Script::shipped(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis/create.lua"
    )))
});
pub(super) static DELETE: LazyLock<Script> = LazyLock::new(|| {
    Script::shipped(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis/delete.lua"
    )))
});

/// How an operation of the adapter failed, before it is classified as a fault of its attempt.
#[derive(Debug)]
pub(super) enum Failure {
    Fault(Fault),
    /// A server the adapter cannot use: `ConfigError` at open.
    Config(String),
    /// The credentials provider failed.
    Provider(String),
    Redis(RedisError),
}

impl From<RedisError> for Failure {
    fn from(e: RedisError) -> Self {
        Failure::Redis(e)
    }
}

impl From<::redis::ParsingError> for Failure {
    fn from(e: ::redis::ParsingError) -> Self {
        Failure::Redis(e.into())
    }
}

impl From<Fault> for Failure {
    fn from(f: Fault) -> Self {
        Failure::Fault(f)
    }
}

/// A refused password or permission: trying again cannot help.
pub(super) fn refused(e: &RedisError) -> bool {
    e.kind() == ErrorKind::AuthenticationFailed
        || e.kind() == ErrorKind::Server(ServerErrorKind::NoPerm)
        || matches!(e.code(), Some("WRONGPASS" | "NOAUTH"))
}

/// An error the server answered, as opposed to a connection lost or timed out.
pub(super) fn answered(e: &RedisError) -> bool {
    matches!(e.kind(), ErrorKind::Server(_) | ErrorKind::Extension)
}

/// A primary of the namespace, its idle connections and the replicas a create waits for there.
pub(super) struct Primary {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) at: String,
    idle: Mutex<Vec<MultiplexedConnection>>,
    pub(super) count: Mutex<Count>,
}

#[derive(Clone, Copy)]
pub(super) struct Count {
    pub(super) replicas: u32,
    pub(super) at: Instant,
}

/// A connection taken from a primary's idle ones for one operation, given back by `keep` once
/// it succeeded: a connection that failed or timed out is dropped.
pub(super) struct Lease<'a> {
    pub(super) conn: MultiplexedConnection,
    primary: &'a Primary,
}

impl Lease<'_> {
    pub(super) fn keep(self) {
        self.primary
            .idle
            .lock()
            .expect("never poisoned")
            .push(self.conn);
    }
}

impl Primary {
    /// An idle connection, or a new one: each operation has its own, since `WAITAOF` blocks the
    /// connection it is sent on and covers the writes sent on it before.
    pub(super) async fn lease(&self, connector: &Connector) -> Result<Lease<'_>, Failure> {
        let idle = self.idle.lock().expect("never poisoned").pop();
        let conn = match idle {
            Some(conn) => conn,
            None => {
                connector
                    .connection(&self.host, self.port, connector.location.db, true, None)
                    .await?
            }
        };
        Ok(Lease {
            conn,
            primary: self,
        })
    }
}

/// Finds the primary of a namespace and opens checked connections to it.
pub(super) struct Connector {
    pub(super) location: RedisLocation,
    auth: RedisAuth,
    slot: u16,
}

fn display(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

impl Connector {
    pub(super) fn new(location: RedisLocation, auth: RedisAuth) -> Self {
        // The namespace holds no brace, so it is the hash tag of every key of the namespace.
        let slot = crc16::State::<crc16::XMODEM>::calculate(location.namespace.as_bytes()) % 16384;
        Connector {
            location,
            auth,
            slot,
        }
    }

    /// A connection with the client's own retries off; RESP3 when `push` takes the pushes of a
    /// subscription.
    pub(super) async fn connection(
        &self,
        host: &str,
        port: u16,
        db: u32,
        auth: bool,
        push: Option<UnboundedSender<PushInfo>>,
    ) -> Result<MultiplexedConnection, Failure> {
        let addr = if self.location.tls {
            ConnectionAddr::TcpTls {
                host: host.to_owned(),
                port,
                insecure: false,
                tls_params: None,
            }
        } else {
            ConnectionAddr::Tcp(host.to_owned(), port)
        };
        let protocol = match push {
            Some(_) => ProtocolVersion::RESP3,
            None => ProtocolVersion::RESP2,
        };
        let mut settings = RedisConnectionInfo::default()
            .set_db(i64::from(db))
            .set_protocol(protocol);
        let credentials = match &self.auth {
            _ if !auth => None,
            RedisAuth::None => None,
            RedisAuth::Static { password, username } => Some((password.clone(), username.clone())),
            RedisAuth::Provided(refresh) => Some(refresh.fresh().await.map_err(Failure::Provider)?),
        };
        if let Some((password, username)) = credentials {
            settings = settings.set_password(password);
            if let Some(username) = username {
                settings = settings.set_username(username);
            }
        }
        let info = addr.into_connection_info()?.set_redis_settings(settings);
        let mut config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(CONNECT_TIMEOUT))
            .set_response_timeout(Some(ATTEMPT_TIMEOUT));
        if let Some(push) = push {
            config = config.set_push_sender(push);
        }
        let client = Client::open(info)?;
        Ok(client
            .get_multiplexed_async_connection_with_config(&config)
            .await?)
    }

    /// Where the primary is: the server, or what the sentinels or the cluster nodes say.
    async fn address(&self) -> Result<(String, u16), Failure> {
        let loc = &self.location;
        if loc.topology == Topology::Standalone {
            return Ok(loc.nodes[0].clone());
        }
        let mut failures = Vec::new();
        for (host, port) in &loc.nodes {
            let node = display(host, *port);
            let asked = tokio::time::timeout(CONNECT_TIMEOUT, self.ask(host, *port)).await;
            match asked {
                Ok(Ok(Ok(primary))) => return Ok(primary),
                Ok(Ok(Err(why))) => failures.push(format!("{node}: {why}")),
                Ok(Err(Failure::Redis(e))) if refused(&e) => {
                    return Err(Fault::Permanent(format!("{node}: {e}")).into());
                }
                Ok(Err(Failure::Redis(e))) => failures.push(format!("{node}: {e}")),
                Ok(Err(other)) => return Err(other),
                Err(_) => failures.push(format!(
                    "{node}: no answer within {} s",
                    CONNECT_TIMEOUT.as_secs()
                )),
            }
        }
        Err(Fault::Transient(format!(
            "no {} node named the primary: {}",
            loc.topology.name(),
            failures.join("; ")
        ))
        .into())
    }

    /// The primary a sentinel or a cluster node names, or why it names none.
    async fn ask(&self, host: &str, port: u16) -> Result<Result<(String, u16), String>, Failure> {
        let loc = &self.location;
        // Sentinels take no password here: only the servers holding data do.
        let cluster = loc.topology == Topology::Cluster;
        let mut seed = self.connection(host, port, 0, cluster, None).await?;
        if !cluster {
            let master = loc
                .master
                .as_deref()
                .expect("a sentinel URL names its master");
            let answer: Option<(String, u16)> = ::redis::cmd("SENTINEL")
                .arg("GET-MASTER-ADDR-BY-NAME")
                .arg(master)
                .query_async(&mut seed)
                .await?;
            return Ok(answer.ok_or_else(|| format!("monitors no {master}")));
        }
        let slots: Vec<Value> = ::redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query_async(&mut seed)
            .await?;
        for range in slots {
            let Value::Array(parts) = range else { continue };
            let (start, end): (u16, u16) = match parts.as_slice() {
                [start, end, ..] => (
                    ::redis::from_redis_value_ref(start)?,
                    ::redis::from_redis_value_ref(end)?,
                ),
                _ => continue,
            };
            if (start..=end).contains(&self.slot) {
                let Some(Value::Array(primary)) = parts.get(2) else {
                    break;
                };
                if let [host, port, ..] = primary.as_slice() {
                    let host: String = ::redis::from_redis_value_ref(host)?;
                    let port: u16 = ::redis::from_redis_value_ref(port)?;
                    return Ok(Ok((host, port)));
                }
            }
        }
        Ok(Err(format!("no node serves slot {}", self.slot)))
    }

    /// Finds the primary and checks it, loading the scripts there.
    pub(super) async fn connect(&self) -> Result<Primary, Failure> {
        let (host, port) = self.address().await?;
        let at = display(&host, port);
        let mut conn = self
            .connection(&host, port, self.location.db, true, None)
            .await?;
        let info: String = ::redis::cmd("INFO")
            .arg(&["persistence", "memory", "replication", "cluster"])
            .query_async(&mut conn)
            .await?;
        let info = InfoDict::new(&info);
        self.check(&at, &info)?;
        let probe: Result<Value, RedisError> = ::redis::cmd("WAITAOF")
            .arg(1)
            .arg(0)
            .arg(WAITAOF_MS)
            .query_async(&mut conn)
            .await;
        match probe {
            Err(e) if answered(&e) && !refused(&e) => {
                return Err(Failure::Config(format!(
                    "{at} has no WAITAOF, which needs Redis 7.2: {e}"
                )));
            }
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        for script in [&*CREATE, &*DELETE] {
            let loaded: String = ::redis::cmd("SCRIPT")
                .arg("LOAD")
                .arg(script.text)
                .query_async(&mut conn)
                .await?;
            if loaded != script.sha {
                return Err(Fault::Permanent(format!(
                    "{at} loaded a script as {loaded}, not {}",
                    script.sha
                ))
                .into());
            }
        }
        let count = Count {
            replicas: self.replicas(&info),
            at: Instant::now(),
        };
        Ok(Primary {
            host,
            port,
            at,
            idle: Mutex::new(vec![conn]),
            count: Mutex::new(count),
        })
    }

    /// The checks at open (I-26): a server that loses or evicts keys is not a store.
    fn check(&self, at: &str, info: &InfoDict) -> Result<(), Failure> {
        let topology = self.location.topology;
        if info.get::<i64>("aof_enabled") != Some(1) {
            return Err(Failure::Config(format!(
                "{at} has no AOF: set appendonly yes and appendfsync always"
            )));
        }
        let policy: Option<String> = info.get("maxmemory_policy");
        if policy.as_deref() != Some("noeviction") {
            return Err(Failure::Config(format!(
                "{at} may evict keys, its maxmemory-policy is {}",
                policy.as_deref().unwrap_or("unknown")
            )));
        }
        let cluster = info.get::<i64>("cluster_enabled") == Some(1);
        if cluster != (topology == Topology::Cluster) {
            let not = if cluster { "" } else { "not " };
            return Err(Failure::Config(format!("{at} is {not}a cluster node")));
        }
        if info.get::<String>("role").as_deref() != Some("master") {
            if topology == Topology::Standalone {
                return Err(Failure::Config(format!(
                    "{at} is a replica: name its primary"
                )));
            }
            return Err(Fault::Transient(format!("{at} is no longer the primary")).into());
        }
        Ok(())
    }

    /// The replicas a create waits for: those of the URL, or every one online.
    pub(super) fn replicas(&self, info: &InfoDict) -> u32 {
        if let Some(n) = self.location.replicas {
            return n;
        }
        // The server numbers its replicas from slave0, one line each.
        let mut online = 0;
        for i in 0.. {
            let Some(line) = info.get::<String>(&format!("slave{i}")) else {
                break;
            };
            if line.split(',').any(|field| field == "state=online") {
                online += 1;
            }
        }
        online
    }
}
