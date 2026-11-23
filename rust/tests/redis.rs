//! What the Redis adapter does beyond the shared contract; all but URL parsing on the standalone
//! server `SUBSTANTIAL_REDIS_URL` names, the last one on the Sentinel of
//! `SUBSTANTIAL_REDIS_SENTINEL_URL` (see `dev/compose.yaml`).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{LONG, failover_lock, redis_url, retry, script};
use futures::StreamExt;
use redis::aio::MultiplexedConnection;
use substantial::__internal::{
    self as internal, Cas, ListWait, RedisLocation, RedisStore, Store, Topology,
};
use substantial::{Context, Credentials, Error, Password, Preset, Substantial, Workflow};
use tokio::sync::RwLock;

/// Taken alone by the tests that reconfigure the server, shared by the others, which those would
/// disturb.
static SERVER: RwLock<()> = RwLock::const_new(());

#[test]
fn urls_need_every_part() {
    let ok = RedisLocation::parse("redis://h:6379/ns/sub?db=2&replicas=1").unwrap();
    assert_eq!(
        ok,
        RedisLocation {
            topology: Topology::Standalone,
            tls: false,
            nodes: vec![("h".into(), 6379)],
            namespace: "ns/sub".into(),
            db: 2,
            replicas: Some(1),
            master: None,
        }
    );
    let sentinel =
        RedisLocation::parse("rediss+sentinel://a:1,[::1]:2/ns?master=m&db=0&replicas=all")
            .unwrap();
    assert_eq!(
        sentinel,
        RedisLocation {
            topology: Topology::Sentinel,
            tls: true,
            nodes: vec![("a".into(), 1), ("::1".into(), 2)],
            namespace: "ns".into(),
            db: 0,
            replicas: None,
            master: Some("m".into()),
        }
    );
    let cluster = RedisLocation::parse("redis+cluster://a:1,b:2/ns?replicas=all").unwrap();
    assert_eq!(cluster.topology, Topology::Cluster);
    assert_eq!(cluster.nodes, [("a".into(), 1), ("b".into(), 2)]);
    assert_eq!(cluster.replicas, None);
    let all = RedisLocation::parse("redis://h:1/ns?db=0&replicas=all").unwrap();
    assert_eq!(all.replicas, None);
    for url in [
        "redis://h:1/ns?db=0",
        "redis://h:1/ns?replicas=0",
        "redis://h/ns?db=0&replicas=0",
        "redis://h:1,g:2/ns?db=0&replicas=0",
        "redis://h:1/?db=0&replicas=0",
        "redis://h:1/a{b}?db=0&replicas=0",
        "redis://h:1/a//b?db=0&replicas=0",
        "redis://h:1/ns?db=x&replicas=0",
        "redis://h:1/ns?db=\u{661}&replicas=0",
        "redis://h:1/ns?db=0&replicas=\u{661}",
        "redis://h:\u{661}/ns?db=0&replicas=0",
        "redis://h:+1/ns?db=+0&replicas=0",
        "redis://h:1/ns?db=0&replicas=-1",
        "redis://h:1/ns?db=0&db=1&replicas=0",
        "redis://h:1/ns?db=0&replicas=0&timeout=1",
        "redis://u:p@h:1/ns?db=0&replicas=0",
        "redis://h:1/ns?db=0&replicas=0#x",
        "redis+sentinel://a:1/ns?db=0&replicas=all",
        "redis+sentinel://a:1/ns?master=&db=0&replicas=all",
        "redis+sentinel://a:1/ns?master=m&db=0&replicas=1",
        "redis+cluster://a:1/ns?replicas=0",
        "redis+cluster://a:1/ns?db=0&replicas=all",
        "valkey://h:1/ns?db=0&replicas=0",
    ] {
        assert!(
            matches!(RedisLocation::parse(url), Err(Error::ConfigError(_))),
            "{url}"
        );
    }
}

fn config_error<T>(opened: substantial::Result<T>) -> String {
    match opened {
        Err(Error::ConfigError(m)) => m,
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("opened"),
    }
}

#[test]
fn credentials_are_a_password() {
    let url = "redis://127.0.0.1:1/ns?db=0&replicas=0";
    let password = |password: &str, username: Option<&str>| Credentials::Password {
        password: password.into(),
        username: username.map(Into::into),
    };
    assert!(!format!("{:?}", password("hunter2", Some("app"))).contains("hunter2"));
    let provided = Password {
        password: "hunter2".into(),
        username: None,
        expires_at: None,
    };
    assert!(!format!("{provided:?}").contains("hunter2"));
    for wrong in [password("", None), password("p", Some(""))] {
        config_error(internal::open(url, &wrong, retry()));
    }
    let keys = Credentials::S3 {
        access_key_id: "id".into(),
        secret_access_key: "secret".into(),
        session_token: None,
    };
    let refused = config_error(internal::open(url, &keys, retry()));
    assert!(refused.contains("a Redis store takes"), "{refused}");
    // No other test of this binary reads these variables.
    unsafe { std::env::remove_var("REDIS_PASSWORD") };
    let unset = config_error(internal::open(url, &Credentials::Env, retry()));
    assert!(unset.contains("REDIS_PASSWORD"), "{unset}");
    unsafe { std::env::set_var("REDIS_PASSWORD", "p") };
    internal::open(url, &Credentials::Env, retry()).unwrap();
    internal::open(url, &Credentials::None, retry()).unwrap();
    let s3 = "s3+http://127.0.0.1:1/b/ns?region=r";
    config_error(internal::open(s3, &password("p", None), retry()));
}

/// A connection to the standalone server, without the adapter.
async fn admin() -> MultiplexedConnection {
    let location = RedisLocation::parse(&redis_url("redis")).unwrap();
    let (host, port) = &location.nodes[0];
    let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Sets a server parameter for the length of `f`.
async fn with_config<T>(
    admin: &mut MultiplexedConnection,
    name: &str,
    value: &str,
    f: impl AsyncFnOnce() -> T,
) -> T {
    let before: (String, String) = redis::cmd("CONFIG")
        .arg("GET")
        .arg(name)
        .query_async(admin)
        .await
        .unwrap();
    let set = |value: &str| {
        redis::cmd("CONFIG")
            .arg("SET")
            .arg(name)
            .arg(value)
            .to_owned()
    };
    let _: () = set(value).query_async(admin).await.unwrap();
    let out = f().await;
    let _: () = set(&before.1).query_async(admin).await.unwrap();
    out
}

#[tokio::test]
async fn a_server_without_aof_is_refused() {
    let mut admin = admin().await;
    let _alone = (SERVER.write().await, failover_lock(true));
    let url = redis_url("redis");
    let m = with_config(&mut admin, "appendonly", "no", async || {
        config_error(Substantial::create(&url).await)
    })
    .await;
    assert!(m.contains("no AOF"), "{m}");
}

#[tokio::test]
async fn a_server_that_evicts_is_refused() {
    let mut admin = admin().await;
    let _alone = (SERVER.write().await, failover_lock(true));
    let url = redis_url("redis");
    let m = with_config(&mut admin, "maxmemory-policy", "allkeys-lru", async || {
        config_error(Substantial::create(&url).await)
    })
    .await;
    assert!(m.contains("allkeys-lru"), "{m}");
}

#[tokio::test]
async fn a_password_reaches_the_server() {
    let mut admin = admin().await;
    let _shared = (SERVER.read().await, failover_lock(false));
    let user = "substantial-test-rust";
    let _: () = redis::cmd("ACL")
        .arg(&["SETUSER", user, "on", ">right", "~*", "&*", "+@all"])
        .query_async(&mut admin)
        .await
        .unwrap();
    let url = redis_url("redis");
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let provider = Credentials::password_provider(move || {
        let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            Ok::<_, std::io::Error>(Password {
                password: if first { "wrong" } else { "right" }.into(),
                username: Some("substantial-test-rust".into()),
                expires_at: None,
            })
        }
    });
    let refused = Substantial::create(&url)
        .credentials(provider.clone())
        .await;
    match refused {
        // The client names a refused password without the server's WRONGPASS.
        Err(Error::StoreError(m)) => assert!(m.contains("Password authentication failed"), "{m}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("created with a wrong password"),
    }
    let sub = Substantial::create(&url)
        .credentials(provider)
        .await
        .unwrap();
    drop(sub);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "asked again after a failed connection, then once"
    );
    let right = Credentials::Password {
        password: "right".into(),
        username: Some(user.into()),
    };
    Substantial::open(&url).credentials(right).await.unwrap();
    let _: () = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(user)
        .query_async(&mut admin)
        .await
        .unwrap();
}

/// Counts the listings of the namespace of `url` the server runs from now on, until dropped:
/// MONITOR, not the server's command counts, as other clients list on it meanwhile.
struct Listings {
    count: Arc<AtomicU32>,
    task: tokio::task::JoinHandle<()>,
}

impl Listings {
    async fn start(url: &str) -> Self {
        let location = RedisLocation::parse(url).unwrap();
        let (host, port) = &location.nodes[0];
        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let mut lines = client
            .get_async_monitor()
            .await
            .unwrap()
            .into_on_message::<String>();
        let index = format!("\"{{{}}}:i\"", location.namespace);
        let count = Arc::new(AtomicU32::new(0));
        let counted = count.clone();
        let task = tokio::spawn(async move {
            while let Some(line) = lines.next().await {
                if line.contains("\"ZRANGE\"") && line.contains(&index) {
                    counted.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        Listings { count, task }
    }

    fn count(&self) -> u32 {
        self.count.load(Ordering::SeqCst)
    }
}

impl Drop for Listings {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn wait(seconds: u32, etag: &Option<String>) -> Option<ListWait> {
    Some(ListWait {
        etag: etag.clone(),
        seconds,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_listing_returns_as_a_key_lands_without_polling() {
    let url = redis_url("redis");
    let _shared = (SERVER.read().await, failover_lock(false));
    let listings = Listings::start(&url).await;
    let store = internal::open(&url, &Credentials::None, retry()).unwrap();
    store
        .create("q/a", Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    let first = store.list("q/", None, 10, wait(0, &None)).await.unwrap();
    assert_eq!(first.keys, ["q/a"]);
    assert!(first.etag.is_some());
    let before = listings.count();
    let started = Instant::now();
    let unchanged = store
        .list("q/", None, 10, wait(2, &first.etag))
        .await
        .unwrap();
    assert_eq!(unchanged, first);
    assert!(started.elapsed() >= Duration::from_secs(2));
    assert_eq!(listings.count() - before, 1, "listed once, then waited");
    let held = tokio::spawn({
        let (store, etag) = (store.clone(), first.etag.clone());
        async move { store.list("q/", None, 10, wait(20, &etag)).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    store
        .create("other/x", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !held.is_finished(),
        "a key under another prefix does not end the wait"
    );
    store
        .create("q/b", Bytes::from_static(b"b"), None)
        .await
        .unwrap();
    // At its deadline the wait answers the page it listed before `q/b` landed: `q/b` in the
    // answer is the key's notification waking it, however long the machine takes to get there.
    let woken = held.await.unwrap().unwrap();
    assert_eq!(woken.keys, ["q/a", "q/b"]);
    assert_ne!(woken.etag, first.etag);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_listing_wakes_on_a_cas_of_a_key_it_lists() {
    let _shared = (SERVER.read().await, failover_lock(false));
    for (topology, _) in common::REDIS_URLS {
        let store = internal::open(&redis_url(topology), &Credentials::None, retry()).unwrap();
        let created = store
            .create("runs/c", Bytes::from_static(b"head 1"), None)
            .await
            .unwrap();
        let substantial::__internal::Create::Created { etag } = created else {
            panic!("{topology}: {created:?}")
        };
        let first = store.list("runs/c", None, 1, wait(0, &None)).await.unwrap();
        let held = tokio::spawn({
            let (store, etag) = (store.clone(), first.etag.clone());
            async move { store.list("runs/c", None, 1, wait(20, &etag)).await }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!held.is_finished(), "{topology}: an unchanged page waits");
        let started = Instant::now();
        let written = store
            .cas("runs/c", Bytes::from_static(b"head 2"), &etag, None)
            .await
            .unwrap();
        assert!(matches!(written, Cas::Written { .. }), "{topology}");
        let woken = tokio::time::timeout(Duration::from_secs(10), held)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(woken.keys, ["runs/c"], "{topology}");
        assert_ne!(woken.etag, first.etag, "{topology}");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{topology}: woken by the cas, not by the end of its wait"
        );
        let again = store.list("runs/c", None, 1, wait(0, &None)).await.unwrap();
        assert_eq!(again.etag, woken.etag, "{topology}");
        store.delete("runs/c").await.unwrap();
        let gone = store.list("runs/c", None, 1, wait(0, &None)).await.unwrap();
        assert!(gone.keys.is_empty(), "{topology}");
        assert_ne!(gone.etag, woken.etag, "{topology}");
    }
}

/// The ids of the subscribed connections of `user`.
async fn subscribers(admin: &mut MultiplexedConnection, user: &str) -> Vec<u64> {
    let clients: String = redis::cmd("CLIENT")
        .arg(&["LIST", "TYPE", "pubsub"])
        .query_async(admin)
        .await
        .unwrap();
    clients
        .lines()
        .filter(|line| line.contains(&format!(" user={user} ")))
        .map(|line| {
            line["id=".len()..]
                .split(' ')
                .next()
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect()
}

/// An ACL user of password `right` allowed `commands`.
async fn user(admin: &mut MultiplexedConnection, commands: &[&str]) -> String {
    let name = format!("substantial-test-{:08x}", rand::random::<u32>());
    let _: () = redis::cmd("ACL")
        .arg(&["SETUSER", &name, "on", ">right", "~*", "&*"])
        .arg(commands)
        .query_async(admin)
        .await
        .unwrap();
    name
}

async fn drop_user(admin: &mut MultiplexedConnection, name: &str) {
    let _: () = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(name)
        .query_async(admin)
        .await
        .unwrap();
}

fn right(name: &str) -> Credentials {
    Credentials::Password {
        password: "right".into(),
        username: Some(name.into()),
    }
}

#[tokio::test]
async fn a_user_denied_waitaof_is_refused_as_such() {
    let mut admin = admin().await;
    let _shared = (SERVER.read().await, failover_lock(false));
    let name = user(&mut admin, &["+@all", "-waitaof"]).await;
    let url = redis_url("redis");
    let refused = Substantial::create(&url).credentials(right(&name)).await;
    drop_user(&mut admin, &name).await;
    match refused {
        Err(Error::StoreError(m)) => assert!(m.contains("no permissions"), "{m}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("created without WAITAOF"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_lost_again_resubscribes_at_once() {
    let mut admin = admin().await;
    let _shared = (SERVER.read().await, failover_lock(false));
    let name = user(&mut admin, &["+@all"]).await;
    let url = redis_url("redis");
    // Not reset by a subscription, a backoff doubling from 100 ms would pause 51 s before the
    // tenth: far beyond a reconnection, seconds at most on a loaded machine.
    let retry = internal::StoreRetry {
        attempts: 5,
        initial: Duration::from_millis(100),
        max: Duration::from_secs(3600),
    };
    let store = internal::open(&url, &right(&name), retry).unwrap();
    store.list("q/", None, 10, wait(1, &None)).await.unwrap();
    let mut subscribed = subscribers(&mut admin, &name).await[0];
    for loss in 1..=10 {
        let killed: u64 = redis::cmd("CLIENT")
            .arg(&["KILL", "USER", &name])
            .query_async(&mut admin)
            .await
            .unwrap();
        assert!(killed >= 1);
        let started = Instant::now();
        loop {
            let ids = subscribers(&mut admin, &name).await;
            if let Some(&again) = ids.iter().find(|&&id| id > subscribed) {
                subscribed = again;
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "subscribed again after loss {loss}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    drop(store);
    drop_user(&mut admin, &name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_from_another_client_comes_through_the_wait() {
    let url = redis_url("redis");
    let _shared = (SERVER.read().await, failover_lock(false));
    let answer = Workflow::without_input("answer", |_c| async { Ok(42) });
    let sub = Substantial::create(&url).await.unwrap();
    let worker = sub
        .worker(answer.clone())
        .preset(Preset::Script)
        .set("max_idle", "5min")
        .start()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let other = Substantial::open(&url).await.unwrap();
    let started = Instant::now();
    let run = other.start(&answer, ()).await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 42);
    assert!(started.elapsed() < Duration::from_secs(2));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_ends_on_redis_through_a_script_worker() {
    let url = redis_url("redis");
    let _shared = (SERVER.read().await, failover_lock(false));
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let double = Workflow::new(
        "double",
        |_c: Context, input: N| async move { Ok(input.n * 2) },
    );
    let sub = Substantial::create(&url).await.unwrap();
    let worker = script(&sub, double.clone()).start().unwrap();
    let run = sub
        .start(&double, serde_json::json!({ "n": 21 }))
        .await
        .unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 42);
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_primary_without_a_replica_online_answers_nothing() {
    let _shared = (SERVER.read().await, failover_lock(false));
    let url = redis_url("redis-sentinel");
    let location = RedisLocation::parse(&url).unwrap();
    let store = RedisStore::open(location, &Credentials::None, retry()).unwrap();
    Store::open(&store).await.unwrap();
    assert!(store.primary_address().is_some());
    // As just promoted, its replicas still syncing.
    store.assume_no_replica_online();
    let refusals = [
        store
            .create("k", Bytes::from_static(b"v"), None)
            .await
            .err(),
        store.get("k").await.err(),
        store.delete("k").await.err(),
    ];
    for refused in refusals {
        let message = refused.expect("refused").message;
        assert!(message.contains("no replica online"), "{message}");
    }
    let counted = internal::open(&url, &Credentials::None, retry()).unwrap();
    assert_eq!(counted.get("k").await, Ok(None), "nothing written");
}

#[tokio::test]
async fn an_expiry_is_the_servers_own() {
    let url = redis_url("redis");
    let _shared = (SERVER.read().await, failover_lock(false));
    let ns = RedisLocation::parse(&url).unwrap().namespace;
    let store = internal::open(&url, &Credentials::None, retry()).unwrap();
    let mut admin = admin().await;
    let (object, expiries) = (format!("{{{ns}}}:o:runs/e"), format!("{{{ns}}}:e"));
    let ttl = async |admin: &mut MultiplexedConnection| -> i64 {
        redis::cmd("PTTL")
            .arg(&object)
            .query_async(admin)
            .await
            .unwrap()
    };
    let indexed = async |admin: &mut MultiplexedConnection| -> Option<f64> {
        let mut at = redis::cmd("ZSCORE");
        at.arg(&expiries).arg("runs/e");
        at.query_async(admin).await.unwrap()
    };
    // § hxvcx7f2a@facgw
    store
        .create("runs/e", Bytes::from_static(b"one"), Some(1))
        .await
        .unwrap();
    let day = 86_400_000;
    let left = ttl(&mut admin).await;
    assert!(left > day - 60_000 && left <= day, "{left}");
    assert!(indexed(&mut admin).await.is_some());
    let read = store.get("runs/e").await.unwrap().unwrap();
    // § one rz3se15bd@jy70n
    let digest = {
        use sha1::Digest;
        hex::encode(sha1::Sha1::digest(b"one"))
    };
    assert_eq!(read.etag, digest);
    let written = store.cas("runs/e", Bytes::from_static(b"two"), &read.etag, None);
    assert!(matches!(written.await.unwrap(), Cas::Written { .. }));
    assert_eq!(
        ttl(&mut admin).await,
        -1,
        "a write without an expiry clears it"
    );
    assert_eq!(indexed(&mut admin).await, None);
    // A key the server expired leaves the index at the next write of an expiry.
    let _: () = redis::cmd("PEXPIRE")
        .arg(&object)
        .arg(1)
        .query_async(&mut admin)
        .await
        .unwrap();
    let _: () = redis::cmd("ZADD")
        .arg(&expiries)
        .arg(0)
        .arg("runs/e")
        .query_async(&mut admin)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        store.list("runs/", None, 10, None).await.unwrap().keys,
        ["runs/e"]
    );
    store
        .create("runs/f", Bytes::from_static(b"f"), Some(1))
        .await
        .unwrap();
    assert_eq!(
        store.list("runs/", None, 10, None).await.unwrap().keys,
        ["runs/f"]
    );
    assert_eq!(store.get("runs/e").await.unwrap(), None);
}
