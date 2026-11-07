//! What the Redis adapter does beyond the shared contract; all but URL parsing run only when
//! `SUBSTANTIAL_REDIS_URL` names a standalone server (see `dev/redis.compose.yaml`), or, for the
//! last one, `SUBSTANTIAL_REDIS_SENTINEL_URL` a Sentinel.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{LONG, redis_url, retry, script};
use futures::StreamExt;
use redis::aio::MultiplexedConnection;
use substantial::__internal::{
    self as internal, ListWait, RedisLocation, RedisStore, Store, Topology,
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
async fn admin() -> Option<MultiplexedConnection> {
    let location = RedisLocation::parse(&redis_url("redis")?).unwrap();
    let (host, port) = &location.nodes[0];
    let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
    Some(client.get_multiplexed_async_connection().await.unwrap())
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
    let Some(mut admin) = admin().await else {
        return;
    };
    let _alone = SERVER.write().await;
    let url = redis_url("redis").unwrap();
    let m = with_config(&mut admin, "appendonly", "no", async || {
        config_error(Substantial::create(&url).partitions(1).await)
    })
    .await;
    assert!(m.contains("no AOF"), "{m}");
}

#[tokio::test]
async fn a_server_that_evicts_is_refused() {
    let Some(mut admin) = admin().await else {
        return;
    };
    let _alone = SERVER.write().await;
    let url = redis_url("redis").unwrap();
    let m = with_config(&mut admin, "maxmemory-policy", "allkeys-lru", async || {
        config_error(Substantial::create(&url).partitions(1).await)
    })
    .await;
    assert!(m.contains("allkeys-lru"), "{m}");
}

#[tokio::test]
async fn a_password_reaches_the_server() {
    let Some(mut admin) = admin().await else {
        return;
    };
    let _shared = SERVER.read().await;
    let user = "substantial-test-rust";
    let _: () = redis::cmd("ACL")
        .arg(&["SETUSER", user, "on", ">right", "~*", "&*", "+@all"])
        .query_async(&mut admin)
        .await
        .unwrap();
    let url = redis_url("redis").unwrap();
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
        .partitions(1)
        .credentials(provider.clone())
        .await;
    match refused {
        // The client names a refused password without the server's WRONGPASS.
        Err(Error::StoreError(m)) => assert!(m.contains("Password authentication failed"), "{m}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("created with a wrong password"),
    }
    let sub = Substantial::create(&url)
        .partitions(1)
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
    let Some(url) = redis_url("redis") else {
        return;
    };
    let _shared = SERVER.read().await;
    let listings = Listings::start(&url).await;
    let store = internal::open(&url, &Credentials::None, retry()).unwrap();
    store.create("q/a", Bytes::from_static(b"a")).await.unwrap();
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
        .create("other/x", Bytes::from_static(b"x"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !held.is_finished(),
        "a key under another prefix does not end the wait"
    );
    let started = Instant::now();
    store.create("q/b", Bytes::from_static(b"b")).await.unwrap();
    let woken = held.await.unwrap().unwrap();
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(woken.keys, ["q/a", "q/b"]);
    assert_ne!(woken.etag, first.etag);
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
    let Some(mut admin) = admin().await else {
        return;
    };
    let _shared = SERVER.read().await;
    let name = user(&mut admin, &["+@all", "-waitaof"]).await;
    let url = redis_url("redis").unwrap();
    let refused = Substantial::create(&url)
        .partitions(1)
        .credentials(right(&name))
        .await;
    drop_user(&mut admin, &name).await;
    match refused {
        Err(Error::StoreError(m)) => assert!(m.contains("no permissions"), "{m}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("created without WAITAOF"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_lost_again_resubscribes_at_once() {
    let Some(mut admin) = admin().await else {
        return;
    };
    let _shared = SERVER.read().await;
    let name = user(&mut admin, &["+@all"]).await;
    let url = redis_url("redis").unwrap();
    let channel = format!("{{{}}}:c", RedisLocation::parse(&url).unwrap().namespace);
    let store = internal::open(&url, &right(&name), retry()).unwrap();
    store.list("q/", None, 10, wait(1, &None)).await.unwrap();
    for _ in 0..6 {
        let killed: u64 = redis::cmd("CLIENT")
            .arg(&["KILL", "USER", &name])
            .query_async(&mut admin)
            .await
            .unwrap();
        assert!(killed >= 1);
        let started = Instant::now();
        loop {
            let (_, subscribers): (String, u64) = redis::cmd("PUBSUB")
                .arg(&["NUMSUB", &channel])
                .query_async(&mut admin)
                .await
                .unwrap();
            if subscribers > 0 {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "subscribed again within a second"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    drop(store);
    drop_user(&mut admin, &name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_from_another_client_comes_through_the_wait() {
    let Some(url) = redis_url("redis") else {
        return;
    };
    let _shared = SERVER.read().await;
    let answer = Workflow::without_input("answer", |_c| async { Ok(42) });
    let sub = Substantial::create(&url).partitions(1).await.unwrap();
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
    let Some(url) = redis_url("redis") else {
        return;
    };
    let _shared = SERVER.read().await;
    #[derive(serde::Deserialize)]
    struct N {
        n: i64,
    }
    let double = Workflow::new(
        "double",
        |_c: Context, input: N| async move { Ok(input.n * 2) },
    );
    let sub = Substantial::create(&url).partitions(2).await.unwrap();
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
    let Some(url) = redis_url("redis-sentinel") else {
        return;
    };
    let location = RedisLocation::parse(&url).unwrap();
    let store = RedisStore::open(location, &Credentials::None, retry()).unwrap();
    Store::open(&store).await.unwrap();
    assert!(store.primary_address().is_some());
    // As just promoted, its replicas still syncing.
    store.assume_no_replica_online();
    let refusals = [
        store.create("k", Bytes::from_static(b"v")).await.err(),
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
