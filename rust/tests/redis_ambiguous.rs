//! Ambiguous creates on Redis (I-26), injected by a proxy that drops a connection; only when
//! `SUBSTANTIAL_REDIS_URL` names a standalone server (see `dev/redis.compose.yaml`).

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::{redis_url, retry};
use redis::aio::MultiplexedConnection;
use substantial::__internal::{self as internal, Create, RedisLocation, RedisStore, Store};
use substantial::Credentials;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A TCP proxy to the server that drops the connection of the next request holding `marker`:
/// after the server got it, so it runs and its answer is lost, or before.
struct Dropping {
    target: String,
    marker: Mutex<Option<(Vec<u8>, bool)>>,
    dropped: AtomicU32,
}

impl Dropping {
    fn drop_next(&self, marker: &[u8], forward: bool) {
        *self.marker.lock().unwrap() = Some((marker.to_vec(), forward));
    }

    /// Whether to forward `data` before dropping, when it holds the marker.
    fn hit(&self, data: &[u8]) -> Option<bool> {
        let mut marker = self.marker.lock().unwrap();
        let (m, forward) = marker.as_ref()?;
        if !data.windows(m.len()).any(|w| w == m.as_slice()) {
            return None;
        }
        let forward = *forward;
        *marker = None;
        self.dropped.fetch_add(1, Ordering::SeqCst);
        Some(forward)
    }

    async fn serve(self: Arc<Self>, client: TcpStream) {
        let Ok(server) = TcpStream::connect(&self.target).await else {
            return;
        };
        let (mut from_client, mut to_client) = client.into_split();
        let (mut from_server, mut to_server) = server.into_split();
        let muted = Arc::new(AtomicBool::new(false));
        let down = tokio::spawn({
            let muted = muted.clone();
            async move {
                let mut buf = vec![0; 65536];
                while let Ok(n @ 1..) = from_server.read(&mut buf).await {
                    if muted.load(Ordering::SeqCst) || to_client.write_all(&buf[..n]).await.is_err()
                    {
                        break;
                    }
                }
            }
        });
        let mut buf = vec![0; 65536];
        while let Ok(n @ 1..) = from_client.read(&mut buf).await {
            let data = &buf[..n];
            if let Some(forward) = self.hit(data) {
                muted.store(true, Ordering::SeqCst);
                if forward {
                    let _ = to_server.write_all(data).await;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                break;
            }
            if to_server.write_all(data).await.is_err() {
                break;
            }
        }
        down.abort();
    }
}

/// A URL through the proxy, the proxy, a connection to the server and the namespace's fence.
struct Proxied {
    url: String,
    proxy: Arc<Dropping>,
    admin: MultiplexedConnection,
    fence: String,
}

async fn proxied() -> Option<Proxied> {
    let url = redis_url("redis")?;
    let location = RedisLocation::parse(&url).unwrap();
    let (host, port) = location.nodes[0].clone();
    let proxy = Arc::new(Dropping {
        target: format!("{host}:{port}"),
        marker: Mutex::default(),
        dropped: AtomicU32::new(0),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap().port();
    tokio::spawn({
        let proxy = proxy.clone();
        async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(proxy.clone().serve(client));
            }
        }
    });
    let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
    let admin = client.get_multiplexed_async_connection().await.unwrap();
    Some(Proxied {
        url: url.replacen(&format!("{host}:{port}"), &format!("127.0.0.1:{local}"), 1),
        proxy,
        admin,
        fence: format!("{{{}}}:f", location.namespace),
    })
}

async fn fence(p: &mut Proxied) -> i64 {
    let at: Option<i64> = redis::cmd("GET")
        .arg(&p.fence)
        .query_async(&mut p.admin)
        .await
        .unwrap();
    at.unwrap_or(0)
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

async fn an_unanswered_create_is_resolved_by_a_fenced_get(forward: bool) {
    let Some(mut p) = proxied().await else {
        return;
    };
    let store = internal::open(&p.url, &Credentials::None, retry()).unwrap();
    store.open().await.unwrap();
    let before = fence(&mut p).await;
    p.proxy.drop_next(b"EVALSHA", forward);
    assert_eq!(
        store.create("runs/a/log/0", b("one")).await,
        Ok(Create::Created)
    );
    assert_eq!(p.proxy.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        fence(&mut p).await,
        before + 1,
        "the get resolving it wrote the fence"
    );
    assert_eq!(store.get("runs/a/log/0").await, Ok(Some(b("one"))));
    assert_eq!(
        store.create("runs/a/log/0", b("two")).await,
        Ok(Create::Exists)
    );
}

#[tokio::test]
async fn an_unanswered_create_is_resolved_by_a_fenced_get_landed() {
    an_unanswered_create_is_resolved_by_a_fenced_get(true).await
}

#[tokio::test]
async fn an_unanswered_create_is_resolved_by_a_fenced_get_lost() {
    an_unanswered_create_is_resolved_by_a_fenced_get(false).await
}

fn unreplicated(url: &str) -> RedisStore {
    let location = RedisLocation::parse(&url.replace("replicas=0", "replicas=1")).unwrap();
    let store = RedisStore::open(location, &Credentials::None, retry()).unwrap();
    store.set_waitaof_ms(50);
    store
}

#[tokio::test]
async fn a_create_the_replicas_never_fsync_is_not_acknowledged() {
    let Some(mut p) = proxied().await else {
        return;
    };
    let store = unreplicated(&p.url);
    let refused = store.create("runs/a/log/0", b("one")).await.unwrap_err();
    assert!(
        refused.message.contains("WAITAOF 1 1 answered [1, 0]"),
        "{}",
        refused.message
    );
    let object = p.fence.replace(":f", ":o:runs/a/log/0");
    let landed: Option<Vec<u8>> = redis::cmd("GET")
        .arg(object)
        .query_async(&mut p.admin)
        .await
        .unwrap();
    assert_eq!(
        landed.as_deref(),
        Some(b"one".as_slice()),
        "it landed, never acknowledged"
    );
}

#[tokio::test]
async fn reads_and_deletes_answer_once_what_they_saw_is_durable() {
    let Some(mut p) = proxied().await else {
        return;
    };
    let store = internal::open(&p.url, &Credentials::None, retry()).unwrap();
    store.create("runs/a/start", b("one")).await.unwrap();
    let before = fence(&mut p).await;
    assert_eq!(store.get("runs/a/start").await, Ok(Some(b("one"))));
    assert_eq!(store.get("runs/b/start").await, Ok(None));
    let listed = store.list("runs/", None, 10, None).await.unwrap();
    assert_eq!(listed.keys, ["runs/a/start"]);
    store.delete("runs/b/start").await.unwrap();
    assert_eq!(
        fence(&mut p).await,
        before + 4,
        "each read and a delete of nothing"
    );
    drop(store);
    let unreplicated = unreplicated(&p.url);
    let short = "WAITAOF 1 1 answered [1, 0]";
    for key in ["runs/a/start", "runs/b/start"] {
        let refusals = [
            unreplicated.get(key).await.err(),
            unreplicated.list(key, None, 10, None).await.err(),
            unreplicated.delete(key).await.err(),
        ];
        for refused in refusals {
            let message = refused.expect("refused").message;
            assert!(message.contains(short), "{key}: {message}");
        }
    }
}
