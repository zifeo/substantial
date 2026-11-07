//! The store contract of I-26, run against every adapter of this host; on S3 only when
//! `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use common::{retry, s3_url};
use substantial::__internal::{
    self as internal, Create, FsStore, ListWait, MemoryStore, Store, StoreRetry,
};
use substantial::{Credentials, Error};

struct Fixture {
    store: Arc<dyn Store>,
    _dir: Option<tempfile::TempDir>,
}

fn memory() -> Option<Fixture> {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let ns = format!("contract-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    Some(Fixture {
        store: Arc::new(MemoryStore::open(&ns).unwrap()),
        _dir: None,
    })
}

fn fs() -> Option<Fixture> {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStore::open(dir.path().join("ns"), retry()).unwrap());
    Some(Fixture {
        store,
        _dir: Some(dir),
    })
}

fn s3() -> Option<Fixture> {
    let url = s3_url("s3")?;
    Some(Fixture {
        store: internal::open(&url, &Credentials::Env, retry()).unwrap(),
        _dir: None,
    })
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

async fn create_exclusive(f: Fixture) {
    let s = &f.store;
    assert_eq!(
        s.create("runs/a/start", b("one")).await,
        Ok(Create::Created)
    );
    assert_eq!(s.create("runs/a/start", b("two")).await, Ok(Create::Exists));
    assert_eq!(s.get("runs/a/start").await, Ok(Some(b("one"))));
    assert_eq!(s.get("runs/b/start").await, Ok(None));
    assert_eq!(s.get("runs/a").await, Ok(None));
    assert_eq!(
        s.create("runs/a/empty", Bytes::new()).await,
        Ok(Create::Created)
    );
    assert_eq!(s.get("runs/a/empty").await, Ok(Some(Bytes::new())));
}

async fn delete_semantics(f: Fixture) {
    let s = &f.store;
    assert_eq!(s.delete("claims/missing").await, Ok(()));
    s.create("claims/x", b("1")).await.unwrap();
    assert_eq!(s.delete("claims/x").await, Ok(()));
    assert_eq!(s.get("claims/x").await, Ok(None));
    assert_eq!(s.delete("claims/x").await, Ok(()));
    assert_eq!(
        s.create("claims/x", b("2")).await,
        Ok(Create::Created),
        "a hint may come back"
    );
    assert_eq!(s.get("claims/x").await, Ok(Some(b("2"))));
}

async fn list_in_byte_order(f: Fixture) {
    let s = &f.store;
    // "q/a/x" sorts after "q/a.b" by full key though its directory "a" sorts before "a.b".
    let keys = [
        "q/a-b", "q/a.b", "q/a/x", "q/a0", "q/9", "q/b/c/d", "q/é", "qq/a", "p/a", "q0",
    ];
    for k in keys {
        s.create(k, b(k)).await.unwrap();
    }
    let list = |prefix: &'static str, after: Option<&'static str>, max: u32| async move {
        s.list(prefix, after, max, None).await.unwrap().keys
    };
    let all = ["q/9", "q/a-b", "q/a.b", "q/a/x", "q/a0", "q/b/c/d", "q/é"];
    assert_eq!(list("q/", None, 100).await, all);
    assert_eq!(list("q/", None, 3).await, all[..3]);
    assert_eq!(list("q/", Some("q/a.b"), 3).await, all[3..6]);
    assert_eq!(
        list("q/", Some("q/a/"), 100).await,
        all[3..],
        "start_after need not be a key"
    );
    assert_eq!(
        list("q/", Some("a"), 100).await,
        all,
        "start_after before the prefix"
    );
    assert_eq!(list("q/", Some("r"), 100).await, Vec::<String>::new());
    assert_eq!(list("q/a", None, 100).await, all[1..5]);
    assert_eq!(list("q/a/", None, 100).await, ["q/a/x"]);
    let mut under_q = all.to_vec();
    under_q.extend(["q0", "qq/a"]);
    assert_eq!(list("q", None, 100).await, under_q);
    assert_eq!(list("q/b/", None, 100).await, ["q/b/c/d"]);
    assert_eq!(list("none/", None, 100).await, Vec::<String>::new());
    assert_eq!(list("", Some("q0"), 100).await, ["qq/a"]);
    assert_eq!(list("", None, 1).await, ["p/a"]);
    assert!(s.list("q/", None, 10, None).await.unwrap().etag.is_none());
}

async fn list_refuses_wait(f: Fixture) {
    assert!(!f.store.capabilities().wait);
    let wait = Some(ListWait {
        etag: None,
        seconds: 1,
    });
    assert!(f.store.list("q/", None, 10, wait).await.is_err());
}

async fn concurrent_creates_of_one_key(f: Fixture) {
    let tasks: Vec<_> = (0..32)
        .map(|i| {
            let s = f.store.clone();
            tokio::spawn(async move {
                (
                    i,
                    s.create("runs/r/log/0", b(&format!("writer {i}")))
                        .await
                        .unwrap(),
                )
            })
        })
        .collect();
    let mut winners = Vec::new();
    for t in tasks {
        let (i, outcome) = t.await.unwrap();
        if outcome == Create::Created {
            winners.push(i);
        }
    }
    assert_eq!(winners.len(), 1, "exactly one create wins: {winners:?}");
    let stored = f.store.get("runs/r/log/0").await.unwrap();
    assert_eq!(stored, Some(b(&format!("writer {}", winners[0]))));
}

macro_rules! contract {
    ($adapter:ident) => {
        mod $adapter {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn create_exclusive() {
                let Some(f) = super::$adapter() else { return };
                super::create_exclusive(f).await
            }
            #[tokio::test]
            async fn delete_semantics() {
                let Some(f) = super::$adapter() else { return };
                super::delete_semantics(f).await
            }
            #[tokio::test]
            async fn list_in_byte_order() {
                let Some(f) = super::$adapter() else { return };
                super::list_in_byte_order(f).await
            }
            #[tokio::test]
            async fn list_refuses_wait() {
                let Some(f) = super::$adapter() else { return };
                super::list_refuses_wait(f).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
            async fn concurrent_creates_of_one_key() {
                let Some(f) = super::$adapter() else { return };
                super::concurrent_creates_of_one_key(f).await
            }
        }
    };
}

contract!(memory);
contract!(fs);
contract!(s3);

#[tokio::test]
async fn fs_checks_key_components() {
    let f = fs().unwrap();
    let long = "x".repeat(256);
    let bad = [
        "a/./b",
        "a/../b",
        "a//b",
        "/a",
        "a/",
        ".tmp/x",
        "a/\0",
        &format!("a/{long}"),
    ];
    for key in bad {
        assert!(
            f.store.create(key, b("x")).await.is_err(),
            "{key:?} must be refused"
        );
        assert!(f.store.get(key).await.is_err(), "{key:?} must be refused");
        assert!(
            f.store.delete(key).await.is_err(),
            "{key:?} must be refused"
        );
    }
    assert!(f.store.list("../", None, 10, None).await.is_err());
    let ok = format!("a/{}", "x".repeat(255));
    assert_eq!(f.store.create(&ok, b("x")).await, Ok(Create::Created));
    assert_eq!(
        f.store.create("a/.tmp", b("x")).await,
        Ok(Create::Created),
        ".tmp is reserved at the root only"
    );
}

#[tokio::test]
async fn fs_keys_survive_reopen_and_hide_temporaries() {
    let dir = tempfile::tempdir().unwrap();
    let first = FsStore::open(dir.path(), retry()).unwrap();
    first.create("runs/a/start", b("one")).await.unwrap();
    std::fs::write(dir.path().join(".tmp").join("leftover"), "x").unwrap();
    let second = FsStore::open(dir.path(), retry()).unwrap();
    assert_eq!(second.get("runs/a/start").await, Ok(Some(b("one"))));
    assert_eq!(
        second.list("", None, 10, None).await.unwrap().keys,
        ["runs/a/start"]
    );
}

#[tokio::test]
async fn memory_namespaces_are_shared_by_name_only() {
    let a = MemoryStore::open("shared-ns").unwrap();
    let again = MemoryStore::open("shared-ns").unwrap();
    let other = MemoryStore::open("other-ns").unwrap();
    a.create("k", b("v")).await.unwrap();
    assert_eq!(again.get("k").await, Ok(Some(b("v"))));
    assert_eq!(other.get("k").await, Ok(None));
}

#[tokio::test]
async fn urls_name_the_adapter() {
    let none = &Credentials::None;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ns");
    internal::open("memory://url-test", none, retry()).unwrap();
    let file = internal::open(&format!("file:{}", path.display()), none, retry()).unwrap();
    file.create("k", b("v")).await.unwrap();
    let again = internal::open(&format!("file://{}", path.display()), none, retry()).unwrap();
    assert_eq!(again.get("k").await, Ok(Some(b("v"))));
    for url in [
        "memory://",
        "memory://x?wait=true",
        "s3://h/bucket/ns",
        "s3://h/bucket/ns?region=r&endpoint=x",
        "redis://h:1/0",
        "nope",
    ] {
        assert!(
            matches!(
                internal::open(url, none, retry()),
                Err(Error::ConfigError(_))
            ),
            "{url}"
        );
    }
    let zero = StoreRetry {
        attempts: 0,
        ..retry()
    };
    assert!(matches!(
        internal::open("memory://x", none, zero),
        Err(Error::ConfigError(_))
    ));
}

#[test]
fn credentials_go_only_where_a_store_takes_them() {
    let explicit = Credentials::S3 {
        access_key_id: "id".into(),
        secret_access_key: "secret".into(),
        session_token: None,
    };
    assert!(!format!("{explicit:?}").contains("secret"));
    let dir = tempfile::tempdir().unwrap();
    let file = format!("file:{}", dir.path().display());
    for (url, credentials) in [
        ("memory://c", &explicit),
        (file.as_str(), &Credentials::Env),
    ] {
        assert!(
            matches!(
                internal::open(url, credentials, retry()),
                Err(Error::ConfigError(_))
            ),
            "{url}"
        );
    }
    let s3 = "s3+http://127.0.0.1:1/b/ns?region=r";
    assert!(matches!(
        internal::open(s3, &Credentials::None, retry()),
        Err(Error::ConfigError(_))
    ));
    let h3 = "h3://127.0.0.1:1/b/ns?region=r";
    assert!(
        !internal::open(s3, &explicit, retry())
            .unwrap()
            .capabilities()
            .wait
    );
    assert!(
        internal::open(h3, &explicit, retry())
            .unwrap()
            .capabilities()
            .wait
    );
}
