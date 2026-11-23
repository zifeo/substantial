//! The store contract of I-26, run against every adapter of this host, Redis with the feature
//! `redis` (see `common` for the stores).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
#[cfg(feature = "redis")]
use common::redis_url;
use common::{S3_VERSIONED_URL, kind, retry, s3_credentials, s3_url, s3_url_of};
use substantial::__internal::{
    self as internal, Cas, Clock, Create, DAY_MS, FsStore, ListWait, MemoryStore, Store, StoreRetry,
};
use substantial::{Credentials, Error};

struct Fixture {
    store: Arc<dyn Store>,
    _dir: Option<tempfile::TempDir>,
    _failover: Option<common::FailoverLock>,
}

fn memory() -> Fixture {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let ns = format!("contract-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    Fixture {
        store: Arc::new(MemoryStore::open(&ns).unwrap()),
        _dir: None,
        _failover: None,
    }
}

fn fs() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStore::open(dir.path().join("ns"), retry()).unwrap());
    Fixture {
        store,
        _dir: Some(dir),
        _failover: None,
    }
}

fn s3() -> Fixture {
    Fixture {
        store: internal::open(&s3_url("s3"), &s3_credentials(), retry()).unwrap(),
        _dir: None,
        _failover: None,
    }
}

#[cfg(feature = "redis")]
fn redis_on(topology: &str) -> Fixture {
    Fixture {
        store: internal::open(&redis_url(topology), &Credentials::None, retry()).unwrap(),
        _dir: None,
        _failover: Some(common::failover_lock(false)),
    }
}

#[cfg(feature = "redis")]
fn redis() -> Fixture {
    redis_on("redis")
}

#[cfg(feature = "redis")]
fn redis_sentinel() -> Fixture {
    redis_on("redis-sentinel")
}

#[cfg(feature = "redis")]
fn redis_cluster() -> Fixture {
    redis_on("redis-cluster")
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// The value of `key`, without its ETag.
async fn value(s: &Arc<dyn Store>, key: &str) -> Option<Bytes> {
    s.get(key).await.unwrap().map(|o| o.bytes)
}

async fn etag(s: &Arc<dyn Store>, key: &str) -> String {
    s.get(key).await.unwrap().expect("the key exists").etag
}

async fn create_exclusive(f: Fixture) {
    let s = &f.store;
    assert_eq!(
        kind(s.create("runs/a/start", b("one"), None).await),
        Ok("created")
    );
    assert_eq!(
        s.create("runs/a/start", b("two"), None).await,
        Ok(Create::Exists)
    );
    assert_eq!(value(s, "runs/a/start").await, Some(b("one")));
    assert_eq!(value(s, "runs/b/start").await, None);
    assert_eq!(value(s, "runs/a").await, None);
    assert_eq!(
        kind(s.create("runs/a/empty", Bytes::new(), None).await),
        Ok("created")
    );
    assert_eq!(value(s, "runs/a/empty").await, Some(Bytes::new()));
}

async fn compare_and_set(f: Fixture) {
    // § kkad6aqxp@zj3er
    let s = &f.store;
    s.create("runs/c", b("one"), None).await.unwrap();
    let first = etag(s, "runs/c").await;
    // An ETag no store makes, which RustFS takes as well formed.
    let wrong = "substantial-probe-mismatch";
    assert_eq!(
        s.cas("runs/c", b("two"), wrong, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(s, "runs/c").await, Some(b("one")));
    let Ok(Cas::Written { etag: second }) = s.cas("runs/c", b("two"), &first, None).await else {
        panic!("the ETag read names the value")
    };
    // § one kkad6aqxp.b@jtmwm
    assert_ne!(second, first, "the bytes changed, so did the ETag");
    assert_eq!(etag(s, "runs/c").await, second);
    assert_eq!(value(s, "runs/c").await, Some(b("two")));
    assert_eq!(
        s.cas("runs/c", b("three"), &first, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(s, "runs/c").await, Some(b("two")));
    // § one kkad6aqxp.a@1f5fg
    assert_eq!(
        s.cas("runs/none", b("x"), &first, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(s, "runs/none").await, None);
}

async fn compare_and_set_after_a_delete(f: Fixture) {
    let s = &f.store;
    s.create("runs/d", b("one"), None).await.unwrap();
    let read = etag(s, "runs/d").await;
    s.delete("runs/d").await.unwrap();
    // § one kkad6aqxp.a@74cwv
    assert_eq!(
        s.cas("runs/d", b("two"), &read, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(s, "runs/d").await, None);
}

/// What the probe of `create` and `open` requires of a store, which every store answers.
async fn probe(f: Fixture) {
    let s = &f.store;
    let first = s
        .create("probe/exclusive", Bytes::new(), None)
        .await
        .unwrap();
    assert!(matches!(first, Create::Created { .. } | Create::Exists));
    let again = s.create("probe/exclusive", Bytes::new(), None).await;
    assert_eq!(again, Ok(Create::Exists));
    // The ETag the core's probe names, which no store makes.
    let wrong = "substantial-probe-mismatch";
    let refused = s.cas("probe/exclusive", Bytes::new(), wrong, None).await;
    assert_eq!(refused, Ok(Cas::Mismatch));
    assert_eq!(value(s, "probe/exclusive").await, Some(Bytes::new()));
    let refused = s.cas("probe/missing", Bytes::new(), wrong, None).await;
    assert_eq!(refused, Ok(Cas::Mismatch));
    assert_eq!(deleted_key_comes_back(s).await, Ok(false));
}

/// The probe's last step: a compare-and-set naming the ETag of a deleted key, which must answer
/// mismatch; true when the key came back instead.
async fn deleted_key_comes_back(s: &Arc<dyn Store>) -> Result<bool, internal::StoreFailure> {
    s.create("probe/deleted", Bytes::new(), None).await?;
    let read = etag(s, "probe/deleted").await;
    s.delete("probe/deleted").await?;
    let back = s.cas("probe/deleted", Bytes::new(), &read, None).await?;
    if let Cas::Written { .. } = back {
        s.delete("probe/deleted").await?;
    }
    Ok(back != Cas::Mismatch)
}

async fn a_create_answers_the_etag_a_cas_names(f: Fixture) {
    let s = &f.store;
    let Ok(Create::Created { etag: created }) = s.create("runs/n", b("one"), None).await else {
        panic!("a fresh key is created")
    };
    assert_eq!(created, etag(s, "runs/n").await);
    let written = s.cas("runs/n", b("two"), &created, None).await.unwrap();
    assert!(matches!(written, Cas::Written { .. }));
}

async fn concurrent_compare_and_sets_of_one_key(f: Fixture) {
    f.store.create("runs/h", b("start"), None).await.unwrap();
    let read = etag(&f.store, "runs/h").await;
    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let (s, read) = (f.store.clone(), read.clone());
            tokio::spawn(async move {
                let written = s.cas("runs/h", b(&format!("writer {i}")), &read, None);
                (i, written.await.unwrap())
            })
        })
        .collect();
    let mut winners = Vec::new();
    for t in tasks {
        let (i, outcome) = t.await.unwrap();
        if let Cas::Written { .. } = outcome {
            winners.push(i);
        }
    }
    assert_eq!(
        winners.len(),
        1,
        "exactly one writer of a read wins: {winners:?}"
    );
    let stored = value(&f.store, "runs/h").await;
    assert_eq!(stored, Some(b(&format!("writer {}", winners[0]))));
}

async fn expiries_in_range(f: Fixture) {
    // § r4rsq6spn.c@jqpsc
    let s = &f.store;
    for days in [0, 36_501] {
        assert!(
            s.create("runs/x", b("x"), Some(days)).await.is_err(),
            "{days}"
        );
        assert!(s.check_expiry(&[days]).await.is_err(), "{days}");
    }
    s.create("runs/x", b("x"), Some(1)).await.unwrap();
    assert_eq!(value(s, "runs/x").await, Some(b("x")));
    let read = etag(s, "runs/x").await;
    assert!(s.cas("runs/x", b("y"), &read, Some(0)).await.is_err());
    let written = s.cas("runs/x", b("y"), &read, Some(36_500)).await.unwrap();
    assert!(matches!(written, Cas::Written { .. }));
    assert_eq!(value(s, "runs/x").await, Some(b("y")));
    assert_eq!(value(s, "runs/x").await, Some(b("y")));
}

async fn delete_semantics(f: Fixture) {
    let s = &f.store;
    assert_eq!(s.delete("claims/missing").await, Ok(()));
    s.create("claims/x", b("1"), None).await.unwrap();
    assert_eq!(s.delete("claims/x").await, Ok(()));
    assert_eq!(value(s, "claims/x").await, None);
    assert_eq!(s.delete("claims/x").await, Ok(()));
    assert_eq!(
        kind(s.create("claims/x", b("2"), None).await),
        Ok("created"),
        "a hint may come back"
    );
    assert_eq!(value(s, "claims/x").await, Some(b("2")));
}

async fn list_in_byte_order(f: Fixture) {
    let s = &f.store;
    // "q/a/x" sorts after "q/a.b" by full key though its directory "a" sorts before "a.b".
    let keys = [
        "q/a-b", "q/a.b", "q/a/x", "q/a0", "q/9", "q/b/c/d", "q/é", "qq/a", "p/a", "q0",
    ];
    for k in keys {
        s.create(k, b(k), None).await.unwrap();
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
                    s.create("runs/r/log/0", b(&format!("writer {i}")), None)
                        .await
                        .unwrap(),
                )
            })
        })
        .collect();
    let mut winners = Vec::new();
    for t in tasks {
        let (i, outcome) = t.await.unwrap();
        if let Create::Created { .. } = outcome {
            winners.push(i);
        }
    }
    assert_eq!(winners.len(), 1, "exactly one create wins: {winners:?}");
    let stored = value(&f.store, "runs/r/log/0").await;
    assert_eq!(stored, Some(b(&format!("writer {}", winners[0]))));
}

/// The contract's tests on `adapter`, and the `extra` ones that apply only to some adapters.
macro_rules! contract {
    ($adapter:ident $(, $extra:ident)*) => {
        mod $adapter {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn create_exclusive() {
                let f = super::$adapter();
                super::create_exclusive(f).await
            }
            #[tokio::test]
            async fn delete_semantics() {
                let f = super::$adapter();
                super::delete_semantics(f).await
            }
            #[tokio::test]
            async fn list_in_byte_order() {
                let f = super::$adapter();
                super::list_in_byte_order(f).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
            async fn concurrent_creates_of_one_key() {
                let f = super::$adapter();
                super::concurrent_creates_of_one_key(f).await
            }
            #[tokio::test]
            async fn compare_and_set() {
                super::compare_and_set(super::$adapter()).await
            }
            #[tokio::test]
            async fn probe() {
                super::probe(super::$adapter()).await
            }
            #[tokio::test]
            async fn a_create_answers_the_etag_a_cas_names() {
                super::a_create_answers_the_etag_a_cas_names(super::$adapter()).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
            async fn concurrent_compare_and_sets_of_one_key() {
                let f = super::$adapter();
                super::concurrent_compare_and_sets_of_one_key(f).await
            }
            #[tokio::test]
            async fn expiries_in_range() {
                super::expiries_in_range(super::$adapter()).await
            }
            $(
                #[tokio::test]
                async fn $extra() {
                    super::$extra(super::$adapter()).await
                }
            )*
        }
    };
}

// Redis listings wait, so they cannot refuse to.
contract!(memory, list_refuses_wait, compare_and_set_after_a_delete);
contract!(fs, list_refuses_wait, compare_and_set_after_a_delete);
contract!(s3, list_refuses_wait, compare_and_set_after_a_delete);
#[cfg(feature = "redis")]
contract!(redis, compare_and_set_after_a_delete);
#[cfg(feature = "redis")]
contract!(redis_sentinel, compare_and_set_after_a_delete);
#[cfg(feature = "redis")]
contract!(redis_cluster, compare_and_set_after_a_delete);

/// RustFS 1.0.1 on a versioned bucket compares `If-Match` with the newest version holding data,
/// past the delete marker that makes the key missing, so a compare-and-set brings a deleted key
/// back, where AWS answers 404: it fails the probe, which refuses the store at open.
/// The only use of the versioned bucket. This fails once RustFS meets the contract.
#[tokio::test]
async fn rustfs_versioned_fails_the_probe_of_a_deleted_key() {
    let url = s3_url_of(S3_VERSIONED_URL, "s3");
    let s = internal::open(&url, &s3_credentials(), retry()).unwrap();
    let back = deleted_key_comes_back(&s).await.unwrap();
    assert!(
        back,
        "RustFS on a versioned bucket now refuses a compare-and-set naming a deleted key: it \
         meets the contract, so the versioned bucket can run the S3 tests again"
    );
}

#[tokio::test]
async fn fs_checks_key_components() {
    let f = fs();
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
            f.store.create(key, b("x"), None).await.is_err(),
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
    assert_eq!(kind(f.store.create(&ok, b("x"), None).await), Ok("created"));
    assert_eq!(
        kind(f.store.create("a/.tmp", b("x"), None).await),
        Ok("created"),
        ".tmp is reserved at the root only"
    );
}

#[tokio::test]
async fn fs_keys_survive_reopen_and_hide_temporaries() {
    let dir = tempfile::tempdir().unwrap();
    let first = FsStore::open(dir.path(), retry()).unwrap();
    first.create("runs/a/start", b("one"), None).await.unwrap();
    std::fs::write(dir.path().join(".tmp").join("leftover"), "x").unwrap();
    let second = FsStore::open(dir.path(), retry()).unwrap();
    let found = second.get("runs/a/start").await.unwrap().map(|o| o.bytes);
    assert_eq!(found, Some(b("one")));
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
    a.create("k", b("v"), None).await.unwrap();
    assert_eq!(again.get("k").await.unwrap().map(|o| o.bytes), Some(b("v")));
    assert_eq!(other.get("k").await, Ok(None));
}

#[tokio::test]
async fn urls_name_the_adapter() {
    let none = &Credentials::None;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ns");
    internal::open("memory://url-test", none, retry()).unwrap();
    let file = internal::open(&format!("file:{}", path.display()), none, retry()).unwrap();
    file.create("k", b("v"), None).await.unwrap();
    let again = internal::open(&format!("file://{}", path.display()), none, retry()).unwrap();
    assert_eq!(again.get("k").await.unwrap().map(|o| o.bytes), Some(b("v")));
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

#[cfg(not(feature = "redis"))]
#[test]
fn redis_urls_without_the_extra_name_it() {
    let url = "redis://127.0.0.1:1/ns?db=0&replicas=0";
    match internal::open(url, &Credentials::None, retry()) {
        Err(Error::ConfigError(m)) => {
            assert!(m.contains("cargo add substantial --features redis"), "{m}")
        }
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("opened without the redis feature"),
    }
}

/// A clock a test moves, in ms since the epoch.
fn clock(at: i64) -> (Clock, Arc<std::sync::atomic::AtomicI64>) {
    let now = Arc::new(std::sync::atomic::AtomicI64::new(at));
    let read = now.clone();
    (Arc::new(move || read.load(Ordering::SeqCst)), now)
}

const T0: i64 = 1_795_000_000_000;

/// How the stores that expire keys themselves, by their own clock, keep them.
async fn exact_expiry(s: Arc<dyn Store>, now: Arc<std::sync::atomic::AtomicI64>) {
    let day = DAY_MS as i64;
    s.create("runs/e", b("one"), Some(1)).await.unwrap();
    s.create("runs/kept", b("kept"), Some(1)).await.unwrap();
    let read = etag(&s, "runs/kept").await;
    // § one r4rsq6spn.a@ahq7m
    let cleared = s.cas("runs/kept", b("kept"), &read, None).await.unwrap();
    assert!(matches!(cleared, Cas::Written { .. }));
    now.store(T0 + day - 1, Ordering::SeqCst);
    assert_eq!(
        value(&s, "runs/e").await,
        Some(b("one")),
        "kept for the whole day"
    );
    let read = etag(&s, "runs/e").await;
    now.store(T0 + day, Ordering::SeqCst);
    // § one r4rsq6spn@0k6gk
    assert_eq!(
        value(&s, "runs/e").await,
        None,
        "removed once its day passed"
    );
    assert_eq!(
        s.cas("runs/e", b("two"), &read, Some(1)).await,
        Ok(Cas::Mismatch)
    );
    // § one r4rsq6spn.b@gxy6v
    assert_eq!(value(&s, "runs/e").await, None);
    assert_eq!(
        value(&s, "runs/kept").await,
        Some(b("kept")),
        "its expiry cleared"
    );
    assert_eq!(
        kind(s.create("runs/e", b("new"), None).await),
        Ok("created")
    );
    assert_eq!(value(&s, "runs/e").await, Some(b("new")));
    now.store(T0 + 100 * day, Ordering::SeqCst);
    assert_eq!(value(&s, "runs/e").await, Some(b("new")));
}

#[tokio::test]
async fn memory_expires_keys_exactly() {
    let (clock, now) = clock(T0);
    let store = MemoryStore::with_clock("contract-expiry", clock).unwrap();
    exact_expiry(Arc::new(store), now).await;
}

/// The file of version `n` of `key` in the file store at `root`.
fn version_file(root: &std::path::Path, key: &str, n: u64) -> std::path::PathBuf {
    root.join(key).join(format!(".{n:020}"))
}

/// The header's expiry of the version file at `path`.
fn header_of(path: &std::path::Path) -> i64 {
    let file = std::fs::read(path).unwrap();
    i64::from_be_bytes(file[..8].try_into().unwrap())
}

/// The ETag of version `n` of the incarnation `etag` names.
fn with_version(etag: &str, n: u64) -> String {
    let (inc, _) = etag.split_once('.').unwrap();
    format!("{inc}.{n}")
}

/// Every path under `root` but the temporary files, as the store leaves it.
fn left(root: &std::path::Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let shown = path.strip_prefix(root).unwrap().display().to_string();
            if shown == ".tmp" {
                continue;
            }
            found.push(shown);
            if entry.file_type().unwrap().is_dir() {
                dirs.push(path);
            }
        }
    }
    found.sort();
    found
}

#[tokio::test]
async fn fs_expires_keys_exactly_and_sweeps_them() {
    let dir = tempfile::tempdir().unwrap();
    let (clock, now) = clock(T0);
    let store = FsStore::with_clock(dir.path(), retry(), clock).unwrap();
    exact_expiry(Arc::new(store.clone()), now.clone()).await;
    store.create("runs/swept", b("x"), Some(2)).await.unwrap();
    // § qf98sbvzj@6kb04
    let file = std::fs::read(version_file(dir.path(), "runs/swept", 0)).unwrap();
    let at = T0 + 100 * DAY_MS as i64 + 2 * DAY_MS as i64;
    assert_eq!(file[..8], at.to_be_bytes(), "the expiry heads the version");
    assert_eq!(&file[24..], b"x", "after the incarnation");
    assert_eq!(
        header_of(&version_file(dir.path(), "runs/kept", 1)),
        0,
        "no expiry"
    );
    now.store(at, Ordering::SeqCst);
    // § one qf98sbvzj.a@4baw1
    assert_eq!(
        store.sweep().unwrap(),
        1,
        "a tombstone over the expired key"
    );
    assert_eq!(header_of(&version_file(dir.path(), "runs/swept", 1)), -1);
    let s: Arc<dyn Store> = Arc::new(store.clone());
    assert_eq!(value(&s, "runs/swept").await, None);
    // The fake clock is past the real time of every link: what they superseded is old enough,
    // and so is the tombstone, which the next sweep reclaims with its directory.
    assert!(!version_file(dir.path(), "runs/swept", 0).exists());
    assert_eq!(store.sweep().unwrap(), 0);
    assert!(!dir.path().join("runs/swept").exists(), "reclaimed");
    assert!(!version_file(dir.path(), "runs/kept", 0).exists());
    assert!(
        version_file(dir.path(), "runs/kept", 1).exists(),
        "the latest stays"
    );
}

#[tokio::test]
async fn fs_keeps_a_key_as_numbered_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(FsStore::open(dir.path(), retry()).unwrap());
    assert!(
        store
            .create("a/.00000000000000000001", b("x"), None)
            .await
            .is_err()
    );
    // § j4b9rkhg8@r2ax7
    let Ok(Create::Created { etag: first }) = store.create("runs/a", b("x"), None).await else {
        panic!("a fresh key")
    };
    assert!(first.ends_with(".0") && first.len() == 34, "{first}");
    let written = store.cas("runs/a", b("y"), &first, None).await.unwrap();
    assert_eq!(
        written,
        Cas::Written {
            etag: with_version(&first, 1)
        }
    );
    assert_eq!(
        std::fs::read(version_file(dir.path(), "runs/a", 1)).unwrap()[24..],
        *b"y"
    );
    assert!(
        version_file(dir.path(), "runs/a", 0).exists(),
        "superseded just now"
    );
    assert_eq!(
        store.list("", None, 10, None).await.unwrap().keys,
        ["runs/a"]
    );
    // A key may hold keys under it, as layout 2's do.
    store.create("runs/a/start", b("s"), None).await.unwrap();
    let keys = store.list("runs/", None, 10, None).await.unwrap().keys;
    assert_eq!(keys, ["runs/a", "runs/a/start"]);
}

#[tokio::test]
async fn fs_a_delete_takes_the_version_a_late_write_names() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(FsStore::open(dir.path(), retry()).unwrap());
    store.create("k", b("one"), None).await.unwrap();
    let read = etag(&store, "k").await;
    // § j4b9rkhg8.a@bvcyt
    store.delete("k").await.unwrap();
    assert_eq!(header_of(&version_file(dir.path(), "k", 1)), -1);
    assert_eq!(
        store.cas("k", b("late"), &read, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(&store, "k").await, None);
    assert!(
        store
            .list("", None, 10, None)
            .await
            .unwrap()
            .keys
            .is_empty()
    );
    // A write that won the version first stands, and the delete buries it next.
    let Ok(Create::Created { etag: again }) = store.create("k", b("two"), None).await else {
        panic!("a deleted key is created again")
    };
    assert!(
        again.ends_with(".2") && again != with_version(&read, 2),
        "a new incarnation"
    );
    let won = store.cas("k", b("three"), &again, None).await.unwrap();
    store.delete("k").await.unwrap();
    let Cas::Written { etag: won } = won else {
        panic!("the ETag read was current")
    };
    assert_eq!(
        store.cas("k", b("late"), &won, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(&store, "k").await, None);
    // A write whose version a removal freed below the latest is refused.
    store.create("g", b("0"), None).await.unwrap();
    for n in 0..3u32 {
        let read = etag(&store, "g").await;
        store
            .cas("g", b(&format!("{}", n + 1)), &read, None)
            .await
            .unwrap();
    }
    let current = etag(&store, "g").await;
    for n in 0..3 {
        std::fs::remove_file(version_file(dir.path(), "g", n)).unwrap();
    }
    let stale = with_version(&current, 1);
    assert_eq!(
        store.cas("g", b("stale"), &stale, None).await,
        Ok(Cas::Mismatch)
    );
    assert!(
        !version_file(dir.path(), "g", 2).exists(),
        "no version written below the latest"
    );
    assert_eq!(value(&store, "g").await, Some(b("3")));
}

#[tokio::test]
async fn fs_refuses_an_etag_of_a_removed_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    let (clock, now) = clock(T0);
    let fs = FsStore::with_clock(dir.path(), retry(), clock).unwrap();
    let store: Arc<dyn Store> = Arc::new(fs.clone());
    store.create("k", b("old"), None).await.unwrap();
    let stale = etag(&store, "k").await;
    store.delete("k").await.unwrap();
    // The fake clock is past the real time of the tombstone's link: old enough to reclaim.
    now.store(T0 + 100 * DAY_MS as i64, Ordering::SeqCst);
    fs.sweep().unwrap();
    assert!(
        !dir.path().join("k").exists(),
        "the key's directory reclaimed"
    );
    // § j4b9rkhg8.d@3h328
    let Ok(Create::Created { etag: fresh }) = store.create("k", b("new"), None).await else {
        panic!("a removed key is created again")
    };
    assert!(fresh.ends_with(".0"), "numbered from 0 again: {fresh}");
    assert_ne!(fresh, stale, "a new incarnation");
    assert_eq!(
        store.cas("k", b("stale"), &stale, None).await,
        Ok(Cas::Mismatch)
    );
    assert_eq!(value(&store, "k").await, Some(b("new")));
}

#[tokio::test]
async fn fs_reclaims_every_deleted_key_and_its_empty_parents() {
    let dir = tempfile::tempdir().unwrap();
    let (clock, now) = clock(T0);
    let fs = FsStore::with_clock(dir.path(), retry(), clock).unwrap();
    let store: Arc<dyn Store> = Arc::new(fs.clone());
    store.create("runs/a/start", b("kept"), None).await.unwrap();
    for cycle in 0..3 {
        for i in 0..10 {
            let key = format!("queues/w/due/{i:03}~{cycle}");
            store.create(&key, Bytes::new(), Some(1)).await.unwrap();
            store.delete(&key).await.unwrap();
            store
                .create("probe/deleted", Bytes::new(), None)
                .await
                .unwrap();
            store.delete("probe/deleted").await.unwrap();
        }
    }
    now.store(T0 + 100 * DAY_MS as i64, Ordering::SeqCst);
    // § w8a4smsae@44x9q
    fs.sweep().unwrap();
    assert_eq!(
        left(dir.path()),
        [
            "runs",
            "runs/a",
            "runs/a/start",
            "runs/a/start/.00000000000000000000"
        ],
        "nothing left of the deleted keys, nor of their parents"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fs_a_sweep_racing_creates_loses_none() {
    let dir = tempfile::tempdir().unwrap();
    // Every tombstone is old enough to reclaim at once, so sweeps remove directories while the
    // creates and deletes run.
    let (clock, _) = clock(T0 + 100 * DAY_MS as i64);
    let fs = FsStore::with_clock(dir.path(), retry(), clock).unwrap();
    let store: Arc<dyn Store> = Arc::new(fs.clone());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sweeper = {
        let (fs, stop) = (fs.clone(), stop.clone());
        // A thread of its own, which a failed test does not wait for.
        std::thread::spawn(move || {
            let mut sweeps = 0;
            while !stop.load(Ordering::SeqCst) {
                fs.sweep().unwrap();
                sweeps += 1;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            sweeps
        })
    };
    let workers: Vec<_> = (0..8)
        .map(|w| {
            let s = store.clone();
            tokio::spawn(async move {
                for i in 0..20 {
                    let key = format!("q/{w}/k");
                    let mine = b(&format!("{w}-{i}"));
                    let created = s.create(&key, mine.clone(), None).await.unwrap();
                    assert!(
                        matches!(created, Create::Created { .. }),
                        "{key} free again"
                    );
                    assert_eq!(
                        value(&s, &key).await,
                        Some(mine),
                        "{key} kept until deleted"
                    );
                    s.delete(&key).await.unwrap();
                    assert_eq!(value(&s, &key).await, None);
                }
            })
        })
        .collect();
    let mut failed = None;
    for w in workers {
        if let Err(e) = w.await {
            failed.get_or_insert(e);
        }
    }
    stop.store(true, Ordering::SeqCst);
    assert!(sweeper.join().unwrap() > 0);
    if let Some(e) = failed {
        std::panic::resume_unwind(e.into_panic());
    }
    fs.sweep().unwrap();
    assert!(left(dir.path()).is_empty(), "{:?}", left(dir.path()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fs_writers_racing_a_delete_never_bring_the_key_back() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn Store> = Arc::new(FsStore::open(dir.path(), retry()).unwrap());
    store.create("r", b("0"), None).await.unwrap();
    let writers: Vec<_> = (0..8)
        .map(|i| {
            let s = store.clone();
            tokio::spawn(async move {
                let mut written = 0;
                while let Some(read) = s.get("r").await.unwrap() {
                    let next = b(&format!("{i}-{written}"));
                    if let Cas::Written { .. } = s.cas("r", next, &read.etag, None).await.unwrap() {
                        written += 1;
                    }
                }
                written
            })
        })
        .collect();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    store.delete("r").await.unwrap();
    let mut total = 0;
    for w in writers {
        total += w.await.unwrap();
    }
    assert!(total > 0, "the writers wrote before the delete");
    assert_eq!(
        value(&store, "r").await,
        None,
        "no write lands after the delete"
    );
    let latest = std::fs::read_dir(dir.path().join("r"))
        .unwrap()
        .filter_map(|e| e.unwrap().file_name().into_string().ok())
        .max()
        .unwrap();
    assert_eq!(
        header_of(&dir.path().join("r").join(latest)),
        -1,
        "the tombstone is the latest"
    );
}

#[tokio::test]
async fn only_s3_checks_its_expiries() {
    for f in [memory(), fs()] {
        f.store.check_expiry(&[1, 7, 36_500]).await.unwrap();
    }
}

/// A client of the test bucket `url` names, beside the adapter, and the namespace's prefix.
fn raw_s3(url: &str) -> (aws_sdk_s3::Client, internal::S3Location) {
    use aws_sdk_s3::config::{BehaviorVersion, Region};
    let location = internal::S3Location::parse(url).unwrap();
    let keys = aws_sdk_s3::config::Credentials::new(
        common::setting("AWS_ACCESS_KEY_ID"),
        common::setting("AWS_SECRET_ACCESS_KEY"),
        None,
        None,
        "tests",
    );
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&location.endpoint)
        .region(Region::new(location.region.clone()))
        .credentials_provider(keys)
        .force_path_style(true)
        .build();
    (aws_sdk_s3::Client::from_conf(config), location)
}

#[tokio::test]
async fn s3_writes_its_expiry_as_a_tag() {
    for variable in [common::S3_URL] {
        let url = s3_url_of(variable, "s3");
        let s = internal::open(&url, &s3_credentials(), retry()).unwrap();
        let (client, at) = raw_s3(&url);
        let tags = async |key: &str| {
            let tagging = client
                .get_object_tagging()
                .bucket(&at.bucket)
                .key(format!("{}/{key}", at.namespace))
                .send()
                .await
                .unwrap();
            let tags = tagging.tag_set().iter();
            tags.map(|t| format!("{}={}", t.key(), t.value()))
                .collect::<Vec<_>>()
        };
        // § c4fyc3wyk@1q4z4
        s.create("runs/t", b("one"), Some(1)).await.unwrap();
        assert_eq!(tags("runs/t").await, ["substantial-retention=1d"]);
        let read = etag(&s, "runs/t").await;
        s.cas("runs/t", b("two"), &read, Some(30)).await.unwrap();
        assert_eq!(tags("runs/t").await, ["substantial-retention=30d"]);
        let read = etag(&s, "runs/t").await;
        s.cas("runs/t", b("three"), &read, None).await.unwrap();
        assert!(
            tags("runs/t").await.is_empty(),
            "a write without an expiry clears it"
        );
        s.create("runs/u", b("one"), None).await.unwrap();
        assert!(tags("runs/u").await.is_empty());
    }
}

#[tokio::test]
async fn s3_checks_a_lifecycle_rule_expires_each_retention() {
    for variable in [common::S3_URL] {
        let url = s3_url_of(variable, "s3");
        let s = internal::open(&url, &s3_credentials(), retry()).unwrap();
        // § cfdckf61n@b8qwp
        s.check_expiry(&[1, 30, 1]).await.unwrap();
        let Err(Error::ConfigError(m)) = s.check_expiry(&[30, 7, 365]).await else {
            panic!("no rule expires 7 or 365 days")
        };
        let ns = internal::S3Location::parse(&url).unwrap().namespace;
        assert!(m.contains("substantial-retention=7d, 365d"), "{m}");
        let rule = format!(
            r#"{{"Expiration":{{"Days":7}},"Filter":{{"And":{{"Prefix":"{ns}/","Tags":[{{"Key":"substantial-retention","Value":"7d"}}]}}}},"ID":"substantial-expire-7d","Status":"Enabled"}}"#
        );
        assert!(m.contains(&rule), "{m}");
        assert!(!m.contains("30d"), "{m}");
        let elsewhere = url.replace("/tests/", "/elsewhere-");
        let s = internal::open(&elsewhere, &s3_credentials(), retry()).unwrap();
        let Err(Error::ConfigError(m)) = s.check_expiry(&[1]).await else {
            panic!("the rules cover tests/ only")
        };
        assert!(m.contains("substantial-retention=1d"), "{m}");
    }
}
