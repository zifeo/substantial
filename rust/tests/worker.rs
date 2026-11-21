//! Opening namespaces and running workers on an idle namespace.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{LONG, Recording, eventually, namespaces, script};
use substantial::__internal::{self as internal, MemoryStore, Store};
use substantial::{Credentials, Error, Substantial, Workflow};

fn workflows() -> Vec<Workflow<i32>> {
    vec![Workflow::without_input("example", |_c| async { Ok(1) })]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_lists_every_workflow_then_shuts_down() {
    for ns in namespaces() {
        let store = Recording::new(ns.store);
        let sub = internal::create(store.clone()).await.unwrap();
        let other = Workflow::without_input("other", |_c| async { Ok(2) });
        let worker = script(&sub, vec![workflows().remove(0), other])
            .start()
            .unwrap();
        eventually("listings", async || store.listed().len() >= 4).await;
        let listed = store.listed();
        for workflow in ["example", "other"] {
            let prefix = format!("queues/{workflow}/due/");
            let times = listed.iter().filter(|l| **l == prefix).count();
            assert!(times >= 2, "{}: {prefix} listed again: {listed:?}", ns.name);
        }
        internal::wake(&worker);
        tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn a_failing_store_never_stops_the_worker() {
    let store = Recording::new(Arc::new(MemoryStore::open("worker-failing").unwrap()));
    let sub = internal::create(store.clone()).await.unwrap();
    store.failing.store(true, Ordering::SeqCst);
    let worker = script(&sub, workflows()).start().unwrap();
    let before = store.listed().len();
    eventually("listings", async || store.listed().len() >= before + 3).await;
    tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_whose_commit_keeps_failing_is_dropped_and_tried_again_once_its_claim_expired() {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open("worker-op-retries").unwrap());
    let store = Recording::new(inner.clone());
    let sub = internal::create(store.clone()).await.unwrap();
    let run = sub.start(&workflows()[0], ()).await.unwrap();
    store.failing_segments.store(true, Ordering::SeqCst);
    let worker = script(&sub, workflows())
        .set("claim_ttl", "2s")
        .start()
        .unwrap();
    let claims = format!("runs/{}/claim/", run.chain());
    let claimed = async || {
        let created = store.created();
        created.iter().any(|(k, _)| k.starts_with(&claims))
    };
    eventually("a claim", claimed).await;
    let started = Instant::now();
    // § t916k65mb.a@t9k68
    eventually("the claims deleted", async || {
        let listing = inner.list(&claims, None, 100, None).await.unwrap();
        listing.keys.is_empty()
    })
    .await;
    store.failing_segments.store(false, Ordering::SeqCst);
    let value: i32 = run.result().timeout(LONG).await.unwrap();
    assert_eq!(value, 1);
    // A worker listing every 50 ms would have taken it at once had the drop freed it.
    let retried = started.elapsed();
    assert!(
        retried >= Duration::from_secs(1),
        "retried after {retried:?}"
    );
    // § end t916k65mb.a
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn workers_refuse_invalid_configuration() {
    let sub = Substantial::create("memory://worker-config").await.unwrap();
    let twice = vec![workflows().remove(0), workflows().remove(0)];
    assert!(matches!(
        script(&sub, twice).start(),
        Err(Error::ConfigError(_))
    ));
    assert!(matches!(
        script(&sub, Vec::<Workflow<i32>>::new()).start(),
        Err(Error::ConfigError(_))
    ));
    assert!(matches!(
        script(&sub, workflows()).set("concurrency", 0).start(),
        Err(Error::ConfigError(_))
    ));
    assert!(matches!(
        script(&sub, workflows()).set("concurency", 1).start(),
        Err(Error::ConfigError(_))
    ));
    assert!(matches!(
        sub.worker(workflows()).start(),
        Err(Error::ConfigError(_))
    ));
}

#[tokio::test]
async fn create_and_open_check_the_layout() {
    for ns in namespaces() {
        let store: Arc<dyn Store> = ns.store;
        let configured = internal::open_store(store.clone())
            .credentials(Credentials::Env)
            .await;
        assert!(matches!(configured, Err(Error::ConfigError(_))));
        // § mgfgmd25p@44ze4
        let missing = internal::open_store(store.clone()).await;
        assert!(
            matches!(missing, Err(Error::LayoutError(_))),
            "{}: open of nothing",
            ns.name
        );
        // § end mgfgmd25p
        // § mgfgmd25p.a@4357k
        internal::create(store.clone()).await.unwrap();
        let again = internal::create(store.clone()).await;
        assert!(
            again.is_ok(),
            "{}: a create of this layout opens it",
            ns.name
        );
        internal::open_store(store.clone()).await.unwrap();
        // § end mgfgmd25p.a
        let marker = store.get("substantial.json").await.unwrap().unwrap();
        // § sqbjc1adw.b@pz6j7
        assert_eq!(&marker[..], br#"{"layout":2,"encoding":"protobuf"}"#);
    }
    // A namespace of the layout before, with its partitions: drained by the version that wrote
    // it, never opened by this one.
    let old: Arc<dyn Store> = Arc::new(MemoryStore::open("worker-layout-1").unwrap());
    let marker = br#"{"layout":1,"encoding":"protobuf","partitions":4,"hash":"xxh3-64"}"#;
    old.create("probe/exclusive", Default::default())
        .await
        .unwrap();
    old.create("substantial.json", bytes::Bytes::from_static(marker))
        .await
        .unwrap();
    for opened in [
        internal::open_store(old.clone()).await,
        internal::create(old.clone()).await,
    ] {
        match opened {
            Err(Error::LayoutError(m)) => assert!(m.contains("has layout 1"), "{m}"),
            other => panic!("{:?}", other.map(|_| ())),
        }
    }
    assert!(matches!(
        Substantial::open("s3://bucket/ns")
            .credentials(Credentials::Env)
            .await,
        Err(Error::ConfigError(_))
    ));
}

#[tokio::test]
async fn a_store_that_accepts_two_creates_fails_the_probe() {
    use bytes::Bytes;
    use substantial::__internal::{Capabilities, Create, ListWait, Listing, StoreFuture};

    struct Overwriting(MemoryStore);
    impl Store for Overwriting {
        fn capabilities(&self) -> Capabilities {
            self.0.capabilities()
        }
        fn open(&self) -> futures::future::BoxFuture<'_, substantial::Result<()>> {
            Store::open(&self.0)
        }
        fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
            Box::pin(async move {
                self.0.delete(key).await?;
                self.0.create(key, bytes).await
            })
        }
        fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
            self.0.get(key)
        }
        fn list<'a>(
            &'a self,
            prefix: &'a str,
            start_after: Option<&'a str>,
            max_keys: u32,
            wait: Option<ListWait>,
        ) -> StoreFuture<'a, Listing> {
            self.0.list(prefix, start_after, max_keys, wait)
        }
        fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
            self.0.delete(key)
        }
    }

    let store = Arc::new(Overwriting(MemoryStore::open("probe-overwrite").unwrap()));
    let created = internal::create(store).await;
    assert!(
        matches!(created, Err(Error::LayoutError(_))),
        "{:?}",
        created.err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_worker_stops_serving() {
    let store = Arc::new(MemoryStore::open("worker-dropped").unwrap());
    let sub = internal::create(store).await.unwrap();
    let worker = script(&sub, workflows()).start().unwrap();
    drop(worker);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let run = sub.start(&workflows()[0], ()).await.unwrap();
    let served = tokio::time::timeout(Duration::from_secs(2), run.result()).await;
    assert!(
        served.is_err(),
        "the dropped worker still serves: {served:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_polls_up_to_max_interval() {
    use std::sync::atomic::AtomicUsize;
    use substantial::{Context, Observed};

    let gets = Arc::new(AtomicUsize::new(0));
    let counted = gets.clone();
    let observe = move |e: &Observed| {
        if matches!(e, Observed::StoreOp(op) if op.op == "get") {
            counted.fetch_add(1, Ordering::SeqCst);
        }
    };
    let url = "memory://max-interval";
    let sub = Substantial::create(url).await.unwrap();
    let client = Substantial::open(url).observe(observe).await.unwrap();
    let a_second = Workflow::without_input("a_second", |c: Context| async move {
        c.sleep(Duration::from_secs(1)).await?;
        Ok(1)
    });
    let worker = script(&sub, &a_second).start().unwrap();
    let run = client.start(&a_second, ()).await.unwrap();
    let ended = run.result().max_interval(Duration::from_millis(20)).await;
    assert_eq!(ended.unwrap(), 1);
    // Every 20 ms for a second, where doubling from 10 ms up to 30 s polls 8 times.
    let gets = gets.load(Ordering::SeqCst);
    assert!(gets > 20, "{gets}");
    let refused = run.result().max_interval(Duration::ZERO).await;
    assert!(
        matches!(&refused, Err(Error::InvalidArgument(m)) if m.contains("max_interval")),
        "{refused:?}"
    );
    worker.shutdown().await.unwrap();
}
