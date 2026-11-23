//! Opening namespaces and running workers on an idle namespace.

mod common;

use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{LONG, Recording, eventually, namespaces, script};
use prost::Message;
use substantial::__internal::{self as internal, MemoryStore, Store, layout};
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
async fn a_replay_whose_commit_keeps_failing_is_dropped_and_tried_again_once_its_lease_expired() {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open("worker-op-retries").unwrap());
    let store = Recording::new(inner.clone());
    let sub = internal::create(store.clone()).await.unwrap();
    // A step, so that its replay leases the run.
    let stepping = Workflow::without_input("stepping", |c| async move {
        c.step("one", || async { Ok::<_, Error>(1) }).await
    });
    let run = sub.start(&stepping, ()).await.unwrap();
    store.failing_segments.store(true, Ordering::SeqCst);
    let worker = script(&sub, &stepping)
        .set("claim_ttl", "2s")
        .start()
        .unwrap();
    // The lease's cas lands, then the commit's is issued again `OP_RETRIES` times, 6 in all.
    let lease = async || {
        let bytes = inner.get(&format!("runs/{}", run.chain())).await.unwrap();
        let run = layout::Run::decode(bytes.unwrap().bytes).unwrap();
        matches!(run.kind, Some(layout::run::Kind::Head(head)) if head.lease.is_some())
    };
    eventually("a lease", lease).await;
    eventually("the commit dropped", async || {
        store.failed_segments.load(Ordering::SeqCst) >= 6
    })
    .await;
    let started = Instant::now();
    // § t916k65mb.a@mmfg7
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
        // § mgfgmd25p@p5e37
        let missing = internal::open_store(store.clone()).await;
        assert!(
            matches!(missing, Err(Error::LayoutError(_))),
            "{}: open of nothing",
            ns.name
        );
        // § end mgfgmd25p
        // § mgfgmd25p.a@ry0y1
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
        // § sqbjc1adw.c@2gypc
        assert_eq!(&marker.bytes[..], br#"{"layout":3,"encoding":"protobuf"}"#);
    }
    // A namespace of the layout before, with its partitions: drained by the version that wrote
    // it, never opened by this one.
    let old: Arc<dyn Store> = Arc::new(MemoryStore::open("worker-layout-1").unwrap());
    let marker = br#"{"layout":1,"encoding":"protobuf","partitions":4,"hash":"xxh3-64"}"#;
    old.create("probe/exclusive", Default::default(), None)
        .await
        .unwrap();
    old.create("substantial.json", bytes::Bytes::from_static(marker), None)
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
    // § mgfgmd25p.d@76hnx
    // A namespace of layout 2, Substantial 0.3's: refused, never read.
    let old: Arc<dyn Store> = Arc::new(MemoryStore::open("worker-layout-2").unwrap());
    let marker = br#"{"layout":2,"encoding":"protobuf"}"#;
    old.create("probe/exclusive", Default::default(), None)
        .await
        .unwrap();
    old.create("substantial.json", bytes::Bytes::from_static(marker), None)
        .await
        .unwrap();
    for opened in [
        internal::open_store(old.clone()).await,
        internal::create(old.clone()).await,
    ] {
        match opened {
            Err(Error::ConfigError(m)) => assert!(m.contains("layout 2"), "{m}"),
            other => panic!("{:?}", other.map(|_| ())),
        }
    }
    // § end mgfgmd25p.d
    assert!(matches!(
        Substantial::open("s3://bucket/ns")
            .credentials(Credentials::Env)
            .await,
        Err(Error::ConfigError(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openers_at_once_each_probe_keys_of_their_own() {
    let store = Recording::new(Arc::new(MemoryStore::open("probe-own").unwrap()));
    internal::create(store.clone()).await.unwrap();
    let opens = (0..4).map(|_| internal::open_store(store.clone()).into_future());
    for opened in futures::future::join_all(opens).await {
        opened.unwrap();
    }
    // § mgfgmd25p.e@4nbg3
    let writes = store.writes.lock().unwrap().clone();
    let probes: Vec<&(String, Option<u32>)> = writes
        .iter()
        .filter(|(k, _)| k.starts_with("probe/"))
        .collect();
    let ids: std::collections::BTreeSet<&str> = (probes.iter())
        .map(|(k, _)| k.split('/').nth(1).unwrap())
        .collect();
    assert_eq!(ids.len(), 5, "one id per opener: {probes:?}");
    assert!(ids.iter().all(|id| id.len() == 26), "{ids:?}");
    let left = store.list("probe/", None, 10, None).await.unwrap().keys;
    assert!(left.is_empty(), "{left:?}");
    // § end mgfgmd25p.e
    // § mgfgmd25p.g@x0e2h
    assert!(
        probes.iter().all(|(_, expiry)| *expiry == Some(1)),
        "{probes:?}"
    );
    // § end mgfgmd25p.g
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_checks_the_rule_of_the_probe_keys_at_start() {
    let store = Recording::new(Arc::new(MemoryStore::open("probe-rule").unwrap()));
    let sub = internal::create(store.clone()).await.unwrap();
    let quick = Workflow::without_input("quick", |_c| async { Ok(1) });
    let worker = script(&sub, &quick)
        .set("retention", "30d")
        .start()
        .unwrap();
    let run = sub.start(&quick, ()).await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 1);
    worker.shutdown().await.unwrap();
    // § cfdckf61n.c@zs4rk
    let checked = store.checked.lock().unwrap().clone();
    let [days] = <[Vec<u32>; 1]>::try_from(checked).unwrap();
    assert!(days.contains(&30) && days.contains(&1), "{days:?}");
    // § end cfdckf61n.c
}

#[tokio::test]
async fn a_store_that_accepts_two_creates_fails_the_probe() {
    use bytes::Bytes;
    use substantial::__internal::{
        Capabilities, Cas, Create, ListWait, Listing, Object, StoreFuture,
    };
    use substantial::Result;

    /// Creates over an existing key, or, with `ignores_etags`, writes every compare-and-set.
    struct Overwriting(MemoryStore, bool);
    impl Store for Overwriting {
        fn capabilities(&self) -> Capabilities {
            self.0.capabilities()
        }
        fn open(&self) -> futures::future::BoxFuture<'_, substantial::Result<()>> {
            Store::open(&self.0)
        }
        fn create<'a>(
            &'a self,
            key: &'a str,
            bytes: Bytes,
            expiry: Option<u32>,
        ) -> StoreFuture<'a, Create> {
            Box::pin(async move {
                if !self.1 {
                    self.0.delete(key).await?;
                }
                self.0.create(key, bytes, expiry).await
            })
        }
        fn cas<'a>(
            &'a self,
            key: &'a str,
            bytes: Bytes,
            etag: &'a str,
            expiry: Option<u32>,
        ) -> StoreFuture<'a, Cas> {
            if !self.1 {
                return self.0.cas(key, bytes, etag, expiry);
            }
            Box::pin(async move {
                self.0.delete(key).await?;
                match self.0.create(key, bytes, expiry).await? {
                    Create::Created { etag } => Ok(Cas::Written { etag }),
                    Create::Exists => Ok(Cas::Mismatch),
                }
            })
        }
        fn check_expiry<'a>(
            &'a self,
            days: &'a [u32],
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            self.0.check_expiry(days)
        }
        fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
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

    let store = Arc::new(Overwriting(
        MemoryStore::open("probe-overwrite").unwrap(),
        false,
    ));
    match internal::create(store).await {
        Err(Error::ConfigError(m)) => assert!(m.contains("not exclusive"), "{m}"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    // § mgfgmd25p.f@sv5zc
    let store = Arc::new(Overwriting(MemoryStore::open("probe-etags").unwrap(), true));
    let refused = internal::create(store).await;
    assert!(
        matches!(&refused, Err(Error::ConfigError(_))),
        "{:?}",
        refused.map(|_| ())
    );
    // § end mgfgmd25p.f
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_whose_clock_is_behind_a_runs_last_segment_leaves_it_alone() {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicI64;
    use substantial::{Observed, RunStatus};

    let told = Arc::new(Mutex::new(Vec::new()));
    let seen = told.clone();
    let sub = Substantial::create("memory://skewed")
        .observe(move |e| {
            if let Observed::Skewed(s) = e {
                seen.lock().unwrap().push(s.clone());
            }
        })
        .await
        .unwrap();
    let waiting =
        Workflow::without_input("waiting", |c| async move { c.receive::<i32>("go").await });
    // A minute ahead, past `claim_skew`, 5 s in the script preset: its segment is in the future of
    // every right clock.
    let ahead = Arc::new(AtomicI64::new(60_000));
    let ahead = internal::shifted(script(&sub, &waiting), ahead)
        .start()
        .unwrap();
    let run = sub.start(&waiting, ()).await.unwrap();
    eventually("the wait", async || {
        run.status().await.unwrap() == RunStatus::Suspended
    })
    .await;
    ahead.shutdown().await.unwrap();
    let right = script(&sub, &waiting).start().unwrap();
    run.send("go", 1).await.unwrap();
    // § kr6b4hm8k@4fz22
    eventually("the skew told", async || !told.lock().unwrap().is_empty()).await;
    let skew = told.lock().unwrap()[0].clone();
    assert_eq!(skew.run, run.chain());
    // Not continued: the chain's current run is its first.
    assert_eq!(skew.current, run.chain());
    assert!(
        skew.segment_at - skew.now > chrono::Duration::seconds(55),
        "{skew:?}"
    );
    assert_eq!(
        run.status().await.unwrap(),
        RunStatus::Suspended,
        "left alone"
    );
    // § end kr6b4hm8k
    right.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_from_a_sender_whose_clock_is_ahead_is_consumed_at_once() {
    use std::sync::atomic::AtomicI64;

    let sub = Substantial::create("memory://sender-ahead").await.unwrap();
    let waiting =
        Workflow::without_input(
            "waiting_for_go",
            |c| async move { c.receive::<i32>("go").await },
        );
    let sending = Workflow::new("sending_go", |c, input: serde_json::Value| async move {
        c.send(input["to"].as_str().unwrap(), "go", 7).await?;
        Ok(0)
    });
    let right = script(&sub, &waiting).start().unwrap();
    // An hour ahead: a message keyed by its sender's time would not be due for an hour.
    let ahead = Arc::new(AtomicI64::new(3_600_000));
    let ahead = internal::shifted(script(&sub, &sending), ahead)
        .start()
        .unwrap();
    let run = sub.start(&waiting, ()).await.unwrap();
    let to = serde_json::json!({ "to": run.chain() });
    let sender = sub.start(&sending, to).await.unwrap();
    sender.result().timeout(LONG).await.unwrap();
    // § d9vmk6y6y.e@nc5ck
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 7);
    // § end d9vmk6y6y.e
    ahead.shutdown().await.unwrap();
    right.shutdown().await.unwrap();
}
