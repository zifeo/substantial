//! Opening namespaces and running workers on an idle namespace.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{Recording, eventually, namespaces, script};
use substantial::__internal::{self as internal, MemoryStore, Store};
use substantial::{Credentials, Error, Substantial, Workflow};

fn workflows() -> Vec<Workflow<i32>> {
    vec![Workflow::without_input("example", |_c| async { Ok(1) })]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_lists_every_partition_then_shuts_down() {
    for ns in namespaces() {
        let store = Recording::new(ns.store);
        let sub = internal::create(store.clone()).partitions(3).await.unwrap();
        let worker = script(&sub, workflows()).start().unwrap();
        eventually("listings", async || store.listed().len() >= 9).await;
        let listed = store.listed();
        for p in 0..3 {
            let prefix = format!("queues/example/{p:04}/due/");
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
    let sub = internal::create(store.clone()).partitions(1).await.unwrap();
    store.failing.store(true, Ordering::SeqCst);
    let worker = script(&sub, workflows()).start().unwrap();
    let before = store.listed().len();
    eventually("listings", async || store.listed().len() >= before + 3).await;
    tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn workers_refuse_invalid_configuration() {
    let sub = Substantial::create("memory://worker-config")
        .partitions(1)
        .await
        .unwrap();
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
        assert!(matches!(
            internal::create(store.clone()).await,
            Err(Error::ConfigError(_))
        ));
        let missing = internal::open_store(store.clone()).await;
        assert!(
            matches!(missing, Err(Error::LayoutError(_))),
            "{}: open of nothing",
            ns.name
        );
        assert_eq!(
            internal::create(store.clone())
                .partitions(4)
                .await
                .unwrap()
                .partitions(),
            4
        );
        assert_eq!(
            internal::create(store.clone())
                .partitions(4)
                .await
                .unwrap()
                .partitions(),
            4
        );
        assert_eq!(
            internal::open_store(store.clone())
                .await
                .unwrap()
                .partitions(),
            4
        );
        let other = internal::create(store.clone()).partitions(2).await;
        assert!(
            matches!(other, Err(Error::LayoutError(_))),
            "{}: other partitions",
            ns.name
        );
        assert!(matches!(
            internal::create(store).partitions(0).await,
            Err(Error::ConfigError(_))
        ));
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
    let created = internal::create(store).partitions(1).await;
    assert!(
        matches!(created, Err(Error::LayoutError(_))),
        "{:?}",
        created.err()
    );
}
