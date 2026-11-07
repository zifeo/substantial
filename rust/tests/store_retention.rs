//! Retention on every store, on the real clock: the collection's deletes and the tombstone, with
//! retention and tombstone shortened through the settings a worker does not take publicly. The
//! port of `python/tests/test_store_retention.py`, under the same name.

mod common;

use std::time::Duration;

use common::{Hidden, LONG, namespaces, script};
use substantial::__internal::{self as internal, Store};
use substantial::{Error, Workflow};

fn quick() -> Workflow<u32> {
    Workflow::without_input("quick", |_c| async { Ok(1) })
}

async fn keys(store: &dyn Store, prefix: &str) -> Vec<String> {
    store.list(prefix, None, 1000, None).await.unwrap().keys
}

/// Waits for `check` to hold, polling, at most `within`.
async fn eventually(what: &str, within: Duration, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + within;
    while !check().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_is_collected_and_its_id_taken_until_the_tombstone_ends() {
    for ns in namespaces() {
        let store = ns.store.clone();
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, quick())
            .hidden("retention_min", "300ms")
            .hidden("retention", "300ms")
            .hidden("tombstone", "2s")
            .hidden("recheck", "200ms")
            .start()
            .unwrap();
        let run = sub.start(&quick(), ()).id("q").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        eventually("the collection", Duration::from_secs(10), async || {
            keys(&*store, "runs/q/").await == ["runs/q/gone"]
        })
        .await;
        match run.status().await {
            Err(Error::RunNotFound { collected, .. }) => assert!(collected, "{}", ns.name),
            other => panic!("{}: {other:?}", ns.name),
        }
        match sub.start(&quick(), ()).id("q").await {
            Err(Error::RunExists { collected, .. }) => assert!(collected, "{}", ns.name),
            other => panic!("{}: {:?}", ns.name, other.map(|_| ())),
        }
        eventually("the tombstone's end", Duration::from_secs(10), async || {
            keys(&*store, "runs/q/").await.is_empty()
        })
        .await;
        let again = sub.start(&quick(), ()).id("q").await.unwrap();
        assert_eq!(
            again.result().timeout(LONG).await.unwrap(),
            1,
            "{}",
            ns.name
        );
        worker.shutdown().await.unwrap();
    }
}
