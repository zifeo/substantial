//! Retention on every store, on the real clock: the collection's deletes and the tombstone, with
//! retention and tombstone shortened through the settings a worker does not take publicly. The
//! port of `python/tests/test_store_retention.py`, under the same name.

mod common;

use std::time::Duration;

use common::{Hidden, LONG, Recording, namespaces, script};
use substantial::__internal::{self as internal, Store};
use substantial::{Error, Substantial, Worker, Workflow};

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
        // The tombstone's end held until the checks made during it are done; dropped on a panic,
        // the sender releases it.
        let (release, held) = tokio::sync::watch::channel(false);
        let recording = Recording::new(ns.store);
        *recording.hold_delete.lock().unwrap() = Some(("runs/q/gone".into(), held));
        let sub = internal::create(recording).await.unwrap();
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
        release.send(true).unwrap();
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

/// A wakeup of `q` due now, as a late sender or a crashed worker leaves one.
fn late_wakeup(workflow: &str) -> String {
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
    let at = now.format("%Y%m%dt%H%M%S%3fz");
    format!("queues/{workflow}/due/{at}~q~s0000000001")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_keys_of_a_collected_run_never_come_back() {
    let publishing = Workflow::without_input("publishing", |c| async move {
        c.publish("p", 1)?;
        c.step("s", || async { Ok::<_, Error>(1) }).await
    });
    for ns in namespaces() {
        let store = ns.store.clone();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &publishing)
            .hidden("retention_min", "300ms")
            .hidden("retention", "300ms")
            .hidden("tombstone", "2s")
            .hidden("recheck", "200ms")
            .start()
            .unwrap();
        let run = sub.start(&publishing, ()).id("q").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        assert!(!keys(&*store, "pub/q/").await.is_empty(), "{}", ns.name);
        eventually("the collection", Duration::from_secs(10), async || {
            keys(&*store, "runs/q/").await == ["runs/q/gone"]
        })
        .await;
        // § nmpjmw8mf.b@yg43z
        let served = async |expected: &[&str]| {
            let late = late_wakeup("publishing");
            store.create(&late, Default::default()).await.unwrap();
            internal::wake(&worker);
            eventually(
                "the late wakeup served",
                Duration::from_secs(10),
                async || {
                    !keys(&*store, "queues/publishing/due/")
                        .await
                        .contains(&late)
                },
            )
            .await;
            assert!(run.read::<u32>("p").await.unwrap().is_none(), "{}", ns.name);
            assert_eq!(keys(&*store, "runs/q/").await, expected, "{}", ns.name);
            assert!(keys(&*store, "pub/q/").await.is_empty(), "{}", ns.name);
        };
        served(&["runs/q/gone"]).await;
        eventually("the tombstone's end", Duration::from_secs(10), async || {
            keys(&*store, "runs/q/").await.is_empty()
        })
        .await;
        served(&[]).await;
        // § end nmpjmw8mf.b
        worker.shutdown().await.unwrap();
    }
}

fn collecting(sub: &Substantial) -> Worker {
    script(sub, quick())
        .hidden("retention_min", "300ms")
        .hidden("retention", "300ms")
        .hidden("tombstone", "3s")
        .hidden("recheck", "200ms")
        .start()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_collector_stopped_halfway_leaves_the_tombstone_with_its_time() {
    for ns in namespaces() {
        let store = ns.store.clone();
        // The first collector stops at its delete of the run's `start`, after `gone`, as one
        // that crashed there would; dropped on a panic, the sender releases it.
        let (release, held) = tokio::sync::watch::channel(false);
        let recording = Recording::new(ns.store);
        *recording.hold_delete.lock().unwrap() = Some(("runs/q/start".into(), held));
        let stalled = internal::create(recording).await.unwrap();
        let first = collecting(&stalled);
        // A client and a second worker on a handle of their own, which nothing holds.
        let sub = internal::open_store(store.clone()).await.unwrap();
        let run = sub.start(&quick(), ()).id("q").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        eventually(
            "the collection's `gone`",
            Duration::from_secs(10),
            async || {
                keys(&*store, "runs/q/")
                    .await
                    .contains(&"runs/q/gone".to_owned())
            },
        )
        .await;
        // § ja67c3az7.b@844vn
        assert!(
            keys(&*store, "runs/q/")
                .await
                .contains(&"runs/q/start".to_owned()),
            "{}: the collection stopped halfway",
            ns.name
        );
        let taken = |started: substantial::Result<_>| match started {
            Err(Error::RunExists {
                collected: true,
                until: Some(until),
                ..
            }) => until,
            other => panic!("{}: {:?}", ns.name, other.map(|_| ())),
        };
        let until = taken(sub.start(&quick(), ()).id("q").await);
        let second = collecting(&sub);
        eventually(
            "the collection's end",
            Duration::from_secs(10),
            async || keys(&*store, "runs/q/").await == ["runs/q/gone"],
        )
        .await;
        assert_eq!(taken(sub.start(&quick(), ()).id("q").await), until);
        match run.status().await {
            Err(Error::RunNotFound {
                collected: true,
                until: Some(kept),
                ..
            }) => assert_eq!(kept, until, "{}", ns.name),
            other => panic!("{}: {other:?}", ns.name),
        }
        eventually("the tombstone's end", Duration::from_secs(10), async || {
            keys(&*store, "runs/q/").await.is_empty()
        })
        .await;
        let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
        assert!(
            now >= until,
            "{}: ended at {now}, not before {until}",
            ns.name
        );
        // § end ja67c3az7.b
        release.send(true).unwrap();
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
    }
}
