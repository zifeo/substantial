//! Retention on every store, on the real clock: an ended run's head is written with its retention
//! as its expiry, which the store then removes it by. The port of
//! `python/tests/test_store_retention.py`, under the same names.

mod common;

use std::time::Duration;

use common::{LONG, Recording, namespaces, script};
use substantial::__internal::{self as internal, Store};
use substantial::{Error, RunStatus, Workflow};

fn quick() -> Workflow<u32> {
    Workflow::without_input("quick", |_c| async { Ok(1) }).with_retention(1)
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
async fn an_ended_run_is_written_to_expire_after_its_retention() {
    for ns in namespaces() {
        let recording = Recording::new(ns.store);
        let sub = internal::create(recording.clone()).await.unwrap();
        let worker = script(&sub, quick()).start().unwrap();
        let run = sub.start(&quick(), ()).id("q").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        worker.shutdown().await.unwrap();
        // § a1ys7at7p.a@zarg9
        let expiries: Vec<Option<u32>> = (recording.writes().into_iter())
            .filter(|(k, _)| k == "runs/q")
            .map(|(_, expiry)| expiry)
            .collect();
        let (end, before) = expiries.split_last().unwrap();
        assert_eq!(*end, Some(1), "{}: {expiries:?}", ns.name);
        assert!(
            before.iter().all(Option::is_none),
            "{}: {expiries:?}",
            ns.name
        );
        // § end a1ys7at7p.a
        assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
        assert_eq!(run.result().await.unwrap(), 1, "{}", ns.name);
        let other = Workflow::without_input("other", |_c| async { Ok(2) });
        match sub.start(&other, ()).id("q").await {
            Err(Error::RunExists { id }) => assert_eq!(id, "q", "{}", ns.name),
            other => panic!("{}: {:?}", ns.name, other.map(|_| ())),
        }
    }
}

/// A wakeup of `q` due now, as a late sender or a crashed worker leaves one.
fn late_wakeup(workflow: &str) -> String {
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
    let at = now.format("%Y%m%dt%H%M%S%3fz");
    format!("queues/{workflow}/due/{at}~q~s0000000001")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_keys_of_a_removed_run_never_come_back() {
    let publishing = Workflow::without_input("publishing", |c| async move {
        c.publish("p", 1)?;
        c.step("s", || async { Ok::<_, Error>(1) }).await
    })
    .with_retention(1);
    for ns in namespaces() {
        let store = ns.store.clone();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &publishing).start().unwrap();
        let run = sub.start(&publishing, ()).id("q").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        assert_eq!(run.read::<u32>("p").await.unwrap(), Some(1), "{}", ns.name);
        // As its expiry removes it.
        store.delete("runs/q").await.unwrap();
        // § rbvr70kyv.b@tqym7
        let late = late_wakeup("publishing");
        // § qkh68yrf1.a@ck2m1
        store.create(&late, Default::default(), None).await.unwrap();
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
        assert!(keys(&*store, "runs/q").await.is_empty(), "{}", ns.name);
        assert!(keys(&*store, "archives/").await.is_empty(), "{}", ns.name);
        // § end qkh68yrf1.a
        // § end rbvr70kyv.b
        worker.shutdown().await.unwrap();
    }
}
