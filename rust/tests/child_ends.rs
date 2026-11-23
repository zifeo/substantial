//! A run's end cancels its children still running, unless started with `abandon`; a replay claims
//! its run only before a step attempt. The ports of `python/tests/test_child_ends.py`, under the
//! same names.

mod common;

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use common::{LONG, Recording, script};
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use substantial::__internal::{self as internal, MemoryStore, Store, layout};
use substantial::testing::Env;
use substantial::{Context, Error, Preset, RunStatus, Workflow};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

fn waiter() -> Workflow<String> {
    Workflow::without_input("waiter", |c| async move { c.receive::<String>("go").await })
}

#[derive(Deserialize)]
struct Leaving {
    fail: bool,
}

fn leaving() -> Workflow<String> {
    Workflow::new(
        "leaving",
        |c: Context, Leaving { fail }: Leaving| async move {
            let child = c.start(&waiter(), ()).id("kid").await?;
            if fail {
                return Err(Error::fail("Done", "the parent gave up"));
            }
            Ok(child.chain().to_owned())
        },
    )
}

fn abandoning() -> Workflow<String> {
    Workflow::without_input("abandoning", |c| async move {
        let child = c.start(&waiter(), ()).id("kid").abandon().await?;
        Ok(child.chain().to_owned())
    })
}

/// The reason the child `kid` was cancelled with.
async fn cancelled_with(env: &Env) -> String {
    match env.run(&waiter(), "kid").result().await {
        Err(Error::RunFailed { status, error, .. }) => {
            assert_eq!(status, RunStatus::Cancelled);
            error.message
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_completing_parent_cancels_its_running_child() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker((leaving(), waiter()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env
        .start(&leaving(), json!({"fail": false}))
        .id("p")
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap(), "kid");
    env.settle().await.unwrap();
    // § fca4h4ker@3epej
    let status = env.run(&waiter(), "kid").status().await.unwrap();
    assert_eq!(status, RunStatus::Cancelled);
    // § end fca4h4ker
    // § fca4h4ker.b@c3e2p
    let reason = cancelled_with(&env).await;
    assert_eq!(reason, "the parent run p completed");
    // § end fca4h4ker.b
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
}

#[tokio::test]
async fn a_failing_parent_cancels_its_running_child() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker((leaving(), waiter()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env
        .start(&leaving(), json!({"fail": true}))
        .id("p")
        .await
        .unwrap();
    assert!(matches!(run.result().await, Err(Error::RunFailed { .. })));
    env.settle().await.unwrap();
    // § fca4h4ker.a@d7qd4
    let reason = cancelled_with(&env).await;
    assert_eq!(reason, "the parent run p failed");
    // § end fca4h4ker.a
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
}

#[tokio::test]
async fn an_abandoned_child_goes_on_and_ends_on_its_own() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker((abandoning(), waiter()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&abandoning(), ()).id("p").await.unwrap();
    assert_eq!(run.result().await.unwrap(), "kid");
    env.settle().await.unwrap();
    let child = env.run(&waiter(), "kid");
    // § fca4h4ker@n1910
    let status = child.status().await.unwrap();
    assert_eq!(
        status,
        RunStatus::Suspended,
        "its parent's end did not cancel it"
    );
    // § end fca4h4ker
    child.send("go", "now").await.unwrap();
    assert_eq!(child.result().await.unwrap(), "now");
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_claims_its_run_only_before_a_step() {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open("child-ends-claims").unwrap());
    let store = Recording::new(inner.clone());
    let sub = internal::create(store.clone()).await.unwrap();
    let stepping = Workflow::without_input("stepping", |c| async move {
        c.receive::<()>("go").await?;
        c.step("one", || async { Ok::<_, Error>(1) }).await
    });
    let worker = script(&sub, &stepping).start().unwrap();
    let run = sub.start(&stepping, ()).id("r").await.unwrap();
    internal::wake(&worker);
    common::eventually("the first replay", async || {
        run.status().await.unwrap() == RunStatus::Suspended
    })
    .await;
    // The head's writes, and its lease.
    let cased = || store.cased().iter().filter(|(k, _)| k == "runs/r").count();
    let lease = async || {
        let bytes = inner.get("runs/r").await.unwrap().unwrap().bytes;
        match layout::Run::decode(bytes).unwrap().kind {
            Some(layout::run::Kind::Head(head)) => head.lease,
            other => panic!("runs/r: {other:?}"),
        }
    };
    // § j1nq6rqpf@0t0x6
    assert_eq!(
        cased(),
        1,
        "a replay that runs no step: its suspension alone"
    );
    assert!(lease().await.is_none(), "a replay that runs no step");
    run.send("go", ()).await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 1);
    assert_eq!(cased(), 3, "the lease before the step, then the end");
    // § end j1nq6rqpf
    // § j1nq6rqpf.c@595nj
    assert!(lease().await.is_none(), "the end drops the lease");
    // § end j1nq6rqpf.c
    worker.shutdown().await.unwrap();
}
