//! `worker.drain(within)`: a worker woken on a schedule serves what is due by its horizon, then
//! stops. The ports of `python/tests/test_drain.py`, under the same names.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use substantial::testing::Env;
use substantial::{
    Context, Error, Observed, Preset, RunStatus, Substantial, Workflow, minutes, seconds,
};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

#[derive(Deserialize)]
struct Order {
    order: String,
    wait: u64,
}

fn order() -> Workflow<String> {
    Workflow::new("order", |c: Context, input: Order| async move {
        c.sleep(seconds(input.wait)).await?;
        let order = input.order;
        c.step("ship", move || {
            let order = order.clone();
            async move { Ok::<_, Error>(format!("shipped {order}")) }
        })
        .await
    })
}

fn input(order: &str, wait: u64) -> serde_json::Value {
    json!({"order": order, "wait": wait})
}

#[tokio::test]
async fn drain_serves_a_run_due_now_then_stops() {
    let env = Env::open(t0()).await.unwrap();
    let run = env.start(order(), input("o1", 0)).await.unwrap();
    let worker = env.worker(order()).preset(Preset::Script).start().unwrap();
    worker.drain(minutes(1)).await.unwrap();
    assert!(!worker.is_alive() && worker.error().is_none());
    assert_eq!(run.result().await.unwrap(), "shipped o1");
    assert_eq!(env.now(), t0());
    done(env).await;
}

#[tokio::test]
async fn drain_serves_a_run_due_within_its_horizon() {
    let env = Env::open(t0()).await.unwrap();
    let run = env.start(order(), input("o1", 30)).await.unwrap();
    let builder = env.worker(order()).preset(Preset::Script);
    builder.drain(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "shipped o1");
    assert_eq!(env.now(), t0() + TimeDelta::seconds(30));
    done(env).await;
}

#[tokio::test]
async fn drain_leaves_a_run_due_after_its_horizon_for_later() {
    let env = Env::open(t0()).await.unwrap();
    let run = env.start(order(), input("o1", 120)).await.unwrap();
    // § m2284agp0.c@e9ntz
    let script = || env.worker(order()).preset(Preset::Script);
    script().drain(minutes(1)).await.unwrap();
    assert_eq!(env.now(), t0());
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    env.advance(minutes(2)).await.unwrap();
    script().drain(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "shipped o1");
    done(env).await;
}

#[tokio::test]
async fn drain_waits_for_a_crashed_workers_claim_expiring_within_its_horizon() {
    let env = Env::open(t0()).await.unwrap();
    let held = env.hold("ship");
    let crashed = env.worker(order()).preset(Preset::Script).start().unwrap();
    let run = env.start(order(), input("o1", 0)).await.unwrap();
    env.settle().await.unwrap();
    assert_eq!(held.held(), 1);
    crashed.crash();
    // § m2284agp0.a@r33qt
    let script = env.worker(order()).preset(Preset::Script);
    script.drain(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "shipped o1");
    // Past claim_ttl plus claim_skew of the crashed worker's claim.
    assert_eq!(env.now(), t0() + TimeDelta::milliseconds(35_001));
    done(env).await;
}

#[tokio::test]
async fn drain_raises_the_workers_error() {
    let observe = |e: &Observed| {
        if matches!(e, Observed::Attempt(_)) {
            panic!("observer down");
        }
    };
    let failing = Env::open(t0()).observe(observe).await.unwrap();
    failing.start(order(), input("o1", 0)).await.unwrap();
    let worker = failing
        .worker(order())
        .preset(Preset::Script)
        .start()
        .unwrap();
    let drained = worker.drain(minutes(1)).await;
    let expected = "the worker panicked: observer down";
    assert!(
        matches!(&drained, Err(Error::Protocol(m)) if m == expected),
        "{drained:?}"
    );
    assert!(matches!(worker.error(), Some(Error::Protocol(m)) if m == expected));
    failing.settle().await.unwrap();
    failing.close().await;
}

/// A `Duration` is never negative: the least `within`, zero, serves what is due now.
#[tokio::test]
async fn drain_checks_within() {
    let env = Env::open(t0()).await.unwrap();
    let run = env.start(order(), input("o1", 0)).await.unwrap();
    let script = env.worker(order()).preset(Preset::Script);
    script.drain(Duration::ZERO).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "shipped o1");
    done(env).await;
}

#[tokio::test]
async fn drain_on_the_real_clock() {
    let ns = format!("memory://drain-{}", std::process::id());
    let sub = Substantial::create(ns).await.unwrap();
    let mut runs = Vec::new();
    for i in 0..3 {
        runs.push(
            sub.start(order(), input(&format!("o{i}"), 0))
                .await
                .unwrap(),
        );
    }
    let late = sub.start(order(), input("late", 3600)).await.unwrap();
    let worker = sub.worker(order()).preset(Preset::Script).start().unwrap();
    worker.drain(seconds(1)).await.unwrap();
    for (i, run) in runs.iter().enumerate() {
        assert_eq!(run.result().await.unwrap(), format!("shipped o{i}"));
    }
    assert_eq!(late.status().await.unwrap(), RunStatus::Suspended);
    assert!(!worker.is_alive() && worker.error().is_none());
}
