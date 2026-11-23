//! Retention on the test environment: ended runs removed by their expiry, their ids taken while
//! they are kept, and the order of one client's messages. The ports of `python/tests/test_retention.py`, under the same
//! names.

use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use substantial::__internal::{self as internal, MemoryStore, Store};
use substantial::testing::Env;
use substantial::{Error, Preset, RunStatus, Workflow, days, hours, minutes};

const KEPT_DAYS: u32 = 90;
const LONG: Duration = Duration::from_secs(30);

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn env() -> Env {
    Env::open(t0()).await.unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

fn quick() -> Workflow<u32> {
    Workflow::without_input("quick", |_c| async { Ok(1) })
}

fn kept() -> Workflow<u32> {
    Workflow::without_input("kept", |_c| async { Ok(2) }).with_retention(KEPT_DAYS)
}

/// Continues as new at every iteration, `n` times, then ends with the count.
fn counter() -> Workflow<u64> {
    Workflow::new("counter", |c, input: serde_json::Value| async move {
        let left = input["left"].as_u64().unwrap();
        if left == 0 {
            return Ok(input["done"].as_u64().unwrap());
        }
        c.sleep(hours(1)).await?;
        let next = json!({ "left": left - 1, "done": input["done"].as_u64().unwrap() + 1 });
        Err(c.continue_as_new(next).await)
    })
}

/// Collects the names of its messages until it has `n`.
fn inbox() -> Workflow<Vec<String>> {
    Workflow::new("inbox", |c, input: serde_json::Value| async move {
        let n = input["n"].as_u64().unwrap() as usize;
        let names = substantial::Var::new(Vec::<String>::new());
        c.handle("m", names.handler(|names, id: String| names.push(id)))?;
        c.wait_until(names.check(move |names| names.len() >= n))
            .await?;
        Ok(names.get())
    })
}

/// Sends to `to` and returns the error the send raised.
fn sender() -> Workflow<String> {
    Workflow::new("sender", |c, input: serde_json::Value| async move {
        match c.send(input["to"].as_str().unwrap(), "m", 1).await {
            Err(Error::RunNotFound { chain }) => Ok(format!("RunNotFound {chain}")),
            other => panic!("{other:?}"),
        }
    })
}

fn not_found(e: Result<RunStatus, Error>) -> bool {
    match e {
        Err(Error::RunNotFound { .. }) => true,
        Ok(_) => false,
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_run_is_removed_30_days_after_its_end() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(30) - minutes(1)).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
    env.advance(minutes(2)).await.unwrap();
    assert!(not_found(run.status().await));
    assert!(matches!(run.result().await, Err(Error::RunNotFound { .. })));
    assert_eq!(run.read::<u32>("any").await.unwrap(), None);
    assert!(env.__keys("runs/").is_empty());
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_while_the_run_is_kept_raises_run_exists() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(30) - minutes(1)).await.unwrap();
    // § jbkv4v0we.e@44n63
    let again = env.start(quick(), json!({ "other": true })).id("q").await;
    assert!(
        matches!(&again, Err(Error::RunExists { id }) if id == "q"),
        "{again:?}"
    );
    assert_eq!(
        again.unwrap_err().to_string(),
        "id q is taken by another workflow, version or input"
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_id_starts_a_new_run_once_its_retention_passed() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(30) + minutes(1)).await.unwrap();
    assert!(not_found(run.status().await));
    let run = env
        .start(quick(), json!({ "other": true }))
        .id("q")
        .await
        .unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_workflow_retention_overrides_the_workers() {
    let env = env().await;
    let worker = env
        .worker((quick(), kept()))
        .preset(Preset::Script)
        .set("retention", "2d")
        .start()
        .unwrap();
    let q = env.start(quick(), ()).id("q").await.unwrap();
    let k = env.start(kept(), ()).id("k").await.unwrap();
    assert_eq!(q.result().await.unwrap(), 1);
    assert_eq!(k.result().await.unwrap(), 2);
    env.advance(days(3)).await.unwrap();
    assert!(not_found(q.status().await));
    assert_eq!(k.status().await.unwrap(), RunStatus::Completed);
    env.advance(days(88)).await.unwrap();
    assert!(not_found(k.status().await));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_long_lived_chain_keeps_a_bounded_number_of_runs() {
    let env = env().await;
    let worker = env
        .worker(counter())
        .preset(Preset::Script)
        .set("retention", "1d")
        .start()
        .unwrap();
    let run = env
        .start(counter(), json!({ "left": 72, "done": 0 }))
        .id("c")
        .await
        .unwrap();
    let mut most = 0;
    for _ in 0..72 {
        env.advance(hours(1)).await.unwrap();
        let kept = env.__keys("runs/").len() + env.__keys("archives/").len();
        most = most.max(kept);
    }
    assert_eq!(run.result().await.unwrap(), 72);
    // A day of hourly runs, each kept by its archive and its id, and the chain's head.
    assert!(most <= 2 * 25 + 1, "{most} objects kept at once");
    env.advance(days(1) + minutes(1)).await.unwrap();
    assert!(env.__keys("runs/").is_empty());
    assert!(env.__keys("archives/").is_empty());
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn messages_of_one_client_in_one_millisecond_are_consumed_in_send_order() {
    let env = env().await;
    let worker = env.worker(inbox()).preset(Preset::Script).start().unwrap();
    let run = env.start(inbox(), json!({ "n": 4 })).id("i").await.unwrap();
    env.settle().await.unwrap();
    for id in ["d", "b", "c", "a"] {
        run.send("m", id).id(id).await.unwrap();
    }
    // § a1qvxtaj6@cbxpk
    assert_eq!(run.result().await.unwrap(), ["d", "b", "c", "a"]);
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Sends `m` to `to` under the ids `e` to `a`, in that order, the first three at once.
fn ordered() -> Workflow<()> {
    Workflow::new("ordered", |c, input: serde_json::Value| async move {
        let to = input["to"].as_str().unwrap();
        let ids = ["e", "d", "c", "b", "a"];
        let together = ids[..3]
            .iter()
            .map(|id| c.send(to, "m", *id).id(id).into_future());
        futures::future::try_join_all(together).await?;
        for id in &ids[3..] {
            c.send(to, "m", *id).id(id).await?;
        }
        Ok(())
    })
}

#[tokio::test(flavor = "current_thread")]
async fn messages_of_one_run_in_one_millisecond_are_consumed_in_call_order() {
    let env = env().await;
    let sending = env
        .worker(ordered())
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(inbox(), json!({ "n": 5 })).id("i").await.unwrap();
    let sender = env.start(ordered(), json!({ "to": "i" })).await.unwrap();
    sender.result().await.unwrap();
    // Served once every message waits: its first replay consumes them in key order.
    let receiving = env.worker(inbox()).preset(Preset::Script).start().unwrap();
    // § one a1qvxtaj6.a@vymxz
    assert_eq!(run.result().await.unwrap(), ["e", "d", "c", "b", "a"]);
    sending.shutdown().await.unwrap();
    receiving.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_result_waiter_ends_with_run_not_found_when_its_run_is_removed() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    env.settle().await.unwrap();
    env.advance(days(31)).await.unwrap();
    assert!(matches!(run.result().await, Err(Error::RunNotFound { .. })));
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Starts a child kept 90 days and returns its value.
fn adopting() -> Workflow<u32> {
    Workflow::without_input("adopting", |c| async move {
        let child = c.start(&kept(), ()).id("child").await?;
        child.result().await
    })
}

#[tokio::test(flavor = "current_thread")]
async fn a_parent_is_removed_after_its_own_retention_whatever_its_child() {
    let env = env().await;
    let worker = env
        .worker((adopting(), kept()))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(adopting(), ()).id("p").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 2);
    let child = env.run(kept(), "child");
    // § a1ys7at7p@mpv1p
    // The parent's own retention is 30 days, its child's 90.
    env.advance(days(30) + minutes(1)).await.unwrap();
    assert!(not_found(run.status().await));
    assert_eq!(child.status().await.unwrap(), RunStatus::Completed);
    env.advance(days(60)).await.unwrap();
    assert!(not_found(child.status().await));
    // § end a1ys7at7p
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Waits for `go`.
fn lingering() -> Workflow<u32> {
    Workflow::without_input("lingering", |c| async move { c.receive::<u32>("go").await })
}

/// Starts `lingering` as `kid`, abandoned, then continues once and ends.
fn continuing() -> Workflow<u32> {
    Workflow::new("continuing", |c, input: serde_json::Value| async move {
        if input["again"].as_bool() == Some(true) {
            return Ok(1);
        }
        c.start(&lingering(), ()).id("kid").abandon().await?;
        Err(c.continue_as_new(json!({ "again": true })).await)
    })
}

#[tokio::test(flavor = "current_thread")]
async fn a_continued_run_expires_while_its_abandoned_child_lives() {
    let env = env().await;
    let worker = env
        .worker((continuing(), lingering()))
        .preset(Preset::Script)
        .set("retention", "1d")
        .start()
        .unwrap();
    let run = env.start(continuing(), json!({})).id("p").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    // § a1ys7at7p.c@cjc0h
    env.advance(days(1) + minutes(1)).await.unwrap();
    assert!(env.__keys("archives/").is_empty(), "the continued run");
    assert!(not_found(run.status().await));
    let kid = env.run(lingering(), "kid");
    assert_eq!(kid.status().await.unwrap(), RunStatus::Suspended);
    // § end a1ys7at7p.c
    // § n0ge6zsws.a@wtp4s
    // Its end tells a parent its retention removed: the notice is deleted unconsumed.
    kid.send("go", 2).await.unwrap();
    assert_eq!(kid.result().await.unwrap(), 2);
    env.settle().await.unwrap();
    assert!(env.__keys("queues/").is_empty(), "{:?}", env.__keys(""));
    assert!(not_found(run.status().await));
    // § end n0ge6zsws.a
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Starts `lingering` as `kid`, abandoned, and ends.
fn leaving() -> Workflow<u32> {
    Workflow::without_input("leaving", |c| async move {
        c.start(&lingering(), ()).id("kid").abandon().await?;
        Ok(0)
    })
}

#[tokio::test(flavor = "current_thread")]
async fn a_child_s_end_reaching_a_run_that_took_its_parent_s_id_is_ignored() {
    let env = env().await;
    let worker = env
        .worker((leaving(), lingering()))
        .preset(Preset::Script)
        .set("retention", "1d")
        .start()
        .unwrap();
    let parent = env.start(leaving(), ()).id("p").await.unwrap();
    assert_eq!(parent.result().await.unwrap(), 0);
    env.advance(days(1) + minutes(1)).await.unwrap();
    assert!(not_found(parent.status().await));
    // § n0ge6zsws@rq0gn
    let stranger = env.start(lingering(), ()).id("p").await.unwrap();
    env.settle().await.unwrap();
    let kid = env.run(lingering(), "kid");
    kid.send("go", 2).await.unwrap();
    assert_eq!(kid.result().await.unwrap(), 2);
    env.settle().await.unwrap();
    assert_eq!(stranger.status().await.unwrap(), RunStatus::Suspended);
    let history = stranger.history().await.unwrap();
    assert!(
        history
            .iter()
            .all(|e| e.data["name"] != "substantial.ended"),
        "{history:?}"
    );
    stranger.send("go", 3).await.unwrap();
    assert_eq!(stranger.result().await.unwrap(), 3);
    // § end n0ge6zsws
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Sleeps 2 days, past its parent's retention, then ends with 4.
fn late() -> Workflow<u32> {
    Workflow::without_input("late", |c| async move {
        c.sleep(days(2)).await?;
        Ok(4)
    })
}

/// Starts `late` as `k4`, abandoned, and ends at once, or, `awaiting`, also returns its result: the
/// same workflow deployed again with the same child start.
fn returning(awaiting: bool) -> Workflow<u32> {
    Workflow::without_input("returning", move |c| async move {
        let child = c.start(&late(), ()).id("k4").abandon().await?;
        if awaiting {
            return child.result().await;
        }
        Ok(0)
    })
    .with_retention(1)
}

#[tokio::test(flavor = "current_thread")]
async fn a_run_that_took_its_parent_s_id_adopting_its_ended_child_gets_its_end() {
    let env = env().await;
    let first = env
        .worker((returning(false), late()))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let parent = env.start(returning(false), ()).id("p4").await.unwrap();
    assert_eq!(parent.result().await.unwrap(), 0);
    env.advance(days(2) + minutes(1)).await.unwrap();
    assert!(not_found(parent.status().await), "the parent expired");
    assert_eq!(env.run(late(), "k4").result().await.unwrap(), 4);
    first.shutdown().await.unwrap();
    let second = env
        .worker((returning(true), late()))
        .preset(Preset::Script)
        .start()
        .unwrap();
    // § svx76xx41@7zmjj
    let again = env.start(returning(true), ()).id("p4").await.unwrap();
    assert_eq!(again.result().await.unwrap(), 4);
    // § end svx76xx41
    second.shutdown().await.unwrap();
    done(env).await;
}

/// A workflow no worker of these tests serves.
fn unserved() -> Workflow<u32> {
    Workflow::without_input("unserved", |_c| async { Ok(9) })
}

/// Starts `unserved` as `kid`, then waits for `go` and ends, which cancels its child.
fn owing() -> Workflow<u32> {
    Workflow::without_input("owing", |c| async move {
        c.start(&unserved(), ()).id("kid").await?;
        c.receive::<u32>("go").await
    })
}

/// A parent whose child never ran, served by no worker; then the child's head and due entry go,
/// as its retention's expiry removes a chain.
async fn child_gone(
    name: &str,
) -> (
    Arc<dyn Store>,
    substantial::Substantial,
    substantial::Worker,
) {
    let store: Arc<dyn Store> = Arc::new(MemoryStore::open(name).unwrap());
    let sub = internal::create(store.clone()).await.unwrap();
    let worker = (sub.worker((owing(), lingering())))
        .preset(Preset::Script)
        .start()
        .unwrap();
    sub.start(owing(), ()).id("p").await.unwrap();
    let run = sub.run(owing(), "p");
    while run.status().await.unwrap() != RunStatus::Suspended {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    store.delete("runs/kid").await.unwrap();
    for key in store
        .list("queues/unserved/", None, 100, None)
        .await
        .unwrap()
        .keys
    {
        store.delete(&key).await.unwrap();
    }
    (store, sub, worker)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_reaching_a_child_past_its_retention_is_dropped() {
    let (store, sub, worker) = child_gone("retention-cancel-gone").await;
    let run = sub.run(owing(), "p");
    // § n0ge6zsws.b@t4d0q
    run.send("go", 1).await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 1);
    worker.shutdown().await.unwrap();
    let keys = store.list("", None, 100, None).await.unwrap().keys;
    assert_eq!(keys, ["runs/p", "substantial.json"]);
    // § end n0ge6zsws.b
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_never_reaches_a_run_that_took_its_child_s_id() {
    let (_store, sub, worker) = child_gone("retention-cancel-retaken").await;
    let run = sub.run(owing(), "p");
    // § n0ge6zsws.b@xrkm5
    let stranger = sub.start(lingering(), ()).id("kid").await.unwrap();
    run.send("go", 1).await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 1);
    stranger.send("go", 3).await.unwrap();
    assert_eq!(stranger.result().timeout(LONG).await.unwrap(), 3);
    // § end n0ge6zsws.b
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_retention_change_reaches_only_the_runs_that_end_after_it() {
    let env = env().await;
    let short = env
        .worker(quick())
        .preset(Preset::Script)
        .set("retention", "2d")
        .start()
        .unwrap();
    let before = env.start(quick(), ()).id("before").await.unwrap();
    assert_eq!(before.result().await.unwrap(), 1);
    short.shutdown().await.unwrap();
    let long = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let after = env.start(quick(), ()).id("after").await.unwrap();
    assert_eq!(after.result().await.unwrap(), 1);
    // § a1ys7at7p.b@3fhyx
    env.advance(days(3)).await.unwrap();
    assert!(not_found(before.status().await));
    assert_eq!(after.status().await.unwrap(), RunStatus::Completed);
    // § end a1ys7at7p.b
    long.shutdown().await.unwrap();
    done(env).await;
}

fn failing() -> Workflow<u32> {
    Workflow::without_input("failing", |_c| async {
        Err::<u32, _>(Error::fail("Refused", "not today"))
    })
}

#[tokio::test(flavor = "current_thread")]
async fn a_retry_of_a_removed_chain_raises_run_not_found() {
    let env = env().await;
    let worker = env
        .worker(failing())
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(failing(), ()).id("f").await.unwrap();
    assert!(matches!(run.result().await, Err(Error::RunFailed { .. })));
    env.advance(days(31)).await.unwrap();
    // § vb6krfmej.d@8t58p
    let retried = run.retry().await;
    assert!(
        matches!(
            &retried,
            Err(Error::RunNotFound { chain }) if chain == "f"
        ),
        "{retried:?}"
    );
    // § end vb6krfmej.d
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Version 1 counts the runs of its step, then fails while `refuse` is set; version 2 ends.
fn retried(version: u32, steps: Arc<AtomicU32>, refuse: Arc<AtomicBool>) -> Workflow<u32> {
    Workflow::without_input("retried", move |c| {
        let (steps, refuse) = (steps.clone(), refuse.clone());
        async move {
            if version == 2 {
                return Ok(0);
            }
            let n = c
                .step("count", move || {
                    let n = steps.fetch_add(1, Ordering::SeqCst) + 1;
                    async move { Ok::<_, Error>(n) }
                })
                .await?;
            if refuse.load(Ordering::SeqCst) {
                return Err(Error::fail("Refused", "not today"));
            }
            Ok(n)
        }
    })
    .with_version(version)
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_run_is_kept_until_its_retry_holds_its_history() {
    let env = env().await;
    let (steps, refuse) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicBool::new(true)));
    let version = |v| retried(v, steps.clone(), refuse.clone()).with_retention(1);
    // The retry's entry, due while no worker takes it, keeps the environment from skipping
    // scans: the service preset and a long `max_idle` keep them few.
    let worker = |v| {
        (env.worker(version(v)).preset(Preset::Service))
            .set("max_idle", "1h")
            .start()
            .unwrap()
    };
    let first = worker(1);
    let run = env.start(version(1), ()).id("f").await.unwrap();
    assert!(matches!(run.result().await, Err(Error::RunFailed { .. })));
    first.shutdown().await.unwrap();
    refuse.store(false, Ordering::SeqCst);
    run.retry().await.unwrap();
    // Serves the name, but not the retry's version, which waits.
    let other = worker(2);
    // The failed run's retention passed: its archive expired, its history is in the retry's head.
    // § a1ys7at7p.d@t5as1
    env.advance(days(2)).await.unwrap();
    assert!(env.__keys("archives/").is_empty());
    let replaying = worker(1);
    assert_eq!(
        run.result().await.unwrap(),
        1,
        "the recorded step, from the retry's head"
    );
    assert_eq!(steps.load(Ordering::SeqCst), 1);
    other.shutdown().await.unwrap();
    // § end a1ys7at7p.d
    replaying.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_retention_under_a_day_is_a_config_error() {
    let env = env().await;
    for value in ["0d", "23h", "1440min"] {
        let refused = env
            .worker(quick())
            .preset(Preset::Script)
            .set("retention", value)
            .start();
        assert!(matches!(refused, Err(Error::ConfigError(_))), "{value}");
    }
    let short = Workflow::without_input("short", |_c| async { Ok(1) }).with_retention(0);
    let refused = env.worker(short).preset(Preset::Script).start();
    assert!(matches!(refused, Err(Error::ConfigError(_))));
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_retention_over_100_years_is_a_config_error() {
    let env = env().await;
    for value in ["36501d", "213503982334602d"] {
        let refused = env
            .worker(quick())
            .preset(Preset::Script)
            .set("retention", value)
            .start();
        assert!(matches!(refused, Err(Error::ConfigError(_))), "{value}");
    }
    for retention in [36_501, u32::MAX] {
        let long = Workflow::without_input("long", |_c| async { Ok(1) }).with_retention(retention);
        let refused = env.worker(long).preset(Preset::Script).start();
        assert!(
            matches!(&refused, Err(Error::ConfigError(m)) if m.contains("whole days from 1 to 36500, ")),
            "{retention}"
        );
    }
    let longest = Workflow::without_input("longest", |_c| async { Ok(1) }).with_retention(36_500);
    let worker = env
        .worker(longest)
        .preset(Preset::Script)
        .set("retention", "36500d")
        .start()
        .unwrap();
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_send_to_a_removed_chain_raises_run_not_found() {
    let env = env().await;
    let worker = env
        .worker((quick(), sender()))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(31)).await.unwrap();
    // § qkh68yrf1@84wmd
    let sent = run.send("m", 1).await;
    assert!(
        matches!(&sent, Err(Error::RunNotFound { chain }) if chain == "q"),
        "{sent:?}"
    );
    let cancelled = run.cancel("late").await;
    assert!(
        matches!(cancelled, Err(Error::RunNotFound { .. })),
        "{cancelled:?}"
    );
    let from_workflow = env.start(sender(), json!({ "to": "q" })).await.unwrap();
    assert_eq!(from_workflow.result().await.unwrap(), "RunNotFound q");
    // § end qkh68yrf1
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_result_wait_sees_its_chain_removed() {
    let store: Arc<dyn Store> = Arc::new(MemoryStore::open("retention-removed").unwrap());
    let sub = internal::create(store.clone()).await.unwrap();
    let run = sub.start(quick(), ()).id("q").await.unwrap();
    let wait = tokio::spawn(run.result().timeout(Duration::from_secs(10)).into_future());
    tokio::time::sleep(Duration::from_millis(20)).await;
    store.delete("runs/q").await.unwrap();
    // § hnjtggsx7.a@5r32h
    assert!(matches!(
        wait.await.unwrap(),
        Err(Error::RunNotFound { .. })
    ));
}
