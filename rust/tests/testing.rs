//! The test environment of docs/testing.md: the environment, its ids and its time. The ports of
//! `python/tests/test_testing.py`, under the same names.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use substantial::__internal::MemoryStore;
use substantial::testing::{Chaos, Env};
use substantial::{Context, Error, Preset, RunStatus, Workflow, days, minutes, seconds};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

fn iso(t: DateTime<Utc>) -> String {
    t.to_rfc3339()
}

fn wall() -> DateTime<Utc> {
    std::time::SystemTime::now().into()
}

/// The environment of a test, checked and closed once the test passed.
async fn env() -> Env {
    Env::open(t0()).await.unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

type Events = Arc<Mutex<Vec<i64>>>;

/// Binds the messages `event` to a list.
fn events(c: &Context) -> Events {
    let events = Events::default();
    let pushed = events.clone();
    c.handle("event", move |v: i64| pushed.lock().unwrap().push(v))
        .unwrap();
    events
}

fn len(events: &Events) -> usize {
    events.lock().unwrap().len()
}

#[derive(Deserialize)]
struct Key {
    #[allow(dead_code)]
    key: String,
}

fn debounce() -> Workflow<Vec<i64>> {
    Workflow::new("debounce", |c: Context, _: Key| async move {
        let events = events(&c);
        let mut seen = 0;
        loop {
            let e = events.clone();
            let more = c.wait_until(move || len(&e) > seen).timeout(minutes(5));
            if !more.await? {
                break;
            }
            seen = len(&events);
        }
        Ok(events.lock().unwrap().clone())
    })
}

/// Publishes how many events it has seen, until two.
fn seen() -> Workflow<usize> {
    Workflow::without_input("seen", |c| async move {
        let events = events(&c);
        while len(&events) < 2 {
            let (n, e) = (len(&events), events.clone());
            c.publish("seen", n)?;
            c.wait_until(move || len(&e) > n).await?;
        }
        Ok(len(&events))
    })
}

fn clocks() -> Workflow<(String, String)> {
    Workflow::without_input("clocks", |c| async move {
        let wall = c.step("wall", || async { Ok::<_, Error>(iso(wall())) });
        Ok((iso(c.now().await?), wall.await?))
    })
}

#[derive(Deserialize)]
struct Days {
    days: u64,
}

fn napping() -> Workflow<String> {
    Workflow::new("napping", |c: Context, input: Days| async move {
        c.sleep(days(input.days)).await?;
        Ok(iso(c.now().await?))
    })
}

fn parent() -> Workflow<String> {
    Workflow::without_input("parent", |c| async move {
        let child = c.start(&napping(), json!({ "days": 0 })).id("the-child");
        child.await?.result().await
    })
}

#[tokio::test]
async fn the_debounce_of_the_docs() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(debounce())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env
        .start(debounce(), json!({ "key": "k" }))
        .id("k")
        .await
        .unwrap();
    run.send("event", 1).await.unwrap();
    env.advance(minutes(4)).await.unwrap();
    run.send("event", 2).await.unwrap();
    env.advance(minutes(4)).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), [1, 2]);
    env.check().await.unwrap();
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn an_environment_opens_at_now_and_drops_its_namespace_when_left() {
    let env = Env::open(t0()).await.unwrap();
    assert_eq!(env.now(), t0());
    let ns = env.__namespace().to_owned();
    assert!(MemoryStore::exists(&ns));
    env.close().await;
    assert!(!MemoryStore::exists(&ns));
    // A naive time does not type check; a time before 1970 has no ULID.
    let before = Utc.with_ymd_and_hms(1969, 12, 31, 0, 0, 0).unwrap();
    assert!(matches!(
        Env::open(before).await,
        Err(Error::InvalidArgument(_))
    ));
    let chaos = Chaos { seed: 0, rate: 2.0 };
    let refused = Env::open(t0()).chaos(chaos).await;
    assert!(matches!(refused, Err(Error::InvalidArgument(_))));
}

#[tokio::test]
async fn an_environments_clock_counts_whole_milliseconds() {
    let whole = t0() + chrono::Duration::milliseconds(123);
    let env = Env::open(whole).await.unwrap();
    assert_eq!(env.now(), whole);
    env.close().await;
    let finer = t0() + chrono::Duration::microseconds(123_456);
    let refused = Env::open(finer).await;
    let Err(Error::InvalidArgument(message)) = refused else {
        panic!("{:?}", refused.map(|_| ()))
    };
    assert!(
        message.contains("whole milliseconds, not 2030-01-07T09:00:00.123456"),
        "{message}"
    );
}

#[tokio::test]
async fn nothing_runs_between_two_calls_of_the_test() {
    let env = env().await;
    let worker = env
        .worker(napping())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(napping(), json!({ "days": 1 })).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Pending);
    env.settle().await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    assert_eq!(env.now(), t0());
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_run_that_only_waits_reads_suspended_once_a_worker_took_it() {
    let env = env().await;
    let idle = Workflow::without_input("idle", |c| async move { c.receive::<i64>("go").await });
    let worker = env.worker(&idle).preset(Preset::Service).start().unwrap();
    let run = env.start(&idle, ()).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Pending);
    env.settle().await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    run.send("go", 1).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn two_sends_in_a_row_reach_the_run_together() {
    let env = env().await;
    let worker = env.worker(seen()).preset(Preset::Service).start().unwrap();
    let together = env.start(seen(), ()).id("together").await.unwrap();
    together.send("event", 1).await.unwrap();
    together.send("event", 2).await.unwrap();
    let apart = env.start(seen(), ()).id("apart").await.unwrap();
    apart.send("event", 1).await.unwrap();
    env.settle().await.unwrap();
    assert_eq!(apart.read::<usize>("seen").await.unwrap(), Some(1));
    apart.send("event", 2).await.unwrap();
    assert_eq!(together.result().await.unwrap(), 2);
    assert_eq!(apart.result().await.unwrap(), 2);
    // One replay took both messages, in one segment; the other run woke for each.
    assert_eq!(env.__keys("runs/together/log/").len(), 1);
    assert_eq!(env.__keys("runs/apart/log/").len(), 2);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_client_shares_the_namespace_and_the_clock() {
    let env = env().await;
    let client = env.client().await.unwrap();
    let worker = env
        .worker(napping())
        .preset(Preset::Service)
        .start()
        .unwrap();
    env.advance(seconds(10)).await.unwrap();
    let days = json!({ "days": 0 });
    let run = client
        .start(napping(), days)
        .id("from-a-client")
        .await
        .unwrap();
    // No worker of the client: the environment's finds the run at its next scan, at 30 s.
    env.advance(seconds(19)).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Pending);
    env.advance(seconds(1)).await.unwrap();
    let ended = env.run(napping(), "from-a-client").result().await.unwrap();
    assert_eq!(ended, iso(t0() + seconds(30)));
    assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
    worker.shutdown().await.unwrap();
    let other = env.client().await.unwrap();
    let status = other
        .run(napping(), "from-a-client")
        .status()
        .await
        .unwrap();
    assert_eq!(status, RunStatus::Completed);
    done(env).await;
}

#[tokio::test]
async fn a_step_reads_the_wall_clock_and_the_run_the_virtual_one() {
    let env = env().await;
    let worker = env.worker(clocks()).preset(Preset::Script).start().unwrap();
    let (virtual_, wall) = env
        .start(clocks(), ())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(virtual_, iso(t0()));
    let wall: DateTime<Utc> = wall.parse().unwrap();
    assert!((wall - self::wall()).abs() < chrono::Duration::minutes(1));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_timer_fires_at_its_due_instant() {
    let env = env().await;
    let worker = env
        .worker(napping())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(napping(), json!({ "days": 30 })).await.unwrap();
    env.advance(days(30) - seconds(1)).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    env.advance(seconds(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), iso(t0() + days(30)));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_child_is_found_at_once_by_the_worker_that_started_it() {
    let env = env().await;
    let worker = env
        .worker((parent(), napping()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(parent(), ()).await.unwrap();
    assert_eq!(run.result().await.unwrap(), iso(t0()));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn result_settles_and_never_moves_the_clock() {
    let env = env().await;
    let worker = env
        .worker(napping())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(napping(), json!({ "days": 2 })).await.unwrap();
    match run.result().await {
        Err(Error::Stuck(m)) => {
            assert!(m.contains("timer due at 2030-01-09T09:00:00+00:00"), "{m}")
        }
        other => panic!("{other:?}"),
    }
    let late = run.result().timeout(days(3)).await;
    assert!(matches!(late, Err(Error::WaitTimeout)), "{late:?}");
    assert_eq!(env.now(), t0());
    env.advance(days(2)).await.unwrap();
    let ended = run.result().timeout(seconds(1)).await.unwrap();
    assert_eq!(ended, iso(t0() + days(2)));
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn stuck<T: std::fmt::Debug>(result: Result<T, Error>, what: &str) {
    match result {
        Err(Error::Stuck(m)) => assert!(m.contains(what), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn stuck_names_a_message_and_a_workflow_no_worker_serves() {
    let env = env().await;
    let worker = env
        .worker((seen(), parent()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let waiting = env.start(seen(), ()).id("waiting").await.unwrap();
    stuck(waiting.result().await, "a message");
    let orphan = env.start(napping(), json!({ "days": 0 })).id("orphan");
    stuck(
        orphan.await.unwrap().result().await,
        "napping, which no worker serves",
    );
    let parent = env.start(parent(), ()).id("a-parent").await.unwrap();
    stuck(parent.result().await, "the-child");
    worker.shutdown().await.unwrap();
    done(env).await;
}
