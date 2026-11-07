//! Retention on the test environment: ended runs collected, their ids taken for 7 days, and the
//! order of one client's messages. The ports of `python/tests/test_retention.py`, under the same
//! names.

use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use substantial::__internal::{self as internal, MemoryStore, Store};
use substantial::testing::Env;
use substantial::{Error, FOREVER, Preset, RunStatus, Workflow, days, hours, minutes};

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
    Workflow::without_input("kept", |_c| async { Ok(2) }).with_retention(days(90))
}

fn forever() -> Workflow<u32> {
    Workflow::without_input("forever", |_c| async { Ok(3) }).with_retention(FOREVER)
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
            Err(Error::RunNotFound {
                chain,
                collected,
                until,
            }) => Ok(format!("RunNotFound {chain} {collected} {until:?}")),
            other => panic!("{other:?}"),
        }
    })
}

fn not_found(e: Result<RunStatus, Error>) -> (bool, Option<DateTime<Utc>>) {
    match e {
        Err(Error::RunNotFound {
            collected, until, ..
        }) => (collected, until),
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_run_is_collected_30_days_after_its_end() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(30) - minutes(1)).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
    env.advance(minutes(2)).await.unwrap();
    let (collected, until) = not_found(run.status().await);
    assert!(collected);
    assert!(until.unwrap() > env.now() + days(7) - minutes(2));
    assert!(until.unwrap() <= env.now() + days(7));
    assert!(matches!(
        run.result().await,
        Err(Error::RunNotFound {
            collected: true,
            ..
        })
    ));
    assert_eq!(run.read::<u32>("any").await.unwrap(), None);
    assert_eq!(env.__keys("runs/q/"), ["runs/q/gone"]);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_during_the_tombstone_raises_run_exists() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(31)).await.unwrap();
    let again = env.start(quick(), ()).id("q").await;
    let Err(Error::RunExists {
        id,
        collected: true,
        until: Some(until),
    }) = again
    else {
        panic!("{again:?}");
    };
    assert_eq!(id, "q");
    assert!(until > env.now() + days(5));
    let message = env
        .start(quick(), ())
        .id("q")
        .await
        .unwrap_err()
        .to_string();
    assert!(message.contains("collected"), "{message}");
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_id_starts_a_new_run_after_the_tombstone() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(30) + days(7) + minutes(2)).await.unwrap();
    assert!(matches!(
        run.status().await,
        Err(Error::RunNotFound {
            collected: false,
            until: None,
            ..
        })
    ));
    assert!(env.__keys("runs/q/").is_empty());
    let run = env.start(quick(), ()).id("q").await.unwrap();
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
    assert!(not_found(q.status().await).0);
    assert_eq!(k.status().await.unwrap(), RunStatus::Completed);
    env.advance(days(88)).await.unwrap();
    assert!(not_found(k.status().await).0);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn forever_keeps_a_run() {
    let env = env().await;
    let worker = env
        .worker((quick(), forever()))
        .preset(Preset::Script)
        .set("retention", FOREVER)
        .start()
        .unwrap();
    let q = env.start(quick(), ()).id("q").await.unwrap();
    let f = env.start(forever(), ()).id("f").await.unwrap();
    assert_eq!(q.result().await.unwrap(), 1);
    assert_eq!(f.result().await.unwrap(), 3);
    env.advance(days(400)).await.unwrap();
    assert_eq!(q.status().await.unwrap(), RunStatus::Completed);
    assert_eq!(f.result().await.unwrap(), 3);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_long_lived_chain_keeps_a_bounded_number_of_runs() {
    let env = env().await;
    let worker = env
        .worker(counter())
        .preset(Preset::Script)
        .set("retention", "1h")
        .start()
        .unwrap();
    let run = env
        .start(counter(), json!({ "left": 48, "done": 0 }))
        .id("c")
        .await
        .unwrap();
    let mut most = 0;
    for _ in 0..48 {
        env.advance(hours(1)).await.unwrap();
        let runs: std::collections::BTreeSet<String> = env
            .__keys("runs/")
            .iter()
            .filter_map(|k| k.split('/').nth(1).map(str::to_owned))
            .collect();
        most = most.max(runs.len());
    }
    assert_eq!(run.result().await.unwrap(), 48);
    assert!(most <= 4, "{most} runs kept at once");
    env.advance(hours(3)).await.unwrap();
    assert_eq!(env.__keys("runs/"), ["runs/c/gone"]);
    assert!(env.__keys("index/").is_empty());
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
    assert_eq!(run.result().await.unwrap(), ["d", "b", "c", "a"]);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_result_waiter_ends_with_run_not_found_when_its_run_is_collected() {
    let env = env().await;
    let worker = env.worker(quick()).preset(Preset::Script).start().unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    env.settle().await.unwrap();
    env.advance(days(31)).await.unwrap();
    assert!(matches!(
        run.result().await,
        Err(Error::RunNotFound {
            collected: true,
            ..
        })
    ));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_retention_under_an_hour_is_a_config_error() {
    let env = env().await;
    let refused = env
        .worker(quick())
        .preset(Preset::Script)
        .set("retention", "59min")
        .start();
    assert!(matches!(refused, Err(Error::ConfigError(_))));
    let short = Workflow::without_input("short", |_c| async { Ok(1) }).with_retention(minutes(59));
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
    for retention in [days(36_501), Duration::from_millis(u64::MAX), Duration::MAX] {
        let long = Workflow::without_input("long", |_c| async { Ok(1) }).with_retention(retention);
        let refused = env.worker(long).preset(Preset::Script).start();
        assert!(
            matches!(&refused, Err(Error::ConfigError(m)) if m.contains("at most 3153600000000 ms or forever")),
            "{retention:?}"
        );
    }
    let longest =
        Workflow::without_input("longest", |_c| async { Ok(1) }).with_retention(days(36_500));
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
async fn a_send_to_a_collected_chain_raises_run_not_found() {
    let env = env().await;
    let worker = env
        .worker((quick(), sender()))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    env.advance(days(31)).await.unwrap();
    let until = match run.send("m", 1).await {
        Err(Error::RunNotFound {
            collected: true,
            until: Some(until),
            ..
        }) => until,
        other => panic!("{other:?}"),
    };
    let cancelled = run.cancel("late").await;
    assert!(
        matches!(
            cancelled,
            Err(Error::RunNotFound {
                collected: true,
                ..
            })
        ),
        "{cancelled:?}"
    );
    let from_workflow = env.start(sender(), json!({ "to": "q" })).await.unwrap();
    assert_eq!(
        from_workflow.result().await.unwrap(),
        format!("RunNotFound q true {:?}", Some(until))
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_result_wait_follows_its_chain_again_and_sees_it_collected() {
    let store: Arc<dyn Store> = Arc::new(MemoryStore::open("retention-refollow").unwrap());
    let sub = internal::create(store.clone()).partitions(1).await.unwrap();
    let run = sub.start(quick(), ()).id("q").await.unwrap();
    let wait = internal::refollow(run.result(), Duration::from_millis(50));
    let wait = tokio::spawn(wait.timeout(Duration::from_secs(10)).into_future());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let listing = store.list("runs/q/", None, 1000, None).await.unwrap();
    for key in &listing.keys {
        store.delete(key).await.unwrap();
    }
    assert!(matches!(
        wait.await.unwrap(),
        Err(Error::RunNotFound {
            collected: false,
            ..
        })
    ));
}
