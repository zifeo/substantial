//! `c.result(chain).timeout(d)`: a workflow awaits the end of any chain, a child of its own or
//! not. The ports of `python/tests/test_chain_result.py`, under the same names.

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use substantial::testing::Env;
use substantial::{Context, Error, Preset, RunStatus, Workflow, days, minutes};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

fn quick() -> Workflow<u32> {
    Workflow::without_input("quick", |_| async move { Ok(1) })
}

fn waiter() -> Workflow<String> {
    Workflow::without_input("waiter", |c| async move { c.receive("go").await })
}

fn doomed() -> Workflow<()> {
    Workflow::without_input("doomed", |c| async move {
        let message: String = c.receive("go").await?;
        Err(Error::fail("ValueError", message))
    })
}

#[derive(Deserialize)]
struct Left {
    left: u32,
}

/// Continues as new at each `go` while `left`, then ends with the last one's value.
fn relay() -> Workflow<String> {
    Workflow::new("relay", |c: Context, input: Left| async move {
        let got: String = c.receive("go").await?;
        if input.left > 0 {
            return Err(c.continue_as_new(json!({"left": input.left - 1})).await);
        }
        Ok(got)
    })
}

#[derive(Deserialize)]
struct Watched {
    chain: String,
}

fn watcher() -> Workflow<Value> {
    Workflow::new("watcher", |c: Context, input: Watched| async move {
        match c.result::<Value>(&input.chain).await {
            Ok(value) => Ok(json!({"value": value})),
            Err(Error::RunFailed { status, error, .. }) => {
                Ok(json!({"status": status.to_string(), "error": error.message}))
            }
            Err(e @ Error::RunNotFound { .. }) => Ok(json!({"not_found": e.to_string()})),
            Err(e) => Err(e),
        }
    })
}

fn service(env: &Env) -> substantial::Worker {
    (env.worker((quick(), waiter(), doomed(), relay(), watcher())))
        .preset(Preset::Service)
        .start()
        .unwrap()
}

#[tokio::test]
async fn a_chain_ended_before_the_watch_answers_at_once() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(quick(), ()).await.unwrap();
    assert_eq!(target.result().await.unwrap(), 1);
    let run = env
        .start(watcher(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    // § e93tpdr4p.a@rz68j
    assert_eq!(run.result().await.unwrap(), json!({"value": 1}));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_chain_that_ends_while_watched_wakes_the_watcher() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(waiter(), ()).await.unwrap();
    let run = env
        .start(watcher(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    env.settle().await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    target.send("go", "done").await.unwrap();
    assert_eq!(run.result().await.unwrap(), json!({"value": "done"}));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_failed_chain_raises_run_failed() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(doomed(), ()).await.unwrap();
    let run = env
        .start(watcher(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    env.settle().await.unwrap();
    target.send("go", "boom").await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        json!({"status": "failed", "error": "boom"})
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_cancelled_chain_raises_run_failed_cancelled() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(waiter(), ()).await.unwrap();
    let run = env
        .start(watcher(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    env.settle().await.unwrap();
    target.cancel("enough").await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        json!({"status": "cancelled", "error": "enough"})
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_chain_that_continued_is_awaited_to_its_last_run() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(relay(), json!({"left": 1})).await.unwrap();
    let run = env
        .start(watcher(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    env.settle().await.unwrap();
    target.send("go", "first").await.unwrap();
    env.settle().await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    target.send("go", "second").await.unwrap();
    // § gzga227zz.b@2nfq4
    assert_eq!(run.result().await.unwrap(), json!({"value": "second"}));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_removed_chain_raises_run_not_found() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(quick(), ()).id("q").await.unwrap();
    assert_eq!(target.result().await.unwrap(), 1);
    env.advance(days(31)).await.unwrap();
    let run = env.start(watcher(), json!({"chain": "q"})).await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        json!({"not_found": "no chain q"})
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_missing_chain_raises_run_not_found() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let run = env
        .start(watcher(), json!({"chain": "nobody"}))
        .await
        .unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        json!({"not_found": "no chain nobody"})
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// Rust's `WaitTimeout` carries no message, as for its other timeouts.
fn patient() -> Workflow<Value> {
    Workflow::new("patient", |c: Context, input: Watched| async move {
        // § gzga227zz.a@p812n
        match c.result::<String>(&input.chain).timeout(minutes(5)).await {
            Err(e @ Error::WaitTimeout) => {
                let at = c.now().await?.to_rfc3339();
                let value: String = c.result(&input.chain).await?;
                Ok(json!([e.to_string(), at, value]))
            }
            other => Err(Error::fail("Unexpected", format!("{other:?}"))),
        }
    })
}

#[tokio::test]
async fn the_timeout_raises_wait_timeout_then_awaiting_again_gets_the_value() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker((waiter(), patient())))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let target = env.start(waiter(), ()).await.unwrap();
    let run = env
        .start(patient(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    env.advance(minutes(6)).await.unwrap();
    target.send("go", "late").await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        json!([
            "the timeout passed first",
            "2030-01-07T09:05:00+00:00",
            "late"
        ])
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn two_watchers_of_one_chain_both_get_its_end() {
    let env = Env::open(t0()).await.unwrap();
    let worker = service(&env);
    let target = env.start(waiter(), ()).await.unwrap();
    let mut runs = Vec::new();
    for _ in 0..2 {
        let input = json!({"chain": target.chain()});
        runs.push(env.start(watcher(), input).await.unwrap());
    }
    env.settle().await.unwrap();
    target.send("go", "both").await.unwrap();
    // § e93tpdr4p@pf53r
    // § one gzga227zz.d@60h60
    for run in runs {
        assert_eq!(run.result().await.unwrap(), json!({"value": "both"}));
    }
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn narcissist() -> Workflow<Vec<String>> {
    Workflow::without_input("narcissist", |c| async move {
        let mut refused = Vec::new();
        for chain in [c.chain().to_owned(), "Not An Id".to_owned()] {
            match c.result::<Value>(&chain).await {
                Err(Error::InvalidArgument(message)) => refused.push(message),
                other => return Err(Error::fail("Unexpected", format!("{other:?}"))),
            }
        }
        Ok(refused)
    })
}

#[tokio::test]
async fn waiting_on_its_own_chain_raises_invalid_argument() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker(narcissist()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(narcissist(), ()).id("me").await.unwrap();
    let refused = run.result().await.unwrap();
    // § gzga227zz.c@m2yq3
    assert!(
        refused[0].contains("cannot await its own chain me"),
        "{refused:?}"
    );
    // § end gzga227zz.c
    assert!(
        refused[1].starts_with(r#"run id "Not An Id" must match"#),
        "{refused:?}"
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn resumed() -> Workflow<String> {
    Workflow::new("resumed", |c: Context, input: Watched| async move {
        let value: String = c.result(&input.chain).await?;
        c.receive::<()>("resume").await?;
        Ok(value)
    })
}

#[tokio::test]
async fn the_outcome_replays() {
    let env = Env::open(t0()).await.unwrap();
    let start = || {
        (env.worker((waiter(), resumed())))
            .preset(Preset::Service)
            .start()
            .unwrap()
    };
    let worker = start();
    let target = env.start(waiter(), ()).await.unwrap();
    let run = env
        .start(resumed(), json!({"chain": target.chain()}))
        .await
        .unwrap();
    env.settle().await.unwrap();
    target.send("go", "kept").await.unwrap();
    env.settle().await.unwrap();
    // The watched chain is removed: a result read again would raise RunNotFound.
    env.advance(days(31)).await.unwrap();
    let status = target.status().await;
    assert!(
        matches!(status, Err(Error::RunNotFound { .. })),
        "{status:?}"
    );
    worker.shutdown().await.unwrap();
    let worker = start();
    run.send("resume", ()).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "kept");
    let history = run.history().await.unwrap();
    let watched: Vec<(&Value, &Value)> = (history.iter())
        .filter(|e| e.kind == "watched")
        .map(|e| (&e.data["chain"], &e.data["waiting"]))
        .collect();
    let chain = json!(target.chain());
    assert_eq!(watched, [(&chain, &chain)]);
    let ended: Vec<(&Value, &Value)> = (history.iter())
        .filter(|e| e.data["name"] == "substantial.watched")
        .map(|e| (&e.data["ended"]["status"], &e.data["ended"]["value"]))
        .collect();
    assert_eq!(ended, [(&json!("completed"), &json!("kept"))]);
    worker.shutdown().await.unwrap();
    done(env).await;
}
