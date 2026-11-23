//! An error of the code that is no end blocks its run: nothing is recorded, the run reads blocked
//! with its error, and it goes on once code that runs it is deployed (I-20). The ports of
//! `python/tests/test_blocking.py`, under the same names.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::json;
use substantial::observe::Blocked;
use substantial::testing::Env;
use substantial::{
    Context, Error, Observed, Preset, RunInfo, RunStatus, WaitingOn, Workflow, seconds,
};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

#[derive(Deserialize)]
struct N {
    n: i64,
}

fn doubling(fixed: bool, doubled: &Arc<Mutex<Vec<i64>>>) -> Workflow<i64> {
    let doubled = doubled.clone();
    Workflow::new("doubling", move |c: Context, N { n }: N| {
        let doubled = doubled.clone();
        async move {
            let x = c
                .step("double", move || {
                    doubled.lock().unwrap().push(n);
                    async move { Ok::<_, Error>(n * 2) }
                })
                .await?;
            if !fixed {
                return Err(anyhow::anyhow!("bug at {x}").into());
            }
            Ok(x)
        }
    })
}

async fn listed<O>(env: &Env, workflow: &Workflow<O>) -> Vec<RunInfo> {
    env.runs(workflow).try_collect().await.unwrap()
}

#[tokio::test]
async fn an_error_of_the_code_blocks_the_run_until_a_fix() {
    let doubled = Arc::new(Mutex::new(Vec::new()));
    let told = Arc::new(Mutex::new(Vec::new()));
    let seen = told.clone();
    let env = Env::open(t0())
        .observe(move |e| {
            if let Observed::Blocked(b) = e {
                seen.lock().unwrap().push(b.clone());
            }
        })
        .await
        .unwrap();
    let broken = doubling(false, &doubled);
    let worker = env.worker(&broken).preset(Preset::Script).start().unwrap();
    let run = env.start(&broken, json!({"n": 2})).id("d").await.unwrap();
    env.settle().await.unwrap();
    // § hzew2nw4v@6praa
    assert_eq!(run.status().await.unwrap(), RunStatus::Blocked);
    let [info] = <[RunInfo; 1]>::try_from(listed(&env, &broken).await).unwrap();
    assert_eq!(info.waiting_on, WaitingOn::Blocked);
    let error = info.error.unwrap();
    assert_eq!(
        (error.kind.as_str(), error.message.as_str()),
        ("Error", "bug at 4")
    );
    assert_eq!(info.due_at, Some(env.now() + seconds(1)));
    // § end hzew2nw4v
    let history = run.history().await.unwrap();
    assert!(
        history.is_empty(),
        "nothing recorded, the step since the last commit"
    );
    let blocked = told.lock().unwrap().clone();
    assert_eq!(
        blocked,
        [Blocked {
            workflow: "doubling".into(),
            run: "d".into(),
            error,
        }]
    );
    env.advance(seconds(1)).await.unwrap();
    let [info] = <[RunInfo; 1]>::try_from(listed(&env, &broken).await).unwrap();
    assert_eq!(
        info.due_at,
        Some(env.now() + seconds(2)),
        "the backoff doubles"
    );
    worker.shutdown().await.unwrap();
    assert_eq!(env.__keys("queues/doubling/blocked/").len(), 1);
    let fixed = doubling(true, &doubled);
    let worker = env.worker(&fixed).preset(Preset::Script).start().unwrap();
    env.advance(seconds(2)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 4);
    // § rbvr70kyv.d@ytenk
    assert_eq!(env.__keys("runs/d/blocked/"), Vec::<String>::new());
    assert_eq!(env.__keys("queues/doubling/blocked/"), Vec::<String>::new());
    // § end rbvr70kyv.d
    worker.shutdown().await.unwrap();
    let doubled = doubled.lock().unwrap().clone();
    assert_eq!(
        doubled,
        [2, 2, 2],
        "a step the replay did not commit runs again"
    );
    let kinds: Vec<String> = (run.history().await.unwrap().into_iter())
        .map(|e| e.kind)
        .collect();
    assert_eq!(kinds, ["step", "completed"]);
    assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
    env.close().await;
}

/// Steps then waits for `go`, or, changed, waits for `go` then steps: the same name and no
/// version.
fn waiting(first: bool) -> Workflow<i64> {
    Workflow::without_input("waiting", move |c| async move {
        let one = || async { Ok::<_, Error>(1) };
        if first {
            c.step("one", one).await?;
            c.receive::<()>("go").await?;
        } else {
            c.receive::<()>("go").await?;
            c.step("one", one).await?;
        }
        Ok(1)
    })
}

#[tokio::test]
async fn a_divergence_blocks_the_run_until_the_code_is_rolled_back() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(waiting(true))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(waiting(true), ()).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    run.send("go", ()).await.unwrap();
    let worker = env
        .worker(waiting(false))
        .preset(Preset::Script)
        .start()
        .unwrap();
    env.settle().await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Blocked);
    let [info] = <[RunInfo; 1]>::try_from(listed(&env, &waiting(false)).await).unwrap();
    let error = info.error.unwrap();
    assert_eq!(error.kind, "NonDeterminism", "{error:?}");
    worker.shutdown().await.unwrap();
    let worker = env
        .worker(waiting(true))
        .preset(Preset::Script)
        .start()
        .unwrap();
    env.advance(seconds(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    worker.shutdown().await.unwrap();
    env.close().await;
}

/// `waiting`, whose code catches what its body raises and fails on purpose, as code that wraps a
/// whole workflow in a handler does.
fn catching(first: bool) -> Workflow<i64> {
    Workflow::without_input("waiting", move |c| async move {
        let one = || async { Ok::<_, Error>(1) };
        let body = async {
            if first {
                c.step("one", one).await?;
                c.receive::<()>("go").await?;
            } else {
                c.receive::<()>("go").await?;
                c.step("one", one).await?;
            }
            Ok::<_, Error>(1)
        };
        match body.await {
            Err(e @ Error::NonDeterminism(_)) => Err(Error::fail("Caught", e.to_string())),
            other => other,
        }
    })
}

#[tokio::test]
async fn a_divergence_the_code_catches_and_fails_on_still_blocks_the_run() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(catching(true))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(catching(true), ()).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    run.send("go", ()).await.unwrap();
    let worker = env
        .worker(catching(false))
        .preset(Preset::Script)
        .start()
        .unwrap();
    env.settle().await.unwrap();
    // § rbwstdetg.e@nr5qc
    assert_eq!(run.status().await.unwrap(), RunStatus::Blocked);
    let [info] = <[RunInfo; 1]>::try_from(listed(&env, &catching(false)).await).unwrap();
    assert_eq!(info.error.unwrap().kind, "NonDeterminism");
    // § end rbwstdetg.e
    worker.shutdown().await.unwrap();
    let worker = env
        .worker(catching(true))
        .preset(Preset::Script)
        .start()
        .unwrap();
    env.advance(seconds(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn fail_ends_the_run_failed() {
    let env = Env::open(t0()).await.unwrap();
    let refusing = Workflow::without_input("refusing", |_c| async move {
        Err::<(), _>(Error::fail("Refused", "not today"))
    });
    let worker = env
        .worker(&refusing)
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(&refusing, ()).await.unwrap();
    // § dxgjekysz.d@szzdt
    let Err(Error::RunFailed { error, .. }) = run.result().await else {
        panic!("a Fail ends the run failed");
    };
    assert_eq!(
        (error.kind.as_str(), error.message.as_str()),
        ("Refused", "not today")
    );
    assert_eq!(run.status().await.unwrap(), RunStatus::Failed);
    // § end dxgjekysz.d
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_result_json_cannot_hold_blocks_the_run() {
    let env = Env::open(t0()).await.unwrap();
    // JSON keys are strings: a map keyed by pairs does not encode.
    let pairs = Workflow::without_input("pairs", |_c| async move {
        Ok(std::collections::BTreeMap::from([((1, 2), 3)]))
    });
    let worker = env.worker(&pairs).preset(Preset::Script).start().unwrap();
    let run = env.start(&pairs, ()).await.unwrap();
    let ended = run.result().await;
    let Err(Error::Stuck(m)) = &ended else {
        panic!("a result JSON cannot hold ended {ended:?}");
    };
    assert!(m.contains("blocked by Json"), "{m}");
    worker.shutdown().await.unwrap();
    env.close().await;
}
