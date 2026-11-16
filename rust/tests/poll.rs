//! `c.poll`: a step tried again after a sleep until it returns a value or its timeout passes. The
//! ports of `python/tests/test_poll.py`, under the same names.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use substantial::testing::Env;
use substantial::{Context, Error, Preset, Retry, Workflow, minutes, seconds};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

type Pokes = Arc<AtomicU64>;

fn pokes(p: &Pokes) -> u64 {
    p.load(Ordering::SeqCst)
}

#[derive(Deserialize)]
struct Input {
    n: u64,
    every: u64,
    timeout: Option<u64>,
}

/// Polls `check`, ready from its `n`th poke; without `timeout`, the poll is missing it.
fn poller(pokes: &Pokes) -> Workflow<Option<String>> {
    let pokes = pokes.clone();
    Workflow::new("poller", move |c: Context, input: Input| {
        let pokes = pokes.clone();
        async move {
            let n = input.n;
            let poll = c
                .poll("check", move || {
                    let pokes = pokes.clone();
                    async move {
                        let poked = pokes.fetch_add(1, Ordering::SeqCst) + 1;
                        Ok::<_, Error>((poked >= n).then(|| "ready".to_owned()))
                    }
                })
                .every(minutes(input.every));
            match input.timeout {
                Some(t) => poll.timeout(minutes(t)).await,
                None => poll.await,
            }
        }
    })
}

#[tokio::test]
async fn poll_returns_the_first_value_that_is_not_none() {
    let env = Env::open(t0()).await.unwrap();
    let p = Pokes::default();
    let wf = poller(&p);
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let input = json!({"n": 3, "every": 1, "timeout": 10});
    let run = env.start(&wf, input).await.unwrap();
    env.advance(minutes(2)).await.unwrap();
    assert_eq!(run.result().await.unwrap().as_deref(), Some("ready"));
    assert_eq!(pokes(&p), 3);
    let history = run.history().await.unwrap();
    let checks: Vec<(&str, u64)> = (history.iter())
        .filter(|e| e.data["name"] == "check")
        .map(|e| {
            (
                e.data["name"].as_str().unwrap(),
                e.data["occurrence"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(checks, [("check", 0), ("check", 1), ("check", 2)]);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn poll_returns_none_once_its_timeout_passed() {
    let env = Env::open(t0()).await.unwrap();
    let p = Pokes::default();
    let wf = poller(&p);
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let input = json!({"n": 100, "every": 3, "timeout": 10});
    let run = env.start(&wf, input).await.unwrap();
    env.advance(minutes(9)).await.unwrap();
    assert_eq!(pokes(&p), 4);
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), None);
    // At 0, 3, 6 and 9 minutes, and at 10, the sleep bounded by the time left.
    assert_eq!(pokes(&p), 5);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_recorded_poke_never_runs_again() {
    let env = Env::open(t0()).await.unwrap();
    let p = Pokes::default();
    let wf = poller(&p);
    let worker = env
        .worker(&wf)
        .preset(Preset::Service)
        .set("continue_as_new_after", "0")
        .start()
        .unwrap();
    let input = json!({"n": 2, "every": 1, "timeout": 10});
    let run = env.start(&wf, input).await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap().as_deref(), Some("ready"));
    assert_eq!(pokes(&p), 2);
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn flaky(pokes: &Pokes) -> Workflow<Option<u64>> {
    let pokes = pokes.clone();
    Workflow::without_input("flaky", move |c| {
        let pokes = pokes.clone();
        async move {
            let retry = Retry {
                max_attempts: 2,
                initial: seconds(1),
                max: seconds(1),
                multiplier: 1.0,
            };
            c.poll("check", move || {
                let pokes = pokes.clone();
                async move {
                    let poked = pokes.fetch_add(1, Ordering::SeqCst) + 1;
                    if poked == 1 {
                        return Err(std::io::Error::other("provider down"));
                    }
                    Ok(Some(poked))
                }
            })
            .every(minutes(1))
            .timeout(minutes(5))
            .retry(retry)
            .await
        }
    })
}

#[tokio::test]
async fn each_poke_retries_as_its_step_says() {
    let env = Env::open(t0()).await.unwrap();
    let p = Pokes::default();
    let wf = flaky(&p);
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let run = env.start(&wf, ()).await.unwrap();
    env.advance(seconds(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), Some(2));
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// A zero `every`, and a missing `timeout`: a `Duration` is never negative.
#[tokio::test]
async fn poll_checks_every_and_timeout() {
    let cases = [
        (
            json!({"n": 1, "every": 0, "timeout": 10}),
            "every is a positive duration",
        ),
        (json!({"n": 1, "every": 1}), "timeout is a duration"),
    ];
    for (input, message) in cases {
        let env = Env::open(t0()).await.unwrap();
        let wf = poller(&Pokes::default());
        let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
        let run = env.start(&wf, input).await.unwrap();
        let failed = run.result().await;
        assert!(
            matches!(&failed, Err(e @ Error::RunFailed { .. }) if e.to_string().contains(message)),
            "{failed:?}"
        );
        worker.shutdown().await.unwrap();
        done(env).await;
    }
}
