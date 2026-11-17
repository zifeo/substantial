//! What a `Substantial` tells its observer, in a test environment.

use std::future::IntoFuture;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures::FutureExt;
use substantial::observe::{Attempt, Replay, Scan, StoreOp};
use substantial::testing::{Chaos, Env};
use substantial::{Amount, Error, Observed, Preset, Prices, Retry, Substantial, Workflow, minutes};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

type Seen = Arc<Mutex<Vec<Observed>>>;

fn recorder() -> (Seen, impl Fn(&Observed) + Send + Sync + 'static) {
    let seen = Seen::default();
    let pushed = seen.clone();
    (seen, move |e: &Observed| {
        pushed.lock().unwrap().push(e.clone())
    })
}

fn store_ops(seen: &Seen) -> Vec<StoreOp> {
    let seen = seen.lock().unwrap();
    (seen.iter())
        .filter_map(|e| match e {
            Observed::StoreOp(op) => Some(op.clone()),
            _ => None,
        })
        .collect()
}

fn attempts(seen: &Seen) -> Vec<Attempt> {
    let seen = seen.lock().unwrap();
    (seen.iter())
        .filter_map(|e| match e {
            Observed::Attempt(a) => Some(a.clone()),
            _ => None,
        })
        .collect()
}

fn replays(seen: &Seen) -> Vec<Replay> {
    let seen = seen.lock().unwrap();
    (seen.iter())
        .filter_map(|e| match e {
            Observed::Replay(r) => Some(r.clone()),
            _ => None,
        })
        .collect()
}

fn scans(seen: &Seen) -> Vec<Scan> {
    let seen = seen.lock().unwrap();
    (seen.iter())
        .filter_map(|e| match e {
            Observed::Scan(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

/// A step that fails once, retried, one that fails at once, then a sleep.
fn flaky(tries: Arc<Mutex<u32>>) -> Workflow<u32> {
    Workflow::without_input("flaky", move |c| {
        let tries = tries.clone();
        async move {
            let retry = Retry {
                max_attempts: 2,
                initial: Duration::from_secs(1),
                max: Duration::from_secs(1),
                multiplier: 1.0,
            };
            let once = c
                .step("once", move || {
                    let tries = tries.clone();
                    async move {
                        let mut tries = tries.lock().unwrap();
                        *tries += 1;
                        match *tries {
                            1 => Err(Error::InvalidArgument("once".into())),
                            n => Ok(n),
                        }
                    }
                })
                .retry(retry)
                .await?;
            let no = c
                .step("no", || async {
                    Err::<u32, _>(Error::InvalidArgument("no".into()))
                })
                .await;
            assert!(matches!(no, Err(Error::StepFailed { .. })), "{no:?}");
            c.sleep(minutes(1)).await?;
            Ok(once)
        }
    })
}

#[tokio::test]
async fn every_kind_of_event_with_its_fields() {
    let (seen, observe) = recorder();
    let prices = Prices::named("aws-s3-standard-us-east-1-2026-11-09").unwrap();
    let env = Env::open(t0())
        .prices(prices)
        .observe(observe)
        .await
        .unwrap();
    let wf = flaky(Arc::default());
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let run = env.start(&wf, ()).await.unwrap();
    env.advance(minutes(2)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 2);

    let ops = store_ops(&seen);
    for op in &ops {
        let per = match op.op {
            "create" | "list" => 5000,
            "get" => 400,
            "delete" => 0,
            other => panic!("op {other}"),
        };
        let cost = Amount {
            currency: "USD".into(),
            nano: per * u64::from(op.attempts),
        };
        assert_eq!(op.cost.as_ref(), Some(&cost), "{op:?}");
        assert!(op.attempts >= 1 && op.seconds >= 0.0, "{op:?}");
        let outcomes: &[&str] = match op.op {
            "create" => &["created", "exists"],
            "get" => &["found", "missing"],
            "list" => &["listed"],
            _ => &["deleted"],
        };
        assert!(outcomes.contains(&op.outcome), "{op:?}");
    }
    for kind in ["create", "get", "list", "delete"] {
        assert!(ops.iter().any(|op| op.op == kind), "no {kind} in {ops:?}");
    }

    let tried: Vec<(String, u32, &str)> = attempts(&seen)
        .into_iter()
        .map(|a| {
            assert_eq!(a.workflow, "flaky");
            assert!(a.seconds >= 0.0);
            (a.step, a.attempt, a.outcome)
        })
        .collect();
    assert_eq!(
        tried,
        [
            ("once".into(), 1, "retry"),
            ("once".into(), 2, "ok"),
            ("no".into(), 1, "failed"),
        ]
    );

    let replayed = replays(&seen);
    assert!(replayed.iter().all(|r| r.workflow == "flaky"));
    let ends: Vec<&str> = replayed.iter().map(|r| r.end).collect();
    assert_eq!(ends.last(), Some(&"ended"), "{replayed:?}");
    assert!(ends.contains(&"suspended"), "{replayed:?}");
    assert!(
        replayed.iter().all(|r| r.delay.is_some_and(|d| d >= 0.0)),
        "each replay serves a due entry: {replayed:?}"
    );

    let scanned = scans(&seen);
    assert!(scanned.iter().all(|s| s.workflow == "flaky"));
    assert!(scanned.iter().any(|s| s.due > 0), "{scanned:?}");
    worker.shutdown().await.unwrap();
    env.close().await;
}

/// On the real clock: an attempt past its timeout, then one left behind by a shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attempts_that_time_out_or_are_left_behind() {
    let (seen, observe) = recorder();
    let url = format!("memory://observe-{}", std::process::id());
    let sub = Substantial::create(url).observe(observe).await.unwrap();
    let wf = Workflow::without_input("late", |c| async move {
        let late = c
            .step("late", || async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok::<_, Error>(0)
            })
            .timeout(Duration::from_millis(100))
            .await;
        assert!(matches!(late, Err(Error::StepFailed { .. })), "{late:?}");
        c.step("slow", || async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<_, Error>(0)
        })
        .await
    });
    let worker = sub
        .worker(&wf)
        .preset(Preset::Script)
        .set("max_idle", "50ms")
        .set("shutdown_grace", "100ms")
        .start()
        .unwrap();
    sub.start(&wf, ()).await.unwrap();
    let outcomes = || -> Vec<(String, &'static str)> {
        let tried = attempts(&seen).into_iter();
        tried.map(|a| (a.step, a.outcome)).collect()
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while outcomes().is_empty() {
        assert!(std::time::Instant::now() < deadline, "no attempt ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The second step runs once the timeout is recorded.
    tokio::time::sleep(Duration::from_millis(300)).await;
    worker.shutdown().await.unwrap();
    while outcomes().len() < 2 {
        assert!(std::time::Instant::now() < deadline, "{:?}", outcomes());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        outcomes(),
        [("late".into(), "timeout"), ("slow".into(), "abandoned")]
    );
    let dropped = replays(&seen)
        .into_iter()
        .map(|r| r.end)
        .collect::<Vec<_>>();
    assert_eq!(dropped.last(), Some(&"dropped"), "{dropped:?}");
}

#[tokio::test]
async fn a_store_operation_retried_reports_its_attempts() {
    let (seen, observe) = recorder();
    let chaos = Chaos { seed: 7, rate: 0.3 };
    let env = Env::open(t0()).chaos(chaos).observe(observe).await.unwrap();
    let wf = Workflow::without_input("steps", |c| async move {
        for i in 0..5 {
            c.step("s", move || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(0)
    });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let run = env.start(&wf, ()).await.unwrap();
    env.advance(minutes(5)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 0);
    assert!(env.faults().failed > 0);
    let ops = store_ops(&seen);
    assert!(
        ops.iter()
            .any(|op| op.attempts > 1 && op.outcome != "failed"),
        "{ops:?}"
    );
    assert!(ops.iter().all(|op| op.cost.is_none()), "no prices, no cost");
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn an_observer_that_panics_stops_the_worker_or_the_client_call() {
    let panicking = |e: &Observed| {
        if matches!(e, Observed::Scan(_)) {
            panic!("observer down");
        }
    };
    let env = Env::open(t0()).observe(panicking).await.unwrap();
    let wf = Workflow::without_input("w", |_c| async move { Ok(0) });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let settled = env.settle().await;
    let expected = "the worker panicked: observer down";
    assert!(
        matches!(&settled, Err(Error::Protocol(m)) if m == expected),
        "{settled:?}"
    );
    assert!(!worker.is_alive());
    assert!(matches!(worker.error(), Some(Error::Protocol(m)) if m == expected));
    env.close().await;

    let on_ops = |e: &Observed| {
        if matches!(e, Observed::StoreOp(_)) {
            panic!("observer down");
        }
    };
    let opening = Env::open(t0()).observe(on_ops).into_future();
    let opened = std::panic::AssertUnwindSafe(opening).catch_unwind().await;
    assert!(opened.is_err(), "the client call panics with the observer");
}
