//! Listing a workflow's live runs, reading a run's history, and retrying a failed run, in the
//! test environment.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::json;
use substantial::testing::Env;
use substantial::{
    Context, Error, Permanent, Preset, RunInfo, WaitingOn, Workflow, minutes, step_key,
};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

#[derive(Deserialize)]
struct Mode {
    mode: String,
}

/// Sleeps 5 minutes, receives `go`, or ends at once, as its input says.
fn waits() -> Workflow<()> {
    Workflow::new("waits", |c: Context, input: Mode| async move {
        match input.mode.as_str() {
            "sleep" => c.sleep(minutes(5)).await?,
            "receive" => c.receive::<i64>("go").await.map(|_| ())?,
            _ => {}
        }
        Ok(())
    })
    .with_version(3)
}

async fn listed(env: &Env) -> Vec<RunInfo> {
    let mut runs: Vec<RunInfo> = env.runs(waits()).try_collect().await.unwrap();
    runs.sort_by(|a, b| a.chain.cmp(&b.chain));
    runs
}

#[tokio::test]
async fn runs_lists_the_live_runs_of_a_workflow_with_what_each_waits_on() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env.worker(waits()).preset(Preset::Service).start().unwrap();
    for (id, mode) in [
        ("slept", "sleep"),
        ("received", "receive"),
        ("messaged", "receive"),
        ("ended", "end"),
    ] {
        env.start(waits(), json!({ "mode": mode }))
            .id(id)
            .await
            .unwrap();
    }
    env.advance(minutes(1)).await.unwrap();
    let t1 = env.now();
    // Neither settles before the listing: the message waits to be consumed, the run to start.
    env.run(waits(), "messaged").send("go", 1).await.unwrap();
    env.start(waits(), json!({ "mode": "end" }))
        .id("pending")
        .await
        .unwrap();
    let info = |chain: &str, due_at, waiting_on| RunInfo {
        chain: chain.into(),
        run: chain.into(),
        version: Some(3),
        due_at,
        waiting_on,
    };
    assert_eq!(
        listed(&env).await,
        [
            info("messaged", Some(t1), WaitingOn::Inbox),
            info("pending", Some(t1), WaitingOn::Start),
            info("received", None, WaitingOn::Message),
            info("slept", Some(t0() + minutes(5)), WaitingOn::Time),
        ]
    );
    assert_eq!(WaitingOn::Inbox.to_string(), "inbox");
    assert_eq!(WaitingOn::Message.to_string(), "message");

    let mut runs = env.runs(waits());
    let first = runs.next().await.unwrap().unwrap();
    drop(runs);
    assert!(listed(&env).await.contains(&first), "early exit");

    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn history_gives_the_events_of_the_last_run_in_order() {
    let env = Env::open(t0()).await.unwrap();
    let stepped = Workflow::without_input("stepped", |c| async move {
        let a = c.step("a", || async { Ok::<_, Error>(7) }).await?;
        c.sleep(minutes(1)).await?;
        Ok(a)
    });
    let worker = env
        .worker(&stepped)
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&stepped, ()).id("h").await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 7);

    let history = run.history().await.unwrap();
    let kinds: Vec<&str> = history.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, ["step", "timer", "fired", "completed"]);
    assert_eq!(history[0].data["name"], "a");
    assert_eq!(history[0].data["value"], 7);
    for e in &history {
        assert_eq!(e.run, "h");
        assert!(e.at >= t0() && e.at <= env.now(), "{e:?}");
    }
    assert!(history.windows(2).all(|w| w[0].segment <= w[1].segment));

    let unknown = env.run(&stepped, "nobody").history().await;
    assert!(
        matches!(&unknown, Err(Error::RunNotFound { chain, collected: false, .. }) if chain == "nobody"),
        "{unknown:?}"
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[derive(Default)]
struct Retried {
    a: AtomicUsize,
    b: AtomicUsize,
    refuse: AtomicBool,
    keys: Mutex<Vec<String>>,
}

/// Step `a` then step `b`, which fails for good while `refuse` is set.
fn retried(seen: Arc<Retried>) -> Workflow<String> {
    Workflow::without_input("retried", move |c| {
        let seen = seen.clone();
        async move {
            let s = seen.clone();
            let a = c
                .step("a", move || {
                    let n = s.a.fetch_add(1, Ordering::SeqCst) + 1;
                    async move { Ok::<_, Error>(format!("a{n}")) }
                })
                .await?;
            c.step("b", move || {
                let seen = seen.clone();
                async move {
                    seen.b.fetch_add(1, Ordering::SeqCst);
                    seen.keys.lock().unwrap().push(step_key());
                    if seen.refuse.load(Ordering::SeqCst) {
                        return Err(Permanent::new(std::io::Error::other("refused")));
                    }
                    Ok(())
                }
            })
            .await?;
            Ok(a)
        }
    })
}

fn invalid(r: &Result<(), Error>) -> bool {
    matches!(r, Err(Error::InvalidArgument(_)))
}

#[tokio::test]
async fn retry_resumes_a_failed_run_keeping_its_recorded_steps_and_step_keys() {
    let env = Env::open(t0()).await.unwrap();
    let seen = Arc::new(Retried::default());
    seen.refuse.store(true, Ordering::SeqCst);
    let w = retried(seen.clone());
    let worker = env.worker(&w).preset(Preset::Service).start().unwrap();
    let run = env.start(&w, ()).id("r").await.unwrap();
    let failed = run.result().await;
    assert!(matches!(failed, Err(Error::RunFailed { .. })), "{failed:?}");

    seen.refuse.store(false, Ordering::SeqCst);
    run.retry().await.unwrap();
    assert_eq!(run.result().await.unwrap(), "a1");
    assert_eq!(seen.a.load(Ordering::SeqCst), 1);
    assert_eq!(seen.b.load(Ordering::SeqCst), 2);
    assert_eq!(*seen.keys.lock().unwrap(), ["r/b/0", "r/b/0"]);
    let history = run.history().await.unwrap();
    let events: Vec<(bool, &str)> = (history.iter())
        .map(|e| (e.run == "r", e.kind.as_str()))
        .collect();
    assert_eq!(
        events,
        [(true, "step"), (false, "step"), (false, "completed")],
        "{history:?}"
    );

    let completed = run.retry().await;
    assert!(invalid(&completed), "{completed:?}");
    let pending = env.start(&w, ()).id("pending").await.unwrap();
    let live = pending.retry().await;
    assert!(invalid(&live), "{live:?}");
    let unknown = env.run(&w, "nobody").retry().await;
    assert!(
        matches!(&unknown, Err(Error::RunNotFound { chain, .. }) if chain == "nobody"),
        "{unknown:?}"
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_run_failed_by_a_rollback_is_not_retried() {
    let env = Env::open(t0()).await.unwrap();
    let rolled = Workflow::without_input("rolled", |c| async move {
        Err::<(), _>(c.rollback("why").await)
    });
    let worker = env.worker(&rolled).preset(Preset::Service).start().unwrap();
    let run = env.start(&rolled, ()).id("rolled").await.unwrap();
    let failed = run.result().await;
    assert!(matches!(failed, Err(Error::RunFailed { .. })), "{failed:?}");
    let retried = run.retry().await;
    assert!(invalid(&retried), "{retried:?}");
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_failed_run_whose_saga_ran_undos_is_not_retried() {
    let env = Env::open(t0()).await.unwrap();
    let undone = Workflow::without_input("undone", |c| async move {
        c.saga(|c| async move {
            c.step("reserve", || async { Ok::<_, Error>(1) })
                .undo(|_: i64| async { Ok::<_, Error>(()) })
                .await?;
            c.step("charge", || async {
                Err::<(), _>(Permanent::new(std::io::Error::other("declined")))
            })
            .await
        })
        .await
    });
    let worker = env.worker(&undone).preset(Preset::Service).start().unwrap();
    let run = env.start(&undone, ()).id("undone").await.unwrap();
    let failed = run.result().await;
    assert!(matches!(failed, Err(Error::RunFailed { .. })), "{failed:?}");
    let retried = run.retry().await;
    assert!(
        matches!(&retried, Err(Error::InvalidArgument(m)) if m.contains("ran undos")),
        "{retried:?}"
    );
    let still = run.result().await;
    assert!(
        matches!(&still, Err(Error::RunFailed { run, .. }) if run == "undone"),
        "{still:?}"
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}
