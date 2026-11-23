//! Listing a workflow's live runs, reading a run's history, retrying a failed run, resetting an
//! ended one and starting with a message, in the test environment. The reset and message tests
//! port `python/tests/test_reset.py` and `python/tests/test_signal_with_start.py`, under the same
//! names.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use substantial::__internal::layout;
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
        error: None,
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
        matches!(&unknown, Err(Error::RunNotFound { chain }) if chain == "nobody"),
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

/// Fails at its step while `refuse` is set, else ends with its version.
fn flaky(version: u32, refuse: Arc<AtomicBool>) -> Workflow<String> {
    Workflow::without_input("flaky", move |c| {
        let refuse = refuse.clone();
        async move {
            c.step("try", move || {
                let refused = refuse.load(Ordering::SeqCst);
                async move {
                    if refused {
                        return Err(Permanent::new(std::io::Error::other("down")));
                    }
                    Ok(())
                }
            })
            .await?;
            Ok(format!("v{version}"))
        }
    })
    .with_version(version)
}

#[tokio::test]
async fn a_retry_runs_the_failed_runs_version_and_has_no_parent() {
    let env = Env::open(t0()).await.unwrap();
    let refuse = Arc::new(AtomicBool::new(true));
    let (one, two) = (flaky(1, refuse.clone()), flaky(2, refuse.clone()));
    let kid = two.clone();
    let parent = Workflow::without_input("parent", move |c| {
        let kid = kid.clone();
        async move {
            let child = c.start(&kid, ()).id("kid").await?;
            Ok(matches!(child.result().await, Err(Error::RunFailed { .. })))
        }
    });
    let worker = env
        .worker((parent.clone(), one.clone(), two.clone()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&parent, ()).id("p").await.unwrap();
    assert!(run.result().await.unwrap(), "the child failed");
    refuse.store(false, Ordering::SeqCst);
    // A handle of version 1: the retry takes the failed run's version, not the handle's.
    let child = env.run(&one, "kid");
    child.retry().await.unwrap();
    // § vb6krfmej.c@kz7tm
    assert_eq!(child.result().await.unwrap(), "v2");
    let history = child.history().await.unwrap();
    let retry = &history.last().unwrap().run;
    assert_ne!(retry, "kid", "{history:?}");
    let objects = env.__objects();
    // The first run's archive, and the chain's head, which holds the retry.
    let start = |key: &str, run: &str| {
        let (_, bytes) = objects.iter().find(|(k, _)| k == key).unwrap();
        match layout::Run::decode(bytes.as_slice()).unwrap().kind {
            Some(layout::run::Kind::Head(head)) if head.run == run => head.start.unwrap(),
            other => panic!("{key}: {other:?}"),
        }
    };
    assert_eq!(start("archives/kid", "kid").parent.unwrap().run, "p");
    let start = start("runs/kid", retry);
    assert_eq!(start.resumes.as_deref(), Some("kid"));
    assert_eq!(start.version, Some(2));
    assert_eq!(start.parent, None, "{start:?}");
    // § end vb6krfmej.c
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

#[derive(Deserialize)]
struct Count {
    n: usize,
}

fn collecting() -> Workflow<Vec<String>> {
    Workflow::new("collecting", |c: Context, Count { n }: Count| async move {
        let mut got = Vec::new();
        for _ in 0..n {
            got.push(c.receive::<String>("add").await?);
        }
        Ok(got)
    })
}

#[tokio::test]
async fn a_start_with_a_message_sends_it_once_the_run_is_created_or_found() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(collecting())
        .preset(Preset::Script)
        .start()
        .unwrap();
    let start = |message: substantial::Message| {
        env.start(collecting(), json!({"n": 2}))
            .id("cart")
            .message(message)
    };
    let first = || substantial::Message::new("add", "a").id("m-a");
    // § fqv5ts8vg@bqsyd
    let run = start(first()).await.unwrap();
    start(first()).await.unwrap();
    start(substantial::Message::new("add", "b")).await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        ["a", "b"],
        "the same id delivered once"
    );
    // § fqv5ts8vg.a@cbyex
    let ended = start(substantial::Message::new("add", "c")).await;
    assert!(matches!(ended, Err(Error::RunEnded { .. })), "{ended:?}");
    // § end fqv5ts8vg.a
    // § end fqv5ts8vg
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_start_refuses_a_message_outside_the_rules() {
    let env = Env::open(t0()).await.unwrap();
    let refused = [
        substantial::Message::new("substantial.x", ()),
        substantial::Message::new("add", ()).id("Upper"),
        substantial::Message::new("add", f64::NAN),
    ];
    for message in refused {
        let start = env.start(collecting(), json!({"n": 1})).message(message);
        let refused = start.await;
        assert!(
            matches!(refused, Err(Error::InvalidArgument(_))),
            "{refused:?}"
        );
    }
    done(env).await;
}

fn ordered(ran: &Arc<Mutex<Vec<&'static str>>>) -> Workflow<Vec<String>> {
    let ran = ran.clone();
    Workflow::without_input("ordered", move |c| {
        let ran = ran.clone();
        async move {
            let note = |name: &'static str| {
                let ran = ran.clone();
                c.step(name, move || {
                    ran.lock().unwrap().push(name);
                    async move { Ok::<_, Error>(name.to_owned()) }
                })
            };
            let a = note("a").await?;
            let go: String = c.receive("go").await?;
            let b = note("b").await?;
            Ok(vec![a, go, b])
        }
    })
}

#[tokio::test]
async fn a_reset_replays_the_first_events_of_an_ended_run() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(ordered(&ran))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(ordered(&ran), ()).id("o").await.unwrap();
    env.settle().await.unwrap();
    run.send("go", "now").await.unwrap();
    assert_eq!(run.result().await.unwrap(), ["a", "now", "b"]);
    let kinds: Vec<String> = (run.history().await.unwrap().into_iter())
        .map(|e| e.kind)
        .collect();
    assert_eq!(kinds, ["step", "message", "step", "completed"]);
    // § grzez328c@62ps0
    run.retry().keep(1).await.unwrap();
    let again = run.result().await.unwrap();
    assert_eq!(
        again,
        ["a", "now", "b"],
        "the message after the point given back"
    );
    assert_eq!(
        *ran.lock().unwrap(),
        ["a", "b", "b"],
        "the kept step not again"
    );
    // § end grzez328c
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_reset_refuses_a_point_past_the_end_and_a_live_run() {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(ordered(&ran))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(ordered(&ran), ()).await.unwrap();
    env.settle().await.unwrap();
    // § grzez328c.a@9e7pf
    let live = run.retry().keep(0).await;
    assert!(matches!(live, Err(Error::InvalidArgument(_))), "{live:?}");
    run.send("go", "now").await.unwrap();
    run.result().await.unwrap();
    let past = run.retry().keep(99).await;
    assert!(matches!(past, Err(Error::InvalidArgument(_))), "{past:?}");
    // § end grzez328c.a
    worker.shutdown().await.unwrap();
    done(env).await;
}
