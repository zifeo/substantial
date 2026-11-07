//! Clients and workers end to end, on every adapter of this host.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{LONG, eventually, namespaces, script};
use substantial::__internal as internal;
use substantial::{Context, Error, Result, Retry, RunStatus, Workflow, seconds};

async fn multiply(c: &Context, a: f64, b: f64) -> Result<f64> {
    c.step("multiply", move || async move { Ok::<_, Error>(a * b) })
        .await
}

/// The workflow of I-3, its sleep shortened.
fn example() -> Workflow<f64> {
    Workflow::without_input("example", |c| async move {
        let retry = Retry {
            max_attempts: 6,
            initial: seconds(1),
            max: seconds(60),
            multiplier: 2.0,
        };
        let res: f64 = c
            .step("roulette", || async { Ok::<_, Error>(100.0) })
            .retry(retry)
            .await?;
        c.sleep(Duration::from_millis(200)).await?;
        let res = multiply(&c, res, 0.5).await?;
        let n: f64 = c.receive("by").await?;
        let res = multiply(&c, res, n).await?;
        let stopped = Arc::new(AtomicBool::new(false));
        let s = stopped.clone();
        c.handle("stop", move |_: ()| s.store(true, Ordering::SeqCst))?;
        c.wait_until(move || stopped.load(Ordering::SeqCst)).await?;
        Ok(res)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_workflow() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, example()).start().unwrap();
        let run = sub.start(&example(), ()).await.unwrap();
        run.send("by", 2).await.unwrap();
        run.send("stop", ()).await.unwrap();
        let result: f64 = run.result().timeout(LONG).await.unwrap();
        assert_eq!(result, 100.0, "{}", ns.name);
        assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
        worker.shutdown().await.unwrap();
    }
}

/// Records a step, so that the run has a segment, then waits for a note.
fn waiting() -> Workflow<String> {
    Workflow::without_input("waiting", |c| async move {
        c.step("first", || async { Ok::<_, Error>(()) }).await?;
        let note: String = c.receive("note").await?;
        Ok(note)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_moves_from_pending_to_the_end() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(2).await.unwrap();
        let run = sub.start(&waiting(), ()).id("status-1").await.unwrap();
        assert_eq!(
            run.status().await.unwrap(),
            RunStatus::Pending,
            "{}",
            ns.name
        );
        let worker = script(&sub, waiting()).start().unwrap();
        eventually("a suspended run", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        let early = run.result().timeout(Duration::from_millis(50)).await;
        assert!(
            matches!(early, Err(Error::WaitTimeout)),
            "{}: {early:?}",
            ns.name
        );
        run.send("note", "hi").id("note-1").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), "hi");
        assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
        let refused = run.send("note", "again").await;
        assert!(
            matches!(refused, Err(Error::RunEnded { .. })),
            "{}: {refused:?}",
            ns.name
        );
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_run_is_cancelled() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, waiting()).start().unwrap();
        let run = sub.start(&waiting(), ()).await.unwrap();
        eventually("a suspended run", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        run.cancel("not needed").await.unwrap();
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                error,
                ..
            }) => {
                assert_eq!(error.message, "not needed", "{}", ns.name);
            }
            other => panic!("{}: {other:?}", ns.name),
        }
        assert_eq!(run.status().await.unwrap(), RunStatus::Cancelled);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unknown_chains_are_not_found() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let run = sub.run(waiting(), "nobody");
        let not_found = |r: &Result<_>| matches!(r, Err(Error::RunNotFound { chain, collected: false, .. }) if chain == "nobody");
        assert!(not_found(&run.send("by", 1).await), "{}", ns.name);
        assert!(not_found(&run.cancel("x").await));
        assert!(not_found(&run.status().await.map(drop)));
        assert!(not_found(&run.result().timeout(LONG).await.map(drop)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inputs_are_compared_as_canonical_json() {
    #[derive(serde::Serialize)]
    struct Inner {
        d: f64,
        c: &'static str,
    }
    #[derive(serde::Serialize)]
    struct Reordered {
        b: (f64, Inner),
        a: f64,
    }
    let echo = Workflow::new("echo", |_c: Context, input: serde_json::Value| async move {
        Ok(input)
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let reordered = Reordered {
            b: (1.0, Inner { d: 2.0, c: "é" }),
            a: 0.5,
        };
        let first = sub.start(&echo, reordered).id("same").await.unwrap();
        let written = serde_json::json!({"a": 0.5, "b": [1, {"c": "é", "d": 2}]});
        let again = sub.start(&echo, &written).id("same").await.unwrap();
        assert_eq!(first.chain(), again.chain(), "{}", ns.name);
        for x in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let refused = sub.start(&echo, HashMap::from([("x", [x])])).await;
            assert!(
                matches!(&refused, Err(Error::InvalidArgument(m)) if m.starts_with("the input is not JSON")),
                "{}: {x}: {refused:?}",
                ns.name
            );
        }
        for refused in [
            sub.start(&echo, HashMap::from([("x", [1u64 << 53])])).await,
            sub.start(&echo, HashMap::from([("x", [-(1i64 << 53)])]))
                .await,
        ] {
            assert!(
                matches!(&refused, Err(Error::InvalidArgument(m)) if m.starts_with("the input is not JSON")),
                "{}: {refused:?}",
                ns.name
            );
        }
        let worker = script(&sub, &echo).start().unwrap();
        let value: serde_json::Value = first.result().timeout(LONG).await.unwrap();
        assert_eq!(value, written);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn starts_are_idempotent_by_id() {
    let runs = Arc::new(AtomicU32::new(0));
    let r = runs.clone();
    let counted = Workflow::new("counted", move |c: Context, input: serde_json::Value| {
        let r = r.clone();
        async move {
            c.step("count", move || {
                let r = r.clone();
                async move { Ok::<_, Error>(r.fetch_add(1, Ordering::SeqCst)) }
            })
            .await?;
            Ok(input)
        }
    });
    for ns in namespaces() {
        runs.store(0, Ordering::SeqCst);
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let input = serde_json::json!({"x": 1});
        let first = sub.start(&counted, &input).id("once").await.unwrap();
        let again = sub.start(&counted, &input).id("once").await.unwrap();
        assert_eq!(first.chain(), again.chain());
        let other = sub
            .start(&counted, serde_json::json!({"x": 2}))
            .id("once")
            .await;
        assert!(
            matches!(other, Err(Error::RunExists { .. })),
            "{}: {other:?}",
            ns.name
        );
        let not_object = sub.start(&counted, 3).await;
        assert!(matches!(not_object, Err(Error::InvalidArgument(_))));
        let worker = script(&sub, &counted).start().unwrap();
        let value: serde_json::Value = first.result().timeout(LONG).await.unwrap();
        assert_eq!(value, input);
        assert_eq!(runs.load(Ordering::SeqCst), 1, "{}: one run", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_no_handler_can_decode_fails_the_run() {
    let typed = Workflow::without_input("typed", |c| async move {
        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s = seen.clone();
        c.handle("n", move |n: u32| s.lock().unwrap().push(n))?;
        c.wait_until(move || seen.lock().unwrap().len() >= 2)
            .await?;
        Ok(())
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, &typed).start().unwrap();
        let run = sub.start(&typed, ()).await.unwrap();
        run.send("n", 1).await.unwrap();
        run.send("n", "not a number").await.unwrap();
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Failed,
                error,
                ..
            }) => {
                assert_eq!(error.kind, "MessageDecode", "{}: {error}", ns.name);
            }
            other => panic!("{}: {other:?}", ns.name),
        }
        worker.shutdown().await.unwrap();
    }
}

fn invalid<T: std::fmt::Debug>(r: Result<T>) -> bool {
    matches!(r, Err(Error::InvalidArgument(_)))
}

#[tokio::test]
async fn names_outside_the_rules_are_invalid_arguments() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let long = "a".repeat(101);
        for id in ["Upper", ".", "..", "a/b", long.as_str()] {
            assert!(
                invalid(sub.start(&waiting(), ()).id(id).await),
                "{}: start {id}",
                ns.name
            );
            let run = sub.run(waiting(), id);
            assert!(
                invalid(run.send("note", "x").await),
                "{}: send to {id}",
                ns.name
            );
            assert!(invalid(run.status().await), "{}: status of {id}", ns.name);
            assert!(
                invalid(run.cancel("x").await),
                "{}: cancel of {id}",
                ns.name
            );
            assert!(
                invalid(run.result().timeout(LONG).await),
                "{}: result of {id}",
                ns.name
            );
        }
        let run = sub.start(&waiting(), ()).id("fine").await.unwrap();
        for id in ["Upper", "..", long.as_str()] {
            assert!(
                invalid(run.send("note", "x").id(id).await),
                "{}: message id {id}",
                ns.name
            );
        }
        assert!(
            invalid(run.send("a note", "x").await),
            "{}: message name",
            ns.name
        );
        let upper = Workflow::without_input("Upper", |_c| async { Ok(()) });
        assert!(invalid(sub.start(&upper, ()).await));
        assert!(invalid(script(&sub, upper).start().map(drop)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ids_of_a_hundred_bytes_work_end_to_end() {
    for ns in namespaces().into_iter().filter(|ns| ns.name != "memory") {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, waiting()).start().unwrap();
        let run = sub.start(waiting(), ()).id(&"r".repeat(100)).await.unwrap();
        run.send("note", "hi").id(&"m".repeat(100)).await.unwrap();
        let result = run.result().timeout(LONG).await.unwrap();
        assert_eq!(result, "hi", "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_name_outside_the_rules_fails_the_run() {
    let bad = Workflow::without_input("bad-step", |c| async move {
        c.step("a step", || async { Ok::<_, Error>(1) }).await
    });
    let ns = namespaces().remove(0);
    let sub = internal::create(ns.store).partitions(1).await.unwrap();
    let worker = script(&sub, &bad).start().unwrap();
    let run = sub.start(&bad, ()).await.unwrap();
    match run.result().timeout(LONG).await {
        Err(Error::RunFailed {
            status: RunStatus::Failed,
            error,
            ..
        }) => {
            assert_eq!(error.kind, "InvalidArgument", "{error}");
        }
        other => panic!("{other:?}"),
    }
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_reaches_a_run_looping_on_steps() {
    let ticks = Arc::new(AtomicU32::new(0));
    let t = ticks.clone();
    let w = Workflow::without_input("looping", move |c| {
        let ticks = t.clone();
        async move {
            loop {
                let ticks = ticks.clone();
                c.step("tick", move || {
                    ticks.fetch_add(1, Ordering::Relaxed);
                    async {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok::<_, Error>(())
                    }
                })
                .await?;
            }
            #[allow(unreachable_code)]
            Ok(())
        }
    });
    for ns in namespaces() {
        ticks.store(0, Ordering::Relaxed);
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, w.clone()).start().unwrap();
        let run = sub.start(&w, ()).await.unwrap();
        eventually("a few steps", async || ticks.load(Ordering::Relaxed) > 2).await;
        run.cancel("stop looping").await.unwrap();
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                ..
            }) => {}
            other => panic!("{}: {other:?}", ns.name),
        }
        worker.shutdown().await.unwrap();
    }
}
