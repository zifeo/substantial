//! Time, step keys, handlers, sends, cancellation from inside, publishing and reading, end to end.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use common::{LONG, Recording, eventually, namespaces, script};
use substantial::__internal::{self as internal, Create};
use substantial::{Context, Error, Handlers, RunStatus, Workflow, step_key};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn now_sleep_until_and_step_keys() {
    let timed = Workflow::without_input("timed", |c| async move {
        let start = c.now().await?;
        c.sleep_until(start + chrono::Duration::milliseconds(300))
            .await?;
        let end: DateTime<Utc> = c.now().await?;
        let key = |name: &str| c.step(name, || async { Ok::<_, Error>(step_key()) });
        let mut keys = vec![key("keyed").await?];
        let (other, keyed) = tokio::try_join!(key("other"), key("keyed"))?;
        keys.extend([other, keyed, key("keyed").await?]);
        Ok(((end - start).num_milliseconds(), keys))
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &timed).start().unwrap();
        let run = sub.start(&timed, ()).id("timed-1").await.unwrap();
        let (slept, keys): (i64, Vec<String>) = run.result().timeout(LONG).await.unwrap();
        assert!(slept >= 300, "{}: slept {slept} ms", ns.name);
        let expected = [
            "timed-1/keyed/0",
            "timed-1/other/0",
            "timed-1/keyed/1",
            "timed-1/keyed/2",
        ];
        // § twravbm6m.a@5b260
        assert_eq!(keys, expected, "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[derive(serde::Deserialize)]
struct Target {
    target: String,
}

fn approver() -> Workflow<Vec<String>> {
    Workflow::new("approver", |c: Context, input: Target| async move {
        let approvals = Arc::new(Mutex::new(Vec::<String>::new()));
        let approved = approvals.clone();
        c.handle("approve", move |name: String| {
            let mut names = approved.lock().unwrap();
            if !names.contains(&name) {
                names.push(name);
            }
        })?;
        let seen = approvals.clone();
        c.wait_until(move || seen.lock().unwrap().len() >= 2)
            .await?;
        let names = approvals.lock().unwrap().clone();
        c.send(&input.target, "note", names.join(",")).await?;
        Ok(names)
    })
}

fn waiting() -> Workflow<String> {
    Workflow::without_input("waiting", |c| async move {
        // § ezxksxayf@7za7b
        let reserved = c.receive::<String>("substantial.ended").await;
        if !matches!(reserved, Err(Error::InvalidArgument(_))) {
            return Err(Error::InvalidArgument(format!("receive: {reserved:?}")));
        }
        let reserved = c.handle("substantial.ended", |_: serde_json::Value| {});
        if !matches!(reserved, Err(Error::InvalidArgument(_))) {
            return Err(Error::InvalidArgument(format!("handle: {reserved:?}")));
        }
        // § end ezxksxayf
        c.receive::<String>("note").await
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handler_sees_past_messages_first_and_a_workflow_sends() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let target = sub.start(&waiting(), ()).id("target").await.unwrap();
        let input = serde_json::json!({"target": "target"});
        let run = sub.start(&approver(), input).await.unwrap();
        for name in ["alice", "alice", "bob"] {
            run.send("approve", name).await.unwrap();
        }
        let worker = script(&sub, (approver(), waiting())).start().unwrap();
        let names: Vec<String> = run.result().timeout(LONG).await.unwrap();
        assert_eq!(names, ["alice", "bob"], "{}", ns.name);
        let note: String = target.result().timeout(LONG).await.unwrap();
        assert_eq!(note, "alice,bob", "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handle_all_sees_past_messages_in_arrival_order() {
    let ordered = Workflow::without_input("ordered", |c| async move {
        // § vcgwgaf4n@ett11
        c.receive::<()>("go").await?;
        let all = Arc::new(Mutex::new(Vec::<String>::new()));
        let (add, remove) = (all.clone(), all.clone());
        c.handle_all(
            Handlers::new()
                .on("add", move |n: u32| {
                    add.lock().unwrap().push(format!("add:{n}"))
                })
                .on("remove", move |n: u32| {
                    remove.lock().unwrap().push(format!("remove:{n}"))
                }),
        )?;
        // § end vcgwgaf4n
        let adds = Arc::new(Mutex::new(Vec::<u32>::new()));
        let a = adds.clone();
        c.handle("add", move |n: u32| a.lock().unwrap().push(n))?;
        let twice = c.handle_all(Handlers::new().on("x", |_: ()| {}).on("x", |_: ()| {}));
        if !matches!(twice, Err(Error::InvalidArgument(_))) {
            return Err(Error::InvalidArgument(format!("handle_all: {twice:?}")));
        }
        let all = all.lock().unwrap().clone();
        let adds = adds.lock().unwrap().clone();
        Ok((all, adds))
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let run = sub.start(&ordered, ()).await.unwrap();
        for (name, n) in [("add", 1), ("remove", 1), ("add", 2), ("remove", 2)] {
            run.send(name, n).await.unwrap();
        }
        run.send("go", ()).await.unwrap();
        let worker = script(&sub, &ordered).start().unwrap();
        let (all, adds): (Vec<String>, Vec<u32>) = run.result().timeout(LONG).await.unwrap();
        // § n4zyj94mr@32xv4
        assert_eq!(
            all,
            ["add:1", "remove:1", "add:2", "remove:2"],
            "{}",
            ns.name
        );
        // § end n4zyj94mr
        assert_eq!(adds, [1, 2], "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserved_names_are_refused_to_a_client_send_and_a_step() {
    let stepper = Workflow::without_input("stepper", |c| async move {
        let refused = c
            .step("substantial.charge", || async { Ok::<_, Error>(()) })
            .await;
        Ok(matches!(refused, Err(Error::InvalidArgument(e)) if e.contains("reserved")))
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let run = sub.start(&stepper, ()).await.unwrap();
        match run.send("substantial.cancel", "forged").await {
            Err(Error::InvalidArgument(e)) => assert!(e.contains("reserved"), "{}: {e}", ns.name),
            other => panic!("{}: {other:?}", ns.name),
        }
        let worker = script(&sub, &stepper).start().unwrap();
        let refused: bool = run.result().timeout(LONG).await.unwrap();
        assert!(refused, "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_cancels_itself() {
    let quitter = Workflow::without_input("quitter", |c| async move {
        c.step("first", || async { Ok::<_, Error>(()) }).await?;
        Err::<(), _>(c.cancel("enough").await)
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &quitter).start().unwrap();
        let run = sub.start(&quitter, ()).await.unwrap();
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                error,
                ..
            }) => assert_eq!(error.message, "enough", "{}", ns.name),
            other => panic!("{}: {other:?}", ns.name),
        }
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_running_when_a_cancel_lands_is_recorded_and_returned() {
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (s, r) = (started.clone(), release.clone());
    let held = Workflow::without_input("held", move |c| {
        let (s, r) = (s.clone(), r.clone());
        async move {
            let value: u32 = c
                .step("held", move || {
                    let (s, r) = (s.clone(), r.clone());
                    async move {
                        s.add_permits(1);
                        r.acquire().await.expect("never closed").forget();
                        Ok::<_, Error>(42)
                    }
                })
                .await?;
            c.publish("got", value)?;
            c.receive::<()>("never").await?;
            Ok(value)
        }
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &held).start().unwrap();
        let run = sub.start(&held, ()).await.unwrap();
        // § vmk2ydwta.d@s7hx0
        // A permit is consumed, not dropped: a dropped one goes back, and the next store's
        // run would then be cancelled before its attempt starts.
        started.acquire().await.unwrap().forget();
        run.cancel("stop").await.unwrap();
        release.add_permits(1);
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                ..
            }) => {}
            other => panic!("{}: {other:?}", ns.name),
        }
        let got: Option<u32> = run.read("got").await.unwrap();
        assert_eq!(got, Some(42), "{}: the outcome reached the code", ns.name);
        // § end vmk2ydwta.d
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caught_cancel_or_rollback_ends_the_run_as_the_code_ends_it() {
    let caught = Workflow::without_input("caught", |c| async move {
        let mut seen = Vec::new();
        if let Error::Cancelled(reason) = c.cancel("changed my mind").await {
            seen.push(reason);
        }
        if let Error::RolledBack(reason) = c.rollback("not this way").await {
            seen.push(reason);
        }
        Ok(seen)
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &caught).start().unwrap();
        let run = sub.start(&caught, ()).await.unwrap();
        let seen: Vec<String> = run.result().timeout(LONG).await.unwrap();
        assert_eq!(seen, ["changed my mind", "not this way"], "{}", ns.name);
        assert_eq!(
            run.status().await.unwrap(),
            RunStatus::Completed,
            "{}",
            ns.name
        );
        worker.shutdown().await.unwrap();
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Round {
    round: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_read_what_the_chain_published() {
    let publisher = Workflow::new("publisher", |c: Context, input: Round| async move {
        if input.round == 1 {
            c.publish("other", "second run")?;
            return Ok(true);
        }
        c.publish("progress", serde_json::json!({"done": 1}))?;
        c.step("work", || async { Ok::<_, Error>(()) }).await?;
        c.publish("progress", serde_json::json!({"done": 2}))?;
        if !matches!(c.publish("Not A Key", 0), Err(Error::InvalidArgument(_))) {
            return Ok(false);
        }
        Err(c.continue_as_new(Round { round: 1 }).await)
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &publisher).start().unwrap();
        let run = sub.start(&publisher, Round { round: 0 }).await.unwrap();
        let refused: bool = run.result().timeout(LONG).await.unwrap();
        assert!(refused, "{}: a key outside the rules", ns.name);
        // § fx1tv8nqp@bma9w
        let progress: Option<serde_json::Value> = run.read("progress").await.unwrap();
        assert_eq!(
            progress,
            Some(serde_json::json!({"done": 2})),
            "{}",
            ns.name
        );
        let other: Option<String> = run.read("other").await.unwrap();
        assert_eq!(other.as_deref(), Some("second run"), "{}", ns.name);
        assert_eq!(run.read::<u32>("never").await.unwrap(), None, "{}", ns.name);
        let bad = run.read::<u32>("Not A Key").await;
        assert!(matches!(bad, Err(Error::InvalidArgument(_))), "{bad:?}");
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_fails_with_its_own_error() {
    use anyhow::Context as _;
    let own = Workflow::without_input("own", |_c| async move {
        Err::<(), _>(Error::failed("Refused", "not today"))
    });
    let parsing = Workflow::without_input("parsing", |_c| async move {
        let n: i64 = "x".parse().context("parsing the count")?;
        Ok(n)
    });
    let wrapped = Workflow::without_input("wrapped", |c| async move {
        let control = anyhow::Error::from(c.cancel("enough").await);
        Err::<(), _>(control)?;
        Ok(())
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflows = vec![own.untyped(), parsing.untyped(), wrapped.untyped()];
        let worker = script(&sub, workflows).start().unwrap();
        let failure = |r| match r {
            Err(Error::RunFailed { status, error, .. }) => (status, error.kind, error.message),
            other => panic!("{}: {other:?}", ns.name),
        };
        let run = sub.start(&own, ()).await.unwrap();
        let expected = (RunStatus::Failed, "Refused".into(), "not today".into());
        assert_eq!(failure(run.result().timeout(LONG).await), expected);
        let run = sub.start(&parsing, ()).await.unwrap();
        let message = "parsing the count: invalid digit found in string".into();
        let expected = (RunStatus::Failed, "Error".into(), message);
        assert_eq!(
            failure(run.result().timeout(LONG).await.map(drop)),
            expected
        );
        let run = sub.start(&wrapped, ()).await.unwrap();
        let expected = (RunStatus::Cancelled, "Cancelled".into(), "enough".into());
        assert_eq!(failure(run.result().timeout(LONG).await), expected);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_send_to_a_missing_or_ended_run_raises_the_same_on_replay() {
    let quick = Workflow::without_input("quick", |_c| async { Ok(1) });
    let sender = Workflow::new("sender", |c, input: serde_json::Value| async move {
        let missing = c.send("late", "m", 1).await;
        let ended = c.send(input["done"].as_str().unwrap(), "m", 1).await;
        // Received after the test starts `late`: this replay reads both outcomes from the history.
        c.receive::<u32>("go").await?;
        Ok(match (missing, ended) {
            (
                Err(Error::RunNotFound {
                    chain,
                    collected: false,
                    ..
                }),
                Err(Error::RunEnded {
                    run,
                    status: RunStatus::Completed,
                }),
            ) => (chain, run),
            other => panic!("{other:?}"),
        })
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (quick.clone(), sender.clone()))
            .start()
            .unwrap();
        let done = sub.start(&quick, ()).id("done").await.unwrap();
        done.result().timeout(LONG).await.unwrap();
        let input = serde_json::json!({ "done": "done" });
        let run = sub.start(&sender, input).await.unwrap();
        eventually("a suspended sender", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        sub.start(&quick, ()).id("late").await.unwrap();
        run.send("go", 1).await.unwrap();
        // § b7g8bsqaq.a@d6x8b
        let refused = run.result().timeout(LONG).await.unwrap();
        assert_eq!(refused, ("late".into(), "done".into()), "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

/// Waits for a message no one sends: a worker that does not serve it leaves its inbox as sent.
fn idle() -> Workflow<u32> {
    Workflow::without_input("idle", |c| async move { c.receive::<u32>("never").await })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_send_without_an_id_gets_one_derived_from_the_call() {
    let sender = Workflow::without_input("sender", |c| async move {
        c.send("target", "m", 1).await?;
        // The replay after the sleep makes the first send again, answered from the history.
        c.sleep(std::time::Duration::from_millis(100)).await?;
        c.send("target", "m", 2).await?;
        Ok(())
    });
    let mut first: Option<Vec<String>> = None;
    for ns in namespaces() {
        let recording = Recording::new(ns.store);
        let sub = internal::create(recording.clone()).await.unwrap();
        sub.start(&idle(), ()).id("target").await.unwrap();
        let worker = script(&sub, &sender).start().unwrap();
        let run = sub.start(&sender, ()).id("s").await.unwrap();
        run.result().timeout(LONG).await.unwrap();
        worker.shutdown().await.unwrap();
        // § b7g8bsqaq.c@tg1e3
        let history = run.history().await.unwrap();
        let ids: Vec<String> = (history.iter())
            .filter(|e| e.kind == "sent")
            .map(|e| e.data["id"].as_str().unwrap().to_owned())
            .collect();
        let inbox: Vec<String> = (recording.created().iter())
            .filter(|(_, created)| *created == Create::Created)
            .filter_map(|(k, _)| k.strip_prefix("runs/target/inbox/"))
            .map(|k| k.split_once('-').unwrap().1.to_owned())
            .collect();
        assert_eq!(ids.len(), 2, "{}: {history:?}", ns.name);
        assert_ne!(ids[0], ids[1], "{}: an id per call", ns.name);
        assert_eq!(inbox, ids, "{}: each message once, under its id", ns.name);
        // The same calls of the same run, replayed afresh in another namespace: the same ids.
        assert_eq!(
            first.get_or_insert_with(|| ids.clone()),
            &ids,
            "{}",
            ns.name
        );
        // § end b7g8bsqaq.c
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn effects_created_again_are_created_once() {
    let sender = Workflow::without_input("sender", |c| async move {
        c.send("target", "m", 1).await?;
        c.receive::<()>("go").await
    });
    for ns in namespaces() {
        let store = ns.store.clone();
        let recording = Recording::new(ns.store);
        let sub = internal::create(recording.clone()).await.unwrap();
        sub.start(&idle(), ()).id("target").await.unwrap();
        let worker = script(&sub, &sender).start().unwrap();
        let run = sub.start(&sender, ()).id("s").await.unwrap();
        eventually("the send's effects", async || {
            let listed = store.list("runs/s/effects/", None, 10, None).await.unwrap();
            listed.keys == ["runs/s/effects/0000000000"]
        })
        .await;
        // As a crash after the effects, before their marker, leaves them.
        store.delete("runs/s/effects/0000000000").await.unwrap();
        run.send("go", ()).await.unwrap();
        run.result().timeout(LONG).await.unwrap();
        worker.shutdown().await.unwrap();
        // § wymbfvjjr.a@zat6c
        let created = recording.created();
        let of = |prefix: &str| -> Vec<Create> {
            (created.iter())
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(_, c)| *c)
                .collect()
        };
        let marker = of("runs/s/effects/0000000000");
        assert_eq!(marker, [Create::Created, Create::Created], "{}", ns.name);
        let message = of("runs/target/inbox/");
        assert_eq!(message, [Create::Created, Create::Exists], "{}", ns.name);
        let inbox = store
            .list("runs/target/inbox/", None, 10, None)
            .await
            .unwrap();
        assert_eq!(inbox.keys.len(), 1, "{}: {inbox:?}", ns.name);
        // § end wymbfvjjr.a
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_whose_hints_are_lost_goes_on_from_its_start_and_segments() {
    let steps = Arc::new(AtomicU32::new(0));
    let s = steps.clone();
    let hinted = Workflow::without_input("hinted", move |c| {
        let s = s.clone();
        async move {
            let n = c
                .step("count", move || {
                    let n = s.fetch_add(1, Ordering::SeqCst) + 1;
                    async move { Ok::<_, Error>(n) }
                })
                .await?;
            c.sleep(std::time::Duration::from_millis(500)).await?;
            Ok(n)
        }
    });
    for ns in namespaces() {
        steps.store(0, Ordering::SeqCst);
        let store = ns.store.clone();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &hinted).start().unwrap();
        let run = sub.start(&hinted, ()).id("h").await.unwrap();
        eventually("a suspended run", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        worker.shutdown().await.unwrap();
        // § nmpjmw8mf@v5d65
        let mut hints = Vec::new();
        for prefix in ["queues/", "index/", "runs/h/claim/"] {
            hints.extend(store.list(prefix, None, 1000, None).await.unwrap().keys);
        }
        assert!(!hints.is_empty(), "{}", ns.name);
        for key in &hints {
            store.delete(key).await.unwrap();
        }
        assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
        // A start of its id creates its due entry again; its wakeup, the timer's.
        sub.start(&hinted, ()).id("h").await.unwrap();
        let worker = script(&sub, &hinted).start().unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        assert_eq!(steps.load(Ordering::SeqCst), 1);
        // § end nmpjmw8mf
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumed_inbox_key_left_behind_is_deleted_and_never_delivered_again() {
    let counting = Workflow::without_input("counting", |c| async move {
        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s = seen.clone();
        c.handle("m", move |n: u32| s.lock().unwrap().push(n))?;
        c.receive::<()>("go").await?;
        let seen = seen.lock().unwrap().clone();
        Ok(seen)
    });
    for ns in namespaces() {
        let store = ns.store.clone();
        let inbox = async || {
            store
                .list("runs/c/inbox/", None, 10, None)
                .await
                .unwrap()
                .keys
        };
        let sub = internal::create(ns.store.clone()).await.unwrap();
        let run = sub.start(&counting, ()).id("c").await.unwrap();
        run.send("m", 1).await.unwrap();
        let [key] = inbox().await.try_into().unwrap();
        let bytes = store.get(&key).await.unwrap().unwrap();
        let worker = script(&sub, &counting).start().unwrap();
        eventually("the message consumed", async || inbox().await.is_empty()).await;
        // As a crash after the commit that consumed it, before its delete, leaves it.
        assert_eq!(store.create(&key, bytes).await.unwrap(), Create::Created);
        run.send("go", ()).await.unwrap();
        // § nmpjmw8mf.c@a37yd
        let seen = run.result().timeout(LONG).await.unwrap();
        assert_eq!(seen, [1], "{}: delivered once", ns.name);
        eventually("the key left deleted", async || inbox().await.is_empty()).await;
        // § end nmpjmw8mf.c
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_send_the_core_refused_creates_nothing() {
    // § dva6cx6nb.b@9s5bt
    let quick = Workflow::without_input("quick", |_c| async { Ok(1) });
    let refused = Workflow::without_input("refused", |c| async move {
        let missing = c.send("nobody", "m", 1).await;
        let ended = c.send("done", "m", 1).await;
        Ok((
            matches!(missing, Err(Error::RunNotFound { .. })),
            matches!(ended, Err(Error::RunEnded { .. })),
        ))
    });
    for ns in namespaces() {
        let recording = Recording::new(ns.store);
        let sub = internal::create(recording.clone()).await.unwrap();
        let worker = script(&sub, (quick.clone(), refused.clone()))
            .start()
            .unwrap();
        let done = sub.start(&quick, ()).id("done").await.unwrap();
        assert_eq!(done.result().timeout(LONG).await.unwrap(), 1);
        let mark = recording.created().len();
        let run = sub.start(&refused, ()).await.unwrap();
        let raised = run.result().timeout(LONG).await.unwrap();
        assert_eq!(raised, (true, true), "{}", ns.name);
        worker.shutdown().await.unwrap();
        let created: Vec<String> = recording.created()[mark..]
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        assert!(
            created.iter().all(|k| !k.contains("nobody")
                && !k.starts_with("runs/done/")
                && !k.contains("~done~")),
            "{}: {created:?}",
            ns.name
        );
    }
}
