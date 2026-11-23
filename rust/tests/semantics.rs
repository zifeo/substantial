//! Time, step keys, handlers, sends, cancellation from inside, publishing and reading, end to end.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use common::{LONG, Recording, blocked, eventually, namespaces, outcome, script};
use prost::Message;
use substantial::__internal::{self as internal, Cas, Create, layout};
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
        // § rbwstdetg.a@r7vgg
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
        // § dnxwdv0ac@jyr3p
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
        // § end dnxwdv0ac
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
        // § n4zyj94mr@gfqaa
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
        // § vmk2ydwta.d@t5d08
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
        // § fx1tv8nqp@msbtp
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
async fn a_published_value_is_read_while_its_run_waits() {
    let waiting = Workflow::without_input("waiting_publisher", |c: Context| async move {
        c.publish("p", 1)?;
        c.receive::<u32>("go").await
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &waiting).start().unwrap();
        let run = sub.start(&waiting, ()).await.unwrap();
        // § fx1tv8nqp.a@fw8az
        eventually("the published value", async || {
            run.read::<u32>("p").await.unwrap() == Some(1)
        })
        .await;
        assert_eq!(
            run.status().await.unwrap(),
            RunStatus::Suspended,
            "{}",
            ns.name
        );
        // § end fx1tv8nqp.a
        run.send("go", 2).await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 2, "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_fails_with_fail_and_blocks_on_its_other_errors() {
    use anyhow::Context as _;
    let own = Workflow::without_input("own", |_c| async move {
        Err::<(), _>(Error::fail("Refused", "not today"))
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
        // § dxgjekysz.d@g4c7x
        let run = sub.start(&parsing, ()).await.unwrap();
        let error = blocked(&sub, &parsing, run.chain()).await;
        let message = "parsing the count: invalid digit found in string";
        assert_eq!(
            (error.kind.as_str(), error.message.as_str()),
            ("Error", message)
        );
        // § end dxgjekysz.d
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
                Err(Error::RunNotFound { chain }),
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
        // § b7g8bsqaq.a@8tby8
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
        // § b7g8bsqaq.c@tgv3r
        let history = run.history().await.unwrap();
        let ids: Vec<String> = (history.iter())
            .filter(|e| e.kind == "sent")
            .map(|e| e.data["id"].as_str().unwrap().to_owned())
            .collect();
        let inbox: Vec<String> = (recording.created().iter())
            .filter(|(_, created)| matches!(created, Create::Created { .. }))
            .filter_map(|(k, _)| k.strip_prefix("queues/idle/due/19700101t000000000z~target~m"))
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
        let messages = "queues/idle/due/19700101t000000000z~target~m";
        let head = async || {
            let found = store.get("runs/s").await.unwrap().unwrap();
            match layout::Run::decode(found.bytes.clone()).unwrap().kind {
                Some(layout::run::Kind::Head(head)) => (head, found.etag),
                other => panic!("runs/s: {other:?}"),
            }
        };
        eventually("the send's effects", async || {
            let listed = store.list(messages, None, 10, None).await.unwrap();
            listed.keys.len() == 1 && head().await.0.pending.is_none()
        })
        .await;
        // As a crash after the commit's write, before its effects, leaves the head.
        let (mut pending, etag) = head().await;
        pending.pending = Some(pending.segments.len() as u64 - 1);
        let bytes = layout::Run {
            kind: Some(layout::run::Kind::Head(pending)),
        }
        .encode_to_vec();
        let written = store
            .cas("runs/s", bytes.into(), &etag, None)
            .await
            .unwrap();
        assert!(matches!(written, Cas::Written { .. }), "{}", ns.name);
        run.send("go", ()).await.unwrap();
        run.result().timeout(LONG).await.unwrap();
        worker.shutdown().await.unwrap();
        // § kkcyc97cz.a@ppr4z
        let created = recording.created();
        let of = |prefix: &str| -> Vec<&str> {
            (created.iter())
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(_, c)| outcome(c))
                .collect()
        };
        let message = of(messages);
        assert_eq!(message, ["created", "exists"], "{}", ns.name);
        let inbox = store.list(messages, None, 10, None).await.unwrap();
        assert_eq!(inbox.keys.len(), 1, "{}: {inbox:?}", ns.name);
        // § end kkcyc97cz.a
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_whose_hints_are_lost_goes_on_from_its_head() {
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
        // § rbvr70kyv@c99jq
        let mut hints = Vec::new();
        for prefix in ["queues/", "watch/"] {
            hints.extend(store.list(prefix, None, 1000, None).await.unwrap().keys);
        }
        assert!(!hints.is_empty(), "{}", ns.name);
        for key in &hints {
            store.delete(key).await.unwrap();
        }
        assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
        // A message wakes it: the head shows its timer, whose due entry its wakeup creates again.
        run.send("nudge", ()).await.unwrap();
        let worker = script(&sub, &hinted).start().unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        assert_eq!(steps.load(Ordering::SeqCst), 1);
        // § end rbvr70kyv
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
                .list(
                    "queues/counting/due/19700101t000000000z~c~m",
                    None,
                    10,
                    None,
                )
                .await
                .unwrap()
                .keys
        };
        let sub = internal::create(ns.store.clone()).await.unwrap();
        let run = sub.start(&counting, ()).id("c").await.unwrap();
        run.send("m", 1).await.unwrap();
        let [key] = inbox().await.try_into().unwrap();
        let bytes = store.get(&key).await.unwrap().unwrap().bytes;
        let worker = script(&sub, &counting).start().unwrap();
        eventually("the message consumed", async || inbox().await.is_empty()).await;
        // As a crash after the commit that consumed it, before its delete, leaves it.
        let recreated = store.create(&key, bytes, None).await.unwrap();
        assert!(matches!(recreated, Create::Created { .. }));
        run.send("go", ()).await.unwrap();
        // § rbvr70kyv.c@psfed
        let seen = run.result().timeout(LONG).await.unwrap();
        assert_eq!(seen, [1], "{}: delivered once", ns.name);
        eventually("the key left deleted", async || inbox().await.is_empty()).await;
        // § end rbvr70kyv.c
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_send_the_core_refused_creates_nothing() {
    // § dva6cx6nb.b@4keze
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
        // The store answers the write before the recording records it.
        eventually("done's end recorded", async || {
            recording.cased().iter().any(|(k, _)| k == "runs/done")
        })
        .await;
        let (mark, cased) = (recording.created().len(), recording.cased().len());
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
                && !k.starts_with("runs/done")
                && !k.contains("~done~")),
            "{}: {created:?}",
            ns.name
        );
        let written = &recording.cased()[cased..];
        assert!(
            written.iter().all(|(k, _)| k != "runs/done"),
            "{}: {written:?}",
            ns.name
        );
    }
}
