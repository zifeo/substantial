//! Time, step keys, handlers, sends, cancellation from inside, publishing and reading, end to end.

mod common;

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use common::{LONG, eventually, namespaces, script};
use substantial::__internal as internal;
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let reserved = c.receive::<String>("substantial.ended").await;
        if !matches!(reserved, Err(Error::InvalidArgument(_))) {
            return Err(Error::InvalidArgument(format!("receive: {reserved:?}")));
        }
        let reserved = c.handle("substantial.ended", |_: serde_json::Value| {});
        if !matches!(reserved, Err(Error::InvalidArgument(_))) {
            return Err(Error::InvalidArgument(format!("handle: {reserved:?}")));
        }
        c.receive::<String>("note").await
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handler_sees_past_messages_first_and_a_workflow_sends() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let run = sub.start(&ordered, ()).await.unwrap();
        for (name, n) in [("add", 1), ("remove", 1), ("add", 2), ("remove", 2)] {
            run.send(name, n).await.unwrap();
        }
        run.send("go", ()).await.unwrap();
        let worker = script(&sub, &ordered).start().unwrap();
        let (all, adds): (Vec<String>, Vec<u32>) = run.result().timeout(LONG).await.unwrap();
        assert_eq!(
            all,
            ["add:1", "remove:1", "add:2", "remove:2"],
            "{}",
            ns.name
        );
        assert_eq!(adds, [1, 2], "{}", ns.name);
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, &publisher).start().unwrap();
        let run = sub.start(&publisher, Round { round: 0 }).await.unwrap();
        let refused: bool = run.result().timeout(LONG).await.unwrap();
        assert!(refused, "{}: a key outside the rules", ns.name);
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let refused = run.result().timeout(LONG).await.unwrap();
        assert_eq!(refused, ("late".into(), "done".into()), "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}
