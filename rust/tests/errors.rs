//! The messages of the errors a user meets first: they state the rule that was broken.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::{Counting, Interrupted, LONG, script};
use substantial::__internal::{self as internal, MemoryStore};
use substantial::{Context, Error, Failure, Permanent, Preset, Retry, Substantial, Workflow};

fn config_error(r: Result<Substantial, Error>) -> String {
    match r {
        Err(Error::ConfigError(m)) => m,
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("opened"),
    }
}

#[tokio::test]
async fn create_states_what_is_missing_or_unknown() {
    let missing = config_error(Substantial::create("memory://errors").await);
    assert_eq!(missing, "create needs partitions, a positive whole number");
    for url in ["file//tmp/x", "/tmp/x", "ftp://host/x"] {
        let m = config_error(Substantial::create(url).partitions(1).await);
        assert!(m.starts_with("unknown scheme"), "{m}");
        assert!(m.contains("memory://, file:, s3://"), "{m}");
    }
    for url in ["sqlite:x.db", "postgres://u@h/db"] {
        let m = config_error(Substantial::create(url).partitions(1).await);
        assert!(m.contains("not available in this host yet"), "{m}");
    }
    let home = config_error(Substantial::create("file:~/x").partitions(1).await);
    assert!(home.contains("give an absolute or relative path"), "{home}");
}

#[tokio::test]
async fn names_outside_the_rules_state_the_rule() {
    let sub = Substantial::create("memory://errors-names")
        .partitions(1)
        .await
        .unwrap();
    let w = Workflow::without_input("named", |_c| async { Ok(1) });
    let started = sub.start(&w, ()).id("Bad Id").await.unwrap_err();
    assert_eq!(
        started.to_string(),
        r#"invalid argument: run id "Bad Id" must match [a-z0-9._-]{1,100}, . and .. excepted, and not start with substantial."#
    );
    let upper = Workflow::without_input("Upper", |_c| async { Ok(1) });
    let named = sub.start(&upper, ()).await.unwrap_err().to_string();
    assert!(
        named.contains(r#"workflow name "Upper" must match"#),
        "{named}"
    );
    let run = sub.start(&w, ()).id("fine").await.unwrap();
    let sent = run.send("a b", ()).await.unwrap_err().to_string();
    assert!(sent.ends_with("must match [A-Za-z0-9._-]{1,128}"), "{sent}");
    let id = run.send("x", ()).id("A").await.unwrap_err().to_string();
    assert!(id.contains(r#"message id "A" must match"#), "{id}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_carry_no_doubled_prefix() {
    let sub = Substantial::create("memory://errors-prefix")
        .partitions(1)
        .await
        .unwrap();
    let bad = sub
        .worker(Workflow::without_input("w", |_c| async { Ok(1) }))
        .preset(Preset::Script)
        .set("max_idle", "1 second")
        .start()
        .map(drop)
        .unwrap_err();
    assert_eq!(
        bad.to_string(),
        r#"config error: setting max_idle: "1 second" is not a duration with a unit (ms, s, min, h, d)"#
    );
    let once = Retry {
        max_attempts: 1,
        initial: LONG,
        max: LONG,
        multiplier: 1.0,
    };
    let failing = Workflow::without_input("failing", move |c| async move {
        let step = c.step("boom", || async {
            Err::<(), _>(std::io::Error::other("boom"))
        });
        match step.retry(once).await {
            Err(e @ Error::StepFailed { .. }) => Ok(e.to_string()),
            other => Ok(format!("unexpected {other:?}")),
        }
    });
    let worker = script(&sub, &failing).start().unwrap();
    let run = sub.start(&failing, ()).await.unwrap();
    let message = run.result().timeout(LONG).await.unwrap();
    assert!(
        message.starts_with("step boom failed after 1 attempt"),
        "{message}"
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_worker_refuses_the_settings_it_does_not_take() {
    let sub = Substantial::create("memory://errors-settings")
        .partitions(1)
        .await
        .unwrap();
    let w = Workflow::without_input("w", |_c| async { Ok(1) });
    let hidden = script(&sub, &w)
        .set("due_pages", 20)
        .set("concurrency", 2)
        .set("snapshot_grace", "1h")
        .start()
        .map(drop)
        .unwrap_err();
    assert_eq!(
        hidden.to_string(),
        r#"config error: unknown settings ["due_pages", "snapshot_grace"]"#
    );
    for name in [
        "commit_every",
        "claim_skew",
        "wait",
        "due_page",
        "snapshot_every",
        "dedup_window",
        "store_retry",
    ] {
        let refused = script(&sub, &w).set(name, 1).start().map(drop);
        assert!(matches!(refused, Err(Error::ConfigError(_))), "{name}");
    }
}

const MIB_64: usize = 64 * 1024 * 1024;

fn too_large(e: &Error) -> bool {
    matches!(e, Error::ValueTooLarge { limit: MIB_64, .. })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn values_over_64_mib_are_refused() {
    let store = Counting::new(
        Arc::new(MemoryStore::open("errors-large").unwrap()),
        Duration::ZERO,
    );
    let sub = internal::create(store.clone()).partitions(1).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let counted = attempts.clone();
    let large = Workflow::new("large", move |c, input: serde_json::Value| {
        let attempts = counted.clone();
        async move {
            match input["what"].as_str().unwrap_or_default() {
                "publish" => match c.publish("p", "x".repeat(MIB_64)) {
                    Err(e) if too_large(&e) => Ok(e.to_string()),
                    other => Ok(format!("unexpected {other:?}")),
                },
                "step" => {
                    let big = c.step("big", move || {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        async { Ok::<_, Error>("x".repeat(MIB_64)) }
                    });
                    match big.await {
                        Err(Error::StepFailed { error, cause }) => Ok(format!(
                            "{} {}: {}",
                            cause.kind, cause.retryable, error.message
                        )),
                        other => Ok(format!("unexpected {other:?}")),
                    }
                }
                _ => Ok("x".repeat(MIB_64)),
            }
        }
    });
    let mark = store.mark();
    let big = serde_json::json!({ "v": "x".repeat(MIB_64) });
    let started = sub.start(&large, big).await.unwrap_err();
    assert!(too_large(&started), "{started:?}");
    assert_eq!(
        started.to_string(),
        format!(
            "a value of {} bytes is over the limit of {MIB_64} bytes",
            MIB_64 + 8
        )
    );
    let run = sub.run(&large, "nobody");
    let sent = run.send("m", "x".repeat(MIB_64)).await.unwrap_err();
    assert!(too_large(&sent), "{sent:?}");
    assert_eq!(
        store.since(mark).len(),
        0,
        "refused before any store operation"
    );

    let worker = script(&sub, &large).start().unwrap();
    let published = sub
        .start(&large, serde_json::json!({ "what": "publish" }))
        .await
        .unwrap();
    let message = published.result().timeout(LONG).await.unwrap();
    assert!(message.starts_with("a value of"), "{message}");
    let stepped = sub
        .start(&large, serde_json::json!({ "what": "step" }))
        .await
        .unwrap();
    let message = stepped.result().timeout(LONG).await.unwrap();
    let expected =
        "ValueTooLarge false: step big failed after 1 attempt: a value of 67108866 bytes";
    assert!(message.starts_with(expected), "{message}");
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "never retried");
    let returned = sub
        .start(&large, serde_json::json!({ "what": "result" }))
        .await
        .unwrap();
    match returned.result().timeout(LONG).await {
        Err(Error::RunFailed { error, .. }) => assert_eq!(error.kind, "ValueTooLarge"),
        other => panic!("{other:?}"),
    }
    worker.shutdown().await.unwrap();
}

/// How a step of `sleep` that sets `own` as its timeout ended: `ok`, or its cause's type.
async fn sleeper(c: &Context, sleep: Duration, own: Option<Duration>) -> Result<String, Error> {
    let step = c.step("sleeper", move || async move {
        tokio::time::sleep(sleep).await;
        Ok::<_, Error>(())
    });
    let step = match own {
        Some(t) => step.timeout(t),
        None => step,
    };
    match step.await {
        Ok(()) => Ok("ok".into()),
        Err(Error::StepFailed { cause, .. }) => Ok(cause.kind),
        Err(e) => Err(e),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_step_timeout_is_its_own_else_its_workflows_else_the_workers() {
    let sub = Substantial::create("memory://errors-timeouts")
        .partitions(1)
        .await
        .unwrap();
    let (short, slow, long) = (
        Duration::from_millis(100),
        Duration::from_millis(500),
        Duration::from_secs(5),
    );
    let plain = Workflow::without_input("plain", move |c| async move {
        Ok((
            sleeper(&c, slow, None).await?,
            sleeper(&c, slow, Some(long)).await?,
        ))
    });
    let patient = Workflow::without_input("patient", move |c| async move {
        Ok((
            sleeper(&c, slow, None).await?,
            sleeper(&c, slow, Some(short)).await?,
        ))
    })
    .with_step_timeout(long);
    let worker = script(&sub, (plain.clone(), patient.clone()))
        .set("step_timeout", "200ms")
        .start()
        .unwrap();
    let run = sub.start(&plain, ()).await.unwrap();
    let ended = run.result().timeout(LONG).await.unwrap();
    assert_eq!(
        ended,
        ("StepTimeout".into(), "ok".into()),
        "worker's, own larger"
    );
    let run = sub.start(&patient, ()).await.unwrap();
    let ended = run.result().timeout(LONG).await.unwrap();
    assert_eq!(
        ended,
        ("ok".into(), "StepTimeout".into()),
        "workflow's, own smaller"
    );
    worker.shutdown().await.unwrap();
}

#[test]
#[should_panic(expected = "step_timeout must be positive")]
fn a_zero_workflow_step_timeout_is_refused_at_its_declaration() {
    let _ = Workflow::without_input("zero", |_c| async { Ok(1) }).with_step_timeout(Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_permanent_error_is_the_cause_of_its_step_failure() {
    let sub = Substantial::create("memory://errors-permanent")
        .partitions(1)
        .await
        .unwrap();
    let w = Workflow::without_input("permanent", |c| async move {
        let step = c.step("refused", || async {
            Err::<(), _>(Permanent::new(std::io::Error::other("card declined")))
        });
        match step.await {
            Err(Error::StepFailed { cause, .. }) => {
                Ok((cause.kind, cause.message, cause.retryable))
            }
            other => Err(Error::failed("Unexpected", format!("{other:?}"))),
        }
    });
    let worker = script(&sub, &w).start().unwrap();
    let run = sub.start(&w, ()).await.unwrap();
    let cause = run.result().timeout(LONG).await.unwrap();
    assert_eq!(cause, ("Error".into(), "card declined".into(), false));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_run_holds_the_cause_of_the_step_that_failed_it() {
    let sub = Substantial::create("memory://errors-causes")
        .partitions(1)
        .await
        .unwrap();
    let paying = Workflow::without_input("paying", |c| async move {
        c.step("charge", || async {
            Err::<(), _>(Permanent::new(std::io::Error::other("card declined")))
        })
        .await
    });
    let refusing = Workflow::without_input("refusing", |_c| async move {
        Err::<(), _>(Error::failed("Declined", "not today"))
    });
    let child = paying.clone();
    let ordering = Workflow::without_input("ordering", move |c| {
        let child = child.clone();
        async move {
            let started = c.start(&child, ()).await?;
            match started.result().await {
                Err(Error::RunFailed {
                    error,
                    cause: Some(cause),
                    ..
                }) => Ok((error.kind, cause.kind, cause.message, cause.retryable)),
                other => Err(Error::failed("Unexpected", format!("{other:?}"))),
            }
        }
    });
    let worker = script(&sub, (&paying, &refusing, &ordering))
        .start()
        .unwrap();
    let paid = sub
        .start(&paying, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    let Err(Error::RunFailed { error, cause, .. }) = paid else {
        panic!("{paid:?}")
    };
    assert_eq!(error.kind, "StepFailed");
    let declined = Failure {
        kind: "Error".into(),
        message: "card declined".into(),
        retryable: false,
    };
    assert_eq!(cause, Some(Box::new(declined)));
    let refused = sub
        .start(&refusing, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    let Err(Error::RunFailed { error, cause, .. }) = refused else {
        panic!("{refused:?}")
    };
    assert_eq!(error.kind, "Declined");
    assert_eq!(cause, None);
    let ordered = sub
        .start(&ordering, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    let expected = (
        "StepFailed".into(),
        "Error".into(),
        "card declined".into(),
        false,
    );
    assert_eq!(ordered.unwrap(), expected);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_async_step_is_interrupted() {
    let sub = Substantial::create("memory://errors-hang")
        .partitions(1)
        .await
        .unwrap();
    let reached: Arc<std::sync::Mutex<Vec<&str>>> = Arc::default();
    let r = reached.clone();
    let hanging = Workflow::without_input("hanging", move |c| {
        let r = r.clone();
        async move {
            let hang = c.step("hang", move || {
                let r = r.clone();
                async move {
                    let interrupted = Interrupted(r.clone());
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    std::mem::forget(interrupted);
                    r.lock().unwrap().push("after the await");
                    Ok::<_, Error>(())
                }
            });
            match hang.timeout(Duration::from_millis(200)).await {
                Err(Error::StepFailed { cause, .. }) => Ok(cause.kind),
                other => Err(Error::failed("Unexpected", format!("{other:?}"))),
            }
        }
    });
    let worker = script(&sub, &hanging).start().unwrap();
    let ended = sub
        .start(&hanging, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    assert_eq!(ended.unwrap(), "StepTimeout");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(*reached.lock().unwrap(), ["interrupted"]);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uninterruptible_step_runs_to_its_end_with_its_outcome_dropped() {
    let sub = Substantial::create("memory://errors-block")
        .partitions(1)
        .await
        .unwrap();
    let finished = Arc::new(AtomicU32::new(0));
    let f = finished.clone();
    let blocking = Workflow::without_input("blocking", move |c| {
        let f = f.clone();
        async move {
            c.step_blocking("block", move || {
                std::thread::sleep(Duration::from_millis(400));
                f.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>("late".to_owned())
            })
            .timeout(Duration::from_millis(200))
            .await
        }
    });
    let worker = script(&sub, &blocking).start().unwrap();
    let ended = sub
        .start(&blocking, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    let Err(Error::RunFailed { cause, .. }) = ended else {
        panic!("{ended:?}")
    };
    assert_eq!(cause.map(|c| c.kind).as_deref(), Some("StepTimeout"));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    worker.shutdown().await.unwrap();
}
