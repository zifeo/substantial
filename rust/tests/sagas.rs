//! Undo, rollback and sagas (I-10), end to end.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{LONG, eventually, namespaces, script};
use substantial::__internal as internal;
use substantial::{
    Context, Error, Failure, Permanent, Retry, RunStatus, Step, Substantial, Workflow,
};

type Log = Arc<Mutex<Vec<String>>>;

/// A step returning `name` whose undo logs what it was given.
fn undoable(c: &Context, log: &Log, name: &'static str) -> Step<String> {
    let log = log.clone();
    c.step(name, move || async move { Ok::<_, Error>(name.to_owned()) })
        .undo(move |value: String| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push(value);
                Ok::<_, Error>(())
            }
        })
}

async fn rolled_back(sub: &Substantial, w: &Workflow<()>) -> Failure {
    let worker = script(sub, w).start().unwrap();
    let run = sub.start(w, ()).await.unwrap();
    let result = run.result().timeout(LONG).await;
    worker.shutdown().await.unwrap();
    match result {
        Err(Error::RunFailed {
            status: RunStatus::Failed,
            error,
            ..
        }) => *error,
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollback_undoes_every_step_most_recent_first() {
    let log = Log::default();
    let l = log.clone();
    let w = Workflow::without_input("rolling", move |c| {
        let log = l.clone();
        async move {
            undoable(&c, &log, "a").await?;
            c.step("plain", || async { Ok::<_, Error>(()) }).await?;
            // A replay after the sleep binds the undos again from the recorded steps.
            c.sleep(Duration::from_millis(100)).await?;
            undoable(&c, &log, "c").await?;
            Err::<(), _>(c.rollback("why").await)
        }
    });
    for ns in namespaces() {
        log.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let error = rolled_back(&sub, &w).await;
        assert_eq!(
            (error.kind.as_str(), error.message.as_str()),
            ("RolledBack", "why")
        );
        // § ej5k8q6w5@0gse6
        assert_eq!(*log.lock().unwrap(), ["c", "a"], "{}", ns.name);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Round {
    round: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollback_leaves_the_earlier_runs_of_the_chain() {
    let log = Log::default();
    let l = log.clone();
    let w = Workflow::new("continued", move |c: Context, input: Round| {
        let log = l.clone();
        async move {
            if input.round == 0 {
                undoable(&c, &log, "earlier").await?;
                return Err(c.continue_as_new(Round { round: 1 }).await);
            }
            undoable(&c, &log, "current").await?;
            Err::<(), _>(c.rollback("why").await)
        }
    });
    for ns in namespaces() {
        log.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &w).start().unwrap();
        let run = sub.start(&w, Round { round: 0 }).await.unwrap();
        let result = run.result().timeout(LONG).await;
        worker.shutdown().await.unwrap();
        assert!(
            matches!(&result, Err(Error::RunFailed { error, .. }) if error.kind == "RolledBack"),
            "{}: {result:?}",
            ns.name
        );
        // § one ej5k8q6w5.a@0pxgz
        assert_eq!(*log.lock().unwrap(), ["current"], "{}", ns.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saga_undoes_its_own_steps_then_raises_again() {
    let log = Log::default();
    let l = log.clone();
    let w = Workflow::without_input("saga", move |c| {
        let log = l.clone();
        async move {
            undoable(&c, &log, "a").await?;
            let failed = c
                .saga(|c| async move {
                    undoable(&c, &log, "b").await?;
                    undoable(&c, &log, "c").await?;
                    c.step("boom", || async {
                        Err::<(), _>(Permanent::new("no stock"))
                    })
                    .await
                })
                .await;
            assert!(
                matches!(failed, Err(Error::StepFailed { .. })),
                "{failed:?}"
            );
            c.saga(|c| async move { c.step("fine", || async { Ok::<_, Error>(()) }).await })
                .await?;
            Err::<(), _>(c.rollback("after the saga").await)
        }
    });
    for ns in namespaces() {
        log.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let error = rolled_back(&sub, &w).await;
        assert_eq!(error.kind, "RolledBack", "{}", ns.name);
        // § sft633bnn@6nb24
        assert_eq!(*log.lock().unwrap(), ["c", "b", "a"], "{}", ns.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undo_without_attempts_left_fails_the_run() {
    let w = Workflow::without_input("stuck", |c| async move {
        c.step("charge", || async { Ok::<_, Error>(1) })
            .undo(|_: u32| async { Err::<(), _>(Permanent::new("refund refused")) })
            .await?;
        Err::<(), _>(c.rollback("fraud").await)
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let error = rolled_back(&sub, &w).await;
        assert_eq!(error.kind, "UndoFailed", "{}: {error}", ns.name);
        // § dzbz2dz7v@xae3t
        assert!(
            error.message.contains("charge.undo"),
            "{}: {error}",
            ns.name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saga_that_succeeds_leaves_its_steps_to_a_rollback() {
    let log = Log::default();
    let l = log.clone();
    let w = Workflow::without_input("kept", move |c| {
        let log = l.clone();
        async move {
            undoable(&c, &log, "a").await?;
            let inner = log.clone();
            let (value, scoped) = c
                .saga(|c| async move { Ok((undoable(&c, &inner, "b").await?, c)) })
                .await?;
            assert_eq!(value, "b");
            // § sft633bnn.b@bpmeb
            let late = scoped.step("late", || async { Ok::<_, Error>(()) }).await;
            assert!(matches!(late, Err(Error::InvalidArgument(_))), "{late:?}");
            // § end sft633bnn.b
            Err::<(), _>(c.rollback("after").await)
        }
    });
    for ns in namespaces() {
        log.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let error = rolled_back(&sub, &w).await;
        assert_eq!(error.kind, "RolledBack", "{}: {error}", ns.name);
        // § sft633bnn.a@f5bq2
        assert_eq!(*log.lock().unwrap(), ["b", "a"], "{}", ns.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undo_has_the_step_key_of_its_occurrence() {
    let keys = Log::default();
    let k = keys.clone();
    let w = Workflow::without_input("keyed-undo", move |c| {
        let keys = k.clone();
        async move {
            for _ in 0..2 {
                let keys = keys.clone();
                c.step("charge", || async { Ok::<_, Error>(()) })
                    .undo(move |_: ()| {
                        let keys = keys.clone();
                        async move {
                            keys.lock().unwrap().push(substantial::step_key());
                            Ok::<_, Error>(())
                        }
                    })
                    .await?;
            }
            Err::<(), _>(c.rollback("undo").await)
        }
    });
    for ns in namespaces() {
        keys.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &w).start().unwrap();
        let run = sub.start(&w, ()).id("undone").await.unwrap();
        assert!(run.result().timeout(LONG).await.is_err());
        worker.shutdown().await.unwrap();
        let keys = keys.lock().unwrap().clone();
        let expected = ["undone/charge.undo/1", "undone/charge.undo/0"];
        assert_eq!(keys, expected, "{}", ns.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inner_saga_ended_hands_its_steps_to_the_outer_one() {
    let log = Log::default();
    let l = log.clone();
    let w = Workflow::without_input("nested", move |c| {
        let log = l.clone();
        async move {
            undoable(&c, &log, "z").await?;
            let outer = log.clone();
            let failed = c
                .saga(|c| async move {
                    undoable(&c, &outer, "a").await?;
                    let inner = outer.clone();
                    c.saga(|c| async move { undoable(&c, &inner, "b").await })
                        .await?;
                    undoable(&c, &outer, "c").await?;
                    c.step("boom", || async {
                        Err::<(), _>(Permanent::new("no stock"))
                    })
                    .await
                })
                .await;
            assert!(
                matches!(failed, Err(Error::StepFailed { .. })),
                "{failed:?}"
            );
            Err::<(), _>(c.rollback("after").await)
        }
    });
    for ns in namespaces() {
        log.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let error = rolled_back(&sub, &w).await;
        assert_eq!(error.kind, "RolledBack", "{}", ns.name);
        assert_eq!(*log.lock().unwrap(), ["c", "b", "a", "z"], "{}", ns.name);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_waiting_for_its_retry_raises_cancelled_and_its_undos_run() {
    let log = Log::default();
    let l = log.clone();
    let w = Workflow::without_input("retrying", move |c| {
        let log = l.clone();
        async move {
            undoable(&c, &log, "a").await?;
            let retry = Retry {
                max_attempts: 3,
                initial: Duration::from_secs(600),
                max: Duration::from_secs(600),
                multiplier: 1.0,
            };
            let waited = c
                .step("down", || async {
                    Err::<(), _>(std::io::Error::other("provider down"))
                })
                .retry(retry)
                .await;
            match waited {
                Err(Error::Cancelled(_)) => Err(c.rollback("cancelled").await),
                other => other,
            }
        }
    });
    for ns in namespaces() {
        log.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &w).start().unwrap();
        let run = sub.start(&w, ()).await.unwrap();
        // § vmk2ydwta.b@xywc4
        eventually("the step waits for its retry", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        run.cancel("not needed").await.unwrap();
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                ..
            }) => {}
            other => panic!("{}: {other:?}", ns.name),
        }
        assert_eq!(*log.lock().unwrap(), ["a"], "{}: the undo ran", ns.name);
        // § end vmk2ydwta.b
        worker.shutdown().await.unwrap();
    }
}
