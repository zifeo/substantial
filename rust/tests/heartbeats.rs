//! Heartbeats of step attempts (I-6): an attempt silent past its heartbeat timeout fails with a
//! retryable `StepTimeout`, one that beats goes on.

mod common;
mod shop;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::{LONG, script};
use substantial::{Context, Error, Retry, Substantial, Workflow, heartbeat, minutes};

const BEAT: Duration = Duration::from_millis(150);

/// A step that runs 500 ms, beating every 50 ms when `beats`: `ok`, or its cause's type and
/// message.
async fn working(
    c: &Context,
    name: &str,
    beats: bool,
    own: Option<Duration>,
) -> Result<String, Error> {
    let step = c.step(name, move || async move {
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if beats {
                heartbeat();
            }
        }
        Ok::<_, Error>(())
    });
    let step = match own {
        Some(t) => step.heartbeat_timeout(t),
        None => step,
    };
    match step.timeout(Duration::from_secs(5)).await {
        Ok(()) => Ok("ok".into()),
        Err(Error::StepFailed { cause, .. }) => Ok(format!("{}: {}", cause.kind, cause.message)),
        Err(e) => Err(e),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_attempt_fails_and_a_beating_one_goes_on() {
    let sub = Substantial::create("memory://heartbeats").await.unwrap();
    let own = Workflow::without_input("own", |c| async move {
        Ok((
            working(&c, "silent", false, Some(BEAT)).await?,
            working(&c, "beating", true, Some(BEAT)).await?,
            working(&c, "unset", false, None).await?,
        ))
    });
    let inherited = Workflow::without_input("inherited", |c| async move {
        Ok((
            working(&c, "silent", false, None).await?,
            working(&c, "patient", false, Some(Duration::from_secs(2))).await?,
        ))
    })
    .with_heartbeat_timeout(BEAT);
    let worker = script(&sub, (own.clone(), inherited.clone()))
        .start()
        .unwrap();
    let silent = "StepTimeout: no heartbeat within 150ms".to_owned();
    let ended = sub
        .start(&own, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    // § vvp37rp40@ngqc8
    assert_eq!(ended.unwrap(), (silent.clone(), "ok".into(), "ok".into()));
    let ended = sub
        .start(&inherited, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    assert_eq!(ended.unwrap(), (silent, "ok".into()));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_attempt_is_retried_and_an_undo_has_its_steps_heartbeat() {
    let sub = Substantial::create("memory://heartbeats-retried")
        .await
        .unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let counted = attempts.clone();
    let retried = Workflow::without_input("retried", move |c| {
        let counted = counted.clone();
        async move {
            let step = c.step("flaky", move || {
                let n = counted.fetch_add(1, Ordering::SeqCst);
                async move {
                    for _ in 0..10 {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        if n > 0 {
                            heartbeat();
                        }
                    }
                    Ok::<_, Error>(n)
                }
            });
            let retry = Retry {
                max_attempts: 2,
                initial: Duration::from_millis(10),
                max: Duration::from_millis(10),
                multiplier: 1.0,
                deadline: None,
            };
            step.retry(retry).heartbeat_timeout(BEAT).await
        }
    });
    let undone = Workflow::without_input("undone", |c| async move {
        c.step("charge", || async { Ok::<_, Error>(1) })
            .heartbeat_timeout(BEAT)
            .undo(|_: u32| async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<_, Error>(())
            })
            .await?;
        match c.rollback("refused").await {
            Error::UndoFailed { step, error } => Ok(format!("{step}: {}", error.message)),
            other => Err(other),
        }
    });
    let worker = script(&sub, (retried.clone(), undone.clone()))
        .start()
        .unwrap();
    let ended = sub
        .start(&retried, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    assert_eq!(ended.unwrap(), 1, "the second attempt beat");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let ended = sub
        .start(&undone, ())
        .await
        .unwrap()
        .result()
        .timeout(LONG)
        .await;
    // § vvp37rp40.b@en2qe
    let ended = ended.unwrap();
    assert!(ended.starts_with("charge.undo: "), "{ended}");
    assert!(ended.contains("no heartbeat within 150ms"), "{ended}");
    worker.shutdown().await.unwrap();
}

#[test]
fn a_heartbeat_outside_a_step_panics() {
    // § vvp37rp40.a@aqrhc
    assert!(std::panic::catch_unwind(heartbeat).is_err());
}

#[tokio::test]
async fn a_held_attempt_is_silent_past_its_heartbeat_timeout() {
    let env = shop::env().await;
    let flow = Workflow::without_input("held", |c| async move {
        let step = c.step("export", || async { Ok::<_, Error>(()) });
        match step
            .timeout(minutes(10))
            .heartbeat_timeout(minutes(1))
            .await
        {
            Ok(()) => Ok("ok".to_owned()),
            Err(Error::StepFailed { cause, .. }) => Ok(cause.kind),
            Err(e) => Err(e),
        }
    });
    let worker = shop::service(&env, &flow);
    let hold = env.hold("export");
    let run = env.start(&flow, ()).await.unwrap();
    env.advance(minutes(1) - Duration::from_millis(1))
        .await
        .unwrap();
    assert_eq!(hold.held(), 1, "still within its heartbeat timeout");
    env.advance(Duration::from_millis(1)).await.unwrap();
    assert_eq!(hold.held(), 0);
    assert_eq!(run.result().await.unwrap(), "StepTimeout");
    worker.shutdown().await.unwrap();
    shop::done(env).await;
}
