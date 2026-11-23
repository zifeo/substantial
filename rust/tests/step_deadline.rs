//! `Retry { deadline }`: no attempt of a step starts past its deadline, counted from its first
//! attempt's start. The ports of `python/tests/test_step_deadline.py`, under the same names.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use substantial::testing::Env;
use substantial::{Error, Preset, Retry, Workflow, seconds};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

fn retry(deadline: Option<Duration>) -> Retry {
    Retry {
        max_attempts: 10,
        initial: seconds(1),
        max: seconds(1),
        multiplier: 1.0,
        deadline,
    }
}

#[tokio::test]
async fn no_attempt_starts_past_the_deadline() {
    let env = Env::open(t0()).await.unwrap();
    let calling = Workflow::without_input("calling", |c| async move {
        let flaky = c.step("flaky", || async {
            Err::<(), _>(std::io::Error::other("down"))
        });
        match flaky.retry(retry(Some(Duration::from_millis(2500)))).await {
            Err(Error::StepFailed { error, .. }) => Ok(error.message),
            other => Ok(format!("{other:?}")),
        }
    });
    let worker = env.worker(&calling).preset(Preset::Script).start().unwrap();
    let run = env.start(&calling, ()).await.unwrap();
    env.advance(seconds(10)).await.unwrap();
    // § s0a5x1qed@7p4wq
    let message = run.result().await.unwrap();
    assert!(message.contains("after 3 attempts"), "{message}");
    let retries: Vec<serde_json::Value> = (run.history().await.unwrap().into_iter())
        .filter(|e| e.kind == "step_retry")
        .map(|e| e.data)
        .collect();
    assert_eq!(retries.len(), 2, "{retries:?}");
    assert!(
        retries.iter().all(|r| r.get("deadline").is_some()),
        "{retries:?}"
    );
    // § end s0a5x1qed
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_deadline_is_a_positive_duration() {
    let env = Env::open(t0()).await.unwrap();
    let zero = Workflow::without_input("zero", |c| async move {
        let step = c.step("s", || async { Ok::<_, Error>(()) });
        Ok(match step.retry(retry(Some(Duration::ZERO))).await {
            Err(Error::InvalidArgument(m)) => m,
            other => format!("{other:?}"),
        })
    });
    let worker = env.worker(&zero).preset(Preset::Script).start().unwrap();
    let run = env.start(&zero, ()).await.unwrap();
    let refused = run.result().await.unwrap();
    // § s0a5x1qed.a@0md8k
    assert!(refused.contains("deadline must be positive"), "{refused}");
    // § end s0a5x1qed.a
    worker.shutdown().await.unwrap();
    env.close().await;
}
