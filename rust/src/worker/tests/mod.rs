//! Replays of hand-built `RunInput`s, the test playing the agent machine's part in memory.

mod harness;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness::{Harness, completed, counter, counting_step, failed};

use crate::proto::boundary::v1::{CommitKind, replay_end};
use crate::proto::layout::v1::{self as layout, event::Kind};
use crate::workflow::json;
use crate::{Context, Error, Permanent, Result, Retry, Workflow, seconds};

#[tokio::test]
async fn a_step_then_the_end() {
    let runs = counter();
    let r = runs.clone();
    let mut h = Harness::new(Workflow::without_input("double", move |c| {
        let r = r.clone();
        async move { Ok(counting_step(&c, "five", &r, 5).await? * 2) }
    }));
    let replayed = h.replay(1_000).await;
    // The code ran on to its end before the next `next`: one commit records both.
    assert_eq!(replayed.kinds(), [CommitKind::End]);
    assert!(matches!(
        replayed.events()[..],
        [Kind::Step(_), Kind::Completed(_)]
    ));
    assert_eq!(completed(&replayed), Some(serde_json::json!(10)));
    assert_eq!(replayed.next_wake(), None);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_replay_returns_recorded_steps_and_fires_sleeps() {
    let (a, b) = (counter(), counter());
    let (ra, rb) = (a.clone(), b.clone());
    let mut h = Harness::new(Workflow::without_input("sleepy", move |c| {
        let (ra, rb) = (ra.clone(), rb.clone());
        async move {
            let x = counting_step(&c, "a", &ra, 1).await?;
            c.sleep(seconds(10)).await?;
            Ok(x + counting_step(&c, "b", &rb, 2).await?)
        }
    }));
    let first = h.replay(1_000).await;
    assert!(
        matches!(first.events()[..], [Kind::Step(_), Kind::Timer(_)]),
        "{:?}",
        first.events()
    );
    assert_eq!(first.next_wake(), Some(11_000));
    let early = h.replay(5_000).await;
    assert!(
        early.commits.is_empty(),
        "nothing to record before the timer"
    );
    let second = h.replay(11_000).await;
    assert_eq!(completed(&second), Some(serde_json::json!(3)));
    assert_eq!((a.load(Ordering::SeqCst), b.load(Ordering::SeqCst)), (1, 1));
}

#[tokio::test]
async fn receive_takes_the_next_message_of_its_name() {
    let mut h = Harness::new(Workflow::without_input("receiver", |c| async move {
        let n: i64 = c.receive("by").await?;
        let m: i64 = c.receive("by").await?;
        Ok(n * m)
    }));
    h.message("k1", "other", "0");
    h.message("k2", "by", "6");
    let first = h.replay(1_000).await;
    assert_eq!(first.next_wake(), None);
    assert!(completed(&first).is_none());
    h.message("k3", "by", "7");
    let second = h.replay(2_000).await;
    assert_eq!(completed(&second), Some(serde_json::json!(42)));
    assert!(h.inbox.is_empty(), "every message consumed: {:?}", h.inbox);
}

#[tokio::test]
async fn handlers_see_the_past_and_wait_until_ends_at_once() {
    let mut h = Harness::new(Workflow::without_input("stopper", |c| async move {
        let stopped = Arc::new(Mutex::new(false));
        let notes = Arc::new(Mutex::new(Vec::<String>::new()));
        let n = notes.clone();
        c.handle("note", move |v: String| n.lock().unwrap().push(v))?;
        let n: i64 = c.receive("by").await?;
        let s = stopped.clone();
        c.handle("stop", move |_: ()| *s.lock().unwrap() = true)?;
        let s = stopped.clone();
        assert!(c.wait_until(move || *s.lock().unwrap()).await?);
        let notes = notes.lock().unwrap().clone();
        Ok((n, notes))
    }));
    h.message("k1", "note", "\"a\"");
    h.message("k2", "by", "2");
    h.message("k3", "stop", "null");
    h.message("k4", "note", "\"b\"");
    let replayed = h.replay(1_000).await;
    // The wait held once "stop" was delivered, so the code ended before the last note.
    assert_eq!(completed(&replayed), Some(serde_json::json!([2, ["a"]])));
}

#[tokio::test]
async fn wait_until_times_out_to_false() {
    let mut h = Harness::new(Workflow::without_input("patient", |c| async move {
        c.wait_until(|| false).timeout(seconds(5)).await
    }));
    assert_eq!(h.replay(1_000).await.next_wake(), Some(6_000));
    assert_eq!(
        completed(&h.replay(6_000).await),
        Some(serde_json::json!(false))
    );
}

const RETRY: Retry = Retry {
    max_attempts: 3,
    initial: Duration::from_secs(1),
    max: Duration::from_secs(4),
    multiplier: 2.0,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn flaky(
    c: &Context,
    runs: &Arc<AtomicU32>,
    fail_first: u32,
    permanent: bool,
) -> crate::Step<String> {
    let runs = runs.clone();
    c.step("flaky", move || {
        let runs = runs.clone();
        async move {
            let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
            match n <= fail_first {
                true if permanent => Err(Box::new(Permanent::new("gone")) as BoxError),
                true => Err(format!("attempt {n}").into()),
                false => Ok(format!("ok after {n}")),
            }
        }
    })
}

#[tokio::test]
async fn a_failed_attempt_backs_off_then_retries() {
    let runs = counter();
    let r = runs.clone();
    let mut h = Harness::new(Workflow::without_input("retrying", move |c| {
        let r = r.clone();
        async move { flaky(&c, &r, 1, false).retry(RETRY).await }
    }));
    let first = h.replay(1_000).await;
    assert!(
        matches!(first.events()[..], [Kind::StepRetry(_)]),
        "{:?}",
        first.events()
    );
    let wake = first.next_wake().unwrap();
    assert!(wake > 1_000);
    let second = h.replay(wake).await;
    assert_eq!(completed(&second), Some(serde_json::json!("ok after 2")));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn steps_without_retry_or_with_a_permanent_error_fail_at_once() {
    for (permanent, retry) in [(false, None), (true, Some(RETRY))] {
        let runs = counter();
        let r = runs.clone();
        let mut h = Harness::new(Workflow::without_input("failing", move |c| {
            let r = r.clone();
            async move {
                let step = flaky(&c, &r, 9, permanent);
                match retry {
                    Some(retry) => step.retry(retry).await,
                    None => step.await,
                }
            }
        }));
        let replayed = h.replay(1_000).await;
        let failure = failed(&replayed).expect("the run failed");
        assert_eq!(failure.r#type, "StepFailed", "{failure:?}");
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn a_late_attempt_times_out() {
    let mut h = Harness::new(Workflow::without_input("slow", |c| async move {
        let slow = c.step("slow", || async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<_, Error>(1)
        });
        let r: Result<i32> = slow.timeout(Duration::from_millis(20)).await;
        match r {
            Err(Error::StepFailed { error, .. }) => Ok(error.message),
            other => panic!("{other:?}"),
        }
    }));
    let replayed = h.replay(1_000).await;
    let message = completed(&replayed).unwrap();
    assert!(
        message.as_str().unwrap().contains("attempt past"),
        "{message}"
    );
}

#[tokio::test]
async fn parallel_steps_replay_the_same() {
    let runs = counter();
    let r = runs.clone();
    let mut h = Harness::new(Workflow::without_input("parallel", move |c| {
        let r = r.clone();
        async move {
            let (a, b) = tokio::try_join!(
                counting_step(&c, "a", &r, 1).into_future(),
                counting_step(&c, "b", &r, 2).into_future()
            )?;
            c.sleep(seconds(1)).await?;
            Ok(a + b)
        }
    }));
    assert_eq!(h.replay(1_000).await.next_wake(), Some(2_000));
    assert_eq!(
        completed(&h.replay(2_000).await),
        Some(serde_json::json!(3))
    );
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn other_calls_than_the_history_fail_the_run() {
    let mut h = Harness::new(Workflow::without_input("changed", |c| async move {
        counting_step(&c, "renamed", &counter(), 1).await
    }));
    let recorded = layout::Step {
        name: "original".into(),
        occurrence: 0,
        attempt: 1,
        value: Some(json(b"1".to_vec()).unwrap()),
    };
    h.history.push(layout::Event {
        kind: Some(Kind::Step(recorded)),
    });
    h.next_n = 1;
    let failure = failed(&h.replay(1_000).await).expect("the run failed");
    assert_eq!(failure.r#type, "NonDeterminism");
}

#[tokio::test]
async fn a_lost_commit_drops_the_replay() {
    let mut h = Harness::new(Workflow::without_input("loser", |c| async move {
        counting_step(&c, "a", &counter(), 1).await
    }));
    let replayed = h.replay_with(1_000, false).await;
    assert!(matches!(replayed.end, replay_end::End::Dropped(_)));
    assert!(h.history.is_empty());
}

#[tokio::test]
async fn inputs_and_errors_of_the_code() {
    #[derive(serde::Deserialize)]
    struct Input {
        x: i64,
    }
    let negative = || crate::Failure {
        kind: "Negative".into(),
        message: "x < 0".into(),
        retryable: false,
    };
    let workflow = Workflow::new("typed", move |_c, input: Input| async move {
        if input.x < 0 {
            return Err(Error::Failed(negative()));
        }
        Ok(input.x + 1)
    });
    let cases = [
        (r#"{"x": 41}"#, None),
        (r#"{"x": -1}"#, Some("Negative")),
        (r#"{"y": 1}"#, Some("Json")),
    ];
    for (input, failure) in cases {
        let mut h = Harness::new(workflow.clone());
        h.input = input.as_bytes().to_vec();
        let replayed = h.replay(1_000).await;
        match failure {
            None => assert_eq!(completed(&replayed), Some(serde_json::json!(42))),
            Some(kind) => assert_eq!(failed(&replayed).unwrap().r#type, kind),
        }
    }
}
