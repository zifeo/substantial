//! A run's history limits: passing one ends the run failed with `HistoryTooLarge`, and a commit
//! past their warning share says so once. The ports of `python/tests/test_history_limits.py`,
//! under the same names.

mod common;

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use common::Hidden;
use serde_json::json;
use substantial::observe::NearLimit;
use substantial::testing::Env;
use substantial::{Error, Observed, Preset, Workflow};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

#[tokio::test]
async fn a_run_past_a_limit_of_its_history_ends_failed() {
    let told = Arc::new(Mutex::new(Vec::new()));
    let seen = told.clone();
    let env = Env::open(t0())
        .observe(move |e| {
            if let Observed::NearLimit(n) = e {
                seen.lock().unwrap().push(n.clone());
            }
        })
        .await
        .unwrap();
    let clocking = Workflow::without_input("clocking", |c| async move {
        for _ in 0..100 {
            if let Err(Error::HistoryTooLarge(_)) = c.now().await {
                return Ok("caught".to_owned());
            }
        }
        Ok("done".to_owned())
    });
    let worker = env
        .worker(&clocking)
        .preset(Preset::Script)
        .hidden("max_history_events", 10)
        .hidden("history_warn_percent", 50)
        .start()
        .unwrap();
    let run = env.start(&clocking, ()).id("clock").await.unwrap();
    // § vkpvtd54g@f9tqz
    let Err(Error::RunFailed { error, .. }) = run.result().await else {
        panic!("a run past a limit of its history ends failed");
    };
    assert_eq!(error.kind, "HistoryTooLarge", "{error:?}");
    // § vkpvtd54g.b@kf0wb
    assert!(!error.retryable);
    let details = json!({"limit": "history_events", "used": "11", "max": "10"});
    assert_eq!(error.details, Some(details));
    // § end vkpvtd54g.b
    // § end vkpvtd54g
    worker.shutdown().await.unwrap();
    // Told once, by the commit that took the run past half of it, its end's here.
    let near = told.lock().unwrap().clone();
    let expected = NearLimit {
        workflow: "clocking".into(),
        run: "clock".into(),
        limit: "history_events".into(),
        used: 12,
        max: 10,
    };
    assert_eq!(near, [expected]);
    env.close().await;
}

#[tokio::test]
async fn a_replay_past_its_inbox_limit_ends_failed() {
    let env = Env::open(t0()).await.unwrap();
    let taking = Workflow::without_input("taking", |c| async move {
        let got = Arc::new(Mutex::new(Vec::new()));
        let pushed = got.clone();
        c.handle("n", move |n: i64| pushed.lock().unwrap().push(n))?;
        c.wait_until(move || got.lock().unwrap().len() >= 5).await?;
        Ok(())
    });
    let run = env.start(&taking, ()).await.unwrap();
    for n in 0..5 {
        run.send("n", n).await.unwrap();
    }
    let worker = env
        .worker(&taking)
        .preset(Preset::Script)
        .hidden("max_replay_inbox", 3)
        .start()
        .unwrap();
    let Err(Error::RunFailed { error, .. }) = run.result().await else {
        panic!("a replay past its inbox limit ends failed");
    };
    let details = error.details.unwrap();
    assert_eq!(details["limit"], "replay_inbox", "{details}");
    worker.shutdown().await.unwrap();
    env.close().await;
}
