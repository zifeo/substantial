//! The test environment of docs/testing.md: failures and faults, what never hangs, what is cheap.
//! The ports of `python/tests/test_testing_faults.py`, under the same names.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use substantial::testing::{Chaos, Env, Faults};
use substantial::{
    Context, Error, Permanent, Preset, RunStatus, Substantial, Workflow, days, minutes, seconds,
};
use tokio::sync::Notify;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

type Pushes = Arc<Mutex<Vec<(DateTime<Utc>, Vec<i64>)>>>;

#[derive(Deserialize)]
struct Key {
    #[allow(dead_code)]
    key: String,
}

fn debounce(pushes: &Pushes) -> Workflow<Vec<i64>> {
    let pushes = pushes.clone();
    Workflow::new("debounce", move |c: Context, _: Key| {
        let pushes = pushes.clone();
        async move {
            let events: Arc<Mutex<Vec<i64>>> = Arc::default();
            let pushed = events.clone();
            c.handle("event", move |v: i64| pushed.lock().unwrap().push(v))?;
            let len = |e: &Arc<Mutex<Vec<i64>>>| e.lock().unwrap().len();
            let mut seen = 0;
            loop {
                let e = events.clone();
                let more = c.wait_until(move || len(&e) > seen).timeout(minutes(2));
                if !more.await? {
                    break;
                }
                seen = len(&events);
            }
            let (at, all) = (c.now().await?, events.lock().unwrap().clone());
            let pushing = all.clone();
            c.step("push", move || {
                let (pushes, events) = (pushes.clone(), pushing.clone());
                async move {
                    pushes.lock().unwrap().push((at, events));
                    Ok::<_, Error>(())
                }
            })
            .await?;
            Ok(all)
        }
    })
}

fn failing() -> Workflow<()> {
    Workflow::without_input("failing", |c| async move {
        let fail = || async { Err::<(), _>(Permanent::new("unknown invoice")) };
        c.step("fail", fail).await
    })
}

fn failing_parent() -> Workflow<()> {
    Workflow::without_input("failing_parent", |c| async move {
        c.start(&failing(), ()).id("its-child").await?;
        Ok(())
    })
}

fn napping() -> Workflow<()> {
    Workflow::without_input("napping", |c| async move { c.sleep(days(30)).await })
}

fn echo() -> Workflow<()> {
    Workflow::without_input("echo", |c| async move {
        let pings: Arc<Mutex<Vec<i64>>> = Arc::default();
        let pushed = pings.clone();
        c.handle("ping", move |v: i64| pushed.lock().unwrap().push(v))?;
        loop {
            let (seen, p) = (pings.lock().unwrap().len(), pings.clone());
            c.wait_until(move || p.lock().unwrap().len() > seen).await?;
            c.send(c.chain(), "ping", seen).await?;
        }
    })
}

#[tokio::test]
async fn an_outage_inside_the_debounce_window() {
    let (env, pushes) = (Env::open(t0()).await.unwrap(), Pushes::default());
    let worker = env
        .worker(debounce(&pushes))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let key = json!({ "key": "c1" });
    let run = env
        .start(debounce(&pushes), key)
        .id("contact-c1")
        .await
        .unwrap();
    run.send("event", 1).id("crm-1").await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    env.outage(minutes(10));
    let sent = run.send("event", 2).id("crm-2").await;
    assert!(matches!(sent, Err(Error::StoreError(_))), "{sent:?}");
    assert_eq!(env.now(), t0() + minutes(1));
    env.advance(minutes(12)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), [1]);
    // The timer came due in the outage: the worker retried, and pushed once the store was back.
    let pushes = pushes.lock().unwrap().clone();
    let [(at, events)] = &pushes[..] else {
        panic!("{pushes:?}")
    };
    assert!(t0() + minutes(11) <= *at && *at <= t0() + minutes(12) && events == &[1]);
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_thousand_events_in_one_instant() {
    let (env, pushes) = (Env::open(t0()).await.unwrap(), Pushes::default());
    let worker = env
        .worker(debounce(&pushes))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env
        .start(debounce(&pushes), json!({ "key": "c1" }))
        .await
        .unwrap();
    for i in 0..1000 {
        run.send("event", i)
            .id(&format!("crm-{i:04}"))
            .await
            .unwrap();
    }
    env.advance(minutes(2)).await.unwrap();
    let all: Vec<i64> = (0..1000).collect();
    assert_eq!(run.result().await.unwrap(), all);
    assert_eq!(*pushes.lock().unwrap(), [(t0() + minutes(2), all)]);
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_run_failing_in_the_background_fails_the_test() {
    let env = Env::open(t0()).await.unwrap();
    let workflows = (failing(), failing_parent());
    let worker = env
        .worker(workflows)
        .preset(Preset::Service)
        .start()
        .unwrap();
    let app = env.client().await.unwrap();
    let read = env.start(failing(), ()).id("read").await.unwrap();
    let looked = env.start(failing(), ()).id("looked").await.unwrap();
    app.start(failing(), ())
        .id("started-by-the-app")
        .await
        .unwrap();
    env.start(failing_parent(), ()).id("parent").await.unwrap();
    let cancelled = env.start(napping(), ()).id("cancelled").await.unwrap();
    cancelled.cancel("not needed").await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    assert!(matches!(read.result().await, Err(Error::RunFailed { .. })));
    assert_eq!(looked.status().await.unwrap(), RunStatus::Failed);
    let by_the_app = app.run(failing(), "started-by-the-app");
    assert_eq!(by_the_app.status().await.unwrap(), RunStatus::Failed);
    let message = match env.check().await {
        Err(Error::Unchecked(m)) => m,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("unknown invoice"), "{message}");
    for chain in ["started-by-the-app", "its-child"] {
        assert!(message.contains(&format!("{chain} (")), "{message}");
    }
    for chain in ["read", "looked", "cancelled", "parent"] {
        assert!(!message.contains(&format!(" {chain} (")), "{message}");
    }
    for chain in ["started-by-the-app", "its-child"] {
        let read = env.run(failing(), chain).result().await;
        assert!(matches!(read, Err(Error::RunFailed { .. })), "{read:?}");
    }
    env.check().await.unwrap();
    worker.shutdown().await.unwrap();
    env.close().await;
}

fn handling_parent() -> Workflow<String> {
    Workflow::without_input("handling_parent", |c| async move {
        let child = c.start(&failing(), ()).id("handled-child").await?;
        match child.result().await {
            Ok(()) => Ok("shipped".to_owned()),
            Err(Error::RunFailed { .. }) => Ok("shipped without it".to_owned()),
            Err(e) => Err(e),
        }
    })
}

#[tokio::test]
async fn a_child_failure_its_parent_read_is_read() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker((failing(), handling_parent()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(handling_parent(), ()).id("order").await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "shipped without it");
    env.check().await.unwrap();
    worker.shutdown().await.unwrap();
    env.close().await;
}

fn failing_briefly_kept() -> Workflow<()> {
    Workflow::without_input("failing_briefly_kept", |c| async move {
        let fail = || async { Err::<(), _>(Permanent::new("card declined")) };
        c.step("fail", fail).await
    })
    .with_retention(Duration::from_secs(3600))
}

#[tokio::test]
async fn a_failed_run_collected_before_check_is_reported() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(failing_briefly_kept())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env
        .start(failing_briefly_kept(), ())
        .id("nightly")
        .await
        .unwrap();
    env.advance(days(1)).await.unwrap();
    let status = run.status().await;
    assert!(
        matches!(status, Err(Error::RunNotFound { .. })),
        "{status:?}"
    );
    let message = match env.check().await {
        Err(Error::Unchecked(m)) => m,
        other => panic!("{other:?}"),
    };
    assert!(message.contains("nightly (StepFailed: "), "{message}");
    assert!(message.contains("card declined"), "{message}");
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_hung_step_is_stuck_and_interrupted_when_its_worker_crashes() {
    let (answered, ended) = (Arc::new(Notify::new()), Arc::new(Mutex::new(Vec::new())));
    let (a, e) = (answered.clone(), ended.clone());
    let hanging = Workflow::without_input("hanging", move |c| {
        let (a, e) = (a.clone(), e.clone());
        async move {
            let call_the_psp = move || {
                let (a, e) = (a.clone(), e.clone());
                async move {
                    a.notified().await;
                    e.lock().unwrap().push("charged");
                    Ok::<_, Error>(())
                }
            };
            c.step("charge", call_the_psp).await
        }
    });
    let env = Env::open(t0()).await.unwrap();
    env.__step_limit(Duration::from_millis(50));
    let worker = env
        .worker(hanging.clone())
        .preset(Preset::Service)
        .start()
        .unwrap();
    env.start(hanging, ()).id("hung").await.unwrap();
    match env.settle().await {
        Err(Error::Stuck(m)) => assert!(m.contains("hung/charge"), "{m}"),
        other => panic!("{other:?}"),
    }
    worker.crash();
    answered.notify_one();
    tokio::task::yield_now().await;
    env.close().await;
    assert!(ended.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_hung_step_fails_the_test_instead_of_hanging_it() {
    let answered = Arc::new(Notify::new());
    let a = answered.clone();
    let hanging = Workflow::without_input("hanging", move |c| {
        let a = a.clone();
        async move {
            let charge = move || {
                let a = a.clone();
                async move {
                    a.notified().await;
                    Ok::<_, Error>(())
                }
            };
            c.step("charge", charge).await
        }
    });
    let env = Env::open(t0()).await.unwrap();
    env.__step_limit(Duration::from_millis(200));
    let worker = env
        .worker(hanging.clone())
        .preset(Preset::Service)
        .start()
        .unwrap();
    env.start(hanging, ()).id("hung").await.unwrap();
    match env.settle().await {
        Err(Error::Stuck(m)) => assert!(m.contains("hung/charge"), "{m}"),
        other => panic!("{other:?}"),
    }
    worker.crash();
    let left = tokio::time::timeout(Duration::from_secs(3), env.close()).await;
    assert!(left.is_ok(), "leaving the environment hung");
    answered.notify_one();
}

#[derive(Deserialize)]
struct Steps {
    n: u32,
}

fn many_steps() -> Workflow<u32> {
    Workflow::new("many_steps", |c: Context, Steps { n }: Steps| async move {
        for i in 0..n {
            c.step("step", move || async move { Ok::<_, Error>(i) })
                .await?;
        }
        Ok(n)
    })
}

#[tokio::test]
async fn chaos_crashes_a_worker_after_about_rate_of_the_step_attempts() {
    let mut crashed = 0;
    for seed in 0..4 {
        let env = Env::open(t0())
            .chaos(Chaos { seed, rate: 0.05 })
            .await
            .unwrap();
        let worker = env
            .worker(many_steps())
            .preset(Preset::Service)
            .set("continue_as_new_after", "0")
            .start()
            .unwrap();
        let run = env.start(many_steps(), json!({ "n": 100 })).await.unwrap();
        env.advance(minutes(60)).await.unwrap();
        assert_eq!(run.result().await.unwrap(), 100);
        crashed += env.faults().crashed;
        worker.shutdown().await.unwrap();
        env.close().await;
    }
    // 400 steps, each attempt that returns drawing a crash with probability 0.05.
    assert!((10..=35).contains(&crashed), "{crashed}");
}

fn stamp() -> Workflow<String> {
    Workflow::without_input("stamp", |c| async move { Ok(c.now().await?.to_rfc3339()) })
}

#[tokio::test]
async fn an_outage_moves_the_scans_after_it() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env.worker(stamp()).preset(Preset::Service).start().unwrap();
    let other_pod = env.client().await.unwrap();
    env.advance(seconds(5)).await.unwrap();
    env.outage(seconds(40));
    env.advance(seconds(57)).await.unwrap();
    let run = other_pod.start(stamp(), ()).id("late").await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    // The scan due at 30 s failed and was retried once the store was back, which moved the scans
    // after it: the run is found at 69 s, not at 90 s.
    assert_eq!(
        run.result().await.unwrap(),
        (t0() + seconds(69)).to_rfc3339()
    );
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn chaos_faults_the_scans_of_an_idle_worker() {
    let chaos = Chaos { seed: 1, rate: 0.5 };
    let env = Env::open(t0()).chaos(chaos).await.unwrap();
    let worker = env.worker(stamp()).preset(Preset::Service).start().unwrap();
    env.advance(minutes(60)).await.unwrap();
    // About 120 scans, half of them failed at least once.
    let failed = env.faults().failed;
    assert!(failed > 60, "{failed}");
    worker.crash();
    env.close().await;
}

fn counter() -> Workflow<usize> {
    Workflow::without_input("counter", |c| async move {
        let events: Arc<Mutex<Vec<i64>>> = Arc::default();
        let pushed = events.clone();
        c.handle("event", move |v: i64| pushed.lock().unwrap().push(v))?;
        let e = events.clone();
        c.wait_until(move || e.lock().unwrap().len() >= 150).await?;
        Ok(events.lock().unwrap().len())
    })
}

#[tokio::test]
async fn many_settles_at_one_instant_are_not_a_livelock() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(counter())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(counter(), ()).id("counter").await.unwrap();
    for i in 0..150 {
        run.send("event", i).await.unwrap();
        env.settle().await.unwrap();
    }
    assert_eq!(run.result().await.unwrap(), 150);
    worker.shutdown().await.unwrap();
    env.close().await;
}

fn quote() -> Workflow<i64> {
    Workflow::without_input("quote", |_| async { Ok(42) })
}

fn priced(engine: &Substantial) -> Workflow<i64> {
    let engine = engine.clone();
    Workflow::without_input("priced", move |c| {
        let engine = engine.clone();
        async move {
            let ask_quote = move || {
                let engine = engine.clone();
                async move {
                    let run = engine.start(quote(), ()).id("quote-a").await?;
                    run.result().timeout(seconds(30)).await
                }
            };
            c.step("ask_quote", ask_quote).await
        }
    })
}

#[tokio::test]
async fn a_step_waiting_on_the_environment_fails_at_once() {
    let env = Env::open(t0()).await.unwrap();
    let engine = env.client().await.unwrap();
    let worker = env
        .worker((priced(&engine), quote()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(priced(&engine), ()).id("p1").await.unwrap();
    let started = Instant::now();
    let result = run.result().await;
    assert!(started.elapsed() < Duration::from_secs(1));
    let Err(failed @ Error::RunFailed { .. }) = result else {
        panic!("{result:?}")
    };
    let message = failed.to_string();
    let expected = "a step cannot wait for the environment to settle";
    assert!(message.contains(expected), "{message}");
    env.check().await.unwrap();
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn runs_waking_each_other_without_the_clock_moving_are_stuck() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env.worker(echo()).preset(Preset::Service).start().unwrap();
    let run = env.start(echo(), ()).id("echo").await.unwrap();
    run.send("ping", 0).await.unwrap();
    match env.advance(minutes(1)).await {
        Err(Error::Stuck(m)) => {
            assert!(
                m.contains("run echo was replayed more than 100 times"),
                "{m}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(env.now(), t0());
    worker.crash();
    env.close().await;
}

#[tokio::test]
async fn chaos_faults_the_workers_not_the_clients() {
    let chaos = Chaos { seed: 1, rate: 1.0 };
    let env = Env::open(t0()).chaos(chaos).await.unwrap();
    let run = env.start(napping(), ()).await.unwrap();
    run.send("note", 1).await.unwrap();
    assert_eq!(run.status().await.unwrap(), RunStatus::Pending);
    assert_eq!(env.faults(), Faults::default());
    let worker = env
        .worker(napping())
        .preset(Preset::Service)
        .start()
        .unwrap();
    env.advance(seconds(1)).await.unwrap();
    assert!(env.faults().failed > 0);
    worker.crash();
    env.close().await;
}

fn ticking() -> Workflow<Vec<String>> {
    Workflow::without_input("ticking", |c| async move {
        let mut instants = Vec::new();
        for _ in 0..6 {
            c.sleep(minutes(4 * 60)).await?;
            c.step("tick", || async { Ok::<_, Error>(()) }).await?;
            instants.push(c.now().await?.to_rfc3339());
        }
        Ok(instants)
    })
}

type Day = (
    Vec<Vec<String>>,
    Faults,
    Vec<(String, Vec<u8>)>,
    DateTime<Utc>,
);

async fn a_chaotic_day(skipping: bool) -> (Day, u64) {
    let chaos = Chaos {
        seed: 11,
        rate: 0.05,
    };
    let env = Env::open(t0()).chaos(chaos).await.unwrap();
    env.__skipping(skipping);
    let worker = env
        .worker(ticking())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let other_pod = env.client().await.unwrap();
    let run = env.start(ticking(), ()).id("early").await.unwrap();
    env.advance(minutes(7 * 60 + 13)).await.unwrap();
    let late = other_pod.start(ticking(), ()).id("late").await.unwrap();
    env.advance(minutes(26 * 60)).await.unwrap();
    let results = vec![run.result().await.unwrap(), late.result().await.unwrap()];
    worker.shutdown().await.unwrap();
    let day = (results, env.faults(), env.__objects(), env.now());
    let skipped = env.__skipped();
    env.close().await;
    (day, skipped)
}

#[tokio::test]
async fn chaos_skips_only_the_scans_whose_draws_succeed() {
    let (skipped, skips) = a_chaotic_day(true).await;
    let (ran, runs) = a_chaotic_day(false).await;
    // The same faults, the same histories, every event at the same instant: the store's keys and
    // bytes, the segments and their times included.
    assert!(skipped == ran, "{:?} {:?}", skipped.1, ran.1);
    let faults = skipped.1;
    assert!(faults.failed > 0 && faults.crashed > 0, "{faults:?}");
    assert!(skips > 2000 && runs == 0, "{skips} {runs}");
}

async fn a_month_of_virtual_time_is_cheap(preset: Preset) {
    let env = Env::open(t0()).await.unwrap();
    let worker = env.worker(napping()).preset(preset).start().unwrap();
    let other_pod = env.client().await.unwrap();
    let run = env.start(napping(), ()).await.unwrap();
    let started = Instant::now();
    env.advance(days(31)).await.unwrap();
    let took = started.elapsed();
    assert!(took < Duration::from_millis(500), "{took:?}");
    assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
    // The scans skipped kept their instants: one started 10 s after a scan is found at the next.
    env.advance(seconds(10)).await.unwrap();
    let late = other_pod.start(napping(), ()).id("late").await.unwrap();
    let before = if preset == Preset::Service { 19 } else { 0 };
    env.advance(seconds(before)).await.unwrap();
    assert_eq!(late.status().await.unwrap(), RunStatus::Pending);
    env.advance(seconds(1)).await.unwrap();
    assert_eq!(late.status().await.unwrap(), RunStatus::Suspended);
    worker.shutdown().await.unwrap();
    env.close().await;
}

mod a_month_of_virtual_time_is_cheap {
    use substantial::Preset;

    #[tokio::test]
    async fn service() {
        super::a_month_of_virtual_time_is_cheap(Preset::Service).await;
    }

    #[tokio::test]
    async fn script() {
        super::a_month_of_virtual_time_is_cheap(Preset::Script).await;
    }
}
