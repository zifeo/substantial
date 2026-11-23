//! Schedules: the cron arithmetic against the vectors every host checks (`dev/cross/cron.json`),
//! and `c.schedule` with `sub.schedule` on the test environment. The ports of
//! `python/tests/test_schedule.py`, under the same names.

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Timelike, Utc};
use serde_json::{Value, json};
use substantial::__internal::{iso, next_fire, parse, plan};
use substantial::testing::Env;
use substantial::{Cron, Error, Preset, RunStatus, Workflow, hours};

fn vectors() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../dev/cross/cron.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// A Monday.
fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

fn hourly() -> Option<Cron> {
    Some(Cron::new("0 * * * *", "UTC").unwrap())
}

fn text(v: &Value) -> &str {
    v.as_str().unwrap()
}

#[test]
fn fire_times_match_the_shared_vectors() {
    for v in vectors()["next"].as_array().unwrap() {
        let cron = Cron::new(text(&v["cron"]), text(&v["tz"])).unwrap();
        let mut t = parse(text(&v["after"])).unwrap();
        let mut fires = Vec::new();
        for _ in v["fires"].as_array().unwrap() {
            t = next_fire(&cron, t).unwrap();
            fires.push(Value::from(iso(t)));
        }
        assert_eq!(Value::from(fires), v["fires"], "{} {}", v["cron"], v["tz"]);
    }
}

#[test]
fn invalid_crons_and_zones_are_refused() {
    for v in vectors()["invalid"].as_array().unwrap() {
        let refused = Cron::new(text(&v["cron"]), text(&v["tz"]));
        assert!(
            matches!(refused, Err(Error::InvalidArgument(_))),
            "{} {}: {refused:?}",
            v["cron"],
            v["tz"]
        );
    }
}

#[test]
fn plans_of_missed_fires_match_the_shared_vectors() {
    for v in vectors()["plan"].as_array().unwrap() {
        let backfill = v["backfill"].as_u64().unwrap();
        let (after, now) = (text(&v["after"]), text(&v["now"]));
        let planned = plan(text(&v["cron"]), text(&v["tz"]), after, now, backfill).unwrap();
        let planned = serde_json::to_value(planned).unwrap();
        assert_eq!(planned, v["result"], "{} {}", v["cron"], v["now"]);
    }
}

type Fired = Arc<Mutex<Vec<String>>>;

fn ticker(fired: &Fired) -> Workflow<()> {
    let fired = fired.clone();
    Workflow::without_input("ticker", move |c| {
        let fired = fired.clone();
        async move {
            c.schedule(move |c, at| {
                let fired = fired.clone();
                async move {
                    c.step("tick", move || {
                        let fired = fired.clone();
                        async move {
                            fired.lock().unwrap().push(at.to_rfc3339());
                            Ok::<_, Error>(())
                        }
                    })
                    .await?;
                    Ok(ControlFlow::Continue(()))
                }
            })
            .await
        }
    })
}

/// A fresh environment at `t0`, and what its ticker fired.
async fn env() -> (Env, Fired) {
    (Env::open(t0()).await.unwrap(), Fired::default())
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

fn fired(f: &Fired) -> Vec<String> {
    f.lock().unwrap().clone()
}

fn at_hours(hs: &[u32]) -> Vec<String> {
    hs.iter()
        .map(|h| t0().with_hour(*h).unwrap().to_rfc3339())
        .collect()
}

fn service(env: &Env, wf: &Workflow<()>) -> substantial::Worker {
    env.worker(wf).preset(Preset::Service).start().unwrap()
}

#[tokio::test]
async fn a_schedule_fires_at_each_cron_time() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    let run = env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    env.advance(hours(3)).await.unwrap();
    assert_eq!(fired(&f), at_hours(&[10, 11, 12]));
    let published: Option<Value> = run.read("schedule").await.unwrap();
    assert_eq!(
        published.unwrap(),
        json!({
            "revision": 1,
            "cron": "0 * * * *",
            "tz": "UTC",
            "backfill": 5,
            "last": "2030-01-07T12:00:00.000Z",
            "recent": [
                "2030-01-07T10:00:00.000Z",
                "2030-01-07T11:00:00.000Z",
                "2030-01-07T12:00:00.000Z",
            ],
            "skipped": {"count": 0, "oldest": null, "newest": null},
            "next": "2030-01-07T13:00:00.000Z",
        })
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn missed_fires_are_backfilled_oldest_first_up_to_backfill() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    let run = env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    env.advance(hours(8)).await.unwrap();
    assert!(fired(&f).is_empty());
    let worker = service(&env, &wf);
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    assert_eq!(fired(&f), at_hours(&[13, 14, 15, 16, 17]));
    let published: Value = run.read("schedule").await.unwrap().unwrap();
    assert_eq!(
        published["skipped"],
        json!({
            "count": 3,
            "oldest": "2030-01-07T10:00:00.000Z",
            "newest": "2030-01-07T12:00:00.000Z",
        })
    );
    assert_eq!(published["next"], "2030-01-07T18:00:00.000Z");
    done(env).await;
}

#[tokio::test]
async fn backfill_is_part_of_the_config() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    env.schedule(&wf, "tick", hourly(), 1)
        .backfill(2)
        .await
        .unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    env.advance(hours(4)).await.unwrap();
    let worker = service(&env, &wf);
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    assert_eq!(fired(&f), at_hours(&[12, 13]));
    done(env).await;
}

#[tokio::test]
async fn a_config_applies_only_if_its_revision_is_higher() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    let run = env.schedule(&wf, "tick", hourly(), 2).await.unwrap();
    env.settle().await.unwrap();
    let stale = json!({"revision": 1, "cron": "*/10 * * * *", "tz": "UTC", "backfill": 5});
    run.send("schedule", stale).await.unwrap();
    env.advance(hours(1)).await.unwrap();
    assert_eq!(fired(&f), at_hours(&[10]));
    let published: Value = run.read("schedule").await.unwrap().unwrap();
    assert_eq!(published["revision"], 2);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn sub_schedule_does_nothing_twice_and_refuses_a_stale_or_another_config() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    env.settle().await.unwrap();
    env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    let zurich = Cron::new("0 * * * *", "Europe/Zurich").unwrap();
    let other = env.schedule(&wf, "tick", Some(zurich), 1).await;
    assert!(
        matches!(&other, Err(Error::ConfigError(m)) if m.contains("revision 1 is applied with another config")),
        "{other:?}"
    );
    let half = Cron::new("30 * * * *", "UTC").unwrap();
    env.schedule(&wf, "tick", Some(half), 2).await.unwrap();
    env.settle().await.unwrap();
    let stale = env.schedule(&wf, "tick", hourly(), 1).await;
    assert!(
        matches!(&stale, Err(Error::ConfigError(m)) if m.contains("revision 2 is applied already, higher than 1")),
        "{stale:?}"
    );
    env.advance(hours(1)).await.unwrap();
    assert_eq!(fired(&f), [t0().with_minute(30).unwrap().to_rfc3339()]);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_malformed_schedule_message_blocks_the_run() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let run = env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    run.send(
        "schedule",
        serde_json::json!({"revision": 2, "cron": "0 * * * *"}),
    )
    .await
    .unwrap();
    let worker = service(&env, &wf);
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    match run.result().await {
        Err(Error::Stuck(m)) => assert!(
            m.contains("blocked by InvalidArgument")
                && m.contains("a schedule message is a config of sub.schedule"),
            "{m}"
        ),
        other => panic!("{other:?}"),
    }
    done(env).await;
}

#[tokio::test]
async fn two_configs_of_one_revision_block_the_run() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    let half = Cron::new("30 * * * *", "UTC").unwrap();
    let run = env.schedule(&wf, "tick", Some(half), 1).await.unwrap();
    let worker = service(&env, &wf);
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    match run.result().await {
        Err(Error::Stuck(m)) => assert!(
            m.contains(
                "blocked by ConfigError: config error: schedule revision 1 came with two configs"
            ),
            "{m}"
        ),
        other => panic!("{other:?}"),
    }
    done(env).await;
}

#[tokio::test]
async fn none_disables_a_schedule_and_a_new_cron_fires_from_when_it_applies() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    let run = env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    env.advance(hours(1)).await.unwrap();
    env.schedule(&wf, "tick", None, 2).await.unwrap();
    env.advance(hours(3)).await.unwrap();
    assert_eq!(fired(&f), at_hours(&[10]));
    let published: Value = run.read("schedule").await.unwrap().unwrap();
    assert_eq!(published["cron"], Value::Null);
    assert_eq!(run.status().await.unwrap(), RunStatus::Suspended);
    env.schedule(&wf, "tick", hourly(), 3).await.unwrap();
    env.advance(hours(1)).await.unwrap();
    assert_eq!(fired(&f), at_hours(&[10, 14]));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_schedule_crosses_compressions() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = env
        .worker(&wf)
        .preset(Preset::Service)
        .set("continue_as_new_after", "0")
        .start()
        .unwrap();
    let run = env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    env.advance(hours(5)).await.unwrap();
    env.schedule(&wf, "tick", hourly(), 2)
        .backfill(1)
        .await
        .unwrap();
    env.advance(hours(2)).await.unwrap();
    assert_eq!(fired(&f), at_hours(&[10, 11, 12, 13, 14, 15, 16]));
    let published: Value = run.read("schedule").await.unwrap().unwrap();
    let recent = published["recent"].as_array().unwrap().len();
    assert_eq!(
        (&published["revision"], &published["backfill"], recent),
        (&json!(2), &json!(1), 7)
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// A declared step named so fails to compile: see the macros' own tests.
#[tokio::test]
async fn the_schedule_step_name_is_reserved() {
    let (env, _) = env().await;
    let inline = Workflow::without_input("inline", |c| async move {
        c.step("schedule.next", || async { Ok::<_, Error>(()) })
            .await
    });
    let worker = env.worker(&inline).preset(Preset::Service).start().unwrap();
    let run = env.start(&inline, ()).await.unwrap();
    let blocked = run.result().await;
    assert!(
        matches!(&blocked, Err(Error::Stuck(m)) if m.contains("blocked by InvalidArgument") && m.contains("reserved")),
        "{blocked:?}"
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// A revision or a backfill below 1; the other refusals of the Python test are types here.
#[tokio::test]
async fn sub_schedule_checks_its_arguments() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    for (revision, backfill) in [(0, 5), (1, 0)] {
        let refused = env
            .schedule(&wf, "tick", hourly(), revision)
            .backfill(backfill)
            .await;
        assert!(
            matches!(refused, Err(Error::InvalidArgument(_))),
            "{refused:?}"
        );
    }
    done(env).await;
}

#[tokio::test]
async fn a_config_publishes_its_next_fire_at_once() {
    let (env, f) = env().await;
    let wf = ticker(&f);
    let worker = service(&env, &wf);
    let run = env.schedule(&wf, "tick", hourly(), 1).await.unwrap();
    env.settle().await.unwrap();
    let published: Value = run.read("schedule").await.unwrap().unwrap();
    assert_eq!(
        (&published["next"], &published["last"]),
        (&json!("2030-01-07T10:00:00.000Z"), &Value::Null)
    );
    env.schedule(&wf, "tick", None, 2).await.unwrap();
    env.settle().await.unwrap();
    let published: Value = run.read("schedule").await.unwrap().unwrap();
    assert_eq!(published["next"], Value::Null);
    worker.shutdown().await.unwrap();
    done(env).await;
}
