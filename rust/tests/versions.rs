//! Versions (I-5): side by side, version-only workers, continuations into a version (I-12),
//! patches and upgrades. The patch and upgrade tests port `python/tests/test_patched.py` and
//! `python/tests/test_upgrade.py`, under the same names.

mod common;

use std::ops::ControlFlow;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use common::{Hidden, LONG, namespaces, script};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use substantial::__internal as internal;
use substantial::testing::Env;
use substantial::{Context, Cron, Error, Preset, RunInfo, RunStatus, Workflow, hours};

fn versioned(version: u32) -> Workflow<String> {
    Workflow::without_input(
        "versioned",
        move |_c| async move { Ok(format!("v{version}")) },
    )
    .with_version(version)
}

fn unversioned() -> Workflow<String> {
    Workflow::without_input("versioned", |_c| async { Ok("none".to_owned()) })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_are_replayed_by_the_code_of_their_version() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let old = script(&sub, vec![unversioned(), versioned(1)])
            .start()
            .unwrap();
        let none = sub.start(&unversioned(), ()).await.unwrap();
        let one = sub.start(&versioned(1), ()).await.unwrap();
        let two = sub.start(&versioned(2), ()).await.unwrap();
        assert_eq!(none.result().timeout(LONG).await.unwrap(), "none");
        assert_eq!(one.result().timeout(LONG).await.unwrap(), "v1");
        // § d9vmk6y6y.c@myyjn
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            two.status().await.unwrap(),
            RunStatus::Pending,
            "{}",
            ns.name
        );
        // § end d9vmk6y6y.c
        let all = vec![unversioned(), versioned(1), versioned(2)];
        let both = script(&sub, all).start().unwrap();
        assert_eq!(two.result().timeout(LONG).await.unwrap(), "v2");
        let twice = vec![unversioned(), versioned(2), versioned(2)];
        let twice = script(&sub, twice).start();
        assert!(matches!(twice.map(drop), Err(Error::ConfigError(_))));
        old.shutdown().await.unwrap();
        both.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_with_versions_only_leaves_unversioned_runs_alone() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let only_one = script(&sub, versioned(1)).start().unwrap();
        let none = sub.start(&unversioned(), ()).await.unwrap();
        let one = sub.start(&versioned(1), ()).await.unwrap();
        assert_eq!(one.result().timeout(LONG).await.unwrap(), "v1");
        // § d9vmk6y6y.d@qmdhy
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            none.status().await.unwrap(),
            RunStatus::Pending,
            "{}",
            ns.name
        );
        let unversioned_code = script(&sub, unversioned()).start().unwrap();
        assert_eq!(none.result().timeout(LONG).await.unwrap(), "none");
        // § end d9vmk6y6y.d
        only_one.shutdown().await.unwrap();
        unversioned_code.shutdown().await.unwrap();
    }
}

#[derive(Serialize, Deserialize)]
struct N {
    n: u32,
}

fn hop() -> Workflow<String> {
    Workflow::without_input("hop", |c| async move {
        Err(c.continue_as_new_to(&target(2), N { n: 2 }).await)
    })
}

fn hop_unversioned() -> Workflow<String> {
    Workflow::without_input("hop-unversioned", |c| async move {
        Err(c
            .continue_as_new_to(&target_unversioned(), N { n: 2 })
            .await)
    })
}

/// Counts down by continuing as itself, which keeps its version.
fn target(version: u32) -> Workflow<String> {
    Workflow::new("target", move |c: Context, input: N| async move {
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok(format!("v{version}"))
    })
    .with_version(version)
}

fn target_unversioned() -> Workflow<String> {
    Workflow::new("target", |c: Context, input: N| async move {
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok("none".to_owned())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_continuation_names_the_version_of_its_workflow() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let all = vec![
            hop(),
            hop_unversioned(),
            target(1),
            target(2),
            target_unversioned(),
        ];
        let worker = script(&sub, all).start().unwrap();
        let to_two = sub.start(&hop(), ()).await.unwrap();
        let to_none = sub.start(&hop_unversioned(), ()).await.unwrap();
        // § db0mrbxkk.a@ncybm
        assert_eq!(to_two.result().timeout(LONG).await.unwrap(), "v2");
        assert_eq!(to_none.result().timeout(LONG).await.unwrap(), "none");
        let one = sub.start(&target(1), N { n: 2 }).await.unwrap();
        assert_eq!(one.result().timeout(LONG).await.unwrap(), "v1");
        // § end db0mrbxkk.a
        worker.shutdown().await.unwrap();
    }
}

/// Version 1 steps then waits for `go`, version 2 waits for `go` then steps: the same name, no
/// version. Waiting on a message keeps the run suspended until the test sends it.
fn changed(first: bool) -> Workflow<i64> {
    Workflow::without_input("changed", move |c| async move {
        let one = || c.step("one", || async { Ok::<_, Error>(1) });
        if first {
            one().await?;
            let _: String = c.receive("go").await?;
        } else {
            let _: String = c.receive("go").await?;
            one().await?;
        }
        Ok(1)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incompatible_change_blocks_the_run_until_the_code_is_rolled_back() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let old = script(&sub, changed(true)).start().unwrap();
        let run = sub.start(&changed(true), ()).await.unwrap();
        common::eventually("the wait", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        old.shutdown().await.unwrap();
        let new = script(&sub, changed(false))
            .hidden("blocked_initial", "50ms")
            .start()
            .unwrap();
        run.send("go", "now").await.unwrap();
        // § rbwstdetg@ezdzz
        let error = common::blocked(&sub, changed(false), run.chain()).await;
        assert_eq!(error.kind, "NonDeterminism", "{}: {error}", ns.name);
        assert!(
            error
                .message
                .ends_with("; an incompatible change needs a new version (version=)"),
            "{error}"
        );
        new.shutdown().await.unwrap();
        let old = script(&sub, changed(true))
            .hidden("blocked_initial", "50ms")
            .start()
            .unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1, "{}", ns.name);
        // § end rbwstdetg
        old.shutdown().await.unwrap();
    }
}

/// Counts down by continuing into the version it names of its own workflow.
fn renamed(version: u32) -> Workflow<String> {
    Workflow::new("renamed", move |c: Context, input: N| async move {
        if input.n > 0 {
            return Err(c
                .continue_as_new_to(&renamed(2), N { n: input.n - 1 })
                .await);
        }
        Ok(format!("v{version}"))
    })
    .with_version(version)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_continuation_naming_its_own_workflow_takes_the_version_named() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (renamed(1), renamed(2))).start().unwrap();
        let run = sub.start(&renamed(1), N { n: 1 }).await.unwrap();
        // § db0mrbxkk.a@x3q2y
        assert_eq!(
            run.result().timeout(LONG).await.unwrap(),
            "v2",
            "{}",
            ns.name
        );
        // § end db0mrbxkk.a
        worker.shutdown().await.unwrap();
    }
}

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

/// Waits for `a`, steps, waits for `b`; patched, it asks for a patch after each wait.
fn patching(patched: bool) -> Workflow<Vec<bool>> {
    Workflow::without_input("patching", move |c| async move {
        let mut seen = Vec::new();
        c.receive::<()>("a").await?;
        if patched {
            seen.push(c.patched("fix-1")?);
        }
        c.step("x", || async { Ok::<_, Error>(1) }).await?;
        c.receive::<()>("b").await?;
        if patched {
            seen.push(c.patched("fix-2")?);
        }
        Ok(seen)
    })
}

#[tokio::test]
async fn a_patch_is_true_live_and_on_replay_and_false_in_an_older_history() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(patching(false))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let older = env.start(patching(false), ()).id("older").await.unwrap();
    older.send("a", ()).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    let worker = env
        .worker(patching(true))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let newer = env.start(patching(true), ()).id("newer").await.unwrap();
    newer.send("a", ()).await.unwrap();
    env.settle().await.unwrap();
    for run in [&older, &newer] {
        run.send("b", ()).await.unwrap();
    }
    // § hbsrjm0w2@ygh98
    assert_eq!(
        older.result().await.unwrap(),
        [false, true],
        "past its point, then live"
    );
    assert_eq!(
        newer.result().await.unwrap(),
        [true, true],
        "recorded, then live"
    );
    let patches: Vec<serde_json::Value> = (newer.history().await.unwrap().into_iter())
        .filter(|e| e.kind == "patched")
        .map(|e| e.data)
        .collect();
    assert_eq!(patches, [json!({"id": "fix-1"}), json!({"id": "fix-2"})]);
    // § end hbsrjm0w2
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_patch_id_outside_the_rules_is_refused() {
    let env = Env::open(t0()).await.unwrap();
    let misnamed = Workflow::without_input("misnamed", |c| async move {
        Ok(match c.patched("Not a label") {
            Err(Error::InvalidArgument(m)) => m,
            other => format!("{other:?}"),
        })
    });
    let worker = env
        .worker(&misnamed)
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(&misnamed, ()).await.unwrap();
    let refused = run.result().await.unwrap();
    // § hbsrjm0w2.a@4nhwr
    assert!(
        refused.contains(r#"patch id "Not a label" is invalid"#),
        "{refused}"
    );
    // § end hbsrjm0w2.a
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[derive(Serialize, Deserialize)]
struct Versions {
    versions: Vec<u32>,
}

/// Each iteration notes its version, then waits for `next`, three times.
fn looping(version: u32, upgrade: bool) -> Workflow<Versions> {
    Workflow::without_input("looping", move |c| async move {
        let body = move |c: Context, mut st: Versions| async move {
            st.versions.push(version);
            if st.versions.len() == 3 {
                return Ok(ControlFlow::Break(st));
            }
            c.receive::<()>("next").await?;
            Ok(ControlFlow::Continue(st))
        };
        let looped = c.iterate(
            Versions {
                versions: Vec::new(),
            },
            body,
        );
        if upgrade {
            looped.upgrade().await
        } else {
            looped.await
        }
    })
    .with_version(version)
}

async fn versions_of(upgrade: bool) -> Vec<u32> {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(looping(1, upgrade))
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = env.start(looping(1, upgrade), ()).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    let both = (looping(1, upgrade), looping(2, upgrade));
    let worker = env.worker(both).preset(Preset::Script).start().unwrap();
    run.send("next", ()).await.unwrap();
    env.settle().await.unwrap();
    let listed: Vec<RunInfo> = env.runs(looping(1, upgrade)).try_collect().await.unwrap();
    run.send("next", ()).await.unwrap();
    let result = run.result().await.unwrap();
    worker.shutdown().await.unwrap();
    env.close().await;
    let mut versions = vec![listed[0].version.unwrap()];
    versions.extend(result.versions);
    versions
}

#[tokio::test]
async fn a_loop_that_upgrades_continues_into_the_highest_version_served() {
    // § zsyv7772c@z4r30
    assert_eq!(versions_of(true).await, [2, 1, 2, 2]);
    // § end zsyv7772c
    assert_eq!(versions_of(false).await, [1, 1, 1, 1]);
}

fn ticking(version: u32) -> Workflow<()> {
    Workflow::without_input("ticking", move |c| async move {
        let tick = |c: Context, _at| async move {
            c.step("tick", move || async move { Ok::<_, Error>(version) })
                .await?;
            Ok(ControlFlow::Continue(()))
        };
        c.schedule(tick).upgrade().await
    })
    .with_version(version)
}

#[tokio::test]
async fn a_schedule_of_a_newer_version_moves_the_running_one() {
    let hourly = || Some(Cron::new("0 * * * *", "UTC").unwrap());
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(ticking(1))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.schedule(ticking(1), "tick", hourly(), 1).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    let both = (ticking(1), ticking(2));
    let worker = env.worker(both).preset(Preset::Service).start().unwrap();
    // § sfzkmddwj@63bhh
    let moved = env.schedule(ticking(2), "tick", hourly(), 2).await.unwrap();
    assert_eq!(moved.chain(), run.chain());
    env.advance(hours(1)).await.unwrap();
    let listed: Vec<RunInfo> = env.runs(ticking(2)).try_collect().await.unwrap();
    assert_eq!(listed[0].version, Some(2));
    // § end sfzkmddwj
    let published: serde_json::Value = run.read("schedule").await.unwrap().unwrap();
    assert_eq!(published["revision"], 2);
    worker.shutdown().await.unwrap();
    env.close().await;
}
