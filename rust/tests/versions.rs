//! Versions (I-5): side by side, version-only workers, and continuations into a version (I-12).

mod common;

use std::time::Duration;

use common::{LONG, namespaces, script};
use serde::{Deserialize, Serialize};
use substantial::__internal as internal;
use substantial::{Context, Error, RunStatus, Workflow};

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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let old = script(&sub, vec![unversioned(), versioned(1)])
            .start()
            .unwrap();
        let none = sub.start(&unversioned(), ()).await.unwrap();
        let one = sub.start(&versioned(1), ()).await.unwrap();
        let two = sub.start(&versioned(2), ()).await.unwrap();
        assert_eq!(none.result().timeout(LONG).await.unwrap(), "none");
        assert_eq!(one.result().timeout(LONG).await.unwrap(), "v1");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            two.status().await.unwrap(),
            RunStatus::Pending,
            "{}",
            ns.name
        );
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let only_one = script(&sub, versioned(1)).start().unwrap();
        let none = sub.start(&unversioned(), ()).await.unwrap();
        let one = sub.start(&versioned(1), ()).await.unwrap();
        assert_eq!(one.result().timeout(LONG).await.unwrap(), "v1");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            none.status().await.unwrap(),
            RunStatus::Pending,
            "{}",
            ns.name
        );
        let unversioned_code = script(&sub, unversioned()).start().unwrap();
        assert_eq!(none.result().timeout(LONG).await.unwrap(), "none");
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        assert_eq!(to_two.result().timeout(LONG).await.unwrap(), "v2");
        assert_eq!(to_none.result().timeout(LONG).await.unwrap(), "none");
        let one = sub.start(&target(1), N { n: 2 }).await.unwrap();
        assert_eq!(one.result().timeout(LONG).await.unwrap(), "v1");
        worker.shutdown().await.unwrap();
    }
}

/// Version 1 steps then sleeps, version 2 sleeps then steps: the same name, no version.
fn changed(first: bool) -> Workflow<i64> {
    Workflow::without_input("changed", move |c| async move {
        let one = || c.step("one", || async { Ok::<_, Error>(1) });
        if first {
            one().await?;
            c.sleep(Duration::from_millis(300)).await?;
        } else {
            c.sleep(Duration::from_millis(300)).await?;
            one().await?;
        }
        Ok(1)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incompatible_change_fails_the_run_with_non_determinism() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let old = script(&sub, changed(true)).start().unwrap();
        let run = sub.start(&changed(true), ()).await.unwrap();
        common::eventually("the sleep", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
        old.shutdown().await.unwrap();
        let new = script(&sub, changed(false)).start().unwrap();
        let failed = run.result().timeout(LONG).await;
        let message = failed.as_ref().err().map(ToString::to_string);
        let Err(Error::RunFailed { status, error, .. }) = failed else {
            panic!("{}: {failed:?}", ns.name);
        };
        assert_eq!(status, RunStatus::Failed, "{}", ns.name);
        assert_eq!(error.kind, "NonDeterminism", "{}: {error}", ns.name);
        let message = message.unwrap();
        assert!(
            message.ends_with("; an incompatible change needs a new version (version=)"),
            "{message}"
        );
        new.shutdown().await.unwrap();
    }
}
