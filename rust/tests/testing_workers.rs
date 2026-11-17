//! The test environment of docs/testing.md: workers, crashes, pauses and claims. The ports of
//! `python/tests/test_testing_workers.py`, under the same names.

mod shop;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde_json::json;
use shop::{Shop, account, checkout, done, env, interrupting, order, service};
use substantial::RunStatus;
use substantial::testing::Env;
use substantial::{Context, Error, Preset, Worker, Workflow, minutes, seconds};

#[tokio::test]
async fn a_crash_after_the_charge_before_it_is_recorded() {
    let (env, shop) = (env().await, Shop::default());
    let (first, second) = (
        service(&env, checkout(&shop)),
        service(&env, checkout(&shop)),
    );
    let hold = env.hold("charge");
    let run = env.start(checkout(&shop), order("a-42")).await.unwrap();
    env.settle().await.unwrap();
    assert_eq!(hold.held(), 1);
    hold.crash();
    assert_eq!(hold.held(), 0);
    env.advance(seconds(34)).await.unwrap();
    assert_eq!(shop.charges(), ["a-42"]);
    env.advance(seconds(2)).await.unwrap();
    assert_eq!(run.result().await.unwrap()["payment"], "pay-a-42");
    assert_eq!(shop.charges(), ["a-42", "a-42"]);
    second.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn claims_are_renewed_and_taken_over_after_the_last_renewal() {
    let (env, shop) = (env().await, Shop::default());
    let (first, second) = (
        service(&env, checkout(&shop)),
        service(&env, checkout(&shop)),
    );
    let hold = env.hold("charge");
    let run = env.start(checkout(&shop), order("a-42")).await.unwrap();
    // Renewed every 15 s, the claim outlives claim_ttl while its worker holds the step.
    env.advance(seconds(61)).await.unwrap();
    assert_eq!(shop.charges(), ["a-42"]);
    hold.crash();
    // The last renewal was at 60 s: the run is taken over 35 s later.
    env.advance(seconds(34)).await.unwrap();
    assert_eq!(shop.charges(), ["a-42"]);
    env.advance(seconds(1)).await.unwrap();
    let at = run.result().await.unwrap()["at"].clone();
    assert_eq!(at, "2030-01-07T09:01:35.001+00:00");
    second.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_paused_workers_step_body_goes_on_to_its_end() {
    let (env, shop) = (env().await, Shop::default());
    let pausing = interrupting(&shop, "pausing", false);
    let worker = Arc::new(service(&env, pausing.clone()));
    shop.worker(&worker);
    let run = env.start(pausing, order("a-42")).await.unwrap();
    env.settle().await.unwrap();
    assert_eq!(shop.charges(), ["a-42"]);
    assert_eq!(run.status().await.unwrap(), RunStatus::Running);
    worker.resume();
    assert_eq!(run.result().await.unwrap(), "pay-a-42");
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_crashed_workers_step_is_interrupted() {
    let (env, shop) = (env().await, Shop::default());
    let abandoning = interrupting(&shop, "abandoning", true);
    let first = Arc::new(service(&env, abandoning.clone()));
    shop.worker(&first);
    let run = env.start(abandoning.clone(), order("a-42")).await.unwrap();
    let started = std::time::Instant::now();
    env.settle().await.unwrap();
    let took = started.elapsed();
    assert!(
        took.as_secs() < 5,
        "settle waited for an interrupted body: {took:?}"
    );
    assert!(!first.is_alive() && first.error().is_none());
    assert!(shop.charges().is_empty());
    let second = service(&env, abandoning);
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), "pay-a-42");
    assert_eq!(shop.charges(), ["a-42"]);
    second.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn two_pods_on_one_entity_never_overlap() {
    let (env, shop) = (env().await, Shop::default());
    let (first, second) = (service(&env, account(&shop)), service(&env, account(&shop)));
    let customer = json!({ "customer": "cus-1" });
    let run = env
        .start(account(&shop), customer)
        .id("customer-cus-1")
        .await
        .unwrap();
    for i in 0..20 {
        let id = format!("op-{i:03}");
        run.send("op", json!({ "id": id })).id(&id).await.unwrap();
    }
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(run.read::<u32>("op.op-019").await.unwrap(), Some(20));
    let expected: Vec<String> = (0..20).map(|i| format!("op-{i:03}")).collect();
    assert_eq!(shop.applied(), expected);
    assert_eq!(shop.in_flight.lock().unwrap().1, 1);
    second.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
    done(env).await;
}

/// An op to the account of `cus-1`, held, then a second pod, the first one paused for `d`.
async fn a_pause_of(d: Duration) -> (Env, Shop) {
    let (env, shop) = (env().await, Shop::default());
    let paused = service(&env, account(&shop));
    let hold = env.hold("apply");
    let customer = json!({ "customer": "cus-1" });
    let run = env
        .start(account(&shop), customer)
        .id("customer-cus-1")
        .await
        .unwrap();
    run.send("op", json!({ "id": "op-1" }))
        .id("op-1")
        .await
        .unwrap();
    env.settle().await.unwrap();
    assert_eq!(hold.held(), 1);
    let other = service(&env, account(&shop));
    paused.pause();
    env.advance(d).await.unwrap();
    let past_the_claim = d > seconds(35);
    if past_the_claim {
        assert_eq!(run.read::<u32>("op.op-1").await.unwrap(), Some(1));
    }
    paused.resume();
    hold.release();
    let then = if past_the_claim {
        Duration::ZERO
    } else {
        minutes(1)
    };
    env.advance(then).await.unwrap();
    assert_eq!(run.read::<u32>("op.op-1").await.unwrap(), Some(1));
    other.shutdown().await.unwrap();
    paused.shutdown().await.unwrap();
    (env, shop)
}

#[tokio::test]
async fn a_paused_pod_does_not_apply_an_op_twice() {
    let (env, shop) = a_pause_of(seconds(65)).await;
    assert_eq!(shop.applied(), ["op-1", "op-1"]);
    done(env).await;
}

#[tokio::test]
async fn a_pause_shorter_than_the_claim_changes_nothing() {
    let (env, shop) = a_pause_of(seconds(20)).await;
    assert_eq!(shop.applied(), ["op-1"]);
    done(env).await;
}

#[tokio::test]
async fn a_shutdown_leaves_held_steps_and_its_runs_to_another_worker() {
    let (env, shop) = (env().await, Shop::default());
    let leaving = service(&env, checkout(&shop));
    let hold = env.hold("charge");
    let run = env.start(checkout(&shop), order("a-42")).await.unwrap();
    env.settle().await.unwrap();
    let other = service(&env, checkout(&shop));
    leaving.shutdown().await.unwrap();
    assert!(hold.held() == 0 && !leaving.is_alive());
    // Its claims are deleted: the other worker takes the run at its next scan, at 30 s.
    env.advance(seconds(30)).await.unwrap();
    let ended = run.result().await.unwrap();
    assert_eq!(ended["payment"], "pay-a-42");
    assert_eq!(ended["at"], "2030-01-07T09:00:30+00:00");
    assert_eq!(shop.charges(), ["a-42", "a-42"]);
    other.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_worker_reports_whether_it_is_alive() {
    let (env, shop) = (env().await, Shop::default());
    // A builder is not a worker yet: a worker is alive from its start.
    let worker = service(&env, checkout(&shop));
    assert!(worker.is_alive() && worker.error().is_none());
    worker.shutdown().await.unwrap();
    assert!(!worker.is_alive() && worker.error().is_none());
    done(env).await;
}

fn napper() -> Workflow<String> {
    Workflow::without_input("napper", |c: Context| async move {
        c.sleep(seconds(1)).await?;
        Ok("woke".to_owned())
    })
}

fn resumer(paused: Arc<OnceLock<Worker>>) -> Workflow<String> {
    Workflow::without_input("resumer", move |c: Context| {
        let paused = paused.clone();
        async move {
            let resume = move || {
                let paused = paused.clone();
                async move {
                    paused.get().expect("a paused worker").resume();
                    Ok::<_, Error>("resumed".to_owned())
                }
            };
            c.step("resume", resume).await
        }
    })
}

#[tokio::test]
async fn a_resume_during_a_settle_does_not_hang_it() {
    let env = env().await;
    let paused: Arc<OnceLock<Worker>> = Arc::default();
    let napping = env
        .worker(napper())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let resuming = env
        .worker(resumer(paused.clone()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let nap = env.start(napper(), ()).id("nap").await.unwrap();
    env.settle().await.unwrap();
    napping.pause();
    let _ = paused.set(napping);
    env.advance(seconds(2)).await.unwrap();
    env.start(resumer(paused.clone()), ())
        .id("res")
        .await
        .unwrap();
    // The step body resumes the paused worker while the settle runs.
    let settled = tokio::time::timeout(Duration::from_secs(5), env.settle()).await;
    assert!(settled.is_ok(), "the settle hung");
    assert_eq!(nap.result().await.unwrap(), "woke");
    resuming.shutdown().await.unwrap();
    paused.get().unwrap().shutdown().await.unwrap();
    env.close().await;
}
