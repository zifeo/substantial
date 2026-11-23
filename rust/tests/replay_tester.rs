//! The replay tester (I-19): runs recorded on a store, replayed by new code in an environment.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use common::{eventually, retry, script};
use substantial::__internal::{self as internal, Store};
use substantial::testing::{ReplayOutcome, replay};
use substantial::{Context, Credentials, Error, RunStatus, Substantial, Workflow, hours};

/// `charge`, a sleep of an hour, `ship`; `reserve` first when `reserving`. Counts the attempts of
/// its steps in `ran`.
fn order(reserving: bool, ran: &Arc<AtomicU32>) -> Workflow<String> {
    let ran = ran.clone();
    Workflow::without_input("order", move |c: Context| {
        let ran = ran.clone();
        async move {
            let step = |name: &'static str| {
                let ran = ran.clone();
                c.step(name, move || {
                    ran.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<_, Error>(()) }
                })
            };
            if reserving {
                step("reserve").await?;
            }
            step("charge").await?;
            c.sleep(hours(1)).await?;
            step("ship").await?;
            Ok("shipped".to_owned())
        }
    })
}

fn actor() -> Workflow<u32> {
    Workflow::without_input("actor", |c| async move {
        let go: u32 = c.receive("go").await?;
        Ok(go)
    })
}

fn quick() -> Workflow<u32> {
    Workflow::without_input("quick", |_c| async { Ok(1) })
}

async fn keys(store: &dyn Store) -> Vec<(String, Vec<u8>)> {
    let mut all = Vec::new();
    let listed = store.list("", None, 10_000, None).await.unwrap().keys;
    for key in listed {
        let bytes = store.get(&key).await.unwrap().map(|o| o.bytes);
        all.push((key, bytes.unwrap_or_default().to_vec()));
    }
    all
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_code_replays_recorded_runs_and_writes_nothing_to_them() {
    let url = format!("memory://replay-{}", hex::encode(rand::random::<[u8; 8]>()));
    let sub = Substantial::create(url.as_str()).await.unwrap();
    let ran = Arc::new(AtomicU32::new(0));
    let worker = script(&sub, (order(false, &ran), actor(), quick()))
        .start()
        .unwrap();
    let suspended = sub.start(order(false, &ran), ()).id("a").await.unwrap();
    let waiting = sub.start(actor(), ()).id("b").await.unwrap();
    let ended = sub.start(quick(), ()).id("c").await.unwrap();
    assert_eq!(ended.result().await.unwrap(), 1);
    for run in [&suspended] {
        eventually("a suspended order", async || {
            run.status().await.unwrap() == RunStatus::Suspended
        })
        .await;
    }
    eventually("a waiting actor", async || {
        waiting.status().await.unwrap() == RunStatus::Suspended
    })
    .await;
    worker.shutdown().await.unwrap();
    let source = internal::open(&url, &Credentials::None, retry()).unwrap();
    let before = keys(&*source).await;

    let replayed_ran = Arc::new(AtomicU32::new(0));
    let same = (order(false, &replayed_ran), actor(), quick());
    let replayed = replay(&url, same).runs(["a", "b", "c"]).await.unwrap();
    let outcomes: Vec<(&str, &ReplayOutcome)> = replayed
        .iter()
        .map(|r| (r.run.as_str(), &r.outcome))
        .collect();
    // § awhhtbr5n@nfcrt
    assert!(
        matches!(
            outcomes[..],
            [
                ("a", ReplayOutcome::Matched),
                ("b", ReplayOutcome::Matched),
                ("c", ReplayOutcome::Ended),
            ]
        ),
        "{outcomes:?}"
    );
    // § end awhhtbr5n
    assert_eq!(replayed_ran.load(Ordering::SeqCst), 0, "no step ran");

    let changed = order(true, &replayed_ran);
    let replayed = replay(&url, changed).runs(["a"]).await.unwrap();
    match &replayed[..] {
        [r] if r.run == "a" => match &r.outcome {
            ReplayOutcome::Diverged(Error::NonDeterminism(m)) => {
                assert!(m.contains("charge"), "{m}")
            }
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
    // § awhhtbr5n.b@gzp6x
    let ran = replayed_ran.load(Ordering::SeqCst);
    assert_eq!(ran, 0, "no step ran");
    // § end awhhtbr5n.b
    // § awhhtbr5n.d@qq7pz
    let after = keys(&*source).await;
    assert_eq!(after, before, "the source is as it was");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_names_runs_its_workflows_serve() {
    let url = format!("memory://replay-{}", hex::encode(rand::random::<[u8; 8]>()));
    let sub = Substantial::create(url.as_str()).await.unwrap();
    sub.start(quick(), ()).id("c").await.unwrap();
    // § awhhtbr5n.c@kmg87
    let none: [&str; 0] = [];
    assert!(matches!(
        replay(&url, quick()).runs(none).await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        replay(&url, quick()).runs(["nope"]).await,
        Err(Error::RunNotFound { .. })
    ));
    assert!(matches!(
        replay(&url, actor()).runs(["c"]).await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        replay(&url, quick().with_version(2)).runs(["c"]).await,
        Err(Error::InvalidArgument(_))
    ));
    let missing = format!("memory://replay-{}", hex::encode(rand::random::<[u8; 8]>()));
    assert!(matches!(
        replay(&missing, quick()).runs(["c"]).await,
        Err(Error::LayoutError(_))
    ));
    let replayed = replay(&url, quick()).runs(["c"]).await.unwrap();
    assert!(
        matches!(replayed[0].outcome, ReplayOutcome::Matched),
        "{replayed:?}"
    );
}
