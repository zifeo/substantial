//! The test environment of docs/testing.md: what the seed draws, ids and randoms, and the faults
//! of Chaos. The ports of `python/tests/test_testing.py` and `test_testing_faults.py` on seeds,
//! under the same names.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use substantial::testing::{Chaos, Env, Faults};
use substantial::{Context, Error, Preset, Workflow, minutes, seconds};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

fn randoms() -> Workflow<(f64, String)> {
    Workflow::without_input("randoms", |c| async move {
        c.step("nothing", || async { Ok::<_, Error>(()) }).await?;
        Ok((c.random().await?, c.uuid().await?))
    })
}

type Receipt = BTreeMap<String, serde_json::Value>;

#[derive(Deserialize)]
struct Order {
    order: String,
}

#[derive(Deserialize)]
struct Capture {
    capture: String,
}

fn order() -> Workflow<Receipt> {
    Workflow::new("order", |c: Context, input: Order| async move {
        let o = input.order;
        let reserved = o.clone();
        let reserve = move || std::future::ready(Ok::<_, Error>(format!("stock-{reserved}")));
        let stock = c.step("reserve", reserve).await?;
        let paid: i64 = c.receive("paid").await?;
        let capture = move || std::future::ready(Ok::<_, Error>(format!("cap-{o}")));
        let capture_id = c.step("capture", capture).await?;
        let receipt = c
            .start(&receipt_of(), json!({ "capture": capture_id }))
            .await?;
        let receipt = receipt.result().await?;
        Ok(Receipt::from([
            ("stock".into(), json!(stock)),
            ("paid".into(), json!(paid)),
            ("receipt".into(), json!(receipt)),
        ]))
    })
}

fn receipt_of() -> Workflow<String> {
    Workflow::new("receipt_of", |c: Context, input: Capture| async move {
        c.sleep(seconds(10)).await?;
        let mail = move || std::future::ready(Ok::<_, Error>(format!("mailed {}", input.capture)));
        c.step("mail", mail).await
    })
}

type Chaotic = (Faults, Vec<String>, Vec<Receipt>, Vec<(usize, u32, String)>);

async fn chaotic(seed: u64, rate: f64, workers: usize) -> Chaotic {
    let env = Env::open(t0()).chaos(Chaos { seed, rate }).await.unwrap();
    let pods: Vec<_> = (0..workers)
        .map(|_| {
            env.worker((order(), receipt_of()))
                .preset(Preset::Service)
                .start()
                .unwrap()
        })
        .collect();
    let mut runs = Vec::new();
    for i in 0..3 {
        runs.push(
            env.start(order(), json!({ "order": format!("o-{i}") }))
                .await
                .unwrap(),
        );
    }
    for run in &runs {
        run.send("paid", 42).await.unwrap();
    }
    env.advance(minutes(30)).await.unwrap();
    let mut results = Vec::new();
    for run in &runs {
        results.push(run.result().await.unwrap());
    }
    for pod in &pods {
        pod.shutdown().await.unwrap();
    }
    env.check().await.unwrap();
    // A worker Chaos replaced has an id of its own.
    let ids = env.__workers();
    let unique: std::collections::HashSet<&String> = ids.iter().map(|(_, _, id)| id).collect();
    assert_eq!(unique.len(), ids.len());
    let faults = env.faults();
    assert_eq!(ids.iter().all(|(_, r, _)| *r == 0), faults.crashed == 0);
    let chains = runs.iter().map(|r| r.chain().to_owned()).collect();
    env.close().await;
    (faults, chains, results, ids)
}

async fn survives(seed: u64) {
    let (faults, _, results, _) = chaotic(seed, 0.05, 1).await;
    let expected: Vec<Receipt> = (0..3)
        .map(|i| {
            Receipt::from([
                ("stock".into(), json!(format!("stock-o-{i}"))),
                ("paid".into(), json!(42)),
                ("receipt".into(), json!(format!("mailed cap-o-{i}"))),
            ])
        })
        .collect();
    assert_eq!(results, expected);
    assert!(
        faults.failed + faults.ambiguous + faults.crashed > 0,
        "{faults:?}"
    );
}

macro_rules! seeds {
    ($($name:ident = $seed:literal),* $(,)?) => {
        mod an_order_survives_any_fault {
            $(
                #[tokio::test]
                async fn $name() {
                    super::survives($seed).await;
                }
            )*
        }
    };
}

seeds!(
    seed_0 = 0,
    seed_1 = 1,
    seed_2 = 2,
    seed_3 = 3,
    seed_4 = 4,
    seed_5 = 5,
    seed_6 = 6,
    seed_7 = 7,
    seed_8 = 8,
    seed_9 = 9,
    seed_10 = 10,
    seed_11 = 11,
    seed_12 = 12,
    seed_13 = 13,
    seed_14 = 14,
    seed_15 = 15,
    seed_16 = 16,
    seed_17 = 17,
    seed_18 = 18,
    seed_19 = 19,
    seed_20 = 20,
    seed_21 = 21,
    seed_22 = 22,
    seed_23 = 23,
    seed_24 = 24,
    seed_25 = 25,
    seed_26 = 26,
    seed_27 = 27,
    seed_28 = 28,
    seed_29 = 29,
    seed_30 = 30,
    seed_31 = 31,
    seed_32 = 32,
    seed_33 = 33,
    seed_34 = 34,
    seed_35 = 35,
    seed_36 = 36,
    seed_37 = 37,
    seed_38 = 38,
    seed_39 = 39,
    seed_40 = 40,
    seed_41 = 41,
    seed_42 = 42,
    seed_43 = 43,
    seed_44 = 44,
    seed_45 = 45,
    seed_46 = 46,
    seed_47 = 47,
    seed_48 = 48,
    seed_49 = 49,
);

#[tokio::test]
async fn the_same_seed_replays_the_same_faults_and_ids() {
    let first = chaotic(11, 0.2, 2).await;
    let faults = first.0;
    assert!(
        faults.failed > 0 && faults.ambiguous > 0 && faults.crashed > 0,
        "{faults:?}"
    );
    assert_eq!(first, chaotic(11, 0.2, 2).await);
    let other = chaotic(12, 0.2, 2).await;
    assert_ne!((first.0, &first.1), (other.0, &other.1));
}

type Ids = (String, (f64, String), String, String);

/// The ids and randoms of one run, by one worker or by a second one after the first crashed.
async fn ids(chaos: Option<Chaos>, takeover: bool) -> Ids {
    let mut open = Env::open(t0());
    if let Some(chaos) = chaos {
        open = open.chaos(chaos);
    }
    let env = open.await.unwrap();
    let _first = env
        .worker(randoms())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let _second = env
        .worker(randoms())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let hold = takeover.then(|| env.hold("nothing"));
    let run = env.start(randoms(), ()).await.unwrap();
    env.settle().await.unwrap();
    if let Some(hold) = hold {
        hold.crash();
        env.advance(minutes(1)).await.unwrap();
    }
    let result = run.result().await.unwrap();
    let workers = env.__workers();
    env.close().await;
    (
        run.chain().to_owned(),
        result,
        workers[0].2.clone(),
        workers[1].2.clone(),
    )
}

#[tokio::test]
async fn ids_and_randoms_are_drawn_from_the_seed_and_what_they_name() {
    let plain = ids(None, false).await;
    assert_eq!(plain, ids(None, false).await);
    assert_eq!(plain, ids(Some(Chaos { seed: 0, rate: 0.0 }), true).await);
    let seeded = ids(Some(Chaos { seed: 7, rate: 0.0 }), false).await;
    assert_ne!(plain.0, seeded.0);
    assert_ne!(plain.1.0, seeded.1.0);
    assert_ne!(plain.1.1, seeded.1.1);
    assert_ne!(plain.2, seeded.2);
    assert_ne!(plain.3, seeded.3);
}
