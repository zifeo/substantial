//! What runs cost: prices, charges, `c.cost()` and budgets, in a test environment.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use substantial::testing::Env;
use substantial::{
    Budget, Context, Cost, Error, Preset, Prices, RunStatus, Workflow, charge, minutes, step_key,
};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

fn s3() -> Prices {
    Prices::named("aws-s3-standard-us-east-1-2026-11-09").unwrap()
}

fn usd(nano: u64) -> BTreeMap<String, u64> {
    BTreeMap::from([("USD".to_owned(), nano)])
}

/// A step that charges `nano` nano-dollars.
async fn pay(c: &Context, nano: u64) -> substantial::Result<()> {
    c.step("pay", move || async move { charge("USD", nano) })
        .await
}

/// Pays 6 nano-dollars `n` times, then returns its cost.
fn payer(name: &str) -> Workflow<Cost> {
    Workflow::new(name, |c: Context, n: serde_json::Value| async move {
        for _ in 0..n["n"].as_u64().unwrap() {
            pay(&c, 6).await?;
        }
        c.cost().await
    })
}

fn is_budget_exceeded(e: &Error) -> bool {
    matches!(e, Error::RunFailed { error, .. } if error.kind == "BudgetExceeded")
}

#[tokio::test]
async fn the_named_prices_and_what_prices_refuse() {
    let named = s3();
    assert_eq!(
        named,
        Prices {
            currency: "USD".into(),
            create: 5_000_000,
            get: 400_000,
            list: 5_000_000,
            delete: 0,
        }
    );
    let overridden = Prices {
        get: 500_000,
        ..s3()
    };
    assert_eq!((overridden.get, overridden.list), (500_000, 5_000_000));
    assert!(matches!(Prices::named("zero"), Err(Error::ConfigError(_))));
    let lower = Prices {
        currency: "usd".into(),
        ..s3()
    };
    let refused = Env::open(t0()).prices(lower).await;
    assert!(matches!(refused, Err(Error::ConfigError(_))));
    let past = Prices {
        create: i64::MAX as u64 + 1,
        ..s3()
    };
    let refused = substantial::Substantial::create("memory://prices-past")
        .prices(past)
        .await;
    assert!(
        matches!(refused, Err(Error::ConfigError(_))),
        "{:?}",
        refused.err()
    );
    let fine = Prices {
        create: i64::MAX as u64,
        ..s3()
    };
    substantial::Substantial::create("memory://prices-fine")
        .prices(fine)
        .await
        .unwrap();

    // Three uppercase letters, not a currency: the worker refuses it when it starts.
    let unknown = Prices {
        currency: "QQQ".into(),
        ..s3()
    };
    let env = Env::open(t0()).prices(unknown).await.unwrap();
    let started = env.worker(payer("p")).preset(Preset::Service).start();
    assert!(
        matches!(started, Err(Error::ConfigError(_))),
        "{:?}",
        started.err()
    );
    env.close().await;
}

#[tokio::test]
async fn charges_reach_the_cost_by_step_and_in_total() {
    let env = Env::open(t0()).prices(s3()).await.unwrap();
    let wf = Workflow::without_input("charged", |c| async move {
        c.step("pay", || async {
            charge("USD", 5)?;
            charge("EUR", 3)?;
            charge("USD", 5)
        })
        .await?;
        c.step("fee", || async { charge("USD", 1) }).await?;
        c.cost().await
    });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let cost = env.start(&wf, ()).await.unwrap().result().await.unwrap();
    let pay = BTreeMap::from([("EUR".to_owned(), 3), ("USD".to_owned(), 10)]);
    assert_eq!(
        cost.steps,
        BTreeMap::from([("fee".to_owned(), usd(1)), ("pay".to_owned(), pay)])
    );
    let store = cost.store.amounts["USD"];
    assert!(store > 0, "{cost:?}");
    assert_eq!(cost.amounts["USD"], store + 11);
    assert_eq!(cost.amounts["EUR"], 3);
    assert!(
        cost.requests > 0 && cost.requests == cost.store.requests,
        "{cost:?}"
    );
    assert_eq!(
        cost.requests,
        cost.create + cost.get + cost.list + cost.delete
    );
    assert_eq!(cost.children.requests, 0);
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn charge_refuses_an_amount_past_the_limit_and_runs_only_in_a_step() {
    let env = Env::open(t0()).await.unwrap();
    let wf = Workflow::without_input("past", |c| async move {
        let refused = c
            .step("pay", || async {
                Ok::<_, Error>(matches!(
                    charge("USD", i64::MAX as u64 + 1),
                    Err(Error::InvalidArgument(_))
                ))
            })
            .await?;
        let blocking = c
            .step_blocking("blocking", || {
                charge("USD", 2)?;
                Ok::<_, Error>(step_key())
            })
            .await?;
        Ok((refused, blocking, c.cost().await?.amounts))
    });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let (refused, key, amounts) = env
        .start(&wf, ())
        .id("p")
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(refused);
    assert_eq!(key, "p/blocking/0");
    assert_eq!(amounts, usd(2));
    worker.shutdown().await.unwrap();
    env.close().await;

    let outside = std::panic::catch_unwind(|| charge("USD", 1));
    assert!(outside.is_err(), "charge outside a step panics");
}

#[tokio::test]
async fn an_unknown_currency_fails_the_step_with_invalid_argument() {
    let env = Env::open(t0()).await.unwrap();
    let wf = Workflow::without_input("lowercase", |c| async move {
        let failed = c.step("pay", || async { charge("usd", 1) }).await;
        match failed {
            Err(Error::StepFailed { cause, .. }) => Ok(cause.kind),
            other => Ok(format!("{other:?}")),
        }
    });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let kind = env.start(&wf, ()).await.unwrap().result().await.unwrap();
    assert_eq!(kind, "InvalidArgument");
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn cost_is_recorded_and_replays_the_same() {
    let env = Env::open(t0()).await.unwrap();
    let seen: Arc<Mutex<Vec<u64>>> = Arc::default();
    let s = seen.clone();
    let wf = Workflow::without_input("twice", move |c| {
        let seen = s.clone();
        async move {
            let before = c.cost().await?;
            seen.lock().unwrap().push(before.requests);
            c.sleep(minutes(1)).await?;
            let after = c.cost().await?;
            Ok((before.requests, after.requests))
        }
    });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let run = env.start(&wf, ()).await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    let (before, after) = run.result().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen, [before, before], "the replay got the recorded value");
    assert!(after > before, "{before} then {after}");
    let history = run.history().await.unwrap();
    assert!(history.iter().all(|e| e.kind != "budget_exceeded"));
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_budget_on_requests_passed_raises_and_fails_the_run() {
    let env = Env::open(t0()).await.unwrap();
    let wf = Workflow::without_input("spender", |c| async move {
        let spent = async {
            for i in 0..100 {
                c.step("s", move || async move { Ok::<_, Error>(i) })
                    .await?;
                c.sleep(minutes(1)).await?;
            }
            Ok(0)
        };
        match spent.await {
            Err(Error::BudgetExceeded { meter, limit, used }) => {
                c.publish("over", (&meter, limit, used))?;
                Err(Error::BudgetExceeded { meter, limit, used })
            }
            other => other,
        }
    });
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let budget = Budget::from([("requests", 20)]);
    let run = env.start(&wf, ()).budget(budget).await.unwrap();
    env.advance(minutes(30)).await.unwrap();
    let failed = run.result().await.unwrap_err();
    assert!(is_budget_exceeded(&failed), "{failed:?}");
    assert_eq!(run.status().await.unwrap(), RunStatus::Failed);
    let (meter, limit, used): (String, u64, u64) = run.read("over").await.unwrap().unwrap();
    assert_eq!((meter.as_str(), limit), ("requests", 20));
    assert!(used > 20, "{used}");
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_budget_in_a_currency_is_passed_by_charges() {
    let env = Env::open(t0()).await.unwrap();
    let wf = payer("payer");
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let run = env
        .start(&wf, serde_json::json!({ "n": 3 }))
        .budget([("USD", 10)])
        .await
        .unwrap();
    let failed = run.result().await.unwrap_err();
    assert!(is_budget_exceeded(&failed), "{failed:?}");
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_declared_budget_a_start_overrides_and_the_same_id_with_another() {
    let env = Env::open(t0()).await.unwrap();
    let wf = payer("declared").with_budget([("USD", 10)]);
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let input = serde_json::json!({ "n": 3 });
    let declared = env.start(&wf, input.clone()).id("d").await.unwrap();
    let failed = declared.result().await.unwrap_err();
    assert!(is_budget_exceeded(&failed), "{failed:?}");
    let given = env
        .start(&wf, input.clone())
        .id("g")
        .budget([("USD", 100)])
        .await
        .unwrap();
    assert_eq!(given.result().await.unwrap().amounts, usd(18));
    // The budget is part of the start's identity.
    let again = env
        .start(&wf, input.clone())
        .id("g")
        .budget([("USD", 100)])
        .await;
    assert!(again.is_ok(), "{again:?}");
    let other = env.start(&wf, input.clone()).id("g").await;
    assert!(matches!(other, Err(Error::RunExists { .. })), "{other:?}");
    let malformed = env
        .start(&wf, input.clone())
        .budget([("Requests", 1)])
        .await;
    assert!(
        matches!(malformed, Err(Error::InvalidArgument(_))),
        "{malformed:?}"
    );
    let past = env
        .start(&wf, input.clone())
        .budget([("get", u64::MAX)])
        .await;
    assert!(matches!(past, Err(Error::InvalidArgument(_))), "{past:?}");
    let unknown = env.start(&wf, input).budget([("QQQ", 1)]).await;
    assert!(
        matches!(unknown, Err(Error::InvalidArgument(_))),
        "{unknown:?}"
    );
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_retry_with_a_larger_budget_completes_and_keeps_the_cost_spent() {
    let env = Env::open(t0()).await.unwrap();
    let wf = payer("retried");
    let worker = env.worker(&wf).preset(Preset::Service).start().unwrap();
    let run = env
        .start(&wf, serde_json::json!({ "n": 3 }))
        .budget([("USD", 10)])
        .await
        .unwrap();
    let failed = run.result().await.unwrap_err();
    assert!(is_budget_exceeded(&failed), "{failed:?}");
    run.retry().budget([("USD", 100)]).await.unwrap();
    let again = run.retry().budget([("USD", 1000)]).await;
    assert!(matches!(again, Err(Error::InvalidArgument(_))), "{again:?}");
    // The two payments of the failed run, and the retry's.
    assert_eq!(run.result().await.unwrap().amounts, usd(18));
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_childs_cost_reaches_its_parent() {
    let env = Env::open(t0()).await.unwrap();
    let kid = payer("kid");
    let k = kid.clone();
    let parent = Workflow::without_input("parent", move |c| {
        let k = k.clone();
        async move {
            let child = c.start(&k, serde_json::json!({ "n": 2 })).await?;
            let theirs = child.result().await?;
            Ok((theirs, c.cost().await?))
        }
    });
    let worker = env
        .worker((parent.clone(), kid))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let (theirs, mine) = env
        .start(&parent, ())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(mine.children.amounts, usd(12));
    assert!(mine.children.requests >= theirs.requests, "{mine:?}");
    assert_eq!(mine.amounts, usd(12));
    assert!(mine.steps.is_empty());
    worker.shutdown().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_child_start_past_the_budget_raises_at_once() {
    let env = Env::open(t0()).await.unwrap();
    let kid = payer("kid");
    let k = kid.clone();
    let parent = Workflow::without_input("parent", move |c| {
        let k = k.clone();
        async move {
            pay(&c, 20).await?;
            let started = match c.start(&k, serde_json::json!({ "n": 0 })).await {
                Err(Error::BudgetExceeded { meter, .. }) => meter,
                Ok(_) => "started".into(),
                Err(e) => return Err(e),
            };
            c.publish("start", &started)?;
            Ok(started)
        }
    });
    let worker = env
        .worker((parent.clone(), kid))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&parent, ()).budget([("USD", 10)]).await.unwrap();
    let failed = run.result().await.unwrap_err();
    assert!(is_budget_exceeded(&failed), "{failed:?}");
    let raised: String = run.read("start").await.unwrap().unwrap();
    assert_eq!(raised, "USD");
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn a_run_failed_by_its_budget_or_a_step_gives_the_failure_details() {
    let env = Env::open(t0()).await.unwrap();
    let over = Workflow::without_input("over", |c| async move {
        pay(&c, 1_500_000).await?;
        c.step("after", || async { Ok::<_, Error>(()) }).await
    });
    let declined = Workflow::without_input("declined", |c| async move {
        c.step("pay", || async {
            Err::<(), _>(Error::failed("Declined", "not today"))
        })
        .await
    });
    let worker = env
        .worker((&over, &declined))
        .preset(Preset::Service)
        .start()
        .unwrap();

    let run = env
        .start(&over, ())
        .budget([("USD", 1_000_000)])
        .await
        .unwrap();
    let Err(Error::RunFailed { error, .. }) = run.result().await else {
        panic!("not a RunFailed")
    };
    assert_eq!(error.kind, "BudgetExceeded");
    assert_eq!(
        error.details,
        Some(serde_json::json!({ "meter": "USD", "limit": "1000000", "used": "1500000" }))
    );

    let run = env.start(&declined, ()).await.unwrap();
    let Err(Error::RunFailed { error, cause, .. }) = run.result().await else {
        panic!("not a RunFailed")
    };
    assert_eq!(error.kind, "StepFailed");
    assert_eq!(error.details.as_ref().unwrap()["step"], "pay");
    assert_eq!(cause.unwrap().details, None);
    worker.shutdown().await.unwrap();
    env.close().await;
}
