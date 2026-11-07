//! The test environment of docs/testing.md: holds of steps, deploys, cancels and messages at a
//! run's end. The ports of `python/tests/test_testing_workers.py`, under the same names.

mod shop;

use std::time::Duration;

use serde_json::json;
use shop::{Order, Shop, account, done, env, order, remind, retrying, service};
use substantial::{Context, Error, Permanent, RunStatus, Workflow, days, minutes, seconds};

#[tokio::test]
async fn a_held_attempt_released_before_its_timeout_counts_once() {
    for (early, attempts) in [(Duration::from_millis(1), 1), (Duration::ZERO, 2)] {
        let (env, shop) = (env().await, Shop::default());
        let worker = service(&env, retrying(&shop));
        let hold = env.hold("charge");
        let run = env.start(retrying(&shop), order("a-42")).await.unwrap();
        env.advance(minutes(5) - early).await.unwrap();
        // At 5:00 the attempt has timed out: the release comes too late.
        assert_eq!(hold.held(), usize::from(!early.is_zero()));
        hold.release();
        env.advance(seconds(1)).await.unwrap();
        assert_eq!(run.result().await.unwrap(), "pay-a-42");
        assert_eq!(shop.charges().len(), attempts);
        worker.shutdown().await.unwrap();
        env.close().await;
    }
}

fn refunded(shop: &Shop) -> Workflow<String> {
    let shop = shop.clone();
    Workflow::new("refunded", move |c: Context, input: Order| {
        let shop = shop.clone();
        async move {
            let order = input.order;
            let saga = c
                .saga(|c| async move {
                    let (refunds, charging) = (shop.charges.clone(), order.clone());
                    c.step("charge", move || shop.charge(charging.clone()))
                        .undo(move |payment: String| {
                            let refunds = refunds.clone();
                            async move {
                                refunds.lock().unwrap().push(format!("refund {payment}"));
                                Ok::<_, Error>(())
                            }
                        })
                        .await?;
                    let ship = || async { Err::<(), _>(Permanent::new("no carrier")) };
                    c.step("ship", ship).await
                })
                .await;
            match saga {
                Ok(()) => Ok("shipped".to_owned()),
                Err(Error::StepFailed { .. }) => Ok("refunded".to_owned()),
                Err(e) => Err(e),
            }
        }
    })
}

#[tokio::test]
async fn a_hold_takes_the_next_attempts_and_undos() {
    let (env, shop) = (env().await, Shop::default());
    let worker = service(&env, refunded(&shop));
    let (charged, undone) = (env.hold("charge").times(2), env.hold("charge.undo"));
    let first = env.start(refunded(&shop), order("a")).await.unwrap();
    let second = env.start(refunded(&shop), order("b")).await.unwrap();
    env.settle().await.unwrap();
    assert!(charged.held() == 2 && undone.held() == 0);
    charged.release();
    env.settle().await.unwrap();
    assert_eq!(undone.held(), 1);
    undone.release();
    assert_eq!(first.result().await.unwrap(), "refunded");
    assert_eq!(second.result().await.unwrap(), "refunded");
    let mut charges = shop.charges();
    charges.sort();
    assert_eq!(charges, ["a", "b", "refund pay-a", "refund pay-b"]);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_deploy_mid_wait_keeps_old_runs_on_old_code() {
    let env = env().await;
    let old_worker = service(&env, remind(None));
    let old = env
        .start(remind(None), json!({ "account": "acc-1" }))
        .await
        .unwrap();
    env.advance(days(10)).await.unwrap();
    old_worker.shutdown().await.unwrap();
    let worker = service(&env, (remind(None), remind(Some(2))));
    let new = env
        .start(remind(Some(2)), json!({ "account": "acc-2" }))
        .await
        .unwrap();
    env.advance(days(30)).await.unwrap();
    assert_eq!(old.result().await.unwrap(), "reminded acc-1");
    assert_eq!(new.result().await.unwrap(), "reminded acc-2, v2");
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_cancel_landing_with_the_timer() {
    let env = env().await;
    let worker = service(&env, remind(None));
    let client = env.client().await.unwrap();
    let own = env
        .start(remind(None), json!({ "account": "acc-1" }))
        .await
        .unwrap();
    let other = env
        .start(remind(None), json!({ "account": "acc-2" }))
        .await
        .unwrap();
    env.advance(days(30) - Duration::from_millis(1))
        .await
        .unwrap();
    own.cancel("account deleted").await.unwrap();
    let from_another_pod = client.run(remind(None), other.chain());
    from_another_pod.cancel("account deleted").await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(own.status().await.unwrap(), RunStatus::Cancelled);
    // The cancel from another pod reaches the store first; the replay at the timer sees it.
    assert_eq!(other.status().await.unwrap(), RunStatus::Cancelled);
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_message_to_a_run_that_just_ended() {
    let env = env().await;
    let worker = service(&env, remind(None));
    let run = env
        .start(remind(None), json!({ "account": "acc-1" }))
        .await
        .unwrap();
    env.advance(days(30)).await.unwrap();
    let sent = run.send("upgraded", ()).id("upgrade-acc-1").await;
    assert!(matches!(sent, Err(Error::RunEnded { .. })), "{sent:?}");
    assert_eq!(run.result().await.unwrap(), "reminded acc-1");
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_redelivered_webhook_applies_once() {
    let (env, shop) = (env().await, Shop::default());
    let worker = service(&env, account(&shop));
    let customer = json!({ "customer": "cus-1" });
    let run = env
        .start(account(&shop), customer.clone())
        .id("customer-cus-1");
    let run = run.await.unwrap();
    let again = env
        .start(account(&shop), customer)
        .id("customer-cus-1")
        .await
        .unwrap();
    assert_eq!(run.chain(), again.chain());
    for _ in 0..2 {
        again
            .send("op", json!({ "id": "op-1" }))
            .id("evt-1")
            .await
            .unwrap();
    }
    env.advance(minutes(1)).await.unwrap();
    assert_eq!(shop.applied(), ["op-1"]);
    worker.shutdown().await.unwrap();
    done(env).await;
}
