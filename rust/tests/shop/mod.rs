//! The shop of the worker tests of docs/testing.md: its fakes and its workflows.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use substantial::testing::Env;
use substantial::{Context, Error, IntoWorkflows, Preset, Retry, Worker, Workflow, days, seconds};

pub fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

pub async fn env() -> Env {
    Env::open(t0()).await.unwrap()
}

pub async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

pub type List<T> = Arc<Mutex<Vec<T>>>;

/// The PSP's charges and the ledger's ops, as the fakes of a shop would record them.
#[derive(Clone, Default)]
pub struct Shop {
    pub charges: List<String>,
    applied: List<String>,
    /// Ops applying now, and the most at once.
    pub in_flight: Arc<Mutex<(u32, u32)>>,
    /// The worker the next attempt of `abandoning` crashes, or `pausing` pauses, from its body.
    worker: Arc<Mutex<Option<Arc<Worker>>>>,
}

impl Shop {
    pub fn charges(&self) -> Vec<String> {
        self.charges.lock().unwrap().clone()
    }

    pub fn applied(&self) -> Vec<String> {
        self.applied.lock().unwrap().clone()
    }

    pub fn charge(&self, order: String) -> impl Future<Output = Result<String, Error>> + use<> {
        let charges = self.charges.clone();
        async move {
            charges.lock().unwrap().push(order.clone());
            Ok(format!("pay-{order}"))
        }
    }

    pub async fn apply(&self, op: String) -> Result<(), Error> {
        {
            let mut n = self.in_flight.lock().unwrap();
            n.0 += 1;
            n.1 = n.1.max(n.0);
        }
        tokio::task::yield_now().await;
        self.applied.lock().unwrap().push(op);
        self.in_flight.lock().unwrap().0 -= 1;
        Ok(())
    }

    pub fn worker(&self, worker: &Arc<Worker>) {
        *self.worker.lock().unwrap() = Some(worker.clone());
    }
}

#[derive(Deserialize)]
pub struct Order {
    pub order: String,
}

pub fn checkout(shop: &Shop) -> Workflow<BTreeMap<String, String>> {
    let shop = shop.clone();
    Workflow::new("checkout", move |c: Context, input: Order| {
        let shop = shop.clone();
        async move {
            let order = input.order;
            let payment = c.step("charge", move || shop.charge(order.clone())).await?;
            let at = c.now().await?.to_rfc3339();
            Ok(BTreeMap::from([
                ("payment".into(), payment),
                ("at".into(), at),
            ]))
        }
    })
}

pub fn retrying(shop: &Shop) -> Workflow<String> {
    let shop = shop.clone();
    Workflow::new("retrying", move |c: Context, input: Order| {
        let shop = shop.clone();
        async move {
            let once = Retry {
                max_attempts: 3,
                initial: seconds(1),
                max: seconds(1),
                multiplier: 1.0,
            };
            let order = input.order;
            c.step("charge", move || shop.charge(order.clone()))
                .retry(once)
                .await
        }
    })
}

#[derive(Deserialize)]
pub struct Op {
    id: String,
}

/// An actor: applies each op it is sent, in order, and publishes the balance after it.
pub fn account(shop: &Shop) -> Workflow<()> {
    let shop = shop.clone();
    Workflow::new("account", move |c: Context, _: serde_json::Value| {
        let shop = shop.clone();
        async move {
            let ops: List<String> = List::default();
            let pushed = ops.clone();
            c.handle("op", move |op: Op| pushed.lock().unwrap().push(op.id))?;
            let mut balance = 0;
            loop {
                let (done, seen) = (balance, ops.clone());
                c.wait_until(move || seen.lock().unwrap().len() > done)
                    .await?;
                let op = ops.lock().unwrap()[done].clone();
                let (shop, applying) = (shop.clone(), op.clone());
                c.step("apply", move || {
                    let (shop, op) = (shop.clone(), applying.clone());
                    async move { shop.apply(op).await }
                })
                .await?;
                balance += 1;
                c.publish(&format!("op.{op}"), balance)?;
            }
        }
    })
}

/// Charges once its attempt crashed (`crashing`) or paused the worker of the shop from its body.
pub fn interrupting(shop: &Shop, name: &str, crashing: bool) -> Workflow<String> {
    let shop = shop.clone();
    Workflow::new(name, move |c: Context, input: Order| {
        let shop = shop.clone();
        async move {
            let order = input.order;
            c.step("charge", move || {
                let (shop, order) = (shop.clone(), order.clone());
                async move {
                    let worker = shop.worker.lock().unwrap().take();
                    match worker {
                        Some(w) if crashing => {
                            w.crash();
                            // Interrupted here: the crash abandoned this attempt.
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        }
                        Some(w) => w.pause(),
                        None => {}
                    }
                    tokio::task::yield_now().await;
                    shop.charge(order).await
                }
            })
            .await
        }
    })
}

#[derive(Deserialize)]
pub struct Account {
    account: String,
}

pub fn remind(version: Option<u32>) -> Workflow<String> {
    let w = Workflow::new("remind", move |c: Context, input: Account| async move {
        c.sleep(days(30)).await?;
        let v2 = if version.is_some() { ", v2" } else { "" };
        Ok(format!("reminded {}{v2}", input.account))
    });
    match version {
        Some(v) => w.with_version(v),
        None => w,
    }
}

pub fn service(env: &Env, workflows: impl IntoWorkflows) -> Worker {
    env.worker(workflows)
        .preset(Preset::Service)
        .start()
        .unwrap()
}

pub fn order(id: &str) -> serde_json::Value {
    json!({ "order": id })
}
