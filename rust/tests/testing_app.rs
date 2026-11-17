//! The test environment of docs/testing.md: an application given `env.client()`, whose lifespan
//! starts its own worker and leaves it. The ports of `python/tests/test_testing_app.py`, under
//! the same names, with the application's routes called in process.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use substantial::testing::Env;
use substantial::{Context, Error, Preset, Result, Substantial, Worker, Workflow, seconds};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

type Charged = Arc<Mutex<Vec<String>>>;

#[derive(Deserialize)]
struct Order {
    order: String,
}

fn fulfil(charged: &Charged) -> Workflow<String> {
    let charged = charged.clone();
    Workflow::new("fulfil", move |c: Context, input: Order| {
        let charged = charged.clone();
        async move {
            let order = input.order;
            let capture = move || {
                let (charged, order) = (charged.clone(), order.clone());
                async move {
                    charged.lock().unwrap().push(order.clone());
                    Ok::<_, Error>(format!("cap-{order}"))
                }
            };
            c.step("capture", capture).await
        }
    })
}

/// The shop: POST /orders starts a run, `?wait=1` waits 5 s for its result; GET /orders/<id>
/// reads its status. Its lifespan opens its engine and starts its worker, then leaves it.
struct App {
    sub: Substantial,
    worker: Worker,
    fulfil: Workflow<String>,
}

impl App {
    async fn start(engine: impl Future<Output = Result<Substantial>>, charged: &Charged) -> Self {
        let sub = engine.await.unwrap();
        let fulfil = fulfil(charged);
        let worker = sub
            .worker(fulfil.clone())
            .preset(Preset::Service)
            .start()
            .unwrap();
        Self {
            sub,
            worker,
            fulfil,
        }
    }

    async fn request(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
        let (path, query) = path.split_once('?').unwrap_or((path, ""));
        if method == "POST" {
            let order = body["order"].as_str().unwrap().to_owned();
            let run = self.sub.start(self.fulfil.clone(), body).id(&order);
            let run = run.await.unwrap();
            if query != "wait=1" {
                return (202, Value::Null);
            }
            return match run.result().timeout(seconds(5)).await {
                Ok(value) => (200, json!(value)),
                Err(Error::WaitTimeout) => (202, Value::Null),
                Err(e) => panic!("{e}"),
            };
        }
        let order = path.rsplit('/').next().unwrap();
        let status = self
            .sub
            .run(self.fulfil.clone(), order)
            .status()
            .await
            .unwrap();
        (200, json!({ "status": status.to_string() }))
    }

    async fn stop(self) {
        self.worker.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn an_asgi_app_given_a_client() {
    let (env, charged) = (Env::open(t0()).await.unwrap(), Charged::default());
    let app = App::start(env.client(), &charged).await;
    for _ in 0..2 {
        let posted = app
            .request("POST", "/orders", json!({ "order": "o-1" }))
            .await;
        assert_eq!(posted, (202, Value::Null));
    }
    let pending = json!({ "status": "pending" });
    assert_eq!(
        app.request("GET", "/orders/o-1", Value::Null).await,
        (200, pending)
    );
    env.settle().await.unwrap();
    let completed = json!({ "status": "completed" });
    assert_eq!(
        app.request("GET", "/orders/o-1", Value::Null).await,
        (200, completed)
    );
    assert_eq!(
        env.run(fulfil(&charged), "o-1").result().await.unwrap(),
        "cap-o-1"
    );
    app.stop().await;
    assert_eq!(*charged.lock().unwrap(), ["o-1"]);
    env.check().await.unwrap();
    env.close().await;
}

#[tokio::test]
async fn an_endpoint_that_waits_answers_at_once_when_the_step_is_slow() {
    let (env, charged) = (Env::open(t0()).await.unwrap(), Charged::default());
    let app = App::start(env.client(), &charged).await;
    env.hold("capture");
    let posted = app
        .request("POST", "/orders?wait=1", json!({ "order": "o-1" }))
        .await;
    assert_eq!(posted, (202, Value::Null));
    assert_eq!(env.now(), t0());
    app.stop().await;
    env.check().await.unwrap();
    env.close().await;
}
