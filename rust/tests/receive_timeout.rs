//! `c.receive(name).timeout(d)`: the next message of a name, or `WaitTimeout` once the timeout
//! passed with none. The ports of `python/tests/test_receive_timeout.py`, under the same names.

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};
use substantial::testing::Env;
use substantial::{Context, Error, Preset, Var, Workflow, minutes};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

fn receiver() -> Workflow<Value> {
    Workflow::without_input("receiver", |c| async move {
        match c.receive::<Value>("note").timeout(minutes(5)).await {
            Ok(value) => Ok(json!({"value": value})),
            Err(e @ Error::WaitTimeout) => {
                Ok(json!({"timeout": e.to_string(), "at": c.now().await?.to_rfc3339()}))
            }
            Err(e) => Err(e),
        }
    })
}

#[tokio::test]
async fn a_message_within_the_timeout_is_returned() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(receiver())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&receiver(), ()).await.unwrap();
    env.advance(minutes(4)).await.unwrap();
    run.send("note", "hello").await.unwrap();
    assert_eq!(run.result().await.unwrap(), json!({"value": "hello"}));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn a_none_value_is_returned_as_such() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(receiver())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&receiver(), ()).await.unwrap();
    run.send("note", ()).await.unwrap();
    assert_eq!(run.result().await.unwrap(), json!({"value": null}));
    worker.shutdown().await.unwrap();
    done(env).await;
}

#[tokio::test]
async fn none_within_the_timeout_raises_wait_timeout_at_its_instant() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env
        .worker(receiver())
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&receiver(), ()).await.unwrap();
    env.advance(minutes(5)).await.unwrap();
    // § j6dvbbdrq.a@m3jxz
    assert_eq!(
        run.result().await.unwrap(),
        json!({"timeout": "the timeout passed first", "at": "2030-01-07T09:05:00+00:00"})
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn interleaved() -> Workflow<Value> {
    Workflow::without_input("interleaved", |c| async move {
        let seen = Var::new(Vec::<u32>::new());
        c.handle("n", seen.handler(|seen, n: u32| seen.push(n)))?;
        let first: u32 = c.receive("n").await?;
        let second: u32 = c.receive("n").timeout(minutes(5)).await?;
        let timed_out = match c.receive::<u32>("n").timeout(minutes(5)).await {
            Err(Error::WaitTimeout) => true,
            other => return Err(Error::fail("Unexpected", format!("{other:?}"))),
        };
        let third: u32 = c.receive("n").await?;
        let fourth: u32 = c.receive("n").timeout(minutes(1)).await?;
        Ok(json!([first, second, timed_out, third, fourth, seen.get()]))
    })
}

#[tokio::test]
async fn receives_with_and_without_timeout_interleave_in_order() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker(interleaved()))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&interleaved(), ()).await.unwrap();
    run.send("n", 1).await.unwrap();
    run.send("n", 2).await.unwrap();
    env.advance(minutes(6)).await.unwrap();
    run.send("n", 3).await.unwrap();
    run.send("n", 4).await.unwrap();
    assert_eq!(
        run.result().await.unwrap(),
        json!([1, 2, true, 3, 4, [1, 2, 3, 4]])
    );
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn pair() -> Workflow<Value> {
    Workflow::without_input("pair", |c| async move {
        let (a, b) = futures::join!(
            c.receive::<u32>("n").timeout(minutes(5)).into_future(),
            c.receive::<u32>("n").timeout(minutes(5)).into_future(),
        );
        let named = |r: Result<u32, Error>| match r {
            Ok(n) => json!(n),
            Err(Error::WaitTimeout) => json!("WaitTimeout"),
            Err(e) => json!(e.to_string()),
        };
        Ok(json!([named(a), named(b)]))
    })
}

#[tokio::test]
async fn two_receives_waiting_together_take_one_message_each() {
    let env = Env::open(t0()).await.unwrap();
    let worker = env.worker(pair()).preset(Preset::Service).start().unwrap();
    let run = env.start(&pair(), ()).await.unwrap();
    env.advance(minutes(1)).await.unwrap();
    run.send("n", 1).await.unwrap();
    env.advance(minutes(5)).await.unwrap();
    assert_eq!(run.result().await.unwrap(), json!([1, "WaitTimeout"]));
    worker.shutdown().await.unwrap();
    done(env).await;
}

/// A receive polled once, so pending, then one of the other kind.
fn mixed() -> Workflow<Value> {
    Workflow::new("mixed", |c: Context, input: Value| async move {
        let refused = if input["timed_first"] == json!(true) {
            let mut timed = c.receive::<u32>("n").timeout(minutes(5)).into_future();
            let _ = futures::poll!(&mut timed);
            c.receive::<u32>("n").await
        } else {
            let mut plain = c.receive::<u32>("n").into_future();
            let _ = futures::poll!(&mut plain);
            c.receive::<u32>("n").timeout(minutes(5)).await
        };
        match refused {
            Err(Error::InvalidArgument(message)) => Ok(json!(message)),
            other => Ok(json!(format!("accepted: {other:?}"))),
        }
    })
}

#[tokio::test]
async fn a_receive_with_and_one_without_timeout_pending_at_once_are_refused() {
    for timed_first in [true, false] {
        let env = Env::open(t0()).await.unwrap();
        let worker = env.worker(mixed()).preset(Preset::Service).start().unwrap();
        let input = json!({"timed_first": timed_first});
        let run = env.start(&mixed(), input).await.unwrap();
        let result = run.result().await.unwrap();
        assert!(
            result.as_str().unwrap().contains("pending at once"),
            "{result}"
        );
        worker.shutdown().await.unwrap();
        done(env).await;
    }
}
