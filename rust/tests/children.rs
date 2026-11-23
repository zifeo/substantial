//! Children (I-11), loops and continue-as-new (I-12), end to end.

mod common;

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use chrono::{TimeZone, Utc};
use common::{LONG, namespaces, script};
use serde::{Deserialize, Serialize};
use substantial::__internal as internal;
use substantial::testing::Env;
use substantial::{Context, Error, Permanent, Preset, RunStatus, Workflow, step_key};

#[derive(Serialize, Deserialize)]
struct N {
    n: u32,
}

fn double() -> Workflow<u32> {
    Workflow::new("double", |c: Context, input: N| async move {
        if input.n == 0 {
            c.step("refuse", || async { Err::<(), _>(Permanent::new("zero")) })
                .await?;
        }
        Ok(input.n * 2)
    })
}

fn listener() -> Workflow<String> {
    Workflow::without_input(
        "listener",
        |c| async move { c.receive::<String>("hurry").await },
    )
}

fn parent() -> Workflow<(u32, String, String, String)> {
    Workflow::without_input("parent", |c| async move {
        let ok = c.start(&double(), N { n: 21 }).id("kid-ok").await?;
        let failing = c.start(&double(), N { n: 0 }).await?;
        // § b7g8bsqaq.b@tqnn8
        let listening = c.start(&listener(), ()).await?;
        listening.send("hurry", "now").await?;
        let heard: String = listening.result().await?;
        // § end b7g8bsqaq.b
        let value: u32 = ok.result().await?;
        let failed = match failing.result().await {
            Err(Error::RunFailed {
                status: RunStatus::Failed,
                error,
                ..
            }) => error.kind,
            other => return Err(Error::InvalidArgument(format!("{other:?}"))),
        };
        Ok((value, failed, ok.chain().to_owned(), heard))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_awaits_its_children() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (parent(), double(), listener()))
            .start()
            .unwrap();
        let run = sub.start(&parent(), ()).await.unwrap();
        let (value, failed, id, heard) = run.result().timeout(LONG).await.unwrap();
        assert_eq!(
            (value, failed.as_str(), id.as_str()),
            (42, "StepFailed", "kid-ok")
        );
        assert_eq!(heard, "now", "{}", ns.name);
        let child: u32 = sub
            .run(double(), "kid-ok")
            .result()
            .timeout(LONG)
            .await
            .unwrap();
        assert_eq!(child, 42, "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[derive(Serialize, Deserialize)]
struct Count {
    i: u32,
}

#[derive(Serialize, Deserialize)]
struct Floats {
    x: Vec<f64>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_state_that_is_not_json_blocks_a_compression_not_compressible() {
    let opaque = Workflow::without_input("opaque", |c| async move {
        c.iterate(Floats { x: Vec::new() }, |c, mut s| async move {
            c.step("tick", || async { Ok::<_, Error>(()) }).await?;
            s.x.push(f64::NAN);
            Ok(ControlFlow::<Floats, _>::Continue(s))
        })
        .await?;
        Ok(())
    });
    let ns = namespaces().remove(0);
    let sub = internal::create(ns.store).await.unwrap();
    let worker = script(&sub, &opaque)
        .set("continue_as_new_after", 0)
        .start()
        .unwrap();
    let run = sub.start(&opaque, ()).await.unwrap();
    let error = common::blocked(&sub, &opaque, run.chain()).await;
    assert_eq!(error.kind, "NotCompressible", "{error}");
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_compresses_at_every_boundary() {
    let keys = Arc::new(Mutex::new(Vec::<String>::new()));
    let k = keys.clone();
    let counter = Workflow::without_input("counter", move |c| {
        let keys = k.clone();
        async move {
            let last = c
                .iterate(Count { i: 0 }, |c, s| {
                    let keys = keys.clone();
                    async move {
                        let key = c
                            .step("tick", || async { Ok::<_, Error>(step_key()) })
                            .await?;
                        keys.lock().unwrap().push(key);
                        Ok(if s.i == 3 {
                            ControlFlow::Break(s)
                        } else {
                            ControlFlow::Continue(Count { i: s.i + 1 })
                        })
                    }
                })
                .await?;
            Ok(last.i)
        }
    });
    for ns in namespaces() {
        keys.lock().unwrap().clear();
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &counter)
            .set("continue_as_new_after", 0)
            .start()
            .unwrap();
        let run = sub.start(&counter, ()).id("counter-1").await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 3);
        let keys = keys.lock().unwrap().clone();
        assert_eq!(
            keys.len(),
            4,
            "{}: one tick per iteration: {keys:?}",
            ns.name
        );
        assert_eq!(keys[0], "counter-1/tick/0");
        let mut runs: Vec<&str> = keys.iter().map(|k| k.split('/').next().unwrap()).collect();
        runs.dedup();
        // § one db0mrbxkk@4cnc0
        assert_eq!(runs.len(), 4, "{}: a run per iteration: {keys:?}", ns.name);
        assert!(keys.iter().all(|k| k.ends_with("/tick/0")), "{keys:?}");
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continue_as_new_by_hand() {
    let countdown = Workflow::new("countdown", |c: Context, input: N| async move {
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok("liftoff".to_owned())
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &countdown).start().unwrap();
        let run = sub.start(&countdown, N { n: 3 }).await.unwrap();
        // § ng8x15zmr@7f7bc
        let value: String = run.result().timeout(LONG).await.unwrap();
        assert_eq!(value, "liftoff", "{}", ns.name);
        // § one f91jrjyjy.a@n8xb9
        assert_eq!(run.status().await.unwrap(), RunStatus::Completed);
        worker.shutdown().await.unwrap();
    }
}

fn waiter() -> Workflow<(bool, bool, u32)> {
    Workflow::without_input("waiter", |c| async move {
        let child = c.start(&double(), N { n: 2 }).await?;
        let before = child.done();
        let kid = child.clone();
        c.wait_until(move || kid.done()).await?;
        // A replay after a sleep sees the child ended again, before its result is asked.
        c.sleep(std::time::Duration::from_millis(50)).await?;
        let after = child.done();
        Ok((before, after, child.result().await?))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_waits_until_a_child_is_done() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (waiter(), double())).start().unwrap();
        let run = sub.start(&waiter(), ()).await.unwrap();
        // § yb61qtt3s.c@np72k
        let done = run.result().timeout(LONG).await.unwrap();
        assert_eq!(done, (false, true, 4), "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_state_is_an_object() {
    let scalar = Workflow::without_input("scalar", |c| async move {
        let looped = c
            .iterate(0u32, |_c, s| async move { Ok(ControlFlow::Break(s)) })
            .await;
        Ok(matches!(looped, Err(Error::InvalidArgument(_))))
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, &scalar).start().unwrap();
        let run = sub.start(&scalar, ()).await.unwrap();
        // § ev8h2ngd8@28jwr
        assert!(run.result().timeout(LONG).await.unwrap(), "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_id_taken_by_another_run_fails_the_child() {
    let w = Workflow::without_input("adopter", |c| async move {
        let kid = c.start(&double(), N { n: 21 }).id("taken").await?;
        match kid.result().await {
            Err(Error::RunFailed {
                status: RunStatus::Failed,
                error,
                ..
            }) => Ok(error.kind),
            other => Err(Error::InvalidArgument(format!("{other:?}"))),
        }
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (w.clone(), double())).start().unwrap();
        let taken = sub.start(&double(), N { n: 1 }).id("taken").await.unwrap();
        assert_eq!(taken.result().timeout(LONG).await.unwrap(), 2);
        let run = sub.start(&w, ()).await.unwrap();
        // § yb61qtt3s.d@wgsst
        let kind = run.result().timeout(LONG).await.unwrap();
        assert_eq!(kind, "RunExists", "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_children_with_one_id_from_one_run_raise_invalid_argument() {
    let twins = Workflow::without_input("twins", |c| async move {
        let first = c.start(&double(), N { n: 21 }).id("twin").await?;
        let second = c.start(&double(), N { n: 21 }).id("twin").await;
        let refused = matches!(second, Err(Error::InvalidArgument(_)));
        Ok((refused, first.result().await?))
    });
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (twins.clone(), double())).start().unwrap();
        let run = sub.start(&twins, ()).await.unwrap();
        // § yb61qtt3s.b@j0f4g
        let (refused, value) = run.result().timeout(LONG).await.unwrap();
        assert!(refused, "{}", ns.name);
        assert_eq!(value, 42, "{}: the first child stands", ns.name);
        // § end yb61qtt3s.b
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_message_reaching_a_run_as_it_continues_goes_to_the_next_run() {
    let env = Env::open(Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap())
        .await
        .unwrap();
    let looping = Workflow::new("looping", |c: Context, input: N| async move {
        if input.n == 0 {
            c.step("work", || async { Ok::<_, Error>(()) }).await?;
            return Err(c.continue_as_new(N { n: 1 }).await);
        }
        c.receive::<String>("m").await
    });
    let worker = env
        .worker(&looping)
        .preset(Preset::Service)
        .start()
        .unwrap();
    let hold = env.hold("work");
    let run = env.start(&looping, N { n: 0 }).id("l").await.unwrap();
    env.settle().await.unwrap();
    assert_eq!(hold.held(), 1);
    // § dnxwdv0ac.a@tyvdw
    run.send("m", "hello").await.unwrap();
    assert_eq!(
        env.__keys("queues/looping/due/19700101t000000000z~l~m")
            .len(),
        1,
        "among the chain's messages, whatever its run"
    );
    hold.release();
    assert_eq!(run.result().await.unwrap(), "hello");
    let history = run.history().await.unwrap();
    let received: Vec<&str> = (history.iter())
        .filter(|e| e.kind == "message")
        .map(|e| e.run.as_str())
        .collect();
    assert_eq!(received.len(), 1, "{history:?}");
    assert_ne!(received[0], "l", "{history:?}");
    assert!(
        env.__keys("queues/looping/due/19700101t000000000z~l~m")
            .is_empty()
    );
    // § end dnxwdv0ac.a
    worker.shutdown().await.unwrap();
    env.check().await.unwrap();
    env.close().await;
}

fn named() -> Workflow<String> {
    Workflow::new("named", |c: Context, input: N| async move {
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok(c.chain().to_owned())
    })
}

fn naming() -> Workflow<(String, String, String)> {
    Workflow::without_input("naming", |c| async move {
        let child = c.start(&named(), N { n: 1 }).await?;
        let theirs = child.result().await?;
        Ok((c.chain().to_owned(), child.chain().to_owned(), theirs))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_knows_its_chain_through_its_continuations() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let worker = script(&sub, (named(), naming())).start().unwrap();
        let run = sub.start(&named(), N { n: 2 }).await.unwrap();
        // § ng8x15zmr.a@bzg10
        assert_eq!(run.result().timeout(LONG).await.unwrap(), run.chain());
        let run = sub.start(&naming(), ()).id("naming-1").await.unwrap();
        let (own, child, theirs) = run.result().timeout(LONG).await.unwrap();
        assert_eq!(own, "naming-1", "{}", ns.name);
        assert_eq!(child, theirs, "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}
