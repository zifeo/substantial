//! Children (I-11), loops and continue-as-new (I-12), end to end.

mod common;

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use common::{LONG, namespaces, script};
use serde::{Deserialize, Serialize};
use substantial::__internal as internal;
use substantial::{Context, Error, Permanent, RunStatus, Workflow, step_key};

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
        let listening = c.start(&listener(), ()).await?;
        listening.send("hurry", "now").await?;
        let heard: String = listening.result().await?;
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, &countdown).start().unwrap();
        let run = sub.start(&countdown, N { n: 3 }).await.unwrap();
        let value: String = run.result().timeout(LONG).await.unwrap();
        assert_eq!(value, "liftoff", "{}", ns.name);
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, (waiter(), double())).start().unwrap();
        let run = sub.start(&waiter(), ()).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, &scalar).start().unwrap();
        let run = sub.start(&scalar, ()).await.unwrap();
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, (w.clone(), double())).start().unwrap();
        let taken = sub.start(&double(), N { n: 1 }).id("taken").await.unwrap();
        assert_eq!(taken.result().timeout(LONG).await.unwrap(), 2);
        let run = sub.start(&w, ()).await.unwrap();
        let kind = run.result().timeout(LONG).await.unwrap();
        assert_eq!(kind, "RunExists", "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
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
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, (named(), naming())).start().unwrap();
        let run = sub.start(&named(), N { n: 2 }).await.unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), run.chain());
        let run = sub.start(&naming(), ()).id("naming-1").await.unwrap();
        let (own, child, theirs) = run.result().timeout(LONG).await.unwrap();
        assert_eq!(own, "naming-1", "{}", ns.name);
        assert_eq!(child, theirs, "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}
