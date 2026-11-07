//! Workers under pressure: concurrency, shutdown, timers during steps, and two workers racing.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{Hidden, LONG, Recording, eventually, namespaces, script};
use substantial::__internal::{self as internal, Create};
use substantial::{Context, Error, RunStatus, Substantial, Workflow};
use tokio::sync::Notify;

/// A step sleeping `ms` that counts its attempts and the most attempts in flight at once.
#[derive(Clone, Default)]
struct Probe {
    attempts: Arc<AtomicU32>,
    in_flight: Arc<AtomicU32>,
    most: Arc<AtomicU32>,
}

impl Probe {
    fn step(
        &self,
        c: &Context,
        ms: impl Fn(u32) -> u64 + Send + Sync + 'static,
    ) -> substantial::Step<u32> {
        let probe = self.clone();
        let ms = Arc::new(ms);
        c.step("probe", move || {
            let (probe, ms) = (probe.clone(), ms.clone());
            async move {
                let attempt = probe.attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let now = probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                probe.most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(ms(attempt))).await;
                probe.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, Error>(attempt)
            }
        })
    }

    fn workflow(&self, ms: impl Fn(u32) -> u64 + Send + Sync + Clone + 'static) -> Workflow<u32> {
        let probe = self.clone();
        Workflow::without_input("probed", move |c| {
            let (probe, ms) = (probe.clone(), ms.clone());
            async move { probe.step(&c, ms).await }
        })
    }

    fn attempts(&self) -> u32 {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_bounds_the_replays_at_once() {
    for (ns, concurrency) in namespaces().into_iter().zip([1, 2, 2]) {
        let probe = Probe::default();
        let workflow = probe.workflow(|_| 100);
        let sub = internal::create(ns.store).partitions(2).await.unwrap();
        let runs =
            futures::future::try_join_all((0..4).map(|_| sub.start(&workflow, ()).into_future()))
                .await
                .unwrap();
        let worker = script(&sub, &workflow)
            .set("concurrency", concurrency)
            .start()
            .unwrap();
        for run in runs {
            run.result().timeout(LONG).await.unwrap();
        }
        assert_eq!(
            probe.most.load(Ordering::SeqCst),
            concurrency,
            "{}",
            ns.name
        );
        assert_eq!(probe.attempts(), 4);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_lets_a_step_end_within_the_grace() {
    for ns in namespaces() {
        let probe = Probe::default();
        let workflow = probe.workflow(|_| 300);
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, &workflow).start().unwrap();
        let run = sub.start(&workflow, ()).await.unwrap();
        eventually("the step", async || probe.attempts() == 1).await;
        assert_eq!(
            run.status().await.unwrap(),
            RunStatus::Running,
            "{}",
            ns.name
        );
        worker.shutdown().await.unwrap();
        let next = script(&sub, &workflow).start().unwrap();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 1);
        assert_eq!(
            probe.attempts(),
            1,
            "{}: the step ended within the grace",
            ns.name
        );
        next.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_past_the_grace_loses_the_step_in_flight() {
    for ns in namespaces() {
        let probe = Probe::default();
        let workflow = probe.workflow(|attempt| if attempt == 1 { 30_000 } else { 10 });
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = || {
            script(&sub, &workflow)
                .set("shutdown_grace", "100ms")
                .set("claim_ttl", "200ms")
                .hidden("claim_skew", "0ms")
                .start()
                .unwrap()
        };
        let first = worker();
        let run = sub.start(&workflow, ()).await.unwrap();
        eventually("the step", async || probe.attempts() == 1).await;
        let started = Instant::now();
        first.shutdown().await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{}: {:?}",
            ns.name,
            started.elapsed()
        );
        let next = worker();
        assert_eq!(run.result().timeout(LONG).await.unwrap(), 2, "{}", ns.name);
        next.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timer_fires_while_a_step_is_in_flight() {
    let waited = Arc::new(Mutex::new(None::<Duration>));
    let w = waited.clone();
    let workflow = Workflow::without_input("racing", move |c| {
        let w = w.clone();
        async move {
            let started = Instant::now();
            let slow = c.step("slow", || async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                Ok::<_, Error>(())
            });
            let wait = c.wait_until(|| false).timeout(Duration::from_millis(100));
            let (_, held) = tokio::join!(slow.into_future(), async {
                let held = wait.await;
                w.lock().unwrap().get_or_insert(started.elapsed());
                held
            });
            held
        }
    });
    for ns in namespaces() {
        *waited.lock().unwrap() = None;
        let sub = internal::create(ns.store).partitions(1).await.unwrap();
        let worker = script(&sub, &workflow).start().unwrap();
        let run = sub.start(&workflow, ()).await.unwrap();
        assert!(!run.result().timeout(LONG).await.unwrap(), "{}", ns.name);
        let waited = waited.lock().unwrap().unwrap();
        assert!(
            waited < Duration::from_millis(1000),
            "{}: the wait ended after {waited:?}",
            ns.name
        );
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_racing_for_a_commit() {
    for ns in namespaces() {
        let started = Arc::new(Notify::new());
        let attempts = Arc::new(AtomicU32::new(0));
        let (s, a) = (started.clone(), attempts.clone());
        let workflow = Workflow::without_input("raced", move |c| {
            let (s, a) = (s.clone(), a.clone());
            async move {
                let attempt = c
                    .step("work", move || {
                        let (s, a) = (s.clone(), a.clone());
                        async move {
                            s.notify_one();
                            Ok::<_, Error>(a.fetch_add(1, Ordering::SeqCst) + 1)
                        }
                    })
                    .await?;
                Ok(attempt)
            }
        });
        let slow = Recording::new(ns.store.clone());
        *slow.freeze_on_segment.lock().unwrap() = Some(Duration::from_millis(1500));
        let sub_a = internal::create(slow.clone()).partitions(1).await.unwrap();
        let sub_b = internal::open_store(ns.store).await.unwrap();
        let worker = |sub: &Substantial| {
            script(sub, &workflow)
                .set("claim_ttl", "200ms")
                .hidden("claim_skew", "0ms")
                .start()
                .unwrap()
        };
        let a = worker(&sub_a);
        let run = sub_a.start(&workflow, ()).await.unwrap();
        started.notified().await;
        let b = worker(&sub_b);
        let value = sub_b
            .run(&workflow, run.chain())
            .result()
            .timeout(LONG)
            .await
            .unwrap();
        assert_eq!(value, 2, "{}: the second worker's attempt won", ns.name);
        eventually("the frozen commit", async || {
            slow.created()
                .iter()
                .any(|(k, c)| k.contains("/log/") && *c == Create::Exists)
        })
        .await;
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
