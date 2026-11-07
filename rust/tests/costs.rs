//! The store operations of every verb, counted: the numbers of `docs/architecture.md`.
//!
//! Each test runs a verb or a scenario against a store wrapped in `Counting`, which logs every
//! operation, and asserts its count by kind and its round trips. A change of the core or a host
//! that changes a count fails here: update the doc with it.
//!
//! - `COSTS_MEASURE=1 cargo test --test costs -- --nocapture --test-threads 1` prints every row
//!   as measured, `| label | PUT | GET | LIST | DELETE | ops | round trips |`, without asserting.
//! - `COSTS_TRACE=1` also prints each worker operation with its key, to explain a row.

mod common;
#[allow(dead_code)]
mod fake_h3;

use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use common::{Counting, Hidden, Kind, LONG, Op, Tally, retry};
use fake_h3::FakeH3;
use serde::{Deserialize, Serialize};
use substantial::__internal::{self as internal, MemoryStore, Store};
use substantial::{
    Context, Error, IntoWorkflows, Preset, Retry, RunStatus, Step, Substantial, Worker,
    WorkerBuilder, Workflow,
};

/// Long enough that operations sent together overlap, short enough for the timings below.
const LATENCY: Duration = Duration::from_millis(2);
/// No operation for this long: the worker is idle.
const QUIET: Duration = Duration::from_millis(300);
/// A sleep or a timeout, longer than `QUIET`.
const SLEEP: Duration = Duration::from_millis(1500);

fn measuring() -> bool {
    std::env::var("COSTS_MEASURE").is_ok()
}

/// A namespace with a client side and a worker side, each counted on its own.
struct Bench {
    inner: Arc<dyn Store>,
    client: Arc<Counting>,
    workers: Arc<Counting>,
    sub: Substantial,
    wsub: Substantial,
}

fn unique() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "costs-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn bench(partitions: u32) -> Bench {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open(&unique()).unwrap());
    bench_on(inner, partitions).await
}

async fn bench_on(inner: Arc<dyn Store>, partitions: u32) -> Bench {
    let client = Counting::new(inner.clone(), LATENCY);
    let sub = internal::create(client.clone())
        .partitions(partitions)
        .await
        .unwrap();
    // Workers open their own handle: the client's starts and sends do not wake them in the
    // process, the test does, so that scans happen when it says.
    let workers = Counting::new(inner.clone(), LATENCY);
    let wsub = internal::open_store(workers.clone()).await.unwrap();
    Bench {
        inner,
        client,
        workers,
        sub,
        wsub,
    }
}

impl Bench {
    /// The script preset, listing only when woken, at a timer, or after a replay.
    fn worker(&self, workflows: impl IntoWorkflows) -> WorkerBuilder {
        self.wsub
            .worker(workflows)
            .preset(Preset::Script)
            .set("max_idle", "1h")
    }

    /// Starts the worker and lets its first scan pass.
    async fn started(&self, builder: WorkerBuilder) -> Worker {
        let worker = builder.start().unwrap();
        quiet(&self.workers).await;
        worker
    }

    /// The client operations of `action`.
    async fn client<T>(&self, action: impl IntoFuture<Output = T>) -> (T, Tally) {
        let mark = self.client.mark();
        let out = action.await;
        (out, Tally::of(&self.client.since(mark)))
    }

    /// What the worker did since `mark`, once it is idle again.
    async fn since(&self, mark: usize) -> Worked {
        quiet(&self.workers).await;
        let ops = self.workers.since(mark);
        if std::env::var("COSTS_TRACE").is_ok() {
            for op in &ops {
                println!("  {:?} {}", op.kind, op.key);
            }
        }
        worked(&ops)
    }

    /// Keys and bytes held by the namespace, by kind of key.
    async fn stored(&self) -> BTreeMap<String, (usize, usize)> {
        let keys = self.inner.list("", None, 1000, None).await.unwrap().keys;
        let mut kinds: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        for key in &keys {
            let parts: Vec<&str> = key.split('/').collect();
            let kind = match parts[0] {
                "runs" => parts[2],
                other => other,
            };
            let bytes = self.inner.get(key).await.unwrap().map_or(0, |b| b.len());
            let entry = kinds.entry(kind.to_owned()).or_default();
            (entry.0, entry.1) = (entry.0 + 1, entry.1 + bytes);
        }
        println!("stored: {kinds:?}");
        kinds
    }
}

async fn quiet(store: &Counting) {
    loop {
        let before = store.mark();
        tokio::time::sleep(QUIET).await;
        if store.mark() == before {
            return;
        }
    }
}

fn is_scan(op: &Op) -> bool {
    matches!(op.kind, Kind::List | Kind::Wait) && op.key.starts_with("queues/")
}

/// A worker's operations: its replays, each the operations between two scans (a worker of
/// concurrency 1 holds one run at a time), and its scans.
#[derive(Debug)]
struct Worked {
    wakeups: Vec<(String, Tally)>,
    scans: usize,
    scan_lists: usize,
}

fn worked(ops: &[Op]) -> Worked {
    let mut wakeups: Vec<(String, Vec<Op>)> = Vec::new();
    let mut in_scan = true;
    for op in ops {
        if is_scan(op) {
            in_scan = true;
            continue;
        }
        if in_scan {
            let run = op
                .key
                .strip_prefix("runs/")
                .and_then(|k| k.split('/').next())
                .unwrap_or("?")
                .to_owned();
            wakeups.push((run, Vec::new()));
        }
        in_scan = false;
        wakeups.last_mut().unwrap().1.push(op.clone());
    }
    let scans: Vec<Op> = ops.iter().filter(|op| is_scan(op)).cloned().collect();
    Worked {
        wakeups: wakeups
            .into_iter()
            .map(|(run, ops)| (run, Tally::of(&ops)))
            .collect(),
        // The listings of one scan are in flight together: one round trip a scan.
        scans: Tally::of(&scans).trips,
        scan_lists: scans.len(),
    }
}

/// Expected operations: creates (PUT), gets, listings, deletes, round trips.
fn ops(put: usize, get: usize, list: usize, delete: usize, trips: usize) -> Tally {
    Tally {
        put,
        get,
        list,
        wait: 0,
        delete,
        trips,
    }
}

fn row(label: &str, t: &Tally) {
    println!(
        "| {label} | {} | {} | {} | {} | {} | {} |",
        t.put,
        t.get,
        t.list + t.wait,
        t.delete,
        t.ops(),
        t.trips
    );
}

fn check(label: &str, measured: Tally, expected: Tally) {
    row(label, &measured);
    if !measuring() {
        assert_eq!(measured, expected, "{label}");
    }
}

fn check_wakeups(label: &str, w: &Worked, expected: &[Tally]) {
    for (i, (run, t)) in w.wakeups.iter().enumerate() {
        row(&format!("{label}, wakeup {} ({run})", i + 1), t);
    }
    println!("{label}: {} scans, {} listings", w.scans, w.scan_lists);
    let measured: Vec<Tally> = w.wakeups.iter().map(|(_, t)| *t).collect();
    if !measuring() {
        assert_eq!(measured, expected, "{label}");
    }
}

fn step_n(c: &Context, i: u32) -> Step<u32> {
    c.step("s", move || async move { Ok::<_, Error>(i) })
}

#[derive(Serialize, Deserialize)]
struct K {
    k: u32,
}

fn steps() -> Workflow<u32> {
    Workflow::new("steps", |c, input: K| async move {
        for i in 0..input.k {
            step_n(&c, i).await?;
        }
        Ok(input.k)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_create_and_open() {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open(&unique()).unwrap());
    let store = Counting::new(inner, LATENCY);
    let mark = store.mark();
    internal::create(store.clone()).partitions(1).await.unwrap();
    let t = Tally::of(&store.since(mark));
    check("create, new namespace", t, ops(3, 0, 0, 0, 3));
    let mark = store.mark();
    internal::create(store.clone()).partitions(1).await.unwrap();
    let t = Tally::of(&store.since(mark));
    check("create, existing namespace", t, ops(2, 1, 0, 0, 3));
    let mark = store.mark();
    internal::open_store(store.clone()).await.unwrap();
    check("open", Tally::of(&store.since(mark)), ops(1, 1, 0, 0, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_verbs() {
    let b = bench(1).await;
    let wf = steps();
    let (run, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    let run = run.unwrap();
    check("start", t, ops(2, 0, 0, 0, 2));
    let (again, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    again.unwrap();
    check("start, same id and input again", t, ops(2, 1, 0, 0, 3));
    let (taken, t) = b.client(b.sub.start(&wf, K { k: 1 }).id("r")).await;
    assert!(matches!(taken, Err(Error::RunExists { .. })), "{taken:?}");
    check("start, id taken: RunExists", t, ops(1, 1, 0, 0, 2));

    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Pending);
    check("status, pending", t, ops(0, 2, 3, 0, 5));
    let (sent, t) = b.client(run.send("m", 1)).await;
    sent.unwrap();
    check("send", t, ops(2, 2, 0, 0, 4));
    let (cancelled, t) = b.client(run.cancel("no")).await;
    cancelled.unwrap();
    check("cancel", t, ops(2, 2, 0, 0, 4));
    let (read, t) = b.client(run.read::<u32>("progress")).await;
    assert_eq!(read.unwrap(), None);
    check("read, nothing published", t, ops(0, 0, 1, 0, 1));
    let missing = b.sub.run(&wf, "nobody");
    let (sent, t) = b.client(missing.send("m", 1)).await;
    assert!(matches!(sent, Err(Error::RunNotFound { .. })), "{sent:?}");
    check("send, no chain: RunNotFound", t, ops(0, 2, 0, 0, 2));

    // Polls at 0, 10, 30, 70, 150, 310, 630, 1,270, 2,550 and 5,110 ms, the interval doubling up
    // to 30 s: 10 in 6 s.
    let (not_ended, t) = b.client(run.result().timeout(Duration::from_secs(6))).await;
    assert!(
        matches!(not_ended, Err(Error::WaitTimeout)),
        "{not_ended:?}"
    );
    row("result, 6 s of polls, not ended", &t);
    // The first poll gets the chain's first run's `result` and `start`, each later one the
    // `result` of the run the poll before answered.
    assert!((10..=11).contains(&t.get), "{t:?}");
    assert_eq!((t.list, t.put, t.delete), (0, 0, 0), "{t:?}");

    let worker = b.started(b.worker(&wf)).await;
    internal::wake(&worker);
    // The run returns at its first replay, before any call could raise its cancellation.
    let (ended, t) = b.client(run.result().timeout(LONG)).await;
    assert_eq!(ended.unwrap(), 0);
    assert_eq!(
        (t.list, t.put, t.delete),
        (0, 0, 0),
        "every poll reads one key, the first two"
    );
    let (_, t) = b.client(run.result()).await;
    check("result, ended", t, ops(0, 1, 0, 0, 1));
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Completed);
    check("status, ended", t, ops(0, 1, 0, 0, 1));
    let (sent, t) = b.client(run.send("m", 1)).await;
    assert!(matches!(sent, Err(Error::RunEnded { .. })), "{sent:?}");
    check("send, run ended: RunEnded", t, ops(0, 1, 0, 0, 1));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_of_a_suspended_run() {
    let b = bench(1).await;
    let wf = Workflow::without_input("idle", |c| async move {
        step_n(&c, 0).await?;
        c.receive::<u32>("never").await
    });
    let worker = b.started(b.worker(&wf)).await;
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Suspended);
    check("status, suspended", t, ops(0, 2, 2, 0, 4));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steps_and_commit_every() {
    let b = bench(1).await;
    let wf = steps();
    let worker = b.started(b.worker(&wf)).await;
    let mut measured = Vec::new();
    for k in [0, 1, 2, 3] {
        let mark = b.workers.mark();
        let run = b.sub.start(&wf, K { k }).await.unwrap();
        internal::wake(&worker);
        run.result().timeout(LONG).await.unwrap();
        let w = b.since(mark).await;
        assert_eq!(w.wakeups.len(), 1, "{w:?}");
        row(&format!("{k} steps, one wakeup"), &w.wakeups[0].1);
        measured.push(w.wakeups[0].1);
        assert_eq!((w.scans, w.scan_lists), (1, 1), "the scan that found it");
    }
    // The last step is in the end's segment; each step before it is a progress commit, made as
    // the next one starts.
    let end = ops(3, 1, 1, 2, 7);
    check("0 steps, start to end", measured[0], end);
    check("1 step, start to end", measured[1], end);
    let progress = ops(1, 0, 1, 0, 2);
    for k in 2..4 {
        let d = Tally {
            put: measured[k].put - measured[k - 1].put,
            get: measured[k].get - measured[k - 1].get,
            list: measured[k].list - measured[k - 1].list,
            wait: 0,
            delete: measured[k].delete - measured[k - 1].delete,
            trips: measured[k].trips - measured[k - 1].trips,
        };
        check(&format!("step {k} of {k}, a progress commit"), d, progress);
    }
    worker.shutdown().await.unwrap();

    let builtins = Workflow::without_input("builtins", |c| async move {
        c.now().await?;
        c.random().await?;
        c.uuid().await?;
        Ok(0)
    });
    let worker = b.started(b.worker(&builtins)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&builtins, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    check_wakeups("now, random, uuid, end", &w, &[end]);
    worker.shutdown().await.unwrap();

    let grouped = b.started(b.worker(&wf).hidden("commit_every", "1h")).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, K { k: 3 }).await.unwrap();
    internal::wake(&grouped);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    check_wakeups("3 steps, commit_every 1h", &w, &[end]);
    grouped.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_steps_a_sleep_and_the_end() {
    let b = bench(1).await;
    let wf = Workflow::without_input("sleeper", |c| async move {
        for i in 0..3 {
            step_n(&c, i).await?;
        }
        c.sleep(SLEEP).await?;
        Ok(1)
    });
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // The third step lands in the suspension; the worker holds the history for the second wakeup.
    let expected = [ops(5, 1, 3, 2, 11), ops(3, 0, 1, 2, 6)];
    check_wakeups("3 steps, sleep, end", &w, &expected);
    assert_eq!(w.scans, 2, "each wakeup found by a scan");
    let stored = b.stored().await;
    assert_eq!(stored["log"].0, 4);
    assert_eq!(stored.values().map(|(n, _)| n).sum::<usize>(), 8);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_order_waits_for_its_payment() {
    let b = bench(1).await;
    let wf = Workflow::without_input("order", |c| async move {
        let amount: u32 = c.receive("payment").await?;
        for i in 0..2 {
            step_n(&c, i).await?;
        }
        Ok(amount)
    });
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    run.send("payment", 3).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // Nothing to record before the payment: the first wakeup writes no segment.
    let expected = [ops(1, 2, 1, 2, 6), ops(4, 1, 2, 3, 10)];
    check_wakeups("receive, 2 steps, end", &w, &expected);
    assert_eq!(w.scans, 2, "each wakeup found by a scan");
    let stored = b.stored().await;
    assert_eq!(stored["log"].0, 2);
    assert_eq!(stored.values().map(|(n, _)| n).sum::<usize>(), 6);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wait_with_a_timeout_leaves_its_timer() {
    let b = bench(1).await;
    let wf = Workflow::without_input("approval", |c| async move {
        let paid = Arc::new(AtomicBool::new(false));
        let p = paid.clone();
        c.handle("paid", move |()| p.store(true, Ordering::SeqCst))?;
        let ok = c
            .wait_until(move || paid.load(Ordering::SeqCst))
            .timeout(SLEEP)
            .await?;
        Ok(ok)
    });
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    run.send("paid", ()).await.unwrap();
    internal::wake(&worker);
    assert!(run.result().timeout(LONG).await.unwrap());
    tokio::time::sleep(SLEEP).await;
    let w = b.since(mark).await;
    // The end deletes the timer's due entry: no wakeup at its time.
    let expected = [ops(3, 1, 1, 2, 7), ops(3, 1, 1, 4, 9)];
    check_wakeups(
        "wait_until with timeout, message, end, stale timer",
        &w,
        &expected,
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_and_its_result() {
    let b = bench(1).await;
    let kid = Workflow::without_input("kid", |_c| async move { Ok(1u32) });
    let k = kid.clone();
    let parent = Workflow::without_input("parent", move |c| {
        let k = k.clone();
        async move {
            let child = c.start(&k, ()).await?;
            child.result().await
        }
    });
    let worker = b.started(b.worker((parent.clone(), kid))).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&parent, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    let expected = [ops(6, 1, 1, 2, 10), ops(5, 2, 1, 2, 10), ops(3, 1, 1, 3, 8)];
    check_wakeups(
        "parent starts a child, child ends, parent ends",
        &w,
        &expected,
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workflow_sends_and_publishes() {
    let b = bench(1).await;
    let target = Workflow::without_input("target", |c| async move { c.receive::<u32>("go").await });
    let sender = Workflow::without_input("sender", |c| async move {
        c.send("t", "go", 1).await?;
        Ok(0u32)
    });
    let publisher = Workflow::without_input("publisher", |c| async move {
        c.publish("progress", 1)?;
        Ok(0u32)
    });
    let worker = b
        .started(b.worker((target.clone(), sender.clone(), publisher.clone())))
        .await;
    let t = b.sub.start(&target, ()).id("t").await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    let s = b.sub.start(&sender, ()).await.unwrap();
    internal::wake(&worker);
    s.result().timeout(LONG).await.unwrap();
    t.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // The sender's replay reads the target's result and start before its commit, its effect none.
    let expected = [ops(5, 3, 1, 2, 11), ops(3, 1, 1, 3, 8)];
    check_wakeups("a run sends and ends, the target ends", &w, &expected);
    let mark = b.workers.mark();
    let p = b.sub.start(&publisher, ()).id("p").await.unwrap();
    internal::wake(&worker);
    p.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    check_wakeups("a run publishes and ends", &w, &[ops(4, 1, 1, 2, 8)]);
    let (read, t) = b.client(p.read::<u32>("progress")).await;
    assert_eq!(read.unwrap(), Some(1));
    check("read, a value", t, ops(0, 1, 1, 0, 2));
    worker.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct N {
    n: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_as_new_by_hand() {
    let b = bench(1).await;
    let countdown = Workflow::new("countdown", |c, input: N| async move {
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok(0u32)
    });
    let worker = b.started(b.worker(&countdown)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&countdown, N { n: 1 }).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    let expected = [ops(6, 1, 1, 2, 10), ops(3, 1, 1, 2, 7)];
    check_wakeups("continue as new, then end", &w, &expected);
    let (_, t) = b.client(run.result()).await;
    check("result, a chain of 2 runs", t, ops(0, 2, 1, 0, 3));
    worker.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct Polls {
    i: u32,
}

/// A poll then a sleep per iteration: one segment each, the history read again at every wakeup
/// up to the last snapshot, a continuation once it holds `continue_as_new_after` segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_polling_loop_snapshots_and_compresses() {
    let b = bench(1).await;
    let cron = Workflow::without_input("cron", |c| async move {
        let last = c
            .iterate(Polls { i: 0 }, |c, s| async move {
                c.step("poll", || async { Ok::<_, Error>(1) }).await?;
                if s.i == 9 {
                    return Ok(ControlFlow::Break(s));
                }
                c.sleep(Duration::from_millis(400)).await?;
                Ok(ControlFlow::Continue(Polls { i: s.i + 1 }))
            })
            .await?;
        Ok(last.i)
    });
    let builder = b
        .worker(&cron)
        .hidden("snapshot_every", 4)
        .set("continue_as_new_after", "8,8MiB");
    let worker = b.started(builder).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&cron, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // An iteration: PUT 3, LIST 1, DELETE 2, a snapshot's PUT every `snapshot_every` segments,
    // and a GET of the run's `start` at its first wakeup: the worker holds the rest.
    let iteration = |first: usize, snap: usize| ops(3 + snap, first, 1, 2, 6 + first + snap);
    let expected = [
        iteration(1, 0),
        iteration(0, 0),
        iteration(0, 0),
        iteration(0, 1),
        iteration(0, 0),
        iteration(0, 0),
        iteration(0, 0),
        iteration(0, 1),
        // The ninth: 8 segments, the threshold passed, the run continues.
        ops(6, 0, 1, 2, 9),
        // The next run of the chain, from an empty history.
        iteration(1, 0),
        // The last iteration ends the chain.
        ops(3, 0, 1, 2, 6),
    ];
    check_wakeups(
        "polling loop, snapshot_every 4, continue after 8",
        &w,
        &expected,
    );
    let stored = b.stored().await;
    assert_eq!((stored["log"].0, stored["snap"].0), (11, 2));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_and_a_rollback() {
    let b = bench(1).await;
    let tries = Arc::new(AtomicU32::new(0));
    let t = tries.clone();
    let flaky = Workflow::without_input("flaky", move |c| {
        let t = t.clone();
        async move {
            let retry = Retry {
                max_attempts: 2,
                initial: Duration::from_millis(500),
                max: Duration::from_millis(500),
                multiplier: 1.0,
            };
            c.step("s", move || {
                let t = t.clone();
                async move {
                    match t.fetch_add(1, Ordering::SeqCst) {
                        0 => Err(Error::InvalidArgument("once".into())),
                        _ => Ok(1u32),
                    }
                }
            })
            .retry(retry)
            .await
        }
    });
    let undone = Workflow::without_input("undone", |c| async move {
        c.step("charge", || async { Ok::<_, Error>(1u32) })
            .undo(|_v: u32| async { Ok::<_, Error>(()) })
            .await?;
        Err::<u32, _>(c.rollback("no").await)
    });
    let worker = b.started(b.worker((flaky.clone(), undone.clone()))).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&flaky, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // The failed attempt and its backoff land in one suspension.
    let expected = [ops(3, 1, 1, 2, 7), ops(3, 0, 1, 2, 6)];
    check_wakeups(
        "a step fails once, retried after its backoff",
        &w,
        &expected,
    );
    let mark = b.workers.mark();
    let run = b.sub.start(&undone, ()).await.unwrap();
    internal::wake(&worker);
    assert!(run.result().timeout(LONG).await.is_err());
    let w = b.since(mark).await;
    check_wakeups("a step, a rollback undoing it", &w, &[ops(4, 1, 2, 2, 9)]);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_reaches_a_child() {
    let b = bench(1).await;
    let kid = Workflow::without_input("waiter", |c| async move { c.receive::<u32>("never").await });
    let k = kid.clone();
    let parent = Workflow::without_input("canceller", move |c| {
        let k = k.clone();
        async move {
            let _child = c.start(&k, ()).await?;
            c.receive::<u32>("never").await
        }
    });
    let worker = b.started(b.worker((parent.clone(), kid))).await;
    let run = b.sub.start(&parent, ()).await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    run.cancel("stop").await.unwrap();
    internal::wake(&worker);
    assert!(run.result().timeout(LONG).await.is_err());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let w = b.since(mark).await;
    // The parent ends cancelled and cancels its child; the child's end reaches the parent,
    // which has ended: that wakeup reads its history, to forward the message if it continued,
    // and deletes the hints.
    let expected = [ops(5, 3, 2, 3, 13), ops(5, 2, 2, 3, 12), ops(0, 3, 1, 1, 5)];
    check_wakeups("cancel a parent with a child", &w, &expected);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claims_renew_while_a_step_runs() {
    let b = bench(1).await;
    let slow = Workflow::without_input("slow", |c| async move {
        c.step("s", || async {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            Ok::<_, Error>(1u32)
        })
        .await
    });
    let worker = b.started(b.worker(&slow).set("claim_ttl", "400ms")).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&slow, ()).await.unwrap();
    internal::wake(&worker);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Running);
    check("status, running", t, ops(0, 3, 1, 0, 4));
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    let t = w.wakeups[0].1;
    row("a 1.2 s step, claim_ttl 400ms", &t);
    // A renewal every claim_ttl / 2: one claim PUT, and its DELETE once the run commits.
    let renewals = t.put - 3;
    assert!((4..=6).contains(&renewals), "{t:?}");
    assert_eq!(t, ops(3 + renewals, 1, 1, 2 + renewals, 7 + 2 * renewals));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_lists_each_partition_every_max_idle() {
    let b = bench(3).await;
    let (a, z) = (
        steps(),
        Workflow::without_input("other", |_c| async { Ok(0) }),
    );
    let worker = b
        .wsub
        .worker((a, z))
        .preset(Preset::Script)
        .set("max_idle", "200ms")
        .start()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mark = b.workers.mark();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let w = worked(&b.workers.since(mark));
    println!("idle 2 s, max_idle 200ms, 2 workflows, 3 partitions: {w:?}");
    assert!(w.wakeups.is_empty());
    assert!((9..=11).contains(&w.scans), "a scan every max_idle: {w:?}");
    assert_eq!(
        w.scan_lists,
        6 * w.scans,
        "a listing per workflow and partition"
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_h3_a_waiting_listing_replaces_polling() {
    let h3 = FakeH3::start().await;
    let url = h3.url(&unique());
    let inner = internal::open(&url, &FakeH3::credentials(), retry()).unwrap();
    let b = bench_on(inner, 2).await;
    let wf = steps();
    let worker = b
        .wsub
        .worker(&wf)
        .preset(Preset::Script)
        .set("max_idle", "3s")
        .hidden("wait", "1s")
        .start()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mark = b.workers.mark();
    tokio::time::sleep(Duration::from_secs(6)).await;
    let idle = Tally::of(&b.workers.since(mark));
    row("idle 6 s on h3, wait 1s, max_idle 3s, 2 partitions", &idle);
    println!("waiting listings {}, listings {}", idle.wait, idle.list);
    // Per partition, a waiting listing held `wait` then sent again, and a scan every max_idle.
    assert!((10..=14).contains(&idle.wait), "{idle:?}");
    assert!((2..=6).contains(&idle.list), "{idle:?}");
    assert_eq!(idle.ops(), idle.wait + idle.list, "{idle:?}");
    worker.shutdown().await.unwrap();

    // max_idle 1h: only a waiting listing answered by the start can wake the worker in time.
    let worker = b
        .wsub
        .worker(&wf)
        .preset(Preset::Script)
        .set("max_idle", "1h")
        .start()
        .unwrap();
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    let started = Instant::now();
    let run = b.sub.start(&wf, K { k: 1 }).await.unwrap();
    run.result().timeout(LONG).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "woken by the start"
    );
    let w = b.since(mark).await;
    check_wakeups("1 step on h3", &w, &[ops(3, 1, 1, 2, 7)]);
    worker.shutdown().await.unwrap();
}
