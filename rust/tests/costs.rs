//! The store operations of every verb, counted: the numbers of `docs/architecture.md`.
//!
//! Each test runs a verb or a scenario against a store wrapped in `Counting`, which logs every
//! operation, and asserts its count by kind and its round trips. A change of the core or a host
//! that changes a count fails here: update the doc with it. For a run's wakeups, it also asserts
//! that the store requests the run's last segment records, its chain's through it, are the ones
//! counted.
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
use futures::TryStreamExt;
use prost::Message;
use serde::{Deserialize, Serialize};
use substantial::__internal::{self as internal, MemoryStore, Store, layout};
use substantial::{
    Context, Error, IntoWorkflows, Preset, Prices, Retry, RunStatus, Step, Substantial, Worker,
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

async fn bench() -> Bench {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open(&unique()).unwrap());
    bench_on(inner).await
}

async fn bench_on(inner: Arc<dyn Store>) -> Bench {
    let client = Counting::new(inner.clone(), LATENCY);
    let sub = internal::create(client.clone()).await.unwrap();
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

    /// The cost the last segment of `run` records: the chain's through it.
    async fn recorded(&self, run: &str) -> layout::Cost {
        let prefix = format!("runs/{run}/log/");
        let keys = self
            .inner
            .list(&prefix, None, 1000, None)
            .await
            .unwrap()
            .keys;
        let n = |k: &String| k[prefix.len()..].parse::<u64>().unwrap();
        let last = keys.iter().max_by_key(|k| n(k)).expect("a segment");
        let bytes = self.inner.get(last).await.unwrap().unwrap();
        let segment = layout::Segment::decode(bytes).unwrap();
        segment.cost.expect("a segment records its cost")
    }

    /// Asserts that the store requests the last segment of `run` records are the ones the
    /// wakeups of `runs` sent, as counted: those of `run` and of the runs of its chain before it.
    async fn check_recorded(&self, label: &str, w: &Worked, run: &str, runs: &[&str]) {
        self.check_requests(label, run, counted(&w.wakeups, runs))
            .await;
    }

    /// Asserts that the last segment of `run` records the store requests `counted`.
    async fn check_requests(&self, label: &str, run: &str, counted: layout::Requests) {
        let recorded = requests(&self.recorded(run).await.store.unwrap_or_default());
        println!("{label}, recorded by {run}: {recorded:?}, counted: {counted:?}");
        if !measuring() {
            assert_eq!(recorded, counted, "{label}: recorded by {run}");
        }
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

fn requests(meters: &layout::Meters) -> layout::Requests {
    meters.requests.unwrap_or_default()
}

/// The store requests of the wakeups of `runs`, as counted.
fn counted(wakeups: &[(String, Tally)], runs: &[&str]) -> layout::Requests {
    let mut counted = layout::Requests::default();
    for (_, t) in wakeups.iter().filter(|(r, _)| runs.contains(&r.as_str())) {
        counted.create += t.put as u64;
        counted.get += t.get as u64;
        counted.list += (t.list + t.wait) as u64;
        counted.delete += t.delete as u64;
    }
    counted
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
    internal::create(store.clone()).await.unwrap();
    let t = Tally::of(&store.since(mark));
    check("create, new namespace", t, ops(3, 0, 0, 0, 3));
    let mark = store.mark();
    internal::create(store.clone()).await.unwrap();
    let t = Tally::of(&store.since(mark));
    check("create, existing namespace", t, ops(2, 1, 0, 0, 3));
    let mark = store.mark();
    internal::open_store(store.clone()).await.unwrap();
    check("open", Tally::of(&store.since(mark)), ops(1, 1, 0, 0, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_verbs() {
    let b = bench().await;
    let wf = steps();
    let (run, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    let run = run.unwrap();
    // The start, its due entry, and a GET of the id's tombstone.
    check("start", t, ops(2, 1, 0, 0, 3));
    let (again, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    again.unwrap();
    check("start, same id and input again", t, ops(2, 2, 0, 0, 4));
    let (taken, t) = b.client(b.sub.start(&wf, K { k: 1 }).id("r")).await;
    assert!(matches!(taken, Err(Error::RunExists { .. })), "{taken:?}");
    check("start, id taken: RunExists", t, ops(1, 1, 0, 0, 2));

    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Pending);
    // The chain's first run's result and start, then one listing of the run.
    check("status, pending", t, ops(0, 2, 1, 0, 3));
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
    // The first run's result and start, then its tombstone.
    check("send, no chain: RunNotFound", t, ops(0, 3, 0, 0, 3));

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
    let b = bench().await;
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
    check("status, suspended", t, ops(0, 2, 1, 0, 3));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steps_and_commit_every() {
    let b = bench().await;
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
        let label = format!("{k} steps, one wakeup");
        row(&label, &w.wakeups[0].1);
        measured.push(w.wakeups[0].1);
        assert_eq!((w.scans, w.scan_lists), (1, 1), "the scan that found it");
        b.check_recorded(&label, &w, run.chain(), &[run.chain()])
            .await;
    }
    // The last step is in the end's segment; each step before it is a progress commit, made as
    // the next one starts. The end creates the run's collection entry and lists its watchers.
    let end = ops(4, 1, 2, 2, 9);
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
    let label = "now, random, uuid, end";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    worker.shutdown().await.unwrap();

    let grouped = b.started(b.worker(&wf).hidden("commit_every", "1h")).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, K { k: 3 }).await.unwrap();
    internal::wake(&grouped);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    check_wakeups("3 steps, commit_every 1h", &w, &[end]);
    let label = "3 steps, commit_every 1h";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    grouped.shutdown().await.unwrap();
}

/// The store requests of a run, priced by the S3 table: 5,000 nano-dollars a PUT or a LIST, 400
/// a GET, deletes free.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_requests_priced() {
    let inner: Arc<dyn Store> = Arc::new(MemoryStore::open(&unique()).unwrap());
    let prices = || Prices::named("aws-s3-standard-us-east-1-2026-11-09").unwrap();
    let client = Counting::new(inner.clone(), LATENCY);
    let sub = internal::create(client.clone())
        .prices(prices())
        .await
        .unwrap();
    let workers = Counting::new(inner.clone(), LATENCY);
    let wsub = internal::open_store(workers.clone())
        .prices(prices())
        .await
        .unwrap();
    let b = Bench {
        inner,
        client,
        workers,
        sub,
        wsub,
    };
    let wf = Workflow::without_input("priced", |c| async move {
        step_n(&c, 0).await?;
        c.sleep(SLEEP).await?;
        step_n(&c, 1).await
    });
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    let label = "a step, a sleep, a step, priced";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    let (mut put, mut get, mut list) = (0, 0, 0);
    for (_, t) in &w.wakeups {
        (put, get, list) = (put + t.put, get + t.get, list + t.list + t.wait);
    }
    let store = b.recorded(run.chain()).await.store.unwrap_or_default();
    let expected = layout::Amount {
        currency: "USD".into(),
        nano: (5000 * (put + list) + 400 * get) as u64,
    };
    println!("{label}: {:?}, expected {expected:?}", store.amounts);
    if !measuring() {
        assert_eq!(store.amounts, vec![expected], "{label}");
    }
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_steps_a_sleep_and_the_end() {
    let b = bench().await;
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
    let expected = [ops(5, 1, 3, 2, 11), ops(4, 0, 2, 2, 8)];
    check_wakeups("3 steps, sleep, end", &w, &expected);
    assert_eq!(w.scans, 2, "each wakeup found by a scan");
    let label = "3 steps, sleep, end";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    let stored = b.stored().await;
    assert_eq!(stored["log"].0, 4);
    assert_eq!(stored["queues"].0, 1, "the collection entry");
    assert_eq!(stored.values().map(|(n, _)| n).sum::<usize>(), 9);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_order_waits_for_its_payment() {
    let b = bench().await;
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
    // Nothing to record before the payment: the first wakeup writes an empty segment, so the
    // run reads suspended, and creates its rest marker instead of a due entry; its end deletes it.
    let expected = [ops(3, 1, 1, 2, 7), ops(5, 1, 3, 4, 13)];
    check_wakeups("receive, 2 steps, end", &w, &expected);
    assert_eq!(w.scans, 2, "each wakeup found by a scan");
    let label = "receive, 2 steps, end";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    let stored = b.stored().await;
    assert_eq!(stored["log"].0, 3);
    assert_eq!(stored.values().map(|(n, _)| n).sum::<usize>(), 8);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wait_with_a_timeout_leaves_its_timer() {
    let b = bench().await;
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
    let expected = [ops(3, 1, 1, 2, 7), ops(4, 1, 2, 4, 11)];
    let label = "wait_until with timeout, message, end, stale timer";
    check_wakeups(label, &w, &expected);
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_and_its_result() {
    let b = bench().await;
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
    // The child's start reads its id's tombstone.
    let expected = [
        ops(7, 2, 1, 2, 12),
        ops(6, 2, 2, 2, 12),
        ops(4, 1, 2, 4, 11),
    ];
    let label = "parent starts a child, child ends, parent ends";
    check_wakeups(label, &w, &expected);
    let (parent, kid) = (run.chain(), w.wakeups[1].0.as_str());
    b.check_recorded(label, &w, parent, &[parent]).await;
    b.check_recorded(label, &w, kid, &[kid]).await;
    // The child's end carried its total, which the parent added to its children.
    let (theirs, mine) = (b.recorded(kid).await, b.recorded(parent).await);
    assert_eq!(
        requests(&mine.children.unwrap_or_default()),
        requests(&theirs.store.unwrap_or_default()),
        "{label}: the child's total in the parent's children"
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workflow_sends_and_publishes() {
    let b = bench().await;
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
    // The target's first wakeup is before the one counted, its cost after.
    let first = b.workers.mark();
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
    let expected = [ops(6, 3, 2, 2, 13), ops(4, 1, 2, 4, 11)];
    let label = "a run sends and ends, the target ends";
    check_wakeups(label, &w, &expected);
    let all = worked(&b.workers.since(first));
    b.check_recorded(label, &all, s.chain(), &[s.chain()]).await;
    b.check_recorded(label, &all, "t", &["t"]).await;
    let mark = b.workers.mark();
    let p = b.sub.start(&publisher, ()).id("p").await.unwrap();
    internal::wake(&worker);
    p.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    check_wakeups("a run publishes and ends", &w, &[ops(5, 1, 2, 2, 10)]);
    b.check_recorded("a run publishes and ends", &w, "p", &["p"])
        .await;
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
    let b = bench().await;
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
    let expected = [ops(7, 1, 2, 2, 12), ops(4, 1, 2, 2, 9)];
    let label = "continue as new, then end";
    check_wakeups(label, &w, &expected);
    // The next run starts from the first's total.
    let (first, next) = (run.chain(), w.wakeups[1].0.as_str());
    b.check_recorded(label, &w, first, &[first]).await;
    b.check_recorded(label, &w, next, &[first, next]).await;
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
    let b = bench().await;
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
        ops(7, 0, 2, 2, 11),
        // The next run of the chain, from an empty history.
        iteration(1, 0),
        // The last iteration ends the chain.
        ops(4, 0, 2, 2, 8),
    ];
    let label = "polling loop, snapshot_every 4, continue after 8";
    check_wakeups(label, &w, &expected);
    let (first, next) = (run.chain(), w.wakeups[9].0.as_str());
    b.check_recorded(label, &w, first, &[first]).await;
    b.check_recorded(label, &w, next, &[first, next]).await;
    let stored = b.stored().await;
    assert_eq!((stored["log"].0, stored["snap"].0), (11, 2));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_and_a_rollback() {
    let b = bench().await;
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
    let expected = [ops(3, 1, 1, 2, 7), ops(4, 0, 2, 2, 8)];
    let label = "a step fails once, retried after its backoff";
    check_wakeups(label, &w, &expected);
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    let mark = b.workers.mark();
    let run = b.sub.start(&undone, ()).await.unwrap();
    internal::wake(&worker);
    assert!(run.result().timeout(LONG).await.is_err());
    let w = b.since(mark).await;
    let label = "a step, a rollback undoing it";
    check_wakeups(label, &w, &[ops(5, 1, 3, 2, 11)]);
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_reaches_a_child() {
    let b = bench().await;
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
    let first = b.workers.mark();
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
    let expected = [ops(6, 3, 3, 4, 16), ops(6, 2, 3, 4, 15), ops(0, 3, 1, 1, 5)];
    let label = "cancel a parent with a child";
    check_wakeups(label, &w, &expected);
    let all = worked(&b.workers.since(first));
    let (parent, kid) = (run.chain(), all.wakeups[1].0.as_str());
    // A cancellation's fan-out to children counts for no run: the listing of the run's index of
    // children, then for each child its `result` and `start`, its message and its due entry.
    let fan_out = |children: u64| layout::Requests {
        create: 2 * children,
        get: 2 * children,
        list: 1,
        delete: 0,
    };
    let less = |a: layout::Requests, b: layout::Requests| layout::Requests {
        create: a.create - b.create,
        get: a.get - b.get,
        list: a.list - b.list,
        delete: a.delete - b.delete,
    };
    // The parent's last wakeup, after its end, writes no segment.
    let (after_end, before) = all.wakeups.split_last().unwrap();
    assert_eq!(after_end.0, parent, "{label}: {all:?}");
    let mine = less(counted(before, &[parent]), fan_out(1));
    b.check_requests(label, parent, mine).await;
    let theirs = less(counted(&all.wakeups, &[kid]), fan_out(0));
    b.check_requests(label, kid, theirs).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_cancels_its_child() {
    let b = bench().await;
    let kid = Workflow::without_input("waiter", |c| async move { c.receive::<u32>("go").await });
    let (k, l) = (kid.clone(), kid.clone());
    let canceller = Workflow::without_input("canceller", move |c| {
        let k = k.clone();
        async move {
            let child = c.start(&k, ()).await?;
            child.cancel("stop").await?;
            match child.result().await {
                Err(Error::RunFailed {
                    status: RunStatus::Cancelled,
                    ..
                }) => Ok(0u32),
                other => Err(Error::failed("Unexpected", format!("{other:?}"))),
            }
        }
    });
    let sender = Workflow::without_input("sender", move |c| {
        let l = l.clone();
        async move {
            let child = c.start(&l, ()).await?;
            child.send("go", 1).await?;
            child.result().await
        }
    });
    let worker = b
        .started(b.worker((canceller.clone(), sender.clone(), kid)))
        .await;
    // The same parent sending its child a message instead: the cancel costs what the send does.
    let mark = b.workers.mark();
    let run = b.sub.start(&sender, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // Before its commit, the send reads the child's `result`, `start` and `gone`, the child's
    // start not created yet: GET 3 and PUT 2 over the row of a child started alone.
    let expected = [
        ops(9, 5, 1, 2, 17),
        ops(6, 3, 2, 4, 15),
        ops(4, 1, 2, 4, 11),
    ];
    let label = "parent starts a child and sends it a message, child ends, parent ends";
    check_wakeups(label, &w, &expected);
    let (parent, kid) = (run.chain(), w.wakeups[1].0.as_str());
    b.check_recorded(label, &w, parent, &[parent]).await;
    b.check_recorded(label, &w, kid, &[kid]).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&canceller, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // The child's cancelled end lists its children, a fan-out no segment records.
    let expected = [
        ops(9, 5, 1, 2, 17),
        ops(6, 3, 3, 4, 16),
        ops(4, 1, 2, 4, 11),
    ];
    let label = "parent starts a child and cancels it, child ends cancelled, parent ends";
    check_wakeups(label, &w, &expected);
    let (parent, kid) = (run.chain(), w.wakeups[1].0.as_str());
    b.check_recorded(label, &w, parent, &[parent]).await;
    let mut theirs = counted(&w.wakeups, &[kid]);
    theirs.list -= 1;
    b.check_requests(label, kid, theirs).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claims_renew_while_a_step_runs() {
    let b = bench().await;
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
    let renewals = t.put - 4;
    assert!((4..=6).contains(&renewals), "{t:?}");
    assert_eq!(t, ops(4 + renewals, 1, 2, 2 + renewals, 9 + 2 * renewals));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_lists_each_workflow_every_max_idle() {
    let b = bench().await;
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
    println!("idle 2 s, max_idle 200ms, 2 workflows: {w:?}");
    assert!(w.wakeups.is_empty());
    // A scan every max_idle, the first after the worker's phase.
    assert!((9..=11).contains(&w.scans), "a scan every max_idle: {w:?}");
    assert_eq!(w.scan_lists, 2 * w.scans, "a listing per workflow");
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_h3_a_waiting_listing_replaces_polling() {
    let h3 = FakeH3::start().await;
    let url = h3.url(&unique());
    let inner = internal::open(&url, &FakeH3::credentials(), retry()).unwrap();
    let b = bench_on(inner).await;
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
    row("idle 6 s on h3, wait 1s, max_idle 3s", &idle);
    println!("waiting listings {}, listings {}", idle.wait, idle.list);
    // A waiting listing held `wait` then sent again, and a scan every max_idle.
    assert!((5..=7).contains(&idle.wait), "{idle:?}");
    assert!((1..=3).contains(&idle.list), "{idle:?}");
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
    check_wakeups("1 step on h3", &w, &[ops(4, 1, 2, 2, 9)]);
    worker.shutdown().await.unwrap();
}

/// A worker whose runs are collected 200 ms after they end, their ids taken for `tombstone`.
fn collecting(b: &Bench, workflows: impl IntoWorkflows, tombstone: &str) -> WorkerBuilder {
    b.worker(workflows)
        .hidden("retention_min", "100ms")
        .set("retention", "200ms")
        .hidden("tombstone", tombstone)
        .hidden("recheck", "1h")
}

/// A run of one segment ended, then collected: its id is taken until the tombstone ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_and_tombstone_of_a_run() {
    let b = bench().await;
    let wf = steps();
    let worker = b.started(collecting(&b, &wf, "1h")).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, K { k: 0 }).id("r").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    // A collection entry sets no timer: the next scan serves it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    internal::wake(&worker);
    let w = b.since(mark).await;
    // The collection reads the run's start, its last segment and its result (the result's age),
    // creates `gone` then the tombstone's entry, and deletes the rest and the served entry; the
    // run published nothing, so no listing of its values.
    let expected = [ops(4, 1, 2, 2, 9), ops(2, 3, 1, 5, 11)];
    check_wakeups("a run of one segment, then its collection", &w, &expected);
    let (status, t) = b.client(run.status()).await;
    assert!(
        matches!(
            status,
            Err(Error::RunNotFound {
                collected: true,
                ..
            })
        ),
        "{status:?}"
    );
    check("status, collected: RunNotFound", t, ops(0, 3, 0, 0, 3));
    let (taken, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    assert!(
        matches!(
            taken,
            Err(Error::RunExists {
                collected: true,
                ..
            })
        ),
        "{taken:?}"
    );
    // The start, `gone`, then the start withdrawn.
    check(
        "start, during the tombstone: RunExists",
        t,
        ops(1, 1, 0, 1, 3),
    );
    let stored = b.stored().await;
    let kinds: Vec<&str> = stored.keys().map(String::as_str).collect();
    assert_eq!(kinds, ["gone", "probe", "queues", "substantial.json"]);
    worker.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_during_and_after_the_tombstone() {
    let b = bench().await;
    let wf = steps();
    let worker = b.started(collecting(&b, &wf, "1s")).await;
    let run = b.sub.start(&wf, K { k: 0 }).id("r").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    internal::wake(&worker);
    quiet(&b.workers).await;
    let (taken, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    assert!(
        matches!(
            taken,
            Err(Error::RunExists {
                collected: true,
                ..
            })
        ),
        "{taken:?}"
    );
    check(
        "start, during the tombstone: RunExists",
        t,
        ops(1, 1, 0, 1, 3),
    );
    let mark = b.workers.mark();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    internal::wake(&worker);
    let w = b.since(mark).await;
    check_wakeups("the tombstone's end", &w, &[ops(0, 1, 1, 2, 4)]);
    let (again, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    again.unwrap();
    check("start, after the tombstone", t, ops(2, 1, 0, 0, 3));
    worker.shutdown().await.unwrap();
}

/// A run whose result is younger than twice `retention_min` when its collection comes due: the
/// collection is deferred to the instant the result is that old.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_young_result_defers_its_collection() {
    let b = bench().await;
    let wf = steps();
    let worker = b
        .started(collecting(&b, &wf, "1h").set("retention", "100ms"))
        .await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, K { k: 0 }).id("r").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    // Due 100 ms after the end, served while the result is younger than 200 ms: deferred until it
    // is that old, and served by the scan after.
    tokio::time::sleep(Duration::from_millis(150)).await;
    internal::wake(&worker);
    quiet(&b.workers).await;
    internal::wake(&worker);
    let w = b.since(mark).await;
    // The end, the deferral reading the run up to the result's age, then the collection.
    let expected = [ops(4, 1, 2, 2, 9), ops(1, 3, 1, 1, 6), ops(2, 3, 1, 5, 11)];
    check_wakeups("a collection deferred by the result's age", &w, &expected);
    worker.shutdown().await.unwrap();
}

/// A due entry of a collected id, as a late sender's leaves: its wakeup reads the id is gone and
/// deletes the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wakeup_of_a_collected_id() {
    let b = bench().await;
    let wf = steps();
    let worker = b.started(collecting(&b, &wf, "1h")).await;
    let run = b.sub.start(&wf, K { k: 0 }).id("r").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    internal::wake(&worker);
    quiet(&b.workers).await;
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
    let late = format!(
        "queues/steps/due/{}~r~s0000000001",
        now.format("%Y%m%dt%H%M%S%3fz")
    );
    b.inner.create(&late, Default::default()).await.unwrap();
    let mark = b.workers.mark();
    internal::wake(&worker);
    let w = b.since(mark).await;
    check_wakeups("a wakeup of a collected id", &w, &[ops(0, 1, 1, 1, 3)]);
    worker.shutdown().await.unwrap();
}

/// A chain of 3 runs, each continued after a sleep: the first run retired, the second collected
/// while the chain lives, the last at the chain's end, which collects the first, then the end of
/// its tombstone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_lived_chain_collects_its_continued_runs() {
    let b = bench().await;
    let chain = Workflow::new("chain", |c, input: N| async move {
        c.sleep(Duration::from_millis(600)).await?;
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok(0u32)
    });
    let worker = b.started(collecting(&b, &chain, "1s")).await;
    let mark = b.workers.mark();
    b.sub.start(&chain, N { n: 2 }).id("c").await.unwrap();
    internal::wake(&worker);
    // Not `result`: the chain is collected 200 ms after it ends, before a poll could see it.
    // Collection entries set no timer: a scan every 150 ms stands for the idle scans.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        internal::wake(&worker);
    }
    let w = b.since(mark).await;
    for (i, (run, t)) in w.wakeups.iter().enumerate() {
        row(&format!("a chain of 3 runs, wakeup {} ({run})", i + 1), t);
    }
    // A run's result is written a few ms after its collection's due time is fixed, so a scan
    // serving the entry as it comes due defers it those few ms.
    let wakeups: Vec<Tally> = w
        .wakeups
        .iter()
        .map(|(_, t)| *t)
        .filter(|t| *t != ops(1, 3, 1, 1, 6))
        .collect();
    assert_eq!(wakeups.len(), 11, "{w:?}");
    // The replays of a worker that read a run more than `retention_min / 2` ago, 50 ms here,
    // read its result and start again before they write: only the collections are counted.
    let collections = [3, 6, 8, 9, 10].map(|i| wakeups[i]);
    let expected = [
        // The first run retired: the chain's next run read, its index listed; it published
        // nothing, so no listing of the chain's values.
        ops(0, 4, 2, 3, 9),
        // The second run, its predecessor retired and its successor started.
        ops(0, 5, 1, 7, 13),
        // The last run: the chain's first run gone, its collection entry created.
        ops(2, 6, 3, 7, 18),
        // The first run's delete phase: the chain continued, so its values are listed.
        ops(1, 2, 2, 4, 9),
        // The tombstone's end.
        ops(0, 1, 1, 2, 4),
    ];
    if !measuring() {
        assert_eq!(collections, expected);
    }
    let stored = b.stored().await;
    let kinds: Vec<&str> = stored.keys().map(String::as_str).collect();
    assert_eq!(kinds, ["probe", "substantial.json"]);
    worker.shutdown().await.unwrap();
}

/// Workers serving `workflow`, each on a handle and a counted store of its own.
async fn fleet(
    b: &Bench,
    n: usize,
    builder: impl Fn(&Substantial) -> WorkerBuilder,
) -> Vec<(Arc<Counting>, Worker)> {
    let mut fleet = Vec::new();
    for _ in 0..n {
        let store = Counting::new(b.inner.clone(), LATENCY);
        let sub = internal::open_store(store.clone()).await.unwrap();
        let worker = builder(&sub).start().unwrap();
        quiet(&store).await;
        fleet.push((store, worker));
    }
    fleet
}

/// Three workers know a run's timer: each lists its workflow when the timer comes due, one
/// replays the run, and the two others find its claim live, skip the run and list again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_at_a_timer() {
    let b = bench().await;
    // Longer than it takes to wake the three workers one by one.
    let wf = Workflow::without_input("napper", |c| async move {
        c.sleep(Duration::from_secs(4)).await?;
        Ok(1u32)
    });
    let script = |sub: &Substantial| sub.worker(&wf).preset(Preset::Script).set("max_idle", "1h");
    let fleet = fleet(&b, 3, script).await;
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&fleet[0].1);
    quiet(&fleet[0].0).await;
    // Each worker lists the run's timer, and sets its own to it.
    for (store, worker) in &fleet {
        internal::wake(worker);
        quiet(store).await;
    }
    let marks: Vec<usize> = fleet.iter().map(|(store, _)| store.mark()).collect();
    run.result().timeout(LONG).await.unwrap();
    let mut wakeups = Vec::new();
    for (i, ((store, _), mark)) in fleet.iter().zip(marks).enumerate() {
        quiet(store).await;
        let w = worked(&store.since(mark));
        for (run, t) in &w.wakeups {
            row(
                &format!("3 workers at a timer, worker {} ({run})", i + 1),
                t,
            );
        }
        println!(
            "worker {}: {} scans, {} listings",
            i + 1,
            w.scans,
            w.scan_lists
        );
        wakeups.push(w);
    }
    let replayed = |w: &Worked| w.wakeups.iter().any(|(_, t)| t.delete > 0);
    assert_eq!(
        wakeups.iter().filter(|w| replayed(w)).count(),
        1,
        "one worker replays the run: {wakeups:?}"
    );
    if !measuring() {
        for w in &wakeups {
            let [(_, t)] = w.wakeups.as_slice() else {
                panic!("one wakeup each: {w:?}")
            };
            // The worker that suspended the run holds its history; another reads its `start`.
            // A worker that loses lists the run and reads its live claim, or meets it as its own
            // create answers it exists; it skips the run and, once it let it go, lists again: the
            // scan at the timer and that one.
            if replayed(w) {
                assert!(
                    [ops(4, 0, 2, 2, 8), ops(4, 1, 2, 2, 9)].contains(t),
                    "{w:?}"
                );
                assert_eq!(w.scans, 1, "{w:?}");
            } else {
                let lost = [
                    ops(0, 2, 1, 0, 3),
                    ops(1, 1, 1, 0, 3),
                    ops(0, 1, 1, 0, 2),
                    ops(1, 0, 1, 0, 2),
                ];
                assert!(lost.contains(t), "{w:?}");
                assert_eq!(w.scans, 2, "{w:?}");
            }
        }
    }
    for (_, worker) in fleet {
        worker.shutdown().await.unwrap();
    }
}

/// Three workers serve a workflow whose run ended: its collection entry sets no worker's timer,
/// the first scan after it is due collects the run, and the scans of the others find nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_and_a_collection() {
    let b = bench().await;
    let wf = steps();
    let fleet = fleet(&b, 3, |sub| {
        sub.worker(&wf)
            .preset(Preset::Script)
            .set("max_idle", "1h")
            .hidden("retention_min", "100ms")
            .set("retention", "3s")
            .hidden("tombstone", "1h")
            .hidden("recheck", "1h")
    })
    .await;
    let run = b.sub.start(&wf, K { k: 0 }).await.unwrap();
    internal::wake(&fleet[0].1);
    run.result().timeout(LONG).await.unwrap();
    let ended = Instant::now();
    // Every worker lists the collection entry, not due yet.
    for (store, worker) in &fleet {
        internal::wake(worker);
        quiet(store).await;
    }
    let marks: Vec<usize> = fleet.iter().map(|(store, _)| store.mark()).collect();
    assert!(
        ended.elapsed() < Duration::from_millis(2500),
        "set up within the retention"
    );
    tokio::time::sleep(Duration::from_millis(3500).saturating_sub(ended.elapsed())).await;
    for (store, mark) in fleet.iter().map(|(store, _)| store).zip(&marks) {
        assert!(
            store.since(*mark).is_empty(),
            "a timer set by a collection entry"
        );
    }
    for (i, ((store, worker), mark)) in fleet.iter().zip(marks).enumerate() {
        internal::wake(worker);
        quiet(store).await;
        let w = worked(&store.since(mark));
        let label = format!("3 workers, a collection, worker {}", i + 1);
        let expected = if i == 0 {
            vec![ops(2, 3, 1, 5, 11)]
        } else {
            Vec::new()
        };
        check_wakeups(&label, &w, &expected);
        assert_eq!((w.scans, w.scan_lists), (1, 1), "{label}: its scan");
    }
    for (_, worker) in fleet {
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_runs_history_and_a_retry() {
    let b = bench().await;
    let refuse = Arc::new(AtomicBool::new(true));
    let r = refuse.clone();
    let flaky = Workflow::without_input("refused", move |c| {
        let r = r.clone();
        async move {
            step_n(&c, 0).await?;
            c.step("b", move || {
                let r = r.clone();
                async move {
                    match r.load(Ordering::SeqCst) {
                        true => Err(substantial::Permanent::new(std::io::Error::other("no"))),
                        false => Ok(2u32),
                    }
                }
            })
            .await
        }
    });
    let waits = Workflow::without_input("waits", |c| async move { c.receive::<u32>("go").await });
    let undone = Workflow::without_input("undone", |c| async move {
        c.saga(|c| async move {
            c.step("charge", || async { Ok::<_, Error>(1u32) })
                .undo(|_v: u32| async { Ok::<_, Error>(()) })
                .await?;
            c.step("ship", || async {
                Err::<u32, _>(substantial::Permanent::new(std::io::Error::other("no")))
            })
            .await
        })
        .await
    });
    for id in ["p1", "p2"] {
        b.sub.start(&waits, ()).id(id).await.unwrap();
    }
    let (listed, t) = b.client(b.sub.runs(&waits).try_collect::<Vec<_>>()).await;
    assert_eq!(listed.unwrap().len(), 2);
    // A page of `due/`, one of `rest/`, the start of each run.
    check("runs, 2 pending", t, ops(0, 2, 2, 0, 4));

    let mark = b.workers.mark();
    let worker = b
        .started(b.worker((waits.clone(), flaky.clone(), undone.clone())))
        .await;
    let w = b.since(mark).await;
    // Each first wait on messages alone creates the run's rest marker.
    check_wakeups(
        "receive at once, 2 runs",
        &w,
        &[ops(3, 1, 1, 2, 7), ops(3, 1, 1, 2, 7)],
    );
    let (listed, t) = b.client(b.sub.runs(&waits).try_collect::<Vec<_>>()).await;
    assert!(
        listed
            .unwrap()
            .iter()
            .all(|r| r.waiting_on == substantial::WaitingOn::Message)
    );
    // Runs found by their marker alone: each one's `result`, missing, then its `start`.
    check("runs, 2 waiting on messages", t, ops(0, 4, 2, 0, 6));
    let p1 = b.sub.run(&waits, "p1");
    let (history, t) = b.client(p1.history()).await;
    assert_eq!(history.unwrap().len(), 0);
    // The follow of a live run, its listing, its one segment.
    check("history, 1 segment", t, ops(0, 3, 1, 0, 4));

    let run = b.sub.start(&flaky, ()).id("f").await.unwrap();
    internal::wake(&worker);
    assert!(run.result().timeout(LONG).await.is_err());
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Failed);
    // A failed run's `result`, then the chain's index, listed for a retry.
    check("status, failed", t, ops(0, 1, 1, 0, 2));
    quiet(&b.workers).await;
    refuse.store(false, Ordering::SeqCst);
    let mark = b.workers.mark();
    let (retried, t) = b.client(run.retry()).await;
    retried.unwrap();
    // The follow, the failed run's segments and its last, its `start`, the retry's, the chain's
    // `gone`, its index and due entries.
    check("retry", t, ops(3, 4, 2, 0, 9));
    internal::wake(&worker);
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 2);
    let w = b.since(mark).await;
    let compensated = b.sub.start(&undone, ()).id("u").await.unwrap();
    internal::wake(&worker);
    assert!(compensated.result().timeout(LONG).await.is_err());
    let (refused, t) = b.client(compensated.retry()).await;
    assert!(
        matches!(refused, Err(Error::InvalidArgument(_))),
        "{refused:?}"
    );
    // The follow of a failed run, its segments and its last, which holds the saga's undo.
    check("retry, a failure that ran undos", t, ops(0, 2, 2, 0, 4));
    // It reads the failed run, its `start` and 2 segments, and its end creates its first snapshot.
    check_wakeups("the retry's first wakeup", &w, &[ops(5, 4, 3, 2, 14)]);
    let (history, t) = b.client(run.history()).await;
    assert_eq!(history.unwrap().len(), 3);
    // The failed run's `result`, the index, the retry's `result`; its listing and its snapshot.
    check("history, a retry", t, ops(0, 3, 2, 0, 5));
    worker.shutdown().await.unwrap();
}

/// What a drain of the worker does since `mark`, its scans and the wakeups of its runs.
fn drained(b: &Bench, label: &str, mark: usize, expected: Tally, scans: usize) -> Worked {
    let ops = b.workers.since(mark);
    let w = worked(&ops);
    check(label, Tally::of(&ops), expected);
    println!("{label}: {} scans, {} listings", w.scans, w.scan_lists);
    if !measuring() {
        assert_eq!(w.scans, scans, "{label}: scans");
    }
    w
}

/// A worker drained on a schedule: its scans list every workflow at once, and it stops once
/// nothing is due by its horizon.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain() {
    let b = bench().await;
    let wf = steps();
    let mark = b.workers.mark();
    b.worker(&wf).drain(SLEEP).await.unwrap();
    drained(&b, "drain, nothing due", mark, ops(0, 0, 1, 0, 1), 1);

    let run = b.sub.start(&wf, K { k: 1 }).await.unwrap();
    let mark = b.workers.mark();
    b.worker(&wf).drain(SLEEP).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    let label = "drain, one run of one step due now";
    let w = drained(&b, label, mark, ops(4, 1, 3, 2, 10), 1);
    check_wakeups(label, &w, &[ops(4, 1, 2, 2, 9)]);
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;

    let napper = Workflow::without_input("napper", |c| async move {
        step_n(&c, 0).await?;
        c.sleep(SLEEP).await?;
        Ok(1)
    });
    let run = b.sub.start(&napper, ()).await.unwrap();
    let mark = b.workers.mark();
    b.worker(&napper).drain(SLEEP * 2).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    let label = "drain, one run whose sleep ends within it";
    let w = drained(&b, label, mark, ops(7, 1, 5, 4, 17), 2);
    check_wakeups(label, &w, &[ops(3, 1, 1, 2, 7), ops(4, 0, 2, 2, 8)]);
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
}

/// A poll whose pokes are not ready twice: each is a wakeup, the step, the time, the sleep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poll_not_ready() {
    let b = bench().await;
    let pokes = Arc::new(AtomicU32::new(0));
    let p = pokes.clone();
    let wf = Workflow::without_input("poller", move |c| {
        let p = p.clone();
        async move {
            let ready = c
                .poll("check", move || {
                    let p = p.clone();
                    async move {
                        let n = p.fetch_add(1, Ordering::SeqCst) + 1;
                        Ok::<_, Error>((n >= 3).then_some(n))
                    }
                })
                .every(Duration::from_millis(400))
                .timeout(LONG)
                .await?;
            Ok(ready.unwrap_or(0))
        }
    });
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 3);
    let w = b.since(mark).await;
    // The first wakeup also gets the run's `start` and makes a progress commit; the third poke is
    // ready and ends the run.
    let not_ready = ops(3, 0, 1, 2, 6);
    let expected = [ops(4, 1, 2, 2, 9), not_ready, ops(4, 0, 2, 2, 8)];
    check_wakeups("poll, 2 pokes not ready, then ready", &w, &expected);
    check(
        "poll, a wakeup of a poke not ready",
        w.wakeups[1].1,
        not_ready,
    );
    let label = "poll, 2 pokes not ready, then ready";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    worker.shutdown().await.unwrap();
}

/// A fire of an every-minute schedule, from its timer to the next suspension: the plan step that
/// finds it due, the body's step, and the plan step that finds the next one. It waits for the
/// minute's end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_schedule_fire() {
    let b = bench().await;
    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    let wf = Workflow::without_input("ticker", move |c| {
        let f = f.clone();
        async move {
            c.schedule(move |c, _at| {
                let f = f.clone();
                async move {
                    c.step("tick", move || {
                        let f = f.clone();
                        async move {
                            f.store(true, Ordering::SeqCst);
                            Ok::<_, Error>(())
                        }
                    })
                    .await?;
                    Ok(ControlFlow::Continue(()))
                }
            })
            .await?;
            Ok(0)
        }
    });
    let worker = b.started(b.worker(&wf)).await;
    let every_minute = substantial::Cron::new("* * * * *", "UTC").unwrap();
    b.sub
        .schedule(&wf, "tick", Some(every_minute), 1)
        .await
        .unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    let started = Instant::now();
    while !fired.load(Ordering::SeqCst) {
        assert!(started.elapsed() < Duration::from_secs(70), "no fire");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let w = b.since(mark).await;
    // The claim, three progress commits (a segment and an inbox listing each), the published
    // value and its effects record, then the suspension's due entry and segment.
    check_wakeups(
        "schedule, a fire whose body runs a step",
        &w,
        &[ops(8, 0, 4, 2, 14)],
    );
    worker.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct Chain {
    chain: String,
}

/// A run awaiting the end of the chain of its input, and a chain that ends at a message `go`,
/// continuing as new first while its `n` is above zero.
fn watching() -> (Workflow<String>, Workflow<String>) {
    let watcher = Workflow::new("watcher", |c, input: Chain| async move {
        c.result::<String>(&input.chain).await
    });
    let target = Workflow::new("target", |c, input: N| async move {
        let go: String = c.receive("go").await?;
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok(go)
    });
    (watcher, target)
}

/// Starts a target under `id` and lets its first wakeup pass, uncounted.
async fn target_started(b: &Bench, worker: &Worker, target: &Workflow<String>, id: &str, n: u32) {
    b.sub.start(target, N { n }).id(id).await.unwrap();
    internal::wake(worker);
    quiet(&b.workers).await;
}

/// Starts watchers of `chain` under `ids`, then wakes the worker.
async fn watchers_started(
    b: &Bench,
    worker: &Worker,
    watcher: &Workflow<String>,
    chain: &str,
    ids: &[&str],
) {
    for id in ids {
        let input = Chain {
            chain: chain.to_owned(),
        };
        b.sub.start(watcher, input).id(id).await.unwrap();
    }
    internal::wake(worker);
}

/// A watcher registers on a live chain of one run: it reads the chain's `result` and `start`,
/// creates its key under the run, reads `result` again, and suspends. The run's end then lists
/// its watchers and tells the one it finds, as a child's end tells its parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workflow_awaits_a_live_chain() {
    let b = bench().await;
    let (watcher, target) = watching();
    let worker = b.started(b.worker((watcher.clone(), target.clone()))).await;
    target_started(&b, &worker, &target, "t", 0).await;
    let mark = b.workers.mark();
    watchers_started(&b, &worker, &watcher, "t", &["w"]).await;
    let w = b.since(mark).await;
    let label = "c.result, a live chain: the registration";
    check_wakeups(label, &w, &[ops(4, 4, 1, 2, 11)]);
    b.check_recorded(label, &w, "w", &["w"]).await;
    let mark = b.workers.mark();
    b.sub.run(&target, "t").send("go", "done").await.unwrap();
    internal::wake(&worker);
    let result = b.sub.run(&watcher, "w").result().timeout(LONG).await;
    assert_eq!(result.unwrap(), "done");
    let w = b.since(mark).await;
    let label = "c.result, the watched end tells one watcher, the watcher ends";
    check_wakeups(label, &w, &[ops(6, 2, 2, 4, 14), ops(4, 1, 2, 4, 11)]);
    let all = worked(&b.workers.since(0));
    b.check_recorded(label, &all, "t", &["t"]).await;
    b.check_recorded(label, &all, "w", &["w"]).await;
    worker.shutdown().await.unwrap();
}

/// A watcher of a chain that ended reads its `result` and ends at once: no key, no message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workflow_awaits_an_ended_chain() {
    let b = bench().await;
    let (watcher, target) = watching();
    let worker = b.started(b.worker((watcher.clone(), target.clone()))).await;
    target_started(&b, &worker, &target, "t", 0).await;
    b.sub.run(&target, "t").send("go", "done").await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    watchers_started(&b, &worker, &watcher, "t", &["w"]).await;
    let result = b.sub.run(&watcher, "w").result().timeout(LONG).await;
    assert_eq!(result.unwrap(), "done");
    let w = b.since(mark).await;
    let label = "c.result, an ended chain";
    check_wakeups(label, &w, &[ops(4, 2, 2, 2, 10)]);
    b.check_recorded(label, &w, "w", &["w"]).await;
    worker.shutdown().await.unwrap();
}

/// Two watchers of one run: its end tells each.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_workflows_await_one_chain() {
    let b = bench().await;
    let (watcher, target) = watching();
    let worker = b.started(b.worker((watcher.clone(), target.clone()))).await;
    target_started(&b, &worker, &target, "t", 0).await;
    watchers_started(&b, &worker, &watcher, "t", &["w1", "w2"]).await;
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    b.sub.run(&target, "t").send("go", "done").await.unwrap();
    internal::wake(&worker);
    for id in ["w1", "w2"] {
        let result = b.sub.run(&watcher, id).result().timeout(LONG).await;
        assert_eq!(result.unwrap(), "done");
    }
    let w = b.since(mark).await;
    let label = "c.result, the watched end tells two watchers, each ends";
    let watcher_end = ops(4, 1, 2, 4, 11);
    check_wakeups(label, &w, &[ops(8, 3, 2, 4, 17), watcher_end, watcher_end]);
    let all = worked(&b.workers.since(0));
    b.check_recorded(label, &all, "t", &["t"]).await;
    worker.shutdown().await.unwrap();
}

/// A watched run continues as new: its end carries the watcher to the next run, which tells it
/// at its own end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watched_chain_continues_as_new() {
    let b = bench().await;
    let (watcher, target) = watching();
    let worker = b.started(b.worker((watcher.clone(), target.clone()))).await;
    target_started(&b, &worker, &target, "t", 1).await;
    watchers_started(&b, &worker, &watcher, "t", &["w"]).await;
    quiet(&b.workers).await;
    let mark = b.workers.mark();
    b.sub.run(&target, "t").send("go", "first").await.unwrap();
    internal::wake(&worker);
    let w = b.since(mark).await;
    let label = "c.result, the watched run continues as new, carrying its watcher";
    check_wakeups(label, &w, &[ops(8, 2, 2, 4, 16), ops(3, 1, 1, 2, 7)]);
    let mark = b.workers.mark();
    b.sub.run(&target, "t").send("go", "second").await.unwrap();
    internal::wake(&worker);
    let result = b.sub.run(&watcher, "w").result().timeout(LONG).await;
    assert_eq!(result.unwrap(), "second");
    let w = b.since(mark).await;
    let label = "c.result, the next run's end tells the carried watcher, the watcher ends";
    check_wakeups(label, &w, &[ops(6, 2, 2, 4, 14), ops(4, 1, 2, 4, 11)]);
    let next = w.wakeups[0].0.clone();
    let all = worked(&b.workers.since(0));
    b.check_recorded(label, &all, "t", &["t"]).await;
    b.check_recorded(label, &all, &next, &["t", &next]).await;
    worker.shutdown().await.unwrap();
}
