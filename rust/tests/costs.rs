//! The store operations of every verb, counted: the numbers of `docs/architecture.md`.
//!
//! Each test runs a verb or a scenario against a store wrapped in `Counting`, which logs every
//! operation, and asserts its count by kind and its round trips. A change of the core or a host
//! that changes a count fails here: update the doc with it. For a run's wakeups, it also asserts
//! that the store requests the run's last segment records, its chain's through it, are the ones
//! counted: every request its worker sends, but the telling of its watchers after its end's last
//! write. A cas is counted apart from a create, both of S3's PUT class.
//!
//! - `COSTS_MEASURE=1 cargo test --test costs -- --nocapture --test-threads 1` prints every row
//!   as measured, `| label | create | cas | GET | LIST | DELETE | ops | round trips |`, without
//!   asserting.
//! - `COSTS_TRACE=1` also prints each worker operation with its key, to explain a row.

mod common;
#[allow(dead_code)]
mod fake_h3;

use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

    /// The head of the run `run`: its chain's while it is the chain's current run (through the
    /// pointer of a later run), else its archive.
    async fn head(&self, run: &str) -> layout::Head {
        let get = async |key: String| {
            let bytes = self.inner.get(&key).await.unwrap()?.bytes;
            match layout::Run::decode(bytes).unwrap().kind {
                Some(layout::run::Kind::Head(head)) => Some(head),
                Some(layout::run::Kind::Pointer(p)) => {
                    let bytes = self
                        .inner
                        .get(&format!("runs/{}", p.chain))
                        .await
                        .unwrap()?;
                    match layout::Run::decode(bytes.bytes).unwrap().kind {
                        Some(layout::run::Kind::Head(head)) => Some(head),
                        other => panic!("runs/{}: {other:?}", p.chain),
                    }
                }
                None => panic!("{key}: neither head nor pointer"),
            }
        };
        match get(format!("runs/{run}")).await {
            Some(head) if head.run == run => head,
            // A chain's first run, once a later run holds its head.
            _ => get(format!("archives/{run}")).await.expect("an archive"),
        }
    }

    /// The head of the chain `chain`, its current run's.
    async fn current(&self, chain: &str) -> layout::Head {
        let bytes = self.inner.get(&format!("runs/{chain}")).await.unwrap();
        match layout::Run::decode(bytes.expect("a head").bytes)
            .unwrap()
            .kind
        {
            Some(layout::run::Kind::Head(head)) => head,
            other => panic!("runs/{chain}: {other:?}"),
        }
    }

    /// The cost the last segment of `run` records: the chain's through it.
    async fn recorded(&self, run: &str) -> layout::Cost {
        let head = self.head(run).await;
        let last = head.segments.last().expect("a segment");
        last.cost.clone().expect("a segment records its cost")
    }

    /// Asserts that the store requests the last segment of `run` records are the ones the
    /// wakeups of the chains `chains` sent, as counted, but the telling of its watchers.
    async fn check_recorded(&self, label: &str, w: &Worked, run: &str, chains: &[&str]) {
        self.check_requests(label, run, recordable(w, chains)).await;
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
                "queues" => format!("queues/{}", parts[2]),
                other => other.to_owned(),
            };
            let bytes = self
                .inner
                .get(key)
                .await
                .unwrap()
                .map_or(0, |o| o.bytes.len());
            let entry = kinds.entry(kind).or_default();
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

/// A listing of a workflow's due entries: the per-step listing of a chain's messages is not one.
fn is_scan(op: &Op) -> bool {
    matches!(op.kind, Kind::List | Kind::Wait)
        && op.key.starts_with("queues/")
        && op.key.ends_with("/due/")
}

/// The chain a key belongs to: `runs/<chain>`, `archives/<chain>`, `watch/<chain>/...`, or the
/// chain a due entry names.
fn chain_of(key: &str) -> String {
    let parts: Vec<&str> = key.split('/').collect();
    let chain = match parts.as_slice() {
        ["runs" | "archives" | "watch", chain, ..] => Some(*chain),
        ["queues", _, _, entry] => entry.split('~').nth(1),
        _ => None,
    };
    chain.unwrap_or("?").to_owned()
}

/// A worker's operations: its replays, each the operations between two scans (a worker of
/// concurrency 1 holds one run at a time), and its scans.
#[derive(Debug)]
struct Worked {
    wakeups: Vec<(String, Tally)>,
    /// Each wakeup's operations.
    ops: Vec<Vec<Op>>,
    scans: usize,
    scan_lists: usize,
}

/// A worker's operations by wakeup, each labelled with the chain of its first operation.
fn groups(ops: &[Op]) -> Vec<(String, Vec<Op>)> {
    let mut wakeups: Vec<(String, Vec<Op>)> = Vec::new();
    let mut in_scan = true;
    for op in ops {
        if is_scan(op) {
            in_scan = true;
            continue;
        }
        if in_scan {
            wakeups.push((chain_of(&op.key), Vec::new()));
        }
        in_scan = false;
        wakeups.last_mut().unwrap().1.push(op.clone());
    }
    wakeups
}

fn worked(ops: &[Op]) -> Worked {
    let scans: Vec<Op> = ops.iter().filter(|op| is_scan(op)).cloned().collect();
    let groups = groups(ops);
    Worked {
        ops: groups.iter().map(|(_, ops)| ops.clone()).collect(),
        wakeups: groups
            .into_iter()
            .map(|(run, ops)| (run, Tally::of(&ops)))
            .collect(),
        // The listings of one scan are in flight together: one round trip a scan.
        scans: Tally::of(&scans).trips,
        scan_lists: scans.len(),
    }
}

/// Expected operations: creates and cas (both PUT), gets, listings, deletes, round trips.
fn ops(put: usize, cas: usize, get: usize, list: usize, delete: usize, trips: usize) -> Tally {
    Tally {
        put,
        cas,
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

fn add(requests: &mut layout::Requests, t: &Tally) {
    requests.create += t.put as u64;
    requests.cas += t.cas as u64;
    requests.get += t.get as u64;
    requests.list += (t.list + t.wait) as u64;
    requests.delete += t.delete as u64;
}

/// What an end does after its last write that its segment does not record: once it listed its
/// chain's watchers, for each one a GET of its head, the message to it, the delete of its key.
fn told_watchers(chain: &str, ops: &[Op]) -> Tally {
    let watchers = format!("watch/{chain}/");
    let Some(listed) = ops.iter().position(|op| op.key == watchers) else {
        return Tally::default();
    };
    let told: Vec<Op> = (ops[listed + 1..].iter())
        .filter(|op| op.key.starts_with("watch/") || chain_of(&op.key) != chain)
        .cloned()
        .collect();
    Tally::of(&told)
}

/// The store requests of the wakeups of the chains `chains` their segments record: those sent,
/// but the telling of watchers.
fn recordable(w: &Worked, chains: &[&str]) -> layout::Requests {
    let (mut sent, mut told) = (layout::Requests::default(), layout::Requests::default());
    for ((chain, t), ops) in w.wakeups.iter().zip(&w.ops) {
        if chains.contains(&chain.as_str()) {
            add(&mut sent, t);
            add(&mut told, &told_watchers(chain, ops));
        }
    }
    layout::Requests {
        create: sent.create - told.create,
        cas: sent.cas - told.cas,
        get: sent.get - told.get,
        list: sent.list - told.list,
        delete: sent.delete - told.delete,
    }
}

/// The store requests of the wakeups of `chain` in `ops` up to the write of `key`, a
/// continuation's pointer, and the deletes of its rest marker right after: what the continued
/// run's archive records, its commit's operations included.
fn through(ops: &[Op], chain: &str, key: &str) -> layout::Requests {
    let rest = |op: &Op| op.kind == Kind::Delete && op.key.ends_with(&format!("/rest/{chain}"));
    let mut sent = Vec::new();
    for (_, ops) in groups(ops).into_iter().filter(|(c, _)| c == chain) {
        if let Some(at) = ops.iter().position(|op| op.key == key) {
            sent.extend_from_slice(&ops[..=at]);
            sent.extend(ops[at + 1..].iter().take_while(|op| rest(op)).cloned());
            let mut requests = layout::Requests::default();
            add(&mut requests, &Tally::of(&sent));
            return requests;
        }
        sent.extend(ops);
    }
    panic!("no write of {key} in the wakeups of {chain}")
}

fn row(label: &str, t: &Tally) {
    println!(
        "| {label} | {} | {} | {} | {} | {} | {} | {} |",
        t.put,
        t.cas,
        t.get,
        t.list + t.wait,
        t.delete,
        t.ops(),
        t.trips
    );
}

fn check(label: &str, measured: Tally, expected: Tally) {
    row(label, &measured);
    // § qashr34zn@0242y
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
    // § qashr34zn@7wgws
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
    check("create, new namespace", t, ops(4, 2, 0, 0, 2, 8));
    let mark = store.mark();
    internal::create(store.clone()).await.unwrap();
    let t = Tally::of(&store.since(mark));
    check("create, existing namespace", t, ops(4, 2, 1, 0, 2, 9));
    let mark = store.mark();
    internal::open_store(store.clone()).await.unwrap();
    check("open", Tally::of(&store.since(mark)), ops(3, 2, 1, 0, 2, 8));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_verbs() {
    let b = bench().await;
    let wf = steps();
    let (run, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    let run = run.unwrap();
    // The chain's head, then its due entry.
    // § one jbkv4v0we@pqc46
    check("start", t, ops(2, 0, 0, 0, 0, 2));
    let (again, t) = b.client(b.sub.start(&wf, K { k: 0 }).id("r")).await;
    again.unwrap();
    // The head exists: read to compare its start, then the due entry again.
    check("start, same id and input again", t, ops(2, 0, 1, 0, 0, 3));
    // § jbkv4v0we.b@ft4xq
    let (taken, t) = b.client(b.sub.start(&wf, K { k: 1 }).id("r")).await;
    assert!(matches!(taken, Err(Error::RunExists { .. })), "{taken:?}");
    check("start, id taken: RunExists", t, ops(1, 0, 1, 0, 0, 2));
    // § end jbkv4v0we.b

    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Pending);
    // The chain's head alone, wherever its run is.
    check("status, pending", t, ops(0, 0, 1, 0, 0, 1));
    // § dva6cx6nb.c@ynjht
    let (sent, t) = b.client(run.send("m", 1)).await;
    sent.unwrap();
    check("send", t, ops(1, 0, 1, 0, 0, 2));
    let (cancelled, t) = b.client(run.cancel("no")).await;
    cancelled.unwrap();
    check("cancel", t, ops(1, 0, 1, 0, 0, 2));
    // § end dva6cx6nb.c
    let (read, t) = b.client(run.read::<u32>("progress")).await;
    assert_eq!(read.unwrap(), None);
    check("read, nothing published", t, ops(0, 0, 1, 0, 0, 1));
    let missing = b.sub.run(&wf, "nobody");
    // § dva6cx6nb@8fmdz
    let (sent, t) = b.client(missing.send("m", 1)).await;
    assert!(matches!(sent, Err(Error::RunNotFound { .. })), "{sent:?}");
    // § end dva6cx6nb
    check("send, no chain: RunNotFound", t, ops(0, 0, 1, 0, 0, 1));

    // Polls at 0, 10, 30, 70, 150, 310, 630, 1,270, 2,550 and 5,110 ms, the interval doubling up
    // to 30 s: 10 in 6 s.
    // § hnjtggsx7@tsywj
    let (not_ended, t) = b.client(run.result().timeout(Duration::from_secs(6))).await;
    assert!(
        matches!(not_ended, Err(Error::WaitTimeout)),
        "{not_ended:?}"
    );
    row("result, 6 s of polls, not ended", &t);
    // Each poll reads the chain's head, one GET.
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
        "every poll reads the head alone"
    );
    // § end hnjtggsx7
    let (_, t) = b.client(run.result()).await;
    // § fw4aw2ath@31j54
    check("result, ended", t, ops(0, 0, 1, 0, 0, 1));
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Completed);
    check("status, ended", t, ops(0, 0, 1, 0, 0, 1));
    let (sent, t) = b.client(run.send("m", 1)).await;
    assert!(matches!(sent, Err(Error::RunEnded { .. })), "{sent:?}");
    check("send, run ended: RunEnded", t, ops(0, 0, 1, 0, 0, 1));
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
    check("status, suspended", t, ops(0, 0, 1, 0, 0, 1));
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
    // § wkryj60b0@5hjtr
    // The last step is in the end's segment; each step before it is a progress commit, the
    // head's cas and a listing of the chain's messages, made as the next one starts. A wakeup
    // reads the head, and one that runs a step takes the lease first, a cas of the head; the
    // end's cas writes the head with its expiry, then lists its watchers and deletes the served
    // entry.
    let (stepless, stepped) = (ops(0, 1, 1, 1, 1, 4), ops(0, 2, 1, 1, 1, 5));
    check("0 steps, start to end", measured[0], stepless);
    check("1 step, start to end", measured[1], stepped);
    let progress = ops(0, 1, 0, 1, 0, 2);
    for k in 2..4 {
        let d = Tally {
            put: measured[k].put - measured[k - 1].put,
            cas: measured[k].cas - measured[k - 1].cas,
            get: measured[k].get - measured[k - 1].get,
            list: measured[k].list - measured[k - 1].list,
            wait: 0,
            delete: measured[k].delete - measured[k - 1].delete,
            trips: measured[k].trips - measured[k - 1].trips,
        };
        check(&format!("step {k} of {k}, a progress commit"), d, progress);
    }
    // § end wkryj60b0
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
    check_wakeups("now, random, uuid, end", &w, &[stepless]);
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
    check_wakeups("3 steps, commit_every 1h", &w, &[stepped]);
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
    // § ap5tfnrja.b@tbd65
    let (mut put, mut get, mut list) = (0, 0, 0);
    for (_, t) in &w.wakeups {
        (put, get, list) = (put + t.put + t.cas, get + t.get, list + t.list + t.wait);
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
    // § end ap5tfnrja.b
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
    // § czph3f9yv.d@z2xe2
    // § gnhavbn5m@7656e
    // The third step lands in the suspension, after its timer's due entry; the worker holds the
    // head for the second wakeup, which needs no GET.
    let expected = [ops(1, 4, 1, 2, 1, 9), ops(0, 1, 0, 1, 1, 3)];
    check_wakeups("3 steps, sleep, end", &w, &expected);
    assert_eq!(w.scans, 2, "each wakeup found by a scan");
    // § end gnhavbn5m
    let label = "3 steps, sleep, end";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    // § end czph3f9yv.d
    let stored = b.stored().await;
    let kinds: Vec<&str> = stored.keys().map(String::as_str).collect();
    assert_eq!(kinds, ["runs", "substantial.json"], "the head alone");
    assert_eq!(stored["runs"].0, 1);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_blocks_then_resumes() {
    let b = bench().await;
    let broken = Arc::new(AtomicBool::new(true));
    let code = broken.clone();
    let wf = Workflow::without_input("fixed", move |c| {
        let broken = code.load(Ordering::SeqCst);
        async move {
            step_n(&c, 0).await?;
            if broken {
                return Err(anyhow::anyhow!("a bug").into());
            }
            Ok(1)
        }
    });
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, ()).await.unwrap();
    internal::wake(&worker);
    let w = b.since(mark).await;
    // The replay records nothing, not even its step: the head's cas holds the block, then the due
    // entry that tries it again and the queue's marker.
    check_wakeups("a step, then a block", &w, &[ops(2, 2, 1, 0, 1, 5)]);
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Blocked);
    // The head, which holds the block.
    check("status, blocked", t, ops(0, 0, 1, 0, 0, 1));
    let (listed, t) = b.client(b.sub.runs(&wf).try_collect::<Vec<_>>()).await;
    assert_eq!(listed.unwrap().len(), 1);
    // A page of `blocked/` and its run's head, then the pages of `due/` and `rest/`, where the
    // blocked run is not read again.
    check("runs, 1 blocked", t, ops(0, 0, 1, 3, 0, 4));
    broken.store(false, Ordering::SeqCst);
    let mark = b.workers.mark();
    // The script preset tries a blocked run again 1 s later, at the worker's timer.
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 1);
    let w = b.since(mark).await;
    // The end as usual, from the head the worker holds, and the delete of the queue's marker.
    check_wakeups("the fix resumes it", &w, &[ops(0, 2, 0, 1, 2, 4)]);
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
    // § pt3fegerz@6sah0
    let (status, _) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Suspended, "an empty replay");
    // § end pt3fegerz
    run.send("payment", 3).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // Nothing to record before the payment: the first wakeup writes an empty segment, so the
    // run reads suspended, and creates its rest marker instead of a due entry; its end deletes it.
    // The message is a due entry holding its body: one GET.
    let expected = [ops(1, 1, 1, 0, 1, 4), ops(0, 3, 1, 2, 2, 8)];
    // § one d9vmk6y6y.b@bgtah
    // silcrow:follow
    check_wakeups("receive, 2 steps, end", &w, &expected);
    assert_eq!(w.scans, 2, "each wakeup found by a scan");
    let label = "receive, 2 steps, end";
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    let stored = b.stored().await;
    let kinds: Vec<&str> = stored.keys().map(String::as_str).collect();
    assert_eq!(kinds, ["runs", "substantial.json"], "the head alone");
    assert_eq!(stored["runs"].0, 1);
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
    let expected = [ops(1, 1, 1, 0, 1, 4), ops(0, 1, 1, 1, 2, 4)];
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
    // The parent's first wait on a message creates its rest marker; its cas, pending, then the
    // child's head and due entry, and the cas that clears the pending. The child's end tells the
    // parent by a message entry. The parent's end reads the message and reads no child's head: it
    // expires after its own retention.
    let expected = [
        ops(3, 2, 1, 0, 1, 7),
        ops(1, 2, 1, 1, 1, 6),
        ops(0, 1, 1, 1, 2, 5),
    ];
    let label = "parent starts a child, child ends, parent ends";
    check_wakeups(label, &w, &expected);
    let (parent, kid) = (run.chain(), w.wakeups[1].0.as_str());
    let ended = groups(&b.workers.since(mark)).pop().unwrap().1;
    assert!(
        ended.iter().all(|op| op.key != format!("runs/{kid}")),
        "{ended:?}"
    );
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
    // The send reads the target's head before the commit; after it, the message entry, then the
    // cas that clears the pending.
    let expected = [ops(1, 2, 2, 1, 1, 7), ops(0, 1, 1, 1, 2, 5)];
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
    check_wakeups("a run publishes and ends", &w, &[ops(0, 1, 1, 1, 1, 4)]);
    b.check_recorded("a run publishes and ends", &w, "p", &["p"])
        .await;
    let (read, t) = b.client(p.read::<u32>("progress")).await;
    assert_eq!(read.unwrap(), Some(1));
    check("read, a value", t, ops(0, 0, 1, 0, 0, 1));
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
    // The next run is replayed in place, in the wakeup that continued, and its end is two-phase:
    // its cas pending, the pointer's expiry, the head's.
    let expected = [ops(3, 4, 1, 1, 2, 10)];
    let label = "continue as new, then end";
    check_wakeups(label, &w, &expected);
    // A continuation is 4 writes: the next run's wakeup, the head, the archive, the pointer.
    let next = b.current(run.chain()).await.run;
    let writes: Vec<(Kind, String)> = (b.workers.since(mark).into_iter())
        .filter(|op| matches!(op.kind, Kind::Put | Kind::Cas))
        .take(4)
        .map(|op| (op.kind, op.key))
        .collect();
    let chain = run.chain();
    assert!(
        writes[0].1.starts_with("queues/countdown/due/")
            && writes[0].1.ends_with(&format!("~{chain}~s0000000000")),
        "{writes:?}"
    );
    assert_eq!(
        writes[1..],
        [
            (Kind::Cas, format!("runs/{chain}")),
            (Kind::Put, format!("archives/{chain}")),
            (Kind::Put, format!("runs/{next}")),
        ],
    );
    // § czph3f9yv.f@wsrht
    // The next run starts from the first's total.
    let first = run.chain();
    let archived = through(&b.workers.since(mark), first, &format!("runs/{next}"));
    b.check_requests(label, first, archived).await;
    b.check_recorded(label, &w, &next, &[first]).await;
    // § end czph3f9yv.f
    let (_, t) = b.client(run.result()).await;
    check("result, a chain of 2 runs", t, ops(0, 0, 1, 0, 0, 1));
    worker.shutdown().await.unwrap();
}

const ITERATION_CONTINUES: Tally = Tally {
    put: 4,
    cas: 3,
    get: 0,
    list: 0,
    wait: 0,
    delete: 2,
    trips: 8,
};
const ITERATION_ENDS: Tally = Tally {
    put: 0,
    cas: 4,
    get: 0,
    list: 1,
    wait: 0,
    delete: 1,
    trips: 6,
};

#[derive(Serialize, Deserialize)]
struct Polls {
    i: u32,
}

/// A poll then a sleep per iteration: one segment each, the head read at the first wakeup and held
/// by the worker after it, a continuation once it holds `continue_as_new_after` segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_polling_loop_compresses() {
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
    let builder = b.worker(&cron).set("continue_as_new_after", "8,8MiB");
    let worker = b.started(builder).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&cron, ()).await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    // § hdzwp5tnf@rzjab
    // An iteration: its lease's cas, its timer's due entry and the suspension's cas, DELETE 1 for
    // the entry it was woken by, and a GET of the head at its first wakeup: the worker holds it
    // after.
    let iteration = |first: usize| ops(1, 2, first, 0, 1, 4 + first);
    let mut expected = vec![iteration(1)];
    expected.extend([iteration(0); 7]);
    expected.extend([
        // The ninth: 8 segments, the threshold passed, the run continues before its step, so
        // with no lease: the next run's wakeup, the head, the archive and the pointer; then the
        // next run's first iteration in place, the cas that clears the continuation, its timer
        // and its suspension, and the deletes of both wakeups.
        ITERATION_CONTINUES,
        // The last iteration ends the chain: its lease, the end's cas pending, the pointer's
        // expiry, the head's.
        ITERATION_ENDS,
    ]);
    let label = "polling loop, continue after 8";
    check_wakeups(label, &w, &expected);
    // § end hdzwp5tnf
    let (first, next) = (run.chain(), b.current(run.chain()).await.run);
    let archived = through(&b.workers.since(mark), first, &format!("runs/{next}"));
    b.check_requests(label, first, archived).await;
    b.check_recorded(label, &w, &next, &[first]).await;
    let stored = b.stored().await;
    let counts: Vec<(&str, usize)> = stored.iter().map(|(k, (n, _))| (k.as_str(), *n)).collect();
    assert_eq!(
        counts,
        [("archives", 1), ("runs", 2), ("substantial.json", 1)]
    );
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
                deadline: None,
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
    let expected = [ops(1, 2, 1, 0, 1, 5), ops(0, 2, 0, 1, 1, 4)];
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
    check_wakeups(label, &w, &[ops(0, 3, 1, 2, 1, 7)]);
    b.check_recorded(label, &w, run.chain(), &[run.chain()])
        .await;
    worker.shutdown().await.unwrap();
}

#[derive(Serialize, Deserialize)]
struct Ending {
    children: u32,
    end: String,
}

/// A parent that starts `children` waiting children, then ends as `end` says: `completed` or
/// `failed` at the message `go`, `cancelled` by a client's cancel; its end cancels the children
/// still running. The wakeups from its end on, its own and its children's, and its chain.
async fn an_end_with_children(end: &str, children: u32) -> (Worked, String, Vec<String>) {
    let b = bench().await;
    let kid = kid_workflow();
    let k = kid.clone();
    let parent = Workflow::new("parent", move |c, input: Ending| {
        let k = k.clone();
        async move {
            for _ in 0..input.children {
                c.start(&k, ()).await?;
            }
            c.receive::<u32>("go").await?;
            match input.end.as_str() {
                "failed" => Err(Error::fail("Refused", "no")),
                _ => Ok(0u32),
            }
        }
    });
    let worker = b.started(b.worker((parent.clone(), kid.clone()))).await;
    let input = Ending {
        children,
        end: end.to_owned(),
    };
    let run = b.sub.start(&parent, input).await.unwrap();
    internal::wake(&worker);
    quiet(&b.workers).await;
    let first = worked(&b.workers.since(0));
    let kids: Vec<String> = (first.wakeups.iter())
        .map(|(chain, _)| chain.clone())
        // The worker's open of the namespace, before its first scan.
        .filter(|chain| chain != run.chain() && chain != "?")
        .collect();
    assert_eq!(kids.len(), children as usize, "{first:?}");
    let mark = b.workers.mark();
    match end {
        "cancelled" => run.cancel("stop").await.unwrap(),
        _ => run.send("go", 1).await.unwrap(),
    }
    internal::wake(&worker);
    let _ = run.result().timeout(LONG).await;
    let w = b.since(mark).await;
    for chain in &kids {
        let status = b.sub.run(&kid, chain).status().await.unwrap();
        assert_eq!(status, RunStatus::Cancelled, "{end}: child {chain}");
    }
    let label = format!("a {end} end with {children} children");
    for (i, (chain, t)) in w.wakeups.iter().enumerate() {
        let who = if chain == run.chain() {
            "parent"
        } else {
            "child"
        };
        row(&format!("{label}, wakeup {} ({who})", i + 1), t);
    }
    let all = worked(&b.workers.since(0));
    // The parent's end is its last segment: the wakeups its children's ends bring come after it.
    let before = worked(&b.workers.since(0)[..mark]);
    // § m0vtdttae.e@k1g1r
    // Its end's fan-out to its children included.
    let mut through_end = recordable(&before, &[run.chain()]);
    assert_eq!(w.wakeups[0].0, run.chain(), "{w:?}");
    add(&mut through_end, &w.wakeups[0].1);
    b.check_requests(&label, run.chain(), through_end).await;
    // § end m0vtdttae.e
    for kid in &kids {
        b.check_recorded(&label, &all, kid, &[kid]).await;
    }
    worker.shutdown().await.unwrap();
    (w, run.chain().to_owned(), kids)
}

fn kid_workflow() -> Workflow<u32> {
    Workflow::without_input("waiter", |c| async move { c.receive::<u32>("never").await })
}

/// The difference `a - b`, operation by operation.
fn minus(a: Tally, b: Tally) -> Tally {
    Tally {
        put: a.put - b.put,
        cas: a.cas - b.cas,
        get: a.get - b.get,
        list: a.list - b.list,
        wait: a.wait - b.wait,
        delete: a.delete - b.delete,
        trips: a.trips.saturating_sub(b.trips),
    }
}

/// An end that cancels its running children, not abandoned: completed, failed or cancelled, its
/// fan-out reads each child's head and sends it the cancel, and each child's cancelled end tells
/// the ended parent, which drops the notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_end_cancels_its_children() {
    for end in ["cancelled", "completed", "failed"] {
        // § one fca4h4ker@mwnnd
        // silcrow:follow
        let (one, parent, _) = an_end_with_children(end, 1).await;
        let (three, three_parent, _) = an_end_with_children(end, 3).await;
        let own = |w: &Worked, parent: &str| -> Vec<Tally> {
            (w.wakeups.iter())
                .filter(|(chain, _)| chain.as_str() == parent)
                .map(|(_, t)| *t)
                .collect()
        };
        // The parent's end wakeup: its own operations, and per child the fan-out.
        let (end_one, end_three) = (own(&one, &parent)[0], own(&three, &three_parent)[0]);
        let delta = minus(end_three, end_one);
        // § fca4h4ker.a@hcbzz
        let per_child = Tally {
            put: delta.put / 2,
            cas: delta.cas / 2,
            get: delta.get / 2,
            list: delta.list / 2,
            wait: 0,
            delete: delta.delete / 2,
            trips: 0,
        };
        println!("a {end} end, its fan-out per child: {per_child:?}");
        // The parent's end: the message that ends it read, the end's cas pending with its expiry,
        // the child's head read and its cancel sent, the cas that clears the pending, its watchers
        // listed, its rest marker and the served entry deleted.
        check(
            &format!("a {end} end, 1 child"),
            end_one,
            ops(1, 2, 2, 1, 2, 8),
        );
        // Per child: 1 GET of its head, 1 PUT of its cancel.
        let measured = Tally {
            trips: 0,
            ..per_child
        };
        check(
            &format!("a {end} end, per child"),
            measured,
            ops(1, 0, 1, 0, 0, 0),
        );
        assert_eq!(
            delta.put % 2 + delta.get % 2 + delta.cas % 2,
            0,
            "{delta:?}"
        );
        let children: Vec<Tally> = (one.wakeups.iter())
            .filter(|(chain, _)| chain.as_str() != parent)
            .map(|(_, t)| *t)
            .collect();
        // The child's cancelled end: its cancel read, its end's cas pending, its notice to the
        // parent, the cas that clears it with the expiry, its rest marker deleted, its watchers
        // listed, the served entry deleted.
        check_wakeups(
            &format!("a {end} end, the child's cancelled end"),
            &Worked {
                wakeups: children.iter().map(|t| ("child".to_owned(), *t)).collect(),
                ops: Vec::new(),
                scans: 0,
                scan_lists: 0,
            },
            &[ops(1, 2, 1, 1, 2, 7)],
        );
        // Each child's notice wakes the ended parent, which reads its head, lists its watchers
        // again and deletes the notice and its entry, writing nothing.
        let after = |w: &Worked, parent: &str, label: &str, expected: &[Tally]| {
            let after: Vec<Tally> = own(w, parent)[1..].to_vec();
            check_wakeups(
                label,
                &Worked {
                    wakeups: after.iter().map(|t| ("parent".to_owned(), *t)).collect(),
                    ops: Vec::new(),
                    scans: 0,
                    scan_lists: 0,
                },
                expected,
            );
        };
        let label = format!("a {end} end, the parent after its child's end");
        let notice = ops(0, 0, 1, 1, 2, 4);
        after(&one, &parent, &label, &[notice]);
        let label = format!("a {end} end, the parent after each of its 3 children's ends");
        after(&three, &three_parent, &label, &[notice, notice, notice]);
        // § end fca4h4ker.a
    }
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
                other => Err(Error::fail("Unexpected", format!("{other:?}"))),
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
    // Before its commit, the send reads the child's head, not created yet: GET 1 over the row of
    // a child started alone, and after it the message entry, PUT 1.
    let expected = [
        ops(4, 2, 2, 0, 1, 8),
        ops(1, 2, 2, 1, 2, 7),
        ops(0, 1, 1, 1, 2, 5),
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
    let expected = [
        ops(4, 2, 2, 0, 1, 8),
        ops(1, 2, 2, 1, 2, 7),
        ops(0, 1, 1, 1, 2, 5),
    ];
    let label = "parent starts a child and cancels it, child ends cancelled, parent ends";
    check_wakeups(label, &w, &expected);
    let (parent, kid) = (run.chain(), w.wakeups[1].0.as_str());
    b.check_recorded(label, &w, parent, &[parent]).await;
    b.check_recorded(label, &w, kid, &[kid]).await;
    worker.shutdown().await.unwrap();
}

/// A parent that starts a child with `abandon` and ends at once: its end is written with its own
/// expiry, and the child's late notice costs the ended parent a read and the deletes, no write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_before_its_abandoned_child() {
    let b = bench().await;
    let stray = Workflow::without_input("stray", |c| async move {
        step_n(&c, 0).await?;
        c.sleep(SLEEP).await?;
        Ok(2u32)
    });
    let s = stray.clone();
    let leaver = Workflow::without_input("leaver", move |c| {
        let s = s.clone();
        async move {
            c.start(&s, ()).id("k2").abandon().await?;
            Ok(0u32)
        }
    });
    let worker = b.started(b.worker((leaver.clone(), stray.clone()))).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&leaver, ()).id("p2").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    b.sub
        .run(&stray, "k2")
        .result()
        .timeout(LONG)
        .await
        .unwrap();
    let w = b.since(mark).await;
    // § a1ys7at7p@afnfz
    assert_eq!(
        b.workers.expiry("runs/p2"),
        Some(30),
        "the parent's own expiry"
    );
    // The parent: its head read, its end pending, the child's head and due entry, the cas that
    // clears the pending with its expiry, its watchers listed. The child: its step and sleep, then
    // its end telling the parent. The ended parent: its head read, its watchers listed again, the
    // notice's entry deleted.
    let expected = [
        ops(2, 2, 1, 1, 1, 7),
        ops(1, 2, 1, 0, 1, 5),
        ops(1, 2, 0, 1, 1, 5),
        ops(0, 0, 1, 1, 1, 3),
    ];
    check_wakeups("a parent before its abandoned child", &w, &expected);
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
    check("status, running", t, ops(0, 0, 1, 0, 0, 1));
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    let t = w.wakeups[0].1;
    row("a 1.2 s step, claim_ttl 400ms", &t);
    // § j1nq6rqpf@mevg5
    // A renewal every claim_ttl / 2: one cas of the head's lease.
    let renewals = t.cas - 2;
    assert!((4..=6).contains(&renewals), "{t:?}");
    assert_eq!(t, ops(0, 2 + renewals, 1, 1, 1, 5 + renewals));
    // § end j1nq6rqpf
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
    // § one qsw5w98x3@b5kg4
    assert!((9..=11).contains(&w.scans), "a scan every max_idle: {w:?}");
    // § one d9vmk6y6y.a@w8da4
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
    // § qsw5w98x3.c@vm41m
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
    check_wakeups("1 step on h3", &w, &[ops(0, 2, 1, 1, 1, 5)]);
    worker.shutdown().await.unwrap();
}

/// A run of one segment ended: its head is written with its retention as its expiry, and nothing
/// else is left of it, no entry and no later operation; once the store removed it, its id is free.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ended_run_expires_and_its_id_is_free_again() {
    let b = bench().await;
    let wf = steps();
    let worker = b.started(b.worker(&wf)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&wf, K { k: 0 }).id("r").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    internal::wake(&worker);
    let w = b.since(mark).await;
    // § jbkv4v0we.e@3tdtt
    // § one a1ys7at7p.a@4552h
    // silcrow:follow
    check_wakeups(
        "a run of one segment, then nothing",
        &w,
        &[ops(0, 1, 1, 1, 1, 4)],
    );
    // § a1ys7at7p@5v1w9
    assert_eq!(b.workers.expiry("runs/r"), Some(30), "the end's expiry");
    let stored = b.stored().await;
    let kinds: Vec<&str> = stored.keys().map(String::as_str).collect();
    assert_eq!(kinds, ["runs", "substantial.json"]);
    // § qkh68yrf1@x2sn2
    let (taken, t) = b.client(b.sub.start(&wf, K { k: 1 }).id("r")).await;
    assert!(matches!(taken, Err(Error::RunExists { .. })), "{taken:?}");
    check("start, id kept: RunExists", t, ops(1, 0, 1, 0, 0, 2));
    // As its expiry removes it.
    b.inner.delete("runs/r").await.unwrap();
    let (status, t) = b.client(run.status()).await;
    assert!(
        matches!(status, Err(Error::RunNotFound { .. })),
        "{status:?}"
    );
    check("status, removed: RunNotFound", t, ops(0, 0, 1, 0, 0, 1));
    let (again, t) = b.client(b.sub.start(&wf, K { k: 1 }).id("r")).await;
    again.unwrap();
    check("start, after the run's removal", t, ops(2, 0, 0, 0, 0, 2));
    // § end qkh68yrf1
    // § end jbkv4v0we.e
    worker.shutdown().await.unwrap();
}

/// A due entry of a removed id, as a late sender's leaves: its wakeup reads the id is gone and
/// deletes the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wakeup_of_a_removed_id() {
    let b = bench().await;
    let wf = steps();
    let worker = b.started(b.worker(&wf)).await;
    let run = b.sub.start(&wf, K { k: 0 }).id("r").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    quiet(&b.workers).await;
    b.inner.delete("runs/r").await.unwrap();
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
    let late = format!(
        "queues/steps/due/{}~r~s0000000001",
        now.format("%Y%m%dt%H%M%S%3fz")
    );
    b.inner
        .create(&late, Default::default(), None)
        .await
        .unwrap();
    let mark = b.workers.mark();
    internal::wake(&worker);
    let w = b.since(mark).await;
    // § rbvr70kyv.a@8qp7d
    check_wakeups("a wakeup of a removed id", &w, &[ops(0, 0, 1, 0, 1, 2)]);
    worker.shutdown().await.unwrap();
}

/// A chain of 3 runs, each continued after a sleep: every run stays readable by its id, the first
/// one's archive under `archives/`, the second's under its id, the last one in the chain's head
/// with a pointer under its id, each written with the retention as its expiry once the chain ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_lived_chain_keeps_each_run_readable() {
    let b = bench().await;
    let chain = Workflow::new("chain", |c, input: N| async move {
        c.sleep(Duration::from_millis(300)).await?;
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok(0u32)
    });
    let worker = b.started(b.worker(&chain)).await;
    let mark = b.workers.mark();
    let run = b.sub.start(&chain, N { n: 2 }).id("c").await.unwrap();
    internal::wake(&worker);
    run.result().timeout(LONG).await.unwrap();
    let w = b.since(mark).await;
    for (i, (run, t)) in w.wakeups.iter().enumerate() {
        row(&format!("a chain of 3 runs, wakeup {} ({run})", i + 1), t);
    }
    // § a1ys7at7p.c@tg0ry
    let head = b.current("c").await;
    let first = b.head(&head.start.clone().unwrap().previous.unwrap()).await;
    let runs = [
        first.start.clone().unwrap().previous.unwrap(),
        first.run,
        head.run,
    ];
    assert_eq!(runs[0], "c");
    for id in &runs {
        let head = b.head(id).await;
        assert_eq!(&head.run, id);
    }
    let stored = b.stored().await;
    for key in b.inner.list("", None, 1000, None).await.unwrap().keys {
        if key.starts_with("runs/") || key.starts_with("archives/") {
            assert_eq!(b.workers.expiry(&key), Some(30), "{key}");
        }
    }
    let counts: Vec<(&str, usize)> = stored.iter().map(|(k, (n, _))| (k.as_str(), *n)).collect();
    assert_eq!(
        counts,
        [("archives", 1), ("runs", 3), ("substantial.json", 1)]
    );
    // § end a1ys7at7p.c
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
/// replays the run, and the two others find its lease live or the run ended, and leave it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_at_a_timer() {
    let b = bench().await;
    // Longer than it takes to wake the three workers one by one. The step after it makes the
    // wakeup lease the run, which the other workers then meet.
    let wf = Workflow::without_input("napper", |c| async move {
        c.sleep(Duration::from_secs(4)).await?;
        c.step("after", || async { Ok::<_, Error>(1u32) }).await
    });
    let script = |sub: &Substantial| sub.worker(&wf).preset(Preset::Script).set("max_idle", "1h");
    let fleet = fleet(&b, 3, script).await;
    // § qsw5w98x3.a@0rebm
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
        if std::env::var("COSTS_TRACE").is_ok() {
            for op in store.since(mark) {
                println!(
                    "  worker {}: {:?} {} at {:?}",
                    i + 1,
                    op.kind,
                    op.key,
                    op.started
                );
            }
        }
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
    // The replay is the worker whose cas of the lease won, then its end's.
    let replayed = |w: &Worked| w.wakeups.iter().any(|(_, t)| t.cas == 2);
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
            // The worker that suspended the run holds its head; another reads it. A worker that
            // loses reads the head, and its cas of the lease may find the winner's: it reads the
            // head again. A head it reads ended, it redoes the end's last steps, the listing of
            // the watchers and the delete of the entry; a head under a live lease, it skips the
            // run and, once it let it go, lists again: the scan at the timer and that one.
            if replayed(w) {
                assert!(
                    [ops(0, 2, 0, 1, 1, 4), ops(0, 2, 1, 1, 1, 5)].contains(t),
                    "{w:?}"
                );
                assert_eq!(w.scans, 1, "{w:?}");
            } else {
                // § j1nq6rqpf.a@vr6dq
                let ended = [ops(0, 1, 2, 1, 1, 5), ops(0, 0, 1, 1, 1, 3)];
                let leased = [ops(0, 1, 2, 0, 0, 3), ops(0, 0, 1, 0, 0, 1)];
                assert!(ended.contains(t) || leased.contains(t), "{w:?}");
                assert_eq!(w.scans, if leased.contains(t) { 2 } else { 1 }, "{w:?}");
                // § j1nq6rqpf.d@0tb9c
                // A cas is its own lease's, which found the run leased since its read: dropped
                // before its attempt, it deleted nothing unless the run had ended.
                if t.cas == 1 && t.list == 0 {
                    assert_eq!(t.delete, 0, "{w:?}");
                }
                // § end j1nq6rqpf.d
            }
        }
    }
    for (_, worker) in fleet {
        worker.shutdown().await.unwrap();
    }
}

/// Three workers serve a workflow whose run ended: its end left no entry, so no worker's timer is
/// set by it, and the scans after it find nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_after_an_end() {
    let b = bench().await;
    let wf = steps();
    let fleet = fleet(&b, 3, |sub| {
        sub.worker(&wf).preset(Preset::Script).set("max_idle", "1h")
    })
    .await;
    let run = b.sub.start(&wf, K { k: 0 }).await.unwrap();
    internal::wake(&fleet[0].1);
    run.result().timeout(LONG).await.unwrap();
    for (store, worker) in &fleet {
        internal::wake(worker);
        quiet(store).await;
    }
    let marks: Vec<usize> = fleet.iter().map(|(store, _)| store.mark()).collect();
    tokio::time::sleep(SLEEP).await;
    // § qsw5w98x3.d@4vr58
    for (store, mark) in fleet.iter().map(|(store, _)| store).zip(&marks) {
        assert!(store.since(*mark).is_empty(), "a timer set by an end");
    }
    for (i, ((store, worker), mark)) in fleet.iter().zip(marks).enumerate() {
        internal::wake(worker);
        quiet(store).await;
        let w = worked(&store.since(mark));
        let label = format!("3 workers after an end, worker {}", i + 1);
        check_wakeups(&label, &w, &[]);
        assert_eq!((w.scans, w.scan_lists), (1, 1), "{label}: its scan");
    }
    // § end qsw5w98x3.d
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
    // A page of `blocked/`, one of `due/`, one of `rest/`, the start of each run.
    check("runs, 2 pending", t, ops(0, 0, 2, 3, 0, 5));

    let mark = b.workers.mark();
    let worker = b
        .started(b.worker((waits.clone(), flaky.clone(), undone.clone())))
        .await;
    let w = b.since(mark).await;
    // Each first wait on messages alone creates the run's rest marker.
    check_wakeups(
        "receive at once, 2 runs",
        &w,
        &[ops(1, 1, 1, 0, 1, 4), ops(1, 1, 1, 0, 1, 4)],
    );
    let (listed, t) = b.client(b.sub.runs(&waits).try_collect::<Vec<_>>()).await;
    assert!(
        listed
            .unwrap()
            .iter()
            .all(|r| r.waiting_on == substantial::WaitingOn::Message)
    );
    // Runs found by their marker alone: each one's head.
    check("runs, 2 waiting on messages", t, ops(0, 0, 2, 3, 0, 5));
    let p1 = b.sub.run(&waits, "p1");
    let (history, t) = b.client(p1.history()).await;
    assert_eq!(history.unwrap().len(), 0);
    // The head, which holds the run's segments.
    check("history, 1 segment", t, ops(0, 0, 1, 0, 0, 1));

    let run = b.sub.start(&flaky, ()).id("f").await.unwrap();
    internal::wake(&worker);
    assert!(run.result().timeout(LONG).await.is_err());
    let (status, t) = b.client(run.status()).await;
    assert_eq!(status.unwrap(), RunStatus::Failed);
    // The head, which holds the failed end.
    check("status, failed", t, ops(0, 0, 1, 0, 0, 1));
    quiet(&b.workers).await;
    refuse.store(false, Ordering::SeqCst);
    let mark = b.workers.mark();
    // § vb6krfmej@v4q5g
    let (retried, t) = b.client(run.retry()).await;
    retried.unwrap();
    // The head, its cas pending with the failed run's archive, the archive, the retry's pointer and
    // due entry, the cas that clears the pending.
    check("retry", t, ops(3, 2, 1, 0, 0, 6));
    internal::wake(&worker);
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 2);
    // § end vb6krfmej
    let w = b.since(mark).await;
    let compensated = b.sub.start(&undone, ()).id("u").await.unwrap();
    internal::wake(&worker);
    assert!(compensated.result().timeout(LONG).await.is_err());
    // § vb6krfmej.a@bf6tr
    let (refused, t) = b.client(compensated.retry()).await;
    assert!(
        matches!(refused, Err(Error::InvalidArgument(_))),
        "{refused:?}"
    );
    // The head, whose last segment holds the saga's undo.
    check("retry, a failure that ran undos", t, ops(0, 0, 1, 0, 0, 1));
    // § end vb6krfmej.a
    // It reads the head, which holds the failed run's events; its lease, its end two-phase: the
    // end's cas pending, the pointer's expiry, the head's.
    check_wakeups("the retry's first wakeup", &w, &[ops(0, 4, 1, 1, 1, 7)]);
    let (history, t) = b.client(run.history()).await;
    assert_eq!(history.unwrap().len(), 3);
    // The head, which holds the retry's resumed events and its own.
    check("history, a retry", t, ops(0, 0, 1, 0, 0, 1));
    let (reset, t) = b.client(run.retry().keep(1)).await;
    reset.unwrap();
    // The head, as a retry reads it, its cas pending, the reset run's archive written over its
    // pointer, a cas, the new run's pointer and due entry, the cas that clears the pending.
    check(
        "retry, keep: a reset of a completed run",
        t,
        ops(2, 3, 1, 0, 0, 6),
    );
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
    // § m2284agp0@f6dbg
    let mark = b.workers.mark();
    b.worker(&wf).drain(SLEEP).await.unwrap();
    // § one m2284agp0.b@hser1
    // silcrow:follow
    drained(&b, "drain, nothing due", mark, ops(0, 0, 0, 1, 0, 1), 1);

    let run = b.sub.start(&wf, K { k: 1 }).await.unwrap();
    let mark = b.workers.mark();
    b.worker(&wf).drain(SLEEP).await.unwrap();
    assert_eq!(run.result().await.unwrap(), 1);
    let label = "drain, one run of one step due now";
    let w = drained(&b, label, mark, ops(0, 2, 1, 2, 1, 6), 1);
    check_wakeups(label, &w, &[ops(0, 2, 1, 1, 1, 5)]);
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
    let w = drained(&b, label, mark, ops(1, 3, 1, 3, 2, 10), 2);
    check_wakeups(label, &w, &[ops(1, 2, 1, 0, 1, 5), ops(0, 1, 0, 1, 1, 3)]);
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
    // The first wakeup also gets the head and makes a progress commit; the third poke is ready and
    // ends the run.
    let not_ready = ops(1, 2, 0, 0, 1, 4);
    let expected = [ops(1, 3, 1, 1, 1, 7), not_ready, ops(0, 2, 0, 1, 1, 4)];
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
/// finds it due, the body's step, and the plan step that finds the next one. Once the setup is done,
/// the worker's clock is moved ahead so that its next minute starts `LEAD` later, not up to a minute
/// later: `LEAD` covers only the wakeup that plans the fire.
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
    const LEAD: Duration = Duration::from_secs(3);
    let shift = Arc::new(AtomicI64::new(0));
    let worker = b
        .started(internal::shifted(b.worker(&wf), shift.clone()))
        .await;
    let every_minute = substantial::Cron::new("* * * * *", "UTC").unwrap();
    b.sub
        .schedule(&wf, "tick", Some(every_minute), 1)
        .await
        .unwrap();
    // The client's schedule does not wake the worker: nothing plans the fire before this wake.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let at = now + LEAD.as_millis() as i64;
    shift.store((at + 59_999) / 60_000 * 60_000 - at, Ordering::SeqCst);
    internal::wake(&worker);
    quiet(&b.workers).await;
    assert!(
        !fired.load(Ordering::SeqCst),
        "the fire came before the count began: its planning wakeup took longer than {LEAD:?}"
    );
    let mark = b.workers.mark();
    let started = Instant::now();
    while !fired.load(Ordering::SeqCst) {
        assert!(started.elapsed() < LEAD + LONG, "no fire");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let w = b.since(mark).await;
    // The lease, three progress commits (a cas and a listing of the chain's messages each), the
    // published value in the head, then the suspension's due entry and cas.
    // § kkcyc97cz@7n4kn
    check_wakeups(
        "schedule, a fire whose body runs a step",
        &w,
        &[ops(1, 5, 0, 3, 1, 10)],
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

/// A watcher registers on a live chain of one run: it reads the chain's head, creates its key
/// under the chain, reads the head again, and suspends. The chain's end then lists its watchers
/// and tells the one it finds: a GET of its head, a message, the delete of its key, which the
/// end's segment does not record, as they come after its last write.
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
    check_wakeups(label, &w, &[ops(2, 1, 3, 0, 1, 7)]);
    b.check_recorded(label, &w, "w", &["w"]).await;
    let mark = b.workers.mark();
    b.sub.run(&target, "t").send("go", "done").await.unwrap();
    internal::wake(&worker);
    let result = b.sub.run(&watcher, "w").result().timeout(LONG).await;
    assert_eq!(result.unwrap(), "done");
    let w = b.since(mark).await;
    let label = "c.result, the watched end tells one watcher, the watcher ends";
    check_wakeups(label, &w, &[ops(1, 1, 2, 1, 3, 8), ops(0, 1, 1, 1, 2, 5)]);
    let all = worked(&b.workers.since(0));
    b.check_recorded(label, &all, "t", &["t"]).await;
    b.check_recorded(label, &all, "w", &["w"]).await;
    worker.shutdown().await.unwrap();
}

/// A watcher of a chain that ended reads its head and ends at once: no key, no message.
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
    check_wakeups(label, &w, &[ops(0, 1, 2, 1, 1, 5)]);
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
    let watcher_end = ops(0, 1, 1, 1, 2, 5);
    check_wakeups(
        label,
        &w,
        &[ops(2, 1, 3, 1, 4, 11), watcher_end, watcher_end],
    );
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
    check_wakeups(label, &w, &[ops(4, 2, 2, 0, 3, 10)]);
    let mark = b.workers.mark();
    b.sub.run(&target, "t").send("go", "second").await.unwrap();
    internal::wake(&worker);
    let result = b.sub.run(&watcher, "w").result().timeout(LONG).await;
    assert_eq!(result.unwrap(), "second");
    let w = b.since(mark).await;
    let label = "c.result, the next run's end tells the carried watcher, the watcher ends";
    check_wakeups(label, &w, &[ops(1, 3, 2, 1, 3, 10), ops(0, 1, 1, 1, 2, 5)]);
    let next = b.current("t").await.run;
    let ops = b.workers.since(0);
    let archived = through(&ops, "t", &format!("runs/{next}"));
    b.check_requests(label, "t", archived).await;
    b.check_recorded(label, &worked(&ops), &next, &["t"]).await;
    worker.shutdown().await.unwrap();
}
