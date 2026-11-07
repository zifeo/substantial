//! What workers and workflows log, alone in its binary: it installs the process's tracing
//! subscriber.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use common::{LONG, script};
use substantial::__internal::{
    self as internal, Capabilities, Create, ListWait, Listing, MemoryStore, Store, StoreFuture,
};
use substantial::{Context, Error, Permanent, Result, Substantial, Workflow, step, workflow};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, span};

/// An event: its target, its level and its message.
type Line = (String, Level, String);

struct Capture(Arc<Mutex<Vec<Line>>>);

struct Message(Option<String>);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut message = Message(None);
        event.record(&mut message);
        let meta = event.metadata();
        if let Some(message) = message.0 {
            let line = (meta.target().to_owned(), *meta.level(), message);
            self.0.lock().unwrap().push(line);
        }
    }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

/// Every line logged by the process so far, the subscriber installed at the first call.
fn lines() -> Vec<Line> {
    static LINES: OnceLock<Arc<Mutex<Vec<Line>>>> = OnceLock::new();
    let lines = LINES.get_or_init(|| {
        let lines = Arc::new(Mutex::new(Vec::new()));
        tracing::subscriber::set_global_default(Capture(lines.clone())).unwrap();
        lines
    });
    lines.lock().unwrap().clone()
}

#[step]
async fn work(n: u32) -> anyhow::Result<u32> {
    Ok(n)
}

#[workflow]
async fn logging(c: Context) -> Result<u32> {
    c.log.info("before");
    let a = work(1).await?;
    c.sleep(Duration::from_millis(100)).await?;
    // The first log of a wakeup, before any step: the replay is past the history at the timer.
    c.log.info("woke");
    let b = work(2).await?;
    c.log.warn(format!("after {}", a + b));
    Ok(a + b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_are_muted_while_a_replay_walks_the_history() {
    lines();
    let sub = Substantial::create("memory://logs")
        .partitions(1)
        .await
        .unwrap();
    let worker = script(&sub, logging).start().unwrap();
    let run = sub.start(logging, ()).await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 3);
    worker.shutdown().await.unwrap();
    let logged: Vec<String> = lines()
        .into_iter()
        .filter(|(target, ..)| target == "substantial::workflow::log")
        .map(|(.., message)| message)
        .collect();
    assert_eq!(logged, ["before", "woke", "after 3"]);
}

/// The lines about the run `chain`.
fn about(chain: &str) -> Vec<(Level, String)> {
    let prefix = format!("run {chain} ");
    lines()
        .into_iter()
        .filter(|(_, _, message)| message.starts_with(&prefix))
        .map(|(_, level, message)| (level, message))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_or_cancelled_run_logs_one_warning() {
    lines();
    let sub = Substantial::create("memory://logs-ends")
        .partitions(1)
        .await
        .unwrap();
    // Each run is replayed several times before it ends: after its step, its sleep, its message.
    let ending = Workflow::without_input("ending", |c| async move {
        c.step("first", || async { Ok::<_, Error>(()) }).await?;
        c.sleep(Duration::from_millis(50)).await?;
        let how: String = c.receive("how").await?;
        if how == "fail" {
            c.step("broken", || async {
                Err::<(), _>(Permanent::new("no disk"))
            })
            .await?;
        }
        Ok(())
    });
    let worker = script(&sub, &ending).start().unwrap();
    for (id, how) in [("ends-failed", "fail"), ("ends-ok", "ok")] {
        let run = sub.start(&ending, ()).id(id).await.unwrap();
        run.send("how", how).await.unwrap();
        let _ = run.result().timeout(LONG).await;
    }
    let cancelled = sub.start(&ending, ()).id("ends-cancelled").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancelled.cancel("not needed").await.unwrap();
    assert!(cancelled.result().timeout(LONG).await.is_err());
    worker.shutdown().await.unwrap();
    assert_eq!(
        about("ends-failed"),
        [(
            Level::WARN,
            "run ends-failed failed: StepFailed: step broken failed after 1 attempt: no disk"
                .into()
        )]
    );
    assert_eq!(about("ends-ok"), []);
    assert_eq!(
        about("ends-cancelled"),
        [(
            Level::WARN,
            "run ends-cancelled cancelled: not needed".into()
        )]
    );
}

/// A memory store whose listings of due entries panic once armed, as a bug in the worker would.
struct Panicking {
    inner: MemoryStore,
    armed: AtomicBool,
}

impl Store for Panicking {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn open(&self) -> futures::future::BoxFuture<'_, Result<()>> {
        Store::open(&self.inner)
    }
    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        self.inner.create(key, bytes)
    }
    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
        self.inner.get(key)
    }
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        if prefix.starts_with("queues/") && self.armed.load(Ordering::SeqCst) {
            panic!("a bug");
        }
        self.inner.list(prefix, start_after, max_keys, wait)
    }
    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        self.inner.delete(key)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_dies_logs_and_fails_the_waits() {
    lines();
    let store = Arc::new(Panicking {
        inner: MemoryStore::open("logs-dies").unwrap(),
        armed: AtomicBool::new(false),
    });
    let sub = internal::create(store.clone()).partitions(1).await.unwrap();
    let waiting = Workflow::without_input(
        "waiting",
        |c| async move { c.receive::<u32>("never").await },
    );
    let worker = script(&sub, &waiting).start().unwrap();
    let run = sub.start(&waiting, ()).await.unwrap();
    let wait = tokio::spawn(async move { run.result().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(worker.is_alive() && worker.error().is_none());
    store.armed.store(true, Ordering::SeqCst);
    let failed = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the wait fails at once")
        .unwrap();
    match failed {
        Err(Error::Protocol(m)) => assert_eq!(m, "the worker panicked: a bug"),
        other => panic!("{other:?}"),
    }
    // What a readiness probe reads.
    assert!(!worker.is_alive());
    assert!(matches!(worker.error(), Some(Error::Protocol(_))));
    let died: Vec<Line> = lines()
        .into_iter()
        .filter(|(_, level, message)| *level == Level::ERROR && message.starts_with("worker died"))
        .collect();
    assert_eq!(died.len(), 1, "{died:?}");
    assert_eq!(
        died[0].2,
        "worker died: unexpected core response: the worker panicked: a bug"
    );
    assert!(matches!(worker.shutdown().await, Err(Error::Protocol(_))));
}
