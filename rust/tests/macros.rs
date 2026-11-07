//! `#[workflow]` and `#[step]`, end to end on a memory store.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex};

use common::{LONG, script};
use serde::{Deserialize, Serialize};
use substantial::{
    AsWorkflow, Context, Error, Handlers, Permanent, Result, Retry, RunStatus, Substantial, Var,
    minutes, step, step_key, workflow,
};

static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static UNDONE: LazyLock<Mutex<Vec<String>>> = LazyLock::new(Mutex::default);

const RETRY: Retry = Retry {
    max_attempts: 3,
    initial: std::time::Duration::from_millis(10),
    max: std::time::Duration::from_millis(10),
    multiplier: 1.0,
};

#[step(retry = RETRY)]
async fn flaky(times: u32) -> anyhow::Result<u32> {
    let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
    if attempt < times {
        anyhow::bail!("attempt {attempt}")
    }
    Ok(attempt)
}

#[step(retry = RETRY)]
async fn refuse() -> anyhow::Result<()> {
    ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    Err(Permanent::new("never").into())
}

async fn unreserve(id: String, item: &str, count: u32) -> anyhow::Result<()> {
    UNDONE.lock().unwrap().push(format!("{id} {item} {count}"));
    Ok(())
}

#[step(name = "reserve-item", timeout = minutes(1), undo = unreserve)]
async fn reserve(item: &str, count: u32) -> anyhow::Result<String> {
    assert!(count > 0);
    Ok(format!("{item}-{}", step_key().rsplit('/').next().unwrap()))
}

#[step]
async fn add(a: u32, b: u32) -> std::result::Result<u32, Error> {
    Ok(a + b)
}

#[derive(Serialize, Deserialize)]
struct Pair {
    a: u32,
    b: u32,
}

#[workflow]
async fn sum(c: Context, input: Pair) -> Result<u32> {
    add(input.a, input.b).await
}

#[derive(Default)]
struct Tally {
    up: u32,
    names: Vec<String>,
}

impl Tally {
    fn done(&self) -> bool {
        self.up >= 3 && !self.names.is_empty()
    }
}

/// Steps with and without arguments, in parallel, a retry, a permanent error, a child, handlers.
#[workflow(name = "everything", version = 2)]
async fn everything_v2(c: Context) -> Result<(u32, u32, u32, Vec<String>, String)> {
    let attempt = flaky(2).await?;
    let (x, y) = tokio::try_join!(add(1, 2), add(3, 4))?;
    let refused = match refuse().await {
        Err(Error::StepFailed { error, .. }) => error.message,
        other => format!("{other:?}"),
    };
    let child = c.start(&sum, Pair { a: x, b: y }).await?;
    let tally = Var::new(Tally::default());
    c.handle("up", tally.handler(|t, by: u32| t.up += by))?;
    c.handle_all(Handlers::new().on("name", tally.handler(|t, name: String| t.names.push(name))))?;
    // A second handler of the same name gets each message too.
    let notes = Var::new(Vec::new());
    c.handle("name", notes.handler(|n, v: String| n.push(v)))?;
    c.wait_until(tally.check(Tally::done)).await?;
    c.log.info("tallied");
    let up = tally.with(|t| t.up);
    let names = notes.get();
    Ok((attempt, child.result().await?, up, names, refused))
}

#[workflow(name = "everything", step_timeout = substantial::minutes(1))]
async fn everything(c: Context) -> Result<()> {
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_workflows_steps_and_handlers() {
    assert_eq!(everything_v2.workflow().name(), "everything");
    assert_eq!(everything_v2.workflow().version(), Some(2));
    assert_eq!(everything_v2.workflow().step_timeout(), None);
    let timeout = everything.workflow().step_timeout();
    assert_eq!(timeout, Some(substantial::minutes(1)));
    assert_eq!(sum.workflow().name(), "sum");
    let sub = Substantial::create("memory://macros")
        .partitions(1)
        .await
        .unwrap();
    let worker = script(&sub, (everything, everything_v2, sum))
        .start()
        .unwrap();
    let run = sub.start(everything_v2, ()).await.unwrap();
    run.send("up", 1).await.unwrap();
    run.send("name", "ada").await.unwrap();
    run.send("up", 2).await.unwrap();
    let (attempt, total, up, names, refused) = run.result().timeout(LONG).await.unwrap();
    assert_eq!((attempt, total, up), (2, 10, 3));
    assert_eq!(names, ["ada"]);
    assert!(refused.contains("never"), "{refused}");
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        3,
        "a permanent error is not retried"
    );
    worker.shutdown().await.unwrap();
}

#[workflow]
async fn reserving(c: Context) -> Result<()> {
    let first = reserve("chair", 2).await?;
    c.saga(|_| async move {
        reserve("desk", 1).await?;
        Err::<(), _>(Error::InvalidArgument("no desk today".into()))
    })
    .await
    .ok();
    let lamp = String::from("lamp");
    let pending = reserve(&lamp, 3);
    drop(lamp);
    pending.await?;
    let _ = first;
    Err(c.rollback("cancelled order").await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undo_gets_the_value_then_the_arguments() {
    let sub = Substantial::create("memory://macros-undo")
        .partitions(1)
        .await
        .unwrap();
    let worker = script(&sub, reserving).start().unwrap();
    let run = sub.start(reserving, ()).await.unwrap();
    match run.result().timeout(LONG).await {
        Err(Error::RunFailed {
            status: RunStatus::Failed,
            error,
            ..
        }) => assert_eq!(error.kind, "RolledBack", "{error}"),
        other => panic!("{other:?}"),
    }
    let undone = UNDONE.lock().unwrap().clone();
    assert_eq!(
        undone,
        ["desk-1 desk 1", "lamp-2 lamp 3", "chair-0 chair 2"],
        "the saga's step first, then the rollback's, most recent first"
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_declared_step_outside_a_workflow_is_not_in_workflow() {
    assert!(matches!(add(1, 2).await, Err(Error::NotInWorkflow)));
}

#[workflow]
async fn inline(c: Context) -> Result<u32> {
    let base = 38;
    let n = c
        .step("inline", move || async move { anyhow::Ok(base + 2) })
        .await?;
    let m = c
        .step_blocking("blocking", move || anyhow::Ok(base / 19))
        .await?;
    Ok(n + m)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inline_step_takes_a_closure_async_or_blocking() {
    let sub = Substantial::create("memory://macros-inline")
        .partitions(1)
        .await
        .unwrap();
    let worker = script(&sub, inline).start().unwrap();
    let run = sub.start(inline, ()).id("inline-1").await.unwrap();
    assert_eq!(run.result().timeout(LONG).await.unwrap(), 42);
    assert_eq!(sub.run(inline, "inline-1").result().await.unwrap(), 42);
    worker.shutdown().await.unwrap();
}
