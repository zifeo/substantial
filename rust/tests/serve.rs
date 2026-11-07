//! `serve` until SIGTERM, alone in its binary: the signal goes to the whole test process.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Recording, eventually};
use substantial::__internal::{self as internal, MemoryStore};
use substantial::{Preset, Workflow};

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_runs_until_sigterm() {
    let store = Recording::new(Arc::new(MemoryStore::open("serve").unwrap()));
    let sub = internal::create(store.clone()).partitions(1).await.unwrap();
    let workflow = Workflow::without_input("served", |_c| async { Ok(1) });
    let served = tokio::spawn(
        sub.worker(workflow)
            .preset(Preset::Script)
            .set("max_idle", "50ms")
            .serve(),
    );
    // `serve` listens for signals before its worker lists anything.
    eventually("a listing", async || !store.listed().is_empty()).await;
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("serve returns after SIGTERM")
        .unwrap()
        .unwrap();
}
