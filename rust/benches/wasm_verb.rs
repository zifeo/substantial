//! A measure, not a check: what a verb costs through a Wasm workflow beside the host's own code.
//! Each cost is the difference between runs of `MANY` verbs and runs of none, so the worker's own
//! latency cancels out, on the memory store, so no store operation hides it.
//!
//!   cargo bench -p substantial --features wasm-runner --bench wasm_verb

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use substantial::__internal::{self as internal, MemoryStore};
use substantial::wasm::{Limits, Module};
use substantial::{Context, Preset, Workflow};

const MANY: u32 = 50_000;
const ROUNDS: usize = 7;
const LONG: Duration = Duration::from_secs(30);

/// The example Wasm workflow `verbs`, built for wasm32-unknown-unknown in release and made a
/// component.
fn verbs() -> Vec<u8> {
    let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("wasm");
    let status = Command::new(env!("CARGO"))
        .args(["build", "--locked", "--release", "-p", "substantial"])
        .args(["--example", "verbs", "--target", "wasm32-unknown-unknown"])
        .arg("--target-dir")
        .arg(&target)
        .status()
        .expect("cargo builds the example Wasm workflow");
    assert!(status.success(), "the example Wasm workflow does not build");
    let module = target.join("wasm32-unknown-unknown/release/examples/verbs.wasm");
    let module = std::fs::read(&module).expect("the example is built");
    wit_component::ComponentEncoder::default()
        .module(&module)
        .expect("the example carries its world")
        .validate(true)
        .encode()
        .expect("the example is made a component")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let limits = Limits {
        memory: 16 << 20,
        code_time: Duration::from_secs(1),
    };
    let module = Module::new(&verbs(), limits, |_| Ok(())).unwrap();
    let wasm = module.workflow("verbs");
    let native = Workflow::new(
        "native",
        |c: Context, input: serde_json::Value| async move {
            let n = input["n"].as_u64().unwrap();
            for _ in 0..n {
                c.random().await?;
            }
            Ok(n)
        },
    );
    let store = MemoryStore::open(&format!("wasm-verb-{}", std::process::id())).unwrap();
    let sub = internal::create(std::sync::Arc::new(store)).await.unwrap();
    let worker = (sub.worker((wasm.clone(), native.clone())))
        .preset(Preset::Script)
        .set("max_idle", "50ms")
        .start()
        .unwrap();
    let median = |mut d: Vec<Duration>| {
        d.sort();
        d[d.len() / 2]
    };
    let (mut wasm_runs, mut native_runs) = ([vec![], vec![]], [vec![], vec![]]);
    for round in 0..ROUNDS {
        for (i, n) in [0, MANY].into_iter().enumerate() {
            let input = serde_json::json!({ "n": n });
            let started = Instant::now();
            let run = (sub.start(&wasm, input.clone()))
                .id(&format!("wasm-{round}-{n}"))
                .await
                .unwrap();
            run.result().timeout(LONG).await.unwrap();
            wasm_runs[i].push(started.elapsed());
            let started = Instant::now();
            let run = (sub.start(&native, input))
                .id(&format!("native-{round}-{n}"))
                .await
                .unwrap();
            run.result().timeout(LONG).await.unwrap();
            native_runs[i].push(started.elapsed());
        }
    }
    worker.shutdown().await.unwrap();
    let [wasm_none, wasm_many] = wasm_runs.map(median);
    let [native_none, native_many] = native_runs.map(median);
    let per_verb = |many: Duration, none: Duration| many.saturating_sub(none) / MANY;
    let (wasm_verb, native_verb) = (
        per_verb(wasm_many, wasm_none),
        per_verb(native_many, native_none),
    );
    println!("medians of {ROUNDS} rounds, {MANY} verbs a run, memory store:");
    println!("  a run of no verb: Wasm {wasm_none:?}, native {native_none:?}");
    println!("  a verb: Wasm {wasm_verb:?}, native {native_verb:?}");
    println!(
        "  the Wasm workflow's extra: {:?} a verb, {:?} a run",
        wasm_verb.saturating_sub(native_verb),
        wasm_none.saturating_sub(native_none)
    );
}
