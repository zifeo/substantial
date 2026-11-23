//! Wasm workflows: workflows compiled to WebAssembly components, served by a worker of this host
//! through the Wasm runner. They are the examples of `substantial` built for wasm32-unknown-unknown
//! here.

mod common;
#[path = "../examples/same_source.rs"]
mod same_source;

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::{LONG, blocked, eventually, namespaces, script};
use substantial::__internal as internal;
use substantial::wasm::wasmtime;
use substantial::wasm::wasmtime::Store;
use substantial::wasm::wasmtime::component::Linker;
use substantial::wasm::{Limits, Module, StepState, TICK};
use substantial::{Error, Observed, RunStatus};

/// The Wasm workflows of `substantial`'s examples: the others are native programs.
const EXAMPLES: [&str; 7] = [
    "bounds",
    "edges",
    "failures",
    "receive_with_timeout",
    "same_source",
    "sleep_then_step",
    "verbs",
];

/// The component of the example `example`, the examples built once per test process.
fn component(example: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("wasm");
    BUILT.get_or_init(|| {
        let mut build = Command::new(env!("CARGO"));
        build.args(["build", "--locked", "-p", "substantial"]);
        for example in EXAMPLES {
            build.args(["--example", example]);
        }
        build
            .args(["--target", "wasm32-unknown-unknown", "--target-dir"])
            .arg(&target)
            .env("CARGO_INCREMENTAL", "0");
        let status = build
            .status()
            .expect("cargo builds the example Wasm workflows");
        assert!(status.success(), "the example Wasm workflows do not build");
    });
    let module = target.join(format!(
        "wasm32-unknown-unknown/debug/examples/{example}.wasm"
    ));
    let module = std::fs::read(&module).expect("the example is built");
    wit_component::ComponentEncoder::default()
        .module(&module)
        .expect("the example carries its world")
        .validate(true)
        .encode()
        .expect("the example is made a component")
}

/// Bounds well above what the examples need within them.
const BOUNDS: Limits = Limits {
    memory: 16 << 20,
    code_time: Duration::from_secs(1),
};

/// The Wasm workflow `sleep_then_step`, its embedder's counter linked to `bumps`.
fn sleep_then_step(bumps: Arc<AtomicU32>) -> Module {
    Module::new(
        &component("sleep_then_step"),
        BOUNDS,
        |linker: &mut Linker<StepState>| {
            linker
                .instance("example:embedder/counter")?
                .func_wrap("bump", move |_, (): ()| {
                    Ok((bumps.fetch_add(1, Ordering::SeqCst) + 1,))
                })
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_sleeps_then_steps_replayed_from_its_history() {
    for ns in namespaces() {
        let replays = Arc::new(Mutex::new(Vec::new()));
        let seen = replays.clone();
        let sub = internal::create(ns.store)
            .observe(move |e| {
                if let Observed::Replay(r) = e {
                    seen.lock().unwrap().push(r.end);
                }
            })
            .await
            .unwrap();
        let bumps = Arc::new(AtomicU32::new(0));
        let workflow = sleep_then_step(bumps.clone()).workflow("sleep_then_step");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({"name": "wasm"});
        let run = sub.start(&workflow, input).await.unwrap();
        let result = run.result().timeout(LONG).await.unwrap();
        assert_eq!(result, "hello wasm, greeting 1", "{}", ns.name);
        // § s3vk5gvsj@sqsv0
        // One replay suspended on the sleep, and the one after it ran the step once.
        let replays = replays.lock().unwrap().clone();
        assert!(replays.contains(&"suspended"), "{}: {replays:?}", ns.name);
        assert_eq!(bumps.load(Ordering::SeqCst), 1, "{}", ns.name);
        // § end s3vk5gvsj
        let kinds: Vec<String> = (run.history().await.unwrap().into_iter())
            .map(|e| e.kind)
            .collect();
        assert_eq!(
            kinds,
            ["timer", "fired", "step", "completed"],
            "{}",
            ns.name
        );
        worker.shutdown().await.unwrap();
    }
}

/// The events of `run`'s history, each its kind and its fields but for the times it holds.
async fn events<O>(run: &substantial::Run<O>) -> Vec<(String, serde_json::Value)> {
    (run.history().await.unwrap().into_iter())
        .map(|e| match e.kind.as_str() {
            "timer" | "fired" => (e.kind, serde_json::Value::Null),
            _ => (e.kind, e.data),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_source_runs_natively_and_as_a_wasm_workflow_alike() {
    let module = Module::new(&component("same_source"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        // § c7aw13gws@mhck7
        let wasm = module.workflow("greeting-wasm");
        let worker = script(&sub, (same_source::greeting, wasm.clone()))
            .start()
            .unwrap();
        let input = serde_json::json!({"name": "both"});
        let native = sub
            .start(same_source::greeting, input.clone())
            .await
            .unwrap();
        let wasm = sub.start(&wasm, input).await.unwrap();
        let from_native = native.result().timeout(LONG).await.unwrap();
        let from_wasm = wasm.result().timeout(LONG).await.unwrap();
        let greeted = |run: &str| format!("hello both, step {run}/greet/0");
        assert_eq!(from_native, greeted(native.chain()), "{}", ns.name);
        assert_eq!(from_wasm, greeted(wasm.chain()), "{}", ns.name);
        // The values hold each run's own chain.
        let from_wasm = format!("{:?}", events(&wasm).await).replace(wasm.chain(), "<run>");
        let from_native = format!("{:?}", events(&native).await).replace(native.chain(), "<run>");
        assert_eq!(from_wasm, from_native, "{}", ns.name);
        // § end c7aw13gws
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_receives_a_message_or_its_timeout_passes() {
    let module = Module::new(&component("receive_with_timeout"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("receive_with_timeout");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let waits = serde_json::json!({"timeout_ms": 60_000});
        let received = sub.start(&workflow, waits).id("received").await.unwrap();
        received.send("note", "hi").await.unwrap();
        let expires = serde_json::json!({"timeout_ms": 200});
        let timed_out = sub.start(&workflow, expires).id("timed-out").await.unwrap();
        let note = received.result().timeout(LONG).await.unwrap();
        assert_eq!(note, "received hi", "{}", ns.name);
        let none = timed_out.result().timeout(LONG).await.unwrap();
        assert_eq!(none, "timed out", "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[test]
fn a_wasm_workflow_importing_what_no_one_gives_is_refused() {
    // § de970qkh4@0xtdz
    let refused = Module::new(&component("sleep_then_step"), BOUNDS, |_| Ok(()));
    let Err(Error::ConfigError(message)) = refused else {
        panic!("a module whose counter no one gives is loaded");
    };
    // § end de970qkh4
    assert!(message.contains("example:embedder/counter"), "{message}");
}

#[test]
fn an_embedder_calls_an_export_of_its_own() {
    let module = sleep_then_step(Arc::default());
    // § jf9zy7nf7@2xn49
    let mut linker = Linker::new(module.engine());
    linker
        .define_unknown_imports_as_traps(module.component())
        .unwrap();
    let mut store = Store::new(module.engine(), ());
    store.set_epoch_deadline((LONG.as_millis() / TICK.as_millis()) as u64);
    let instance = linker.instantiate(&mut store, module.component()).unwrap();
    let describe = instance
        .get_export_index(&mut store, None, "example:embedder/describe")
        .and_then(|i| instance.get_export_index(&mut store, Some(&i), "describe"))
        .unwrap();
    let describe = instance
        .get_typed_func::<(), (String,)>(&mut store, &describe)
        .unwrap();
    let (description,) = describe.call(&mut store, ()).unwrap();
    // § end jf9zy7nf7
    assert_eq!(description, "sleeps, then greets");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_past_its_bounds_traps_and_its_worker_goes_on() {
    let module = Module::new(&component("bounds"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("bounds");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let start = |past: &'static str| {
            let input = serde_json::json!({ "past": past });
            sub.start(&workflow, input).id(past)
        };
        let code_time = start("code-time").await.unwrap();
        let step_time = start("step-time").await.unwrap();
        let step_memory = start("step-memory").await.unwrap();
        let code_memory = start("code-memory").await.unwrap();
        let within = start("within").await.unwrap();
        // § byxbyrk1c.b@sm0va
        let error = blocked(&sub, &workflow, code_time.chain()).await;
        assert_eq!(error.kind, "WasmTrap", "{}: {error:?}", ns.name);
        // § end byxbyrk1c.b
        // § byxbyrk1c.c@d44gx
        let cause = step_time.result().timeout(LONG).await.unwrap();
        assert_eq!(cause, "StepTimeout", "{}", ns.name);
        // § end byxbyrk1c.c
        // § byxbyrk1c.a@jsjgh
        let cause = step_memory.result().timeout(LONG).await.unwrap();
        assert_eq!(cause, "WasmTrap", "{}", ns.name);
        let error = blocked(&sub, &workflow, code_memory.chain()).await;
        assert_eq!(error.kind, "WasmTrap", "{}: {error:?}", ns.name);
        // § end byxbyrk1c.a
        // § jy9pjegjy.b@1jpdg
        // A step tried thrice if it failed retryably.
        let kinds = kinds(&step_memory).await;
        assert!(
            !kinds.contains(&"step_retry".to_owned()),
            "{}: {kinds:?}",
            ns.name
        );
        // § end jy9pjegjy.b
        let grown = within.result().timeout(LONG).await.unwrap();
        assert_eq!(grown, (1 << 20).to_string(), "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[test]
fn bounds_with_no_memory_or_under_a_tick_are_refused() {
    let wasm = component("receive_with_timeout");
    // § byxbyrk1c.d@4ymwm
    for limits in [
        Limits {
            memory: 0,
            ..BOUNDS
        },
        Limits {
            code_time: TICK / 2,
            ..BOUNDS
        },
    ] {
        let refused = Module::new(&wasm, limits, |_| Ok(()));
        assert!(matches!(refused, Err(Error::ConfigError(_))), "{limits:?}");
    }
    // § end byxbyrk1c.d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_awaits_its_calls_through_any_combinator() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § gtezqzzdn@gv1p6
        for (edge, slept) in [("unordered", "2"), ("join-all", "40")] {
            let input = serde_json::json!({ "edge": edge });
            let run = sub.start(&workflow, input).id(edge).await.unwrap();
            let ended = run.result().timeout(LONG).await.unwrap();
            assert_eq!(ended, slept, "{}: {edge}", ns.name);
        }
        // § end gtezqzzdn
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_duration_past_the_longest_blocks_its_run_and_the_worker_goes_on() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § b60bm4szk@gjzg1
        for edge in ["huge-sleep", "huge-receive", "max-sleep"] {
            let input = serde_json::json!({ "edge": edge });
            let run = sub.start(&workflow, input).id(edge).await.unwrap();
            let error = blocked(&sub, &workflow, run.chain()).await;
            assert_eq!(
                error.kind, "InvalidArgument",
                "{}: {edge} {error:?}",
                ns.name
            );
        }
        // § end b60bm4szk
        let input = serde_json::json!({ "edge": "unordered" });
        let after = sub.start(&workflow, input).id("after").await.unwrap();
        assert_eq!(
            after.result().timeout(LONG).await.unwrap(),
            "2",
            "{}",
            ns.name
        );
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_returning_a_verb_error_ends_as_the_host_code_would() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let native = substantial::Workflow::without_input("native", |c| async move {
            c.receive::<String>("never")
                .timeout(Duration::from_millis(10))
                .await
        });
        let worker = script(&sub, (workflow.clone(), native.clone()))
            .start()
            .unwrap();
        let input = serde_json::json!({ "edge": "timeout-returned" });
        let wasm_run = sub.start(&workflow, input).id("wasm").await.unwrap();
        let native_run = sub.start(&native, ()).id("native").await.unwrap();
        // § q2x1ma914@pbzce
        let ended = |r: substantial::Result<_>| match r {
            Err(Error::RunFailed { error, .. }) => *error,
            other => panic!("{}: ended {other:?}", ns.name),
        };
        let from_wasm = ended(wasm_run.result().timeout(LONG).await.map(|_| ()));
        let from_native = ended(native_run.result().timeout(LONG).await.map(|_| ()));
        assert_eq!(from_wasm, from_native, "{}", ns.name);
        assert_eq!(from_wasm.kind, "WaitTimeout", "{}", ns.name);
        // § end q2x1ma914
        worker.shutdown().await.unwrap();
    }
}

/// The Wasm workflows an embedder loaded, by the name its runs give: a digest in production.
type Shelf = Arc<Mutex<std::collections::HashMap<String, Module>>>;

/// The embedder's one workflow, which runs the Wasm workflow its input names with the input it holds.
fn by_name(shelf: Shelf) -> substantial::Workflow<serde_json::Value> {
    substantial::Workflow::new(
        "by-name",
        move |c: substantial::Context, input: serde_json::Value| {
            let shelf = shelf.clone();
            async move {
                let name = input["module"].as_str().unwrap_or_default().to_owned();
                let module = shelf.lock().unwrap().get(&name).cloned();
                let module = module
                    .ok_or_else(|| Error::InvalidArgument(format!("no Wasm workflow {name}")))?;
                module.run(c, input["input"].clone()).await
            }
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_of_the_embedder_runs_the_wasm_workflows_its_runs_name() {
    let edges = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let shelf = Shelf::default();
        let workflow = by_name(shelf.clone());
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § c53nbqaa7@jh2zr
        // Loaded once the worker serves: what a Wasm workflow published later is.
        let bumps = Arc::new(AtomicU32::new(0));
        let greeter = sleep_then_step(bumps.clone());
        (shelf.lock().unwrap()).insert("sleep_then_step".into(), greeter);
        (shelf.lock().unwrap()).insert("edges".into(), edges.clone());
        let input = serde_json::json!({"module": "sleep_then_step", "input": {"name": "wasm"}});
        let greeted = sub.start(&workflow, input).id("greeted").await.unwrap();
        let input = serde_json::json!({"module": "edges", "input": {"edge": "unordered"}});
        let edged = sub.start(&workflow, input).id("edged").await.unwrap();
        let greeting = greeted.result().timeout(LONG).await.unwrap();
        assert_eq!(greeting, "hello wasm, greeting 1", "{}", ns.name);
        assert_eq!(
            edged.result().timeout(LONG).await.unwrap(),
            "2",
            "{}",
            ns.name
        );
        assert_eq!(bumps.load(Ordering::SeqCst), 1, "{}", ns.name);
        // § end c53nbqaa7
        let input = serde_json::json!({"module": "none", "input": {}});
        let unknown = sub.start(&workflow, input).id("unknown").await.unwrap();
        let error = blocked(&sub, &workflow, unknown.chain()).await;
        assert_eq!(error.kind, "InvalidArgument", "{}: {error:?}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_run_ends_on_another_worker_that_loads_its_module_anew() {
    for ns in namespaces() {
        let suspended = Arc::new(AtomicU32::new(0));
        let seen = suspended.clone();
        let sub = internal::create(ns.store)
            .observe(move |e| {
                if matches!(e, Observed::Replay(r) if r.end == "suspended") {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await
            .unwrap();
        let (first, second) = (Shelf::default(), Shelf::default());
        let bumps_first = Arc::new(AtomicU32::new(0));
        let bumps_second = Arc::new(AtomicU32::new(0));
        let greeter = sleep_then_step(bumps_first.clone());
        (first.lock().unwrap()).insert("sleep_then_step".into(), greeter);
        let worker = script(&sub, by_name(first.clone())).start().unwrap();
        let input = serde_json::json!({"module": "sleep_then_step", "input": {"name": "wasm"}});
        let run = sub
            .start(&by_name(first), input)
            .id("handed")
            .await
            .unwrap();
        eventually("the run suspended on its sleep", async || {
            suspended.load(Ordering::SeqCst) > 0
        })
        .await;
        worker.shutdown().await.unwrap();
        // § c53nbqaa7.a@e7hhr
        // Another process: its own module, loaded anew from the same bytes.
        let greeter = sleep_then_step(bumps_second.clone());
        (second.lock().unwrap()).insert("sleep_then_step".into(), greeter);
        let worker = script(&sub, by_name(second)).start().unwrap();
        let greeting = run.result().timeout(LONG).await.unwrap();
        assert_eq!(greeting, "hello wasm, greeting 1", "{}", ns.name);
        assert_eq!(bumps_first.load(Ordering::SeqCst), 0, "{}", ns.name);
        assert_eq!(bumps_second.load(Ordering::SeqCst), 1, "{}", ns.name);
        // § end c53nbqaa7.a
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_embedders_async_function_runs_in_a_step_that_knows_its_key() {
    for ns in namespaces() {
        let keys: Arc<tokio::sync::Mutex<Vec<String>>> = Arc::default();
        let held = keys.clone();
        let imports = move |linker: &mut Linker<StepState>| {
            linker
                .instance("example:embedder/counter")?
                .func_wrap_async("bump", move |_, (): ()| {
                    let held = held.clone();
                    Box::new(async move {
                        let mut keys = held.lock().await;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        keys.push(substantial::step_key());
                        Ok((u32::try_from(keys.len()).unwrap(),))
                    })
                })
        };
        let module = Module::new(&component("sleep_then_step"), BOUNDS, imports).unwrap();
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("sleep_then_step");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({"name": "wasm"});
        let run = sub.start(&workflow, input).id("keyed").await.unwrap();
        let greeting = run.result().timeout(LONG).await.unwrap();
        assert_eq!(greeting, "hello wasm, greeting 1", "{}", ns.name);
        assert_eq!(*keys.lock().await, ["keyed/greet/0"], "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_wasm_workflow_ends_cancelled_with_its_reason() {
    let module = Module::new(&component("receive_with_timeout"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("receive_with_timeout");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({"timeout_ms": 60_000});
        let run = sub.start(&workflow, input).id("cancelled").await.unwrap();
        run.cancel("stop").await.unwrap();
        match run.result().timeout(LONG).await {
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                error,
                ..
            }) => assert_eq!(error.message, "stop", "{}", ns.name),
            other => panic!("{}: {other:?}", ns.name),
        }
        worker.shutdown().await.unwrap();
    }
}

// What a verb costs through a Wasm workflow beside the host's own code is a measure, not a check:
// `cargo bench -p substantial --features wasm-runner --bench wasm_verb` (benches/wasm_verb.rs).

/// The kinds of `run`'s history, in order.
async fn kinds<O>(run: &substantial::Run<O>) -> Vec<String> {
    (run.history().await.unwrap().into_iter())
        .map(|e| e.kind)
        .collect()
}

/// The failure `run` ended with, which fails the test unless it failed.
async fn failed<O: std::fmt::Debug + serde::de::DeserializeOwned + Send + 'static>(
    run: &substantial::Run<O>,
) -> (substantial::Failure, Option<substantial::Failure>) {
    match run.result().timeout(LONG).await {
        Err(Error::RunFailed { error, cause, .. }) => (*error, cause.map(|c| *c)),
        other => panic!("{}: ended {other:?}", run.chain()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_time_of_the_code_counts_anew_from_each_verb_it_calls() {
    // Each segment far within the code's time, the same work in one segment past it. The bound
    // counts wall time, which a loaded worker stretches: the runner's property, on the memory store.
    let limits = Limits {
        code_time: Duration::from_millis(100),
        ..BOUNDS
    };
    let module = Module::new(&component("bounds"), limits, |_| Ok(())).unwrap();
    let ns = (namespaces().into_iter())
        .find(|ns| ns.name == "memory")
        .unwrap();
    let sub = internal::create(ns.store).await.unwrap();
    let workflow = module.workflow("bounds");
    let worker = script(&sub, workflow.clone()).start().unwrap();
    // § byxbyrk1c.b@86bzd
    let segments = serde_json::json!({"past": "segments", "spins": 50_000, "segments": 600});
    let segments = sub.start(&workflow, segments).id("segments").await.unwrap();
    let whole = serde_json::json!({"past": "segments", "spins": 30_000_000, "segments": 1});
    let whole = sub.start(&workflow, whole).id("whole").await.unwrap();
    let ended = segments.result().timeout(LONG).await.unwrap();
    assert_eq!(ended, "600 segments");
    assert_eq!(
        blocked(&sub, &workflow, whole.chain()).await.kind,
        "WasmTrap"
    );
    // § end byxbyrk1c.b
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_runs_its_branches_at_once_in_the_order_it_calls() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({ "edge": "parallel" });
        let run = sub.start(&workflow, input).id("parallel").await.unwrap();
        assert_eq!(
            run.result().timeout(LONG).await.unwrap(),
            "both",
            "{}",
            ns.name
        );
        let history = run.history().await.unwrap();
        // § gtezqzzdn@frfdr
        // Both timers taken before either fired: the branches ran at once.
        let kinds: Vec<&str> = history.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["timer", "timer", "fired", "fired", "completed"],
            "{}",
            ns.name
        );
        // § end gtezqzzdn
        // § gtezqzzdn.a@8gz89
        // The longer sleep, called first, is the first timer.
        let timers: Vec<_> = (history.iter())
            .filter(|e| e.kind == "timer")
            .map(|e| (e.data["occurrence"].clone(), e.data["until"].to_string()))
            .collect();
        assert!(timers[0].1 > timers[1].1, "{}: {timers:?}", ns.name);
        // § end gtezqzzdn.a
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_that_nothing_would_end_traps() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § gtezqzzdn.c@q3cej
        for edge in ["wait-none", "wait-taken"] {
            let input = serde_json::json!({ "edge": edge });
            let run = sub.start(&workflow, input).id(edge).await.unwrap();
            let error = blocked(&sub, &workflow, run.chain()).await;
            assert_eq!(error.kind, "WasmTrap", "{}: {edge}", ns.name);
        }
        // § end gtezqzzdn.c
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replay_and_every_attempt_runs_in_an_instance_of_its_own() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({ "edge": "fresh" });
        let run = sub.start(&workflow, input).id("fresh").await.unwrap();
        // § s3vk5gvsj@x28x4
        // Each count starts from zero: no replay or attempt saw another's memory.
        assert_eq!(
            run.result().timeout(LONG).await.unwrap(),
            "1,1,1",
            "{}",
            ns.name
        );
        // § end s3vk5gvsj
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_code_calling_what_a_step_calls_and_a_step_calling_a_verb_trap() {
    let edges = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    let greeter = sleep_then_step(Arc::default());
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let (edges, greeter) = (edges.workflow("edges"), greeter.workflow("sleep_then_step"));
        let worker = script(&sub, (edges.clone(), greeter.clone()))
            .start()
            .unwrap();
        // § mry4d64zv.a@jc3pz
        let input = serde_json::json!({ "edge": "code-step-key" });
        let keyed = sub.start(&edges, input).id("code-step-key").await.unwrap();
        let error = blocked(&sub, &edges, keyed.chain()).await;
        assert_eq!(error.kind, "WasmTrap", "{}", ns.name);
        let input = serde_json::json!({ "name": "bump-in-code" });
        let bumped = sub.start(&greeter, input).id("bump-in-code").await.unwrap();
        let error = blocked(&sub, &greeter, bumped.chain()).await;
        assert_eq!(error.kind, "WasmTrap", "{}", ns.name);
        let input = serde_json::json!({ "edge": "step-verb" });
        let verb = sub.start(&edges, input).id("step-verb").await.unwrap();
        assert_eq!(
            verb.result().timeout(LONG).await.unwrap(),
            "WasmTrap",
            "{}",
            ns.name
        );
        // § end mry4d64zv.a
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_that_export_does_not_list_fails_not_retryable() {
    let module = Module::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({ "edge": "unlisted" });
        let run = sub.start(&workflow, input).id("unlisted").await.unwrap();
        // § ztt9a58wy.d@272db
        assert_eq!(
            run.result().timeout(LONG).await.unwrap(),
            "Error false",
            "{}",
            ns.name
        );
        // § end ztt9a58wy.d
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wasm_workflow_blocks_on_what_json_cannot_hold_and_fails_as_its_steps_say() {
    let module = Module::new(&component("failures"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("failures");
        let native = substantial::Workflow::without_input("native", |_c| async move {
            Err::<(), _>(Error::Fail(substantial::Failure {
                kind: "Mine".into(),
                message: "mine".into(),
                retryable: false,
                details: Some(serde_json::json!({ "why": "mine" })),
            }))
        });
        let worker = script(&sub, (workflow.clone(), native.clone()))
            .start()
            .unwrap();
        let native_run = sub.start(&native, ()).id("native").await.unwrap();
        let start = |failure: &'static str| {
            let input = serde_json::json!({ "failure": failure });
            sub.start(&workflow, input).id(failure)
        };
        // § ea0fw6qrw.b@k1376
        for (failure, kind) in [
            ("nan-result", "Json"),
            ("nan-message", "InvalidArgument"),
            ("huge-published", "InvalidArgument"),
            ("nan-arguments", "InvalidArgument"),
        ] {
            let run = start(failure).await.unwrap();
            let error = blocked(&sub, &workflow, run.chain()).await;
            assert_eq!(error.kind, kind, "{}: {failure}", ns.name);
        }
        // § end ea0fw6qrw.b
        // § ztt9a58wy.c@dsjtt
        for (failure, cause, retried) in [
            ("nan-value", ("Json", false), false),
            ("step-error", ("Error", true), true),
            ("step-permanent", ("Error", false), false),
        ] {
            let run = start(failure).await.unwrap();
            let (error, of) = failed(&run).await;
            assert_eq!(error.kind, "StepFailed", "{}: {failure}", ns.name);
            let of = of.unwrap_or_else(|| panic!("{}: {failure} has no cause", ns.name));
            assert_eq!(
                (of.kind.as_str(), of.retryable),
                cause,
                "{}: {failure}",
                ns.name
            );
            let kinds = kinds(&run).await;
            let was_retried = kinds.contains(&"step_retry".to_owned());
            assert_eq!(was_retried, retried, "{}: {failure} {kinds:?}", ns.name);
        }
        // § end ztt9a58wy.c
        // § q2x1ma914@1wbq7
        let run = start("own-failure").await.unwrap();
        let native = native_run.result().timeout(LONG).await;
        let Err(Error::RunFailed { error: native, .. }) = native else {
            panic!("{}: the native failure ended {native:?}", ns.name);
        };
        let (error, _) = failed(&run).await;
        assert_eq!(error, *native, "{}", ns.name);
        assert_eq!(error.kind, "Mine", "{}", ns.name);
        // § end q2x1ma914
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_attempt_stops_at_once_and_a_host_error_traps_its_attempt() {
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let bumps = Arc::new(AtomicU32::new(0));
        let spinning = sleep_then_step(bumps.clone()).workflow("sleep_then_step");
        let failing = Module::new(
            &component("sleep_then_step"),
            BOUNDS,
            |linker: &mut Linker<StepState>| {
                linker.instance("example:embedder/counter")?.func_wrap(
                    "bump",
                    |_, (): ()| -> wasmtime::Result<(u32,)> {
                        Err(wasmtime::format_err!("the counter is down"))
                    },
                )
            },
        )
        .unwrap()
        .workflow("failing");
        let worker = script(&sub, (spinning.clone(), failing.clone()))
            .start()
            .unwrap();
        // § ztt9a58wy.b@3jc10
        let input = serde_json::json!({ "name": "spin" });
        let spun = sub.start(&spinning, input).id("spin").await.unwrap();
        assert_eq!(
            spun.result().timeout(LONG).await.unwrap(),
            "StepTimeout",
            "{}",
            ns.name
        );
        let after = bumps.load(Ordering::SeqCst);
        assert!(after > 0, "{}: the step never bumped", ns.name);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(bumps.load(Ordering::SeqCst), after, "{}", ns.name);
        // § end ztt9a58wy.b
        // § jy9pjegjy.c@qw3g0
        let input = serde_json::json!({ "name": "wasm" });
        let down = sub.start(&failing, input).id("down").await.unwrap();
        let (error, cause) = failed(&down).await;
        assert_eq!(error.kind, "StepFailed", "{}", ns.name);
        let cause = cause.unwrap_or_else(|| panic!("{}: no cause", ns.name));
        assert_eq!(
            (cause.kind.as_str(), cause.retryable),
            ("WasmTrap", false),
            "{}",
            ns.name
        );
        assert!(
            cause.message.contains("the counter is down"),
            "{}: {cause:?}",
            ns.name
        );
        // § end jy9pjegjy.c
        worker.shutdown().await.unwrap();
    }
}

/// The component of the text `wat`.
fn wat(wat: &str) -> Vec<u8> {
    ::wat::parse_str(wat).expect("the text is a component")
}

#[test]
fn a_module_with_threads_does_not_compile() {
    // § ad2tnbnvg@ancsc
    let shared = wat("(component (core module (memory 1 1 shared)))");
    let Err(Error::ConfigError(message)) = Module::new(&shared, BOUNDS, |_| Ok(())) else {
        panic!("a module with a shared memory loaded");
    };
    assert!(message.contains("does not compile"), "{message}");
    // § end ad2tnbnvg
}

#[test]
fn a_module_of_another_minor_version_of_the_world_is_refused() {
    let importing = |version: &str| {
        wat(&format!(
            r#"(component (import "substantial:wasm/context@{version}"
                (instance (export "chain" (func (result string))))))"#
        ))
    };
    // § j67tqxstm.a@kn513
    let Err(Error::ConfigError(message)) = Module::new(&importing("0.2.0"), BOUNDS, |_| Ok(()))
    else {
        panic!("a module of the world 0.2.0 loaded");
    };
    assert!(message.contains("imports what neither"), "{message}");
    // A patch version links: what refuses this one is its missing export.
    let Err(Error::ConfigError(message)) = Module::new(&importing("0.1.1"), BOUNDS, |_| Ok(()))
    else {
        panic!("a module with no export loaded");
    };
    assert!(message.contains("exports no"), "{message}");
    // § end j67tqxstm.a
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_module_whose_workflow_has_other_types_loads_then_blocks_its_runs() {
    let other = wat(r#"(component
            (core module $m (func (export "f")))
            (core instance $i (instantiate $m))
            (func $f (canon lift (core func $i "f")))
            (instance $w (export "run" (func $f)) (export "step" (func $f)))
            (export "substantial:wasm/workflow@0.1.0" (instance $w)))"#);
    // § jf9zy7nf7.a@5rngk
    let module = Module::new(&other, BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = module.workflow("other");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let run = sub
            .start(&workflow, serde_json::json!({}))
            .id("other")
            .await
            .unwrap();
        let error = blocked(&sub, &workflow, run.chain()).await;
        assert_eq!(error.kind, "ConfigError", "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
    // § end jf9zy7nf7.a
}
