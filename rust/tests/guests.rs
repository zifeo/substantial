//! Guests: workflows compiled to WebAssembly components, served by a worker of this host. The
//! guests are the examples of `substantial-guest`, built for wasm32-unknown-unknown here.

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::{LONG, eventually, namespaces, script};
use substantial::__internal as internal;
use substantial::guest::wasmtime::Store;
use substantial::guest::wasmtime::component::Linker;
use substantial::guest::{Guest, Limits, StepState, TICK};
use substantial::{Error, Observed, RunStatus};

/// The component of the example guest `example`, built once per test process.
fn component(example: &str) -> Vec<u8> {
    built(example, false)
}

/// The component of the example guest `example`, built in release when `release`, as an embedder
/// publishes a guest, else in debug.
fn built(example: &str, release: bool) -> Vec<u8> {
    static DEBUG: OnceLock<()> = OnceLock::new();
    static RELEASE: OnceLock<()> = OnceLock::new();
    let (once, profile) = if release {
        (&RELEASE, "release")
    } else {
        (&DEBUG, "debug")
    };
    let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("guests");
    once.get_or_init(|| {
        let mut build = Command::new(env!("CARGO"));
        build
            .args(["build", "--locked", "-p", "substantial-guest", "--examples"])
            .args(["--target", "wasm32-unknown-unknown", "--target-dir"])
            .arg(&target)
            .env("CARGO_INCREMENTAL", "0");
        if release {
            build.arg("--release");
        }
        let status = build.status().expect("cargo builds the example guests");
        assert!(status.success(), "the example guests do not build");
    });
    let module = target.join(format!(
        "wasm32-unknown-unknown/{profile}/examples/{example}.wasm"
    ));
    let module = std::fs::read(&module).expect("the example guest is built");
    wit_component::ComponentEncoder::default()
        .module(&module)
        .expect("the guest carries its world")
        .validate(true)
        .encode()
        .expect("the guest is made a component")
}

/// Bounds well above what the example guests need within them.
const BOUNDS: Limits = Limits {
    memory: 16 << 20,
    code_time: Duration::from_secs(1),
};

/// The guest `sleep_then_step`, its embedder's counter linked to `bumps`.
fn sleep_then_step(bumps: Arc<AtomicU32>) -> Guest {
    Guest::new(
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
async fn a_guest_sleeps_then_steps_replayed_from_its_history() {
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
        let input = serde_json::json!({"name": "guest"});
        let run = sub.start(&workflow, input).await.unwrap();
        let result = run.result().timeout(LONG).await.unwrap();
        assert_eq!(result, "hello guest, greeting 1", "{}", ns.name);
        // § fqyh5tkhx@hjanq
        // One replay suspended on the sleep, and the one after it ran the step once.
        let replays = replays.lock().unwrap().clone();
        assert!(replays.contains(&"suspended"), "{}: {replays:?}", ns.name);
        assert_eq!(bumps.load(Ordering::SeqCst), 1, "{}", ns.name);
        // § end fqyh5tkhx
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guest_receives_a_message_or_its_timeout_passes() {
    let guest = Guest::new(&component("receive_with_timeout"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("receive_with_timeout");
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
fn a_guest_importing_what_no_one_gives_is_refused() {
    // § dt9hm67zy@x4xxm
    let refused = Guest::new(&component("sleep_then_step"), BOUNDS, |_| Ok(()));
    let Err(Error::ConfigError(message)) = refused else {
        panic!("a guest whose counter no one gives is loaded");
    };
    // § end dt9hm67zy
    assert!(message.contains("example:embedder/counter"), "{message}");
}

#[test]
fn an_embedder_calls_an_export_of_its_own() {
    let guest = sleep_then_step(Arc::default());
    // § symbbqcxc@ct001
    let mut linker = Linker::new(guest.engine());
    linker
        .define_unknown_imports_as_traps(guest.component())
        .unwrap();
    let mut store = Store::new(guest.engine(), ());
    store.set_epoch_deadline((LONG.as_millis() / TICK.as_millis()) as u64);
    let instance = linker.instantiate(&mut store, guest.component()).unwrap();
    let describe = instance
        .get_export_index(&mut store, None, "example:embedder/describe")
        .and_then(|i| instance.get_export_index(&mut store, Some(&i), "describe"))
        .unwrap();
    let describe = instance
        .get_typed_func::<(), (String,)>(&mut store, &describe)
        .unwrap();
    let (description,) = describe.call(&mut store, ()).unwrap();
    // § end symbbqcxc
    assert_eq!(description, "sleeps, then greets");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guest_past_its_bounds_traps_and_its_worker_goes_on() {
    let guest = Guest::new(&component("bounds"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("bounds");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let start = |past: &'static str| {
            let input = serde_json::json!({ "past": past });
            sub.start(&workflow, input).id(past)
        };
        let code_time = start("code-time").await.unwrap();
        let step_time = start("step-time").await.unwrap();
        let step_memory = start("step-memory").await.unwrap();
        let within = start("within").await.unwrap();
        // § p9d3qccma.b@jt3ya
        let spun = code_time.result().timeout(LONG).await;
        let Err(Error::RunFailed { error, .. }) = spun else {
            panic!("{}: code past its time ended {spun:?}", ns.name);
        };
        assert_eq!(error.kind, "GuestTrap", "{}: {error:?}", ns.name);
        // § end p9d3qccma.b
        // § p9d3qccma.c@8t0xp
        let cause = step_time.result().timeout(LONG).await.unwrap();
        assert_eq!(cause, "StepTimeout", "{}", ns.name);
        // § end p9d3qccma.c
        // § p9d3qccma.a@5bre2
        let cause = step_memory.result().timeout(LONG).await.unwrap();
        assert_eq!(cause, "GuestTrap", "{}", ns.name);
        // § end p9d3qccma.a
        let grown = within.result().timeout(LONG).await.unwrap();
        assert_eq!(grown, (1 << 20).to_string(), "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[test]
fn bounds_with_no_memory_or_under_a_tick_are_refused() {
    let wasm = component("receive_with_timeout");
    // § p9d3qccma@vp5g2
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
        let refused = Guest::new(&wasm, limits, |_| Ok(()));
        assert!(matches!(refused, Err(Error::ConfigError(_))), "{limits:?}");
    }
    // § end p9d3qccma
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guest_awaits_its_calls_through_any_combinator() {
    let guest = Guest::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § em5h7z7cg@bg8bj
        for (edge, slept) in [("unordered", "2"), ("join-all", "40")] {
            let input = serde_json::json!({ "edge": edge });
            let run = sub.start(&workflow, input).id(edge).await.unwrap();
            let ended = run.result().timeout(LONG).await.unwrap();
            assert_eq!(ended, slept, "{}: {edge}", ns.name);
        }
        // § end em5h7z7cg
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guest_duration_past_the_longest_fails_its_run_and_the_worker_goes_on() {
    let guest = Guest::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("edges");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § b60bm4szk@dby4c
        for edge in ["huge-sleep", "huge-receive"] {
            let input = serde_json::json!({ "edge": edge });
            let run = sub.start(&workflow, input).id(edge).await.unwrap();
            let ended = run.result().timeout(LONG).await;
            let Err(Error::RunFailed { error, .. }) = ended else {
                panic!("{}: {edge} ended {ended:?}", ns.name);
            };
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
async fn a_guest_returning_a_verb_error_ends_as_the_host_code_would() {
    let guest = Guest::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("edges");
        let native = substantial::Workflow::without_input("native", |c| async move {
            c.receive::<String>("never")
                .timeout(Duration::from_millis(10))
                .await
        });
        let worker = script(&sub, (workflow.clone(), native.clone()))
            .start()
            .unwrap();
        let input = serde_json::json!({ "edge": "timeout-returned" });
        let guest_run = sub.start(&workflow, input).id("guest").await.unwrap();
        let native_run = sub.start(&native, ()).id("native").await.unwrap();
        // § kn3j2hh9y@rmagt
        let ended = |r: substantial::Result<_>| match r {
            Err(Error::RunFailed { error, .. }) => *error,
            other => panic!("{}: ended {other:?}", ns.name),
        };
        let from_guest = ended(guest_run.result().timeout(LONG).await.map(|_| ()));
        let from_native = ended(native_run.result().timeout(LONG).await.map(|_| ()));
        assert_eq!(from_guest, from_native, "{}", ns.name);
        assert_eq!(from_guest.kind, "WaitTimeout", "{}", ns.name);
        // § end kn3j2hh9y
        worker.shutdown().await.unwrap();
    }
}

/// The guests an embedder loaded, by the name its runs give: a digest in production.
type Shelf = Arc<Mutex<std::collections::HashMap<String, Guest>>>;

/// The embedder's one workflow, which runs the guest its input names with the input it holds.
fn by_name(shelf: Shelf) -> substantial::Workflow<serde_json::Value> {
    substantial::Workflow::new(
        "by-name",
        move |c: substantial::Context, input: serde_json::Value| {
            let shelf = shelf.clone();
            async move {
                let name = input["module"].as_str().unwrap_or_default().to_owned();
                let guest = shelf.lock().unwrap().get(&name).cloned();
                let guest =
                    guest.ok_or_else(|| Error::InvalidArgument(format!("no guest {name}")))?;
                guest.run(c, input["input"].clone()).await
            }
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_of_the_embedder_runs_the_guests_its_runs_name() {
    let edges = Guest::new(&component("edges"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let shelf = Shelf::default();
        let workflow = by_name(shelf.clone());
        let worker = script(&sub, workflow.clone()).start().unwrap();
        // § xvn8jfd26@cb68c
        // Loaded once the worker serves: what a guest published later is.
        let bumps = Arc::new(AtomicU32::new(0));
        let greeter = sleep_then_step(bumps.clone());
        (shelf.lock().unwrap()).insert("sleep_then_step".into(), greeter);
        (shelf.lock().unwrap()).insert("edges".into(), edges.clone());
        let input = serde_json::json!({"module": "sleep_then_step", "input": {"name": "guest"}});
        let greeted = sub.start(&workflow, input).id("greeted").await.unwrap();
        let input = serde_json::json!({"module": "edges", "input": {"edge": "unordered"}});
        let edged = sub.start(&workflow, input).id("edged").await.unwrap();
        let greeting = greeted.result().timeout(LONG).await.unwrap();
        assert_eq!(greeting, "hello guest, greeting 1", "{}", ns.name);
        assert_eq!(
            edged.result().timeout(LONG).await.unwrap(),
            "2",
            "{}",
            ns.name
        );
        assert_eq!(bumps.load(Ordering::SeqCst), 1, "{}", ns.name);
        // § end xvn8jfd26
        let input = serde_json::json!({"module": "none", "input": {}});
        let unknown = sub.start(&workflow, input).id("unknown").await.unwrap();
        let Err(Error::RunFailed { error, .. }) = unknown.result().timeout(LONG).await else {
            panic!("{}: a run of no guest ended otherwise", ns.name);
        };
        assert_eq!(error.kind, "InvalidArgument", "{}: {error:?}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guest_run_ends_on_another_worker_that_loads_its_guest_anew() {
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
        let input = serde_json::json!({"module": "sleep_then_step", "input": {"name": "guest"}});
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
        // Another process: its own guest, loaded anew from the same bytes.
        let greeter = sleep_then_step(bumps_second.clone());
        (second.lock().unwrap()).insert("sleep_then_step".into(), greeter);
        let worker = script(&sub, by_name(second)).start().unwrap();
        let greeting = run.result().timeout(LONG).await.unwrap();
        assert_eq!(greeting, "hello guest, greeting 1", "{}", ns.name);
        assert_eq!(bumps_first.load(Ordering::SeqCst), 0, "{}", ns.name);
        assert_eq!(bumps_second.load(Ordering::SeqCst), 1, "{}", ns.name);
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
        let guest = Guest::new(&component("sleep_then_step"), BOUNDS, imports).unwrap();
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("sleep_then_step");
        let worker = script(&sub, workflow.clone()).start().unwrap();
        let input = serde_json::json!({"name": "guest"});
        let run = sub.start(&workflow, input).id("keyed").await.unwrap();
        let greeting = run.result().timeout(LONG).await.unwrap();
        assert_eq!(greeting, "hello guest, greeting 1", "{}", ns.name);
        assert_eq!(*keys.lock().await, ["keyed/greet/0"], "{}", ns.name);
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_guest_ends_cancelled_with_its_reason() {
    let guest = Guest::new(&component("receive_with_timeout"), BOUNDS, |_| Ok(())).unwrap();
    for ns in namespaces() {
        let sub = internal::create(ns.store).await.unwrap();
        let workflow = guest.workflow("receive_with_timeout");
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

/// A measure, not a check: what a verb costs through a guest beside the host's own code. Each
/// cost is the difference between runs of `MANY` verbs and runs of none, so the worker's own
/// latency cancels out, on the memory store, so no store operation hides it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measure, run with --release --ignored --nocapture"]
async fn measure_a_verb_through_a_guest_and_natively() {
    const MANY: u32 = 50_000;
    const ROUNDS: usize = 7;
    let guest = Guest::new(&built("verbs", true), BOUNDS, |_| Ok(())).unwrap();
    let module = guest.workflow("verbs");
    let native = substantial::Workflow::new(
        "native",
        |c: substantial::Context, input: serde_json::Value| async move {
            let n = input["n"].as_u64().unwrap();
            for _ in 0..n {
                c.random().await?;
            }
            Ok(n)
        },
    );
    let ns = (namespaces().into_iter())
        .find(|ns| ns.name == "memory")
        .unwrap();
    let sub = internal::create(ns.store).await.unwrap();
    let worker = script(&sub, (module.clone(), native.clone()))
        .start()
        .unwrap();
    let median = |mut d: Vec<Duration>| {
        d.sort();
        d[d.len() / 2]
    };
    let (mut guest_runs, mut native_runs) = ([vec![], vec![]], [vec![], vec![]]);
    for round in 0..ROUNDS {
        for (i, n) in [0, MANY].into_iter().enumerate() {
            let input = serde_json::json!({ "n": n });
            let started = std::time::Instant::now();
            let run = (sub.start(&module, input.clone()))
                .id(&format!("guest-{round}-{n}"))
                .await
                .unwrap();
            run.result().timeout(LONG).await.unwrap();
            guest_runs[i].push(started.elapsed());
            let started = std::time::Instant::now();
            let run = (sub.start(&native, input))
                .id(&format!("native-{round}-{n}"))
                .await
                .unwrap();
            run.result().timeout(LONG).await.unwrap();
            native_runs[i].push(started.elapsed());
        }
    }
    worker.shutdown().await.unwrap();
    let [guest_none, guest_many] = guest_runs.map(median);
    let [native_none, native_many] = native_runs.map(median);
    let per_verb = |many: Duration, none: Duration| many.saturating_sub(none) / MANY;
    let (guest_verb, native_verb) = (
        per_verb(guest_many, guest_none),
        per_verb(native_many, native_none),
    );
    println!("medians of {ROUNDS} rounds, {MANY} verbs a run, memory store:");
    println!("  a run of no verb: guest {guest_none:?}, native {native_none:?}");
    println!("  a verb: guest {guest_verb:?}, native {native_verb:?}");
    println!(
        "  the guest's extra: {:?} a verb, {:?} a run",
        guest_verb.saturating_sub(native_verb),
        guest_none.saturating_sub(native_none)
    );
}
