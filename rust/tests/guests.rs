//! Guests: workflows compiled to WebAssembly components, served by a worker of this host. The
//! guests are the examples of `substantial-guest`, built for wasm32-unknown-unknown here.

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::{LONG, namespaces, script};
use substantial::__internal as internal;
use substantial::guest::wasmtime::Store;
use substantial::guest::wasmtime::component::Linker;
use substantial::guest::{Guest, Limits, StepState, TICK};
use substantial::{Error, Observed};

/// The component of the example guest `example`, built once per test process.
fn component(example: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("guests");
    BUILT.get_or_init(|| {
        let status = Command::new(env!("CARGO"))
            .args(["build", "--locked", "-p", "substantial-guest", "--examples"])
            .args(["--target", "wasm32-unknown-unknown", "--target-dir"])
            .arg(&target)
            .env("CARGO_INCREMENTAL", "0")
            .status()
            .expect("cargo builds the example guests");
        assert!(status.success(), "the example guests do not build");
    });
    let module = target.join(format!(
        "wasm32-unknown-unknown/debug/examples/{example}.wasm"
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
        // § fqyh5tkhx@69yb3
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
    // § dt9hm67zy@xrfmp
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
    // § symbbqcxc@thk71
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
        // § p9d3qccma.b@jpa34
        let spun = code_time.result().timeout(LONG).await;
        let Err(Error::RunFailed { error, .. }) = spun else {
            panic!("{}: code past its time ended {spun:?}", ns.name);
        };
        assert_eq!(error.kind, "GuestTrap", "{}: {error:?}", ns.name);
        // § end p9d3qccma.b
        // § p9d3qccma.c@47hhc
        let cause = step_time.result().timeout(LONG).await.unwrap();
        assert_eq!(cause, "StepTimeout", "{}", ns.name);
        // § end p9d3qccma.c
        // § p9d3qccma.a@6wx15
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
    // § p9d3qccma@qnvg1
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
        // § em5h7z7cg@svsmq
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
        // § b60bm4szk@ycw9h
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
        // § kn3j2hh9y@jsmwt
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
