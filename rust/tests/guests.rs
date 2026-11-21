//! Guests: workflows compiled to WebAssembly components, served by a worker of this host. The
//! guests are the examples of `substantial-guest`, built for wasm32-unknown-unknown here.

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use common::{LONG, namespaces, script};
use substantial::__internal as internal;
use substantial::guest::wasmtime::Store;
use substantial::guest::wasmtime::component::Linker;
use substantial::guest::{Guest, StepState};
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

/// The guest `sleep_then_step`, its embedder's counter linked to `bumps`.
fn sleep_then_step(bumps: Arc<AtomicU32>) -> Guest {
    Guest::new(
        &component("sleep_then_step"),
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
    let guest = Guest::new(&component("receive_with_timeout"), |_| Ok(())).unwrap();
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
    // § dt9hm67zy@0v4ah
    let refused = Guest::new(&component("sleep_then_step"), |_| Ok(()));
    let Err(Error::ConfigError(message)) = refused else {
        panic!("a guest whose counter no one gives is loaded");
    };
    // § end dt9hm67zy
    assert!(message.contains("example:embedder/counter"), "{message}");
}

#[test]
fn an_embedder_calls_an_export_of_its_own() {
    let guest = sleep_then_step(Arc::default());
    // § symbbqcxc@8x94v
    let mut linker = Linker::new(guest.engine());
    linker
        .define_unknown_imports_as_traps(guest.component())
        .unwrap();
    let mut store = Store::new(guest.engine(), ());
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
