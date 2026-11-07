use std::path::PathBuf;

fn main() {
    // Read when the script runs, not when it is compiled: a shared target dir reuses one script
    // binary across checkouts. `proto` and `wasm` are symlinks to the repository's single
    // copies: cargo package follows them, so the published crate carries the files themselves.
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let proto = root.join("proto");
    let files = [
        "substantial/layout/v1/layout.proto",
        "substantial/boundary/v1/boundary.proto",
        "substantial/boundary/v1/run.proto",
        "substantial/boundary/v1/agent.proto",
        "substantial/boundary/v1/plan.proto",
    ];
    let fds = protox::compile(files, [&proto]).expect("compile protos");
    prost_build::Config::new()
        .compile_fds(fds)
        .expect("generate prost code");
    println!("cargo:rerun-if-changed={}", proto.display());
    for file in ["wasm/substantial.wasm", "wasm/SHA256SUMS"] {
        println!("cargo:rerun-if-changed={}", root.join(file).display());
    }
    precompile(&root);
}

/// Compiles the core for the target once, here, so that a process loads it instead of running
/// Cranelift at start: unoptimized, as in a dependent's debug build, that takes seconds. The
/// wasm is copied next to it, so that the bytes the host verifies are the ones compiled.
fn precompile(root: &std::path::Path) {
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let wasm = std::fs::read(root.join("wasm/substantial.wasm")).expect("read the core");
    let target = std::env::var("TARGET").expect("TARGET");
    let mut config = wasmtime::Config::new();
    config
        .target(&target)
        .expect("a target wasmtime compiles for");
    let engine = wasmtime::Engine::new(&config).expect("a wasmtime engine");
    let compiled = engine
        .precompile_module(&wasm)
        .expect("precompile the core");
    std::fs::write(out.join("substantial.wasm"), &wasm).expect("copy the core");
    std::fs::write(out.join("substantial.cwasm"), compiled).expect("write the compiled core");
}
