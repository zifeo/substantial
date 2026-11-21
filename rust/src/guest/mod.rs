//! Guests: workflows compiled to WebAssembly components of the world `substantial:guest/guest`
//! (`wit/guest.wit`), loaded at run time and replayed against the core as the host's own code.
//!
//! A guest runs in two kinds of instance, each made anew: one per replay runs its workflow code,
//! with Substantial's verbs, and one per step attempt runs a step, with the interfaces the
//! embedder adds. The embedder's host functions are wasmtime's, so this module re-exports it.

mod code;
mod step;

use std::sync::{Arc, OnceLock};

pub use wasmtime;
use wasmtime::component::{Component, ComponentExportIndex, InstancePre, Linker};
use wasmtime::{Config, Engine};

pub use self::step::StepState;
use crate::error::{Error, Failure, Result};
use crate::workflow::{Context, Workflow};

const CONTEXT: &str = "substantial:guest/context@0.1.0";
const ATTEMPT: &str = "substantial:guest/attempt@0.1.0";
const PANICS: &str = "substantial:guest/panics@0.1.0";
const WORKFLOW: &str = "substantial:guest/workflow@0.1.0";

/// A guest loaded: its component compiled, its imports checked, and linked for its replays and
/// its step attempts. Cheap to clone.
#[derive(Clone)]
pub struct Guest(Arc<Loaded>);

struct Loaded {
    engine: Engine,
    component: Component,
    code: InstancePre<code::CodeState>,
    steps: InstancePre<StepState>,
    run: ComponentExportIndex,
    step: ComponentExportIndex,
}

impl Guest {
    /// Compiles the component `wasm` and links it: Substantial's interfaces, and those `imports`
    /// adds to the linker of its steps, the only ones its steps may call. `ConfigError` for a
    /// component that does not compile, exports no `workflow`, or imports anything else.
    pub fn new(
        wasm: &[u8],
        imports: impl FnOnce(&mut Linker<StepState>) -> wasmtime::Result<()>,
    ) -> Result<Self> {
        let engine = engine()?;
        let component = Component::new(&engine, wasm)
            .map_err(|e| Error::ConfigError(format!("the guest does not compile: {e:#}")))?;
        // § symbbqcxc@f2e3g
        let export = |instance: Option<&ComponentExportIndex>, name: &str| {
            component
                .get_export_index(instance, name)
                .ok_or_else(|| Error::ConfigError(format!("the guest exports no {name}")))
        };
        let workflow = export(None, WORKFLOW)?;
        let (run, step) = (
            export(Some(&workflow), "run")?,
            export(Some(&workflow), "step")?,
        );
        // § end symbbqcxc
        let mut steps = step::linker(&engine).map_err(linking)?;
        imports(&mut steps).map_err(|e| {
            Error::ConfigError(format!("the embedder's interfaces do not link: {e:#}"))
        })?;
        // § dt9hm67zy@90dzr
        let steps = steps.instantiate_pre(&component).map_err(|e| {
            Error::ConfigError(format!(
                "the guest imports what neither Substantial nor the embedder gives: {e:#}"
            ))
        })?;
        // § end dt9hm67zy
        let mut code = code::linker(&engine).map_err(linking)?;
        // § h85pj12ap.a@sw020
        // What is left are the embedder's interfaces, the step linker having refused any other.
        code.define_unknown_imports_as_traps(&component)
            .map_err(linking)?;
        // § end h85pj12ap.a
        let code = code.instantiate_pre(&component).map_err(linking)?;
        Ok(Self(Arc::new(Loaded {
            engine,
            component,
            code,
            steps,
            run,
            step,
        })))
    }

    /// The guest's workflow, served under `name`: give it to a worker as any workflow, and set its
    /// version, step timeout, retention or budget on it.
    pub fn workflow(&self, name: &str) -> Workflow<serde_json::Value> {
        let guest = self.clone();
        Workflow::new(name, move |c: Context, input: serde_json::Value| {
            let guest = guest.clone();
            async move { guest.run(c, input).await }
        })
    }

    /// The engine the guest is compiled for, for an embedder calling exports of its own.
    pub fn engine(&self) -> &Engine {
        &self.0.engine
    }

    /// The compiled component, for an embedder calling exports of its own.
    pub fn component(&self) -> &Component {
        &self.0.component
    }
}

/// The engine of every guest of the process.
fn engine() -> Result<Engine> {
    static ENGINE: OnceLock<std::result::Result<Engine, String>> = OnceLock::new();
    ENGINE
        .get_or_init(|| {
            // § jgv9ssc51@xd2mb
            // Threads are not compiled in: a guest with shared memory does not compile.
            let mut config = Config::new();
            config
                .cranelift_nan_canonicalization(true)
                .relaxed_simd_deterministic(true);
            // § end jgv9ssc51
            Engine::new(&config).map_err(|e| format!("{e:#}"))
        })
        .clone()
        .map_err(Error::Module)
}

fn linking(e: wasmtime::Error) -> Error {
    Error::ConfigError(format!("the guest does not link: {e:#}"))
}

/// The failure of a guest's trap, with its panic's message when it panicked.
fn trapped(panic: Option<String>, e: &wasmtime::Error) -> Failure {
    // § a3qbj1ttt@8npew
    Failure {
        kind: "GuestTrap".into(),
        message: match panic {
            Some(message) => format!("{message} ({e})"),
            None => format!("{e:#}"),
        },
        retryable: false,
        details: None,
    }
    // § end a3qbj1ttt
}

/// A guest's JSON, checked as a value of the host's own code.
fn guest_value(json: &str) -> std::result::Result<serde_json::Value, serde_json::Error> {
    // § c0mcah841@wjfsx
    let value: serde_json::Value = serde_json::from_str(json)?;
    crate::workflow::to_json(&value)?;
    Ok(value)
    // § end c0mcah841
}
