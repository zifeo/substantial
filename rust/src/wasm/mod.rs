//! The Wasm runner: Wasm workflows, compiled to WebAssembly components of the world
//! `substantial:wasm/module` (`wit/workflow.wit`), loaded at run time and replayed against the core
//! as the host's own code.
//!
//! A Wasm workflow runs in two kinds of instance, each made anew: one per replay runs its workflow code,
//! with Substantial's verbs, and one per step attempt runs a step, with the interfaces the
//! embedder adds. The embedder's host functions are wasmtime's, so this module re-exports it.

mod code;
mod step;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub use wasmtime;
use wasmtime::component::{Component, ComponentExportIndex, InstancePre, Linker};
use wasmtime::{Config, Engine, ResourceLimiter, WasmFeatures};

pub use self::step::StepState;
use crate::error::{Error, Failure, Result};
use crate::workflow::{Context, Workflow};

const CONTEXT: &str = "substantial:wasm/context@0.1.0";
const ATTEMPT: &str = "substantial:wasm/attempt@0.1.0";
const PANICS: &str = "substantial:wasm/panics@0.1.0";
const WORKFLOW: &str = "substantial:wasm/workflow@0.1.0";

/// The period of the epoch clock that interrupts Wasm workflows, the grain of their time bound. A
/// store of the engine that runs a module's code needs an epoch deadline in these ticks.
pub const TICK: Duration = Duration::from_millis(10);

/// The bounds of a Wasm workflow, which its instances trap past.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// The memory an instance may grow to, in bytes.
    pub memory: usize,
    /// How long the workflow code may run without a wait, at least a `TICK`.
    pub code_time: Duration,
}

/// What an instance may still grow by, in bytes: its linear memories and its tables together,
/// since a component may hold several of each.
#[derive(Clone)]
pub(crate) struct Held {
    left: usize,
}

/// A table element holds a pointer.
const TABLE_ELEMENT: usize = std::mem::size_of::<usize>();

impl Held {
    fn take(&mut self, bytes: usize) -> wasmtime::Result<bool> {
        // § byxbyrk1c.a@8p5g6
        match self.left.checked_sub(bytes) {
            Some(left) => {
                self.left = left;
                Ok(true)
            }
            None => wasmtime::bail!("the instance grows past its memory bound by {bytes} bytes"),
        }
    }
}

// wasmtime asks before it checks the declared maximum: a growth past it is refused here, so that
// it spends nothing of the bound.
impl ResourceLimiter for Held {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if maximum.is_some_and(|maximum| desired > maximum) {
            return Ok(false);
        }
        self.take(desired.saturating_sub(current))
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if maximum.is_some_and(|maximum| desired > maximum) {
            return Ok(false);
        }
        self.take(
            desired
                .saturating_sub(current)
                .saturating_mul(TABLE_ELEMENT),
        )
    }
}

/// A Wasm workflow loaded: its component compiled, its imports checked, and linked for its replays and
/// its step attempts. Cheap to clone.
#[derive(Clone)]
pub struct Module(Arc<Loaded>);

struct Loaded {
    engine: Engine,
    component: Component,
    code: InstancePre<code::CodeState>,
    steps: InstancePre<StepState>,
    run: ComponentExportIndex,
    step: ComponentExportIndex,
    memory: Held,
    /// The code's time, in ticks.
    code_ticks: u64,
}

impl Module {
    /// Compiles the component `wasm` and links it: Substantial's interfaces, and those `imports`
    /// adds to the linker of its steps, the only ones its steps may call. `ConfigError` for a
    /// component that does not compile, exports no `workflow`, or imports anything else, and for
    /// `limits` with no memory or a code time under a `TICK`.
    pub fn new(
        wasm: &[u8],
        limits: Limits,
        imports: impl FnOnce(&mut Linker<StepState>) -> wasmtime::Result<()>,
    ) -> Result<Self> {
        // § byxbyrk1c@8nrna
        // § byxbyrk1c.d@x4tq1
        if limits.memory == 0 || limits.code_time < TICK {
            return Err(Error::ConfigError(format!(
                "a Wasm workflow's bounds hold some memory and a code time of a tick at least: {limits:?}"
            )));
        }
        // A deadline of n ticks passes after (n - 1) to n of them, the current one partly gone: one
        // tick more makes the code's time at least `code_time`.
        let code_ticks = u64::try_from(limits.code_time.as_nanos().div_ceil(TICK.as_nanos()))
            .ok()
            .and_then(|ticks| ticks.checked_add(1))
            .ok_or_else(|| Error::ConfigError(format!("a code time too long: {limits:?}")))?;
        // § end byxbyrk1c.d
        let memory = Held {
            left: limits.memory,
        };
        // § end byxbyrk1c
        let engine = engine()?;
        let component = Component::new(&engine, wasm)
            .map_err(|e| Error::ConfigError(format!("the module does not compile: {e:#}")))?;
        let mut steps = step::linker(&engine).map_err(linking)?;
        imports(&mut steps).map_err(|e| {
            Error::ConfigError(format!("the embedder's interfaces do not link: {e:#}"))
        })?;
        // § de970qkh4@hhw58
        let steps = steps.instantiate_pre(&component).map_err(|e| {
            Error::ConfigError(format!(
                "the module imports what neither Substantial nor the embedder gives: {e:#}"
            ))
        })?;
        // § end de970qkh4
        // § jf9zy7nf7@he4v3
        let export = |instance: Option<&ComponentExportIndex>, name: &str| {
            component
                .get_export_index(instance, name)
                .ok_or_else(|| Error::ConfigError(format!("the module exports no {name}")))
        };
        let workflow = export(None, WORKFLOW)?;
        let (run, step) = (
            export(Some(&workflow), "run")?,
            export(Some(&workflow), "step")?,
        );
        // § end jf9zy7nf7
        let mut code = code::linker(&engine).map_err(linking)?;
        // § mry4d64zv.a@s2fav
        // What is left are the embedder's interfaces, the step linker having refused any other.
        code.define_unknown_imports_as_traps(&component)
            .map_err(linking)?;
        // § end mry4d64zv.a
        let code = code.instantiate_pre(&component).map_err(linking)?;
        Ok(Self(Arc::new(Loaded {
            engine,
            component,
            code,
            steps,
            run,
            step,
            memory,
            code_ticks,
        })))
    }

    /// The module's workflow, served under `name`: give it to a worker as any workflow, and set its
    /// version, step timeout, retention or budget on it.
    pub fn workflow(&self, name: &str) -> Workflow<serde_json::Value> {
        let module = self.clone();
        Workflow::new(name, move |c: Context, input: serde_json::Value| {
            let module = module.clone();
            async move { module.run(c, input).await }
        })
    }

    /// The engine the module is compiled for, for an embedder calling exports of its own. It
    /// interrupts by epochs: a store of it traps at once unless given a deadline in `TICK`s.
    pub fn engine(&self) -> &Engine {
        &self.0.engine
    }

    /// The compiled component, for an embedder calling exports of its own.
    pub fn component(&self) -> &Component {
        &self.0.component
    }
}

/// The engine of every Wasm workflow of the process.
fn engine() -> Result<Engine> {
    static ENGINE: OnceLock<std::result::Result<Engine, String>> = OnceLock::new();
    ENGINE
        .get_or_init(|| {
            // § ad2tnbnvg@6tm2c
            // Off by name: an embedder's own wasmtime with default features compiles threads in.
            let mut config = Config::new();
            config
                .cranelift_nan_canonicalization(true)
                .relaxed_simd_deterministic(true)
                .wasm_features(
                    WasmFeatures::THREADS | WasmFeatures::SHARED_EVERYTHING_THREADS,
                    false,
                );
            // § end ad2tnbnvg
            config.epoch_interruption(true);
            let engine = Engine::new(&config).map_err(|e| format!("{e:#}"))?;
            let ticking = engine.clone();
            std::thread::Builder::new()
                .name("substantial-wasm-epochs".into())
                .spawn(move || {
                    loop {
                        std::thread::sleep(TICK);
                        ticking.increment_epoch();
                    }
                })
                .map_err(|e| format!("the epoch clock does not start: {e}"))?;
            Ok(engine)
        })
        .clone()
        .map_err(Error::Module)
}

fn linking(e: wasmtime::Error) -> Error {
    Error::ConfigError(format!("the module does not link: {e:#}"))
}

/// The failure of a module's trap, with its panic's message when it panicked.
fn trapped(panic: Option<String>, e: &wasmtime::Error) -> Failure {
    // § jy9pjegjy@abgqe
    Failure {
        kind: "WasmTrap".into(),
        message: match panic {
            Some(message) => format!("{message} ({e})"),
            None => format!("{e:#}"),
        },
        retryable: false,
        details: None,
    }
    // § end jy9pjegjy
}

/// A module's JSON, checked as a value of the host's own code.
fn module_value(json: &str) -> std::result::Result<serde_json::Value, serde_json::Error> {
    // § ea0fw6qrw@326q5
    let value: serde_json::Value = serde_json::from_str(json)?;
    crate::workflow::to_json(&value)?;
    Ok(value)
    // § end ea0fw6qrw
}
