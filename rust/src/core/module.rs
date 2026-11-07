use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use sha2::{Digest, Sha256};
use wasmtime::{Engine, Instance as WasmInstance, Linker, Memory, Module, Store, TypedFunc};

use crate::error::{Error, Result};

// The copy the build script compiled into `COMPILED`, from `wasm/substantial.wasm`.
const WASM: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/substantial.wasm"));
const COMPILED: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/substantial.cwasm"));
const SUMS: &str = include_str!("../../wasm/SHA256SUMS");
const WASM_NAME: &str = "substantial.wasm";

/// The compiled core module, shared by every instance of the process.
#[derive(Clone)]
pub struct CoreModule {
    engine: Engine,
    module: Module,
}

impl CoreModule {
    /// The module shipped in `wasm/`, verified against `wasm/SHA256SUMS` and compiled once.
    pub fn shipped() -> Result<Self> {
        static SHIPPED: OnceLock<std::result::Result<CoreModule, String>> = OnceLock::new();
        SHIPPED
            .get_or_init(|| Self::precompiled().map_err(|e| e.to_string()))
            .clone()
            .map_err(Error::Module)
    }

    /// The shipped module as the build script compiled it; compiled again when wasmtime finds
    /// that this engine or host cannot run that code, which it checks before loading any.
    fn precompiled() -> Result<Self> {
        verify(WASM, SUMS)?;
        let engine = Engine::default();
        // SAFETY: `COMPILED` comes from this build's script, compiled by this wasmtime from
        // `WASM`, which was just verified; it never comes from outside the binary.
        let module = match unsafe { Module::deserialize(&engine, COMPILED) } {
            Ok(module) => module,
            Err(e) => {
                tracing::warn!(error = %e, "the precompiled core does not fit, compiling it");
                Module::new(&engine, WASM).map_err(|e| Error::Module(e.to_string()))?
            }
        };
        Self::checked(engine, module)
    }

    /// Compiles `wasm` once its sha256 matches the `substantial.wasm` line of `sums`.
    #[cfg(test)]
    fn from_bytes(wasm: &[u8], sums: &str) -> Result<Self> {
        verify(wasm, sums)?;
        let engine = Engine::default();
        let module = Module::new(&engine, wasm).map_err(|e| Error::Module(e.to_string()))?;
        Self::checked(engine, module)
    }

    fn checked(engine: Engine, module: Module) -> Result<Self> {
        if module.imports().len() != 0 {
            return Err(Error::Module("the core must import nothing".into()));
        }
        Ok(Self { engine, module })
    }

    /// A fresh instance, with a memory and machines of its own.
    pub fn instantiate(&self) -> Result<Core> {
        let mut store = Store::new(&self.engine, ());
        let instance = Linker::new(&self.engine)
            .instantiate(&mut store, &self.module)
            .map_err(|e| Error::Module(e.to_string()))?;
        let abi = Abi::new(&mut store, &instance)?;
        Ok(Core(Arc::new(Mutex::new(Inner {
            store,
            abi,
            trapped: None,
        }))))
    }
}

fn verify(wasm: &[u8], sums: &str) -> Result<()> {
    let expected = sums
        .lines()
        .filter_map(|line| line.split_once(char::is_whitespace))
        .find(|(_, name)| name.trim_start_matches([' ', '*']) == WASM_NAME)
        .map(|(sum, _)| sum.to_ascii_lowercase())
        .ok_or_else(|| Error::Module(format!("no {WASM_NAME} line in SHA256SUMS")))?;
    let actual = hex::encode(Sha256::digest(wasm));
    if actual != expected {
        return Err(Error::Module(format!(
            "{WASM_NAME} sha256 is {actual}, SHA256SUMS expects {expected}"
        )));
    }
    Ok(())
}

struct Abi {
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    free: TypedFunc<(u32, u32), ()>,
    new: TypedFunc<(u32, u32, u32), u32>,
    call: TypedFunc<(u32, u32, u32), u64>,
    drop: TypedFunc<u32, ()>,
}

impl Abi {
    fn new(store: &mut Store<()>, instance: &WasmInstance) -> Result<Self> {
        let missing = |name: &str| Error::Module(format!("missing export {name}"));
        let func = |store: &mut Store<()>, name: &str| {
            instance
                .get_func(&mut *store, name)
                .ok_or_else(|| missing(name))
        };
        let typed = |e: wasmtime::Error| Error::Module(e.to_string());
        Ok(Self {
            memory: instance
                .get_memory(&mut *store, "memory")
                .ok_or_else(|| missing("memory"))?,
            alloc: func(store, "sub_alloc")?.typed(&*store).map_err(typed)?,
            free: func(store, "sub_free")?.typed(&*store).map_err(typed)?,
            new: func(store, "sub_new")?.typed(&*store).map_err(typed)?,
            call: func(store, "sub_call")?.typed(&*store).map_err(typed)?,
            drop: func(store, "sub_drop")?.typed(&*store).map_err(typed)?,
        })
    }
}

struct Inner {
    store: Store<()>,
    abi: Abi,
    trapped: Option<String>,
}

/// One instance of the core and the machines in it.
///
/// A wasmtime store is not `Sync`, so the instance sits behind a mutex: each boundary call is
/// short, synchronous and never awaits, so the lock is never held across an `.await`. A worker
/// owns one instance, shared by its agent machine and every run machine of its replays.
#[derive(Clone)]
pub struct Core(Arc<Mutex<Inner>>);

#[derive(Clone, Copy, Debug)]
#[repr(u32)]
pub(crate) enum Kind {
    Run = 1,
    Agent = 2,
    Plan = 3,
}

impl Core {
    /// An instance of the shipped module.
    pub fn new() -> Result<Self> {
        CoreModule::shipped()?.instantiate()
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        let inner = self
            .0
            .lock()
            .map_err(|_| Error::Trap("instance lock poisoned".into()))?;
        match &inner.trapped {
            Some(trap) => Err(Error::Trap(trap.clone())),
            None => Ok(inner),
        }
    }

    pub(crate) fn new_machine(&self, kind: Kind, input: &[u8]) -> Result<u32> {
        self.new_raw(kind as u32, input)
    }

    fn new_raw(&self, kind: u32, input: &[u8]) -> Result<u32> {
        let mut inner = self.lock()?;
        let (ptr, len) = inner.write(input)?;
        let new = inner.abi.new.clone();
        inner.guard(|store| new.call(store, (kind, ptr, len)))
    }

    pub(crate) fn call(&self, machine: u32, request: &[u8]) -> Result<Vec<u8>> {
        let mut inner = self.lock()?;
        let (ptr, len) = inner.write(request)?;
        let call = inner.abi.call.clone();
        let packed = inner.guard(|store| call.call(store, (machine, ptr, len)))?;
        inner.take((packed >> 32) as u32, packed as u32)
    }

    pub(crate) fn drop_machine(&self, machine: u32) {
        if let Ok(mut inner) = self.lock() {
            let drop = inner.abi.drop.clone();
            let _ = inner.guard(|store| drop.call(store, machine));
        }
    }
}

impl Inner {
    fn trap(&mut self, trap: String) -> Error {
        self.trapped = Some(trap.clone());
        Error::Trap(trap)
    }

    fn guard<T>(&mut self, f: impl FnOnce(&mut Store<()>) -> wasmtime::Result<T>) -> Result<T> {
        f(&mut self.store).map_err(|e| self.trap(format!("{e:?}")))
    }

    /// Copies a request into the module; the module frees it when it takes it.
    fn write(&mut self, bytes: &[u8]) -> Result<(u32, u32)> {
        let len =
            u32::try_from(bytes.len()).map_err(|_| Error::Protocol("request too large".into()))?;
        let alloc = self.abi.alloc.clone();
        let ptr = self.guard(|store| alloc.call(store, len))?;
        let memory = self.abi.memory;
        if let Err(e) = memory.write(&mut self.store, ptr as usize, bytes) {
            return Err(self.trap(format!("request out of bounds: {e}")));
        }
        Ok((ptr, len))
    }

    /// Copies a response out of the module and frees it.
    fn take(&mut self, ptr: u32, len: u32) -> Result<Vec<u8>> {
        let mut out = vec![0; len as usize];
        let memory = self.abi.memory;
        if let Err(e) = memory.read(&self.store, ptr as usize, &mut out) {
            return Err(self.trap(format!("response out of bounds: {e}")));
        }
        let free = self.abi.free.clone();
        self.guard(|store| free.call(store, (ptr, len)))?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::proto::boundary::v1::{self as b, ErrorKind, agent_request, agent_response};

    fn agent_error(core: &Core, machine: u32, request: &[u8]) -> b::CoreError {
        let bytes = core.call(machine, request).unwrap();
        match b::AgentResponse::decode(bytes.as_slice()).unwrap().response {
            Some(agent_response::Response::Error(e)) => e,
            other => panic!("expected an error, got {other:?}"),
        }
    }

    fn next() -> Vec<u8> {
        let request = agent_request::Request::Next(crate::proto::layout::v1::Time { ms: 0 });
        b::AgentRequest {
            request: Some(request),
        }
        .encode_to_vec()
    }

    #[test]
    fn the_precompiled_core_loads_without_compiling() {
        // SAFETY: as in `precompiled`.
        let loaded = unsafe { Module::deserialize(&Engine::default(), COMPILED) };
        assert!(loaded.is_ok(), "{:?}", loaded.err());
    }

    #[test]
    fn the_shipped_module_matches_its_checksum() {
        CoreModule::shipped().unwrap();
        let sums = format!("{} {WASM_NAME}\n", "0".repeat(64));
        assert!(
            matches!(CoreModule::from_bytes(WASM, &sums), Err(Error::Module(m)) if m.contains("sha256"))
        );
        assert!(matches!(
            CoreModule::from_bytes(WASM, "abc other.wasm\n"),
            Err(Error::Module(_))
        ));
        let mut tampered = WASM.to_vec();
        tampered[100] ^= 1;
        assert!(matches!(
            CoreModule::from_bytes(&tampered, SUMS),
            Err(Error::Module(_))
        ));
    }

    #[test]
    fn invalid_machines_answer_every_call_with_their_error() {
        let core = Core::new().unwrap();
        for (kind, input) in [
            (9, vec![]),
            (Kind::Agent as u32, vec![0xff, 0xff]),
            (Kind::Agent as u32, vec![]),
        ] {
            let machine = core.new_raw(kind, &input).unwrap();
            assert_ne!(machine, 0, "sub_new never returns 0");
            let first = agent_error(&core, machine, &next());
            assert_eq!(agent_error(&core, machine, &next()), first);
            core.drop_machine(machine);
        }
    }

    #[test]
    fn undecodable_requests_and_unknown_machines_are_errors() {
        let core = Core::new().unwrap();
        let config = b::AgentConfig {
            partitions: 1,
            worker: "w".into(),
            workflows: vec![b::Served {
                name: "w".into(),
                versions: vec![],
                unversioned: Some(true),
                retention: None,
            }],
            concurrency: 1,
            claim_ttl_ms: 30_000,
            claim_skew_ms: 5_000,
            max_idle_ms: 1_000,
            due_page: 100,
            due_pages: 10,
            snapshot_every: 64,
            op_retries: 5,
            wait_seconds: 20,
            seed: vec![0; 32],
            tombstone_ms: 7 * 86_400_000,
            retention_min_ms: 3_600_000,
            recheck_ms: 86_400_000,
            settings: Some(b::RunSettings {
                continue_as_new_after_segments: 1000,
                retention: Some(b::Retention {
                    keep: Some(b::retention::Keep::Forever(b::Empty {})),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let machine = core
            .new_machine(Kind::Agent, &config.encode_to_vec())
            .unwrap();
        let e = agent_error(&core, machine, &[0xff, 0xff, 0xff]);
        assert_eq!(e.kind(), ErrorKind::Invalid, "{e:?}");
        let answer =
            b::AgentResponse::decode(core.call(machine, &next()).unwrap().as_slice()).unwrap();
        assert!(
            matches!(answer.response, Some(agent_response::Response::Command(_))),
            "left as it was"
        );
        let e = agent_error(&core, 123_456, &next());
        assert_eq!(e.kind(), ErrorKind::Invalid, "{e:?}");
        core.drop_machine(machine);
        let e = agent_error(&core, machine, &next());
        assert_eq!(
            e.kind(),
            ErrorKind::Invalid,
            "a dropped machine no longer exists: {e:?}"
        );
    }
}
