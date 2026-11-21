//! The instance of a step attempt, which runs one step with the interfaces the embedder adds.

use std::sync::Arc;

use wasmtime::component::{ComponentType, Lift, Linker, ResourceTable, ResourceType};
use wasmtime::{Engine, Store};

use super::{ATTEMPT, CONTEXT, Module, PANICS, module_value, trapped};
use crate::error::Failure;
use crate::workflow::scheduler::StepBody;

/// What the instance of a step attempt holds: the table of the embedder's resources.
pub struct StepState {
    table: ResourceTable,
    panic: Option<String>,
    memory: super::Held,
}

impl StepState {
    pub fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

#[derive(ComponentType, Lift)]
#[component(record)]
struct StepError {
    message: String,
    permanent: bool,
}

/// The verbs of the workflow code, which trap in a step.
const VERBS: &[&str] = &[
    "[method]call.take",
    "wait",
    "step",
    "sleep",
    "sleep-until",
    "receive",
    "send",
    "now",
    "random",
    "uuid",
    "publish",
    "chain",
    "log",
];

/// The linker of the step attempts: the attempt's interface, and the workflow verbs trapping.
pub(super) fn linker(engine: &Engine) -> wasmtime::Result<Linker<StepState>> {
    let mut linker = Linker::<StepState>::new(engine);
    linker
        .instance(ATTEMPT)?
        .func_wrap("step-key", |_, (): ()| Ok((crate::workflow::step_key(),)))?;
    linker
        .instance(PANICS)?
        .func_wrap("panicked", |mut store, (message,): (String,)| {
            store.data_mut().panic = Some(message);
            Ok(())
        })?;
    let mut context = linker.instance(CONTEXT)?;
    // A call of the context cannot exist in a step: its resource is never made there.
    context.resource("call", ResourceType::host::<()>(), |_, _| Ok(()))?;
    // § mry4d64zv.a@dz04a
    for verb in VERBS {
        context.func_new(verb, move |_, _, _, _| {
            wasmtime::bail!("a step called {verb}, a verb of the workflow code")
        })?;
    }
    // § end mry4d64zv.a
    Ok(linker)
}

impl Module {
    /// The body of the step `name` called with `args`: each attempt in an instance of its own.
    pub(super) fn attempts(&self, name: String, args: String) -> StepBody {
        let module = self.clone();
        Arc::new(move || {
            let (module, name, args) = (module.clone(), name.clone(), args.clone());
            Box::pin(async move { module.attempt(name, args).await })
        })
    }

    async fn attempt(&self, name: String, args: String) -> Result<Vec<u8>, Failure> {
        // § ztt9a58wy.b@aww6a
        // The store and its instance are this future's: an abandoned attempt drops them.
        let state = StepState {
            table: ResourceTable::new(),
            panic: None,
            memory: self.0.memory.clone(),
        };
        let mut store = Store::new(&self.0.engine, state);
        // § end ztt9a58wy.b
        store.limiter(|s| &mut s.memory);
        // § jrg0gw7y7.c@hxv95
        let every_tick = 1;
        store.set_epoch_deadline(every_tick);
        store.epoch_deadline_async_yield_and_update(every_tick);
        // § end jrg0gw7y7.c
        // § s3vk5gvsj@x1ckk
        let instance = self.0.steps.instantiate_async(&mut store).await;
        // § end s3vk5gvsj
        // § cy53pm5cn.b@q6j4p
        let trap = |store: &mut Store<StepState>, e: wasmtime::Error| {
            trapped(store.data_mut().panic.take(), &e)
        };
        // § end cy53pm5cn.b
        let instance = instance.map_err(|e| trap(&mut store, e))?;
        let step = instance
            .get_typed_func::<(String, String), (Result<String, StepError>,)>(
                &mut store,
                &self.0.step,
            )
            .map_err(|e| trap(&mut store, e))?;
        // § ztt9a58wy@8bskw
        match step.call_async(&mut store, (name, args)).await {
            // § ztt9a58wy.c@9xsma
            Ok((Ok(value),)) => module_value(&value)
                .and_then(|value| crate::workflow::to_json(&value))
                .map_err(|e| Failure {
                    kind: "Json".into(),
                    message: e.to_string(),
                    retryable: false,
                    details: None,
                }),
            Ok((Err(StepError { message, permanent }),)) => Err(Failure {
                kind: "Error".into(),
                message,
                retryable: !permanent,
                details: None,
            }),
            // § end ztt9a58wy.c
            Err(e) => Err(trap(&mut store, e)),
        }
        // § end ztt9a58wy
    }
}
