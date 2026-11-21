//! The instance of a step attempt, which runs one step with the interfaces the embedder adds.

use std::sync::Arc;

use wasmtime::component::{ComponentType, Lift, Linker, ResourceTable, ResourceType};
use wasmtime::{Engine, Store};

use super::{ATTEMPT, CONTEXT, Guest, PANICS, guest_value, trapped};
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
    // § h85pj12ap.a@hv3jb
    for verb in VERBS {
        context.func_new(verb, move |_, _, _, _| {
            wasmtime::bail!("a step called {verb}, a verb of the workflow code")
        })?;
    }
    // § end h85pj12ap.a
    Ok(linker)
}

impl Guest {
    /// The body of the step `name` called with `args`: each attempt in an instance of its own.
    pub(super) fn attempts(&self, name: String, args: String) -> StepBody {
        let guest = self.clone();
        Arc::new(move || {
            let (guest, name, args) = (guest.clone(), name.clone(), args.clone());
            Box::pin(async move { guest.attempt(name, args).await })
        })
    }

    async fn attempt(&self, name: String, args: String) -> Result<Vec<u8>, Failure> {
        // § sfd6dmd7t.b@vqk2w
        // The store and its instance are this future's: an abandoned attempt drops them.
        let state = StepState {
            table: ResourceTable::new(),
            panic: None,
            memory: self.0.memory.clone(),
        };
        let mut store = Store::new(&self.0.engine, state);
        // § end sfd6dmd7t.b
        store.limiter(|s| &mut s.memory);
        // § p9d3qccma.c@bmpve
        let every_tick = 1;
        store.set_epoch_deadline(every_tick);
        store.epoch_deadline_async_yield_and_update(every_tick);
        // § end p9d3qccma.c
        // § fqyh5tkhx@5qmj7
        let instance = self.0.steps.instantiate_async(&mut store).await;
        // § end fqyh5tkhx
        // § a3qbj1ttt.b@f3xbx
        let trap = |store: &mut Store<StepState>, e: wasmtime::Error| {
            trapped(store.data_mut().panic.take(), &e)
        };
        // § end a3qbj1ttt.b
        let instance = instance.map_err(|e| trap(&mut store, e))?;
        let step = instance
            .get_typed_func::<(String, String), (Result<String, StepError>,)>(
                &mut store,
                &self.0.step,
            )
            .map_err(|e| trap(&mut store, e))?;
        // § sfd6dmd7t@xdgb8
        match step.call_async(&mut store, (name, args)).await {
            // § sfd6dmd7t.c@1nrjg
            Ok((Ok(value),)) => guest_value(&value)
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
            // § end sfd6dmd7t.c
            Err(e) => Err(trap(&mut store, e)),
        }
        // § end sfd6dmd7t
    }
}
