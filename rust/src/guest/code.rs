//! The instance of a replay, which runs a guest's workflow code with Substantial's verbs.

use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use chrono::SecondsFormat;
use futures::future::BoxFuture;
use wasmtime::component::{ComponentType, Lift, Linker, Lower, Resource, ResourceTable};
use wasmtime::{AsContextMut, Engine, Store, StoreContextMut};

use super::{ATTEMPT, CONTEXT, Guest, PANICS, guest_value, trapped};
use crate::error::{Error, Failure, Result};
use crate::workflow::{Context, Retry, failure_of};

/// A failure as the world gives it, its details JSON text.
#[derive(ComponentType, Lift, Lower, Clone, Debug, PartialEq)]
#[component(record)]
pub(super) struct WitFailure {
    kind: String,
    message: String,
    retryable: bool,
    details: Option<String>,
}

impl From<&Failure> for WitFailure {
    fn from(f: &Failure) -> Self {
        Self {
            kind: f.kind.clone(),
            message: f.message.clone(),
            retryable: f.retryable,
            details: f.details.as_ref().map(serde_json::Value::to_string),
        }
    }
}

#[derive(ComponentType, Lower)]
#[component(record)]
struct Raised {
    failure: WitFailure,
    cause: Option<WitFailure>,
}

#[derive(ComponentType, Lift)]
#[component(record)]
struct WitRetry {
    #[component(name = "max-attempts")]
    max_attempts: u32,
    #[component(name = "initial-ms")]
    initial_ms: u64,
    #[component(name = "max-ms")]
    max_ms: u64,
    multiplier: f64,
}

#[derive(ComponentType, Lift)]
#[component(record)]
struct StepOptions {
    retry: Option<WitRetry>,
    #[component(name = "timeout-ms")]
    timeout_ms: Option<u64>,
}

// Built by wasmtime alone, as it lifts a guest's level.
#[allow(dead_code)]
#[derive(ComponentType, Lift, Clone, Copy)]
#[component(enum)]
#[repr(u8)]
enum Level {
    #[component(name = "trace")]
    Trace,
    #[component(name = "debug")]
    Debug,
    #[component(name = "info")]
    Info,
    #[component(name = "warn")]
    Warn,
    #[component(name = "error")]
    Error,
}

#[derive(ComponentType, Lift)]
#[component(variant)]
pub(super) enum End {
    #[component(name = "failed")]
    Failed(WitFailure),
    #[component(name = "cancelled")]
    Cancelled(String),
}

/// A call of the code: its future until it ends, then its outcome until the code takes it.
pub(super) struct GuestCall {
    future: Option<BoxFuture<'static, Result<String>>>,
    ended: Option<Result<String>>,
}

/// What the instance of a replay holds.
pub(super) struct CodeState {
    ctx: Context,
    guest: Guest,
    table: ResourceTable,
    /// The errors given to the code, for its run to end with one it returns.
    raised: Vec<Error>,
    panic: Option<String>,
    memory: super::Held,
    /// The code's time, in ticks.
    code_ticks: u64,
}

/// The failure the code sees of an error: a `Cancelled`'s reason as its message.
fn visible(e: &Error) -> Failure {
    match e {
        Error::Cancelled(reason) => Failure {
            kind: "Cancelled".into(),
            message: reason.clone(),
            retryable: false,
            details: None,
        },
        other => failure_of(other),
    }
}

impl CodeState {
    fn raise(&mut self, e: Error) -> Raised {
        let cause = match &e {
            Error::StepFailed { cause, .. } => Some(WitFailure::from(&**cause)),
            _ => None,
        };
        let raised = Raised {
            failure: WitFailure::from(&visible(&e)),
            cause,
        };
        self.raised.push(e);
        raised
    }

    /// The error a run ends with for a failure its code returned: the one given to the code
    /// with that failure, else the failure itself.
    fn ended_with(&mut self, f: WitFailure) -> Error {
        // § kn3j2hh9y@211mv
        let found = (self.raised.iter()).rposition(|e| WitFailure::from(&visible(e)) == f);
        if let Some(i) = found {
            return self.raised.remove(i);
        }
        let details = match f.details.as_deref().map(serde_json::from_str).transpose() {
            Ok(details) => details,
            Err(e) => return Error::Json(e),
        };
        Error::Failed(Failure {
            kind: f.kind,
            message: f.message,
            retryable: f.retryable,
            details,
        })
        // § end kn3j2hh9y
    }
}

impl Guest {
    /// One replay of the guest's workflow code.
    pub(super) async fn run(
        &self,
        ctx: Context,
        input: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let state = CodeState {
            ctx,
            guest: self.clone(),
            table: ResourceTable::new(),
            raised: Vec::new(),
            panic: None,
            memory: self.0.memory.clone(),
            code_ticks: self.0.code_ticks,
        };
        let mut store = Store::new(&self.0.engine, state);
        store.limiter(|s| &mut s.memory);
        store.epoch_deadline_trap();
        new_time(&mut store.as_context_mut());
        // § fqyh5tkhx@zx341
        let instance = self.0.code.instantiate_async(&mut store).await;
        // § end fqyh5tkhx
        let trap = |store: &mut Store<CodeState>, e: wasmtime::Error| {
            // § a3qbj1ttt.a@5sg3v
            Error::Failed(trapped(store.data_mut().panic.take(), &e))
        };
        let instance = instance.map_err(|e| trap(&mut store, e))?;
        let run = instance
            .get_typed_func::<(String,), (std::result::Result<String, End>,)>(
                &mut store,
                &self.0.run,
            )
            .map_err(|e| Error::ConfigError(format!("the guest's run: {e:#}")))?;
        let input = serde_json::to_string(&input)?;
        // § one g1k3x3gg5@55cds
        match run.call_async(&mut store, (input,)).await {
            Ok((Ok(output),)) => Ok(guest_value(&output)?),
            Ok((Err(End::Failed(f)),)) => Err(store.data_mut().ended_with(f)),
            Ok((Err(End::Cancelled(reason)),)) => Err(Error::Cancelled(reason)),
            Err(e) => Err(trap(&mut store, e)),
        }
    }
}

/// Makes a call of the code, polled once so that the core takes it now, in the code's order.
async fn start(
    store: &mut StoreContextMut<'_, CodeState>,
    future: impl Future<Output = Result<String>> + Send + 'static,
) -> wasmtime::Result<(Resource<GuestCall>,)> {
    let mut future: BoxFuture<'static, Result<String>> = Box::pin(future);
    // § em5h7z7cg.a@0nq28
    let first = std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
    // § end em5h7z7cg.a
    let pending = match first {
        Poll::Ready(ended) => GuestCall {
            future: None,
            ended: Some(ended),
        },
        Poll::Pending => GuestCall {
            future: Some(future),
            ended: None,
        },
    };
    Ok((store.data_mut().table.push(pending)?,))
}

/// Gives the code its time anew, from now.
fn new_time(store: &mut StoreContextMut<'_, CodeState>) {
    // § p9d3qccma.b@nmerr
    let ticks = store.data().code_ticks;
    store.set_epoch_deadline(ticks);
}

fn as_text(value: &impl serde::Serialize) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

/// The linker of the replays: Substantial's verbs, and the attempt's interface trapping.
pub(super) fn linker(engine: &Engine) -> wasmtime::Result<Linker<CodeState>> {
    let mut linker = Linker::<CodeState>::new(engine);
    let mut i = linker.instance(CONTEXT)?;
    i.resource(
        "call",
        wasmtime::component::ResourceType::host::<GuestCall>(),
        |mut store, rep| {
            store
                .data_mut()
                .table
                .delete(Resource::<GuestCall>::new_own(rep))?;
            Ok(())
        },
    )?;
    i.func_wrap(
        "[method]call.take",
        |mut store, (call,): (Resource<GuestCall>,)| {
            let state = store.data_mut();
            let taken = match state.table.get_mut(&call)?.ended.take() {
                None => None,
                Some(Ok(value)) => Some(Ok(value)),
                Some(Err(e)) => Some(Err(state.raise(e))),
            };
            Ok((taken,))
        },
    )?;
    i.func_wrap_async(
        "wait",
        |mut store, (calls,): (Vec<Resource<GuestCall>>,)| {
            Box::new(async move {
                if calls.is_empty() {
                    wasmtime::bail!("a wait on no call, which nothing would end");
                }
                // § em5h7z7cg@13nma
                let ended = std::future::poll_fn(|cx| {
                    let table = &mut store.data_mut().table;
                    let mut ended = Vec::new();
                    for (i, call) in calls.iter().enumerate() {
                        let pending = match table.get_mut(call) {
                            Ok(pending) => pending,
                            Err(e) => return Poll::Ready(Err(e)),
                        };
                        if let Some(future) = pending.future.as_mut()
                            && let Poll::Ready(outcome) = future.as_mut().poll(cx)
                        {
                            pending.future = None;
                            pending.ended = Some(outcome);
                        }
                        // § one em5h7z7cg.b@h1vv4
                        if pending.ended.is_some() {
                            ended.push(
                                u32::try_from(i).expect("a wait names fewer than 2^32 calls"),
                            );
                        }
                    }
                    match ended.is_empty() {
                        true => Poll::Pending,
                        false => Poll::Ready(Ok(ended)),
                    }
                })
                .await?;
                // § end em5h7z7cg
                new_time(&mut store);
                Ok((ended,))
            })
        },
    )?;
    i.func_wrap_async(
        "step",
        |mut store, (name, args, options): (String, String, StepOptions)| {
            let (ctx, guest) = (store.data().ctx.clone(), store.data().guest.clone());
            Box::new(async move {
                // The arguments cross to the step's instance: checked here, as every value is.
                // § c0mcah841@e03xp
                if let Err(e) = guest_value(&args) {
                    return start(&mut store, async move { Err(Error::Json(e)) }).await;
                }
                // § end c0mcah841
                let body = guest.attempts(name.clone(), args);
                let mut step = ctx.step_of::<serde_json::Value>(&name, body);
                if let Some(r) = options.retry {
                    step = step.retry(Retry {
                        max_attempts: r.max_attempts,
                        initial: Duration::from_millis(r.initial_ms),
                        max: Duration::from_millis(r.max_ms),
                        multiplier: r.multiplier,
                    });
                }
                if let Some(ms) = options.timeout_ms {
                    step = step.timeout(Duration::from_millis(ms));
                }
                start(&mut store, async move { as_text(&step.await?) }).await
            })
        },
    )?;
    i.func_wrap_async("sleep", |mut store, (ms,): (u64,)| {
        let ctx = store.data().ctx.clone();
        Box::new(async move {
            start(&mut store, async move {
                ctx.sleep(Duration::from_millis(ms)).await?;
                as_text(&())
            })
            .await
        })
    })?;
    i.func_wrap_async("sleep-until", |mut store, (ms,): (i64,)| {
        let ctx = store.data().ctx.clone();
        Box::new(async move {
            start(&mut store, async move {
                let until = chrono::DateTime::from_timestamp_millis(ms).ok_or_else(|| {
                    Error::InvalidArgument(format!("{ms} ms since the epoch is not a time"))
                })?;
                ctx.sleep_until(until).await?;
                as_text(&())
            })
            .await
        })
    })?;
    i.func_wrap_async(
        "receive",
        |mut store, (name, timeout): (String, Option<u64>)| {
            let ctx = store.data().ctx.clone();
            Box::new(async move {
                start(&mut store, async move {
                    let receive = ctx.receive::<serde_json::Value>(&name);
                    let received = match timeout {
                        Some(ms) => receive.timeout(Duration::from_millis(ms)).await?,
                        None => receive.await?,
                    };
                    as_text(&received)
                })
                .await
            })
        },
    )?;
    i.func_wrap_async(
        "send",
        |mut store, (chain, name, value, id): (String, String, String, Option<String>)| {
            let ctx = store.data().ctx.clone();
            Box::new(async move {
                start(&mut store, async move {
                    let mut send = ctx.send(&chain, &name, guest_value(&value)?);
                    if let Some(id) = &id {
                        send = send.id(id);
                    }
                    send.await?;
                    as_text(&())
                })
                .await
            })
        },
    )?;
    i.func_wrap_async("now", |mut store, (): ()| {
        let ctx = store.data().ctx.clone();
        Box::new(async move {
            start(&mut store, async move {
                let now = ctx.now().await?;
                as_text(&now.to_rfc3339_opts(SecondsFormat::Millis, true))
            })
            .await
        })
    })?;
    i.func_wrap_async("random", |mut store, (): ()| {
        let ctx = store.data().ctx.clone();
        Box::new(
            async move { start(&mut store, async move { as_text(&ctx.random().await?) }).await },
        )
    })?;
    i.func_wrap_async("uuid", |mut store, (): ()| {
        let ctx = store.data().ctx.clone();
        Box::new(async move { start(&mut store, async move { as_text(&ctx.uuid().await?) }).await })
    })?;
    i.func_wrap("publish", |mut store, (key, value): (String, String)| {
        let state = store.data_mut();
        let published = guest_value(&value)
            .map_err(Error::from)
            .and_then(|value| state.ctx.publish(&key, value));
        Ok((published.map_err(|e| state.raise(e)),))
    })?;
    i.func_wrap("chain", |store, (): ()| {
        Ok((store.data().ctx.chain().to_owned(),))
    })?;
    i.func_wrap("log", |store, (level, message): (Level, String)| {
        let log = &store.data().ctx.log;
        match level {
            Level::Trace => log.trace(message),
            Level::Debug => log.debug(message),
            Level::Info => log.info(message),
            Level::Warn => log.warn(message),
            Level::Error => log.error(message),
        }
        Ok(())
    })?;
    linker
        .instance(PANICS)?
        .func_wrap("panicked", |mut store, (message,): (String,)| {
            store.data_mut().panic = Some(message);
            Ok(())
        })?;
    // § h85pj12ap.a@r8f5h
    linker.instance(ATTEMPT)?.func_wrap(
        "step-key",
        |_, (): ()| -> wasmtime::Result<(String,)> {
            wasmtime::bail!("the workflow code called step-key, which a step calls")
        },
    )?;
    // § end h85pj12ap.a
    Ok(linker)
}
