//! A Wasm workflow at the edges its input names: calls awaited through combinators that poll only the
//! futures they were woken for, waits that nothing would end, durations longer than the longest,
//! memory that would outlive an instance, and calls of what the code or a step may not call.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use std::sync::atomic::{AtomicU32, Ordering};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::Deserialize;
use substantial as s;
use substantial::__bindings::substantial::wasm::context as wit;

#[derive(Deserialize)]
struct Input {
    edge: String,
}

/// Counts the replays an instance ran, which a fresh instance holds at zero.
static REPLAYS: AtomicU32 = AtomicU32::new(0);
/// Counts the attempts an instance ran.
static ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[s::step]
async fn count() -> Result<u32, std::io::Error> {
    Ok(ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1)
}

#[s::step]
async fn verb_in_step() -> Result<f64, std::io::Error> {
    let call = wit::random();
    wit::wait(&[&call]);
    Ok(0.0)
}

#[s::step]
async fn unlisted() -> Result<(), std::io::Error> {
    Ok(())
}

#[s::workflow]
async fn edges(c: s::Context, input: Input) -> s::Result<String> {
    let ms = std::time::Duration::from_millis;
    match input.edge.as_str() {
        "unordered" => {
            let mut sleeps: FuturesUnordered<_> =
                [10, 20].into_iter().map(|n| c.sleep(ms(n))).collect();
            let mut slept = 0;
            while let Some(done) = sleeps.next().await {
                done?;
                slept += 1;
            }
            Ok(slept.to_string())
        }
        // Past 30 futures, `join_all` polls only the woken ones.
        "join-all" => {
            let sleeps = (0..40).map(|n| c.sleep(ms(n % 4 * 10)));
            let slept = futures::future::join_all(sleeps).await;
            Ok(slept.into_iter().filter(Result::is_ok).count().to_string())
        }
        "parallel" => {
            let (long, short) = futures::future::join(c.sleep(ms(30)), c.sleep(ms(10))).await;
            long.and(short).map(|()| "both".to_owned())
        }
        "wait-none" => {
            wit::wait(&[]);
            Ok("woken".to_owned())
        }
        "wait-taken" => {
            let call = wit::sleep(1);
            wit::wait(&[&call]);
            let _ = call.take();
            wit::wait(&[&call]);
            Ok("woken".to_owned())
        }
        "fresh" => {
            let replays = REPLAYS.fetch_add(1, Ordering::SeqCst) + 1;
            c.sleep(ms(1)).await?;
            let (a, b) = (count().await?, count().await?);
            Ok(format!("{replays},{a},{b}"))
        }
        "code-step-key" => Ok(s::step_key()),
        "step-verb" => match verb_in_step().await {
            Err(s::Error::StepFailed { cause, .. }) => Ok(cause.kind),
            other => other.map(|v| v.to_string()),
        },
        "unlisted" => match unlisted().await {
            Err(s::Error::StepFailed { cause, .. }) => {
                Ok(format!("{} {}", cause.kind, cause.retryable))
            }
            other => other.map(|()| "ran".to_owned()),
        },
        "huge-sleep" => c.sleep(ms(u64::MAX)).await.map(|()| "slept".to_owned()),
        "max-sleep" => c
            .sleep(std::time::Duration::MAX)
            .await
            .map(|()| "slept".to_owned()),
        "huge-receive" => c.receive::<String>("never").timeout(ms(u64::MAX)).await,
        "timeout-returned" => c.receive::<String>("never").timeout(ms(10)).await,
        other => Ok(format!("no edge {other}")),
    }
}

s::export!(edges, steps = [count, verb_in_step]);
