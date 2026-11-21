//! A Wasm workflow that runs past the bound its input names: its code's time, a step's time, or a step's
//! memory. Its steps' failures come back as their causes' kinds.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial as s;

#[derive(Deserialize)]
struct Input {
    past: String,
}

const ONCE: s::Retry = s::Retry {
    max_attempts: 1,
    initial: std::time::Duration::from_secs(1),
    max: std::time::Duration::from_secs(1),
    multiplier: 1.0,
};

/// Spins without a host call, which only an interruption ends.
fn spin() -> ! {
    loop {
        std::hint::black_box(());
    }
}

#[s::step(retry = ONCE, timeout = std::time::Duration::from_millis(300))]
async fn spin_step() -> Result<(), std::io::Error> {
    spin()
}

#[s::step(retry = ONCE)]
async fn grow(bytes: usize) -> Result<usize, std::io::Error> {
    Ok(std::hint::black_box(vec![1u8; bytes]).len())
}

#[s::workflow]
async fn bounds(_c: s::Context, input: Input) -> s::Result<String> {
    let ended = match input.past.as_str() {
        "code-time" => spin(),
        "step-time" => spin_step().await.map(|()| "spun".to_owned()),
        "step-memory" => grow(64 << 20).await.map(|n| n.to_string()),
        _ => grow(1 << 20).await.map(|n| n.to_string()),
    };
    match ended {
        Err(s::Error::StepFailed { cause, .. }) => Ok(cause.kind),
        other => other,
    }
}

s::export!(bounds, steps = [spin_step, grow]);
