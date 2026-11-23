//! A Wasm workflow that runs past the bound its input names: its code's time or memory, a step's time
//! or memory, or its code's time in segments between waits, each within it. Its steps' failures
//! come back as their causes' kinds.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial as s;

#[derive(Deserialize)]
#[serde(tag = "past", rename_all = "kebab-case")]
enum Past {
    CodeTime,
    CodeMemory,
    StepTime,
    StepMemory,
    /// The code spins `spins` iterations, `segments` times, with a verb between two.
    Segments {
        spins: u64,
        segments: u32,
    },
    Within,
}

const ONCE: s::Retry = s::Retry {
    max_attempts: 1,
    initial: std::time::Duration::from_secs(1),
    max: std::time::Duration::from_secs(1),
    multiplier: 1.0,
    deadline: None,
};

/// Tried thrice if retryable, so that one attempt shows a failure that is not.
const THRICE: s::Retry = s::Retry {
    max_attempts: 3,
    initial: std::time::Duration::from_millis(1),
    max: std::time::Duration::from_millis(1),
    multiplier: 1.0,
    deadline: None,
};

/// Spins without a host call, which only an interruption ends.
fn spin() -> ! {
    loop {
        std::hint::black_box(());
    }
}

/// Spins `n` iterations without a host call.
fn spin_for(n: u64) {
    for i in 0..n {
        std::hint::black_box(i);
    }
}

#[s::step(retry = ONCE, timeout = std::time::Duration::from_millis(300))]
async fn spin_step() -> Result<(), std::io::Error> {
    spin()
}

#[s::step(retry = THRICE)]
async fn grow(bytes: usize) -> Result<usize, std::io::Error> {
    Ok(std::hint::black_box(vec![1u8; bytes]).len())
}

#[s::workflow]
async fn bounds(c: s::Context, past: Past) -> s::Result<String> {
    let ended = match past {
        Past::CodeTime => spin(),
        Past::CodeMemory => Ok(std::hint::black_box(vec![1u8; 64 << 20]).len().to_string()),
        Past::StepTime => spin_step().await.map(|()| "spun".to_owned()),
        Past::StepMemory => grow(64 << 20).await.map(|n| n.to_string()),
        Past::Segments { spins, segments } => {
            for _ in 0..segments {
                spin_for(spins);
                c.random().await?;
            }
            Ok(format!("{segments} segments"))
        }
        Past::Within => grow(1 << 20).await.map(|n| n.to_string()),
    };
    match ended {
        Err(s::Error::StepFailed { cause, .. }) => Ok(cause.kind),
        other => other,
    }
}

s::export!(bounds, steps = [spin_step, grow]);
