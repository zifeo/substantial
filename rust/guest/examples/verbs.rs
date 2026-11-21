//! A guest that calls a verb as many times as its input says, for measuring what a verb costs.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial_guest as s;

#[derive(Deserialize)]
struct Input {
    n: u32,
}

#[s::workflow]
async fn verbs(c: s::Context, input: Input) -> s::Result<u32> {
    for _ in 0..input.n {
        c.random().await?;
    }
    Ok(input.n)
}

s::export!(verbs);
