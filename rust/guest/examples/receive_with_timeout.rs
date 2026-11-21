//! A guest that waits for a message, or for its timeout to pass.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial_guest as s;

#[derive(Deserialize)]
struct Input {
    timeout_ms: u64,
}

#[s::workflow]
async fn receive_with_timeout(c: s::Context, input: Input) -> s::Result<String> {
    let timeout = std::time::Duration::from_millis(input.timeout_ms);
    match c.receive::<String>("note").timeout(timeout).await {
        Ok(note) => Ok(format!("received {note}")),
        Err(s::Error::WaitTimeout) => Ok("timed out".to_owned()),
        Err(e) => Err(e),
    }
}

s::export!(receive_with_timeout);
