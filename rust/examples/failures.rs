//! A Wasm workflow that fails as its input names: values JSON cannot hold at each crossing, a step's
//! errors, retryable or not, and a failure of its own.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial as s;

#[derive(Deserialize)]
struct Input {
    failure: String,
}

const TWICE: s::Retry = s::Retry {
    max_attempts: 2,
    initial: std::time::Duration::from_millis(1),
    max: std::time::Duration::from_millis(1),
    multiplier: 1.0,
};

#[s::step(retry = TWICE)]
async fn echo(x: f64) -> Result<f64, std::io::Error> {
    Ok(x)
}

#[s::step(retry = TWICE)]
async fn not_a_number() -> Result<f64, std::io::Error> {
    Ok(f64::NAN)
}

#[s::step(retry = TWICE)]
async fn failing() -> Result<(), std::io::Error> {
    Err(std::io::Error::other("failing"))
}

#[s::step(retry = TWICE)]
async fn permanent() -> Result<(), s::Permanent> {
    Err(s::Permanent::new("permanent"))
}

#[s::workflow]
async fn failures(c: s::Context, input: Input) -> s::Result<f64> {
    match input.failure.as_str() {
        "nan-result" => Ok(f64::NAN),
        "nan-message" => c.send(c.chain(), "x", f64::NAN).await.map(|()| 0.0),
        "huge-published" => c.publish("k", 1u64 << 60).map(|()| 0.0),
        "nan-arguments" => echo(f64::NAN).await,
        "nan-value" => not_a_number().await,
        "step-error" => failing().await.map(|()| 0.0),
        "step-permanent" => permanent().await.map(|()| 0.0),
        "own-failure" => Err(s::Error::Failed(s::Failure {
            kind: "Mine".into(),
            message: "mine".into(),
            retryable: false,
            details: Some(serde_json::json!({ "why": "mine" })),
        })),
        other => Err(s::Error::InvalidArgument(format!("no failure {other}"))),
    }
}

s::export!(failures, steps = [echo, not_a_number, failing, permanent]);
