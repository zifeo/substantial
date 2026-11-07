//! The test environment of `docs/testing.md`: a namespace in memory on a virtual clock, whose
//! workers run in the test's process and move only when the test says so.
//!
//! It runs on a current-thread runtime, as `#[tokio::test]` gives. A multi-thread one works too,
//! but its threads interleave the workers at will, so they make more or fewer store operations:
//! a Chaos seed then gives the same ids and results, not the same counts in `Env::faults`.
//!
//! ```no_run
//! # use substantial as s;
//! # async fn t(debounce: s::Workflow<Vec<i32>>) -> s::Result<()> {
//! let t0 = chrono::DateTime::parse_from_rfc3339("2026-11-06T09:00:00Z").unwrap().into();
//! let env = s::testing::Env::open(t0).await?;
//! let worker = env.worker(&debounce).preset(s::Preset::Service).start()?;
//! let run = env.start(&debounce, serde_json::json!({ "key": "k" })).id("k").await?;
//! run.send("event", 1).await?;
//! env.advance(s::minutes(5)).await?;
//! assert_eq!(run.result().await?, vec![1]);
//! env.check().await?;
//! # Ok(()) }
//! ```

mod attached;
mod env;
mod hold;
mod park;
mod store;
mod time;
mod workers;
mod world;

use std::fmt::Display;

use sha2::{Digest, Sha256};

pub(crate) use attached::Attached;
pub use env::{Env, OpenEnv};
pub use hold::Hold;
pub use store::{Chaos, Faults};
pub(crate) use time::threaded;
pub(crate) use workers::{EnvWorker, Incarnation};

/// 32 bytes from `seed` and what they name, never from a place in a sequence.
fn derive(seed: u64, parts: &[&dyn Display]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(seed.to_le_bytes());
    for part in parts {
        let part = part.to_string();
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    hash.finalize().into()
}

/// A draw in [0, 1) from `seed` and what it names.
fn unit(seed: u64, parts: &[&dyn Display]) -> f64 {
    let bytes = derive(seed, parts);
    let n = u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"));
    (n >> 11) as f64 / (1u64 << 53) as f64
}

/// The random part of an environment's ULID made in millisecond `ms`.
pub(crate) fn ulid_bits(seed: u64, ms: u128) -> u128 {
    let bytes = derive(seed, &[&"ulid", &ms]);
    let mut wide = [0; 16];
    wide[6..].copy_from_slice(&bytes[..10]);
    u128::from_be_bytes(wide)
}
