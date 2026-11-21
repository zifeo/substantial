//! A Wasm workflow that sleeps, then runs a step: its run is replayed from its history at its wakeup.
//! Its step bumps a counter of the embedder's, and it exports a description of its own, as an
//! embedder's Wasm workflow would. The name `spin` runs a step that bumps until its timeout
//! abandons it, and `bump-in-code` bumps from the workflow code, which may not.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial as s;

mod embedder {
    wit_bindgen::generate!({
        inline: "
            package example:embedder;

            interface counter {
                bump: func() -> u32;
            }

            interface describe {
                describe: func() -> string;
            }

            world embedder {
                import counter;
                export describe;
            }
        ",
    });
}

struct Description;

impl embedder::exports::example::embedder::describe::Guest for Description {
    fn describe() -> String {
        "sleeps, then greets".to_owned()
    }
}

embedder::export!(Description with_types_in embedder);

#[derive(Deserialize)]
struct Input {
    name: String,
}

#[s::step]
async fn greet(name: &str) -> Result<String, std::io::Error> {
    let n = embedder::example::embedder::counter::bump();
    Ok(format!("hello {name}, greeting {n}"))
}

const ONCE: s::Retry = s::Retry {
    max_attempts: 1,
    initial: std::time::Duration::from_secs(1),
    max: std::time::Duration::from_secs(1),
    multiplier: 1.0,
};

#[s::step(retry = ONCE, timeout = std::time::Duration::from_millis(200))]
async fn bump_forever() -> Result<(), std::io::Error> {
    loop {
        embedder::example::embedder::counter::bump();
    }
}

#[s::workflow]
async fn sleep_then_step(c: s::Context, input: Input) -> s::Result<String> {
    match input.name.as_str() {
        "spin" => match bump_forever().await {
            Err(s::Error::StepFailed { cause, .. }) => Ok(cause.kind),
            other => other.map(|()| "bumped".to_owned()),
        },
        "bump-in-code" => Ok(embedder::example::embedder::counter::bump().to_string()),
        name => {
            c.sleep(s::seconds(1)).await?;
            greet(name).await
        }
    }
}

s::export!(sleep_then_step, steps = [greet, bump_forever]);
