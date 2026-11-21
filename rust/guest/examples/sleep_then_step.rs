//! A guest that sleeps, then runs a step: its run is replayed from its history at its wakeup.
//! Its step bumps a counter of the embedder's, and it exports a description of its own, as an
//! embedder's guest would.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use serde::Deserialize;
use substantial_guest as s;

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

#[s::workflow]
async fn sleep_then_step(c: s::Context, input: Input) -> s::Result<String> {
    c.sleep(s::seconds(1)).await?;
    greet(&input.name).await
}

s::export!(sleep_then_step, steps = [greet]);
