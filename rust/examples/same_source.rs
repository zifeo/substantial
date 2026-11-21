//! A workflow whose one source builds both ways: natively, a workflow of the host, and for wasm32, a
//! Wasm workflow. The runner's tests serve both and compare their runs.

use serde::Deserialize;
use substantial as s;

#[derive(Deserialize)]
pub struct Input {
    name: String,
}

#[s::step]
async fn greet(name: &str) -> Result<String, std::io::Error> {
    Ok(format!("hello {name}"))
}

#[s::workflow]
pub async fn greeting(c: s::Context, input: Input) -> s::Result<String> {
    c.sleep(s::seconds(1)).await?;
    greet(&input.name).await
}

s::export!(greeting, steps = [greet]);
