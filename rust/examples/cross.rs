//! The Rust side of the cross-language run of `dev/cross/run.py`: the same workflow as
//! `dev/cross/cross.py` and `dev/cross/cross.ts`, its runs started and served by any of them.
//!
//! ```text
//! cargo run -p substantial --example cross -- worker <url>   # serves the workflow until SIGTERM
//! cargo run -p substantial --example cross -- client <url>   # starts a run, messages it, prints its result
//! cargo run -p substantial --example cross -- start <url>    # starts the run `same-input`, its input written its own way
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};
use substantial::{Context, Credentials, Preset, Result, Substantial, minutes, step, workflow};

const LANGUAGE: &str = "rust";

#[derive(Serialize, Deserialize)]
struct Input {
    name: String,
    delay_ms: u64,
}

/// The input of `Input`, its fields in another order and its number a float.
#[derive(Serialize)]
struct Written {
    delay_ms: f64,
    name: &'static str,
}

#[derive(Serialize, Deserialize)]
struct Suffix {
    text: String,
    times: usize,
}

#[derive(Serialize, Deserialize)]
struct Output {
    greeting: String,
    suffix: String,
    worker: String,
}

#[step]
async fn greet(name: String) -> anyhow::Result<String> {
    Ok(format!("hello {name}"))
}

#[step]
async fn repeat(text: String, times: usize) -> anyhow::Result<String> {
    Ok(text.repeat(times))
}

#[workflow(name = "cross", version = 1)]
async fn cross(c: Context, input: Input) -> Result<Output> {
    let greeting = greet(input.name).await?;
    c.sleep(Duration::from_millis(input.delay_ms)).await?;
    let suffix: Suffix = c.receive("suffix").await?;
    Ok(Output {
        greeting,
        suffix: repeat(suffix.text, suffix.times).await?,
        worker: LANGUAGE.into(),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [mode, url] = args.as_slice() else {
        anyhow::bail!("usage: cross worker|client|start <url>");
    };
    let create = Substantial::create(url.as_str()).partitions(1);
    let sub = if url.starts_with("s3") {
        create.credentials(Credentials::Env).await?
    } else {
        create.await?
    };
    match mode.as_str() {
        "worker" => sub.worker(cross).preset(Preset::Script).serve().await?,
        "client" => {
            let input = Input {
                name: "ada".into(),
                delay_ms: 200,
            };
            let run = sub.start(cross, input).id("cross-1").await?;
            run.send(
                "suffix",
                Suffix {
                    text: "!".into(),
                    times: 3,
                },
            )
            .id("suffix-1")
            .await?;
            let result = run.result().timeout(minutes(1)).await?;
            let status = format!("{:?}", run.status().await?).to_lowercase();
            let line =
                serde_json::json!({ "result": result, "status": status, "client": LANGUAGE });
            println!("{line}");
        }
        "start" => {
            let written = Written {
                delay_ms: 200.0,
                name: "ada",
            };
            let run = sub.start(cross, written).id("same-input").await?;
            let status = format!("{:?}", run.status().await?).to_lowercase();
            println!(
                "{}",
                serde_json::json!({ "chain": run.chain(), "status": status })
            );
        }
        _ => anyhow::bail!("usage: cross worker|client|start <url>"),
    }
    Ok(())
}
