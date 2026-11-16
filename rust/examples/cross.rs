//! The Rust side of the cross-language run of `dev/cross/run.py`: the same workflows as
//! `dev/cross/cross.py` and `dev/cross/cross.ts`, their runs started and served by any of them.
//!
//! ```text
//! cargo run -p substantial --example cross -- worker <url>   # serves the workflows until SIGTERM
//! cargo run -p substantial --example cross -- client <url>   # starts a run with a budget, waits until it waits on messages, messages it, prints its result and history, and the failure of a run of `over` that passes its budget and the config a run of `ticks` published
//! cargo run -p substantial --example cross -- start <url>    # starts the run `same-input`, its input written its own way
//! ```

use std::ops::ControlFlow;
use std::time::Duration;

use futures::TryStreamExt;

use serde::{Deserialize, Serialize};
use substantial::{
    Context, Credentials, Cron, Error, Preset, Result, RunStatus, Substantial, WaitingOn, Workflow,
    charge, minutes, step, workflow,
};

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
    charged: String,
    counted: bool,
}

#[step]
async fn greet(name: String) -> anyhow::Result<String> {
    Ok(format!("hello {name}"))
}

#[step]
async fn repeat(text: String, times: usize) -> anyhow::Result<String> {
    charge("USD", 1_500_000)?;
    Ok(text.repeat(times))
}

#[workflow(name = "cross", version = 1)]
async fn cross(c: Context, input: Input) -> Result<Output> {
    let greeting = greet(input.name).await?;
    c.sleep(Duration::from_millis(input.delay_ms)).await?;
    let suffix: Suffix = c.receive("suffix").await?;
    let repeated = repeat(suffix.text, suffix.times).await?;
    let cost = c.cost().await?;
    Ok(Output {
        greeting,
        suffix: repeated,
        worker: LANGUAGE.into(),
        charged: cost.steps["repeat"]["USD"].to_string(),
        counted: cost.requests > 0,
    })
}

#[derive(Serialize, Deserialize)]
struct Over {
    text: String,
}

/// Charges more than a budget of USD 1000000 allows, so its next step is refused.
#[workflow(name = "over", version = 1)]
async fn over(_c: Context, input: Over) -> Result<String> {
    let repeated = repeat(input.text, 1).await?;
    greet(repeated).await
}

/// Starts `cross-over` with a budget it passes; its failure as its result gives it, its history
/// ending failed.
async fn overrun(sub: &Substantial) -> anyhow::Result<serde_json::Value> {
    let run = sub
        .start(over, Over { text: "!".into() })
        .id("cross-over")
        .budget([("USD", 1_000_000)])
        .await?;
    let failure = match run.result().timeout(minutes(1)).await {
        Err(Error::RunFailed { error, .. }) => error,
        Ok(_) => anyhow::bail!("cross-over completed within a budget it passes"),
        Err(e) => return Err(e.into()),
    };
    let history = run.history().await?;
    let ended = history.last().map(|e| &e.kind);
    if ended.is_none_or(|kind| kind != "failed") {
        anyhow::bail!("cross-over failed, its history ending with {ended:?}");
    }
    let Some(details) = failure.details else {
        anyhow::bail!("cross-over failed without details");
    };
    Ok(serde_json::json!({
        "type": failure.kind,
        "meter": details["meter"],
        "limit": details["limit"],
        "used": details["used"],
    }))
}

#[workflow(name = "ticks", version = 1)]
async fn ticks(c: Context) -> Result<()> {
    c.schedule(|_c, _at| async { Ok(ControlFlow::Continue(())) })
        .await
}

fn nightly() -> Result<Cron> {
    Cron::new("0 3 * * *", "Europe/Zurich")
}

/// Configures `cross-ticks`, served by another host, until it publishes the config; the same call
/// again does nothing, an older revision is refused.
async fn scheduled(sub: &Substantial) -> anyhow::Result<serde_json::Value> {
    let run = sub
        .schedule(ticks, "cross-ticks", Some(nightly()?), 2)
        .backfill(3)
        .await?;
    let published = loop {
        let published: Option<serde_json::Value> = run.read("schedule").await?;
        match published {
            Some(p) if p["revision"] == 2 => break p,
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };
    sub.schedule(ticks, "cross-ticks", Some(nightly()?), 2)
        .backfill(3)
        .await?;
    let stale = match sub
        .schedule(ticks, "cross-ticks", Some(nightly()?), 1)
        .await
    {
        Err(Error::ConfigError(_)) => "ConfigError",
        Err(e) => return Err(e.into()),
        Ok(_) => "accepted",
    };
    let mut line = serde_json::json!({ "stale": stale });
    for key in ["revision", "cron", "tz", "backfill", "last", "recent"] {
        line[key] = published[key].clone();
    }
    Ok(line)
}

#[workflow(name = "awaited", version = 1)]
async fn awaited(c: Context) -> Result<serde_json::Value> {
    let go: String = c.receive("go").await?;
    Ok(serde_json::json!({ "worker": LANGUAGE, "go": go }))
}

#[derive(Serialize, Deserialize)]
struct Watch {
    chain: String,
}

/// `watch-<language>`, which awaits the end of a chain it did not start: served by that
/// language's worker, a stub elsewhere.
fn watcher(language: &str) -> Workflow<serde_json::Value> {
    Workflow::new(
        &format!("watch-{language}"),
        |c: Context, input: Watch| async move {
            let result: serde_json::Value = c.result(&input.chain).await?;
            Ok(serde_json::json!({ "watcher": LANGUAGE, "result": result }))
        },
    )
    .with_version(1)
}

/// Starts `cross-awaited`, served by the worker, and `cross-watch`, served by the watcher of
/// `language`, which awaits it; once the watcher waits, lets `cross-awaited` end.
async fn watched(sub: &Substantial, language: &str) -> anyhow::Result<serde_json::Value> {
    let run = sub.start(awaited, ()).id("cross-awaited").await?;
    let watch = Watch {
        chain: run.chain().to_owned(),
    };
    let watch = sub
        .start(&watcher(language), watch)
        .id("cross-watch")
        .await?;
    while watch.status().await? != RunStatus::Suspended {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    run.send("go", "now").await?;
    Ok(watch.result().timeout(minutes(1)).await?)
}

/// Until `cross-1`, served by another host, is listed waiting on messages alone.
async fn waits_on_messages(sub: &Substantial) -> Result<()> {
    loop {
        let mut runs = sub.runs(cross);
        while let Some(info) = runs.try_next().await? {
            if info.run == "cross-1" && info.waiting_on == WaitingOn::Message {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mode, url, watching) = match args.as_slice() {
        [mode, url] if mode != "client" => (mode, url, None),
        [mode, url, watching] if mode == "client" => (mode, url, Some(watching.as_str())),
        _ => anyhow::bail!("usage: cross worker|watcher|start <url>, cross client <url> <watcher>"),
    };
    let create = Substantial::create(url.as_str());
    let sub = if url.starts_with("s3") {
        create.credentials(Credentials::Env).await?
    } else {
        create.await?
    };
    match mode.as_str() {
        "worker" => {
            sub.worker((cross, over, ticks, awaited))
                .preset(Preset::Script)
                .serve()
                .await?
        }
        "watcher" => {
            sub.worker(watcher(LANGUAGE))
                .preset(Preset::Script)
                .serve()
                .await?
        }
        "client" => {
            let input = Input {
                name: "ada".into(),
                delay_ms: 200,
            };
            let run = sub
                .start(cross, input)
                .id("cross-1")
                .budget([("requests", 100_000), ("USD", 10_000_000)])
                .await?;
            waits_on_messages(&sub).await?;
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
            let history: Vec<String> = run.history().await?.into_iter().map(|e| e.kind).collect();
            let line = serde_json::json!({
                "result": result, "status": status, "history": history,
                "over": overrun(&sub).await?, "schedule": scheduled(&sub).await?,
                "watched": watched(&sub, watching.expect("a client's watcher")).await?,
                "client": LANGUAGE
            });
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
