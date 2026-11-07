//! The workflow of I-3 on a file store, to try durability by hand:
//!
//! ```text
//! cargo run -p substantial --example first_workflow -- /tmp/substantial-demo
//! ```
//!
//! Stop it with Ctrl-C during the sleep and run it again: the run resumes, and the steps it had
//! recorded do not print again.

use substantial as s;

#[s::step(retry = s::Retry { max_attempts: 6, initial: s::seconds(1), max: s::minutes(1), multiplier: 2.0 })]
async fn roulette() -> anyhow::Result<f64> {
    println!("step roulette");
    if rand::random::<f64>() < 1.0 / 6.0 {
        anyhow::bail!("shot!")
    }
    Ok(100.0)
}

#[s::step]
async fn multiply(a: f64, b: f64) -> anyhow::Result<f64> {
    println!("step multiply({a}, {b})");
    Ok(a * b)
}

#[s::workflow]
async fn example(c: s::Context) -> s::Result<f64> {
    let res = roulette().await?;
    println!("at the sleep (code outside steps runs again on every replay)");
    c.sleep(s::seconds(10)).await?;
    let res = multiply(res, 0.5).await?;

    let n: f64 = c.receive("by").await?;
    let res = multiply(res, n).await?;

    let stopped = s::Var::new(false);
    c.handle("stop", stopped.handler(|stopped, ()| *stopped = true))?;
    c.wait_until(stopped.check(|stopped| *stopped)).await?;
    Ok(res)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: first_workflow <directory>");
    let sub = s::Substantial::create(format!("file:{dir}"))
        .partitions(1)
        .await?;
    let worker = sub.worker(example).preset(s::Preset::Script).start()?;
    // The id makes the start idempotent: running the example again finds the same run. A send to
    // a run that ended raises `RunEnded`, so the messages go only to a run still going.
    let run = sub.start(example, ()).id("example-1").await?;
    let status = run.status().await?;
    if !matches!(
        status,
        s::RunStatus::Completed | s::RunStatus::Failed | s::RunStatus::Cancelled
    ) {
        run.send("by", 2).id("by-1").await?;
        run.send("stop", ()).id("stop-1").await?;
    }
    println!("Final output {}", run.result().await?);
    worker.shutdown().await?;
    Ok(())
}
