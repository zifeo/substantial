//! The backup of I-4, on a memory store instead of an h3 server, its steps stubbed:
//!
//! ```text
//! cargo run -p substantial --example backup
//! ```

use serde::{Deserialize, Serialize};
use substantial as s;

#[derive(Serialize, Deserialize)]
struct BackupInput {
    source: String,
    target: String,
}

#[derive(Serialize, Deserialize)]
struct VerifyInput {
    archive: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Report {
    url: String,
    approved_by: Vec<String>,
    check: bool,
}

async fn delete_archive(archive: String, files: &[String], tag: &str) -> anyhow::Result<()> {
    println!("delete {archive} of {} files, tag {tag}", files.len());
    Ok(())
}

async fn delete_upload(url: String, archive: &str, target: &str) -> anyhow::Result<()> {
    println!("delete {url}, the upload of {archive} to {target}");
    Ok(())
}

#[s::step]
async fn list_files(source: &str) -> anyhow::Result<Vec<String>> {
    Ok(vec![format!("{source}/a"), format!("{source}/b")])
}

#[s::step(timeout = s::minutes(10), undo = delete_archive)]
async fn make_archive(files: &[String], tag: &str) -> anyhow::Result<String> {
    Ok(format!("/tmp/{tag}-{}.tar", files.len()))
}

#[s::step(retry = s::Retry { max_attempts: 5, initial: s::seconds(1), max: s::minutes(1), multiplier: 2.0 }, undo = delete_upload)]
async fn upload(archive: &str, target: &str) -> anyhow::Result<String> {
    Ok(format!("{target}/{}", archive.trim_start_matches("/tmp/")))
}

#[s::step]
async fn check_archive(archive: &str) -> anyhow::Result<bool> {
    Ok(archive.ends_with(".tar"))
}

#[s::workflow]
async fn verify(c: s::Context, input: VerifyInput) -> s::Result<bool> {
    check_archive(&input.archive).await
}

#[s::workflow]
async fn backup(c: s::Context, input: BackupInput) -> s::Result<Report> {
    let tag = c.uuid().await?;
    let files = list_files(&input.source).await?;
    let archive = make_archive(&files, &tag).await?;
    let url = upload(&archive, &input.target).await?;
    c.log.info(format!("archived {} files", files.len()));

    let approvers = s::Var::new(Vec::<String>::new());
    c.handle(
        "approve",
        approvers.handler(|names, name: String| {
            if !names.contains(&name) {
                names.push(name)
            }
        }),
    )?;
    if !c
        .wait_until(approvers.check(|names| names.len() >= 2))
        .timeout(s::days(1))
        .await?
    {
        return Err(c.rollback("not approved within a day").await);
    }

    let check = c.start(&verify, VerifyInput { archive }).await?;
    Ok(Report {
        url,
        approved_by: approvers.get(),
        check: check.result().await?,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sub = s::Substantial::create("memory://backups")
        .partitions(1)
        .await?;
    let worker = sub
        .worker((backup, verify))
        .preset(s::Preset::Script)
        .start()?;
    let input = BackupInput {
        source: "/data".into(),
        target: "s3://archive".into(),
    };
    let run = sub.start(backup, input).id("backup-2026-10-31").await?;
    run.send("approve", "alice").await?;
    run.send("approve", "bob").await?;
    println!("{:?}", run.result().await?);
    worker.shutdown().await?;
    Ok(())
}
