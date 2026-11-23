//! `child.cancel`: a workflow cancels a child it started, as a client's `run.cancel` does. The
//! ports of `python/tests/test_child_cancel.py`, under the same names; a reason is a `&str`, so
//! the test of a reason that is not a string has no port.

use chrono::{DateTime, TimeZone, Utc};
use substantial::testing::Env;
use substantial::{Error, Preset, RunStatus, Workflow};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap()
}

async fn done(env: Env) {
    env.check().await.unwrap();
    env.close().await;
}

fn waiter() -> Workflow<String> {
    Workflow::without_input("waiter", |c| async move { c.receive("go").await })
}

fn quick() -> Workflow<u32> {
    Workflow::without_input("quick", |_| async move { Ok(1) })
}

fn canceller() -> Workflow<(String, String, String)> {
    Workflow::without_input("canceller", |c| async move {
        let child = c.start(&waiter(), ()).await?;
        child.cancel("no longer needed").await?;
        match child.result().await {
            Err(Error::RunFailed { status, error, .. }) => {
                Ok((child.chain().to_owned(), status.to_string(), error.message))
            }
            other => Err(Error::fail("Unexpected", format!("{other:?}"))),
        }
    })
}

#[tokio::test]
async fn cancel_a_running_child() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker((canceller(), waiter())))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&canceller(), ()).await.unwrap();
    // § t53k37khf.b@nwj63
    let (chain, status, reason) = run.result().await.unwrap();
    assert_eq!(
        (status.as_str(), reason.as_str()),
        ("cancelled", "no longer needed")
    );
    let child = env.run(waiter(), &chain).result().await;
    assert!(
        matches!(
            child,
            Err(Error::RunFailed {
                status: RunStatus::Cancelled,
                ..
            })
        ),
        "{child:?}"
    );
    // § end t53k37khf.b
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn late_canceller() -> Workflow<(String, String)> {
    Workflow::without_input("late_canceller", |c| async move {
        let child = c.start(&quick(), ()).await?;
        child.result().await?;
        match child.cancel("too late").await {
            Err(e @ Error::RunEnded { .. }) => Ok((child.chain().to_owned(), e.to_string())),
            other => Err(Error::fail("Unexpected", format!("{other:?}"))),
        }
    })
}

#[tokio::test]
async fn cancel_a_child_that_ended_raises_run_ended() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker((late_canceller(), quick())))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&late_canceller(), ()).await.unwrap();
    // § t53k37khf.a@yxnez
    let (chain, message) = run.result().await.unwrap();
    assert_eq!(message, format!("run {chain} has ended, completed"));
    // § end t53k37khf.a
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn suspending_canceller() -> Workflow<(String, String)> {
    Workflow::without_input("suspending_canceller", |c| async move {
        let child = c.start(&waiter(), ()).await?;
        child.cancel("no longer needed").await?;
        c.receive::<()>("resume").await?;
        match child.result().await {
            Err(Error::RunFailed { status, .. }) => {
                Ok((child.chain().to_owned(), status.to_string()))
            }
            other => Err(Error::fail("Unexpected", format!("{other:?}"))),
        }
    })
}

#[tokio::test]
async fn the_cancel_outcome_replays() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker((suspending_canceller(), waiter())))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&suspending_canceller(), ()).await.unwrap();
    env.settle().await.unwrap();
    worker.shutdown().await.unwrap();
    // The child ended cancelled: a cancel read again would raise RunEnded.
    let worker = (env.worker((suspending_canceller(), waiter())))
        .preset(Preset::Service)
        .start()
        .unwrap();
    run.send("resume", ()).await.unwrap();
    let (chain, status) = run.result().await.unwrap();
    assert_eq!(status, "cancelled");
    // § t53k37khf@09kec
    let history = run.history().await.unwrap();
    let sent: Vec<(&str, &str)> = (history.iter())
        .filter(|e| e.kind == "sent")
        .map(|e| {
            (
                e.data["name"].as_str().unwrap(),
                e.data["value"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(sent, [("substantial.cancel", "no longer needed")]);
    let child = env.run(waiter(), &chain).history().await.unwrap();
    assert_eq!(child.iter().filter(|e| e.kind == "cancelled").count(), 1);
    // § end t53k37khf
    worker.shutdown().await.unwrap();
    done(env).await;
}

fn impostor() -> Workflow<Vec<String>> {
    Workflow::without_input("impostor", |c| async move {
        let child = c.start(&waiter(), ()).await?;
        let mut refused = Vec::new();
        for sent in [
            child.send("substantial.cancel", "x").await,
            c.send(child.chain(), "substantial.cancel", "x").await,
        ] {
            if let Err(e @ Error::InvalidArgument(_)) = sent {
                refused.push(e.to_string());
            }
        }
        child.send("go", "done").await?;
        child.result().await?;
        Ok(refused)
    })
}

#[tokio::test]
async fn a_workflow_sending_substantial_cancel_raises_invalid_argument() {
    let env = Env::open(t0()).await.unwrap();
    let worker = (env.worker((impostor(), waiter())))
        .preset(Preset::Service)
        .start()
        .unwrap();
    let run = env.start(&impostor(), ()).await.unwrap();
    let message = "invalid argument: message name \"substantial.cancel\" starts with \
                   substantial., which is reserved";
    assert_eq!(run.result().await.unwrap(), [message, message]);
    worker.shutdown().await.unwrap();
    done(env).await;
}
