//! The replay tester: recorded runs copied from a store into an environment, replayed by new code
//! against their histories, for CI before a deploy.

use std::future::IntoFuture;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use prost::Message;

use super::env::Env;
use crate::error::{Error, Result};
use crate::proto::layout::v1 as layout;
use crate::settings::{Preset, STORE_RETRY};
use crate::store::{self, Credentials, Store};
use crate::workflow::{IntoWorkflows, Untyped};

/// How a run replayed against its history.
#[derive(Debug)]
pub enum ReplayOutcome {
    /// The code made every call the history recorded, up to where the run waits.
    Matched,
    /// The code made a call the history does not answer: `Error::NonDeterminism`.
    Diverged(Error),
    /// The run had ended, so no worker replays it again.
    Ended,
}

/// A run the replay tester replayed.
#[derive(Debug)]
pub struct Replayed {
    pub run: String,
    pub outcome: ReplayOutcome,
}

/// Replays the runs `runs` of the store at `source` with the code of `workflows`: each run is
/// copied into an environment's namespace in memory, by get and list alone, and replayed there up
/// to the end of its history, with no step run. A run that diverges is `Diverged`, with the
/// `NonDeterminism` a worker of this code would meet.
pub fn replay(source: impl AsRef<str>, workflows: impl IntoWorkflows) -> Replay {
    Replay {
        source: source.as_ref().to_owned(),
        workflows: workflows.into_workflows(),
        credentials: None,
        runs: Vec::new(),
    }
}

/// A replay of recorded runs, awaited to run it; `runs` names them, `credentials` reaches the
/// source as `Substantial::open` does.
#[must_use = "a replay does nothing until awaited"]
pub struct Replay {
    source: String,
    workflows: Vec<Untyped>,
    credentials: Option<Credentials>,
    runs: Vec<String>,
}

impl Replay {
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// The ids of the runs to replay: for a chain that continued, its run that is live
    /// (`RunInfo::run`).
    pub fn runs<S: Into<String>>(mut self, runs: impl IntoIterator<Item = S>) -> Self {
        self.runs.extend(runs.into_iter().map(Into::into));
        self
    }
}

impl IntoFuture for Replay {
    type Output = Result<Vec<Replayed>>;
    type IntoFuture = BoxFuture<'static, Result<Vec<Replayed>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.replayed())
    }
}

impl Replay {
    async fn replayed(self) -> Result<Vec<Replayed>> {
        // § awhhtbr5n@e16j3
        if self.runs.is_empty() {
            return Err(Error::InvalidArgument(
                "a replay names at least one run".into(),
            ));
        }
        let credentials = self.credentials.unwrap_or(Credentials::None);
        let source = store::open(&self.source, &credentials, STORE_RETRY)?;
        source.open().await?;
        let now = DateTime::<Utc>::from_timestamp_millis(crate::time::now().ms)
            .expect("now, in chrono's range");
        let env = Env::open(now).await?;
        let world = env.world().clone();
        world.check_only();
        let target: &dyn Store = &world.memory;
        same_layout(&*source, target).await?;
        let mut replayed = Vec::new();
        for run in self.runs {
            // § awhhtbr5n.c@sspp0
            let head = head_of(&*source, &run)
                .await?
                .ok_or_else(|| Error::RunNotFound { chain: run.clone() })?;
            let start = head
                .start
                .clone()
                .ok_or_else(|| Error::LayoutError(format!("runs/{run}: a head without start")))?;
            let served = self
                .workflows
                .iter()
                .any(|w| w.name() == start.workflow && w.version() == start.version);
            if !served {
                return Err(Error::InvalidArgument(format!(
                    "run {run} is of workflow {:?} version {:?}, which no workflow given serves",
                    start.workflow, start.version
                )));
            }
            // § end awhhtbr5n.c
            // A run its chain continued, retried or reset past, or one that ended.
            if head.run != run || ended(&head) {
                replayed.push(Replayed {
                    run,
                    outcome: ReplayOutcome::Ended,
                });
                continue;
            }
            // § awhhtbr5n.d@57cmf
            // Without its lease, which would hold it for its worker, and its block, which would
            // hold it until its backoff.
            let held = layout::Head {
                lease: None,
                blocked: None,
                ..head
            };
            let chain = start.chain.clone();
            let bytes = layout::Run {
                kind: Some(layout::run::Kind::Head(held.clone())),
            }
            .encode_to_vec();
            create(target, &format!("runs/{chain}"), bytes).await?;
            // § end awhhtbr5n.d
            let due = format!(
                "queues/{}/due/{}~{chain}~s{:010}",
                start.workflow,
                now.format("%Y%m%dt%H%M%S%3fz"),
                held.segments.len()
            );
            create(target, &due, Vec::new()).await?;
            let worker = env
                .worker(self.workflows.clone())
                .preset(Preset::Script)
                .start()?;
            env.settle().await?;
            worker.crash();
            // A replay that starts no step takes no claim, so the next run's worker would replay
            // this one again, its wakeup still due.
            target
                .delete(&due)
                .await
                .map_err(|e| Error::StoreError(e.message))?;
            let outcome = match world.checked_run(&run) {
                Some(None) => ReplayOutcome::Matched,
                Some(Some(message)) => ReplayOutcome::Diverged(Error::NonDeterminism(message)),
                None => {
                    return Err(Error::Protocol(format!(
                        "run {run} was not replayed: {}",
                        world.worker_errors()
                    )));
                }
            };
            replayed.push(Replayed { run, outcome });
        }
        env.close().await;
        Ok(replayed)
    }
}

/// Refuses a source of another layout than the environment's.
async fn same_layout(source: &dyn Store, target: &dyn Store) -> Result<()> {
    // § awhhtbr5n.c@r737v
    let marker = "substantial.json";
    let (theirs, ours) = (get(source, marker).await?, get(target, marker).await?);
    match theirs {
        Some(theirs) if Some(&theirs) == ours.as_ref() => Ok(()),
        Some(theirs) => Err(Error::LayoutError(format!(
            "the source's {marker} is {}, not this version's {}",
            String::from_utf8_lossy(&theirs),
            String::from_utf8_lossy(&ours.unwrap_or_default())
        ))),
        None => Err(Error::LayoutError(
            "the source names no namespace: it has no substantial.json".into(),
        )),
    }
}

/// The head `runs/<id>` holds, through the pointer of a later run to its chain's head.
async fn head_of(source: &dyn Store, id: &str) -> Result<Option<layout::Head>> {
    let key = format!("runs/{id}");
    let Some(found) = decode::<layout::Run>(source, &key).await? else {
        return Ok(None);
    };
    match found.kind {
        Some(layout::run::Kind::Head(head)) => Ok(Some(head)),
        Some(layout::run::Kind::Pointer(p)) => {
            let key = format!("runs/{}", p.chain);
            match decode::<layout::Run>(source, &key)
                .await?
                .and_then(|r| r.kind)
            {
                Some(layout::run::Kind::Head(head)) => Ok(Some(head)),
                Some(layout::run::Kind::Pointer(_)) => {
                    Err(Error::LayoutError(format!("{key}: a pointer to a pointer")))
                }
                None => Ok(None),
            }
        }
        None => Err(Error::LayoutError(format!(
            "{key}: neither head nor pointer"
        ))),
    }
}

/// Whether the head's run ended: its last segment holds an end.
fn ended(head: &layout::Head) -> bool {
    use layout::event::Kind;
    head.segments.last().is_some_and(|s| {
        s.events.iter().any(|e| {
            matches!(
                e.kind,
                Some(
                    Kind::Completed(_) | Kind::Failed(_) | Kind::Cancelled(_) | Kind::Continued(_)
                )
            )
        })
    })
}

async fn create(target: &dyn Store, key: &str, bytes: Vec<u8>) -> Result<()> {
    target
        .create(key, bytes.into(), None)
        .await
        .map(drop)
        .map_err(|e| Error::StoreError(e.message))
}

async fn get(store: &dyn Store, key: &str) -> Result<Option<bytes::Bytes>> {
    match store.get(key).await {
        Ok(found) => Ok(found.map(|o| o.bytes)),
        Err(e) => Err(Error::StoreError(e.message)),
    }
}

async fn decode<M: Message + Default>(store: &dyn Store, key: &str) -> Result<Option<M>> {
    match get(store, key).await? {
        Some(bytes) => M::decode(bytes)
            .map(Some)
            .map_err(|e| Error::LayoutError(format!("{key}: {e}"))),
        None => Ok(None),
    }
}
