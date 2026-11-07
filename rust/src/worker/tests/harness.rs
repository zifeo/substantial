//! An in-memory stand-in for the agent machine: it wins or loses every commit and keeps the
//! history and inbox a store would hold.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

use tokio::sync::mpsc;

use crate::core::Core;
use crate::proto::boundary::v1::{self as b, CommitKind, replay_end};
use crate::proto::layout::v1::{self as layout, Time, event::Kind};
use crate::store::MemoryStore;
use crate::worker::replay::{Host, ReplayEvent, ToReplay, replay};
use crate::workflow::{Untyped, json};
use crate::{Context, Error, Workflow};

pub(super) struct Harness {
    pub(super) core: Core,
    pub(super) workflow: Untyped,
    pub(super) input: Vec<u8>,
    pub(super) history: Vec<layout::Event>,
    pub(super) next_n: u64,
    pub(super) inbox: Vec<b::InboxMessage>,
    pub(super) clock: Arc<AtomicI64>,
}

pub(super) struct Replayed {
    pub(super) commits: Vec<b::Commit>,
    pub(super) end: replay_end::End,
}

impl Replayed {
    pub(super) fn events(&self) -> Vec<Kind> {
        self.commits
            .iter()
            .flat_map(|c| c.segment.clone().unwrap().events)
            .map(|e| e.kind.unwrap())
            .collect()
    }

    pub(super) fn kinds(&self) -> Vec<CommitKind> {
        self.commits.iter().map(|c| c.kind()).collect()
    }

    pub(super) fn next_wake(&self) -> Option<i64> {
        match &self.end {
            replay_end::End::Idle(idle) => idle.next_wake.as_ref().map(|t| t.ms),
            replay_end::End::Dropped(_) => panic!("dropped"),
        }
    }
}

impl Harness {
    pub(super) fn new<O>(workflow: Workflow<O>) -> Self {
        Self {
            core: Core::new().unwrap(),
            workflow: workflow.untyped(),
            input: b"{}".to_vec(),
            history: vec![],
            next_n: 0,
            inbox: vec![],
            clock: Arc::new(AtomicI64::new(0)),
        }
    }

    pub(super) fn message(&mut self, key: &str, name: &str, value: &str) {
        let body = layout::Message {
            id: format!("id-{key}"),
            name: name.into(),
            value: Some(json(value.as_bytes().to_vec()).unwrap()),
            sent_at: Some(Time { ms: 0 }),
            ended: None,
        };
        self.inbox.push(b::InboxMessage {
            key: key.into(),
            body: Some(body),
        });
    }

    pub(super) fn run_input(&self, now: i64) -> b::RunInput {
        let start = layout::Start {
            workflow: self.workflow.name().into(),
            input: Some(json(self.input.clone()).unwrap()),
            chain: "run-1".into(),
            created_at: Some(Time { ms: 0 }),
            ..Default::default()
        };
        b::RunInput {
            run: "run-1".into(),
            start: Some(start),
            history: self.history.clone(),
            history_bytes: 0,
            next_n: self.next_n,
            inbox: self.inbox.clone(),
            now: Some(Time { ms: now }),
            seed: vec![42; 32],
            prefix: String::new(),
            partitions: 1,
            worker: "test".into(),
            settings: Some(b::RunSettings {
                commit_every_ms: 0,
                continue_as_new_after_segments: 1000,
                continue_as_new_after_bytes: 8 << 20,
                dedup_window_ms: 86_400_000,
            }),
        }
    }

    /// Replays at `now`, winning or losing every commit, and keeps what was committed.
    pub(super) async fn replay_with(&mut self, now: i64, won: bool) -> Replayed {
        self.clock.store(now, Ordering::SeqCst);
        let clock = self.clock.clone();
        let (events_tx, mut events) = mpsc::unbounded_channel();
        let (tell, inbox) = mpsc::unbounded_channel();
        let input = self.run_input(now);
        let host = Host {
            core: self.core.clone(),
            store: Arc::new(MemoryStore::open("harness").unwrap()),
            clock: Arc::new(move || Time {
                ms: clock.load(Ordering::SeqCst),
            }),
        };
        tokio::spawn(replay(
            host,
            self.workflow.clone(),
            crate::time::minutes(5),
            input,
            events_tx,
            inbox,
        ));
        let mut commits = vec![];
        loop {
            match events.recv().await.unwrap() {
                ReplayEvent::Commit(_, commit) => {
                    let segment = commit.segment.clone().unwrap();
                    tell.send(ToReplay::Committed {
                        n: segment.n,
                        won,
                        inbox: Vec::new(),
                    })
                    .unwrap();
                    if won {
                        self.history.extend(segment.events);
                        self.next_n = segment.n + 1;
                        self.inbox.retain(|m| !commit.consumed.contains(&m.key));
                        commits.push(commit);
                    }
                }
                ReplayEvent::Ended(_, end) => return Replayed { commits, end },
            }
        }
    }

    pub(super) async fn replay(&mut self, now: i64) -> Replayed {
        self.replay_with(now, true).await
    }
}

pub(super) fn completed(r: &Replayed) -> Option<serde_json::Value> {
    r.events().into_iter().find_map(|k| match k {
        Kind::Completed(c) => match c.value.unwrap().kind.unwrap() {
            layout::value::Kind::Json(bytes) => Some(serde_json::from_slice(&bytes).unwrap()),
            other => panic!("{other:?}"),
        },
        _ => None,
    })
}

pub(super) fn failed(r: &Replayed) -> Option<layout::Failure> {
    r.events().into_iter().find_map(|k| match k {
        Kind::Failed(f) => f.error,
        _ => None,
    })
}

pub(super) fn counter() -> Arc<AtomicU32> {
    Arc::new(AtomicU32::new(0))
}

pub(super) fn counting_step(
    c: &Context,
    name: &str,
    runs: &Arc<AtomicU32>,
    value: u32,
) -> crate::Step<u32> {
    let runs = runs.clone();
    c.step(name, move || {
        let runs = runs.clone();
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(value)
        }
    })
}
