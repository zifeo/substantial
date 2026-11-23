use std::future::IntoFuture;
use std::marker::PhantomData;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Substantial;
use crate::cost::Budget;
use crate::error::{Error, Failure, Result, run_failed_cause};
use crate::names::{check_key, check_sent_name};
use crate::plan;
use crate::proto::boundary::v1::{
    self as b, history, plan_input::Plan, plan_step::Step, read_value, retried, run_result,
    run_state, sent,
};
use crate::proto::layout::v1::Status;
use crate::store::h3;
use crate::workflow::{decode, json};

/// The status of a chain's last run (I-17).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Pending,
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
    /// Its code raised an error that is no end, or diverged from its history: it records nothing
    /// and is tried again after a backoff, until code that runs it is deployed.
    Blocked,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RunStatus::Pending => "pending",
            RunStatus::Running => "running",
            RunStatus::Suspended => "suspended",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
            RunStatus::Blocked => "blocked",
        })
    }
}

impl TryFrom<b::RunStatus> for RunStatus {
    type Error = Error;

    fn try_from(s: b::RunStatus) -> Result<Self> {
        Ok(match s {
            b::RunStatus::Pending => RunStatus::Pending,
            b::RunStatus::Running => RunStatus::Running,
            b::RunStatus::Suspended => RunStatus::Suspended,
            b::RunStatus::Completed => RunStatus::Completed,
            b::RunStatus::Failed => RunStatus::Failed,
            b::RunStatus::Cancelled => RunStatus::Cancelled,
            b::RunStatus::Blocked => RunStatus::Blocked,
            b::RunStatus::Unspecified => {
                return Err(Error::Protocol("unspecified run status".into()));
            }
        })
    }
}

/// An event of a run's history, as its replay reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryEvent {
    /// The run whose segment recorded it: the chain's last run, or a failed run it resumes.
    pub run: String,
    pub segment: u64,
    /// When the segment was committed.
    pub at: chrono::DateTime<chrono::Utc>,
    /// `step`, `step_retry`, `step_failed`, `timer`, `fired`, `message`, `child`, `sent`,
    /// `published`, `budget_exceeded`, `completed`, `failed`, `cancelled` or `continued`.
    pub kind: String,
    /// The event's fields: times in RFC 3339, failures as `{"type", "message", "retryable",
    /// "details"}`, a value of raw bytes as `{"$bytes": "<base64>"}`.
    pub data: serde_json::Value,
}

impl TryFrom<b::HistoryEvent> for HistoryEvent {
    type Error = Error;

    fn try_from(e: b::HistoryEvent) -> Result<Self> {
        let at =
            e.at.ok_or_else(|| Error::Protocol("history event without a time".into()))?;
        Ok(HistoryEvent {
            run: e.run,
            segment: e.segment,
            at: super::datetime(&at),
            kind: e.kind,
            data: serde_json::from_slice(&e.data)?,
        })
    }
}

/// A handle on a chain whose result is an `O`: `send`, `result`, `status`, `cancel`, `history`
/// and `retry` act on its last run.
pub struct Run<O> {
    sub: Substantial,
    chain: String,
    output: PhantomData<fn() -> O>,
}

impl<O> Clone for Run<O> {
    fn clone(&self) -> Self {
        Self::new(self.sub.clone(), self.chain.clone())
    }
}

impl<O> std::fmt::Debug for Run<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run").field("chain", &self.chain).finish()
    }
}

impl<O> Run<O> {
    pub(super) fn new(sub: Substantial, chain: String) -> Self {
        Self {
            sub,
            chain,
            output: PhantomData,
        }
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// Sends a message, delivered once per `.id(..)`, a ULID when none is given.
    pub fn send(&self, name: &str, value: impl Serialize) -> SendMessage {
        SendMessage {
            sub: self.sub.clone(),
            chain: self.chain.clone(),
            name: name.to_owned(),
            value: crate::workflow::argument_json(&value, "the message"),
            id: None,
        }
    }

    /// Cancels the run: it raises `Cancelled` at every call it is blocked on.
    pub async fn cancel(&self, reason: &str) -> Result<()> {
        self.check_chain()?;
        let now = self.sub.now();
        // § wtwx1e769.c@yvbnz
        let ord = self.sub.ulid(now.ms);
        let cancel = b::CancelPlan {
            prefix: String::new(),
            chain: self.chain.clone(),
            reason: reason.to_owned(),
            id: ord.clone(),
            now: Some(now),
            ord,
        };
        // § end wtwx1e769.c
        self.sent(Plan::Cancel(cancel)).await
    }

    async fn sent(&self, plan: Plan) -> Result<()> {
        sent(&self.sub, &self.chain, plan).await
    }

    fn check_chain(&self) -> Result<()> {
        check_key("run id", &self.chain)
    }

    fn not_found(&self) -> Error {
        not_found(&self.chain)
    }

    pub async fn status(&self) -> Result<RunStatus> {
        self.check_chain()?;
        let status = b::StatusPlan {
            prefix: String::new(),
            chain: self.chain.clone(),
            now: Some(self.sub.now()),
        };
        let status = match self.sub.plan(Plan::Status(status)).await? {
            Step::Status(b::RunState { state: Some(state) }) => match state {
                run_state::State::Status(of) => of.status().try_into(),
                run_state::State::NoChain(_) => Err(self.not_found()),
                run_state::State::Failed(f) => Err(Error::StoreError(f.message)),
            },
            other => Err(plan::unexpected("status", other)),
        }?;
        #[cfg(feature = "testing")]
        if let Some(env) = self.sub.env() {
            env.read(&self.chain, status);
        }
        Ok(status)
    }

    /// The events of the chain's last run as its replay reads them, a retry's including those of
    /// the failed run it resumes.
    pub async fn history(&self) -> Result<Vec<HistoryEvent>> {
        self.check_chain()?;
        let plan = b::HistoryPlan {
            prefix: String::new(),
            chain: self.chain.clone(),
        };
        match self.sub.plan(Plan::History(plan)).await? {
            Step::History(b::History {
                history: Some(history),
            }) => match history {
                history::History::Events(e) => {
                    e.events.into_iter().map(TryInto::try_into).collect()
                }
                history::History::NoChain(_) => Err(self.not_found()),
                history::History::Failed(f) => Err(Error::StoreError(f.message)),
            },
            other => Err(plan::unexpected("history", other)),
        }
    }

    /// Retries the chain's last run, which failed: a new run replays its history without its
    /// failure, so its steps that succeeded return their recorded values and the step that
    /// failed runs again with a fresh retry budget. The retry keeps the failed run's budget and
    /// its cost so far, unless `.budget(..)` gives another budget.
    pub fn retry(&self) -> RetryRun {
        RetryRun {
            sub: self.sub.clone(),
            chain: self.chain.clone(),
            budget: None,
            keep: None,
        }
    }

    /// The last value published under `key` by any run of the chain, `None` before the first.
    pub async fn read<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.check_chain()?;
        check_key("published key", key)?;
        let read = b::ReadPlan {
            prefix: String::new(),
            chain: self.chain.clone(),
            key: key.to_owned(),
        };
        match self.sub.plan(Plan::Read(read)).await? {
            Step::Read(b::ReadValue { read: Some(read) }) => match read {
                read_value::Read::Value(v) => decode(Some(v)).map(Some),
                read_value::Read::None(_) => Ok(None),
                read_value::Read::Failed(f) => Err(Error::StoreError(f.message)),
            },
            other => Err(plan::unexpected("read", other)),
        }
    }

    /// The value the chain ended with, waited for through its continuations.
    pub fn result(&self) -> ResultWait<O> {
        ResultWait {
            run: self.clone(),
            timeout: None,
            max_interval: None,
        }
    }

    /// The chain's end, or its last run while it has none. `last` is the run the poll before
    /// answered, whose `result` alone the core then gets.
    async fn ended(&self, last: Option<String>) -> Result<Ended<O>>
    where
        O: DeserializeOwned,
    {
        self.check_chain()?;
        let plan = b::ResultPlan {
            prefix: String::new(),
            chain: self.chain.clone(),
            run: last,
        };
        let (run, result) = match self.sub.plan(Plan::Result(plan)).await? {
            Step::Result(b::RunResult {
                result: Some(outcome),
            }) => match outcome {
                run_result::Result::Ended(e) => {
                    let result = e
                        .result
                        .ok_or_else(|| Error::Protocol("ended without a result".into()))?;
                    (e.run, result)
                }
                run_result::Result::NotEnded(live) => return Ok(Ended::Live(live)),
                run_result::Result::NoChain(_) => return Err(self.not_found()),
                run_result::Result::Failed(f) => return Err(Error::StoreError(f.message)),
            },
            other => return Err(plan::unexpected("result", other)),
        };
        let status = result.status();
        let failed = |status, error, cause| {
            Err(Error::RunFailed {
                run: run.clone(),
                status,
                error: Box::new(error),
                cause,
            })
        };
        match status {
            Status::Completed => decode(result.value).map(Ended::Value),
            Status::Failed => {
                let error = Failure::try_from(
                    result
                        .error
                        .ok_or_else(|| Error::Protocol("failed without an error".into()))?,
                )?;
                let cause = run_failed_cause(&error.kind, error.details.clone())?;
                failed(RunStatus::Failed, error, cause)
            }
            Status::Cancelled => {
                let error = Failure {
                    kind: "Cancelled".into(),
                    message: result.reason,
                    retryable: false,
                    details: None,
                };
                failed(RunStatus::Cancelled, error, None)
            }
            other => Err(Error::Protocol(format!(
                "result of {run} with status {other:?}"
            ))),
        }
    }
}

enum Ended<O> {
    Value(O),
    /// The run to poll next, and its chain's head to wait on.
    Live(run_result::NotEnded),
}

fn not_found(chain: &str) -> Error {
    Error::RunNotFound {
        chain: chain.to_owned(),
    }
}

async fn sent(sub: &Substantial, chain: &str, plan: Plan) -> Result<()> {
    match sub.plan(plan).await? {
        Step::Sent(b::Sent {
            sent: Some(outcome),
        }) => sent_outcome(sub, chain, outcome),
        other => Err(plan::unexpected("send", other)),
    }
}

/// What a send to `chain` answered: `Ok` once accepted, or the error it raises.
pub(super) fn sent_outcome(sub: &Substantial, chain: &str, outcome: sent::Sent) -> Result<()> {
    match outcome {
        sent::Sent::Accepted(accepted) => {
            sub.wake_workers(&accepted.workflow);
            Ok(())
        }
        sent::Sent::Refused(r) => Err(Error::RunEnded {
            status: ended(r.status())?,
            run: r.run,
        }),
        sent::Sent::NoChain(_) => Err(not_found(chain)),
        sent::Sent::Failed(f) => Err(Error::StoreError(f.message)),
    }
}

/// A retry of a chain's failed last run, awaited to create it.
#[must_use = "a retry does nothing until awaited"]
pub struct RetryRun {
    sub: Substantial,
    chain: String,
    budget: Option<Budget>,
    keep: Option<u64>,
}

impl RetryRun {
    /// The retry's budget, instead of the failed run's. A second retry of the same failed run
    /// with another budget is `InvalidArgument`.
    pub fn budget(mut self, budget: impl Into<Budget>) -> Self {
        self.budget = Some(budget.into());
        self
    }

    /// Resets the chain's last run, ended completed, failed or cancelled, to its first `keep`
    /// events of `history()`: the new run replays them, and runs the code live from there.
    pub fn keep(mut self, keep: u64) -> Self {
        self.keep = Some(keep);
        self
    }
}

impl IntoFuture for RetryRun {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'static, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            check_key("run id", &self.chain)?;
            let plan = b::RetryPlan {
                prefix: String::new(),
                chain: self.chain.clone(),
                now: Some(self.sub.now()),
                budget: self.budget.as_ref().map(Budget::to_proto).transpose()?,
                keep: self.keep,
            };
            match self.sub.plan(Plan::Retry(plan)).await? {
                Step::Retried(b::Retried {
                    retried: Some(outcome),
                }) => match outcome {
                    retried::Retried::Created(r) | retried::Retried::Existing(r) => {
                        self.sub.wake_workers(&r.workflow);
                        Ok(())
                    }
                    retried::Retried::NotRetryable(n) => Err(Error::InvalidArgument(n.message)),
                    retried::Retried::NoChain(_) => Err(not_found(&self.chain)),
                    retried::Retried::Failed(f) => Err(Error::StoreError(f.message)),
                },
                other => Err(plan::unexpected("retry", other)),
            }
        })
    }
}

/// A message to send, awaited to send it; `id` makes the send idempotent.
#[must_use = "a message is not sent until awaited"]
pub struct SendMessage {
    sub: Substantial,
    chain: String,
    name: String,
    value: Result<Vec<u8>>,
    id: Option<String>,
}

impl SendMessage {
    /// Delivers the message once whatever the number of sends with this id.
    pub fn id(mut self, id: &str) -> Self {
        self.id = Some(id.to_owned());
        self
    }
}

impl IntoFuture for SendMessage {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'static, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            check_key("run id", &self.chain)?;
            check_sent_name(&self.name)?;
            if let Some(id) = &self.id {
                check_key("message id", id)?;
            }
            let now = self.sub.now();
            // § wtwx1e769@zy8s5
            let ord = self.sub.ulid(now.ms);
            let send = b::SendPlan {
                prefix: String::new(),
                chain: self.chain.clone(),
                name: self.name,
                // § one h8wg39cct.a@ncvyy
                value: Some(json(self.value?)?),
                // § one wtwx1e769.a@0yr00
                id: self.id.unwrap_or_else(|| ord.clone()),
                now: Some(now),
                ord,
            };
            // § end wtwx1e769
            sent(&self.sub, &self.chain, Plan::Send(send)).await
        })
    }
}

/// A wait for a chain's result, its head polled until it ended, held on a waiting listing of the
/// head on a store that can wait; `timeout` bounds it with `WaitTimeout`.
#[must_use = "a result does nothing until awaited"]
pub struct ResultWait<T> {
    run: Run<T>,
    timeout: Option<Duration>,
    max_interval: Option<Duration>,
}

impl<T> ResultWait<T> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The longest wait between two polls, which double their interval up to it, 30 s unless
    /// given; positive. No effect on a store whose listings wait, nor in a test environment,
    /// where nothing polls.
    pub fn max_interval(mut self, max_interval: Duration) -> Self {
        self.max_interval = Some(max_interval);
        self
    }
}

// § y025f8qen@kkdg6
const FIRST_POLL: Duration = Duration::from_millis(10);
const MAX_POLL: Duration = Duration::from_secs(30);
// § end y025f8qen

impl<T: DeserializeOwned + Send + 'static> IntoFuture for ResultWait<T> {
    type Output = Result<T>;
    type IntoFuture = BoxFuture<'static, Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        let cap = match self.max_interval {
            None => MAX_POLL,
            Some(cap) if !cap.is_zero() => cap,
            Some(zero) => {
                let refused = Error::InvalidArgument(format!(
                    "max_interval is a positive duration, not {zero:?}"
                ));
                return Box::pin(async { Err(refused) });
            }
        };
        #[cfg(feature = "testing")]
        if let Some(env) = self.run.sub.env().cloned() {
            return Box::pin(settled(self.run, env, self.timeout));
        }
        Box::pin(async move {
            let mut died = self.run.sub.0.died.subscribe();
            // § y025f8qen.b@gc5pb
            let store = &*self.run.sub.0.store;
            let polled = async {
                // § y025f8qen@jp635
                let (mut delay, mut last, mut etag) = (FIRST_POLL.min(cap), None, None);
                loop {
                    let live = match self.run.ended(last.take()).await? {
                        Ended::Value(value) => return Ok(value),
                        Ended::Live(live) => live,
                    };
                    last = Some(live.run);
                    if store.capabilities().wait {
                        // The first listing waits for nothing: it takes the etag the next one waits
                        // on, and the read after it sees any commit that landed before it. A wait
                        // that ends on the same etag saw no write of the head: no read.
                        // § y025f8qen.e@1esnd
                        loop {
                            let changed =
                                h3::listing_changed(store, &live.result_key, etag.clone());
                            let changed =
                                changed.await.map_err(|e| Error::StoreError(e.message))?;
                            let same = etag.as_ref() == Some(&changed);
                            etag = Some(changed);
                            if !same {
                                break;
                            }
                        }
                    } else {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(cap);
                    }
                }
            };
            // A worker of this handle that dies while the wait is in progress fails it at once.
            let poll = async {
                tokio::select! {
                    ended = polled => return ended,
                    Ok(()) = died.changed() => {}
                }
                let error = died.borrow().clone().expect("only a death changes it");
                Err(error.again())
            };
            match self.timeout {
                Some(t) => tokio::time::timeout(t, poll)
                    .await
                    .map_err(|_| Error::WaitTimeout)?,
                None => poll.await,
            }
        })
    }
}

/// In a test environment: settles, then the value the chain ended with; `Stuck` naming what it
/// waits on if it has not ended, `WaitTimeout` instead when a timeout is given. The clock does
/// not move.
#[cfg(feature = "testing")]
async fn settled<T: DeserializeOwned>(
    run: Run<T>,
    env: crate::testing::Attached,
    timeout: Option<Duration>,
) -> Result<T> {
    env.settle().await?;
    match run.ended(None).await {
        Ok(Ended::Value(value)) => Ok(value),
        Ok(Ended::Live(_)) if timeout.is_some() => Err(Error::WaitTimeout),
        Ok(Ended::Live(live)) => Err(env.stuck(&run.chain, &live.run)),
        Err(e) => {
            if let Error::RunFailed { status, .. } = &e {
                env.read(&run.chain, *status);
            }
            Err(e)
        }
    }
}

/// How a refused send's run ended: never continued, since a continued run forwards the message.
fn ended(s: Status) -> Result<RunStatus> {
    match s {
        Status::Completed => Ok(RunStatus::Completed),
        Status::Failed => Ok(RunStatus::Failed),
        Status::Cancelled => Ok(RunStatus::Cancelled),
        other => Err(Error::Protocol(format!("send refused by a run {other:?}"))),
    }
}
