use std::future::IntoFuture;
use std::marker::PhantomData;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Substantial;
use crate::error::{Error, Failure, Result, run_failed_cause};
use crate::names::{check_key, check_name};
use crate::plan;
use crate::proto::boundary::v1::{
    self as b, plan_input::Plan, plan_step::Step, read_value, run_result, run_state, sent,
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
            b::RunStatus::Unspecified => {
                return Err(Error::Protocol("unspecified run status".into()));
            }
        })
    }
}

/// A handle on a chain whose result is an `O`: `send`, `result`, `status` and `cancel` act on
/// its last run.
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
            value: serde_json::to_vec(&value).map_err(Error::from),
            id: None,
        }
    }

    /// Cancels the run: it raises `Cancelled` at every call it is blocked on.
    pub async fn cancel(&self, reason: &str) -> Result<()> {
        self.check_chain()?;
        let now = self.sub.now();
        let ord = self.sub.ulid(now.ms);
        let cancel = b::CancelPlan {
            prefix: String::new(),
            partitions: self.sub.partitions(),
            chain: self.chain.clone(),
            reason: reason.to_owned(),
            id: ord.clone(),
            now: Some(now),
            ord,
        };
        self.sent(Plan::Cancel(cancel)).await
    }

    async fn sent(&self, plan: Plan) -> Result<()> {
        sent(&self.sub, &self.chain, plan).await
    }

    fn check_chain(&self) -> Result<()> {
        check_key("run id", &self.chain)
    }

    fn not_found(&self, no_chain: b::NoChain) -> Error {
        not_found(&self.chain, no_chain)
    }

    pub async fn status(&self) -> Result<RunStatus> {
        self.check_chain()?;
        let status = b::StatusPlan {
            prefix: String::new(),
            partitions: self.sub.partitions(),
            chain: self.chain.clone(),
            now: Some(self.sub.now()),
        };
        let status = match self.sub.plan(Plan::Status(status)).await? {
            Step::Status(b::RunState { state: Some(state) }) => match state {
                run_state::State::Status(of) => of.status().try_into(),
                run_state::State::NoChain(n) => Err(self.not_found(n)),
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

    /// The last value published under `key` by any run of the chain, `None` before the first.
    pub async fn read<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.check_chain()?;
        check_key("published key", key)?;
        let read = b::ReadPlan {
            prefix: String::new(),
            partitions: self.sub.partitions(),
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
            refollow: REFOLLOW,
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
            partitions: self.sub.partitions(),
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
                run_result::Result::NoChain(n) => return Err(self.not_found(n)),
                run_result::Result::Failed(f) => return Err(Error::StoreError(f.message)),
            },
            other => return Err(plan::unexpected("result", other)),
        };
        let status = result.status();
        let failed = |status, error, cause| {
            Err(Error::RunFailed {
                run: run.clone(),
                status,
                error,
                cause,
            })
        };
        match status {
            Status::Completed => decode(result.value).map(Ended::Value),
            Status::Failed => {
                let mut error = result
                    .error
                    .ok_or_else(|| Error::Protocol("failed without an error".into()))?;
                let details = error.details.take().map(|v| decode(Some(v))).transpose()?;
                let cause = run_failed_cause(&error.r#type, details)?;
                failed(RunStatus::Failed, error.into(), cause)
            }
            Status::Cancelled => {
                let error = Failure {
                    kind: "Cancelled".into(),
                    message: result.reason,
                    retryable: false,
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
    /// The run to poll next, and its `result` key to wait on.
    Live(run_result::NotEnded),
}

fn not_found(chain: &str, no_chain: b::NoChain) -> Error {
    let until = no_chain.collected_until.map(|t| super::datetime(&t));
    Error::RunNotFound {
        chain: chain.to_owned(),
        collected: until.is_some(),
        until,
    }
}

async fn sent(sub: &Substantial, chain: &str, plan: Plan) -> Result<()> {
    match sub.plan(plan).await? {
        Step::Sent(b::Sent {
            sent: Some(outcome),
        }) => match outcome {
            sent::Sent::Accepted(_) => {
                sub.wake_workers();
                Ok(())
            }
            sent::Sent::Refused(r) => Err(Error::RunEnded {
                status: ended(r.status())?,
                run: r.run,
            }),
            sent::Sent::NoChain(n) => Err(not_found(chain, n)),
            sent::Sent::Failed(f) => Err(Error::StoreError(f.message)),
        },
        other => Err(plan::unexpected("send", other)),
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
            check_name("message name", &self.name)?;
            if let Some(id) = &self.id {
                check_key("message id", id)?;
            }
            let now = self.sub.now();
            let ord = self.sub.ulid(now.ms);
            let send = b::SendPlan {
                prefix: String::new(),
                partitions: self.sub.partitions(),
                chain: self.chain.clone(),
                name: self.name,
                value: Some(json(self.value?)?),
                id: self.id.unwrap_or_else(|| ord.clone()),
                now: Some(now),
                ord,
            };
            sent(&self.sub, &self.chain, Plan::Send(send)).await
        })
    }
}

/// A wait for a run's result, polled until it exists, held on a waiting listing of it on a store
/// that can wait; `timeout` bounds it with `WaitTimeout`.
#[must_use = "a result does nothing until awaited"]
pub struct ResultWait<T> {
    run: Run<T>,
    timeout: Option<Duration>,
    refollow: Duration,
}

impl<T> ResultWait<T> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub(crate) fn refollow(mut self, refollow: Duration) -> Self {
        self.refollow = refollow;
        self
    }
}

const FIRST_POLL: Duration = Duration::from_millis(10);
const MAX_POLL: Duration = Duration::from_secs(30);
/// A wait follows its chain from the first run again this often, as polling one run's `result`
/// never sees that run collected (I-17).
const REFOLLOW: Duration = Duration::from_secs(crate::settings::RETENTION_MIN.as_secs() / 2);

impl<T: DeserializeOwned + Send + 'static> IntoFuture for ResultWait<T> {
    type Output = Result<T>;
    type IntoFuture = BoxFuture<'static, Result<T>>;

    fn into_future(self) -> Self::IntoFuture {
        #[cfg(feature = "testing")]
        if let Some(env) = self.run.sub.env().cloned() {
            return Box::pin(settled(self.run, env, self.timeout));
        }
        Box::pin(async move {
            let mut died = self.run.sub.0.died.subscribe();
            let store = &*self.run.sub.0.store;
            let polled = async {
                let (mut delay, mut last) = (FIRST_POLL, None);
                let mut followed = tokio::time::Instant::now();
                loop {
                    if followed.elapsed() >= self.refollow {
                        (last, followed) = (None, tokio::time::Instant::now());
                    }
                    let live = match self.run.ended(last.take()).await? {
                        Ended::Value(value) => return Ok(value),
                        Ended::Live(live) => live,
                    };
                    last = Some(live.run);
                    if store.capabilities().wait {
                        let created = h3::result_created(store, &live.result_key);
                        let left = self.refollow.saturating_sub(followed.elapsed());
                        if let Ok(created) = tokio::time::timeout(left, created).await {
                            created.map_err(|e| Error::StoreError(e.message))?;
                        }
                    } else {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(MAX_POLL);
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
