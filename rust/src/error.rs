use crate::proto::boundary::v1::{CoreError, ErrorKind};
use crate::proto::layout::v1 as layout;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The hint a divergence from the history carries; the core records the failure without it.
pub(crate) const NEW_VERSION: &str = "an incompatible change needs a new version (version=)";

fn rfc3339(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn not_found_until(until: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    match until {
        Some(until) => format!(
            ": it was collected, and its id is taken until {}",
            rfc3339(until)
        ),
        None => String::new(),
    }
}

fn taken_by(until: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    match until {
        Some(until) => format!("a run collected, until {}", rfc3339(until)),
        None => "another workflow, version or input".into(),
    }
}

fn version_hint(error: &Failure) -> String {
    if error.kind == "NonDeterminism" {
        format!("; {NEW_VERSION}")
    } else {
        String::new()
    }
}

/// A failure as recorded in a history: a type name, a message, and whether a retry may help.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl From<layout::Failure> for Failure {
    fn from(f: layout::Failure) -> Self {
        Self {
            kind: f.r#type,
            message: f.message,
            retryable: f.retryable,
        }
    }
}

impl From<Failure> for layout::Failure {
    fn from(f: Failure) -> Self {
        Self {
            r#type: f.kind,
            message: f.message,
            retryable: f.retryable,
            details: None,
        }
    }
}

/// The errors of I-20 the host raises so far, and the host's own failures around the core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A step had no attempt left, or failed with a permanent error: `error` names the step,
    /// `cause` is its last attempt's failure, such as `StepTimeout` or `ValueTooLarge`.
    #[error("{}", .error.message)]
    StepFailed { error: Failure, cause: Failure },
    /// A call resolved with an error other than a step's, or a workflow's own failure:
    /// [`Error::failed`], or an `anyhow::Error` through `?`.
    #[error("{0}")]
    Failed(Failure),
    #[error("cancelled: {0}")]
    Cancelled(String),
    /// The undos of a rollback ran; the run ends failed with this reason.
    #[error("rolled back: {0}")]
    RolledBack(String),
    /// An undo had no attempt left; `step` is the undo's step, `<step>.undo`.
    #[error("undo {step} failed: {error}")]
    UndoFailed { step: String, error: Failure },
    /// Control flow of `continue_as_new` and `iterate`: returned from the workflow, never handled.
    #[error("continued as new")]
    ContinuedAsNew(Continuation),
    #[error("non-determinism: {0}")]
    NonDeterminism(String),
    #[error("run {run} has ended, {status}")]
    RunEnded {
        run: String,
        status: crate::RunStatus,
    },
    /// A name or id outside the naming rules (I-17), or an input that is not an object.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// `collected` when the chain's first run was collected, its id taken `until` then.
    #[error("no chain {chain}{}", not_found_until(.until))]
    RunNotFound {
        chain: String,
        collected: bool,
        until: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// `collected` when the id is taken by a run collected, `until` then.
    #[error("id {id} is taken by {}", taken_by(.until))]
    RunExists {
        id: String,
        collected: bool,
        until: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// The run ended failed or cancelled. `cause` is the `StepFailed` cause of the step that
    /// failed it, else `None`; boxed, so that every `Result` stays small.
    #[error("run {run} {status:?}: {error}{}", version_hint(.error))]
    RunFailed {
        run: String,
        status: crate::client::RunStatus,
        error: Failure,
        cause: Option<Box<Failure>>,
    },
    #[error("the run did not end within the timeout")]
    WaitTimeout,
    /// In a test environment: a run waits on something the environment cannot bring, or a
    /// settle cannot end.
    #[error("{0}")]
    Stuck(String),
    /// In a test environment: runs ended failed without the test reading their failure.
    #[error("{0}")]
    Unchecked(String),
    /// A value whose JSON encoding is over the limit of 64 MiB: a step result, an input or a
    /// result, a message, a published value, a loop state.
    #[error("a value of {size} bytes is over the limit of {limit} bytes")]
    ValueTooLarge { size: usize, limit: usize },
    /// A step declared with `#[step]` was called outside a workflow.
    #[error("a declared step was called outside a workflow")]
    NotInWorkflow,
    #[error("store error: {0}")]
    StoreError(String),
    #[error("layout error: {0}")]
    LayoutError(String),
    #[error("config error: {0}")]
    ConfigError(String),
    /// A core error other than non-determinism and layout: `INVALID`, `ENDED`, `CORRUPT`.
    #[error("core error {kind:?}: {message}")]
    Core { kind: ErrorKind, message: String },
    /// The core trapped: a bug of the core. Its instance is dropped with every machine in it.
    #[error("core trapped: {0}")]
    Trap(String),
    /// The module could not be loaded: checksum, compilation, missing export.
    #[error("module: {0}")]
    Module(String),
    /// The core answered with a response this host does not expect at that point.
    #[error("unexpected core response: {0}")]
    Protocol(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The host's own I/O outside a store, such as listening for signals.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// The same error, for each wait a dead worker fails; an I/O or JSON error keeps its message.
    pub(crate) fn again(&self) -> Error {
        match self {
            Error::StepFailed { error, cause } => Error::StepFailed {
                error: error.clone(),
                cause: cause.clone(),
            },
            Error::Failed(f) => Error::Failed(f.clone()),
            Error::Cancelled(r) => Error::Cancelled(r.clone()),
            Error::RolledBack(r) => Error::RolledBack(r.clone()),
            Error::UndoFailed { step, error } => Error::UndoFailed {
                step: step.clone(),
                error: error.clone(),
            },
            Error::ContinuedAsNew(c) => Error::ContinuedAsNew(Continuation {
                workflow: c.workflow.clone(),
                version: c.version,
                input: c.input.clone(),
                state: c.state.clone(),
            }),
            Error::NonDeterminism(m) => Error::NonDeterminism(m.clone()),
            Error::RunEnded { run, status } => Error::RunEnded {
                run: run.clone(),
                status: *status,
            },
            Error::InvalidArgument(m) => Error::InvalidArgument(m.clone()),
            Error::RunNotFound {
                chain,
                collected,
                until,
            } => Error::RunNotFound {
                chain: chain.clone(),
                collected: *collected,
                until: *until,
            },
            Error::RunExists {
                id,
                collected,
                until,
            } => Error::RunExists {
                id: id.clone(),
                collected: *collected,
                until: *until,
            },
            Error::RunFailed {
                run,
                status,
                error,
                cause,
            } => Error::RunFailed {
                run: run.clone(),
                status: *status,
                error: error.clone(),
                cause: cause.clone(),
            },
            Error::WaitTimeout => Error::WaitTimeout,
            Error::Stuck(m) => Error::Stuck(m.clone()),
            Error::Unchecked(m) => Error::Unchecked(m.clone()),
            Error::ValueTooLarge { size, limit } => Error::ValueTooLarge {
                size: *size,
                limit: *limit,
            },
            Error::NotInWorkflow => Error::NotInWorkflow,
            Error::StoreError(m) => Error::StoreError(m.clone()),
            Error::LayoutError(m) => Error::LayoutError(m.clone()),
            Error::ConfigError(m) => Error::ConfigError(m.clone()),
            Error::Core { kind, message } => Error::Core {
                kind: *kind,
                message: message.clone(),
            },
            Error::Trap(m) => Error::Trap(m.clone()),
            Error::Module(m) => Error::Module(m.clone()),
            Error::Protocol(m) => Error::Protocol(m.clone()),
            Error::Json(e) => Error::Protocol(format!("json: {e}")),
            Error::Io(e) => Error::Io(std::io::Error::new(e.kind(), e.to_string())),
        }
    }

    /// A failure of the workflow's own: the run ends failed with `kind` as its error type.
    pub fn failed(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Error::Failed(Failure {
            kind: kind.into(),
            message: message.into(),
            retryable: false,
        })
    }
}

/// An `anyhow::Error` holding an [`Error`] gives it back, so that control flow survives `?`;
/// any other fails the run as `Error` with its message and causes.
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        match e.downcast::<Error>() {
            Ok(e) => e,
            Err(e) => Error::failed("Error", format!("{e:#}")),
        }
    }
}

impl From<CoreError> for Error {
    fn from(e: CoreError) -> Self {
        match e.kind() {
            ErrorKind::NonDeterminism => Error::NonDeterminism(e.message),
            ErrorKind::Layout => Error::LayoutError(e.message),
            ErrorKind::ValueTooLarge => value_too_large(&e.message),
            kind => Error::Core {
                kind,
                message: e.message,
            },
        }
    }
}

/// What the next run of the chain starts with.
#[derive(Debug)]
pub struct Continuation {
    pub(crate) workflow: Option<String>,
    /// The version of `workflow`, given only with it.
    pub(crate) version: Option<u32>,
    pub(crate) input: Vec<u8>,
    pub(crate) state: Option<Vec<u8>>,
}

/// `<what> is <size> bytes, over the limit of <limit> bytes`, as the core refuses a value.
fn value_too_large(message: &str) -> Error {
    let parsed = message
        .strip_suffix(" bytes")
        .and_then(|m| m.split_once(" bytes, over the limit of "))
        .and_then(|(head, limit)| Some((head.rsplit_once(" is ")?.1, limit)))
        .and_then(|(size, limit)| Some((size.parse().ok()?, limit.parse().ok()?)));
    match parsed {
        Some((size, limit)) => Error::ValueTooLarge { size, limit },
        None => Error::Protocol(format!("ValueTooLarge as {message:?}")),
    }
}

impl From<layout::Failure> for Error {
    fn from(f: layout::Failure) -> Self {
        match f.r#type.as_str() {
            "RunFailed" => run_failed(f),
            "StepFailed" => step_failed(f),
            "RunNotFound" => match details::<RunDetails>(&f) {
                Ok(d) => match d.until.as_deref().map(chrono::DateTime::parse_from_rfc3339) {
                    None => Error::RunNotFound {
                        chain: d.run,
                        collected: false,
                        until: None,
                    },
                    Some(Ok(until)) => Error::RunNotFound {
                        chain: d.run,
                        collected: true,
                        until: Some(until.to_utc()),
                    },
                    Some(Err(e)) => Error::Protocol(format!("RunNotFound details: {e}")),
                },
                Err(e) => e,
            },
            "RunEnded" => match details::<EndedDetails>(&f) {
                Ok(d) => Error::RunEnded {
                    run: d.run,
                    status: d.status,
                },
                Err(e) => e,
            },
            _ => Error::Failed(f.into()),
        }
    }
}

fn details<T: serde::de::DeserializeOwned>(f: &layout::Failure) -> Result<T> {
    match f.details.as_ref().and_then(|v| v.kind.as_ref()) {
        Some(layout::value::Kind::Json(bytes)) => serde_json::from_slice(bytes)
            .map_err(|e| Error::Protocol(format!("{} details: {e}", f.r#type))),
        other => Err(Error::Protocol(format!(
            "{} with details {other:?}",
            f.r#type
        ))),
    }
}

/// The details of a workflow's send refused: the chain it sent to, and until when its id is taken
/// when it was collected.
#[derive(serde::Deserialize)]
struct RunDetails {
    run: String,
    until: Option<String>,
}

#[derive(serde::Deserialize)]
struct EndedDetails {
    run: String,
    status: crate::RunStatus,
}

/// The details of a `StepFailed`: the last attempt's failure.
#[derive(serde::Deserialize)]
struct StepFailedDetails {
    error: FailureDetails,
}

fn step_failed(f: layout::Failure) -> Error {
    match details::<StepFailedDetails>(&f) {
        Ok(d) => Error::StepFailed {
            error: f.into(),
            cause: d.error.into(),
        },
        Err(e) => e,
    }
}

/// The details of a child's `RunFailed`, as the core resolves its result.
#[derive(serde::Deserialize)]
struct RunFailedDetails {
    run: String,
    status: String,
    error: Option<FailureDetails>,
    reason: Option<String>,
}

#[derive(serde::Deserialize)]
struct FailureDetails {
    r#type: String,
    message: String,
    retryable: bool,
    details: Option<serde_json::Value>,
}

impl From<FailureDetails> for Failure {
    fn from(d: FailureDetails) -> Self {
        Self {
            kind: d.r#type,
            message: d.message,
            retryable: d.retryable,
        }
    }
}

fn run_failed(f: layout::Failure) -> Error {
    let d = match details::<RunFailedDetails>(&f) {
        Ok(d) => d,
        Err(e) => return e,
    };
    let (status, error, cause) = match (d.status.as_str(), d.error, d.reason) {
        ("failed", Some(mut e), _) => match run_failed_cause(&e.r#type, e.details.take()) {
            Ok(cause) => (crate::client::RunStatus::Failed, e.into(), cause),
            Err(e) => return e,
        },
        ("cancelled", _, Some(reason)) => (
            crate::client::RunStatus::Cancelled,
            Failure {
                kind: "Cancelled".into(),
                message: reason,
                retryable: false,
            },
            None,
        ),
        (status, ..) => return Error::Protocol(format!("RunFailed of {} as {status}", d.run)),
    };
    Error::RunFailed {
        run: d.run,
        status,
        error,
        cause,
    }
}

/// The cause of a run failed with `kind`: a `StepFailed`'s wraps it in its details.
pub(crate) fn run_failed_cause(
    kind: &str,
    details: Option<serde_json::Value>,
) -> Result<Option<Box<Failure>>> {
    if kind != "StepFailed" {
        return Ok(None);
    }
    let shown = format!("{details:?}");
    let parsed = details.map(serde_json::from_value::<StepFailedDetails>);
    match parsed {
        Some(Ok(d)) => Ok(Some(Box::new(d.error.into()))),
        _ => Err(Error::Protocol(format!(
            "a run failed with StepFailed with details {shown}"
        ))),
    }
}

/// A `StepFailed`'s failure as a run ends with it, its cause in its details as the core writes
/// them, for a client's `RunFailed` to read.
pub(crate) fn step_failed_end(error: &Failure, cause: &Failure) -> layout::Failure {
    let details = serde_json::json!({
        "error": { "type": cause.kind, "message": cause.message, "retryable": cause.retryable }
    });
    layout::Failure {
        details: Some(layout::Value {
            kind: Some(layout::value::Kind::Json(details.to_string().into_bytes())),
        }),
        ..error.clone().into()
    }
}
