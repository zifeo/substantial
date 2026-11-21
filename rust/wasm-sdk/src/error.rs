use std::cell::RefCell;
use std::fmt;

use crate::__bindings::exports::substantial::wasm::workflow::End;
use crate::__bindings::substantial::wasm::context as wit;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A failure, with the fields of every host's.
#[derive(Clone, Debug, PartialEq)]
pub struct Failure {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
    pub details: Option<serde_json::Value>,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

/// An error a verb raised, of a kind without a variant of its own: its failure as the host gave it.
#[derive(Clone, Debug, PartialEq)]
pub struct Raised(Failure);

impl Raised {
    pub fn failure(&self) -> &Failure {
        &self.0
    }
}

/// The errors of a Wasm workflow's code, named as the Rust host's. One a verb raised keeps the
/// failure the host gave, so that returning it ends the run as the host's own code would.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A step out of attempts, or failed by an error that is not retryable.
    StepFailed {
        error: Failure,
        cause: Box<Failure>,
    },
    /// The run is cancelled, or the code cancels it with `c.cancel(..)`.
    Cancelled(String),
    /// A `receive` whose timeout passed with no message.
    WaitTimeout,
    NonDeterminism(String),
    InvalidArgument(String),
    /// A value of the code that does not encode or decode.
    Json(String),
    /// A failure of the code's own, which fails the run with it.
    Failed(Failure),
    /// Any other error a verb raised.
    Raised(Raised),
}

impl Error {
    /// The failure type the run ends with when the code returns this error.
    pub fn kind(&self) -> &str {
        match self {
            Error::StepFailed { .. } => "StepFailed",
            Error::Cancelled(_) => "Cancelled",
            Error::Json(_) => "Json",
            Error::Failed(f) => &f.kind,
            Error::WaitTimeout => "WaitTimeout",
            Error::NonDeterminism(_) => "NonDeterminism",
            Error::InvalidArgument(_) => "InvalidArgument",
            Error::Raised(r) => &r.0.kind,
        }
    }

    pub(crate) fn end(self) -> End {
        match self {
            Error::Cancelled(reason) => End::Cancelled(reason),
            Error::StepFailed { error, .. } => End::Failed(error.into()),
            Error::Json(message) => End::Failed(wit::Failure {
                kind: "Json".into(),
                message,
                retryable: false,
                details: None,
            }),
            Error::Failed(f) => End::Failed(f.into()),
            Error::WaitTimeout => End::Failed(raised_as("WaitTimeout", None).into()),
            Error::NonDeterminism(m) => End::Failed(raised_as("NonDeterminism", Some(m)).into()),
            Error::InvalidArgument(m) => End::Failed(raised_as("InvalidArgument", Some(m)).into()),
            Error::Raised(r) => End::Failed(r.0.into()),
        }
    }
}

thread_local! {
    /// The failures the verbs raised of a kind whose variant holds less than its failure, so that
    /// the code returning one ends its run with the failure the host gave, which it matches.
    static RAISED: RefCell<Vec<Failure>> = const { RefCell::new(Vec::new()) };
}

/// The failure the host gave of `kind` with `message`, the last one, else a failure of the code's.
fn raised_as(kind: &str, message: Option<String>) -> Failure {
    let found = RAISED.with(|r| {
        (r.borrow().iter().rev())
            .find(|f| f.kind == kind && message.as_ref().is_none_or(|m| *m == f.message))
            .cloned()
    });
    found.unwrap_or_else(|| Failure {
        kind: kind.to_owned(),
        message: message.unwrap_or_else(|| kind.to_owned()),
        retryable: false,
        details: None,
    })
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::StepFailed { error, cause } => write!(f, "{error} (cause: {cause})"),
            Error::Cancelled(reason) => write!(f, "Cancelled: {reason}"),
            Error::Json(message) => write!(f, "Json: {message}"),
            Error::Failed(failure) => failure.fmt(f),
            Error::WaitTimeout => write!(f, "WaitTimeout"),
            Error::NonDeterminism(m) => write!(f, "NonDeterminism: {m}"),
            Error::InvalidArgument(m) => write!(f, "InvalidArgument: {m}"),
            Error::Raised(r) => r.0.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e.to_string())
    }
}

impl From<wit::Raised> for Error {
    fn from(r: wit::Raised) -> Self {
        let failure = Failure::from(r.failure);
        match (failure.kind.as_str(), r.cause) {
            ("StepFailed", Some(cause)) => Error::StepFailed {
                error: failure,
                cause: Box::new(cause.into()),
            },
            ("Cancelled", _) => Error::Cancelled(failure.message),
            (kind @ ("WaitTimeout" | "NonDeterminism" | "InvalidArgument"), _) => {
                let message = failure.message.clone();
                let error = match kind {
                    "WaitTimeout" => Error::WaitTimeout,
                    "NonDeterminism" => Error::NonDeterminism(message),
                    _ => Error::InvalidArgument(message),
                };
                RAISED.with(|r| r.borrow_mut().push(failure));
                error
            }
            _ => Error::Raised(Raised(failure)),
        }
    }
}

impl From<wit::Failure> for Failure {
    fn from(f: wit::Failure) -> Self {
        Failure {
            kind: f.kind,
            message: f.message,
            retryable: f.retryable,
            // The host gives details as JSON it wrote itself.
            details: f
                .details
                .map(|d| serde_json::from_str(&d).expect("the host gives details as JSON")),
        }
    }
}

impl From<Failure> for wit::Failure {
    fn from(f: Failure) -> Self {
        wit::Failure {
            kind: f.kind,
            message: f.message,
            retryable: f.retryable,
            details: f.details.map(|d| d.to_string()),
        }
    }
}

/// An error that fails a step at once, attempts left or not.
#[derive(Debug)]
pub struct Permanent(pub Box<dyn std::error::Error + Send + Sync>);

impl Permanent {
    pub fn new(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(e.into())
    }
}

impl fmt::Display for Permanent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Permanent {}
