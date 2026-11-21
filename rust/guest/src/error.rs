use std::fmt;

use crate::__bindings::exports::substantial::guest::workflow::End;
use crate::__bindings::substantial::guest::context as wit;

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

/// The errors of a guest's workflow code, named as the Rust host's. One a verb raised keeps the
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
    WaitTimeout(Raised),
    NonDeterminism(Raised),
    InvalidArgument(Raised),
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
            Error::WaitTimeout(r)
            | Error::NonDeterminism(r)
            | Error::InvalidArgument(r)
            | Error::Raised(r) => &r.0.kind,
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
            Error::WaitTimeout(r)
            | Error::NonDeterminism(r)
            | Error::InvalidArgument(r)
            | Error::Raised(r) => End::Failed(r.0.into()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::StepFailed { error, cause } => write!(f, "{error} (cause: {cause})"),
            Error::Cancelled(reason) => write!(f, "Cancelled: {reason}"),
            Error::Json(message) => write!(f, "Json: {message}"),
            Error::Failed(failure) => failure.fmt(f),
            Error::WaitTimeout(r)
            | Error::NonDeterminism(r)
            | Error::InvalidArgument(r)
            | Error::Raised(r) => r.0.fmt(f),
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
            ("WaitTimeout", _) => Error::WaitTimeout(Raised(failure)),
            ("NonDeterminism", _) => Error::NonDeterminism(Raised(failure)),
            ("InvalidArgument", _) => Error::InvalidArgument(Raised(failure)),
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
