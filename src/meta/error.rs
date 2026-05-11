use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetaErrorKind {
    Conflict,
    NotFound,
    AlreadyExists,
    InvalidRequest,
    Unsupported,
    Transient,
    DefiniteCommitFailure,
    CommitUnknown,
    ProviderCorruption,
}

#[derive(Debug)]
pub struct MetaError {
    kind: MetaErrorKind,
    message: String,
}

impl MetaError {
    pub fn new(kind: MetaErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> MetaErrorKind {
        self.kind
    }
}

impl fmt::Display for MetaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for MetaError {}
