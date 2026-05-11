use std::fmt;

use crate::meta::{MetaError, MetaErrorKind};

pub mod iceberg_catalog;
pub mod id_scopes;
pub mod job;
pub mod managed_lake;
pub mod managed_txn;
pub mod mv;

pub use crate::meta::payload::{decode_json_payload, encode_json_payload};

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    Conflict,
    NotFound,
    InvalidRequest,
    Provider,
}

#[derive(Debug)]
pub struct RepositoryError {
    kind: RepositoryErrorKind,
    message: String,
}

impl RepositoryError {
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Conflict, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::NotFound, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::InvalidRequest, message)
    }

    pub fn provider(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Provider, message)
    }

    pub fn kind(&self) -> RepositoryErrorKind {
        self.kind
    }

    fn new(kind: RepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.kind {
            RepositoryErrorKind::Conflict => "conflict",
            RepositoryErrorKind::NotFound => "not found",
            RepositoryErrorKind::InvalidRequest => "invalid request",
            RepositoryErrorKind::Provider => "provider error",
        };
        write!(f, "metadata repository {label}: {}", self.message)
    }
}

impl std::error::Error for RepositoryError {}

impl From<MetaError> for RepositoryError {
    fn from(err: MetaError) -> Self {
        match err.kind() {
            MetaErrorKind::Conflict | MetaErrorKind::AlreadyExists => {
                Self::conflict(err.to_string())
            }
            MetaErrorKind::NotFound => Self::not_found(err.to_string()),
            MetaErrorKind::InvalidRequest | MetaErrorKind::Unsupported => {
                Self::invalid(err.to_string())
            }
            MetaErrorKind::Transient
            | MetaErrorKind::DefiniteCommitFailure
            | MetaErrorKind::CommitUnknown
            | MetaErrorKind::ProviderCorruption => Self::provider(err.to_string()),
        }
    }
}
