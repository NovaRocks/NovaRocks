use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskIoOperation {
    ExchangeTransmit,
    ResultOpen,
    ResultWrite,
    ResultFinish,
    Lookup,
}

impl fmt::Display for TaskIoOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::ExchangeTransmit => "exchange transmit",
            Self::ResultOpen => "result open",
            Self::ResultWrite => "result write",
            Self::ResultFinish => "result finish",
            Self::Lookup => "lookup",
        };
        formatter.write_str(label)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskIoErrorKind {
    Unavailable,
    Timeout,
    RemoteRejected,
    InvalidResponse,
    Cancelled,
    Internal,
}

impl fmt::Display for TaskIoErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::RemoteRejected => "remote rejected",
            Self::InvalidResponse => "invalid response",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        };
        formatter.write_str(label)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskIoError {
    operation: TaskIoOperation,
    kind: TaskIoErrorKind,
    message: String,
    source: Option<crate::runtime::task::SharedDomainFailure>,
}

impl TaskIoError {
    pub fn new(
        operation: TaskIoOperation,
        kind: TaskIoErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            operation,
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Preserve a known producer's original source and neutral fact. The IO
    /// operation/kind remain transport context and cannot reclassify it.
    pub fn from_domain<E>(operation: TaskIoOperation, kind: TaskIoErrorKind, cause: E) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source = crate::runtime::task::SharedDomainFailure::new(cause);
        Self {
            operation,
            kind,
            message: novarocks_failure_contract::BoundedDetail::truncating_display(&source)
                .into_string(),
            source: Some(source),
        }
    }

    pub const fn operation(&self) -> TaskIoOperation {
        self.operation
    }

    pub const fn kind(&self) -> TaskIoErrorKind {
        self.kind
    }
}

impl fmt::Display for TaskIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {}: {}",
            self.operation, self.kind, self.message
        )
    }
}

impl std::error::Error for TaskIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl novarocks_failure_contract::FailureProjection for TaskIoError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        if let Some(source) = &self.source {
            return source.failure_fact();
        }
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let namespace = match self.operation {
            TaskIoOperation::ExchangeTransmit => domain::EXCHANGE,
            TaskIoOperation::ResultOpen
            | TaskIoOperation::ResultWrite
            | TaskIoOperation::ResultFinish => domain::RESULT,
            TaskIoOperation::Lookup => domain::EXECUTION,
        };
        let (class, code) = match self.kind {
            TaskIoErrorKind::Unavailable => (
                FailureClass::Dependency,
                ExecutionReason::FragmentIoUnavailable,
            ),
            TaskIoErrorKind::Timeout => (FailureClass::Control, ExecutionReason::FragmentIoTimeout),
            TaskIoErrorKind::RemoteRejected => (
                FailureClass::Dependency,
                ExecutionReason::FragmentIoRemoteRejected,
            ),
            TaskIoErrorKind::InvalidResponse => (
                FailureClass::Invariant,
                ExecutionReason::FragmentIoInvalidResponse,
            ),
            TaskIoErrorKind::Cancelled => {
                (FailureClass::Control, ExecutionReason::FragmentIoCancelled)
            }
            TaskIoErrorKind::Internal => (
                FailureClass::Unexpected,
                ExecutionReason::FragmentIoInternal,
            ),
        };
        FailureFact::new(class, FailureReason::new(namespace, code.code()))
    }
}
impl From<TaskIoError> for crate::runtime::task::ExecutionFailure {
    fn from(error: TaskIoError) -> Self {
        Self::from_domain(error)
    }
}
