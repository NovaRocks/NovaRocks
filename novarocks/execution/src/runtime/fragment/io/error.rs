use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentIoOperation {
    ExchangeTransmit,
    ResultOpen,
    ResultWrite,
    ResultFinish,
    Lookup,
}

impl fmt::Display for FragmentIoOperation {
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
pub enum FragmentIoErrorKind {
    Unavailable,
    Timeout,
    RemoteRejected,
    InvalidResponse,
    Cancelled,
    Internal,
}

impl fmt::Display for FragmentIoErrorKind {
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
pub struct FragmentIoError {
    operation: FragmentIoOperation,
    kind: FragmentIoErrorKind,
    message: String,
}

impl FragmentIoError {
    pub fn new(
        operation: FragmentIoOperation,
        kind: FragmentIoErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            operation,
            kind,
            message: message.into(),
        }
    }

    pub const fn operation(&self) -> FragmentIoOperation {
        self.operation
    }

    pub const fn kind(&self) -> FragmentIoErrorKind {
        self.kind
    }
}

impl fmt::Display for FragmentIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {}: {}",
            self.operation, self.kind, self.message
        )
    }
}

impl std::error::Error for FragmentIoError {}
