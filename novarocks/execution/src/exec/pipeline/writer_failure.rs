// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Failures whose facts belong to the writer and its asynchronous actor.

use crate::runtime::task::connector_failure::ConnectorOperation;
use novarocks_failure_contract::BoundedDetail;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts, ResourceScope,
};
use std::{error::Error, fmt, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterCommandOperation {
    Append,
    Finish,
}
impl fmt::Display for WriterCommandOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Append => "append",
            Self::Finish => "finish",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriterFailure {
    FrozenContract {
        detail: BoundedDetail,
    },
    RowCountDrift {
        expected: u64,
        actual: u64,
    },
    OutputBytes {
        bytes: usize,
        limit: usize,
    },
    AlreadyBound,
    InputBatchRows {
        rows: usize,
        limit: usize,
    },
    InputBatchBytes {
        bytes: usize,
        limit: usize,
    },
    QueueCapacity {
        dimension: ResourceDimension,
        incoming: usize,
        live: usize,
        limit: usize,
    },
    AdditionalBytes {
        additional: usize,
        retained: usize,
    },
    AfterTerminal {
        operation: WriterCommandOperation,
    },
    MailboxFull {
        operation: WriterCommandOperation,
    },
    MailboxClosed {
        operation: WriterCommandOperation,
    },
    MissingTerminalOutcome,
    PartialDemandClosed,
    ActorInterrupted,
    ExitUnresolved,
    CommitFragmentLimit {
        requested: usize,
        limit: usize,
    },
    Panicked {
        operation: Option<ConnectorOperation>,
        detail: BoundedDetail,
    },
    AbortTimeout {
        timeout: Duration,
    },
    AcceptedRowsOverflow {
        current: u64,
        incoming: usize,
    },
}
impl fmt::Display for WriterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrozenContract { detail } => detail.fmt(f),
            Self::RowCountDrift { expected, actual } => write!(
                f,
                "composite table writer row count drift: aggregate accepted {expected}, writer accepted {actual}"
            ),
            Self::OutputBytes { bytes, limit } => write!(
                f,
                "table writer output requires {bytes} bytes under a {limit} byte construction profile"
            ),
            Self::InputBatchRows { rows, limit } => write!(
                f,
                "connector writer input batch has {rows} rows, above the per-batch limit of {limit} rows"
            ),
            Self::InputBatchBytes { bytes, limit } => write!(
                f,
                "connector writer input batch retains {bytes} bytes, above the per-batch limit of {limit} bytes"
            ),
            Self::QueueCapacity {
                dimension,
                incoming,
                live,
                limit,
            } => write!(
                f,
                "connector writer queue has no reserved {dimension:?} capacity: incoming={incoming}, live={live}, limit={limit}"
            ),
            Self::AdditionalBytes {
                additional,
                retained,
            } => write!(
                f,
                "connector writer additional retained bytes {additional} exceed total retained bytes {retained}"
            ),
            Self::AfterTerminal { operation } => {
                write!(f, "connector writer {operation} after terminal transition")
            }
            Self::MailboxFull { operation } => {
                write!(f, "connector writer {operation} mailbox is full")
            }
            Self::MailboxClosed { operation } => {
                write!(f, "connector writer {operation} mailbox is closed")
            }
            Self::AlreadyBound => f.write_str("connector writer actor is already bound"),
            Self::CommitFragmentLimit { requested, limit } => write!(
                f,
                "table writer commit fragment of {requested} bytes exceeds the frozen single-fragment budget of {limit} bytes"
            ),
            Self::PartialDemandClosed => {
                f.write_str("writer partial aggregate closed before the writer input ended")
            }
            Self::MissingTerminalOutcome => {
                f.write_str("connector writer actor exited without a terminal result")
            }
            Self::ExitUnresolved => f.write_str("writer physical exit is unresolved"),
            Self::ActorInterrupted => {
                f.write_str("connector writer actor was interrupted before reporting its outcome")
            }
            Self::Panicked { operation, detail } => match operation {
                Some(operation) => write!(f, "{operation} panicked: {detail}"),
                None => write!(f, "connector writer actor panicked: {detail}"),
            },
            Self::AbortTimeout { timeout } => write!(
                f,
                "abort connector writer exceeded the bounded wait of {} ms",
                timeout.as_millis()
            ),
            Self::AcceptedRowsOverflow { .. } => {
                f.write_str("connector writer accepted row count overflowed u64")
            }
        }
    }
}
impl Error for WriterFailure {}
impl FailureProjection for WriterFailure {
    fn failure_fact(&self) -> FailureFact {
        use crate::runtime::task::ExecutionReason;
        let (class, code) = match self {
            Self::FrozenContract { .. } => (
                FailureClass::Invariant,
                ExecutionReason::WriterFrozenContract,
            ),
            Self::RowCountDrift { .. } => (
                FailureClass::Invariant,
                ExecutionReason::WriterRowCountDrift,
            ),
            Self::OutputBytes { .. } => {
                (FailureClass::Resource, ExecutionReason::WriterOutputBytes)
            }
            Self::InputBatchRows { .. } => (
                FailureClass::Resource,
                ExecutionReason::WriterInputBatchRows,
            ),
            Self::InputBatchBytes { .. } => (
                FailureClass::Resource,
                ExecutionReason::WriterInputBatchBytes,
            ),
            Self::QueueCapacity { .. } => {
                (FailureClass::Resource, ExecutionReason::WriterQueueCapacity)
            }
            Self::AdditionalBytes { .. } => (
                FailureClass::Invariant,
                ExecutionReason::WriterAdditionalBytes,
            ),
            Self::AfterTerminal { .. } => (
                FailureClass::Invariant,
                ExecutionReason::WriterAfterTerminal,
            ),
            Self::MailboxFull { .. } => {
                (FailureClass::Invariant, ExecutionReason::WriterMailboxFull)
            }
            Self::MailboxClosed { .. } => (
                FailureClass::Unexpected,
                ExecutionReason::WriterMailboxClosed,
            ),
            Self::AlreadyBound => (FailureClass::Invariant, ExecutionReason::WriterAlreadyBound),
            Self::MissingTerminalOutcome => (
                FailureClass::Invariant,
                ExecutionReason::WriterMissingOutcome,
            ),
            Self::PartialDemandClosed => (
                FailureClass::Invariant,
                ExecutionReason::WriterPartialDemandClosed,
            ),
            Self::ExitUnresolved => (
                FailureClass::Unexpected,
                ExecutionReason::WriterExitUnresolved,
            ),
            Self::ActorInterrupted => {
                (FailureClass::Unexpected, ExecutionReason::WriterInterrupted)
            }
            Self::CommitFragmentLimit { .. } => (
                FailureClass::Resource,
                ExecutionReason::WriterCommitFragmentLimit,
            ),
            Self::Panicked { .. } => (FailureClass::Unexpected, ExecutionReason::WriterPanicked),
            Self::AbortTimeout { .. } => {
                (FailureClass::Control, ExecutionReason::WriterAbortTimeout)
            }
            Self::AcceptedRowsOverflow { .. } => (
                FailureClass::Resource,
                ExecutionReason::WriterAcceptedRowsOverflow,
            ),
        };
        let fact = FailureFact::new(class, code.reason());
        let capacity = match self {
            Self::InputBatchRows { rows, limit } => {
                Some((ResourceDimension::Positions, *rows, None, *limit))
            }
            Self::InputBatchBytes { bytes, limit } => {
                Some((ResourceDimension::Bytes, *bytes, None, *limit))
            }
            Self::QueueCapacity {
                dimension,
                incoming,
                live,
                limit,
            } => Some((*dimension, *incoming, Some(*live), *limit)),
            _ => None,
        };
        if let Some((dimension, incoming, live, limit)) = capacity {
            return fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Operator),
                dimension,
                requested: u64::try_from(incoming).ok(),
                required_growth: None,
                limit: u64::try_from(limit).ok(),
                live: live.and_then(|live| u64::try_from(live).ok()),
                committed: None,
            });
        }
        if let Self::AcceptedRowsOverflow { current, incoming } = self {
            fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Operator),
                constraint_scope: Some(ResourceScope::Operator),
                dimension: ResourceDimension::Items,
                requested: u64::try_from(*incoming).ok(),
                required_growth: None,
                limit: Some(u64::MAX),
                live: None,
                committed: Some(*current),
            })
        } else if let Self::CommitFragmentLimit { requested, limit }
        | Self::OutputBytes {
            bytes: requested,
            limit,
        } = self
        {
            fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Request),
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: u64::try_from(*limit).ok(),
                live: None,
                committed: None,
            })
        } else {
            fact
        }
    }
}

/// Keeps a bounded rendered detail alongside the machine-readable local cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterOwnerFailure {
    cause: WriterFailure,
    detail: BoundedDetail,
}
impl From<WriterFailure> for WriterOwnerFailure {
    fn from(cause: WriterFailure) -> Self {
        Self {
            detail: BoundedDetail::truncating_display(&cause),
            cause,
        }
    }
}
impl WriterOwnerFailure {
    pub const fn cause(&self) -> &WriterFailure {
        &self.cause
    }
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}
impl fmt::Display for WriterOwnerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl Error for WriterOwnerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}
impl FailureProjection for WriterOwnerFailure {
    fn failure_fact(&self) -> FailureFact {
        self.cause.failure_fact()
    }
}
