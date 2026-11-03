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

//! Required parent data and host invariants of a compiled table invocation.
use crate::runtime::task::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, RepresentationBoundary, RepresentationFacts,
    RepresentationLayout, domain,
};
use novarocks_functions::{RowDataError, TableCapacityRequirements, TableStepCapacity};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Debug)]
pub(super) struct CompiledTableFailure {
    pub node: usize,
    pub reason: CompiledTableFailureKind,
}
#[derive(Debug)]
pub(super) enum CompiledTableFailureKind {
    ParentData {
        row: usize,
        cause: RowDataError,
    },
    Capacity {
        required: TableCapacityRequirements,
        granted: TableStepCapacity,
    },
    ParentErrorOrdinal {
        ordinal: usize,
    },
    RowAfterCompletion {
        parent: usize,
    },
    DuplicateCompletion {
        parent: usize,
    },
    IncompleteParents {
        first: usize,
    },
    Arrow {
        result: Option<usize>,
        cause: arrow::error::ArrowError,
    },
    OutputExtent {
        prefix: usize,
        additional: usize,
    },
    InputPort,
    InputValueBindings,
    AlreadyFailed,
    MissingBatchRows,
    InvalidBatchRows,
}
impl fmt::Display for CompiledTableFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "compiled TableFunction at local node {} ", self.node)?;
        match &self.reason {
            CompiledTableFailureKind::ParentData { row, cause } => {
                write!(f, "failed at batch row {row}: {cause}")
            }
            CompiledTableFailureKind::Capacity { required, granted } => write!(
                f,
                "table cursor requires {required:?} beyond its full step grant {granted:?}"
            ),
            CompiledTableFailureKind::ParentErrorOrdinal { ordinal } => write!(
                f,
                "reported parent error ordinal {ordinal} outside its invocation"
            ),
            CompiledTableFailureKind::RowAfterCompletion { parent } => {
                write!(f, "produced a row after parent {parent} completed")
            }
            CompiledTableFailureKind::DuplicateCompletion { parent } => {
                write!(f, "completed parent {parent} twice")
            }
            CompiledTableFailureKind::IncompleteParents { first } => {
                write!(f, "ended before parent {first} completed")
            }
            CompiledTableFailureKind::Arrow { result, cause } => {
                write!(f, "result {result:?} Arrow assembly failed: {cause}")
            }
            CompiledTableFailureKind::OutputExtent { .. } => {
                f.write_str("output row count exceeds the host range")
            }
            CompiledTableFailureKind::InputPort => {
                f.write_str("input differs from its frozen input port")
            }
            CompiledTableFailureKind::InputValueBindings => {
                f.write_str("input differs from its complete frozen value bindings")
            }
            CompiledTableFailureKind::AlreadyFailed => f.write_str("instance has already failed"),
            CompiledTableFailureKind::MissingBatchRows => f.write_str("batch size was not bound"),
            CompiledTableFailureKind::InvalidBatchRows => f.write_str("batch size is zero"),
        }
    }
}
impl Error for CompiledTableFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.reason {
            CompiledTableFailureKind::ParentData { cause, .. } => Some(cause),
            CompiledTableFailureKind::Arrow { cause, .. } => Some(cause),
            _ => None,
        }
    }
}
impl FailureProjection for CompiledTableFailure {
    fn failure_fact(&self) -> FailureFact {
        use CompiledTableFailureKind::*;
        let reason = match &self.reason {
            ParentData { cause, .. } => return cause.failure_fact(),
            Capacity { .. } => ExecutionReason::TableCursorCapacity,
            ParentErrorOrdinal { .. } => ExecutionReason::TableCursorErrorOrdinal,
            RowAfterCompletion { .. } => ExecutionReason::TableCursorRowAfterCompletion,
            DuplicateCompletion { .. } => ExecutionReason::TableCursorDuplicateCompletion,
            IncompleteParents { .. } => ExecutionReason::TableCursorIncompleteParents,
            Arrow { .. } => ExecutionReason::TableCursorArrow,
            OutputExtent { prefix, additional } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::TableCursorOutputExtent.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: NonZeroU16::new(5).unwrap(),
                    },
                    required: (*prefix as u128)
                        .checked_add(*additional as u128)
                        .and_then(|rows| u64::try_from(rows).ok()),
                    extent_limit: None,
                    representable_prefix: None,
                });
            }
            InputPort | InputValueBindings => ExecutionReason::TableCursorInputPort,
            AlreadyFailed => ExecutionReason::TableCursorAlreadyFailed,
            MissingBatchRows => ExecutionReason::TableCursorMissingBatchRows,
            InvalidBatchRows => ExecutionReason::TableCursorInvalidBatchRows,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<CompiledTableFailure> for ExecutionFailure {
    fn from(cause: CompiledTableFailure) -> Self {
        Self::from_domain(cause)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_functions::DataFailureReason;
    #[test]
    fn parent_data_keeps_row_reason_and_source_while_host_extent_stays_representation() {
        let row = RowDataError::with_reason(
            3,
            DataFailureReason::NumericOutOfRange,
            "encoding diagnostic",
        );
        let expected = row.failure_fact();
        let failure = CompiledTableFailure {
            node: 9,
            reason: CompiledTableFailureKind::ParentData {
                row: 17,
                cause: row,
            },
        };
        assert_eq!(failure.failure_fact(), expected);
        let source = failure
            .source()
            .unwrap()
            .downcast_ref::<RowDataError>()
            .unwrap();
        assert_eq!(source.selected_ordinal(), 3);
        assert_eq!(source.reason(), DataFailureReason::NumericOutOfRange);
        assert!(failure.to_string().contains("batch row 17"));
        let extent = CompiledTableFailure {
            node: 9,
            reason: CompiledTableFailureKind::OutputExtent {
                prefix: usize::MAX,
                additional: 1,
            },
        };
        assert_eq!(extent.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            extent.failure_fact().representation().unwrap().extent_limit,
            None
        );
    }
}
