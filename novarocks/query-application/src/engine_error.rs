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

use std::fmt;

use novarocks_types::UniqueId;

pub use novarocks_types::EngineErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InternalInvariantCode {
    BoundarySchemaMissingDescriptor,
    UnexpectedReportStatusShape,
}

impl InternalInvariantCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundarySchemaMissingDescriptor => "BoundarySchemaMissingDescriptor",
            Self::UnexpectedReportStatusShape => "UnexpectedReportStatusShape",
        }
    }
}

#[derive(Clone, Debug)]
pub enum EngineErrorDetail {
    WriteCoordinatorGone {
        query_id: UniqueId,
    },
    DistributedWriteOutputMismatch {
        operation: &'static str,
        reason: String,
    },
    InternalInvariantViolation {
        code: InternalInvariantCode,
        message: String,
    },
    Message {
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct EngineError {
    code: EngineErrorCode,
    detail: EngineErrorDetail,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineErrorLogFields {
    pub code: &'static str,
    pub message: String,
}

impl EngineError {
    fn new(code: EngineErrorCode, detail: EngineErrorDetail) -> Self {
        Self { code, detail }
    }

    pub fn code(&self) -> EngineErrorCode {
        self.code
    }

    pub fn write_coordinator_gone(query_id: UniqueId) -> Self {
        Self::new(
            EngineErrorCode::WriteCoordinatorGone,
            EngineErrorDetail::WriteCoordinatorGone { query_id },
        )
    }

    pub fn distributed_write_output_mismatch(
        operation: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(
            EngineErrorCode::DistributedWriteOutputMismatch,
            EngineErrorDetail::DistributedWriteOutputMismatch {
                operation,
                reason: reason.into(),
            },
        )
    }

    pub fn internal_invariant(code: InternalInvariantCode, message: impl Into<String>) -> Self {
        Self::new(
            EngineErrorCode::InternalInvariantViolation,
            EngineErrorDetail::InternalInvariantViolation {
                code,
                message: message.into(),
            },
        )
    }

    fn static_message(code: EngineErrorCode, message: impl Into<String>) -> Self {
        Self::new(
            code,
            EngineErrorDetail::Message {
                message: message.into(),
            },
        )
    }

    pub fn commit_known_uncommitted(message: impl Into<String>) -> Self {
        Self::static_message(EngineErrorCode::CommitKnownUncommitted, message)
    }

    pub fn commit_unknown(message: impl Into<String>) -> Self {
        Self::static_message(EngineErrorCode::CommitUnknown, message)
    }

    pub fn commit_known_committed_finalize_failed(message: impl Into<String>) -> Self {
        Self::static_message(EngineErrorCode::CommitKnownCommittedFinalizeFailed, message)
    }

    pub fn unsupported_compiled_shape(message: impl Into<String>) -> Self {
        Self::static_message(EngineErrorCode::UnsupportedCompiledShape, message)
    }

    pub fn query_timeout(message: impl Into<String>) -> Self {
        Self::static_message(EngineErrorCode::QueryTimeout, message)
    }

    pub fn to_user_message(&self) -> String {
        match &self.detail {
            EngineErrorDetail::WriteCoordinatorGone { query_id } => {
                format!(
                    "write coordinator not found for query {}/{}",
                    query_id.high(),
                    query_id.low()
                )
            }
            EngineErrorDetail::DistributedWriteOutputMismatch { operation, reason } => {
                format!("{operation}: {reason}")
            }
            EngineErrorDetail::InternalInvariantViolation { code, message } => {
                format!("{}: {}", code.as_str(), message)
            }
            EngineErrorDetail::Message { message, .. } => message.clone(),
        }
    }

    pub fn to_bracketed_user_message(&self) -> String {
        format!("[{}] {}", self.code.as_str(), self.to_user_message())
    }

    pub fn to_log_fields(&self) -> EngineErrorLogFields {
        EngineErrorLogFields {
            code: self.code.as_str(),
            message: self.to_user_message(),
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_user_message())
    }
}

impl std::error::Error for EngineError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_error_code_round_trips_from_wire_name() {
        for code in [
            EngineErrorCode::TypeMismatch,
            EngineErrorCode::TypeDeterminismViolation,
            EngineErrorCode::ExchangeDescriptorMismatch,
            EngineErrorCode::AggregateStateLayoutMismatch,
            EngineErrorCode::IcebergWriteDescriptorMismatch,
            EngineErrorCode::UnsupportedPositionDeleteDescriptor,
            EngineErrorCode::UnsupportedDistributedDmlShape,
            EngineErrorCode::UnsupportedCompiledShape,
            EngineErrorCode::DistributedWriteOutputMismatch,
            EngineErrorCode::WriteCoordinatorGone,
            EngineErrorCode::CommitKnownUncommitted,
            EngineErrorCode::CommitUnknown,
            EngineErrorCode::CommitKnownCommittedFinalizeFailed,
            EngineErrorCode::QueryTimeout,
            EngineErrorCode::ProtocolDecodeError,
            EngineErrorCode::InternalInvariantViolation,
        ] {
            assert_eq!(EngineErrorCode::parse(code.as_str()), Some(code));
        }
        assert_eq!(EngineErrorCode::parse("NotARealCode"), None);
    }

    #[test]
    fn write_coordinator_gone_preserves_query_identity() {
        let err = EngineError::write_coordinator_gone(UniqueId::new(11, 22));
        assert_eq!(err.code(), EngineErrorCode::WriteCoordinatorGone);
        assert!(err.to_user_message().contains("11/22"));
    }

    #[test]
    fn named_message_constructors_return_stable_codes() {
        let known_uncommitted = EngineError::commit_known_uncommitted("commit was aborted");
        assert_eq!(
            known_uncommitted.code(),
            EngineErrorCode::CommitKnownUncommitted
        );
        assert!(
            known_uncommitted
                .to_user_message()
                .contains("commit was aborted")
        );

        let unknown = EngineError::commit_unknown("coordinator did not return a decision");
        assert_eq!(unknown.code(), EngineErrorCode::CommitUnknown);
        assert!(
            unknown
                .to_user_message()
                .contains("coordinator did not return a decision")
        );

        let timeout = EngineError::query_timeout("query timed out after 1000 ms");
        assert_eq!(timeout.code(), EngineErrorCode::QueryTimeout);
        assert_eq!(
            timeout.to_bracketed_user_message(),
            "[QueryTimeout] query timed out after 1000 ms"
        );
    }

    #[test]
    fn compiled_refusal_formats_the_stable_client_identity() {
        let error = EngineError::unsupported_compiled_shape("FULL OUTER hash join");
        assert_eq!(error.code(), EngineErrorCode::UnsupportedCompiledShape);
        assert_eq!(
            error.to_bracketed_user_message(),
            "[UnsupportedCompiledShape] FULL OUTER hash join"
        );
    }

    #[test]
    fn distributed_dml_helpers_use_stable_codes() {
        let mismatch =
            EngineError::distributed_write_output_mismatch("insert", "slot 3 changed type");
        assert_eq!(
            mismatch.code(),
            EngineErrorCode::DistributedWriteOutputMismatch
        );
        assert_eq!(
            mismatch.to_bracketed_user_message(),
            "[DistributedWriteOutputMismatch] insert: slot 3 changed type"
        );
    }

    #[test]
    fn log_fields_include_stable_code_and_readable_message() {
        let err = EngineError::commit_unknown("commit outcome unavailable");
        let fields = err.to_log_fields();
        assert_eq!(fields.code, "CommitUnknown");
        assert!(fields.message.contains("commit outcome unavailable"));
    }
}
