use std::fmt;

pub const REPORT_EXEC_STATUS_OK: i32 = 0;
pub const REPORT_EXEC_STATUS_ERROR: i32 = 1;
pub const REPORT_EXEC_STATUS_QUERY_GONE: i32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum EngineErrorCode {
    TypeMismatch,
    TypeDeterminismViolation,
    ExchangeDescriptorMismatch,
    AggregateStateLayoutMismatch,
    IcebergWriteDescriptorMismatch,
    UnsupportedDistributedDmlShape,
    DistributedWriteOutputMismatch,
    WriteCoordinatorGone,
    CommitKnownUncommitted,
    CommitUnknown,
    ProtocolDecodeError,
    InternalInvariantViolation,
}

impl EngineErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TypeMismatch => "TypeMismatch",
            Self::TypeDeterminismViolation => "TypeDeterminismViolation",
            Self::ExchangeDescriptorMismatch => "ExchangeDescriptorMismatch",
            Self::AggregateStateLayoutMismatch => "AggregateStateLayoutMismatch",
            Self::IcebergWriteDescriptorMismatch => "IcebergWriteDescriptorMismatch",
            Self::UnsupportedDistributedDmlShape => "UnsupportedDistributedDmlShape",
            Self::DistributedWriteOutputMismatch => "DistributedWriteOutputMismatch",
            Self::WriteCoordinatorGone => "WriteCoordinatorGone",
            Self::CommitKnownUncommitted => "CommitKnownUncommitted",
            Self::CommitUnknown => "CommitUnknown",
            Self::ProtocolDecodeError => "ProtocolDecodeError",
            Self::InternalInvariantViolation => "InternalInvariantViolation",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input {
            "TypeMismatch" => Some(Self::TypeMismatch),
            "TypeDeterminismViolation" => Some(Self::TypeDeterminismViolation),
            "ExchangeDescriptorMismatch" => Some(Self::ExchangeDescriptorMismatch),
            "AggregateStateLayoutMismatch" => Some(Self::AggregateStateLayoutMismatch),
            "IcebergWriteDescriptorMismatch" => Some(Self::IcebergWriteDescriptorMismatch),
            "UnsupportedDistributedDmlShape" => Some(Self::UnsupportedDistributedDmlShape),
            "DistributedWriteOutputMismatch" => Some(Self::DistributedWriteOutputMismatch),
            "WriteCoordinatorGone" => Some(Self::WriteCoordinatorGone),
            "CommitKnownUncommitted" => Some(Self::CommitKnownUncommitted),
            "CommitUnknown" => Some(Self::CommitUnknown),
            "ProtocolDecodeError" => Some(Self::ProtocolDecodeError),
            "InternalInvariantViolation" => Some(Self::InternalInvariantViolation),
            _ => None,
        }
    }
}

impl fmt::Display for EngineErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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
        query_id: crate::types::TUniqueId,
    },
    ProtocolDecode {
        message: String,
    },
    UnsupportedDistributedDmlShape {
        operation: &'static str,
        reason: String,
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

    pub fn to_report_error_code(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn write_coordinator_gone(query_id: crate::types::TUniqueId) -> Self {
        Self::new(
            EngineErrorCode::WriteCoordinatorGone,
            EngineErrorDetail::WriteCoordinatorGone { query_id },
        )
    }

    pub fn protocol_decode(message: impl Into<String>) -> Self {
        Self::new(
            EngineErrorCode::ProtocolDecodeError,
            EngineErrorDetail::ProtocolDecode {
                message: message.into(),
            },
        )
    }

    pub fn unsupported_distributed_dml_shape(
        operation: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(
            EngineErrorCode::UnsupportedDistributedDmlShape,
            EngineErrorDetail::UnsupportedDistributedDmlShape {
                operation,
                reason: reason.into(),
            },
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

    pub fn iceberg_write_descriptor_mismatch(message: impl Into<String>) -> Self {
        Self::static_message(EngineErrorCode::IcebergWriteDescriptorMismatch, message)
    }

    pub fn to_user_message(&self) -> String {
        match &self.detail {
            EngineErrorDetail::WriteCoordinatorGone { query_id } => {
                format!(
                    "write coordinator not found for query {}/{}",
                    query_id.hi, query_id.lo
                )
            }
            EngineErrorDetail::ProtocolDecode { message } => message.clone(),
            EngineErrorDetail::UnsupportedDistributedDmlShape { operation, reason } => {
                format!("{operation}: {reason}")
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

    pub fn to_tstatus_code(&self) -> crate::status_code::TStatusCode {
        match self.code {
            EngineErrorCode::UnsupportedDistributedDmlShape => {
                crate::status_code::TStatusCode::NOT_IMPLEMENTED_ERROR
            }
            EngineErrorCode::ProtocolDecodeError => {
                crate::status_code::TStatusCode::INVALID_ARGUMENT
            }
            _ => crate::status_code::TStatusCode::INTERNAL_ERROR,
        }
    }

    pub fn to_mysql_error_kind(&self) -> opensrv_mysql::ErrorKind {
        match self.code {
            EngineErrorCode::UnsupportedDistributedDmlShape => {
                opensrv_mysql::ErrorKind::ER_NOT_SUPPORTED_YET
            }
            EngineErrorCode::ProtocolDecodeError => opensrv_mysql::ErrorKind::ER_PARSE_ERROR,
            _ => opensrv_mysql::ErrorKind::ER_UNKNOWN_ERROR,
        }
    }

    pub fn to_report_status_code(&self) -> i32 {
        match self.code {
            EngineErrorCode::WriteCoordinatorGone => REPORT_EXEC_STATUS_QUERY_GONE,
            _ => REPORT_EXEC_STATUS_ERROR,
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_user_message())
    }
}

impl std::error::Error for EngineError {}

impl From<crate::connector::iceberg::commit::CommitServiceError> for EngineError {
    fn from(value: crate::connector::iceberg::commit::CommitServiceError) -> Self {
        let is_unknown = value.is_unknown();
        let message = value.into_legacy_string();
        if is_unknown {
            Self::commit_unknown(message)
        } else {
            Self::commit_known_uncommitted(message)
        }
    }
}

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
            EngineErrorCode::UnsupportedDistributedDmlShape,
            EngineErrorCode::DistributedWriteOutputMismatch,
            EngineErrorCode::WriteCoordinatorGone,
            EngineErrorCode::CommitKnownUncommitted,
            EngineErrorCode::CommitUnknown,
            EngineErrorCode::ProtocolDecodeError,
            EngineErrorCode::InternalInvariantViolation,
        ] {
            assert_eq!(EngineErrorCode::parse(code.as_str()), Some(code));
        }
        assert_eq!(EngineErrorCode::parse("NotARealCode"), None);
    }

    #[test]
    fn write_coordinator_gone_maps_to_query_gone_report_status() {
        let err = EngineError::write_coordinator_gone(crate::types::TUniqueId { hi: 11, lo: 22 });
        assert_eq!(err.code(), EngineErrorCode::WriteCoordinatorGone);
        assert_eq!(err.to_report_status_code(), REPORT_EXEC_STATUS_QUERY_GONE);
        assert_eq!(
            err.to_tstatus_code(),
            crate::status_code::TStatusCode::INTERNAL_ERROR
        );
        assert!(err.to_user_message().contains("11/22"));
    }

    #[test]
    fn protocol_decode_error_has_stable_code_and_message() {
        let err = EngineError::protocol_decode("failed to deserialize payload");
        assert_eq!(err.code().as_str(), "ProtocolDecodeError");
        assert_eq!(err.to_report_error_code(), "ProtocolDecodeError");
        assert!(
            err.to_user_message()
                .contains("failed to deserialize payload")
        );
    }

    #[test]
    fn unsupported_distributed_dml_shape_maps_to_not_supported() {
        let err = EngineError::unsupported_distributed_dml_shape("insert", "missing coordinator");
        assert_eq!(
            err.to_tstatus_code(),
            crate::status_code::TStatusCode::NOT_IMPLEMENTED_ERROR
        );
        assert_eq!(
            err.to_mysql_error_kind(),
            opensrv_mysql::ErrorKind::ER_NOT_SUPPORTED_YET
        );
    }

    #[test]
    fn protocol_decode_error_maps_to_invalid_argument_and_parse_error() {
        let err = EngineError::protocol_decode("bad report payload");
        assert_eq!(
            err.to_tstatus_code(),
            crate::status_code::TStatusCode::INVALID_ARGUMENT
        );
        assert_eq!(
            err.to_mysql_error_kind(),
            opensrv_mysql::ErrorKind::ER_PARSE_ERROR
        );
    }

    #[test]
    fn default_engine_error_maps_to_internal_and_unknown() {
        let err = EngineError::internal_invariant(
            InternalInvariantCode::UnexpectedReportStatusShape,
            "missing status",
        );
        assert_eq!(
            err.to_tstatus_code(),
            crate::status_code::TStatusCode::INTERNAL_ERROR
        );
        assert_eq!(
            err.to_mysql_error_kind(),
            opensrv_mysql::ErrorKind::ER_UNKNOWN_ERROR
        );
        assert_eq!(err.to_report_status_code(), REPORT_EXEC_STATUS_ERROR);
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

        let descriptor =
            EngineError::iceberg_write_descriptor_mismatch("writer output slot changed");
        assert_eq!(
            descriptor.code(),
            EngineErrorCode::IcebergWriteDescriptorMismatch
        );
        assert!(
            descriptor
                .to_user_message()
                .contains("writer output slot changed")
        );
    }

    #[test]
    fn iceberg_write_descriptor_helper_preserves_specific_code() {
        let err = EngineError::iceberg_write_descriptor_mismatch("missing partition descriptor");

        assert_eq!(err.code(), EngineErrorCode::IcebergWriteDescriptorMismatch);
        assert_eq!(
            err.to_bracketed_user_message(),
            "[IcebergWriteDescriptorMismatch] missing partition descriptor"
        );
    }

    #[test]
    fn distributed_dml_helpers_use_stable_codes() {
        let unsupported =
            EngineError::unsupported_distributed_dml_shape("delete", "missing coordinator");
        assert_eq!(
            unsupported.code(),
            EngineErrorCode::UnsupportedDistributedDmlShape
        );
        assert_eq!(
            unsupported.to_bracketed_user_message(),
            "[UnsupportedDistributedDmlShape] delete: missing coordinator"
        );

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
    fn iceberg_write_descriptor_error_converts_to_engine_error() {
        let err = EngineError::from(
            crate::connector::iceberg::write_descriptor::IcebergWriteDescriptorError::MissingDescriptor,
        );

        assert_eq!(err.code(), EngineErrorCode::IcebergWriteDescriptorMismatch);
        assert!(
            err.to_bracketed_user_message()
                .starts_with("[IcebergWriteDescriptorMismatch] "),
            "got: {}",
            err.to_bracketed_user_message()
        );
        assert!(
            err.to_user_message()
                .contains("missing partition descriptor")
        );
    }

    #[test]
    fn commit_service_error_converts_to_engine_error() {
        let known = crate::connector::iceberg::commit::CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            crate::connector::iceberg::commit::CleanupAttempt::not_attempted(),
        );
        let known = EngineError::from(known);
        assert_eq!(known.code(), EngineErrorCode::CommitKnownUncommitted);
        assert_eq!(
            known.to_bracketed_user_message(),
            "[CommitKnownUncommitted] catalog commit conflict"
        );

        let unknown = crate::connector::iceberg::commit::CommitServiceError::unknown(
            "connection reset by peer".to_string(),
            crate::connector::iceberg::commit::RecoveryEvidence {
                table_ident: "db.t".to_string(),
                op_kind: crate::connector::iceberg::commit::CommitOpKind::FastAppend,
                base_snapshot_id: Some(10),
                base_sequence_number: 7,
                staging_dir: "s3://bucket/db/t/_staging/abc".to_string(),
            },
        );
        let unknown = EngineError::from(unknown);
        assert_eq!(unknown.code(), EngineErrorCode::CommitUnknown);
        assert!(
            unknown
                .to_bracketed_user_message()
                .starts_with("[CommitUnknown] iceberg commit unknown"),
            "got: {}",
            unknown.to_bracketed_user_message()
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
