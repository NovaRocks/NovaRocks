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

use super::abort::CleanupError;
use super::collector::IcebergCommitCollector;
use super::types::{CommitOpKind, CommitOutcome};

pub type CommitServiceOutcome = CommitOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFailureKind {
    KnownUncommitted,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupAttempt {
    pub attempted: bool,
    pub error_count: usize,
    pub error_paths: Vec<String>,
}

impl CleanupAttempt {
    pub fn not_attempted() -> Self {
        Self {
            attempted: false,
            error_count: 0,
            error_paths: Vec::new(),
        }
    }

    pub fn completed(error_paths: Vec<String>) -> Self {
        Self {
            attempted: true,
            error_count: error_paths.len(),
            error_paths,
        }
    }

    pub fn from_cleanup_errors(errors: &[CleanupError]) -> Self {
        Self::completed(errors.iter().map(|err| err.path.clone()).collect())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryEvidence {
    pub table_ident: String,
    pub op_kind: CommitOpKind,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub staging_dir: String,
}

impl RecoveryEvidence {
    pub fn from_collector(collector: &IcebergCommitCollector) -> Self {
        Self {
            table_ident: collector.table_ident.to_string(),
            op_kind: collector.op_kind,
            base_snapshot_id: collector.base_snapshot_id,
            base_sequence_number: collector.base_sequence_number,
            staging_dir: collector.staging_dir.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitServiceError {
    KnownUncommitted {
        message: String,
        cleanup: CleanupAttempt,
    },
    Unknown {
        message: String,
        evidence: RecoveryEvidence,
    },
}

impl CommitServiceError {
    pub fn known_uncommitted(message: String, cleanup: CleanupAttempt) -> Self {
        Self::KnownUncommitted { message, cleanup }
    }

    pub fn unknown(message: String, evidence: RecoveryEvidence) -> Self {
        Self::Unknown { message, evidence }
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }

    pub fn message(&self) -> &str {
        match self {
            Self::KnownUncommitted { message, .. } | Self::Unknown { message, .. } => message,
        }
    }

    pub fn into_legacy_string(self) -> String {
        match self {
            Self::KnownUncommitted { message, cleanup } => format!(
                "iceberg commit failed: {message}; abort cleanup ran ({} error(s))",
                cleanup.error_count
            ),
            Self::Unknown { message, evidence } => format!(
                "iceberg commit unknown ({message}); staged files left at {} for manual review",
                evidence.staging_dir
            ),
        }
    }
}

pub fn classify_commit_error(err: &str) -> CommitFailureKind {
    let lower = err.to_lowercase();
    let definite_signals = [
        "conflict",
        "assertrefsnapshotid",
        "ref_snapshot_id_match",
        "schema id mismatch",
        "schemaidmatch",
        "spec id mismatch",
        "specidmatch",
        "data invalid",
        "datainvalid",
        "feature unsupported",
        "featureunsupported",
        "table not found",
        "tablenotfound",
        "table already exists",
        "tablealreadyexists",
        "namespace not found",
        "namespacenotfound",
        "namespace already exists",
        "namespacealreadyexists",
        "precondition failed",
        "preconditionfailed",
        "catalog commit conflict",
        "catalogcommitconflict",
        "expected data only",
        "pipeline cancelled",
        "pipeline failed",
    ];
    if definite_signals.iter().any(|s| lower.contains(s)) {
        CommitFailureKind::KnownUncommitted
    } else {
        CommitFailureKind::Unknown
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PartitionSpec, Schema, Type};
    use iceberg::{NamespaceIdent, TableIdent};
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::{CommitOpKind, IcebergCommitCollector};

    use super::*;

    fn test_collector() -> IcebergCommitCollector {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let partition_spec = PartitionSpec::builder(schema.clone())
            .build()
            .expect("partition spec");
        IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::new(NamespaceIdent::new("db".to_string()), "tbl".to_string()),
            Some(42),
            7,
            Arc::new(schema),
            Arc::new(partition_spec),
            "s3://bucket/db/tbl/data/_staging/abc".to_string(),
            UniqueId { hi: 11, lo: 22 },
        )
    }

    #[test]
    fn classify_commit_error_returns_known_uncommitted_for_definite_failures() {
        assert_eq!(
            classify_commit_error(
                "FastAppend commit failed: catalog commit conflict on assert-ref-snapshot-id"
            ),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("RowDelta commit failed: data invalid"),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("pipeline cancelled mid-write"),
            CommitFailureKind::KnownUncommitted
        );
    }

    #[test]
    fn classify_commit_error_returns_unknown_for_transport_like_failures() {
        assert_eq!(
            classify_commit_error("FastAppend commit failed: connection reset by peer"),
            CommitFailureKind::Unknown
        );
        assert_eq!(
            classify_commit_error("RowDelta commit failed: unexpected error"),
            CommitFailureKind::Unknown
        );
    }

    #[test]
    fn unknown_error_carries_recovery_evidence_and_legacy_message() {
        let collector = test_collector();
        let evidence = RecoveryEvidence::from_collector(&collector);
        let err =
            CommitServiceError::unknown("connection reset by peer".to_string(), evidence.clone());
        assert!(err.is_unknown());
        assert_eq!(err.message(), "connection reset by peer");
        assert_eq!(evidence.table_ident, "db.tbl");
        assert_eq!(evidence.op_kind, CommitOpKind::FastAppend);
        assert_eq!(evidence.base_snapshot_id, Some(42));
        assert_eq!(evidence.base_sequence_number, 7);
        assert_eq!(
            err.clone().into_legacy_string(),
            "iceberg commit unknown (connection reset by peer); staged files left at s3://bucket/db/tbl/data/_staging/abc for manual review"
        );
    }

    #[test]
    fn known_uncommitted_error_carries_cleanup_summary_and_legacy_message() {
        let cleanup =
            CleanupAttempt::completed(vec!["a.parquet".to_string(), "m.avro".to_string()]);
        let err = CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            cleanup.clone(),
        );
        assert!(!err.is_unknown());
        assert_eq!(err.message(), "catalog commit conflict");
        assert_eq!(cleanup.error_count, 2);
        assert_eq!(
            err.into_legacy_string(),
            "iceberg commit failed: catalog commit conflict; abort cleanup ran (2 error(s))"
        );
    }
}
