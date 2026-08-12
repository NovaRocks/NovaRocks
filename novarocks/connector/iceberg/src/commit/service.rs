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

//! Provider-owned commit terminal classification and recovery evidence.

use super::abort::CleanupError;
use super::{CommitOpKind, CommitOutcome};
use std::sync::Arc;

pub type CommitServiceOutcome = CommitOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFailureKind {
    KnownUncommitted,
    FinalizeFailedKnownCommitted,
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
        Self::completed(errors.iter().map(|error| error.path.clone()).collect())
    }
}

/// Provider-local projection required to retain recovery evidence.
pub(crate) trait CommitRecoverySource {
    fn recovery_table_ident(&self) -> String;
    fn recovery_op_kind(&self) -> CommitOpKind;
    fn recovery_base_snapshot_id(&self) -> Option<i64>;
    fn recovery_base_sequence_number(&self) -> i64;
    fn recovery_staging_dir(&self) -> String;
    fn recovery_manifest_cleanup_token(&self) -> Option<String>;
}

impl<T> CommitRecoverySource for Arc<T>
where
    T: CommitRecoverySource + ?Sized,
{
    fn recovery_table_ident(&self) -> String {
        self.as_ref().recovery_table_ident()
    }

    fn recovery_op_kind(&self) -> CommitOpKind {
        self.as_ref().recovery_op_kind()
    }

    fn recovery_base_snapshot_id(&self) -> Option<i64> {
        self.as_ref().recovery_base_snapshot_id()
    }

    fn recovery_base_sequence_number(&self) -> i64 {
        self.as_ref().recovery_base_sequence_number()
    }

    fn recovery_staging_dir(&self) -> String {
        self.as_ref().recovery_staging_dir()
    }

    fn recovery_manifest_cleanup_token(&self) -> Option<String> {
        self.as_ref().recovery_manifest_cleanup_token()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryEvidence {
    pub table_ident: String,
    pub op_kind: CommitOpKind,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub staging_dir: String,
    pub manifest_cleanup_token: Option<String>,
}

impl RecoveryEvidence {
    pub(crate) fn from_collector(source: &impl CommitRecoverySource) -> Self {
        Self {
            table_ident: source.recovery_table_ident(),
            op_kind: source.recovery_op_kind(),
            base_snapshot_id: source.recovery_base_snapshot_id(),
            base_sequence_number: source.recovery_base_sequence_number(),
            staging_dir: source.recovery_staging_dir(),
            manifest_cleanup_token: source.recovery_manifest_cleanup_token(),
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
    InvalidInput {
        message: String,
    },
    FinalizeFailedKnownCommitted {
        outcome: Option<CommitOutcome>,
        finalize_error: String,
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

    pub fn invalid_input(message: String) -> Self {
        Self::InvalidInput { message }
    }

    pub fn finalize_failed_known_committed(
        outcome: Option<CommitOutcome>,
        finalize_error: String,
        evidence: RecoveryEvidence,
    ) -> Self {
        Self::FinalizeFailedKnownCommitted {
            outcome,
            finalize_error,
            evidence,
        }
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }

    pub fn is_finalize_failed_known_committed(&self) -> bool {
        matches!(self, Self::FinalizeFailedKnownCommitted { .. })
    }

    pub fn message(&self) -> &str {
        match self {
            Self::KnownUncommitted { message, .. }
            | Self::Unknown { message, .. }
            | Self::InvalidInput { message } => message,
            Self::FinalizeFailedKnownCommitted { finalize_error, .. } => finalize_error,
        }
    }

    pub fn failure_kind(&self) -> CommitFailureKind {
        match self {
            Self::KnownUncommitted { .. } | Self::InvalidInput { .. } => {
                CommitFailureKind::KnownUncommitted
            }
            Self::Unknown { .. } => CommitFailureKind::Unknown,
            Self::FinalizeFailedKnownCommitted { .. } => {
                CommitFailureKind::FinalizeFailedKnownCommitted
            }
        }
    }

    pub fn into_legacy_string(self) -> String {
        match self {
            Self::KnownUncommitted { message, cleanup } => {
                if cleanup.attempted {
                    format!(
                        "iceberg commit failed: {message}; abort cleanup ran ({} error(s))",
                        cleanup.error_count
                    )
                } else {
                    message
                }
            }
            Self::Unknown { message, evidence } => format!(
                "iceberg commit unknown ({message}); staged files left at {} for manual review",
                evidence.staging_dir
            ),
            Self::InvalidInput { message } => message,
            Self::FinalizeFailedKnownCommitted {
                outcome,
                finalize_error,
                evidence,
            } => match outcome {
                Some(outcome) => format!(
                    "iceberg commit is known committed at snapshot {} but finalization failed: {finalize_error}; do not retry commit",
                    outcome.new_snapshot_id
                ),
                None => format!(
                    "iceberg commit is known committed but finalization failed before snapshot id was captured: {finalize_error}; manual recovery required for {}",
                    evidence.table_ident
                ),
            },
        }
    }
}

pub fn classify_commit_error(error: &str) -> CommitFailureKind {
    let lower = error.to_lowercase();
    if (lower.contains("committed but")
        && (lower.contains("not visible") || lower.contains("snapshot id")))
        || (lower.contains("known committed") && lower.contains("finalization failed"))
    {
        return CommitFailureKind::FinalizeFailedKnownCommitted;
    }

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
    if definite_signals.iter().any(|signal| lower.contains(signal)) {
        CommitFailureKind::KnownUncommitted
    } else {
        CommitFailureKind::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definite_and_unknown_failures_stay_distinct() {
        assert_eq!(
            classify_commit_error("catalog commit conflict"),
            CommitFailureKind::KnownUncommitted
        );
        assert_eq!(
            classify_commit_error("connection reset by peer"),
            CommitFailureKind::Unknown
        );
    }

    #[test]
    fn committed_finalization_failure_is_not_retryable_commit() {
        assert_eq!(
            classify_commit_error("committed but snapshot id was not captured"),
            CommitFailureKind::FinalizeFailedKnownCommitted
        );
    }
}
