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

use crate::connector::iceberg::commit::{CleanupAttempt, CommitOutcome, CommitServiceError};
use crate::meta::repository::iceberg_operation::{
    IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord, IcebergOperationFailureKind,
    IcebergOperationFailureRecord, IcebergOperationNextAction, IcebergOperationState,
    IcebergRecoveryEvidenceRecord,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergOperationFact {
    pub state: IcebergOperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
}

pub fn operation_fact_from_commit_result(
    result: Result<&CommitOutcome, &CommitServiceError>,
) -> IcebergOperationFact {
    match result {
        Ok(outcome) => IcebergOperationFact {
            state: IcebergOperationState::Committed,
            commit_outcome: Some(IcebergCommitOutcomeRecord {
                snapshot_id: outcome.new_snapshot_id,
                written_manifest_paths: outcome.written_manifest_paths.clone(),
            }),
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
        },
        Err(CommitServiceError::KnownUncommitted { message, cleanup }) => IcebergOperationFact {
            state: IcebergOperationState::FailedKnownUncommitted,
            commit_outcome: None,
            cleanup_outcome: Some(cleanup_outcome_from_attempt(cleanup)),
            recovery_evidence: None,
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::KnownUncommitted,
                message: message.clone(),
                next_action: if cleanup.attempted {
                    IcebergOperationNextAction::None
                } else {
                    IcebergOperationNextAction::RetryAbort
                },
            }),
        },
        Err(CommitServiceError::Unknown { message, evidence }) => IcebergOperationFact {
            state: IcebergOperationState::CommitUnknown,
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: Some(IcebergRecoveryEvidenceRecord {
                table_ident: evidence.table_ident.clone(),
                commit_op_kind: format!("{:?}", evidence.op_kind),
                base_snapshot_id: evidence.base_snapshot_id,
                base_sequence_number: Some(evidence.base_sequence_number),
                staging_dir: evidence.staging_dir.clone(),
            }),
            failure: Some(IcebergOperationFailureRecord {
                kind: IcebergOperationFailureKind::Unknown,
                message: message.clone(),
                next_action: IcebergOperationNextAction::ManualInspect,
            }),
        },
    }
}

pub fn operation_fact_from_finalize_failure(message: String) -> IcebergOperationFact {
    IcebergOperationFact {
        state: IcebergOperationState::FinalizeFailedKnownCommitted,
        commit_outcome: None,
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
            message,
            next_action: IcebergOperationNextAction::RetryFinalize,
        }),
    }
}

fn cleanup_outcome_from_attempt(cleanup: &CleanupAttempt) -> IcebergCleanupOutcomeRecord {
    IcebergCleanupOutcomeRecord {
        attempted: cleanup.attempted,
        error_count: cleanup.error_count as i64,
        error_paths: cleanup.error_paths.clone(),
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::iceberg::commit::{
        CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, RecoveryEvidence,
    };
    use crate::meta::repository::iceberg_operation::{
        IcebergOperationFailureKind, IcebergOperationNextAction, IcebergOperationState,
    };

    use super::*;

    #[test]
    fn committed_outcome_maps_to_committed_state_and_snapshot_record() {
        let outcome = CommitOutcome {
            new_snapshot_id: 99,
            written_manifest_paths: vec!["s3://warehouse/metadata/m0.avro".to_string()],
        };
        let fact = operation_fact_from_commit_result(Ok(&outcome));
        assert_eq!(fact.state, IcebergOperationState::Committed);
        assert_eq!(fact.commit_outcome.expect("outcome").snapshot_id, 99);
        assert_eq!(fact.failure, None);
        assert_eq!(fact.cleanup_outcome, None);
        assert_eq!(fact.recovery_evidence, None);
    }

    #[test]
    fn known_uncommitted_error_maps_cleanup_and_failure() {
        let error = CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            CleanupAttempt::completed(vec!["s3://warehouse/data/a.parquet".to_string()]),
        );
        let fact = operation_fact_from_commit_result(Err(&error));
        assert_eq!(fact.state, IcebergOperationState::FailedKnownUncommitted);
        assert_eq!(
            fact.failure.as_ref().expect("failure").kind,
            IcebergOperationFailureKind::KnownUncommitted
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").next_action,
            IcebergOperationNextAction::None
        );
        assert_eq!(
            fact.cleanup_outcome.as_ref().expect("cleanup").attempted,
            true
        );
        assert_eq!(
            fact.cleanup_outcome.as_ref().expect("cleanup").error_count,
            1
        );
        assert_eq!(fact.recovery_evidence, None);
    }

    #[test]
    fn unknown_error_maps_to_commit_unknown_with_manual_inspect() {
        let error = CommitServiceError::unknown(
            "connection reset by peer".to_string(),
            RecoveryEvidence {
                table_ident: "ice.sales.orders".to_string(),
                op_kind: CommitOpKind::FastAppend,
                base_snapshot_id: Some(42),
                base_sequence_number: 7,
                staging_dir: "s3://warehouse/orders/_staging/attempt-1".to_string(),
            },
        );
        let fact = operation_fact_from_commit_result(Err(&error));
        assert_eq!(fact.state, IcebergOperationState::CommitUnknown);
        assert_eq!(
            fact.failure.as_ref().expect("failure").kind,
            IcebergOperationFailureKind::Unknown
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").next_action,
            IcebergOperationNextAction::ManualInspect
        );
        assert_eq!(
            fact.recovery_evidence
                .as_ref()
                .expect("evidence")
                .staging_dir,
            "s3://warehouse/orders/_staging/attempt-1"
        );
        assert_eq!(fact.cleanup_outcome, None);
    }

    #[test]
    fn finalize_failure_maps_to_known_committed_failure() {
        let fact = operation_fact_from_finalize_failure("mv metadata update failed".to_string());
        assert_eq!(
            fact.state,
            IcebergOperationState::FinalizeFailedKnownCommitted
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").kind,
            IcebergOperationFailureKind::FinalizeKnownCommitted
        );
        assert_eq!(
            fact.failure.as_ref().expect("failure").next_action,
            IcebergOperationNextAction::RetryFinalize
        );
    }
}
