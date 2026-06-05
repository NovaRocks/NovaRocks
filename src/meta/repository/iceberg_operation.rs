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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::meta::keys::NS_ICEBERG_OPERATION;
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};

const ICEBERG_OPERATION_KIND: &str = "iceberg.operation";

#[derive(Default)]
pub struct IcebergOperationRepository;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationKind {
    InsertAppend,
    InsertOverwrite,
    RowDelta,
    MvRefresh,
    Maintenance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationState {
    Preparing,
    Writing,
    Collecting,
    Committing,
    Committed,
    CommitUnknown,
    Finalizing,
    Finalized,
    Aborting,
    Aborted,
    FailedKnownUncommitted,
    FinalizeFailedKnownCommitted,
}

impl IcebergOperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Writing => "WRITING",
            Self::Collecting => "COLLECTING",
            Self::Committing => "COMMITTING",
            Self::Committed => "COMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::Finalizing => "FINALIZING",
            Self::Finalized => "FINALIZED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::FailedKnownUncommitted => "FAILED_KNOWN_UNCOMMITTED",
            Self::FinalizeFailedKnownCommitted => "FINALIZE_FAILED_KNOWN_COMMITTED",
        }
    }

    pub fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::Aborted | Self::FailedKnownUncommitted
        )
    }
}

pub fn validate_operation_transition(
    from: IcebergOperationState,
    to: IcebergOperationState,
) -> RepositoryResult<()> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (
            IcebergOperationState::Preparing,
            IcebergOperationState::Writing
        ) | (
            IcebergOperationState::Preparing,
            IcebergOperationState::Committing
        ) | (
            IcebergOperationState::Preparing,
            IcebergOperationState::Aborting
        ) | (
            IcebergOperationState::Preparing,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::Collecting
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::Committing
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::Aborting
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Collecting,
            IcebergOperationState::Committing
        ) | (
            IcebergOperationState::Collecting,
            IcebergOperationState::Aborting
        ) | (
            IcebergOperationState::Collecting,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Committing,
            IcebergOperationState::Committed
        ) | (
            IcebergOperationState::Committing,
            IcebergOperationState::CommitUnknown
        ) | (
            IcebergOperationState::Committing,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::Committed
        ) | (
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Committed,
            IcebergOperationState::Finalizing
        ) | (
            IcebergOperationState::Committed,
            IcebergOperationState::Finalized
        ) | (
            IcebergOperationState::Finalizing,
            IcebergOperationState::Finalized
        ) | (
            IcebergOperationState::Finalizing,
            IcebergOperationState::FinalizeFailedKnownCommitted
        ) | (
            IcebergOperationState::Finalizing,
            IcebergOperationState::CommitUnknown
        ) | (
            IcebergOperationState::FinalizeFailedKnownCommitted,
            IcebergOperationState::Finalizing
        ) | (
            IcebergOperationState::Aborting,
            IcebergOperationState::Aborted
        ) | (
            IcebergOperationState::Aborting,
            IcebergOperationState::FailedKnownUncommitted
        )
    );
    if allowed {
        Ok(())
    } else {
        Err(RepositoryError::conflict(format!(
            "invalid Iceberg operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergOperationTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    #[serde(default)]
    pub ref_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationFailureKind {
    KnownUncommitted,
    Unknown,
    FinalizeKnownCommitted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationNextAction {
    None,
    RetryAbort,
    RetryFinalize,
    ManualInspect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergOperationFailureRecord {
    pub kind: IcebergOperationFailureKind,
    pub message: String,
    pub next_action: IcebergOperationNextAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergCommitOutcomeRecord {
    pub snapshot_id: i64,
    pub written_manifest_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergCleanupOutcomeRecord {
    pub attempted: bool,
    pub error_count: i64,
    pub error_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergRecoveryEvidenceRecord {
    pub table_ident: String,
    pub commit_op_kind: String,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: Option<i64>,
    pub staging_dir: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredIcebergOperation {
    pub operation_id: i64,
    pub operation_kind: IcebergOperationKind,
    pub target: IcebergOperationTarget,
    pub state: IcebergOperationState,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    #[serde(default)]
    pub commit_request: Option<String>,
    #[serde(default)]
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    #[serde(default)]
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    #[serde(default)]
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    #[serde(default)]
    pub failure: Option<IcebergOperationFailureRecord>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedIcebergOperation {
    pub record_revision: MetaRevision,
    pub value: StoredIcebergOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIcebergOperationRequest {
    pub operation_kind: IcebergOperationKind,
    pub target: IcebergOperationTarget,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergOperationFactUpdate {
    pub operation_id: i64,
    pub state: IcebergOperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
    pub now_ms: i64,
}

impl IcebergOperationRepository {
    pub fn create_operation(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateIcebergOperationRequest,
    ) -> RepositoryResult<StoredIcebergOperation> {
        let operation_id = txn.allocate_id(id_scopes::iceberg_operation())?;
        let stored = StoredIcebergOperation {
            operation_id,
            operation_kind: req.operation_kind,
            target: req.target,
            state: IcebergOperationState::Preparing,
            attempt_id: req.attempt_id,
            base_snapshot_id: req.base_snapshot_id,
            base_snapshot_map: req.base_snapshot_map,
            staged_artifacts: req.staged_artifacts,
            commit_request: None,
            commit_outcome: None,
            cleanup_outcome: None,
            recovery_evidence: None,
            failure: None,
            created_at_ms: req.created_at_ms,
            updated_at_ms: req.created_at_ms,
            finished_at_ms: None,
        };
        put_operation(txn, &stored, ExpectedRevision::NotExists)?;
        Ok(stored)
    }

    pub fn load_operation(
        &self,
        txn: &dyn MetaReadTxn,
        operation_id: i64,
    ) -> RepositoryResult<Option<StoredIcebergOperation>> {
        Ok(load_versioned_operation(txn, operation_id)?.map(|versioned| versioned.value))
    }

    pub fn list_unfinished_operations(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<StoredIcebergOperation>> {
        Ok(txn
            .scan(&key_prefix_operation()?, None)?
            .into_iter()
            .map(decode_operation_record)
            .collect::<RepositoryResult<Vec<_>>>()?
            .into_iter()
            .map(|versioned| versioned.value)
            .filter(|operation| !operation.state.is_finished())
            .collect())
    }

    pub fn transition_operation(
        &self,
        txn: &mut dyn MetaWriteTxn,
        operation_id: i64,
        to_state: IcebergOperationState,
        now_ms: i64,
    ) -> RepositoryResult<()> {
        let mut versioned = load_versioned_operation(txn, operation_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("iceberg operation {operation_id} not found"))
        })?;
        validate_operation_transition(versioned.value.state, to_state)?;
        if versioned.value.state == to_state {
            return Ok(());
        }
        versioned.value.state = to_state;
        versioned.value.updated_at_ms = now_ms;
        if to_state.is_finished() {
            versioned.value.finished_at_ms = Some(now_ms);
        }
        put_operation(
            txn,
            &versioned.value,
            ExpectedRevision::Exact(versioned.record_revision),
        )
    }

    pub fn record_operation_fact(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: IcebergOperationFactUpdate,
    ) -> RepositoryResult<()> {
        let mut versioned = load_versioned_operation(txn, req.operation_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("iceberg operation {} not found", req.operation_id))
        })?;
        validate_operation_transition(versioned.value.state, req.state)?;
        if versioned.value.state == req.state {
            if operation_fact_fields_match(&versioned.value, &req) {
                return Ok(());
            }
            return Err(RepositoryError::conflict(format!(
                "conflicting Iceberg operation fact replay for operation {} in state {}",
                req.operation_id,
                req.state.as_str()
            )));
        }

        versioned.value.state = req.state;
        versioned.value.commit_outcome = req.commit_outcome;
        versioned.value.cleanup_outcome = req.cleanup_outcome;
        versioned.value.recovery_evidence = req.recovery_evidence;
        versioned.value.failure = req.failure;
        versioned.value.updated_at_ms = req.now_ms;
        if req.state.is_finished() {
            versioned.value.finished_at_ms = Some(req.now_ms);
        }
        put_operation(
            txn,
            &versioned.value,
            ExpectedRevision::Exact(versioned.record_revision),
        )
    }
}

fn operation_fact_fields_match(
    operation: &StoredIcebergOperation,
    req: &IcebergOperationFactUpdate,
) -> bool {
    operation.commit_outcome.as_ref() == req.commit_outcome.as_ref()
        && operation.cleanup_outcome.as_ref() == req.cleanup_outcome.as_ref()
        && operation.recovery_evidence.as_ref() == req.recovery_evidence.as_ref()
        && operation.failure.as_ref() == req.failure.as_ref()
}

fn load_versioned_operation(
    txn: &dyn MetaReadTxn,
    operation_id: i64,
) -> RepositoryResult<Option<VersionedIcebergOperation>> {
    txn.get(&key_operation(operation_id)?)?
        .map(decode_operation_record)
        .transpose()
}

fn decode_operation_record(record: MetaRecord) -> RepositoryResult<VersionedIcebergOperation> {
    if record.kind.as_str() != ICEBERG_OPERATION_KIND {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {ICEBERG_OPERATION_KIND}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    let value =
        decode_payload_for_kind(ICEBERG_OPERATION_KIND, &record.payload).map_err(|err| {
            RepositoryError::provider(format!(
                "failed to decode metadata record {} as {ICEBERG_OPERATION_KIND}: {err}",
                record.key.canonical_path()
            ))
        })?;
    Ok(VersionedIcebergOperation {
        record_revision: record.revision,
        value,
    })
}

fn put_operation(
    txn: &mut dyn MetaWriteTxn,
    operation: &StoredIcebergOperation,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_operation(operation.operation_id)?,
        record_kind(ICEBERG_OPERATION_KIND)?,
        expected,
        encode_record_payload(ICEBERG_OPERATION_KIND, operation)?,
    ))?;
    Ok(())
}

fn key_operation(operation_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_ICEBERG_OPERATION,
        ["by-id".to_string(), operation_id.to_string()],
    )?)
}

fn key_prefix_operation() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_ICEBERG_OPERATION, ["by-id"])?)
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::meta::repository::RepositoryErrorKind;

    #[test]
    fn operation_state_as_str_is_stable_for_diagnostics() {
        assert_eq!(IcebergOperationState::Preparing.as_str(), "PREPARING");
        assert_eq!(
            IcebergOperationState::CommitUnknown.as_str(),
            "COMMIT_UNKNOWN"
        );
        assert_eq!(
            IcebergOperationState::FinalizeFailedKnownCommitted.as_str(),
            "FINALIZE_FAILED_KNOWN_COMMITTED"
        );
    }

    #[test]
    fn transition_helper_allows_main_commit_path_and_idempotent_replay() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::Preparing,
                IcebergOperationState::Writing
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Writing,
                IcebergOperationState::Collecting
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Collecting,
                IcebergOperationState::Committing
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Committing,
                IcebergOperationState::Committed
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Committed,
                IcebergOperationState::Finalizing
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Finalizing,
                IcebergOperationState::Finalized
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::CommitUnknown,
                IcebergOperationState::CommitUnknown
            )
            .is_ok()
        );
    }

    #[test]
    fn transition_helper_rejects_commit_unknown_to_aborted() {
        let err = validate_operation_transition(
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::Aborted,
        )
        .expect_err("commit unknown must not be treated as aborted");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(err.to_string().contains("COMMIT_UNKNOWN"));
        assert!(err.to_string().contains("ABORTED"));
    }

    #[test]
    fn transition_helper_allows_commit_unknown_recovery_outcomes() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::CommitUnknown,
                IcebergOperationState::Committed
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::CommitUnknown,
                IcebergOperationState::FailedKnownUncommitted
            )
            .is_ok()
        );
    }

    #[test]
    fn transition_helper_routes_finalize_failure_to_known_committed_failure() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::Finalizing,
                IcebergOperationState::FinalizeFailedKnownCommitted
            )
            .is_ok()
        );
        assert!(!IcebergOperationState::FinalizeFailedKnownCommitted.is_finished());
        assert!(IcebergOperationState::Finalized.is_finished());
        assert!(IcebergOperationState::Aborted.is_finished());
        assert!(IcebergOperationState::FailedKnownUncommitted.is_finished());
    }

    #[test]
    fn transition_helper_retries_known_committed_finalize_failure_through_finalizing() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::FinalizeFailedKnownCommitted,
                IcebergOperationState::Finalizing
            )
            .is_ok()
        );
        let err = validate_operation_transition(
            IcebergOperationState::FinalizeFailedKnownCommitted,
            IcebergOperationState::Finalized,
        )
        .expect_err("finalize retry must pass through FINALIZING");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(err.to_string().contains("FINALIZE_FAILED_KNOWN_COMMITTED"));
        assert!(err.to_string().contains("FINALIZED"));
    }
}
