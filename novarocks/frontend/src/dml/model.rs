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
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use novarocks::connector::iceberg::commit::{
    CleanupAttempt, CommitOpKind, CommitOutcome, CommitServiceError, RecoveryEvidence,
};

pub const DML_OPERATION_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DmlOperationId(Uuid);

impl DmlOperationId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for DmlOperationId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl fmt::Display for DmlOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationKind {
    InsertAppend,
    InsertOverwrite,
    RowDelta,
    MvRefresh,
    Maintenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationState {
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

impl OperationState {
    pub const fn as_str(self) -> &'static str {
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

    pub const fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::Aborted | Self::FailedKnownUncommitted
        )
    }
}

pub fn validate_operation_transition(
    from: OperationState,
    to: OperationState,
) -> Result<(), String> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (OperationState::Preparing, OperationState::Writing)
            | (OperationState::Preparing, OperationState::Committing)
            | (OperationState::Preparing, OperationState::Aborting)
            | (
                OperationState::Preparing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Writing, OperationState::Collecting)
            | (OperationState::Writing, OperationState::Committing)
            | (OperationState::Writing, OperationState::Aborting)
            | (
                OperationState::Writing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Collecting, OperationState::Committing)
            | (OperationState::Collecting, OperationState::Aborting)
            | (
                OperationState::Collecting,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Committing, OperationState::Committed)
            | (OperationState::Committing, OperationState::CommitUnknown)
            | (
                OperationState::Committing,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::CommitUnknown, OperationState::Committed)
            | (
                OperationState::CommitUnknown,
                OperationState::FailedKnownUncommitted
            )
            | (OperationState::Committed, OperationState::Finalizing)
            | (OperationState::Committed, OperationState::Finalized)
            | (OperationState::Finalizing, OperationState::Finalized)
            | (
                OperationState::Finalizing,
                OperationState::FinalizeFailedKnownCommitted
            )
            | (OperationState::Finalizing, OperationState::CommitUnknown)
            | (
                OperationState::FinalizeFailedKnownCommitted,
                OperationState::Finalizing
            )
            | (OperationState::Aborting, OperationState::Aborted)
            | (
                OperationState::Aborting,
                OperationState::FailedKnownUncommitted
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(format!(
            "invalid DML operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    #[serde(default)]
    pub ref_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationFailureKind {
    KnownUncommitted,
    Unknown,
    FinalizeKnownCommitted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationNextAction {
    None,
    RetryAbort,
    RetryFinalize,
    ManualInspect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergOperationFailureRecord {
    pub kind: IcebergOperationFailureKind,
    pub message: String,
    pub next_action: IcebergOperationNextAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergCommitOutcomeRecord {
    pub snapshot_id: i64,
    pub written_manifest_paths: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergCleanupOutcomeRecord {
    pub attempted: bool,
    pub error_count: i64,
    pub error_paths: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IcebergRecoveryEvidenceRecord {
    pub table_ident: String,
    pub commit_op_kind: String,
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: Option<i64>,
    pub staging_dir: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationFact {
    pub state: OperationState,
    pub commit_outcome: Option<IcebergCommitOutcomeRecord>,
    pub cleanup_outcome: Option<IcebergCleanupOutcomeRecord>,
    pub recovery_evidence: Option<IcebergRecoveryEvidenceRecord>,
    pub failure: Option<IcebergOperationFailureRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePreparingRequest {
    pub operation_kind: OperationKind,
    pub operation_subkind: Option<String>,
    pub target: OperationTarget,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOperation {
    pub schema_version: u8,
    pub operation_id: DmlOperationId,
    pub revision: u64,
    pub last_mutation_id: Uuid,
    pub operation_kind: OperationKind,
    #[serde(default)]
    pub operation_subkind: Option<String>,
    pub target: OperationTarget,
    pub state: OperationState,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
    pub staged_artifacts: Vec<String>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionSpec {
    pub target: OperationTarget,
    pub operation_kind: OperationKind,
    pub commit_op_kind: CommitOpKind,
    pub attempt_id: String,
    pub base_snapshot_id: Option<i64>,
    pub base_snapshot_map: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteTransactionOutcome {
    pub operation_id: Option<DmlOperationId>,
    pub committed_snapshot_id: Option<i64>,
}
