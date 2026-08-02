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
use sha2::{Digest, Sha256};

/// The largest provider-controlled byte field that can be retained in the
/// frontend MV refresh ledger. This mirrors the SPI evidence bound so a
/// persisted refresh cannot become an unbounded StateStore record.
pub const MAX_FRONTEND_MV_REFRESH_EVIDENCE_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshExternalOutcome {
    pub target_snapshot_id: Option<i64>,
    pub commit_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshCommitMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvRefresh {
    pub refresh_id: i64,
    pub mv_id: i64,
    #[serde(default)]
    pub operation_id: Option<i64>,
    pub state: MvRefreshState,
    #[serde(default)]
    pub target_catalog: Option<String>,
    #[serde(default)]
    pub target_namespace: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    #[serde(default)]
    pub staging_branch: Option<String>,
    #[serde(default)]
    pub expected_main_snapshot_id: Option<i64>,
    #[serde(default)]
    pub staging_snapshot_id: Option<i64>,
    #[serde(default)]
    pub published_snapshot_id: Option<i64>,
    #[serde(default)]
    pub target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub base_table_uuids: BTreeMap<String, String>,
    #[serde(default)]
    pub rows: Option<i64>,
    #[serde(default)]
    pub marker: Option<RefreshCommitMarker>,
    #[serde(default)]
    pub external_outcome: Option<RefreshExternalOutcome>,
    /// v3 records are owned by the frontend. Historical v1/v2 records decode
    /// to `LegacyCore` and must remain exclusively recoverable by the legacy
    /// adapter.
    #[serde(default)]
    pub lifecycle_owner: MvRefreshLifecycleOwner,
    #[serde(default)]
    pub frontend_ledger: Option<FrontendMvRefreshLedger>,
    /// v4 recovery state is frontend-owned. v1-v3 records decode with no
    /// value and are upgraded atomically before a startup inspection runs.
    #[serde(default)]
    pub frontend_recovery: Option<FrontendMvRefreshRecoveryLedger>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvRefreshLifecycleOwner {
    #[default]
    LegacyCore,
    FrontendCurrent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FrontendMvRefreshActionPhase {
    StagingCreate,
    Write,
    Publication,
    StagingDrop,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FrontendMvRefreshActionState {
    Prepared,
    KnownUncommitted,
    KnownCommitted,
    CommitUnknown,
}

/// Provider-neutral proof retained by the frontend. Payloads are opaque: only
/// their digest is checked here; providers remain the sole decoder.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshEvidence {
    #[serde(default)]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub digest: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshCommittedVersion {
    pub payload: Vec<u8>,
    pub digest: Vec<u8>,
    #[serde(default)]
    pub snapshot_id: Option<i64>,
}

impl FrontendMvRefreshCommittedVersion {
    pub fn try_new(payload: Vec<u8>, snapshot_id: Option<i64>) -> Result<Self, String> {
        let version = Self {
            digest: committed_version_digest(&payload, snapshot_id).to_vec(),
            payload,
            snapshot_id,
        };
        validate_committed_version(&version)?;
        Ok(version)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshAction {
    pub phase: FrontendMvRefreshActionPhase,
    pub state: FrontendMvRefreshActionState,
    pub operation_id: Vec<u8>,
    #[serde(default)]
    pub receipt: Option<FrontendMvRefreshEvidence>,
    #[serde(default)]
    pub committed_version: Option<FrontendMvRefreshCommittedVersion>,
    #[serde(default)]
    pub external_evidence: Option<FrontendMvRefreshEvidence>,
    #[serde(default)]
    pub provider_finalized: bool,
}

/// StateStore payload for a frontend-owned v3 refresh attempt. It contains
/// values and opaque provider facts only; repositories, catalog clients and
/// execution-local programs never cross this persistence boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshLedger {
    pub request_id: Vec<u8>,
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: Vec<u8>,
    #[serde(default)]
    pub expected_target_version: Option<FrontendMvRefreshCommittedVersion>,
    pub staging_create_operation_id: Vec<u8>,
    pub write_operation_id: Vec<u8>,
    pub publication_operation_id: Vec<u8>,
    pub staging_drop_operation_id: Vec<u8>,
    #[serde(default)]
    pub cohort_ids: Vec<String>,
    #[serde(default)]
    pub actions: Vec<FrontendMvRefreshAction>,
    #[serde(default)]
    pub cleanup_pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FrontendMvRefreshRecoveryStatus {
    Pending,
    Inspecting,
    ResolvedAborted,
    ResolvedPublished,
    CleanupPending,
    Unresolved,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FrontendMvRefreshRecoveryDisposition {
    KnownUncommitted,
    Staged,
    Published,
    Superseded,
    CleanupPending,
    Ambiguous,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshRecoveryBaseFact {
    pub table: String,
    pub uuid: String,
    #[serde(default)]
    pub from_snapshot: Option<i64>,
    pub to_snapshot: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshRecoveryObservation {
    pub disposition: FrontendMvRefreshRecoveryDisposition,
    pub digest: Vec<u8>,
    pub proof: FrontendMvRefreshEvidence,
    #[serde(default)]
    pub committed_version: Option<FrontendMvRefreshCommittedVersion>,
    #[serde(default)]
    pub resulting_row_count: Option<i64>,
    #[serde(default)]
    pub bases: Vec<FrontendMvRefreshRecoveryBaseFact>,
    #[serde(default)]
    pub definition_fingerprint: Option<String>,
    #[serde(default)]
    pub staging_snapshot_id: Option<i64>,
    #[serde(default)]
    pub target_snapshot_id: Option<i64>,
    #[serde(default)]
    pub cleanup_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontendMvRefreshRecoveryLedger {
    pub status: FrontendMvRefreshRecoveryStatus,
    #[serde(default)]
    pub cycle_id: Option<Vec<u8>>,
    #[serde(default)]
    pub inspection_provider_id: Option<String>,
    #[serde(default)]
    pub inspection_instance_id: Option<String>,
    #[serde(default)]
    pub inspection_incarnation: Option<Vec<u8>>,
    pub cleanup_operation_id: Vec<u8>,
    #[serde(default)]
    pub observation: Option<FrontendMvRefreshRecoveryObservation>,
    #[serde(default)]
    pub cleanup_state: Option<FrontendMvRefreshActionState>,
    #[serde(default)]
    pub cleanup_evidence: Option<FrontendMvRefreshEvidence>,
    #[serde(default)]
    pub provider_finalized: bool,
    #[serde(default)]
    pub last_unresolved_reason: Option<String>,
}

impl FrontendMvRefreshRecoveryLedger {
    pub fn pending(cleanup_operation_id: Vec<u8>) -> Result<Self, String> {
        validate_identity("recovery cleanup_operation_id", &cleanup_operation_id)?;
        Ok(Self {
            status: FrontendMvRefreshRecoveryStatus::Pending,
            cycle_id: None,
            inspection_provider_id: None,
            inspection_instance_id: None,
            inspection_incarnation: None,
            cleanup_operation_id,
            observation: None,
            cleanup_state: None,
            cleanup_evidence: None,
            provider_finalized: false,
            last_unresolved_reason: None,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identity("recovery cleanup_operation_id", &self.cleanup_operation_id)?;
        if let Some(cycle_id) = &self.cycle_id {
            validate_identity("recovery cycle_id", cycle_id)?;
        }
        if self
            .inspection_provider_id
            .as_ref()
            .is_some_and(String::is_empty)
            || self
                .inspection_instance_id
                .as_ref()
                .is_some_and(String::is_empty)
            || self
                .inspection_incarnation
                .as_ref()
                .is_some_and(|value| value.len() != 16)
        {
            return Err("frontend MV refresh recovery has an invalid inspection identity".into());
        }
        if let Some(observation) = &self.observation {
            if observation.digest.len() != 32 {
                return Err(
                    "frontend MV refresh recovery observation requires a 32-byte digest".into(),
                );
            }
            validate_evidence(&observation.proof)?;
            if observation.resulting_row_count.is_some_and(|rows| rows < 0) {
                return Err("frontend MV refresh recovery row count cannot be negative".into());
            }
            if observation.bases.len() > 4096 {
                return Err("frontend MV refresh recovery has too many base facts".into());
            }
            if let Some(version) = &observation.committed_version {
                validate_committed_version(version)?;
            }
        }
        if let Some(evidence) = &self.cleanup_evidence {
            validate_evidence(evidence)?;
        }
        if self
            .last_unresolved_reason
            .as_ref()
            .is_some_and(|reason| reason.len() > 4096)
        {
            return Err(
                "frontend MV refresh recovery unresolved reason exceeds the bounded limit".into(),
            );
        }
        Ok(())
    }
}

impl FrontendMvRefreshLedger {
    pub fn validate(&self) -> Result<(), String> {
        validate_identity("request_id", &self.request_id)?;
        validate_identity("incarnation", &self.incarnation)?;
        for (name, value) in [
            (
                "staging_create_operation_id",
                &self.staging_create_operation_id,
            ),
            ("write_operation_id", &self.write_operation_id),
            ("publication_operation_id", &self.publication_operation_id),
            ("staging_drop_operation_id", &self.staging_drop_operation_id),
        ] {
            validate_identity(name, value)?;
        }
        if self.provider_id.is_empty() || self.instance_id.is_empty() {
            return Err("frontend MV refresh ledger requires provider and instance IDs".into());
        }
        if self.actions.len() > 4 {
            return Err("frontend MV refresh ledger has more than four external actions".into());
        }
        if !self.actions.is_empty() {
            if self.actions.len() != 4 {
                return Err("frontend MV refresh ledger must retain all four action phases".into());
            }
            for (phase, operation_id) in [
                (
                    FrontendMvRefreshActionPhase::StagingCreate,
                    &self.staging_create_operation_id,
                ),
                (
                    FrontendMvRefreshActionPhase::Write,
                    &self.write_operation_id,
                ),
                (
                    FrontendMvRefreshActionPhase::Publication,
                    &self.publication_operation_id,
                ),
                (
                    FrontendMvRefreshActionPhase::StagingDrop,
                    &self.staging_drop_operation_id,
                ),
            ] {
                let action = self
                    .actions
                    .iter()
                    .find(|action| action.phase == phase)
                    .ok_or_else(|| {
                        format!("frontend MV refresh ledger is missing prepared {phase:?} action")
                    })?;
                if action.operation_id != *operation_id {
                    return Err(format!(
                        "frontend MV refresh {phase:?} action does not use its preallocated operation ID"
                    ));
                }
            }
        }
        if let Some(version) = &self.expected_target_version {
            validate_committed_version(version)?;
        }
        for action in &self.actions {
            validate_identity("action operation_id", &action.operation_id)?;
            if let Some(receipt) = &action.receipt {
                validate_evidence(receipt)?;
            }
            if let Some(version) = &action.committed_version {
                validate_committed_version(version)?;
            }
            if let Some(evidence) = &action.external_evidence {
                validate_evidence(evidence)?;
            }
        }
        Ok(())
    }
}

fn validate_identity(name: &str, value: &[u8]) -> Result<(), String> {
    if value.len() == 16 {
        Ok(())
    } else {
        Err(format!("frontend MV refresh {name} must be a 16-byte UUID"))
    }
}

fn validate_evidence(evidence: &FrontendMvRefreshEvidence) -> Result<(), String> {
    if evidence.payload.len() > MAX_FRONTEND_MV_REFRESH_EVIDENCE_BYTES {
        return Err("frontend MV refresh evidence exceeds 64 KiB".into());
    }
    if evidence.payload.is_empty() != evidence.digest.is_empty() || evidence.digest.len() != 32 {
        return Err("frontend MV refresh evidence requires a 32-byte digest".into());
    }
    if evidence.digest.as_slice() != Sha256::digest(&evidence.payload).as_slice() {
        return Err("frontend MV refresh evidence digest does not match payload".into());
    }
    Ok(())
}

fn validate_committed_version(version: &FrontendMvRefreshCommittedVersion) -> Result<(), String> {
    if version.payload.len() > MAX_FRONTEND_MV_REFRESH_EVIDENCE_BYTES {
        return Err("frontend MV committed version exceeds 64 KiB".into());
    }
    if version.digest.len() != 32
        || version.digest.as_slice()
            != committed_version_digest(&version.payload, version.snapshot_id).as_slice()
    {
        return Err("frontend MV committed version digest does not match payload".into());
    }
    if matches!(version.snapshot_id, Some(snapshot_id) if snapshot_id <= 0) {
        return Err("frontend MV committed version snapshot ID must be positive".into());
    }
    Ok(())
}

fn committed_version_digest(payload: &[u8], snapshot_id: Option<i64>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-committed-version.v1\0");
    hasher.update(snapshot_id.unwrap_or_default().to_be_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvRefreshState {
    IntentCreated,
    StagingCommitted,
    #[serde(alias = "EXTERNAL_COMMITTED")]
    PublishCommitted,
    Finalized,
    AbortRequested,
    Aborted,
    CommitUnknown,
}

impl MvRefreshState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::StagingCommitted => "STAGING_COMMITTED",
            Self::PublishCommitted => "PUBLISH_COMMITTED",
            Self::Finalized => "FINALIZED",
            Self::AbortRequested => "ABORT_REQUESTED",
            Self::Aborted => "ABORTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginIcebergMvRefreshRequest {
    pub mv_id: i64,
    pub operation_id: Option<i64>,
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub marker_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordStagingCommitRequest {
    pub refresh_id: i64,
    pub staging_snapshot_id: i64,
    pub rows: i64,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPublishCommitRequest {
    pub refresh_id: i64,
    pub published_snapshot_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshFinalizeRequest {
    pub refresh_id: i64,
    pub rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub target_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateStarRocksMvRefreshSummaryRequest {
    pub mv_id: i64,
    pub last_refresh_ms: i64,
    pub last_refresh_rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> FrontendMvRefreshLedger {
        let mut ledger = FrontendMvRefreshLedger {
            request_id: vec![1; 16],
            provider_id: "iceberg".to_string(),
            instance_id: "rest".to_string(),
            incarnation: vec![2; 16],
            expected_target_version: Some(
                FrontendMvRefreshCommittedVersion::try_new(vec![3; 8], Some(7))
                    .expect("committed version"),
            ),
            staging_create_operation_id: vec![4; 16],
            write_operation_id: vec![5; 16],
            publication_operation_id: vec![6; 16],
            staging_drop_operation_id: vec![7; 16],
            cohort_ids: vec!["cohort-a".to_string()],
            actions: Vec::new(),
            cleanup_pending: false,
        };
        ledger.actions = [
            (
                FrontendMvRefreshActionPhase::StagingCreate,
                ledger.staging_create_operation_id.clone(),
            ),
            (
                FrontendMvRefreshActionPhase::Write,
                ledger.write_operation_id.clone(),
            ),
            (
                FrontendMvRefreshActionPhase::Publication,
                ledger.publication_operation_id.clone(),
            ),
            (
                FrontendMvRefreshActionPhase::StagingDrop,
                ledger.staging_drop_operation_id.clone(),
            ),
        ]
        .into_iter()
        .map(|(phase, operation_id)| FrontendMvRefreshAction {
            phase,
            state: FrontendMvRefreshActionState::Prepared,
            operation_id,
            receipt: None,
            committed_version: None,
            external_evidence: None,
            provider_finalized: false,
        })
        .collect();
        ledger
    }

    #[test]
    fn frontend_ledger_requires_all_preallocated_action_ids() {
        let ledger = ledger();
        ledger.validate().expect("complete prepared ledger");

        let mut corrupted = ledger;
        corrupted.actions[1].operation_id = vec![9; 16];
        assert!(
            corrupted
                .validate()
                .expect_err("mismatched action ID must fail")
                .contains("preallocated operation ID")
        );
    }

    #[test]
    fn frontend_ledger_rejects_oversized_opaque_evidence() {
        let mut ledger = ledger();
        let payload = vec![0; MAX_FRONTEND_MV_REFRESH_EVIDENCE_BYTES + 1];
        ledger.actions[0].receipt = Some(FrontendMvRefreshEvidence {
            digest: Sha256::digest(&payload).to_vec(),
            payload,
        });
        assert!(
            ledger
                .validate()
                .expect_err("oversized evidence must fail")
                .contains("exceeds 64 KiB")
        );
    }

    #[test]
    fn frontend_recovery_ledger_requires_bounded_exact_identities() {
        let recovery =
            FrontendMvRefreshRecoveryLedger::pending(vec![8; 16]).expect("pending recovery ledger");
        recovery.validate().expect("pending recovery is valid");

        let mut invalid_recovery = recovery;
        invalid_recovery.cycle_id = Some(vec![9; 15]);
        assert!(
            invalid_recovery
                .validate()
                .expect_err("short cycle identity must fail")
                .contains("cycle_id")
        );
    }
}
