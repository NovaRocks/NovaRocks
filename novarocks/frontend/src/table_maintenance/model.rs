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

use crate::query_execution::maintenance::OptimizeJobState;
use bytes::Bytes;
use novarocks::maintenance::MaintenanceTarget;
use novarocks_spi::connector::ConnectorWriteExecutionId;
use novarocks_state_store::coordination::FencingToken;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::durable::{DurableOpaqueBytes, DurableRecord};

pub const OPTIMIZE_JOB_LEGACY_SCHEMA_VERSION: u8 = 1;
pub const OPTIMIZE_JOB_SCHEMA_VERSION: u8 = 2;
pub const METADATA_MAINTENANCE_OPERATION_LEGACY_SCHEMA_VERSION: u8 = 2;
pub const METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION: u8 = 3;
/// A metadata payload contributes at most 24 KiB after lowercase-hex durable
/// encoding. Together with the 48 KiB full-record budget this leaves more
/// than 20 KiB for target, owner, digests, lifecycle fields, and JSON framing.
pub const METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES: usize = 12 * 1024;
pub const METADATA_MAINTENANCE_RECORD_ENCODED_LIMIT: usize = 48 * 1024;
/// E2 stores only bounded, credential-free handles in StateStore.  The
/// provider-owned immutable manifest and reports live in object storage.
pub const DISTRIBUTED_REWRITE_OPERATION_LEGACY_SCHEMA_VERSION: u8 = 3;
pub const DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION: u8 = 4;
/// A rewrite payload contributes at most 24 KiB after durable encoding.
/// Operation, payload, attempt, and transaction records each have separate
/// 56 KiB budgets, leaving room for their fixed identifiers and framing.
pub const DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES: usize = 12 * 1024;
pub const DISTRIBUTED_REWRITE_MAX_ATTEMPT_HANDLE_BYTES: usize = 12 * 1024;
pub const DISTRIBUTED_REWRITE_RECORD_ENCODED_LIMIT: usize = 56 * 1024;
/// V4 cleanup records retain only bounded, credential-free provider artifact
/// handles. Candidate locations and object identities remain provider-owned.
pub const CLEANUP_OPERATION_LEGACY_SCHEMA_VERSION: u8 = 4;
pub const CLEANUP_OPERATION_SCHEMA_VERSION: u8 = 5;
/// Cleanup plan records contain one handle and batch records can contain two.
/// A 10 KiB raw handle becomes at most 20 KiB hex, so the two-handle batch
/// remains below its 56 KiB whole-record budget with fixed metadata included.
pub const CLEANUP_MAX_PAYLOAD_BYTES: usize = 10 * 1024;
pub const CLEANUP_RECORD_ENCODED_LIMIT: usize = 56 * 1024;
pub const CLEANUP_MAX_BATCHES: u16 = 256;
/// A fencing token contributes at most 8 KiB after durable hex encoding,
/// leaving the enclosing operation records well below their 48/56 KiB limits.
pub const MAINTENANCE_FENCING_TOKEN_MAX_BYTES: usize = 4 * 1024;
pub const OPTIMIZE_JOB_RECORD_ENCODED_LIMIT: usize = 48 * 1024;

/// Durable execution authority for one table-maintenance attempt.
///
/// The token is preserved in its StateStore coordination v1 wire form so a
/// repository record remains provider-neutral and can prove which exact lease
/// epoch was allowed to write it. `try_new` rejects non-canonical or
/// unbounded provenance rather than accepting a best-effort replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceAuthorityV1 {
    pub attempt_id: Uuid,
    pub fencing_token_v1: Vec<u8>,
}

impl MaintenanceAuthorityV1 {
    pub fn try_new(attempt_id: Uuid, fencing_token_v1: Vec<u8>) -> Result<Self, String> {
        DurableOpaqueBytes::<MAINTENANCE_FENCING_TOKEN_MAX_BYTES>::try_new(
            fencing_token_v1.clone(),
        )
        .map_err(|error| format!("maintenance authority fencing token is invalid: {error}"))?;
        let authority = Self {
            attempt_id,
            fencing_token_v1,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.attempt_id.get_version_num() != 7
            || self.attempt_id.get_variant() != uuid::Variant::RFC4122
        {
            return Err("maintenance authority attempt id must be UUIDv7".to_string());
        }
        let token = FencingToken::decode_v1(Bytes::copy_from_slice(&self.fencing_token_v1))
            .map_err(|error| format!("maintenance authority fencing token is invalid: {error}"))?;
        let canonical = token.encode_v1().map_err(|error| {
            format!("encode maintenance authority fencing token failed: {error}")
        })?;
        if canonical.as_ref() != self.fencing_token_v1.as_slice() {
            return Err("maintenance authority fencing token is non-canonical".to_string());
        }
        Ok(())
    }
}

impl Serialize for MaintenanceAuthorityV1 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct DurableAuthority {
            attempt_id: Uuid,
            fencing_token_v1: DurableOpaqueBytes<MAINTENANCE_FENCING_TOKEN_MAX_BYTES>,
        }

        let fencing_token_v1 =
            DurableOpaqueBytes::try_new(self.fencing_token_v1.clone()).map_err(S::Error::custom)?;
        DurableAuthority {
            attempt_id: self.attempt_id,
            fencing_token_v1,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MaintenanceAuthorityV1 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct DurableAuthority {
            attempt_id: Uuid,
            fencing_token_v1: DurableOpaqueBytes<MAINTENANCE_FENCING_TOKEN_MAX_BYTES>,
        }

        let durable = DurableAuthority::deserialize(deserializer)?;
        Ok(Self {
            attempt_id: durable.attempt_id,
            fencing_token_v1: durable.fencing_token_v1.as_bytes().to_vec(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobCreate {
    pub target: MaintenanceTarget,
    pub base_snapshot_id: i64,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJobOutcome {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    pub added_data_files: i64,
    pub output_record_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizeJob {
    pub job_id: i64,
    pub target: MaintenanceTarget,
    pub base_snapshot_id: i64,
    pub state: OptimizeJobState,
    pub outcome: Option<OptimizeJobOutcome>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    /// Set once this job has dispatched a distributed rewrite. Recovery must
    /// never re-execute a job that already has one.
    pub dispatched_child: Option<Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredMaintenanceTargetV1 {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoredOptimizeJobStateV1 {
    Pending,
    Running,
    Finished,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOptimizeOutcomeV1 {
    pub target_snapshot_id: Option<i64>,
    pub rewritten_data_files: i64,
    pub deleted_data_files: i64,
    pub added_data_files: i64,
    pub output_record_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOptimizeJobV1 {
    pub schema_version: u8,
    pub job_id: i64,
    pub target: StoredMaintenanceTargetV1,
    pub base_snapshot_id: i64,
    pub state: StoredOptimizeJobStateV1,
    pub outcome: Option<StoredOptimizeOutcomeV1>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub last_operation_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<MaintenanceAuthorityV1>,
    /// The distributed-rewrite operation this claimed job already dispatched.
    ///
    /// Recovery needs to tell "claimed but never dispatched" from "dispatched
    /// and the external outcome is unknown". Only the second case is unsafe to
    /// run again, so the link is written in the same transaction that creates
    /// the child. `None` on a RUNNING record proves no child exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_child: Option<Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredOptimizeCounterV1 {
    pub schema_version: u8,
    pub last_job_id: i64,
}

impl DurableRecord for StoredOptimizeCounterV1 {
    const RECORD_KIND: &'static str = "table-maintenance-optimize-counter";
    const SCHEMA_VERSION: u8 = OPTIMIZE_JOB_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = OPTIMIZE_JOB_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredOptimizeOperationActionV1 {
    Create,
    Claim,
    RecordOutcome,
    Finish,
    Fail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredOptimizeOperationV1 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub action: StoredOptimizeOperationActionV1,
    pub job_id: i64,
    pub post_job: StoredOptimizeJobV1,
}

impl DurableRecord for StoredOptimizeOperationV1 {
    const RECORD_KIND: &'static str = "table-maintenance-optimize-transaction";
    const SCHEMA_VERSION: u8 = OPTIMIZE_JOB_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = OPTIMIZE_JOB_RECORD_ENCODED_LIMIT;
}

impl DurableRecord for StoredOptimizeJobV1 {
    const RECORD_KIND: &'static str = "table-maintenance-optimize-job";
    const SCHEMA_VERSION: u8 = OPTIMIZE_JOB_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = OPTIMIZE_JOB_RECORD_ENCODED_LIMIT;
}

impl From<&MaintenanceTarget> for StoredMaintenanceTargetV1 {
    fn from(value: &MaintenanceTarget) -> Self {
        Self {
            catalog: value.catalog.clone(),
            namespace: value.namespace.clone(),
            table: value.table.clone(),
        }
    }
}

impl From<StoredMaintenanceTargetV1> for MaintenanceTarget {
    fn from(value: StoredMaintenanceTargetV1) -> Self {
        Self {
            catalog: value.catalog,
            namespace: value.namespace,
            table: value.table,
        }
    }
}

impl From<OptimizeJobState> for StoredOptimizeJobStateV1 {
    fn from(value: OptimizeJobState) -> Self {
        match value {
            OptimizeJobState::Pending => Self::Pending,
            OptimizeJobState::Running => Self::Running,
            OptimizeJobState::Finished => Self::Finished,
            OptimizeJobState::Failed => Self::Failed,
        }
    }
}

impl From<StoredOptimizeJobStateV1> for OptimizeJobState {
    fn from(value: StoredOptimizeJobStateV1) -> Self {
        match value {
            StoredOptimizeJobStateV1::Pending => Self::Pending,
            StoredOptimizeJobStateV1::Running => Self::Running,
            StoredOptimizeJobStateV1::Finished => Self::Finished,
            StoredOptimizeJobStateV1::Failed => Self::Failed,
        }
    }
}

impl From<&OptimizeJobOutcome> for StoredOptimizeOutcomeV1 {
    fn from(value: &OptimizeJobOutcome) -> Self {
        Self {
            target_snapshot_id: value.target_snapshot_id,
            rewritten_data_files: value.rewritten_data_files,
            deleted_data_files: value.deleted_data_files,
            added_data_files: value.added_data_files,
            output_record_count: value.output_record_count,
        }
    }
}

impl From<StoredOptimizeOutcomeV1> for OptimizeJobOutcome {
    fn from(value: StoredOptimizeOutcomeV1) -> Self {
        Self {
            target_snapshot_id: value.target_snapshot_id,
            rewritten_data_files: value.rewritten_data_files,
            deleted_data_files: value.deleted_data_files,
            added_data_files: value.added_data_files,
            output_record_count: value.output_record_count,
        }
    }
}

impl From<&StoredOptimizeJobV1> for OptimizeJob {
    fn from(value: &StoredOptimizeJobV1) -> Self {
        Self {
            job_id: value.job_id,
            target: value.target.clone().into(),
            base_snapshot_id: value.base_snapshot_id,
            state: value.state.into(),
            outcome: value.outcome.clone().map(Into::into),
            error_message: value.error_message.clone(),
            created_at_ms: value.created_at_ms,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
            dispatched_child: value.dispatched_child,
        }
    }
}

/// Durable, frontend-owned state for a metadata-only connector maintenance
/// operation.  This is deliberately separate from the v1 OPTIMIZE job model:
/// E1 operations run synchronously but still need a recovery record before any
/// external catalog dispatch occurs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataMaintenanceOperationKind {
    RewriteMetadataLayout,
    ExpireTableVersions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataMaintenanceOperationState {
    Pending,
    Running,
    ReconcilePending,
    Finished,
    Failed,
    Unresolved,
}

impl MetadataMaintenanceOperationState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }

    pub const fn holds_active_fence(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Running | Self::ReconcilePending | Self::Unresolved
        )
    }

    pub const fn as_key_component(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::ReconcilePending => "reconcile-pending",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Unresolved => "unresolved",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenanceOperationCreate {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: MetadataMaintenanceOperationKind,
    pub request_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub request_payload: Vec<u8>,
    pub created_at_ms: i64,
}

/// Persisted exact-generation identity.  It is stored as canonical primitive
/// fields rather than a process-local lease so recovery can reconstruct and
/// validate the exact `ConnectorExecutionBindingKey` before reconcile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataMaintenanceExactOwner {
    pub instance_id: String,
    pub incarnation_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenancePlanPayload {
    pub plan_digest: [u8; 32],
    pub payload_digest: [u8; 32],
    pub payload: Vec<u8>,
    pub summary: [u64; 5],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenanceOpaquePayload {
    pub digest: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMaintenanceOperation {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: MetadataMaintenanceOperationKind,
    pub request_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    pub plan_summary: Option<[u64; 5]>,
    pub state: MetadataMaintenanceOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetadataMaintenanceOperationV2 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub target: StoredMaintenanceTargetV1,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: MetadataMaintenanceOperationKind,
    pub request_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    #[serde(default)]
    pub plan_summary: Option<[u64; 5]>,
    pub state: MetadataMaintenanceOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<MaintenanceAuthorityV1>,
}

impl DurableRecord for StoredMetadataMaintenanceOperationV2 {
    const RECORD_KIND: &'static str = "table-maintenance-metadata-operation";
    const SCHEMA_VERSION: u8 = METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = METADATA_MAINTENANCE_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredMetadataMaintenanceTransactionActionV2 {
    Create,
    Start,
    ReconcilePending,
    Finish,
    Fail,
    Unresolve,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetadataMaintenanceTransactionV2 {
    pub schema_version: u8,
    pub transaction_operation_id: Uuid,
    pub action: StoredMetadataMaintenanceTransactionActionV2,
    pub operation_id: Uuid,
    pub post_operation: StoredMetadataMaintenanceOperationV2,
}

impl DurableRecord for StoredMetadataMaintenanceTransactionV2 {
    const RECORD_KIND: &'static str = "table-maintenance-metadata-transaction";
    const SCHEMA_VERSION: u8 = METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = METADATA_MAINTENANCE_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredMetadataMaintenancePayloadKindV2 {
    Request,
    Plan,
    Receipt,
    Evidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetadataMaintenancePayloadV2 {
    pub schema_version: u8,
    pub kind: StoredMetadataMaintenancePayloadKindV2,
    pub digest: [u8; 32],
    pub payload: DurableOpaqueBytes<METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES>,
}

impl DurableRecord for StoredMetadataMaintenancePayloadV2 {
    const RECORD_KIND: &'static str = "table-maintenance-metadata-payload";
    const SCHEMA_VERSION: u8 = METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = METADATA_MAINTENANCE_RECORD_ENCODED_LIMIT;
}

impl From<&StoredMetadataMaintenanceOperationV2> for MetadataMaintenanceOperation {
    fn from(value: &StoredMetadataMaintenanceOperationV2) -> Self {
        Self {
            operation_id: value.operation_id,
            target: value.target.clone().into(),
            owner: value.owner.clone(),
            kind: value.kind,
            request_digest: value.request_digest,
            request_payload_digest: value.request_payload_digest,
            base_state_digest: value.base_state_digest,
            plan_digest: value.plan_digest,
            plan_summary: value.plan_summary,
            state: value.state,
            error_message: value.error_message.clone(),
            created_at_ms: value.created_at_ms,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
        }
    }
}

/// Durable frontend state for a C1-backed connector rewrite operation.  The
/// record deliberately contains digests and bounded artifact handles only;
/// it never persists an Iceberg file list, credentials, or writer report.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DistributedRewriteOperationKind {
    RewriteDataFiles,
    RewritePositionDeleteFiles,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DistributedRewriteOperationState {
    Pending,
    Planned,
    Staging,
    AbortPending,
    CommitPending,
    ReconcilePending,
    Finished,
    Failed,
    Unresolved,
}

impl DistributedRewriteOperationState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }

    pub const fn holds_active_fence(self) -> bool {
        !self.is_terminal()
    }

    pub const fn as_key_component(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Planned => "planned",
            Self::Staging => "staging",
            Self::AbortPending => "abort-pending",
            Self::CommitPending => "commit-pending",
            Self::ReconcilePending => "reconcile-pending",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Unresolved => "unresolved",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedRewriteOperationCreate {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: DistributedRewriteOperationKind,
    pub request_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub request_payload: Vec<u8>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedRewritePlanPayload {
    pub plan_digest: [u8; 32],
    pub manifest_digest: [u8; 32],
    pub cohort_set_digest: [u8; 32],
    pub payload_digest: [u8; 32],
    pub payload: Vec<u8>,
    pub cohort_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedRewriteOpaquePayload {
    pub digest: [u8; 32],
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DistributedRewriteAttemptDisposition {
    Accepted,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedRewriteAttemptCheckpoint {
    pub cohort_id: [u8; 32],
    pub execution_id: ConnectorWriteExecutionId,
    pub disposition: DistributedRewriteAttemptDisposition,
    pub attempt_digest: [u8; 32],
    pub artifact_digest: [u8; 32],
    pub artifact_handle: Vec<u8>,
    pub checkpoint_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedRewriteOperation {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: DistributedRewriteOperationKind,
    pub request_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    pub manifest_digest: Option<[u8; 32]>,
    pub cohort_set_digest: Option<[u8; 32]>,
    pub cohort_count: Option<u32>,
    pub state: DistributedRewriteOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredDistributedRewriteOperationV3 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub target: StoredMaintenanceTargetV1,
    pub owner: MetadataMaintenanceExactOwner,
    pub kind: DistributedRewriteOperationKind,
    pub request_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub request_payload_digest: [u8; 32],
    pub plan_digest: Option<[u8; 32]>,
    pub manifest_digest: Option<[u8; 32]>,
    pub cohort_set_digest: Option<[u8; 32]>,
    pub cohort_count: Option<u32>,
    pub state: DistributedRewriteOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<MaintenanceAuthorityV1>,
}

impl DurableRecord for StoredDistributedRewriteOperationV3 {
    const RECORD_KIND: &'static str = "table-maintenance-distributed-rewrite-operation";
    const SCHEMA_VERSION: u8 = DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = DISTRIBUTED_REWRITE_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredDistributedRewritePayloadKindV3 {
    Request,
    Plan,
    Receipt,
    Evidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredDistributedRewritePayloadV3 {
    pub schema_version: u8,
    pub kind: StoredDistributedRewritePayloadKindV3,
    pub digest: [u8; 32],
    pub payload: DurableOpaqueBytes<DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES>,
}

impl DurableRecord for StoredDistributedRewritePayloadV3 {
    const RECORD_KIND: &'static str = "table-maintenance-distributed-rewrite-payload";
    const SCHEMA_VERSION: u8 = DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = DISTRIBUTED_REWRITE_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredDistributedRewriteAttemptV3 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub cohort_id: [u8; 32],
    pub execution_query_id: [u8; 16],
    pub execution_attempt_id: u64,
    pub disposition: DistributedRewriteAttemptDisposition,
    pub attempt_digest: [u8; 32],
    pub artifact_digest: [u8; 32],
    pub artifact_handle: DurableOpaqueBytes<DISTRIBUTED_REWRITE_MAX_ATTEMPT_HANDLE_BYTES>,
    pub checkpoint_digest: [u8; 32],
}

impl DurableRecord for StoredDistributedRewriteAttemptV3 {
    const RECORD_KIND: &'static str = "table-maintenance-distributed-rewrite-attempt";
    const SCHEMA_VERSION: u8 = DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = DISTRIBUTED_REWRITE_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredDistributedRewriteTransactionActionV3 {
    Create,
    Plan,
    StartStaging,
    Checkpoint,
    AbortPending,
    CommitPending,
    ReconcilePending,
    Finish,
    Fail,
    Unresolve,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredDistributedRewriteTransactionV3 {
    pub schema_version: u8,
    pub transaction_operation_id: Uuid,
    pub action: StoredDistributedRewriteTransactionActionV3,
    pub operation_id: Uuid,
    pub post_operation: StoredDistributedRewriteOperationV3,
}

impl DurableRecord for StoredDistributedRewriteTransactionV3 {
    const RECORD_KIND: &'static str = "table-maintenance-distributed-rewrite-transaction";
    const SCHEMA_VERSION: u8 = DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = DISTRIBUTED_REWRITE_RECORD_ENCODED_LIMIT;
}

impl From<&StoredDistributedRewriteOperationV3> for DistributedRewriteOperation {
    fn from(value: &StoredDistributedRewriteOperationV3) -> Self {
        Self {
            operation_id: value.operation_id,
            target: value.target.clone().into(),
            owner: value.owner.clone(),
            kind: value.kind,
            request_digest: value.request_digest,
            base_state_digest: value.base_state_digest,
            plan_digest: value.plan_digest,
            manifest_digest: value.manifest_digest,
            cohort_set_digest: value.cohort_set_digest,
            cohort_count: value.cohort_count,
            state: value.state,
            error_message: value.error_message.clone(),
            created_at_ms: value.created_at_ms,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
        }
    }
}

/// Durable state for a frontend-owned connector orphan cleanup. The state
/// machine intentionally keeps a fence on unresolved dispatch so that a later
/// connector incarnation cannot take over an exact-generation operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CleanupOperationState {
    Pending,
    Planned,
    Running,
    ReconcilePending,
    Finished,
    Failed,
    Unresolved,
}

impl CleanupOperationState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }

    pub const fn holds_active_fence(self) -> bool {
        !self.is_terminal()
    }

    pub const fn as_key_component(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Planned => "planned",
            Self::Running => "running",
            Self::ReconcilePending => "reconcile-pending",
            Self::Finished => "finished",
            Self::Failed => "failed",
            Self::Unresolved => "unresolved",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupOperationCreate {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub request_digest: [u8; 32],
    pub older_than_ms: i64,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupPlanPayload {
    pub plan_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub manifest_digest: [u8; 32],
    pub artifact_handle_digest: [u8; 32],
    pub artifact_handle: Vec<u8>,
    pub candidate_count: u32,
    pub total_bytes: u64,
    pub manifest_parts: u16,
    pub batch_count: u16,
}

/// The batch checkpoint has no paths, identity fields, or per-object errors.
/// Those remain in the immutable provider manifest and receipt artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupBatchCheckpoint {
    pub ordinal: u16,
    pub prepared_handle_digest: [u8; 32],
    pub prepared_handle: Vec<u8>,
    pub receipt_handle_digest: Option<[u8; 32]>,
    pub receipt_handle: Option<Vec<u8>>,
    pub deleted_count: u32,
    pub already_absent_count: u32,
    pub failed_count: u32,
    pub unknown_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupOperation {
    pub operation_id: Uuid,
    pub target: MaintenanceTarget,
    pub owner: MetadataMaintenanceExactOwner,
    pub request_digest: [u8; 32],
    pub older_than_ms: i64,
    pub plan_digest: Option<[u8; 32]>,
    pub manifest_digest: Option<[u8; 32]>,
    pub candidate_count: Option<u32>,
    pub batch_count: Option<u16>,
    pub next_batch_ordinal: u16,
    pub state: CleanupOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredCleanupOperationV4 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub target: StoredMaintenanceTargetV1,
    pub owner: MetadataMaintenanceExactOwner,
    pub request_digest: [u8; 32],
    pub older_than_ms: i64,
    pub plan_digest: Option<[u8; 32]>,
    pub manifest_digest: Option<[u8; 32]>,
    pub candidate_count: Option<u32>,
    pub batch_count: Option<u16>,
    pub next_batch_ordinal: u16,
    pub state: CleanupOperationState,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<MaintenanceAuthorityV1>,
}

impl DurableRecord for StoredCleanupOperationV4 {
    const RECORD_KIND: &'static str = "table-maintenance-cleanup-operation";
    const SCHEMA_VERSION: u8 = CLEANUP_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = CLEANUP_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredCleanupPlanV4 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub plan_digest: [u8; 32],
    pub base_state_digest: [u8; 32],
    pub manifest_digest: [u8; 32],
    pub artifact_handle_digest: [u8; 32],
    pub artifact_handle: DurableOpaqueBytes<CLEANUP_MAX_PAYLOAD_BYTES>,
    pub candidate_count: u32,
    pub total_bytes: u64,
    pub manifest_parts: u16,
    pub batch_count: u16,
}

impl DurableRecord for StoredCleanupPlanV4 {
    const RECORD_KIND: &'static str = "table-maintenance-cleanup-plan";
    const SCHEMA_VERSION: u8 = CLEANUP_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = CLEANUP_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredCleanupBatchV4 {
    pub schema_version: u8,
    pub operation_id: Uuid,
    pub ordinal: u16,
    pub prepared_handle_digest: [u8; 32],
    pub prepared_handle: DurableOpaqueBytes<CLEANUP_MAX_PAYLOAD_BYTES>,
    pub receipt_handle_digest: Option<[u8; 32]>,
    pub receipt_handle: Option<DurableOpaqueBytes<CLEANUP_MAX_PAYLOAD_BYTES>>,
    pub deleted_count: u32,
    pub already_absent_count: u32,
    pub failed_count: u32,
    pub unknown_count: u32,
}

impl DurableRecord for StoredCleanupBatchV4 {
    const RECORD_KIND: &'static str = "table-maintenance-cleanup-batch";
    const SCHEMA_VERSION: u8 = CLEANUP_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = CLEANUP_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StoredCleanupTransactionActionV4 {
    Create,
    Plan,
    Prepare,
    Checkpoint,
    ReconcilePending,
    Resume,
    Finish,
    Fail,
    Unresolve,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredCleanupTransactionV4 {
    pub schema_version: u8,
    pub transaction_operation_id: Uuid,
    pub action: StoredCleanupTransactionActionV4,
    pub operation_id: Uuid,
    pub post_operation: StoredCleanupOperationV4,
}

impl DurableRecord for StoredCleanupTransactionV4 {
    const RECORD_KIND: &'static str = "table-maintenance-cleanup-transaction";
    const SCHEMA_VERSION: u8 = CLEANUP_OPERATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = CLEANUP_RECORD_ENCODED_LIMIT;
}

impl From<&StoredCleanupOperationV4> for CleanupOperation {
    fn from(value: &StoredCleanupOperationV4) -> Self {
        Self {
            operation_id: value.operation_id,
            target: value.target.clone().into(),
            owner: value.owner.clone(),
            request_digest: value.request_digest,
            older_than_ms: value.older_than_ms,
            plan_digest: value.plan_digest,
            manifest_digest: value.manifest_digest,
            candidate_count: value.candidate_count,
            batch_count: value.batch_count,
            next_batch_ordinal: value.next_batch_ordinal,
            state: value.state,
            error_message: value.error_message.clone(),
            created_at_ms: value.created_at_ms,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
        }
    }
}

#[cfg(test)]
mod durable_record_budget_tests {
    use novarocks_spi::state_store::{MAX_VALUE_BYTES, StateStoreLimits};

    use super::*;
    use crate::durable::{DurableRecordError, DurableRecordStore};

    const SENTINEL: &str = "maintenance-opaque-budget-sentinel";

    fn opaque<const MAX_BYTES: usize>(bytes: usize) -> DurableOpaqueBytes<MAX_BYTES> {
        let payload = SENTINEL
            .as_bytes()
            .iter()
            .copied()
            .cycle()
            .take(bytes)
            .collect();
        DurableOpaqueBytes::try_new(payload).expect("bounded opaque payload")
    }

    fn authority() -> MaintenanceAuthorityV1 {
        // Serialization deliberately tests the largest accepted durable token;
        // validation of a provider-issued token is covered at the authority
        // boundary, not by this record-size proof.
        MaintenanceAuthorityV1 {
            attempt_id: Uuid::nil(),
            fencing_token_v1: vec![0xa5; MAINTENANCE_FENCING_TOKEN_MAX_BYTES],
        }
    }

    fn target() -> StoredMaintenanceTargetV1 {
        StoredMaintenanceTargetV1 {
            catalog: "c".repeat(1024),
            namespace: "n".repeat(1024),
            table: "t".repeat(1024),
        }
    }

    fn owner() -> MetadataMaintenanceExactOwner {
        MetadataMaintenanceExactOwner {
            instance_id: "i".repeat(1024),
            incarnation_id: Uuid::nil(),
        }
    }

    fn metadata_operation() -> StoredMetadataMaintenanceOperationV2 {
        StoredMetadataMaintenanceOperationV2 {
            schema_version: METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            target: target(),
            owner: owner(),
            kind: MetadataMaintenanceOperationKind::RewriteMetadataLayout,
            request_digest: [1; 32],
            request_payload_digest: [2; 32],
            base_state_digest: [3; 32],
            plan_digest: Some([4; 32]),
            plan_summary: Some([u64::MAX; 5]),
            state: MetadataMaintenanceOperationState::Unresolved,
            error_message: Some(SENTINEL.repeat(32)),
            created_at_ms: i64::MAX,
            started_at_ms: Some(i64::MAX),
            finished_at_ms: Some(i64::MAX),
            authority: Some(authority()),
        }
    }

    fn rewrite_operation() -> StoredDistributedRewriteOperationV3 {
        StoredDistributedRewriteOperationV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            target: target(),
            owner: owner(),
            kind: DistributedRewriteOperationKind::RewritePositionDeleteFiles,
            request_digest: [1; 32],
            base_state_digest: [2; 32],
            request_payload_digest: [3; 32],
            plan_digest: Some([4; 32]),
            manifest_digest: Some([5; 32]),
            cohort_set_digest: Some([6; 32]),
            cohort_count: Some(u32::MAX),
            state: DistributedRewriteOperationState::Unresolved,
            error_message: Some(SENTINEL.repeat(32)),
            created_at_ms: i64::MAX,
            started_at_ms: Some(i64::MAX),
            finished_at_ms: Some(i64::MAX),
            authority: Some(authority()),
        }
    }

    fn cleanup_operation() -> StoredCleanupOperationV4 {
        StoredCleanupOperationV4 {
            schema_version: CLEANUP_OPERATION_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            target: target(),
            owner: owner(),
            request_digest: [1; 32],
            older_than_ms: i64::MIN,
            plan_digest: Some([2; 32]),
            manifest_digest: Some([3; 32]),
            candidate_count: Some(u32::MAX),
            batch_count: Some(u16::MAX),
            next_batch_ordinal: u16::MAX,
            state: CleanupOperationState::Unresolved,
            error_message: Some(SENTINEL.repeat(32)),
            created_at_ms: i64::MAX,
            started_at_ms: Some(i64::MAX),
            finished_at_ms: Some(i64::MAX),
            authority: Some(authority()),
        }
    }

    fn assert_budget<R: DurableRecord>(record: R) {
        let standard = DurableRecordStore::with_limits(StateStoreLimits::default());
        let encoded = standard
            .encode(&record)
            .expect("maximal bounded record must fit its declared budget");
        let actual_bytes = encoded.as_bytes().len();
        assert!(actual_bytes <= R::ENCODED_LIMIT, "{}", R::RECORD_KIND);
        assert!(R::ENCODED_LIMIT <= MAX_VALUE_BYTES, "{}", R::RECORD_KIND);

        // with_limits has no StateStore attached. A failed encode therefore
        // proves rejection occurs before any transaction, index, or record
        // write can be opened.
        let mut restricted = StateStoreLimits::default();
        restricted.max_value_bytes = actual_bytes - 1;
        let error = DurableRecordStore::with_limits(restricted)
            .encode(&record)
            .expect_err("one byte below the actual encoding must fail before writing");
        assert_eq!(
            error,
            DurableRecordError::BudgetExceeded {
                record_kind: R::RECORD_KIND,
                schema_version: R::SCHEMA_VERSION,
                actual_bytes,
                limit_bytes: actual_bytes - 1,
            }
        );
        assert!(!format!("{error}").contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));
    }

    #[test]
    fn every_table_maintenance_record_has_a_checked_maximal_encoding() {
        assert_budget(StoredOptimizeCounterV1 {
            schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
            last_job_id: i64::MAX,
        });
        assert_budget(StoredOptimizeJobV1 {
            schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
            job_id: i64::MAX,
            target: target(),
            base_snapshot_id: i64::MAX,
            state: StoredOptimizeJobStateV1::Failed,
            outcome: Some(StoredOptimizeOutcomeV1 {
                target_snapshot_id: Some(i64::MAX),
                rewritten_data_files: i64::MAX,
                deleted_data_files: i64::MAX,
                added_data_files: i64::MAX,
                output_record_count: i64::MAX,
            }),
            error_message: Some(SENTINEL.repeat(32)),
            created_at_ms: i64::MAX,
            started_at_ms: Some(i64::MAX),
            finished_at_ms: Some(i64::MAX),
            last_operation_id: Uuid::nil(),
            authority: Some(authority()),
            dispatched_child: Some(Uuid::nil()),
        });
        assert_budget(StoredOptimizeOperationV1 {
            schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            action: StoredOptimizeOperationActionV1::Fail,
            job_id: i64::MAX,
            post_job: StoredOptimizeJobV1 {
                schema_version: OPTIMIZE_JOB_SCHEMA_VERSION,
                job_id: i64::MAX,
                target: target(),
                base_snapshot_id: i64::MAX,
                state: StoredOptimizeJobStateV1::Failed,
                outcome: None,
                error_message: Some(SENTINEL.repeat(32)),
                created_at_ms: i64::MAX,
                started_at_ms: Some(i64::MAX),
                finished_at_ms: Some(i64::MAX),
                last_operation_id: Uuid::nil(),
                authority: Some(authority()),
                dispatched_child: Some(Uuid::nil()),
            },
        });

        assert_budget(metadata_operation());
        assert_budget(StoredMetadataMaintenanceTransactionV2 {
            schema_version: METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
            transaction_operation_id: Uuid::nil(),
            action: StoredMetadataMaintenanceTransactionActionV2::Unresolve,
            operation_id: Uuid::nil(),
            post_operation: metadata_operation(),
        });
        assert_budget(StoredMetadataMaintenancePayloadV2 {
            schema_version: METADATA_MAINTENANCE_OPERATION_SCHEMA_VERSION,
            kind: StoredMetadataMaintenancePayloadKindV2::Evidence,
            digest: [1; 32],
            payload: opaque(METADATA_MAINTENANCE_MAX_PAYLOAD_BYTES),
        });

        assert_budget(rewrite_operation());
        assert_budget(StoredDistributedRewritePayloadV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            kind: StoredDistributedRewritePayloadKindV3::Evidence,
            digest: [1; 32],
            payload: opaque(DISTRIBUTED_REWRITE_MAX_PAYLOAD_BYTES),
        });
        assert_budget(StoredDistributedRewriteAttemptV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            cohort_id: [1; 32],
            execution_query_id: [2; 16],
            execution_attempt_id: u64::MAX,
            disposition: DistributedRewriteAttemptDisposition::Superseded,
            attempt_digest: [3; 32],
            artifact_digest: [4; 32],
            artifact_handle: opaque(DISTRIBUTED_REWRITE_MAX_ATTEMPT_HANDLE_BYTES),
            checkpoint_digest: [5; 32],
        });
        assert_budget(StoredDistributedRewriteTransactionV3 {
            schema_version: DISTRIBUTED_REWRITE_OPERATION_SCHEMA_VERSION,
            transaction_operation_id: Uuid::nil(),
            action: StoredDistributedRewriteTransactionActionV3::Unresolve,
            operation_id: Uuid::nil(),
            post_operation: rewrite_operation(),
        });

        assert_budget(cleanup_operation());
        assert_budget(StoredCleanupPlanV4 {
            schema_version: CLEANUP_OPERATION_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            plan_digest: [1; 32],
            base_state_digest: [2; 32],
            manifest_digest: [3; 32],
            artifact_handle_digest: [4; 32],
            artifact_handle: opaque(CLEANUP_MAX_PAYLOAD_BYTES),
            candidate_count: u32::MAX,
            total_bytes: u64::MAX,
            manifest_parts: u16::MAX,
            batch_count: u16::MAX,
        });
        assert_budget(StoredCleanupBatchV4 {
            schema_version: CLEANUP_OPERATION_SCHEMA_VERSION,
            operation_id: Uuid::nil(),
            ordinal: u16::MAX,
            prepared_handle_digest: [1; 32],
            prepared_handle: opaque(CLEANUP_MAX_PAYLOAD_BYTES),
            receipt_handle_digest: Some([2; 32]),
            receipt_handle: Some(opaque(CLEANUP_MAX_PAYLOAD_BYTES)),
            deleted_count: u32::MAX,
            already_absent_count: u32::MAX,
            failed_count: u32::MAX,
            unknown_count: u32::MAX,
        });
        assert_budget(StoredCleanupTransactionV4 {
            schema_version: CLEANUP_OPERATION_SCHEMA_VERSION,
            transaction_operation_id: Uuid::nil(),
            action: StoredCleanupTransactionActionV4::Unresolve,
            operation_id: Uuid::nil(),
            post_operation: cleanup_operation(),
        });
    }
}
