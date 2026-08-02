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

//! Provider-neutral materialized-view persistence port.
//!
//! Commands describe MV consistency boundaries directly. They deliberately do
//! not expose provider transactions, keys, revisions, or commit handles.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::mv::model::MvTarget;
use crate::mv::persistence::definition::{
    CreateMvDefinitionRequest, StoredMvDefinition, StoredMvRefreshPolicy,
    UpdateMvRefreshMetadataRequest,
};
pub use crate::mv::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::persistence::dependency::StoredMvDependency;
use crate::mv::persistence::partition::{
    RecordFailedMvPartitionStatesRequest, ReplaceMvPartitionStatesRequest, StoredMvPartitionState,
    UpdateMvPartitionContractRequest,
};
use crate::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, FrontendMvRefreshAction, FrontendMvRefreshActionState,
    FrontendMvRefreshEvidence, FrontendMvRefreshLedger, FrontendMvRefreshRecoveryLedger,
    FrontendMvRefreshRecoveryObservation, MvRefreshFinalizeRequest, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshExternalOutcome, StoredMvRefresh,
    UpdateStarRocksMvRefreshSummaryRequest,
};

pub const MV_REPOSITORY_UNAVAILABLE_MESSAGE: &str =
    "materialized view service requires [state_store]";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRepositoryErrorKind {
    InvalidRequest,
    NotFound,
    Conflict,
    Corruption,
    Unavailable,
    CommitUnknown,
    KnownCommittedFinalizeFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRepositoryError {
    kind: MvRepositoryErrorKind,
    message: String,
}

impl MvRepositoryError {
    pub fn new(kind: MvRepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> MvRepositoryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn unavailable() -> Self {
        Self::new(
            MvRepositoryErrorKind::Unavailable,
            MV_REPOSITORY_UNAVAILABLE_MESSAGE,
        )
    }
}

impl fmt::Display for MvRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvRepositoryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRepositoryAvailability {
    Available,
    Unavailable,
}

impl MvRepositoryAvailability {
    pub fn is_available(self) -> bool {
        matches!(self, Self::Available)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialMvRefreshConfiguration {
    pub policy: StoredMvRefreshPolicy,
    pub paused: bool,
    pub interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
    pub next_refresh_after_ms: Option<i64>,
}

impl Default for InitialMvRefreshConfiguration {
    fn default() -> Self {
        Self {
            policy: StoredMvRefreshPolicy::Manual,
            paused: false,
            interval_ms: None,
            max_staleness_ms: None,
            next_refresh_after_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvRepositoryRequest {
    pub definition: CreateMvDefinitionRequest,
    pub refresh: InitialMvRefreshConfiguration,
    pub dependencies: Vec<CreateMvDependencyRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvRepositoryWithIdRequest {
    pub mv_id: i64,
    pub create: CreateMvRepositoryRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebuildMvRepositoryRequest {
    pub create: CreateMvRepositoryRequest,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeMvRefreshWithPartitionsRequest {
    pub refresh: MvRefreshFinalizeRequest,
    pub partitions: Option<ReplaceMvPartitionStatesRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordExternalCommitAndFinalizeRequest {
    pub refresh_id: i64,
    pub external_outcome: RefreshExternalOutcome,
    pub finalize: MvRefreshFinalizeRequest,
}

/// Frontend-owned v3 refresh intent. The provider-neutral repository port
/// carries only durable application facts; concrete StateStore mechanics stay
/// in the frontend repository implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginFrontendMvRefreshIntentRequest {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub marker_token: String,
    /// `false` is the explicit no-op/metadata form: it writes the durable v3
    /// intent but does not synthesize writer or staging actions.
    pub prepare_external_actions: bool,
    pub ledger: FrontendMvRefreshLedger,
}

/// Atomically starts a frontend-owned recovery inspection. The inspection
/// identity is current-generation only and never authorizes replay of the
/// historical write or publication actions recorded by the refresh.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginFrontendMvRecoveryCycleRequest {
    pub refresh_id: i64,
    pub cycle_id: Vec<u8>,
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: Vec<u8>,
    pub cleanup_operation_id: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordFrontendMvRecoveryObservationRequest {
    pub refresh_id: i64,
    pub observation: FrontendMvRefreshRecoveryObservation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordFrontendMvRecoveryCleanupOutcomeRequest {
    pub refresh_id: i64,
    pub state: FrontendMvRefreshActionState,
    pub evidence: Option<FrontendMvRefreshEvidence>,
    pub provider_finalized: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeRecoveredMvRefreshRequest {
    pub finalize: MvRefreshFinalizeRequest,
    pub recovery: FrontendMvRefreshRecoveryLedger,
}

/// Synchronous consumer port used by core SQL and maintenance workers.
///
/// Concrete repositories may bridge to an async store internally, but the
/// provider and its transaction model never cross this boundary.
pub trait MvRepository: Send + Sync {
    fn availability(&self) -> MvRepositoryAvailability;

    fn create(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError>;

    fn create_with_id(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError>;

    fn rebuild(
        &self,
        operation_id: Uuid,
        request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError>;

    fn reserve_definition_id(&self, mv_id: i64) -> Result<(), MvRepositoryError>;
    fn load_by_id(&self, mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError>;
    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError>;
    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError>;
    fn drop_by_id(&self, mv_id: i64) -> Result<bool, MvRepositoryError>;
    fn drop_by_target(&self, target: &MvTarget) -> Result<bool, MvRepositoryError>;

    fn set_rebuilt_refresh_watermark(
        &self,
        mv_id: i64,
        base_snapshots: BTreeMap<String, i64>,
        base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError>;
    fn update_refresh_metadata(
        &self,
        request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError>;
    fn update_partition_contract(
        &self,
        request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError>;

    fn begin_refresh_intent(
        &self,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError>;
    fn begin_iceberg_refresh_intent(
        &self,
        request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError>;
    fn begin_frontend_refresh_intent(
        &self,
        _request: BeginFrontendMvRefreshIntentRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    /// Reserve a positive refresh identity before SQL preparation.  This is
    /// intentionally separate from the v3 intent write: a failed preparation
    /// may leave an unused identity, but it must never leave an active refresh
    /// or an incomplete lifecycle ledger.
    fn reserve_frontend_refresh_id(&self) -> Result<i64, MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn record_frontend_refresh_action(
        &self,
        _refresh_id: i64,
        _action: FrontendMvRefreshAction,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    /// Lists only frontend-owned records that remain fenced for recovery. The
    /// legacy adapter must never receive these v3/v4 attempts.
    fn list_frontend_recovery_candidates(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn begin_frontend_recovery_cycle(
        &self,
        _request: BeginFrontendMvRecoveryCycleRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn record_frontend_recovery_observation(
        &self,
        _request: RecordFrontendMvRecoveryObservationRequest,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn record_frontend_recovery_unresolved(
        &self,
        _refresh_id: i64,
        _reason: String,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn record_frontend_recovery_cleanup_outcome(
        &self,
        _request: RecordFrontendMvRecoveryCleanupOutcomeRequest,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn finalize_recovered_published_refresh(
        &self,
        _request: FinalizeRecoveredMvRefreshRequest,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn abort_recovered_uncommitted_refresh(
        &self,
        _refresh_id: i64,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn record_staging_commit(
        &self,
        request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError>;
    fn record_publish_commit(
        &self,
        request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError>;
    fn mark_refresh_commit_unknown(&self, refresh_id: i64) -> Result<(), MvRepositoryError>;
    fn record_external_commit_outcome(
        &self,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError>;
    fn finalize_refresh(&self, request: MvRefreshFinalizeRequest) -> Result<(), MvRepositoryError>;
    /// Atomically finalize a frontend-owned refresh that deliberately has no
    /// external phases (`NoOp` or `MetadataOnly`).  This is separate from the
    /// historical publish-based finalizer so callers cannot invent a staging
    /// or publication record merely to advance metadata.
    fn finalize_frontend_refresh_without_external_actions(
        &self,
        _request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        Err(MvRepositoryError::unavailable())
    }
    fn finalize_refresh_with_partitions(
        &self,
        request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError>;
    fn record_external_commit_and_finalize(
        &self,
        request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError>;
    fn clear_refresh_progress(&self, mv_id: i64) -> Result<bool, MvRepositoryError>;
    fn load_refresh(&self, refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError>;
    fn list_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError>;
    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError>;
    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError>;
    fn update_starrocks_refresh_summary_if_present(
        &self,
        request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError>;

    fn replace_partition_states(
        &self,
        request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError>;
    fn record_failed_partition_states(
        &self,
        request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError>;
    fn clear_partition_states(&self, mv_id: i64) -> Result<bool, MvRepositoryError>;
    fn list_partition_states(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError>;
    fn adopt_target_compaction_snapshot(
        &self,
        target: &MvTarget,
        expected_snapshot_id: i64,
        adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError>;

    fn replace_dependencies_for_mv(
        &self,
        mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError>;
    fn delete_dependencies_for_mv(&self, mv_id: i64) -> Result<(), MvRepositoryError>;
    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &crate::mv::dependency::model::MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError>;
    fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;
    fn list_downstream_dependencies(
        &self,
        upstream: &crate::mv::dependency::model::MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableMvRepository;

macro_rules! unavailable {
    () => {
        Err(MvRepositoryError::unavailable())
    };
}

impl MvRepository for UnavailableMvRepository {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Unavailable
    }

    fn create(
        &self,
        _operation_id: Uuid,
        _request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unavailable!()
    }

    fn create_with_id(
        &self,
        _operation_id: Uuid,
        _request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unavailable!()
    }

    fn rebuild(
        &self,
        _operation_id: Uuid,
        _request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unavailable!()
    }

    fn reserve_definition_id(&self, _mv_id: i64) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn load_by_id(&self, _mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        unavailable!()
    }

    fn find_by_target(
        &self,
        _target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        unavailable!()
    }

    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        unavailable!()
    }

    fn drop_by_id(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        unavailable!()
    }

    fn drop_by_target(&self, _target: &MvTarget) -> Result<bool, MvRepositoryError> {
        unavailable!()
    }

    fn set_rebuilt_refresh_watermark(
        &self,
        _mv_id: i64,
        _base_snapshots: BTreeMap<String, i64>,
        _base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unavailable!()
    }

    fn update_refresh_metadata(
        &self,
        _request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unavailable!()
    }

    fn update_partition_contract(
        &self,
        _request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unavailable!()
    }

    fn begin_refresh_intent(
        &self,
        _mv_id: i64,
        _target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unavailable!()
    }

    fn begin_iceberg_refresh_intent(
        &self,
        _request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unavailable!()
    }

    fn begin_frontend_refresh_intent(
        &self,
        _request: BeginFrontendMvRefreshIntentRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unavailable!()
    }

    fn record_frontend_refresh_action(
        &self,
        _refresh_id: i64,
        _action: FrontendMvRefreshAction,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn record_staging_commit(
        &self,
        _request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn record_publish_commit(
        &self,
        _request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn mark_refresh_commit_unknown(&self, _refresh_id: i64) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn record_external_commit_outcome(
        &self,
        _refresh_id: i64,
        _outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn finalize_refresh(
        &self,
        _request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn finalize_refresh_with_partitions(
        &self,
        _request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn record_external_commit_and_finalize(
        &self,
        _request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn clear_refresh_progress(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        unavailable!()
    }

    fn load_refresh(&self, _refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        unavailable!()
    }

    fn list_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        unavailable!()
    }

    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        unavailable!()
    }

    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        unavailable!()
    }

    fn update_starrocks_refresh_summary_if_present(
        &self,
        _request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        unavailable!()
    }

    fn replace_partition_states(
        &self,
        _request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn record_failed_partition_states(
        &self,
        _request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn clear_partition_states(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        unavailable!()
    }

    fn list_partition_states(
        &self,
        _mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        unavailable!()
    }

    fn adopt_target_compaction_snapshot(
        &self,
        _target: &MvTarget,
        _expected_snapshot_id: i64,
        _adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        unavailable!()
    }

    fn replace_dependencies_for_mv(
        &self,
        _mv_id: i64,
        _dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn delete_dependencies_for_mv(&self, _mv_id: i64) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn ensure_no_downstream_dependencies(
        &self,
        _upstream: &crate::mv::dependency::model::MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        unavailable!()
    }

    fn list_dependencies_by_downstream(
        &self,
        _mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        unavailable!()
    }

    fn list_downstream_dependencies(
        &self,
        _upstream: &crate::mv::dependency::model::MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        unavailable!()
    }
}
