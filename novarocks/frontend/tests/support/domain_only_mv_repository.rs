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

use novarocks_frontend::mv::domain::dependency::model::MvDependencyObjectRef;
use novarocks_frontend::mv::domain::persistence::definition::{
    StoredMvDefinition, UpdateMvRefreshMetadataRequest,
};
use novarocks_frontend::mv::domain::persistence::dependency::StoredMvDependency;
use novarocks_frontend::mv::domain::persistence::partition::{
    RecordFailedMvPartitionStatesRequest, ReplaceMvPartitionStatesRequest, StoredMvPartitionState,
    UpdateMvPartitionContractRequest,
};
use novarocks_frontend::mv::domain::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshExternalOutcome, StoredMvRefresh,
    UpdateStarRocksMvRefreshSummaryRequest,
};
use novarocks_frontend::mv::domain::repository::{
    CreateMvDependencyRequest, CreateMvRepositoryRequest, CreateMvRepositoryWithIdRequest,
    FinalizeMvRefreshWithPartitionsRequest, MvRepository, MvRepositoryAvailability,
    MvRepositoryError, MvRepositoryErrorKind, MvTarget, RebuildMvRepositoryRequest,
    RecordExternalCommitAndFinalizeRequest,
};
use uuid::Uuid;

pub struct DomainOnlyMvRepository;

fn unsupported<T>() -> Result<T, MvRepositoryError> {
    Err(MvRepositoryError::new(
        MvRepositoryErrorKind::InvalidRequest,
        "domain-only fake does not persist records",
    ))
}

impl MvRepository for DomainOnlyMvRepository {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Available
    }

    fn create(
        &self,
        _operation_id: Uuid,
        _request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unsupported()
    }

    fn create_with_id(
        &self,
        _operation_id: Uuid,
        _request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unsupported()
    }

    fn rebuild(
        &self,
        _operation_id: Uuid,
        _request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unsupported()
    }

    fn reserve_definition_id(&self, _mv_id: i64) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn load_by_id(&self, _mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        Ok(None)
    }

    fn find_by_target(
        &self,
        _target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        Ok(None)
    }

    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn drop_by_id(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn drop_by_target(&self, _target: &MvTarget) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn initialize_rebuilt_refresh_watermark(
        &self,
        _mv_id: i64,
        _base_snapshots: BTreeMap<String, i64>,
        _base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unsupported()
    }

    fn update_refresh_metadata(
        &self,
        _request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unsupported()
    }

    fn update_partition_contract(
        &self,
        _request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unsupported()
    }

    fn begin_refresh_intent(
        &self,
        _mv_id: i64,
        _target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unsupported()
    }

    fn begin_iceberg_refresh_intent(
        &self,
        _request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unsupported()
    }

    fn record_staging_commit(
        &self,
        _request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn record_publish_commit(
        &self,
        _request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn mark_refresh_commit_unknown(&self, _refresh_id: i64) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn record_external_commit_outcome(
        &self,
        _refresh_id: i64,
        _outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn finalize_refresh(
        &self,
        _request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn finalize_refresh_with_partitions(
        &self,
        _request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn record_external_commit_and_finalize(
        &self,
        _request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn clear_refresh_progress(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn load_refresh(&self, _refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        Ok(None)
    }

    fn list_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn update_starrocks_refresh_summary_if_present(
        &self,
        _request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn replace_partition_states(
        &self,
        _request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn record_failed_partition_states(
        &self,
        _request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn clear_partition_states(&self, _mv_id: i64) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn list_partition_states(
        &self,
        _mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn adopt_target_compaction_snapshot(
        &self,
        _target: &MvTarget,
        _expected_snapshot_id: i64,
        _adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn replace_dependencies_for_mv(
        &self,
        _mv_id: i64,
        _dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn delete_dependencies_for_mv(&self, _mv_id: i64) -> Result<(), MvRepositoryError> {
        unsupported()
    }

    fn ensure_no_downstream_dependencies(
        &self,
        _upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        Ok(())
    }

    fn list_dependencies_by_downstream(
        &self,
        _mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn list_downstream_dependencies(
        &self,
        _upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        Ok(Vec::new())
    }
}
