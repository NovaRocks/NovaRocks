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

//! Provider-neutral MV repository used only by core integration tests.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use super::dependency::model::MvDependencyObjectRef;
use super::persistence::definition::{
    CreateMvDefinitionRequest, StoredMvDefinition, UpdateMvRefreshMetadataRequest,
};
use super::persistence::dependency::StoredMvDependency;
use super::persistence::partition::{
    MvPartitionRefreshStatus, RecordFailedMvPartitionStatesRequest,
    ReplaceMvPartitionStatesRequest, StoredMvPartitionState, UpdateMvPartitionContractRequest,
};
use super::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, MvRefreshLifecycleOwner,
    MvRefreshState, RecordPublishCommitRequest, RecordStagingCommitRequest, RefreshCommitMarker,
    RefreshExternalOutcome, StoredMvRefresh, UpdateStarRocksMvRefreshSummaryRequest,
};
use super::repository::{
    CreateMvDependencyRequest, CreateMvRepositoryRequest, CreateMvRepositoryWithIdRequest,
    FinalizeMvRefreshWithPartitionsRequest, MvRepository, MvRepositoryAvailability,
    MvRepositoryError, MvRepositoryErrorKind, MvTarget, RebuildMvRepositoryRequest,
    RecordExternalCommitAndFinalizeRequest,
};

#[derive(Default)]
pub struct InMemoryMvRepository {
    state: Mutex<State>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestMvRepositoryFailurePoint {
    Create,
    CreateWithId,
    DropById,
    FinalizeRefresh,
    UpdateStarRocksRefreshSummary,
}

thread_local! {
    static NEXT_FAILURE: RefCell<Option<TestMvRepositoryFailurePoint>> = const { RefCell::new(None) };
    static AFTER_CREATE: RefCell<Option<Arc<dyn Fn() + Send + Sync>>> = const { RefCell::new(None) };
}

pub(crate) struct TestMvRepositoryFailureGuard;

pub(crate) fn fail_next_mv_repository_command(
    point: TestMvRepositoryFailurePoint,
) -> TestMvRepositoryFailureGuard {
    NEXT_FAILURE.with(|slot| *slot.borrow_mut() = Some(point));
    TestMvRepositoryFailureGuard
}

impl Drop for TestMvRepositoryFailureGuard {
    fn drop(&mut self) {
        NEXT_FAILURE.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(crate) struct TestMvRepositoryAfterCreateGuard;

pub(crate) fn after_next_mv_repository_create(
    callback: Arc<dyn Fn() + Send + Sync>,
) -> TestMvRepositoryAfterCreateGuard {
    AFTER_CREATE.with(|slot| *slot.borrow_mut() = Some(callback));
    TestMvRepositoryAfterCreateGuard
}

impl Drop for TestMvRepositoryAfterCreateGuard {
    fn drop(&mut self) {
        AFTER_CREATE.with(|slot| *slot.borrow_mut() = None);
    }
}

fn fail_if_requested(point: TestMvRepositoryFailurePoint) -> Result<(), MvRepositoryError> {
    let requested = NEXT_FAILURE.with(|slot| slot.borrow_mut().take());
    if requested == Some(point) {
        return Err(MvRepositoryError::new(
            MvRepositoryErrorKind::InvalidRequest,
            format!("test-only injected MV repository failure at {point:?}"),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct State {
    next_id: i64,
    definitions: BTreeMap<i64, StoredMvDefinition>,
    refreshes: BTreeMap<i64, StoredMvRefresh>,
    partitions: BTreeMap<(i64, String), StoredMvPartitionState>,
    dependencies: BTreeMap<i64, Vec<StoredMvDependency>>,
}

impl InMemoryMvRepository {
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>, MvRepositoryError> {
        self.state.lock().map_err(|_| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Corruption,
                "in-memory MV repository lock poisoned",
            )
        })
    }

    fn target(definition: &StoredMvDefinition) -> Option<MvTarget> {
        Some(MvTarget {
            catalog: definition.target_catalog.clone(),
            database: definition.target_namespace.clone()?,
            name: definition.target_table.clone()?,
        })
    }

    fn allocate(state: &mut State) -> i64 {
        state.next_id += 1;
        state.next_id
    }

    fn create_locked(
        state: &mut State,
        mv_id: i64,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        if state.definitions.contains_key(&mv_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!("MV definition {mv_id} already exists"),
            ));
        }
        let definition = definition_from_request(mv_id, request.definition, request.refresh);
        if let Some(target) = Self::target(&definition) {
            if state
                .definitions
                .values()
                .any(|candidate| Self::target(candidate).as_ref() == Some(&target))
            {
                return Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::Conflict,
                    format!("MV target {} already exists", target.display_name()),
                ));
            }
        }
        let dependencies = request
            .dependencies
            .into_iter()
            .map(|dependency| StoredMvDependency {
                downstream_mv_id: mv_id,
                upstream: dependency.upstream,
                created_at_ms: dependency.created_at_ms,
            })
            .collect();
        state.next_id = state.next_id.max(mv_id);
        state.dependencies.insert(mv_id, dependencies);
        state.definitions.insert(mv_id, definition.clone());
        Ok(definition)
    }

    fn refresh_mut<'a>(
        state: &'a mut State,
        refresh_id: i64,
    ) -> Result<&'a mut StoredMvRefresh, MvRepositoryError> {
        state.refreshes.get_mut(&refresh_id).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV refresh {refresh_id} does not exist"),
            )
        })
    }

    fn expect_refresh_state(
        refresh: &StoredMvRefresh,
        expected: MvRefreshState,
    ) -> Result<(), MvRepositoryError> {
        if refresh.state == expected {
            return Ok(());
        }
        Err(MvRepositoryError::new(
            MvRepositoryErrorKind::Conflict,
            format!(
                "mv refresh {} is {}, expected {}",
                refresh.refresh_id,
                refresh.state.as_str(),
                expected.as_str()
            ),
        ))
    }

    fn persisted_publish_target_snapshot(refresh: &StoredMvRefresh) -> Option<i64> {
        refresh.published_snapshot_id.or_else(|| {
            refresh
                .external_outcome
                .as_ref()
                .and_then(|outcome| outcome.target_snapshot_id)
        })
    }

    fn finish_locked(
        state: &mut State,
        request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        let refresh = Self::refresh_mut(state, request.refresh_id)?.clone();
        let definition = state.definitions.get_mut(&refresh.mv_id).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV definition {} does not exist", refresh.mv_id),
            )
        })?;
        definition.last_refresh_rows = Some(request.rows);
        definition.last_refresh_snapshots = request.base_snapshots;
        definition.last_refresh_table_uuids = request.base_table_uuids;
        definition.last_refreshed_iceberg_snapshot_id =
            request.target_snapshot_id.or(refresh.published_snapshot_id);
        definition.refresh_in_progress = false;
        definition.active_refresh_id = None;
        definition.refresh_target_snapshots.clear();
        Self::refresh_mut(state, request.refresh_id)?.state = MvRefreshState::Finalized;
        Ok(())
    }
}

fn definition_from_request(
    mv_id: i64,
    request: CreateMvDefinitionRequest,
    refresh: super::repository::InitialMvRefreshConfiguration,
) -> StoredMvDefinition {
    StoredMvDefinition {
        mv_id,
        select_sql: request.select_sql,
        base_table_refs: request.base_table_refs,
        primary_key_columns: request.primary_key_columns,
        storage_engine: request.storage_engine,
        target_catalog: request.target_catalog,
        target_namespace: request.target_namespace,
        target_table: request.target_table,
        schema_contract: request.schema_contract,
        partition_spec: request.partition_spec,
        partition_state_complete: false,
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: BTreeMap::new(),
        last_refresh_table_uuids: BTreeMap::new(),
        last_refreshed_iceberg_snapshot_id: None,
        refresh_in_progress: false,
        active_refresh_id: None,
        refresh_target_snapshots: BTreeMap::new(),
        refresh_policy: refresh.policy,
        refresh_paused: refresh.paused,
        refresh_interval_ms: refresh.interval_ms,
        max_staleness_ms: refresh.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: refresh.next_refresh_after_ms,
        created_at_ms: request.created_at_ms,
    }
}

impl MvRepository for InMemoryMvRepository {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Available
    }
    fn create(
        &self,
        _: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::Create)?;
        let definition = {
            let mut state = self.state()?;
            let id = Self::allocate(&mut state);
            Self::create_locked(&mut state, id, request)?
        };
        if let Some(callback) = AFTER_CREATE.with(|slot| slot.borrow_mut().take()) {
            callback();
        }
        Ok(definition)
    }
    fn create_with_id(
        &self,
        _: Uuid,
        request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::CreateWithId)?;
        let mut state = self.state()?;
        Self::create_locked(&mut state, request.mv_id, request.create)
    }
    fn rebuild(
        &self,
        operation_id: Uuid,
        request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        let definition = self.create(operation_id, request.create)?;
        self.set_rebuilt_refresh_watermark(
            definition.mv_id,
            request.base_snapshots,
            request.base_table_uuids,
        )
    }
    fn reserve_definition_id(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        if state.definitions.contains_key(&mv_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!("MV definition {mv_id} already exists"),
            ));
        }
        state.next_id = state.next_id.max(mv_id);
        Ok(())
    }
    fn load_by_id(&self, mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        Ok(self.state()?.definitions.get(&mv_id).cloned())
    }
    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        Ok(self
            .state()?
            .definitions
            .values()
            .find(|definition| InMemoryMvRepository::target(definition).as_ref() == Some(target))
            .cloned())
    }
    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        Ok(self.state()?.definitions.values().cloned().collect())
    }
    fn drop_by_id(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::DropById)?;
        let mut state = self.state()?;
        let existed = state.definitions.remove(&mv_id).is_some();
        if existed {
            state.dependencies.remove(&mv_id);
            state.partitions.retain(|(id, _), _| *id != mv_id);
        }
        Ok(existed)
    }
    fn drop_by_target(&self, target: &MvTarget) -> Result<bool, MvRepositoryError> {
        let id = self
            .find_by_target(target)?
            .map(|definition| definition.mv_id);
        id.map_or(Ok(false), |mv_id| self.drop_by_id(mv_id))
    }
    fn set_rebuilt_refresh_watermark(
        &self,
        mv_id: i64,
        base_snapshots: BTreeMap<String, i64>,
        base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        let mut state = self.state()?;
        let definition = state.definitions.get_mut(&mv_id).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV definition {mv_id} does not exist"),
            )
        })?;
        definition.last_refresh_snapshots = base_snapshots;
        definition.last_refresh_table_uuids = base_table_uuids;
        Ok(definition.clone())
    }
    fn update_refresh_metadata(
        &self,
        request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        let mut state = self.state()?;
        let definition = state.definitions.get_mut(&request.mv_id).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV definition {} does not exist", request.mv_id),
            )
        })?;
        definition.refresh_policy = request.refresh_policy;
        definition.refresh_paused = request.refresh_paused;
        definition.refresh_interval_ms = request.refresh_interval_ms;
        definition.max_staleness_ms = request.max_staleness_ms;
        definition.last_scheduler_error = request.last_scheduler_error;
        definition.next_refresh_after_ms = request.next_refresh_after_ms;
        Ok(definition.clone())
    }
    fn update_partition_contract(
        &self,
        request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        let mut state = self.state()?;
        let updated = {
            let definition = state.definitions.get_mut(&request.mv_id).ok_or_else(|| {
                MvRepositoryError::new(
                    MvRepositoryErrorKind::NotFound,
                    format!("MV definition {} does not exist", request.mv_id),
                )
            })?;
            let schema = definition.schema_contract.as_mut().ok_or_else(|| {
                MvRepositoryError::new(
                    MvRepositoryErrorKind::Corruption,
                    "MV definition has no schema contract",
                )
            })?;
            schema.target.partition = Some(request.partition_spec.clone());
            definition.partition_spec = Some(request.partition_spec);
            definition.partition_state_complete = false;
            definition.clone()
        };
        state.partitions.retain(|(id, _), _| *id != request.mv_id);
        Ok(updated)
    }
    fn begin_refresh_intent(
        &self,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        let mut state = self.state()?;
        let refresh_id = Self::allocate(&mut state);
        let definition = state.definitions.get_mut(&mv_id).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV definition {mv_id} does not exist"),
            )
        })?;
        if definition.refresh_in_progress {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV refresh is already in progress",
            ));
        }
        definition.refresh_in_progress = true;
        definition.active_refresh_id = Some(refresh_id);
        definition.refresh_target_snapshots = target_snapshots.clone();
        let refresh = StoredMvRefresh {
            refresh_id,
            mv_id,
            operation_id: None,
            state: MvRefreshState::IntentCreated,
            target_catalog: None,
            target_namespace: None,
            target_table: None,
            staging_branch: None,
            expected_main_snapshot_id: None,
            staging_snapshot_id: None,
            published_snapshot_id: None,
            target_snapshots,
            base_table_uuids: BTreeMap::new(),
            rows: None,
            marker: None,
            external_outcome: None,
            lifecycle_owner: MvRefreshLifecycleOwner::LegacyCore,
            frontend_ledger: None,
            frontend_recovery: None,
        };
        state.refreshes.insert(refresh_id, refresh.clone());
        Ok(refresh)
    }
    fn begin_iceberg_refresh_intent(
        &self,
        request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        let mut state = self.state()?;
        let refresh_id = Self::allocate(&mut state);
        let definition = state.definitions.get_mut(&request.mv_id).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV definition {} does not exist", request.mv_id),
            )
        })?;
        if definition.refresh_in_progress {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV refresh is already in progress",
            ));
        }
        definition.refresh_in_progress = true;
        definition.active_refresh_id = Some(refresh_id);
        definition.refresh_target_snapshots = request.base_snapshots.clone();
        let refresh = StoredMvRefresh {
            refresh_id,
            mv_id: request.mv_id,
            operation_id: request.operation_id,
            state: MvRefreshState::IntentCreated,
            target_catalog: (!request.target_catalog.is_empty()).then_some(request.target_catalog),
            target_namespace: (!request.target_namespace.is_empty())
                .then_some(request.target_namespace),
            target_table: (!request.target_table.is_empty()).then_some(request.target_table),
            staging_branch: (!request.staging_branch.is_empty()).then_some(request.staging_branch),
            expected_main_snapshot_id: request.expected_main_snapshot_id,
            staging_snapshot_id: None,
            published_snapshot_id: None,
            target_snapshots: request.base_snapshots,
            base_table_uuids: BTreeMap::new(),
            rows: None,
            marker: Some(RefreshCommitMarker {
                refresh_id,
                mv_id: request.mv_id,
                token: request.marker_token,
            }),
            external_outcome: None,
            lifecycle_owner: MvRefreshLifecycleOwner::LegacyCore,
            frontend_ledger: None,
            frontend_recovery: None,
        };
        state.refreshes.insert(refresh_id, refresh.clone());
        Ok(refresh)
    }
    fn record_staging_commit(
        &self,
        request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        let refresh = Self::refresh_mut(&mut state, request.refresh_id)?;
        if refresh.state == MvRefreshState::StagingCommitted {
            if refresh.staging_snapshot_id == Some(request.staging_snapshot_id)
                && refresh.rows == Some(request.rows)
                && refresh.base_table_uuids == request.base_table_uuids
            {
                return Ok(());
            }
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!(
                    "mv refresh {} staging commit differs from recorded value",
                    request.refresh_id
                ),
            ));
        }
        Self::expect_refresh_state(refresh, MvRefreshState::IntentCreated)?;
        refresh.state = MvRefreshState::StagingCommitted;
        refresh.staging_snapshot_id = Some(request.staging_snapshot_id);
        refresh.rows = Some(request.rows);
        refresh.base_table_uuids = request.base_table_uuids;
        Ok(())
    }
    fn record_publish_commit(
        &self,
        request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        let refresh = Self::refresh_mut(&mut state, request.refresh_id)?;
        if refresh.state == MvRefreshState::PublishCommitted {
            if refresh.published_snapshot_id == Some(request.published_snapshot_id)
                && Self::persisted_publish_target_snapshot(refresh)
                    == Some(request.published_snapshot_id)
            {
                return Ok(());
            }
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!(
                    "mv refresh {} publish commit differs from recorded value",
                    request.refresh_id
                ),
            ));
        }
        Self::expect_refresh_state(refresh, MvRefreshState::StagingCommitted)?;
        refresh.state = MvRefreshState::PublishCommitted;
        refresh.published_snapshot_id = Some(request.published_snapshot_id);
        refresh.external_outcome = Some(RefreshExternalOutcome {
            target_snapshot_id: Some(request.published_snapshot_id),
            commit_id: format!("iceberg-snapshot-{}", request.published_snapshot_id),
        });
        Ok(())
    }
    fn mark_refresh_commit_unknown(&self, refresh_id: i64) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        let refresh = Self::refresh_mut(&mut state, refresh_id)?;
        if !matches!(
            refresh.state,
            MvRefreshState::Finalized | MvRefreshState::Aborted
        ) {
            refresh.state = MvRefreshState::CommitUnknown;
        }
        Ok(())
    }
    fn record_external_commit_outcome(
        &self,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        let refresh = Self::refresh_mut(&mut state, refresh_id)?;
        Self::expect_refresh_state(refresh, MvRefreshState::IntentCreated)?;
        refresh.state = MvRefreshState::PublishCommitted;
        refresh.published_snapshot_id = outcome.target_snapshot_id;
        refresh.external_outcome = Some(outcome);
        Ok(())
    }
    fn finalize_refresh(&self, request: MvRefreshFinalizeRequest) -> Result<(), MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::FinalizeRefresh)?;
        let mut state = self.state()?;
        InMemoryMvRepository::finish_locked(&mut state, request)
    }
    fn finalize_refresh_with_partitions(
        &self,
        request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        if let Some(partitions) = request.partitions {
            replace_partitions(&mut state, partitions)?;
        }
        InMemoryMvRepository::finish_locked(&mut state, request.refresh)
    }
    fn record_external_commit_and_finalize(
        &self,
        request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        Self::refresh_mut(&mut state, request.refresh_id)?.external_outcome =
            Some(request.external_outcome);
        InMemoryMvRepository::finish_locked(&mut state, request.finalize)
    }
    fn clear_refresh_progress(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        let mut state = self.state()?;
        let Some(active_refresh_id) = state
            .definitions
            .get(&mv_id)
            .map(|definition| definition.active_refresh_id)
        else {
            return Ok(false);
        };
        if let Some(refresh_id) = active_refresh_id {
            let refresh = Self::refresh_mut(&mut state, refresh_id)?;
            if refresh.state == MvRefreshState::CommitUnknown {
                return Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::Conflict,
                    format!("mv definition {mv_id} active refresh {refresh_id} is commit-unknown"),
                ));
            }
            if !matches!(
                refresh.state,
                MvRefreshState::Finalized | MvRefreshState::Aborted
            ) {
                refresh.state = MvRefreshState::Aborted;
            }
        }
        let definition = state
            .definitions
            .get_mut(&mv_id)
            .expect("definition checked above");
        definition.refresh_in_progress = false;
        definition.active_refresh_id = None;
        definition.refresh_target_snapshots.clear();
        Ok(true)
    }
    fn load_refresh(&self, refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        Ok(self.state()?.refreshes.get(&refresh_id).cloned())
    }
    fn list_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Ok(self.state()?.refreshes.values().cloned().collect())
    }
    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Ok(self
            .state()?
            .refreshes
            .values()
            .filter(|refresh| {
                !matches!(
                    refresh.state,
                    MvRefreshState::Finalized | MvRefreshState::Aborted
                )
            })
            .cloned()
            .collect())
    }
    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        Ok(self
            .list_unfinished_refreshes()?
            .into_iter()
            .filter(|refresh| {
                refresh.lifecycle_owner == MvRefreshLifecycleOwner::LegacyCore
                    && refresh.staging_branch.is_some()
            })
            .collect())
    }
    fn update_starrocks_refresh_summary_if_present(
        &self,
        request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        fail_if_requested(TestMvRepositoryFailurePoint::UpdateStarRocksRefreshSummary)?;
        let mut state = self.state()?;
        let Some(definition) = state.definitions.get_mut(&request.mv_id) else {
            return Ok(false);
        };
        definition.last_refresh_ms = Some(request.last_refresh_ms);
        definition.last_refresh_rows = Some(request.last_refresh_rows);
        definition.last_refresh_snapshots = request.base_snapshots;
        definition.last_refresh_table_uuids = request.base_table_uuids;
        Ok(true)
    }
    fn replace_partition_states(
        &self,
        request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        replace_partitions(&mut state, request)
    }
    fn record_failed_partition_states(
        &self,
        request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        for key in request.partition_keys.into_iter().take(request.max_entries) {
            state.partitions.insert(
                (request.mv_id, key.clone()),
                StoredMvPartitionState {
                    mv_id: request.mv_id,
                    partition_key: key,
                    status: MvPartitionRefreshStatus::Failed,
                    last_refresh_ms: Some(request.last_refresh_ms),
                    base_snapshots: request.base_snapshots.clone(),
                    target_snapshot_id: request.target_snapshot_id,
                    last_refresh_id: Some(request.last_refresh_id),
                    failure_message: Some(request.failure_message.clone()),
                },
            );
        }
        Ok(())
    }
    fn clear_partition_states(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        let mut state = self.state()?;
        let before = state.partitions.len();
        state.partitions.retain(|(id, _), _| *id != mv_id);
        Ok(before != state.partitions.len())
    }
    fn list_partition_states(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        Ok(self
            .state()?
            .partitions
            .range((mv_id, String::new())..)
            .take_while(|((id, _), _)| *id == mv_id)
            .map(|(_, value)| value.clone())
            .collect())
    }
    fn adopt_target_compaction_snapshot(
        &self,
        target: &MvTarget,
        expected_snapshot_id: i64,
        adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        let mut state = self.state()?;
        let Some(definition) = state
            .definitions
            .values_mut()
            .find(|definition| InMemoryMvRepository::target(definition).as_ref() == Some(target))
        else {
            return Ok(false);
        };
        if definition.last_refreshed_iceberg_snapshot_id != Some(expected_snapshot_id) {
            return Ok(false);
        };
        definition.last_refreshed_iceberg_snapshot_id = Some(adopted_snapshot_id);
        Ok(true)
    }
    fn replace_dependencies_for_mv(
        &self,
        mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        if !state.definitions.contains_key(&mv_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("MV definition {mv_id} does not exist"),
            ));
        }
        state.dependencies.insert(
            mv_id,
            dependencies
                .into_iter()
                .map(|dependency| StoredMvDependency {
                    downstream_mv_id: mv_id,
                    upstream: dependency.upstream,
                    created_at_ms: dependency.created_at_ms,
                })
                .collect(),
        );
        Ok(())
    }
    fn delete_dependencies_for_mv(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.state()?.dependencies.remove(&mv_id);
        Ok(())
    }
    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let dependents = self.list_downstream_dependencies(upstream)?;
        if dependents.is_empty() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV dependency prevents drop",
            ))
        }
    }
    fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        Ok(self
            .state()?
            .dependencies
            .get(&mv_id)
            .cloned()
            .unwrap_or_default())
    }
    fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        Ok(self
            .state()?
            .dependencies
            .values()
            .flatten()
            .filter(|dependency| &dependency.upstream == upstream)
            .cloned()
            .collect())
    }
}

fn replace_partitions(
    state: &mut State,
    request: ReplaceMvPartitionStatesRequest,
) -> Result<(), MvRepositoryError> {
    if !state.definitions.contains_key(&request.mv_id) {
        return Err(MvRepositoryError::new(
            MvRepositoryErrorKind::NotFound,
            format!("MV definition {} does not exist", request.mv_id),
        ));
    }
    state.partitions.retain(|(id, _), _| *id != request.mv_id);
    for key in request.partition_keys.into_iter().take(request.max_entries) {
        state.partitions.insert(
            (request.mv_id, key.clone()),
            StoredMvPartitionState {
                mv_id: request.mv_id,
                partition_key: key,
                status: MvPartitionRefreshStatus::Fresh,
                last_refresh_ms: Some(request.last_refresh_ms),
                base_snapshots: request.base_snapshots.clone(),
                target_snapshot_id: request.target_snapshot_id,
                last_refresh_id: Some(request.last_refresh_id),
                failure_message: None,
            },
        );
    }
    if let Some(definition) = state.definitions.get_mut(&request.mv_id) {
        definition.partition_state_complete = true;
    }
    Ok(())
}
