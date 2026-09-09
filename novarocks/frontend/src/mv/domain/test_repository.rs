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

//! Provider-neutral in-memory MV Accelerator used by domain tests.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use bytes::Bytes;
use novarocks_state_store_api::VersionToken;
use uuid::Uuid;

use super::dependency::model::MvDependencyObjectRef;
use super::persistence::definition::StoredMvDefinition;
use super::persistence::dependency::StoredMvDependency;
use super::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvProjectionRequest, MvProjectionVersion,
    MvPublishedProjection, MvRepository, MvRepositoryError, MvRepositoryErrorKind, MvTarget,
    ReplaceMvProjectionRequest,
};

#[derive(Default)]
pub struct InMemoryMvRepository {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_id: i64,
    next_version: u64,
    projections: BTreeMap<i64, StoredMvDefinition>,
    versions: BTreeMap<i64, MvProjectionVersion>,
    dependencies: BTreeMap<i64, Vec<StoredMvDependency>>,
}

impl InMemoryMvRepository {
    fn state(&self) -> Result<MutexGuard<'_, State>, MvRepositoryError> {
        self.state.lock().map_err(|_| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Corruption,
                "in-memory MV Accelerator lock poisoned",
            )
        })
    }

    fn next_version(state: &mut State) -> MvProjectionVersion {
        state.next_version += 1;
        let bytes = Bytes::copy_from_slice(&state.next_version.to_be_bytes());
        MvProjectionVersion::from_store(
            VersionToken::try_from(bytes).expect("non-empty test version token"),
        )
    }

    fn loaded(state: &State, mv_id: i64) -> Option<LoadedMvProjection> {
        Some(LoadedMvProjection {
            definition: state.projections.get(&mv_id)?.clone(),
            version: state.versions.get(&mv_id)?.clone(),
        })
    }

    fn target(definition: &StoredMvDefinition) -> Option<MvTarget> {
        Some(MvTarget {
            catalog: definition.target_catalog.clone(),
            database: definition.target_namespace.clone()?,
            name: definition.target_table.clone()?,
        })
    }

    fn definition(mv_id: i64, request: &MvProjectionRequest) -> StoredMvDefinition {
        let (
            last_refresh_ms,
            last_refresh_rows,
            last_refresh_snapshots,
            last_refresh_table_object_ids,
            last_refreshed_iceberg_snapshot_id,
        ) = match &request.publication {
            MvPublishedProjection::NeverPublished => {
                (None, None, BTreeMap::new(), BTreeMap::new(), None)
            }
            MvPublishedProjection::Published(waterline) => (
                Some(waterline.last_refresh_ms),
                Some(waterline.last_refresh_rows),
                waterline.base_snapshots.clone(),
                waterline.base_table_object_ids.clone(),
                Some(waterline.last_refreshed_iceberg_snapshot_id),
            ),
        };
        StoredMvDefinition {
            mv_id,
            query_definition: request.definition.query_definition.clone(),
            base_table_refs: request.definition.base_table_refs.clone(),
            primary_key_columns: request.definition.primary_key_columns.clone(),
            storage_engine: request.definition.storage_engine.clone(),
            target_catalog: request.definition.target_catalog.clone(),
            target_namespace: request.definition.target_namespace.clone(),
            target_table: request.definition.target_table.clone(),
            schema_contract: request.definition.schema_contract.clone(),
            partition_spec: request.definition.partition_spec.clone(),
            last_refresh_ms,
            last_refresh_rows,
            last_refresh_snapshots,
            last_refresh_table_object_ids,
            last_refreshed_iceberg_snapshot_id,
            refresh_policy: request.refresh.policy.clone(),
            refresh_paused: request.refresh.paused,
            refresh_interval_ms: request.refresh.interval_ms,
            max_staleness_ms: request.refresh.max_staleness_ms,
            created_at_ms: request.definition.created_at_ms,
            source_revision: request.source_revision.clone(),
        }
    }

    fn stored_dependencies(mv_id: i64, request: &MvProjectionRequest) -> Vec<StoredMvDependency> {
        request
            .dependencies
            .iter()
            .map(|dependency| StoredMvDependency {
                downstream_mv_id: mv_id,
                upstream: dependency.upstream.clone(),
                created_at_ms: dependency.created_at_ms,
            })
            .collect()
    }
}

impl MvRepository for InMemoryMvRepository {
    fn create_projection(
        &self,
        _operation_id: Uuid,
        projection: MvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        let mut state = self.state()?;
        let candidate = Self::definition(state.next_id + 1, &projection);
        let target = Self::target(&candidate).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::InvalidRequest,
                "MV Accelerator projection target is incomplete",
            )
        })?;
        if state
            .projections
            .values()
            .any(|definition| Self::target(definition).as_ref() == Some(&target))
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV Accelerator target already exists",
            ));
        }
        state.next_id += 1;
        let mv_id = state.next_id;
        let version = Self::next_version(&mut state);
        state.projections.insert(mv_id, candidate.clone());
        state.versions.insert(mv_id, version.clone());
        state
            .dependencies
            .insert(mv_id, Self::stored_dependencies(mv_id, &projection));
        Ok(LoadedMvProjection {
            definition: candidate,
            version,
        })
    }

    fn replace_projection(
        &self,
        _operation_id: Uuid,
        request: ReplaceMvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        let mut state = self.state()?;
        if state.versions.get(&request.mv_id) != Some(&request.expected_version) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV projection changed before CAS",
            ));
        }
        let next = Self::definition(request.mv_id, &request.projection);
        let next_target = Self::target(&next).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::InvalidRequest,
                "MV Accelerator projection target is incomplete",
            )
        })?;
        if state.projections.iter().any(|(mv_id, definition)| {
            *mv_id != request.mv_id && Self::target(definition).as_ref() == Some(&next_target)
        }) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "replacement MV Accelerator target already exists",
            ));
        }
        let version = Self::next_version(&mut state);
        state.projections.insert(request.mv_id, next.clone());
        state.versions.insert(request.mv_id, version.clone());
        state.dependencies.insert(
            request.mv_id,
            Self::stored_dependencies(request.mv_id, &request.projection),
        );
        Ok(LoadedMvProjection {
            definition: next,
            version,
        })
    }

    fn load_by_id(&self, mv_id: i64) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        let state = self.state()?;
        Ok(Self::loaded(&state, mv_id))
    }

    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        let state = self.state()?;
        Ok(state
            .projections
            .iter()
            .find(|(_, definition)| Self::target(definition).as_ref() == Some(target))
            .and_then(|(mv_id, _)| Self::loaded(&state, *mv_id)))
    }

    fn list_projections(&self) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        let state = self.state()?;
        Ok(state
            .projections
            .keys()
            .filter_map(|mv_id| Self::loaded(&state, *mv_id))
            .collect())
    }

    fn delete_projection(
        &self,
        _operation_id: Uuid,
        request: DeleteMvProjectionRequest,
    ) -> Result<bool, MvRepositoryError> {
        let mut state = self.state()?;
        let Some(current) = state.projections.get(&request.mv_id) else {
            return Ok(false);
        };
        if state.versions.get(&request.mv_id) != Some(&request.expected_version)
            || current.source_revision != request.expected_source_revision
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "MV projection source or version changed before delete",
            ));
        }
        state.projections.remove(&request.mv_id);
        state.versions.remove(&request.mv_id);
        state.dependencies.remove(&request.mv_id);
        Ok(true)
    }

    fn wipe_projection_by_target(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        let Some(loaded) = self.find_by_target(target)? else {
            return Ok(false);
        };
        self.delete_projection(
            operation_id,
            DeleteMvProjectionRequest {
                mv_id: loaded.definition.mv_id,
                expected_version: loaded.version,
                expected_source_revision: loaded.definition.source_revision,
            },
        )
    }

    fn wipe_accelerator(&self, _operation_id: Uuid) -> Result<(), MvRepositoryError> {
        let mut state = self.state()?;
        *state = State::default();
        Ok(())
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

    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        if self.list_downstream_dependencies(upstream)?.is_empty() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!(
                    "{} has downstream materialized views",
                    upstream.display_name()
                ),
            ))
        }
    }
}
