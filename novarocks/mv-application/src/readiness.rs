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

//! Product-owned MV accelerator readiness and candidate inventory.

use std::sync::Arc;

use novarocks_spi::connector::LakePublicationId;
use uuid::Uuid;

use crate::dependency::MvDependencyObjectRef;
use crate::persistence::definition::StoredMvDefinition;
use crate::persistence::dependency::StoredMvDependency;
use crate::process_runtime::{ProcessRuntime, TargetReadiness};
use crate::product::MvTarget;
use crate::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvProjectionRequest, MvRepository,
    MvRepositoryError, MvRepositoryErrorKind, ReplaceMvProjectionRequest,
};

pub struct MvReadinessService {
    repository: Arc<dyn MvRepository>,
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
}

#[derive(Clone)]
pub struct MvCandidateReader {
    repository: Arc<dyn MvRepository>,
}

pub struct MvRuntimePublicationLease {
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
    target: MvTarget,
    publication_id: LakePublicationId,
}

/// Product decision from DROP's durable/readiness preflight. SQL and provider
/// adapters interpret neither missing-target policy nor dependency safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvDropReadiness {
    ReadyToDrop,
    AlreadyAbsent,
}

impl Drop for MvRuntimePublicationLease {
    fn drop(&mut self) {
        self.runtime.finish(&self.target, self.publication_id);
    }
}

impl MvReadinessService {
    pub fn new(
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
    ) -> Self {
        Self {
            repository,
            runtime,
        }
    }
    pub fn candidate_reader(&self) -> MvCandidateReader {
        MvCandidateReader {
            repository: Arc::clone(&self.repository),
        }
    }
    pub async fn project(
        &self,
        operation_id: Uuid,
        target: MvTarget,
        projection: MvProjectionRequest,
    ) -> Result<(), MvRepositoryError> {
        let result = match self.repository.find_by_target(&target).await? {
            None => self
                .repository
                .create_projection(operation_id, projection)
                .await
                .map(|_| ()),
            Some(current) if current.definition.source_revision == projection.source_revision => {
                Ok(())
            }
            Some(current) => self
                .repository
                .replace_projection(
                    operation_id,
                    ReplaceMvProjectionRequest {
                        mv_id: current.definition.mv_id,
                        expected_version: current.version,
                        projection,
                    },
                )
                .await
                .map(|_| ()),
        };
        match result {
            Ok(()) => {
                self.runtime.set_ready(target);
                Ok(())
            }
            Err(error) => {
                self.runtime.set_unavailable(target, error.to_string());
                Err(error)
            }
        }
    }
    pub fn quarantine(&self, target: MvTarget, reason: String) {
        self.runtime.set_unavailable(target, reason);
    }
    pub async fn quarantine_catalog(
        &self,
        catalog: &str,
        reason: String,
    ) -> Result<(), MvRepositoryError> {
        for projection in self.repository.list_projections().await? {
            let definition = projection.definition;
            if definition
                .target_catalog
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(catalog))
            {
                if let Some(target) = target_of(&definition) {
                    self.runtime.set_unavailable(target, reason.clone());
                }
            }
        }
        Ok(())
    }
    pub async fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        if let TargetReadiness::Unavailable(reason) = self.runtime.readiness(target) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                format!("MV target is unavailable: {reason}"),
            ));
        }
        self.repository.find_by_target(target).await
    }
    pub async fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        Ok(self
            .repository
            .list_projections()
            .await?
            .into_iter()
            .filter(|projection| {
                target_of(&projection.definition).is_some_and(|target| {
                    !matches!(
                        self.runtime.readiness(&target),
                        TargetReadiness::Unavailable(_)
                    )
                })
            })
            .collect())
    }
    pub async fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let target = target_of(&projection.definition).ok_or_else(|| {
            MvRepositoryError::new(
                MvRepositoryErrorKind::Corruption,
                "MV Accelerator projection has no canonical target",
            )
        })?;
        self.load_ready(&target).await?;
        self.repository
            .list_dependencies_by_downstream(projection.definition.mv_id)
            .await
    }
    pub async fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let mut ids = Vec::new();
        for projection in self.list_ready_projections().await? {
            if self
                .list_ready_dependencies_by_downstream(&projection)
                .await?
                .iter()
                .any(|dependency| dependency.upstream == *upstream)
            {
                ids.push(projection.definition.mv_id);
            }
        }
        if ids.is_empty() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!(
                    "{} has downstream materialized views: {}",
                    upstream.display_name(),
                    ids.into_iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ))
        }
    }
    pub async fn delete_ready_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        let Some(loaded) = self.load_ready(target).await? else {
            return Ok(false);
        };
        self.repository
            .delete_projection(
                operation_id,
                DeleteMvProjectionRequest {
                    mv_id: loaded.definition.mv_id,
                    expected_version: loaded.version,
                    expected_source_revision: loaded.definition.source_revision,
                },
            )
            .await
    }
    /// Validate the durable side of a DROP before its provider target effect.
    /// An `IF EXISTS` miss is a product decision; a ready target is also
    /// checked against every currently consumable downstream dependency.
    pub async fn prepare_drop(
        &self,
        target: &MvTarget,
        upstream: &MvDependencyObjectRef,
        if_exists: bool,
    ) -> Result<MvDropReadiness, MvRepositoryError> {
        if self.load_ready(target).await?.is_none() {
            return if if_exists {
                Ok(MvDropReadiness::AlreadyAbsent)
            } else {
                Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::InvalidRequest,
                    format!(
                        "materialized view does not exist: {}.{}.{}",
                        target.catalog().unwrap_or_default(),
                        target.namespace(),
                        target.name()
                    ),
                ))
            };
        }
        self.ensure_no_ready_downstream_dependencies(upstream)
            .await?;
        Ok(MvDropReadiness::ReadyToDrop)
    }

    /// Remove the exact ready projection only after the provider drop has
    /// completed. A disappeared projection is a corruption, not a successful
    /// no-op, because the external target has already been removed.
    pub async fn delete_after_provider_drop(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<(), MvRepositoryError> {
        if self.delete_ready_projection(operation_id, target).await? {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Corruption,
                format!(
                    "materialized view {}.{}.{} metadata disappeared during drop",
                    target.catalog().unwrap_or_default(),
                    target.namespace(),
                    target.name()
                ),
            ))
        }
    }
    pub fn begin_publication(
        &self,
        target: MvTarget,
        publication_id: LakePublicationId,
    ) -> Result<MvRuntimePublicationLease, MvRepositoryError> {
        if !self.runtime.begin(target.clone(), publication_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "an MV publication is already active for this target",
            ));
        }
        Ok(MvRuntimePublicationLease {
            runtime: Arc::clone(&self.runtime),
            target,
            publication_id,
        })
    }
    pub fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        if self.runtime.has_active_publications() {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "cannot wipe MV Accelerator while an MV publication is active",
            ))
        } else {
            Ok(())
        }
    }
    pub async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError> {
        self.repository.wipe_accelerator(operation_id).await
    }
    pub async fn wipe_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        self.repository
            .wipe_projection_by_target(operation_id, target)
            .await
    }
}

impl MvCandidateReader {
    pub async fn list_candidate_definitions(
        &self,
    ) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        self.repository.list_projections().await.map(|projections| {
            projections
                .into_iter()
                .map(|projection| projection.definition)
                .collect()
        })
    }
}

fn target_of(definition: &StoredMvDefinition) -> Option<MvTarget> {
    MvTarget::try_new(
        definition.target_catalog.clone(),
        definition.target_namespace.clone()?,
        definition.target_table.clone()?,
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{MvDropReadiness, MvReadinessService};
    use crate::dependency::iceberg_mv_dependency_ref;
    use crate::process_runtime::ProcessRuntime;
    use crate::product::MvTarget;
    use crate::repository::MvRepositoryErrorKind;
    use crate::test_repository::InMemoryMvRepository;
    use novarocks_spi::connector::LakePublicationId;

    fn service() -> MvReadinessService {
        MvReadinessService::new(
            Arc::new(InMemoryMvRepository::default()),
            Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default(),
        )
    }

    #[tokio::test]
    async fn drop_preflight_keeps_if_exists_and_missing_target_policy_in_product() {
        let service = service();
        let target = MvTarget::from_parts(Some("iceberg"), "db", "missing_mv");
        let upstream = iceberg_mv_dependency_ref("iceberg", "db", "missing_mv");

        assert_eq!(
            service
                .prepare_drop(&target, &upstream, true)
                .await
                .expect("IF EXISTS missing target is a product no-op"),
            MvDropReadiness::AlreadyAbsent
        );
        let error = service
            .prepare_drop(&target, &upstream, false)
            .await
            .expect_err("missing target without IF EXISTS is rejected");
        assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
        assert!(error.message().contains("materialized view does not exist"));
    }
}
