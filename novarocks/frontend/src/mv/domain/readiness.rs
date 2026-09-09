// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Readiness-aware access to the rebuildable MV Accelerator.
//!
//! Consumers use this façade instead of reading StateStore directly.  An
//! unavailable target never falls back to a retained projection, so SHOW,
//! rewrite, and scheduling cannot accidentally consume stale lake facts.

use std::sync::Arc;

use uuid::Uuid;

use crate::mv::activity::CanonicalMvTarget;
use crate::mv::domain::dependency::model::MvDependencyObjectRef;
use crate::mv::domain::persistence::dependency::StoredMvDependency;
use crate::mv::domain::projector::MvAcceleratorProjector;
use crate::mv::domain::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvRepository, MvRepositoryError,
    MvRepositoryErrorKind, MvTarget,
};
use crate::mv::domain::storage_observation::MvLakePackageObservation;
use crate::mv::process_runtime::{MvTargetReadiness, ProcessRuntime};

/// The synchronous face of an asynchronous MV projection store.
///
/// The repository contract is async because its work is durable I/O. Almost
/// every reader of MV readiness, though, sits inside the `spawn_blocking`
/// closure that runs SQL statements, or on one of the two dedicated maintenance
/// threads. Propagating `await` through them would not remove the bridge — it
/// would move it onto a thread that has to block anyway — and would further
/// require restructuring statement admission, which belongs to the
/// query-application and query-preparation work lines rather than here.
///
/// So the adaptation lives in this one type, whose job is adaptation, and in no
/// domain contract. It is a single place to delete once those work lines lift
/// the surrounding boundaries.
pub struct MvReadinessPort {
    repository: Arc<dyn MvRepository>,
    projector: MvAcceleratorProjector,
    runtime: Arc<ProcessRuntime>,
    handle: tokio::runtime::Handle,
}

/// RAII ownership of the one current-process publication for a target.
///
/// It has no durable representation: process loss intentionally forgets it.
pub(crate) struct MvRuntimePublicationLease {
    runtime: Arc<ProcessRuntime>,
    target: CanonicalMvTarget,
    publication_id: novarocks_spi::connector::LakePublicationId,
}

impl Drop for MvRuntimePublicationLease {
    fn drop(&mut self) {
        self.runtime.finish(&self.target, self.publication_id);
    }
}

impl MvReadinessPort {
    pub(crate) fn new(
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime>,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            projector: MvAcceleratorProjector::new(Arc::clone(&repository)),
            repository,
            runtime,
            handle,
        }
    }

    /// Drives one durable MV operation from a synchronous caller. Confined to
    /// this type on purpose; see the type-level note.
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| self.handle.block_on(future)),
            Err(_) => self.handle.block_on(future),
        }
    }

    pub(crate) fn project_observed(
        &self,
        operation_id: Uuid,
        package: &MvLakePackageObservation,
    ) -> Result<(), MvRepositoryError> {
        let target = canonical_target(package);
        match self.block_on(self.projector.project_once(operation_id, package)) {
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

    pub(crate) fn quarantine(&self, target: CanonicalMvTarget, reason: String) {
        self.runtime.set_unavailable(target, reason);
    }

    /// Isolate every retained projection in one catalog after an incomplete
    /// enumeration.  This is intentionally process-local: a later complete
    /// lake observation is the only operation that may make a target ready.
    pub(crate) fn quarantine_catalog(
        &self,
        catalog: &str,
        reason: String,
    ) -> Result<(), MvRepositoryError> {
        for projection in self.block_on(self.repository.list_projections())? {
            let definition = projection.definition;
            if !definition
                .target_catalog
                .as_deref()
                .is_some_and(|target_catalog| target_catalog.eq_ignore_ascii_case(catalog))
            {
                continue;
            }
            let (Some(namespace), Some(table)) = (
                definition.target_namespace.as_deref(),
                definition.target_table.as_deref(),
            ) else {
                continue;
            };
            self.runtime.set_unavailable(
                CanonicalMvTarget::from_parts(Some(catalog), namespace, table),
                reason.clone(),
            );
        }
        Ok(())
    }

    pub(crate) fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        let canonical = CanonicalMvTarget::from_mv_target(target);
        if let MvTargetReadiness::Unavailable(reason) = self.runtime.readiness(&canonical) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                format!("MV target is unavailable: {reason}"),
            ));
        }
        self.block_on(self.repository.find_by_target(target))
    }

    /// Enumerate only projections whose current-process readiness permits
    /// consumption.  A catalog/package observation failure therefore removes
    /// exactly that target from SHOW, rewrite, scheduler and maintenance
    /// candidates instead of reviving a retained StateStore projection.
    pub(crate) fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        let projections = self
            .block_on(self.repository.list_projections())?
            .into_iter()
            .filter(|projection| {
                let target = MvTarget {
                    catalog: projection.definition.target_catalog.clone(),
                    database: projection
                        .definition
                        .target_namespace
                        .clone()
                        .unwrap_or_default(),
                    name: projection
                        .definition
                        .target_table
                        .clone()
                        .unwrap_or_default(),
                };
                !target.database.is_empty()
                    && !target.name.is_empty()
                    && !matches!(
                        self.runtime
                            .readiness(&CanonicalMvTarget::from_mv_target(&target)),
                        MvTargetReadiness::Unavailable(_)
                    )
            })
            .collect::<Vec<_>>();
        Ok(projections)
    }

    /// Dependency reads are tied to a ready downstream projection.  Callers
    /// cannot accidentally pair a live dependency index with a quarantined
    /// lake package.
    pub(crate) fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let target = MvTarget {
            catalog: projection.definition.target_catalog.clone(),
            database: projection
                .definition
                .target_namespace
                .clone()
                .unwrap_or_default(),
            name: projection
                .definition
                .target_table
                .clone()
                .unwrap_or_default(),
        };
        if target.database.is_empty() || target.name.is_empty() {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Corruption,
                format!(
                    "MV Accelerator projection {} has no canonical target",
                    projection.definition.mv_id
                ),
            ));
        }
        let _ = self.load_ready(&target)?;
        self.block_on(
            self.repository
                .list_dependencies_by_downstream(projection.definition.mv_id),
        )
    }

    /// Reject a mutation only when a currently consumable downstream MV
    /// depends on the upstream object. Quarantined projections are retained
    /// accelerator data, not active semantic dependencies.
    pub(crate) fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let mut downstream_ids = Vec::new();
        for projection in self.list_ready_projections()? {
            let dependencies = self.list_ready_dependencies_by_downstream(&projection)?;
            if dependencies
                .iter()
                .any(|dependency| dependency.upstream == *upstream)
            {
                downstream_ids.push(projection.definition.mv_id);
            }
        }
        if downstream_ids.is_empty() {
            return Ok(());
        }
        Err(MvRepositoryError::new(
            MvRepositoryErrorKind::Conflict,
            format!(
                "{} has downstream materialized views: {}",
                upstream.display_name(),
                downstream_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ))
    }

    /// Delete the current ready projection using its opaque loaded version.
    /// DDL never fabricates a repository version or reads the Accelerator
    /// behind the readiness boundary.
    pub(crate) fn delete_ready_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        let Some(loaded) = self.load_ready(target)? else {
            return Ok(false);
        };
        self.block_on(self.repository.delete_projection(
            operation_id,
            DeleteMvProjectionRequest {
                mv_id: loaded.definition.mv_id,
                expected_version: loaded.version,
                expected_source_revision: loaded.definition.source_revision,
            },
        ))
    }

    pub(crate) fn begin_publication(
        &self,
        target: &MvTarget,
        publication_id: novarocks_spi::connector::LakePublicationId,
    ) -> Result<MvRuntimePublicationLease, MvRepositoryError> {
        let canonical = CanonicalMvTarget::from_mv_target(target);
        if !self.runtime.begin(canonical.clone(), publication_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "an MV publication is already active for this target",
            ));
        }
        Ok(MvRuntimePublicationLease {
            runtime: Arc::clone(&self.runtime),
            target: canonical,
            publication_id,
        })
    }

    /// Reject the test/harness-only whole-family Accelerator wipe while this
    /// FE owns any publication. A wipe has no distributed coordination role;
    /// the runner immediately restarts this FE after the wipe succeeds.
    pub(crate) fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        if self.runtime.has_active_publications() {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "cannot wipe MV Accelerator while an MV publication is active",
            ));
        }
        Ok(())
    }

    /// Test/harness-only wipe of the whole current Accelerator family.
    ///
    /// Process readiness is deliberately left untouched: the wipe procedure's
    /// contract is that the runner restarts this FE immediately afterwards, and
    /// startup observation is the only permitted rebuild path.
    pub(crate) fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError> {
        self.block_on(self.repository.wipe_accelerator(operation_id))
    }

    /// Test/harness-only wipe of one target's rebuildable projection, used by
    /// the stateless-rebuild round-trip to prove the lake alone can restore it.
    pub(crate) fn wipe_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        self.block_on(
            self.repository
                .wipe_projection_by_target(operation_id, target),
        )
    }
}

fn canonical_target(package: &MvLakePackageObservation) -> CanonicalMvTarget {
    CanonicalMvTarget::from_parts(
        Some(package.table.instance_id.as_str()),
        &package.table.namespace,
        &package.table.table,
    )
}
