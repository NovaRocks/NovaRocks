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

use novarocks_mv_application::activity::CanonicalMvTarget;
use novarocks_mv_application::process_runtime::ProcessRuntime;
use novarocks_mv_application::readiness::{
    MvCandidateReader as ProductCandidateReader, MvDropReadiness, MvReadinessService,
    MvRuntimePublicationLease,
};
use uuid::Uuid;

use crate::mv::activity::canonical_mv_target;
use crate::mv::domain::accelerator::projection_from_lake;
use crate::mv::domain::storage_observation::MvLakePackageObservation;
use novarocks_mv_application::dependency::MvDependencyObjectRef;
use novarocks_mv_application::persistence::definition::StoredMvDefinition;
use novarocks_mv_application::persistence::dependency::StoredMvDependency;
use novarocks_mv_application::repository::{LoadedMvProjection, MvRepository, MvRepositoryError};
use novarocks_spi::connector::LakePublicationId;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

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
    service: MvReadinessService,
    handle: tokio::runtime::Handle,
}

/// Read-only inventory for query-local MV candidate discovery.
///
/// A query freezes and validates every returned lake publication before it can
/// substitute a scan. It therefore must not inherit the refresh executor's
/// process-local readiness state: that state controls effect-capable refresh
/// and DDL consumers, not whether a retained candidate can be independently
/// proven against its exact publication.
#[derive(Clone)]
pub(crate) struct MvCandidateReader {
    reader: ProductCandidateReader,
    handle: tokio::runtime::Handle,
}

impl MvReadinessPort {
    pub(crate) fn new(
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime<CanonicalMvTarget, LakePublicationId>>,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            service: MvReadinessService::new(repository, runtime),
            handle,
        }
    }

    /// Construct the separate query inventory from the same product-owned
    /// repository. The reader intentionally does not inherit this port's
    /// process-local readiness or refresh authority.
    pub(crate) fn candidate_reader(&self) -> MvCandidateReader {
        MvCandidateReader::new(self.service.candidate_reader(), self.handle.clone())
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
        let projection = projection_from_lake(package).map_err(|error| {
            novarocks_mv_application::repository::MvRepositoryError::new(
                novarocks_mv_application::repository::MvRepositoryErrorKind::Corruption,
                error,
            )
        })?;
        self.block_on(self.service.project(operation_id, target, projection))
    }

    pub(crate) fn quarantine(&self, target: CanonicalMvTarget, reason: String) {
        self.service.quarantine(target, reason);
    }

    /// Isolate every retained projection in one catalog after an incomplete
    /// enumeration.  This is intentionally process-local: a later complete
    /// lake observation is the only operation that may make a target ready.
    pub(crate) fn quarantine_catalog(
        &self,
        catalog: &str,
        reason: String,
    ) -> Result<(), MvRepositoryError> {
        self.block_on(self.service.quarantine_catalog(catalog, reason))
    }

    pub(crate) fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        self.block_on(self.service.load_ready(&canonical_mv_target(target)))
    }

    /// Enumerate only projections whose current-process readiness permits
    /// consumption.  A catalog/package observation failure therefore removes
    /// exactly that target from SHOW, rewrite, scheduler and maintenance
    /// candidates instead of reviving a retained StateStore projection.
    pub(crate) fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        self.block_on(self.service.list_ready_projections())
    }

    /// Dependency reads are tied to a ready downstream projection.  Callers
    /// cannot accidentally pair a live dependency index with a quarantined
    /// lake package.
    pub(crate) fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.block_on(
            self.service
                .list_ready_dependencies_by_downstream(projection),
        )
    }

    /// Reject a mutation only when a currently consumable downstream MV
    /// depends on the upstream object. Quarantined projections are retained
    /// accelerator data, not active semantic dependencies.
    pub(crate) fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        self.block_on(
            self.service
                .ensure_no_ready_downstream_dependencies(upstream),
        )
    }

    /// Delete the current ready projection using its opaque loaded version.
    /// DDL never fabricates a repository version or reads the Accelerator
    /// behind the readiness boundary.
    pub(crate) fn delete_ready_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        self.block_on(
            self.service
                .delete_ready_projection(operation_id, &canonical_mv_target(target)),
        )
    }

    /// Synchronous bridge to the product-owned DROP durable preflight.
    pub(crate) fn prepare_drop(
        &self,
        target: &MvTarget,
        upstream: &MvDependencyObjectRef,
        if_exists: bool,
    ) -> Result<MvDropReadiness, MvRepositoryError> {
        self.block_on(
            self.service
                .prepare_drop(&canonical_mv_target(target), upstream, if_exists),
        )
    }

    /// Synchronous bridge to the product-owned post-provider-delete CAS.
    pub(crate) fn delete_after_provider_drop(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<(), MvRepositoryError> {
        self.block_on(
            self.service
                .delete_after_provider_drop(operation_id, &canonical_mv_target(target)),
        )
    }

    pub(crate) fn begin_publication(
        &self,
        target: &MvTarget,
        publication_id: novarocks_spi::connector::LakePublicationId,
    ) -> Result<MvRuntimePublicationLease, MvRepositoryError> {
        self.service
            .begin_publication(canonical_mv_target(target), publication_id)
    }

    /// Reject the test/harness-only whole-family Accelerator wipe while this
    /// FE owns any publication. A wipe has no distributed coordination role;
    /// the runner immediately restarts this FE after the wipe succeeds.
    pub(crate) fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        self.service.ensure_no_active_publications()
    }

    /// Test/harness-only wipe of the whole current Accelerator family.
    ///
    /// Process readiness is deliberately left untouched: the wipe procedure's
    /// contract is that the runner restarts this FE immediately afterwards, and
    /// startup observation is the only permitted rebuild path.
    pub(crate) fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError> {
        self.block_on(self.service.wipe_accelerator(operation_id))
    }

    /// Test/harness-only wipe of one target's rebuildable projection, used by
    /// the stateless-rebuild round-trip to prove the lake alone can restore it.
    pub(crate) fn wipe_projection(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        self.block_on(
            self.service
                .wipe_projection(operation_id, &canonical_mv_target(target)),
        )
    }
}

impl MvCandidateReader {
    /// Construct the read-only query candidate inventory from its durable
    /// discovery dependency. This intentionally accepts neither process
    /// readiness nor any refresh executor capability.
    pub(crate) fn new(reader: ProductCandidateReader, handle: tokio::runtime::Handle) -> Self {
        Self { reader, handle }
    }

    /// Enumerate retained candidate definitions without exposing a loaded
    /// projection's CAS version to query discovery. Every member remains
    /// optional until its exact lake publication and every frozen input/output
    /// revision are verified.
    pub(crate) fn list_candidate_definitions(
        &self,
    ) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| {
                self.handle
                    .block_on(self.reader.list_candidate_definitions())
            }),
            Err(_) => self
                .handle
                .block_on(self.reader.list_candidate_definitions()),
        }
    }
}

fn canonical_target(package: &MvLakePackageObservation) -> CanonicalMvTarget {
    CanonicalMvTarget::from_parts(
        Some(package.table.instance_id.as_str()),
        &package.table.namespace,
        &package.table.table,
    )
}
