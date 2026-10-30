// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Readiness-aware access to the rebuildable MV Accelerator.
//!
//! Consumers use this façade instead of reading StateStore directly.  An
//! unavailable target never falls back to a retained projection, so SHOW,
//! rewrite, and scheduling cannot accidentally consume stale lake facts.

use novarocks_mv_application::activity::CanonicalMvTarget;
use novarocks_mv_application::readiness::{
    MvCurrentProjectionRequest, MvKeyedCandidateReader as ProductCandidateReader,
    MvKeyedCurrentProjectionSource, MvKeyedDropReadiness, MvKeyedProjectionDeleteGuard,
    MvKeyedProjectionInstallOutcome, MvKeyedReadOnlyCurrentProjectionSource,
    MvKeyedReadinessExecution as ProductReadinessExecution, MvKeyedReadinessService,
    MvProjectionError,
};
#[cfg(novarocks_dev_controls)]
use std::sync::Arc;
use uuid::Uuid;

use crate::mv::activity::canonical_mv_target;
use novarocks_mv_application::dependency::MvDependencyObjectIdentity;
use novarocks_mv_application::persistence::dependency::StoredMvDependency;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::repository::MvRepositoryError;
use novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

/// Role-local readiness facade over the same product service.
///
/// Unmigrated synchronous callers retain an explicit legacy bridge. Governed
/// async callers select an original request view; it never blocks its Source
/// driver or changes this role-local service's execution mode.
pub struct MvReadinessPort {
    service: MvKeyedReadinessService,
    management: std::sync::Arc<novarocks_mv_application::management::ManagementEntrance>,
    handle: tokio::runtime::Handle,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<crate::mv::test_controls::MvTestControls>>,
}

fn local_inventory_bound() -> novarocks_mv_application::repository::MvProjectionInventoryBound {
    use novarocks_mv_application::repository::MvProjectionInventoryBound;

    MvProjectionInventoryBound {
        entries: novarocks_query_application::api::LocalResultBound::V1.rows,
        snapshot_bytes: 16 * 1024 * 1024,
        raw_page_bytes: 1024 * 1024,
        single_name_bytes: 65_536,
        continuation_token_bytes: 4096,
        decode: novarocks_mv_application::persistence::validation::PersistenceDecodeBudget {
            max_document_bytes: 1024 * 1024,
            max_working_set_bytes: 4 * 1024 * 1024,
            ..Default::default()
        },
    }
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
    service: MvKeyedReadinessService,
    management: std::sync::Arc<novarocks_mv_application::management::ManagementEntrance>,
    handle: tokio::runtime::Handle,
    execution: CandidateExecution,
}
#[derive(Clone)]
enum CandidateExecution {
    Legacy,
    Original(std::sync::Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>),
}

/// Borrower only: the caller keeps the same Source/Root in stable custody.
pub(crate) struct MvReadinessExecution<'a> {
    port: &'a MvReadinessPort,
    execution: std::sync::Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
    service: ProductReadinessExecution,
}

impl MvReadinessPort {
    /// A mutation guard already holds the exact Provider observation. Revoke
    /// foreign local management without reading or refreshing an Accelerator.
    pub(crate) fn revoke_foreign_current_management(
        &self,
        view: &novarocks_spi::connector::KeyedDocumentView,
        catalog: &novarocks_spi::connector::CatalogHandle,
    ) -> Result<bool, novarocks_query_application::session_error::QueryServiceError> {
        self.management
            .revoke_foreign_current_management(view, catalog)
            .map_err(document_application_error)
    }
    /// Construct the role-local synchronous adapter from the already-owned
    /// product readiness service. This adapter cannot create an independent
    /// repository/runtime state pair.
    pub(crate) fn from_product(
        service: MvKeyedReadinessService,
        handle: tokio::runtime::Handle,
        management: std::sync::Arc<novarocks_mv_application::management::ManagementEntrance>,
    ) -> Self {
        Self {
            service,
            management,
            handle,
            #[cfg(novarocks_dev_controls)]
            controls: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Arc<crate::mv::test_controls::MvTestControls>,
    ) -> Self {
        self.controls = Some(controls);
        self
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn test_controls(&self) -> Option<&crate::mv::test_controls::MvTestControls> {
        self.controls.as_deref()
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn test_controls_owner(
        &self,
    ) -> Option<Arc<crate::mv::test_controls::MvTestControls>> {
        self.controls.clone()
    }

    #[cfg(test)]
    pub(crate) fn in_execution<'a>(
        &'a self,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
    ) -> MvReadinessExecution<'a> {
        self.in_requests(std::sync::Arc::new(execution.clone()))
    }
    pub(crate) fn in_requests(
        &self,
        execution: std::sync::Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
    ) -> MvReadinessExecution<'_> {
        MvReadinessExecution {
            port: self,
            service: self.service.in_requests(std::sync::Arc::clone(&execution)),
            execution,
        }
    }

    /// Construct the separate query inventory from the same product-owned
    /// repository. The reader intentionally does not inherit this port's
    /// process-local readiness or refresh authority.
    pub(crate) fn candidate_reader(&self) -> MvCandidateReader {
        MvCandidateReader {
            reader: self.service.candidate_reader(),
            service: self.service.clone(),
            management: self.management.clone(),
            handle: self.handle.clone(),
            execution: CandidateExecution::Legacy,
        }
    }

    /// Legacy bridge only. Original request views expose async methods and
    /// never pass their borrowed context through this blocking adapter.
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| self.handle.block_on(future)),
            Err(_) => self.handle.block_on(future),
        }
    }

    pub(crate) fn observe_current_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.block_on(
            self.service
                .observe_and_install(operation_id, request, source),
        )
    }

    pub(crate) fn observe_current_read_only_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedReadOnlyCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.block_on(self.service.observe_current_read_only_and_install(
            operation_id,
            request,
            source,
            "read-only lake observation requires management readmission".to_string(),
        ))
    }

    pub(crate) fn quarantine(
        &self,
        target: CanonicalMvTarget,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        self.block_on(self.service.invalidate_current(target, reason))
    }

    /// Isolate every retained projection in one catalog after an incomplete
    /// enumeration.  This is intentionally process-local: a later complete
    /// lake observation is the only operation that may make a target ready.
    pub(crate) fn quarantine_catalog(
        &self,
        catalog: &str,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        let projections =
            self.block_on(self.service.candidate_reader().list_candidate_definitions())?;
        for projection in projections {
            if projection.facts.target().catalog() == Some(catalog) {
                self.block_on(
                    self.service
                        .invalidate_current(projection.facts.target().clone(), reason.clone()),
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.block_on(self.service.list_ready_projections())
    }

    /// Dependency reads are tied to a ready downstream projection.  Callers
    /// cannot accidentally pair a live dependency index with a quarantined
    /// lake package.
    pub(crate) fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedKeyedMvProjection,
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
        upstream: &MvDependencyObjectIdentity,
    ) -> Result<(), MvRepositoryError> {
        self.block_on(
            self.service
                .ensure_no_ready_downstream_dependencies(upstream),
        )
    }

    /// Reject the test/harness-only whole-family Accelerator wipe while this
    /// FE owns any publication. A wipe has no distributed coordination role;
    /// the runner immediately restarts this FE after the wipe succeeds.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        self.service.ensure_no_active_publications()
    }

    // Design: ADR-0174 (docs/adr/ADR-0174-test-capabilities-and-finite-role-authorization.md)
    /// Permanently stop MV admission and join the existing product workers
    /// before the authorized whole-family wipe. The caller must stop/reap this
    /// FE immediately; this operation cannot resume its old MV runtime.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn wipe_accelerator_for_test(
        &self,
        product: &novarocks_mv_application::service::MvKeyedProductService,
        operation_id: Uuid,
        claim: &mut novarocks_failpoint::scope::ActionClaim,
    ) -> Result<(), String> {
        if !claim.cancellation().is_active() {
            return Err("MV wipe authorization expired before stopping".into());
        }
        product.begin_stopping();
        self.block_on(product.shutdown_background_workers_until(claim.cancellation().deadline()))
            .map_err(|error| format!("stop MV background work before wipe: {error}"))?;
        self.ensure_no_active_publications()
            .map_err(|error| error.to_string())?;
        if !claim.cancellation().is_active() {
            return Err("MV wipe authorization expired before destructive action".into());
        }
        self.block_on(self.service.wipe_accelerator(operation_id))
            .map_err(|error| format!("wipe MV Accelerator family: {error}"))?;
        claim.record_hit().map_err(|error| error.to_string())
    }
}

impl MvReadinessExecution<'_> {
    pub(crate) fn candidate_reader(&self) -> MvCandidateReader {
        self.port
            .candidate_reader()
            .in_requests(std::sync::Arc::clone(&self.execution))
    }
    pub(crate) async fn observe_current_and_install(
        &self,
        operation: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.service
            .observe_and_install(operation, request, source)
            .await
    }
    pub(crate) async fn observe_current_read_only_and_install(
        &self,
        operation: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedReadOnlyCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.service
            .observe_current_read_only_and_install(
                operation,
                request,
                source,
                "read-only lake observation requires management readmission".into(),
            )
            .await
    }
    #[cfg(test)]
    pub(crate) async fn query_admission(
        &self,
        target: &MvTarget,
    ) -> Result<novarocks_mv_application::readiness::MvQueryAdmission, MvRepositoryError> {
        self.service
            .query_admission(&canonical_mv_target(target))
            .await
    }
    pub(crate) async fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.service
            .load_ready_settled(&canonical_mv_target(target))
            .await
    }
    /// The returned inventory already retains this same weak request selection;
    /// its next_ready/next_listable methods are async product methods.
    pub(crate) async fn local_projection_inventory(
        &self,
    ) -> Result<
        novarocks_mv_application::readiness::MvBoundedKeyedProjectionInventory,
        MvRepositoryError,
    > {
        self.service
            .bounded_projection_inventory(local_inventory_bound())
            .await
    }
    pub(crate) async fn list_ready_projections(
        &self,
    ) -> Result<Vec<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.service.list_ready_projections().await
    }
    pub(crate) async fn list_ready_dependencies_by_downstream(
        &self,
        projection: &LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.service
            .list_ready_dependencies_by_downstream(projection)
            .await
    }
    pub(crate) async fn list_local_dependencies_by_downstream(
        &self,
        projection: &LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.service
            .list_ready_dependencies_by_downstream_bounded(
                projection,
                novarocks_mv_application::repository::MvDependencyReadBound {
                    inventory: local_inventory_bound(),
                    entries: 4096,
                    collection_bytes: 4 * 1024 * 1024,
                },
            )
            .await
    }
    pub(crate) async fn prepare_drop(
        &self,
        target: &MvTarget,
        if_exists: bool,
    ) -> Result<MvKeyedDropReadiness, MvProjectionError> {
        self.service
            .prepare_drop(&canonical_mv_target(target), if_exists)
            .await
    }
    pub(crate) async fn delete_after_provider_drop(
        &self,
        operation: Uuid,
        guard: MvKeyedProjectionDeleteGuard,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.service
            .delete_after_provider_drop(operation, guard)
            .await
    }
}

impl MvCandidateReader {
    /// A request clone retains only the exact weak Context, never a new owner.
    #[cfg(test)]
    pub(crate) fn in_execution(
        &self,
        execution: &novarocks_state_store_api::StateStoreExecutionContext,
    ) -> Self {
        self.in_requests(std::sync::Arc::new(execution.clone()))
    }
    pub(crate) fn in_requests(
        &self,
        execution: std::sync::Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
    ) -> Self {
        Self {
            reader: self
                .service
                .in_requests(std::sync::Arc::clone(&execution))
                .candidate_reader(),
            service: self.service.clone(),
            management: self.management.clone(),
            handle: self.handle.clone(),
            execution: CandidateExecution::Original(execution),
        }
    }
    fn require_legacy_sync(&self) -> Result<(), MvRepositoryError> {
        match &self.execution {
            CandidateExecution::Legacy => Ok(()),
            CandidateExecution::Original(_) => Err(MvRepositoryError::new(
                novarocks_mv_application::repository::MvRepositoryErrorKind::InvalidRequest,
                "original MV candidate request requires asynchronous execution",
            )),
        }
    }
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let handle = &self.handle;
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => handle.block_on(future),
        }
    }
    pub(crate) fn list_candidate_definitions(
        &self,
    ) -> Result<Vec<StoredKeyedMvProjection>, MvRepositoryError> {
        self.require_legacy_sync()?;
        self.block_on(self.reader.list_candidate_definitions())
    }

    pub(crate) async fn list_candidate_definitions_async(
        &self,
    ) -> Result<Vec<StoredKeyedMvProjection>, MvRepositoryError> {
        self.reader.list_candidate_definitions().await
    }
    pub(crate) fn observe_current(
        &self,
        lease: &novarocks_spi::connector::document_storage::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::document_storage::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        String,
    > {
        self.require_legacy_sync()
            .map_err(|error| error.to_string())?;
        self.block_on(self.observe_current_async(lease, request))
    }

    /// Legacy MV preparation still renders at its pre-Action String boundary.
    pub(crate) async fn observe_current_async(
        &self,
        lease: &novarocks_spi::connector::document_storage::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::document_storage::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        String,
    > {
        self.observe_current_application_async(lease, request)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn observe_current_application_async(
        &self,
        lease: &novarocks_spi::connector::document_storage::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::document_storage::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        novarocks_query_application::session_error::QueryServiceError,
    > {
        let Self {
            service,
            management,
            ..
        } = self;
        let (mut observed,convergence)=novarocks_mv_application::persistence::keyed_documents::observe_current_keyed_documents_with_management_async(lease,request.clone(),Default::default(),management).await.map_err(document_application_error)?;
        if observed.deployment_owner != *management.owner()
            || observed.process_incarnation != *management.incarnation()
        {
            management
                .close_on_keyed_current_management_mismatch_async(lease, request.clone())
                .await
                .map_err(document_application_error)?;
            struct ReadOnly(novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments);
            #[async_trait::async_trait]
            impl MvKeyedReadOnlyCurrentProjectionSource for ReadOnly {
                async fn observe_current_keyed_read_only(&self,_:MvCurrentProjectionRequest)->Result<novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,MvProjectionError>{
                    Ok(self.0.clone())
                }
            }
            let target = CanonicalMvTarget::from_parts(
                Some(request.target.instance_id.as_str()),
                &request.target.namespace,
                &request.target.table,
            );
            let projection_request = MvCurrentProjectionRequest::try_new(
                request.catalog_handle,
                target,
                request.context,
                Default::default(),
            )
            .map_err(projection_application_error)?;
            let operation = Uuid::now_v7();
            let source = ReadOnly(observed.clone());
            let reason = "fresh MV management slot names another writer".into();
            match &self.execution {
                CandidateExecution::Legacy => {
                    service
                        .observe_current_read_only_and_install(
                            operation,
                            projection_request,
                            &source,
                            reason,
                        )
                        .await
                }
                CandidateExecution::Original(execution) => {
                    service
                        .in_requests(std::sync::Arc::clone(execution))
                        .observe_current_read_only_and_install(
                            operation,
                            projection_request,
                            &source,
                            reason,
                        )
                        .await
                }
            }
            .map_err(projection_application_error)?;
            return Ok(observed);
        }
        if convergence.reinstall_current_projection {
            let (fresh, admission, _) = management
                .observe_keyed_admission_async(lease, request.clone(), Default::default())
                .await
                .map_err(document_application_error)?;
            observed = fresh;
            struct Source{observed:novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,admission:std::sync::Mutex<Option<novarocks_mv_application::management::MvCurrentManagementAdmission>>}
            #[async_trait::async_trait]
            impl MvKeyedCurrentProjectionSource for Source {
                async fn observe_current_keyed(&self,_:MvCurrentProjectionRequest)->Result<(novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,novarocks_mv_application::management::MvCurrentManagementAdmission),MvProjectionError>{
                    Ok((self.observed.clone(), self.admission.lock().map_err(|_|MvProjectionError::new(novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,"slot projection admission lock poisoned"))?.take().ok_or_else(||MvProjectionError::new(novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,"slot projection admission already consumed"))?))
                }
            }
            let target = CanonicalMvTarget::from_parts(
                Some(request.target.instance_id.as_str()),
                &request.target.namespace,
                &request.target.table,
            );
            let projection_request = MvCurrentProjectionRequest::try_new(
                request.catalog_handle,
                target,
                request.context,
                Default::default(),
            )
            .map_err(projection_application_error)?;
            let operation = Uuid::now_v7();
            let source = Source {
                observed: observed.clone(),
                admission: std::sync::Mutex::new(Some(admission)),
            };
            match &self.execution {
                CandidateExecution::Legacy => {
                    service
                        .observe_and_install(operation, projection_request, &source)
                        .await
                }
                CandidateExecution::Original(execution) => {
                    service
                        .in_requests(std::sync::Arc::clone(execution))
                        .observe_and_install(operation, projection_request, &source)
                        .await
                }
            }
            .map_err(projection_application_error)?;
        }
        Ok(observed)
    }
}

/// Project only the concrete Connector author; never classify a display string.
pub(crate) fn connector_failure_cause(
    error: novarocks_spi::connector::ConnectorError,
) -> novarocks_failure_contract::SharedFailureCause {
    use novarocks_failure_contract::SharedFailureCause;
    if let Some(cause) = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<SharedFailureCause>())
    {
        return cause.clone();
    }
    SharedFailureCause::from_static_detail(error, "MV guard provider operation failed")
}

pub(crate) fn connector_application_error(
    error: novarocks_spi::connector::ConnectorError,
) -> novarocks_query_application::session_error::QueryServiceError {
    novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
        connector_failure_cause(error),
    )
}

pub(crate) fn document_application_error(
    error: novarocks_mv_application::persistence::documents::MvDocumentError,
) -> novarocks_query_application::session_error::QueryServiceError {
    use novarocks_mv_application::persistence::documents::MvDocumentError;
    match error {
        MvDocumentError::Connector(error)
        | MvDocumentError::KeyedStorage(
            novarocks_spi::connector::KeyedDocumentReadError::Invalid(error),
        ) => connector_application_error(error),
        MvDocumentError::KeyedStorage(
            legacy @ novarocks_spi::connector::KeyedDocumentReadError::LegacyFormat { .. },
        ) => novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
            novarocks_failure_contract::SharedFailureCause::new(legacy),
        ),
        error => application_error(error.to_string()),
    }
}

fn projection_application_error(
    error: MvProjectionError,
) -> novarocks_query_application::session_error::QueryServiceError {
    match error.original_failure() {
        Some(cause) => {
            novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
                cause.clone(),
            )
        }
        None => application_error(error.to_string()),
    }
}

pub(crate) fn application_error(
    message: impl Into<String>,
) -> novarocks_query_application::session_error::QueryServiceError {
    novarocks_query_application::session_error::QueryServiceError::new(
        novarocks_query_application::session_error::QueryServiceErrorKind::Internal,
        message,
    )
}

#[cfg(all(test, unix, novarocks_dev_controls))]
mod controlled_wipe_tests {
    use super::*;
    use crate::mv::test_controls::tests::{TestSession, wipe_grant};
    use novarocks_failpoint::session::SessionRole;
    use novarocks_mv_application::{
        activity::{MvActivityGateError, MvActivityOwner},
        process_runtime::MvBackgroundRuntime,
        product::MvTarget,
        service::MvKeyedProductService,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    async fn product_and_port() -> (
        MvKeyedProductService,
        MvReadinessPort,
        Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>,
    ) {
        let (product, repository) = crate::mv::domain::test_admission::keyed_product().await;
        let service = product.keyed_readiness_service().unwrap();
        service
            .seed_projection(
                Uuid::now_v7(),
                crate::mv::domain::test_admission::sample_keyed_projection(
                    MvTarget::from_parts(Some("ice"), "sales", "orders"),
                    Some(7),
                ),
            )
            .await
            .unwrap();
        let port = MvReadinessPort::from_product(
            service,
            tokio::runtime::Handle::current(),
            product.management_entrance().unwrap(),
        );
        (product, port, repository)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_actual_exit_precedes_wipe_and_old_admission_stays_stopped() {
        let session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let (product, port, repository) = product_and_port().await;
        let stop = novarocks_mv_application::process_runtime::MvBackgroundStop::default();
        let refresh_stop = stop.clone();
        let maintenance_stop = stop.clone();
        let (wakeup_tx, _wakeup_rx) = tokio::sync::mpsc::channel(1);
        let refresh_exited = Arc::new(AtomicBool::new(false));
        let maintenance_exited = Arc::new(AtomicBool::new(false));
        let refresh_fact = refresh_exited.clone();
        let maintenance_fact = maintenance_exited.clone();
        let runtime = MvBackgroundRuntime::new(
            stop,
            tokio::spawn(async move {
                refresh_stop.cancelled().await;
                refresh_fact.store(true, Ordering::Release);
            }),
            wakeup_tx,
            tokio::spawn(async move {
                maintenance_stop.cancelled().await;
                maintenance_fact.store(true, Ordering::Release);
            }),
        );
        product
            .begin_background_start()
            .unwrap()
            .install(runtime)
            .unwrap();
        port.wipe_accelerator_for_test(&product, Uuid::now_v7(), &mut claim)
            .unwrap();
        assert!(refresh_exited.load(Ordering::Acquire));
        assert!(maintenance_exited.load(Ordering::Acquire));
        assert!(
            repository
                .list_keyed_projections()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            product.request_activity(
                canonical_mv_target(&novarocks_sql::planning::mv::SqlMvTarget {
                    catalog: Some("ice".into()),
                    database: "sales".into(),
                    name: "orders".into()
                }),
                MvActivityOwner::ScheduledRefresh
            ),
            Err(MvActivityGateError::Stopping)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unjoined_background_worker_preserves_accelerator_and_joins_on_cleanup() {
        let session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 400);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let (product, port, repository) = product_and_port().await;
        let before = repository.list_keyed_projections().await.unwrap();
        let (wakeup_tx, _wakeup_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let runtime = MvBackgroundRuntime::new(
            novarocks_mv_application::process_runtime::MvBackgroundStop::default(),
            tokio::spawn(async move {
                release_rx.await.unwrap();
            }),
            wakeup_tx,
            tokio::spawn(async {}),
        );
        product
            .begin_background_start()
            .unwrap()
            .install(runtime)
            .unwrap();
        let result = port.wipe_accelerator_for_test(&product, Uuid::now_v7(), &mut claim);
        release_tx.send(()).unwrap();
        product
            .shutdown_background_workers_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(result.unwrap_err().contains("shared shutdown deadline"));
        assert_eq!(repository.list_keyed_projections().await.unwrap(), before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_join_failure_preserves_accelerator() {
        let session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let (product, port, repository) = product_and_port().await;
        let before = repository.list_keyed_projections().await.unwrap();
        let (wakeup_tx, _wakeup_rx) = tokio::sync::mpsc::channel(1);
        let runtime = MvBackgroundRuntime::new(
            novarocks_mv_application::process_runtime::MvBackgroundStop::default(),
            tokio::spawn(async {
                panic!("injected MV worker panic");
            }),
            wakeup_tx,
            tokio::spawn(async {}),
        );
        product
            .begin_background_start()
            .unwrap()
            .install(runtime)
            .unwrap();
        let result = port.wipe_accelerator_for_test(&product, Uuid::now_v7(), &mut claim);
        let repeated = product
            .shutdown_background_workers_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("an observed original panic remains a shutdown failure");
        assert!(repeated.contains("panicked"));
        assert!(result.unwrap_err().contains("panicked"));
        assert_eq!(repository.list_keyed_projections().await.unwrap(), before);
    }
}

#[cfg(test)]
mod guard_original_projection_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, KeyedDocumentReadError};

    #[test]
    fn keyed_invalid_and_mva_projection_keep_the_same_original_cause() {
        let connector = ConnectorError::from_domain_static(
            ConnectorErrorKind::Interrupted,
            novarocks_type_contract::CompileControlError::Cancelled,
            "actual document read refused",
        );
        let cause = connector_failure_cause(connector.clone());
        let keyed = document_application_error(
            novarocks_mv_application::persistence::documents::MvDocumentError::KeyedStorage(
                KeyedDocumentReadError::Invalid(connector),
            ),
        );
        assert_eq!(keyed.failure_cause(), Some(&cause));
        let projection = MvProjectionError::new(
            novarocks_mv_application::readiness::MvProjectionErrorKind::Cancelled,
            "projection refused",
        )
        .with_original_failure(cause.clone());
        let service = projection_application_error(projection);
        assert_eq!(service.failure_cause(), Some(&cause));
        assert_eq!(
            service.execution_failure().unwrap().fact(),
            cause.failure_fact()
        );
        let plain = projection_application_error(MvProjectionError::new(
            novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,
            "MV projection admission already consumed",
        ));
        assert!(plain.failure_cause().is_none());
        assert!(plain.execution_failure().is_none());
        assert_eq!(plain.message(), "MV projection admission already consumed");
    }

    #[test]
    fn legacy_document_and_projection_bridges_keep_generic_unsupported_and_codec_source() {
        use novarocks_failure_contract::{FailureClass, SharedFailureCause, domain};
        let legacy = KeyedDocumentReadError::LegacyFormat {
            key: "original-legacy-key".into(),
        };
        let document = document_application_error(
            novarocks_mv_application::persistence::documents::MvDocumentError::KeyedStorage(
                legacy.clone(),
            ),
        );
        let projection = MvProjectionError::from(
            novarocks_mv_application::persistence::documents::MvDocumentError::KeyedStorage(
                legacy.clone(),
            ),
        );
        let original_projection_cause = projection.original_failure().unwrap().clone();
        let projected = projection_application_error(projection);
        assert_eq!(projected.failure_cause(), Some(&original_projection_cause));
        for service in [document, projected] {
            let cause = service.failure_cause().unwrap();
            assert_eq!(
                cause.downcast_ref::<KeyedDocumentReadError>(),
                Some(&legacy)
            );
            assert_eq!(cause.failure_fact().class(), FailureClass::Unsupported);
            assert_eq!(
                cause.failure_fact().reason().domain(),
                domain::METADATA.get()
            );
            assert_eq!(cause.failure_fact().reason().code(), 916);
            assert_eq!(
                std::error::Error::source(&service)
                    .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
                Some(cause)
            );
            assert_eq!(
                service.execution_failure().unwrap().fact(),
                cause.failure_fact()
            );
            assert_eq!(
                service
                    .execution_failure()
                    .unwrap()
                    .descriptor()
                    .descriptor
                    .code
                    .as_str(),
                "query.execution.unsupported"
            );
            assert!(service.compile_control_error().is_none());
            assert!(service.compile_control_provenance().is_none());
            assert!(!service.message().contains("[UnsupportedCompiledShape]"));
        }
    }
}

#[cfg(test)]
mod original_request_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_state_store_api::{StateStoreExecutionContext, StateStoreExecutionPort};
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    struct NoPhysicalPort;
    impl StateStoreExecutionPort for NoPhysicalPort {
        fn bind_original_in(
            &self,
            _: &Arc<dyn novarocks_state_store_api::StateStoreBlockingOriginal>,
            _: Instant,
            _: &mut Option<Box<dyn novarocks_state_store_api::StateStoreBlockingSlot>>,
        ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
            panic!("pure readiness fixture must not dispatch physical work")
        }
        fn request_progress(&self) {}
    }
    fn context() -> (Arc<dyn StateStoreExecutionPort>, StateStoreExecutionContext) {
        let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
        let context =
            StateStoreExecutionContext::new(&port, Instant::now() + Duration::from_secs(5));
        (port, context)
    }
    async fn readiness(store: Arc<dyn novarocks_state_store_api::StateStore>) -> MvReadinessPort {
        let repository =
            novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
                store,
                Default::default(),
            )
            .await
            .unwrap();
        MvReadinessPort::from_product(
            MvKeyedReadinessService::new(repository, Arc::default()),
            tokio::runtime::Handle::current(),
            crate::mv::domain::test_admission::management_entrance(),
        )
    }
    #[tokio::test(flavor = "current_thread")]
    async fn original_candidate_refusal_preserves_repository_fact_without_legacy_fallback() {
        let (owner, context) = context();
        let port = readiness(Arc::new(
            novarocks_state_store_testkit::testing::InMemoryStateStore::new(
                "readiness-no-original",
            ),
        ))
        .await;
        let legacy = port.candidate_reader();
        assert!(
            legacy
                .list_candidate_definitions_async()
                .await
                .unwrap()
                .is_empty()
        );
        let original = legacy.in_execution(&context);
        assert_eq!(
            Arc::strong_count(&owner),
            1,
            "request carries only the weak context"
        );
        let error = original
            .list_candidate_definitions_async()
            .await
            .unwrap_err();
        assert_eq!(
            error.failure_fact(),
            novarocks_state_store_api::StateStoreOriginalUnsupported.failure_fact()
        );
        assert!(
            error.original_failure().is_some(),
            "keep the whole repository cause"
        );
        // Mint the need through the public SQL completion author, never by
        // constructing its private occurrence identity or an execution Root.
        use novarocks_sql::compiler::{
            DEFAULT_COMPLETION_LIMITS, MaterializedViewOutcome, SessionOptimizerSettings,
            SqlCompileControl, SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFactBatch,
            SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment, SqlSessionContext,
            SqlStatementInput, builtin_sql_function_catalog,
        };
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            novarocks_physical_plan::PlanVersionId::try_new([17; 16]).unwrap(),
            SqlStatementInput::sql("select order_key from orders"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("ice".into()),
                current_database: "db".into(),
                optimizer_settings: SessionOptimizerSettings {
                    enable_materialized_view_rewrite: Some(true),
                    ..SessionOptimizerSettings::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            crate::query_execution::constant_eval::constant_evaluator(),
            crate::application::test_constant_policy(),
            control.clone(),
            novarocks_physical_plan::PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            novarocks_physical_plan::ScanReadBudget {
                max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        );
        let SqlCompileProgress::Incomplete(compilation) =
            SqlCompiler::start(request.try_into_completion().unwrap(), &control).unwrap()
        else {
            panic!("MV-enabled external query must ask its original optional need");
        };
        let need = match compilation.needs() {
            SqlNeedBatch::MaterializedViews(needs) if needs.len() == 1 => needs[0].clone(),
            other => panic!("expected the original MV need, got {other:?}"),
        };
        let connector_control =
            crate::query_execution::compiler::TestConnectorControlRegistry::default();
        let storage = novarocks_spi::connector::UnavailableMvStorageObservationPort;
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let connector_request =
            crate::connector::connector_request_context(None, stop.view()).unwrap();
        let fact = crate::mv::domain::rewrite_prep::freeze_materialized_view_fact_with_ports_async(
            &need,
            &original,
            &connector_control,
            &storage,
            &connector_request,
        )
        .await;
        assert_eq!(fact.id(), need.id());
        match fact.outcome() {
            MaterializedViewOutcome::Missing { reason } => assert_eq!(
                reason.as_ref(),
                "materialized-view rewrite inventory is unavailable",
            ),
            other => panic!("actual inventory refusal must become Missing, got {other:?}"),
        }
        let SqlCompileProgress::Incomplete(base) = SqlCompiler::finish(
            compilation,
            SqlFactBatch::MaterializedViews(vec![fact].into_boxed_slice()),
            &control,
        )
        .unwrap() else {
            panic!("missing optional inventory must continue the base query");
        };
        match base.needs() {
            SqlNeedBatch::CatalogRelations(needs) if needs.len() == 1 => {
                assert_eq!(needs[0].relation().fqn(), "ice.db.orders");
            }
            other => panic!("expected the original base relation need, got {other:?}"),
        }
        assert_eq!(
            Arc::strong_count(&owner),
            1,
            "optional facts must not invent an execution owner"
        );
        assert!(
            original.list_candidate_definitions().is_err(),
            "sync original must refuse before block_in_place"
        );
        let cloned = original.clone();
        assert_eq!(
            cloned
                .list_candidate_definitions_async()
                .await
                .unwrap_err()
                .failure_fact(),
            error.failure_fact()
        );
        // Selecting a request must not mutate the role-local legacy facade.
        assert!(
            legacy
                .list_candidate_definitions_async()
                .await
                .unwrap()
                .is_empty()
        );
        let execution = port.in_execution(&context);
        assert_eq!(
            execution
                .candidate_reader()
                .list_candidate_definitions_async()
                .await
                .unwrap_err()
                .failure_fact(),
            error.failure_fact()
        );
        assert!(execution.local_projection_inventory().await.is_err());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn pure_original_request_is_async_and_expired_request_keeps_its_actual_failure() {
        let mut host = crate::state_store::testing::open_persistent(format!(
            "mv-original-readiness-{}",
            Uuid::now_v7()
        ))
        .await;
        let (owner, context) = context();
        let port = readiness(host.state_store().unwrap()).await;
        let execution = port.in_execution(&context);
        assert!(
            execution
                .candidate_reader()
                .list_candidate_definitions_async()
                .await
                .unwrap()
                .is_empty()
        );
        let target = MvTarget {
            catalog: Some("ice".into()),
            database: "db".into(),
            name: "absent".into(),
        };
        assert!(execution.load_ready(&target).await.unwrap().is_none());
        assert!(matches!(
            execution.query_admission(&target).await.unwrap(),
            novarocks_mv_application::readiness::MvQueryAdmission::NotAnMv
        ));
        let mut inventory = execution.local_projection_inventory().await.unwrap();
        assert!(inventory.next_ready().await.unwrap().is_none());
        assert!(inventory.next_listable().await.unwrap().is_none());
        let expired = context.with_deadline(Instant::now() - Duration::from_secs(1));
        let error = port
            .candidate_reader()
            .in_execution(&expired)
            .list_candidate_definitions_async()
            .await
            .unwrap_err();
        let cause = error
            .original_failure()
            .expect("original provider error survives facade");
        assert_eq!(
            cause
                .downcast_ref::<novarocks_state_store_api::StateStoreError>()
                .unwrap()
                .kind(),
            novarocks_state_store_api::StateStoreErrorKind::DeadlineExceeded
        );
        assert_eq!(error.failure_fact(), cause.failure_fact());
        assert_eq!(Arc::strong_count(&owner), 1);
        drop(inventory);
        drop(execution);
        drop(port);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
