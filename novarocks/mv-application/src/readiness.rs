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

//! Product-owned keyed Current observation, ordered installation and inventory.
use crate::dependency::MvDependencyObjectIdentity;
use crate::management::MvCurrentManagementAdmission;
use crate::persistence::documents::MvDocumentError;
use crate::persistence::validation::PersistenceDecodeBudget;
use crate::process_runtime::{ProcessRuntime, ProjectionOrder, TargetReadiness};
use crate::product::MvTarget;
use crate::repository::{
    MvProjectionInventoryBound, MvProjectionInventoryEntry, MvRepositoryError,
    MvRepositoryErrorKind,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorRequestContext, LakePublicationId};
use std::sync::Arc;
use uuid::Uuid;
pub struct MvRuntimePublicationLease {
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
    target: MvTarget,
    publication_id: LakePublicationId,
}
impl Drop for MvRuntimePublicationLease {
    fn drop(&mut self) {
        self.runtime.finish(&self.target, self.publication_id);
    }
}

#[derive(Clone)]
pub struct MvCurrentProjectionRequest {
    catalog: CatalogHandle,
    target: MvTarget,
    context: ConnectorRequestContext,
    decode_budget: PersistenceDecodeBudget,
}
impl MvCurrentProjectionRequest {
    pub fn try_new(
        catalog: CatalogHandle,
        target: MvTarget,
        context: ConnectorRequestContext,
        decode_budget: PersistenceDecodeBudget,
    ) -> Result<Self, MvProjectionError> {
        if target.catalog() != Some(catalog.catalog_name().as_str()) {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "MV projection target and catalog binding disagree",
            ));
        }
        Ok(Self {
            catalog,
            target,
            context,
            decode_budget,
        })
    }
    pub fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
    pub fn decode_budget(&self) -> PersistenceDecodeBudget {
        self.decode_budget
    }
}

/// Whether plain SQL may read one MV target's storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvQueryAdmission {
    /// Nothing in this process claims this table as an MV.
    NotAnMv,
    /// The projection is sound, whatever its management state.
    Admitted,
    /// The projection itself is in doubt, so its storage must not be read as
    /// though it were a published materialization.
    Quarantined(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvProjectionErrorKind {
    SourceConflict,
    Unsupported,
    CorruptDocument,
    Unavailable,
    BudgetExceeded,
    Cancelled,
    DeadlineExceeded,
    Repository,
}

#[derive(Debug)]
pub struct MvProjectionError {
    kind: MvProjectionErrorKind,
    message: String,
    original_failure: Option<novarocks_failure_contract::SharedFailureCause>,
}
impl MvProjectionError {
    pub fn new(kind: MvProjectionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            original_failure: None,
        }
    }
    pub fn with_original_failure(
        mut self,
        cause: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        self.original_failure = Some(cause);
        self
    }
    pub fn original_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.original_failure.as_ref()
    }
    pub fn kind(&self) -> MvProjectionErrorKind {
        self.kind
    }
}
impl std::fmt::Display for MvProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for MvProjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original_failure.as_ref().map(|cause| cause as _)
    }
}
impl From<MvRepositoryError> for MvProjectionError {
    fn from(error: MvRepositoryError) -> Self {
        Self::new(MvProjectionErrorKind::Repository, error.to_string())
            .with_original_failure(novarocks_failure_contract::SharedFailureCause::new(error))
    }
}
impl From<MvDocumentError> for MvProjectionError {
    fn from(error: MvDocumentError) -> Self {
        use novarocks_failure_contract::SharedFailureCause;
        use novarocks_spi::connector::{ConnectorErrorKind as K, KeyedDocumentReadError};
        let message = error.to_string();
        match error {
            MvDocumentError::KeyedStorage(legacy @ KeyedDocumentReadError::LegacyFormat { .. }) => {
                Self::new(MvProjectionErrorKind::Unsupported, message)
                    .with_original_failure(SharedFailureCause::new(legacy))
            }
            MvDocumentError::KeyedStorage(KeyedDocumentReadError::Invalid(error)) => {
                Self::from(MvDocumentError::Connector(error))
            }
            MvDocumentError::Connector(error) => {
                let kind = match error.kind() {
                    K::Unsupported => MvProjectionErrorKind::Unsupported,
                    K::Cancelled => MvProjectionErrorKind::Cancelled,
                    K::DeadlineExceeded => MvProjectionErrorKind::DeadlineExceeded,
                    K::ResourceExhausted => MvProjectionErrorKind::BudgetExceeded,
                    K::NotFound | K::InvalidRequest => MvProjectionErrorKind::SourceConflict,
                    K::CorruptData => MvProjectionErrorKind::CorruptDocument,
                    _ => MvProjectionErrorKind::Unavailable,
                };
                let original = std::error::Error::source(&error)
                    .and_then(|source| source.downcast_ref::<SharedFailureCause>())
                    .cloned()
                    .unwrap_or_else(|| SharedFailureCause::new(error));
                Self::new(kind, message).with_original_failure(original)
            }
            error => {
                let kind = match &error {
                    MvDocumentError::Codec(
                        crate::persistence::codec::PersistenceCodecError::ResourceBudget { .. },
                    ) => MvProjectionErrorKind::BudgetExceeded,
                    _ => MvProjectionErrorKind::CorruptDocument,
                };
                Self::new(kind, message)
            }
        }
    }
}

/// Whether this process may manage a projection it can show.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvListedManageability {
    Manageable,
    /// Queryable, but not this process's to write, and why.
    ReadOnly(String),
    /// This process does not trust its own projection of the target, and why.
    /// The view is still listed: an operator has to see the one that needs
    /// attention, not lose it from the inventory.
    Unavailable(String),
}

pub enum MvDropReadiness<G = MvKeyedProjectionDeleteGuard> {
    ReadyToDrop(G),
    AlreadyAbsent,
}
fn check_context(context: &ConnectorRequestContext) -> Result<(), MvProjectionError> {
    if context.is_cancelled() {
        return Err(MvProjectionError::new(
            MvProjectionErrorKind::Cancelled,
            "MV Current observation cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(MvProjectionError::new(
            MvProjectionErrorKind::DeadlineExceeded,
            "MV Current observation deadline elapsed",
        ));
    }
    Ok(())
}

const OBSERVATION_SETTLE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
/// The sole readiness service for the Accelerator repository and process runtime.
#[derive(Clone)]
pub struct MvKeyedReadinessService {
    repository: Arc<crate::state_store_repository::StateStoreMvRepository>,
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
}
/// Request-local selection of the repository's existing execution path.
/// Context is a weak borrower; the Action/Residual retains the physical owner.
#[derive(Clone)]
pub struct MvKeyedReadinessExecution {
    repository: ReadinessRepository,
    runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
}
#[derive(Clone)]
enum ReadinessExecution {
    Legacy,
    Original(Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>),
}
#[derive(Clone)]
struct ReadinessRepository {
    repository: Arc<crate::state_store_repository::StateStoreMvRepository>,
    execution: ReadinessExecution,
}
impl MvKeyedReadinessService {
    pub fn new(
        repository: Arc<crate::state_store_repository::StateStoreMvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, LakePublicationId>>,
    ) -> Self {
        Self {
            repository,
            runtime,
        }
    }
    /// Borrow this request's exact owner/deadline without changing the service.
    pub fn in_execution(
        &self,
        execution: &novarocks_state_store_api::StateStoreExecutionContext,
    ) -> MvKeyedReadinessExecution {
        self.in_requests(Arc::new(execution.clone()))
    }
    /// Retain only the request factory's weak execution view across Provider
    /// observations. Each independent repository operation obtains its context.
    pub fn in_requests(
        &self,
        requests: Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
    ) -> MvKeyedReadinessExecution {
        self.request(ReadinessExecution::Original(requests))
    }
    fn legacy(&self) -> MvKeyedReadinessExecution {
        self.request(ReadinessExecution::Legacy)
    }
    fn request(&self, execution: ReadinessExecution) -> MvKeyedReadinessExecution {
        MvKeyedReadinessExecution {
            repository: ReadinessRepository {
                repository: Arc::clone(&self.repository),
                execution,
            },
            runtime: Arc::clone(&self.runtime),
        }
    }
    pub async fn observe_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.legacy()
            .observe_and_install(operation_id, request, source)
            .await
    }
    pub async fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        self.legacy().load_ready(target).await
    }
    pub fn candidate_reader(&self) -> MvKeyedCandidateReader {
        self.legacy().candidate_reader()
    }
    pub async fn invalidate_current(
        &self,
        target: MvTarget,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        self.legacy().invalidate_current(target, reason).await
    }
    pub fn begin_publication(
        &self,
        target: MvTarget,
        publication_id: LakePublicationId,
    ) -> Result<MvRuntimePublicationLease, MvRepositoryError> {
        self.legacy().begin_publication(target, publication_id)
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn ensure_no_active_publications(&self) -> Result<(), MvRepositoryError> {
        self.legacy().ensure_no_active_publications()
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvProjectionError> {
        self.legacy().wipe_accelerator(operation_id).await
    }
    pub async fn query_admission(
        &self,
        target: &MvTarget,
    ) -> Result<MvQueryAdmission, MvRepositoryError> {
        self.legacy().query_admission(target).await
    }
    pub async fn load_ready_settled(
        &self,
        target: &MvTarget,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        self.legacy().load_ready_settled(target).await
    }
    pub async fn bounded_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<MvBoundedKeyedProjectionInventory, MvRepositoryError> {
        self.legacy().bounded_projection_inventory(bound).await
    }
    pub async fn list_ready_projections(
        &self,
    ) -> Result<Vec<crate::state_store_repository::keyed::LoadedKeyedMvProjection>, MvRepositoryError>
    {
        self.legacy().list_ready_projections().await
    }
    pub async fn list_ready_dependencies_by_downstream_bounded(
        &self,
        loaded: &crate::state_store_repository::keyed::LoadedKeyedMvProjection,
        bound: crate::repository::MvDependencyReadBound,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        self.legacy()
            .list_ready_dependencies_by_downstream_bounded(loaded, bound)
            .await
    }
    pub async fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectIdentity,
    ) -> Result<(), MvRepositoryError> {
        self.legacy()
            .ensure_no_ready_downstream_dependencies(upstream)
            .await
    }
    pub async fn reserve_projection_delete(
        &self,
        target: MvTarget,
    ) -> Result<MvKeyedProjectionDeleteGuard, MvProjectionError> {
        self.legacy().reserve_projection_delete(target).await
    }
    pub async fn delete_after_provider_drop(
        &self,
        operation: Uuid,
        guard: MvKeyedProjectionDeleteGuard,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.legacy()
            .delete_after_provider_drop(operation, guard)
            .await
    }
    pub async fn prepare_drop(
        &self,
        target: &MvTarget,
        if_exists: bool,
    ) -> Result<MvKeyedDropReadiness, MvProjectionError> {
        self.legacy().prepare_drop(target, if_exists).await
    }
    pub async fn observe_current_read_only_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedReadOnlyCurrentProjectionSource,
        reason: String,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.legacy()
            .observe_current_read_only_and_install(operation_id, request, source, reason)
            .await
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub async fn seed_projection(
        &self,
        operation: Uuid,
        facts: crate::persistence::keyed_documents::MvKeyedDocumentProjection,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        self.legacy().seed_projection(operation, facts).await
    }
    pub async fn list_ready_dependencies_by_downstream(
        &self,
        loaded: &crate::state_store_repository::keyed::LoadedKeyedMvProjection,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        self.legacy()
            .list_ready_dependencies_by_downstream(loaded)
            .await
    }
}
#[async_trait::async_trait]
pub trait MvKeyedCurrentProjectionSource: Send + Sync {
    async fn observe_current_keyed(
        &self,
        request: MvCurrentProjectionRequest,
    ) -> Result<
        (
            crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
            MvCurrentManagementAdmission,
        ),
        MvProjectionError,
    >;
}
#[derive(Debug)]
pub enum MvKeyedProjectionInstallOutcome {
    AlreadyProjectedElsewhere(MvTarget),
    AlreadyAbsent,
    Removed,
    Installed(crate::state_store_repository::keyed::LoadedKeyedMvProjection),
    Unchanged(crate::state_store_repository::keyed::LoadedKeyedMvProjection),
    Superseded,
}
impl MvKeyedReadinessExecution {
    /// Reserve order before invoking the provider. A slower earlier read can
    /// never overwrite a later observation or an explicit invalidation.
    pub async fn observe_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedCurrentProjectionSource,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        let target = request.target().clone();
        let budget = request.decode_budget();
        let order = self.runtime.projection_order(target.clone());
        let (generation, expected) = {
            let mut cell = order.lock().await;
            let generation = cell.advance()?;
            cell.installed = None;
            cell.pending = Some(generation);
            self.runtime.set_unavailable(
                target.clone(),
                "fresh keyed Current observation pending".into(),
            );
            let expected = self
                .repository
                .find_keyed_by_target(&target, budget)
                .await?;
            (generation, expected)
        };
        let observed = source.observe_current_keyed(request.clone()).await;
        let mut cell = order.lock().await;
        if cell.generation != generation {
            return Ok(MvKeyedProjectionInstallOutcome::Superseded);
        }
        cell.settle(generation);
        let current = self
            .repository
            .find_keyed_by_target(&target, budget)
            .await?;
        if current.as_ref().map(|v| &v.version) != expected.as_ref().map(|v| &v.version) {
            return Ok(MvKeyedProjectionInstallOutcome::Superseded);
        }
        let (observed, admission) = match observed {
            Ok(value) => value,
            Err(error) => {
                self.runtime.set_unavailable(target, error.to_string());
                return Err(error);
            }
        };
        check_context(request.context())?;
        if !admission.matches_keyed(&observed)
            || observed.view.table.instance_id.as_str() != target.catalog().unwrap_or("")
            || observed.view.table.namespace.as_ref() != target.namespace()
            || observed.view.table.table.as_ref() != target.name()
        {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "keyed projection lacks admission for this exact observation",
            ));
        }
        let facts =
            crate::persistence::keyed_documents::MvKeyedDocumentProjection::try_from_current(
                observed,
            )
            .map_err(|e| MvProjectionError::new(MvProjectionErrorKind::CorruptDocument, e))?;
        let unchanged = expected
            .as_ref()
            .is_some_and(|old| old.projection.facts.source_revision() == facts.source_revision());
        if expected.is_none()
            && let Some(existing) = self
                .repository
                .find_keyed_by_target_object(&facts.source_revision().target_object_id)
                .await?
        {
            let other = existing.projection.facts.target();
            self.runtime.set_unavailable(
                target,
                "target object is already projected through another catalog".into(),
            );
            return Ok(MvKeyedProjectionInstallOutcome::AlreadyProjectedElsewhere(
                other,
            ));
        }
        let loaded = if unchanged {
            expected.expect("unchanged requires an expected root")
        } else {
            self.repository
                .install_keyed_projection(operation_id, facts, expected)
                .await?
        };
        check_context(request.context())?;
        if !admission.is_open() {
            self.runtime.set_unavailable(
                target,
                "management admission closed during keyed installation".into(),
            );
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "management admission closed during keyed installation",
            ));
        }
        cell.installed = Some(loaded.version.clone());
        self.runtime.set_ready(target);
        Ok(if unchanged {
            MvKeyedProjectionInstallOutcome::Unchanged(loaded)
        } else {
            MvKeyedProjectionInstallOutcome::Installed(loaded)
        })
    }
    pub async fn load_ready(
        &self,
        target: &MvTarget,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        let order = self.runtime.projection_order(target.clone());
        let cell = order.lock().await;
        let Some(loaded) = self
            .repository
            .find_keyed_by_target(target, PersistenceDecodeBudget::default())
            .await?
        else {
            return Ok(None);
        };
        if !matches!(self.runtime.readiness(target), TargetReadiness::Ready)
            || cell.installed.as_ref() != Some(&loaded.version)
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "keyed MV target requires a successful fresh Current observation",
            ));
        }
        Ok(Some(loaded))
    }
}

#[derive(Clone)]
pub struct MvKeyedCandidateReader {
    repository: ReadinessRepository,
}
impl MvKeyedCandidateReader {
    pub async fn list_candidate_definitions(
        &self,
    ) -> Result<Vec<crate::persistence::keyed_documents::StoredKeyedMvProjection>, MvRepositoryError>
    {
        Ok(self
            .repository
            .list_keyed_projections()
            .await?
            .into_iter()
            .map(|v| v.projection)
            .collect())
    }
}
pub struct MvBoundedKeyedProjectionInventory {
    service: MvKeyedReadinessExecution,
    entries: std::vec::IntoIter<MvProjectionInventoryEntry>,
    bound: MvProjectionInventoryBound,
}
#[derive(Debug)]
pub struct ListedKeyedMvProjection {
    pub loaded: crate::state_store_repository::keyed::LoadedKeyedMvProjection,
    pub manageability: MvListedManageability,
}
impl MvBoundedKeyedProjectionInventory {
    pub fn order_by_namespace_and_name(&mut self) {
        self.entries.as_mut_slice().sort_by(|a, b| {
            a.target
                .namespace()
                .cmp(b.target.namespace())
                .then(a.target.name().cmp(b.target.name()))
        });
    }
    pub async fn next_ready(
        &mut self,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        for entry in self.entries.by_ref() {
            let order = self.service.runtime.projection_order(entry.target.clone());
            let cell = order.lock().await;
            let Some(loaded) = self
                .service
                .repository
                .find_keyed_by_target_bounded(&entry.target, self.bound)
                .await?
            else {
                continue;
            };
            if matches!(
                self.service.runtime.readiness(&entry.target),
                TargetReadiness::Ready
            ) && cell.installed.as_ref() == Some(&loaded.version)
            {
                return Ok(Some(loaded));
            }
        }
        Ok(None)
    }
    pub async fn next_listable(
        &mut self,
    ) -> Result<Option<ListedKeyedMvProjection>, MvRepositoryError> {
        for entry in self.entries.by_ref() {
            let mut bound = self.bound;
            let state = self
                .service
                .runtime
                .with_readiness(&entry.target, |state| {
                    let bytes = match state {
                        TargetReadiness::ReadOnly(r) | TargetReadiness::Unavailable(r) => r.len(),
                        _ => 0,
                    };
                    bound.decode.max_working_set_bytes = bound
                        .decode
                        .max_working_set_bytes
                        .checked_sub(bytes)
                        .filter(|n| *n > 0)
                        .ok_or_else(|| {
                            MvRepositoryError::new(
                                MvRepositoryErrorKind::InvalidRequest,
                                "keyed readiness reason exceeds decode budget",
                            )
                        })?;
                    Ok::<_, MvRepositoryError>(match state {
                        TargetReadiness::Ready => Some(MvListedManageability::Manageable),
                        TargetReadiness::ReadOnly(r) => {
                            Some(MvListedManageability::ReadOnly(r.clone()))
                        }
                        TargetReadiness::Unavailable(r) => {
                            Some(MvListedManageability::Unavailable(r.clone()))
                        }
                        TargetReadiness::Unobserved => None,
                    })
                })?;
            let Some(manageability) = state else {
                continue;
            };
            let order = self.service.runtime.projection_order(entry.target.clone());
            let cell = order.lock().await;
            let Some(loaded) = self
                .service
                .repository
                .find_keyed_by_target_bounded(&entry.target, bound)
                .await?
            else {
                continue;
            };
            if manageability == MvListedManageability::Manageable
                && cell.installed.as_ref() != Some(&loaded.version)
            {
                continue;
            }
            return Ok(Some(ListedKeyedMvProjection {
                loaded,
                manageability,
            }));
        }
        Ok(None)
    }
}
pub struct MvKeyedProjectionDeleteGuard {
    request: MvKeyedReadinessExecution,
    target: MvTarget,
    generation: u64,
    expected: Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
    order: Arc<tokio::sync::Mutex<ProjectionOrder>>,
}
impl MvKeyedProjectionDeleteGuard {
    pub fn has_projection(&self) -> bool {
        self.expected.is_some()
    }
    pub fn expected_target_object_id(
        &self,
    ) -> Option<&novarocks_spi::connector::ConnectorTableObjectId> {
        self.expected
            .as_ref()
            .map(|e| &e.projection.facts.source_revision().target_object_id)
    }
}
pub type MvKeyedDropReadiness = MvDropReadiness<MvKeyedProjectionDeleteGuard>;
#[async_trait::async_trait]
pub trait MvKeyedReadOnlyCurrentProjectionSource: Send + Sync {
    async fn observe_current_keyed_read_only(
        &self,
        request: MvCurrentProjectionRequest,
    ) -> Result<
        crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        MvProjectionError,
    >;
}
impl MvKeyedReadinessExecution {
    pub fn candidate_reader(&self) -> MvKeyedCandidateReader {
        MvKeyedCandidateReader {
            repository: self.repository.clone(),
        }
    }
    pub async fn invalidate_current(
        &self,
        target: MvTarget,
        reason: String,
    ) -> Result<(), MvProjectionError> {
        let order = self.runtime.projection_order(target.clone());
        let mut cell = order.lock().await;
        cell.advance()?;
        cell.installed = None;
        cell.supersede();
        self.runtime.set_unavailable(target, reason);
        Ok(())
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
    #[cfg(any(test, novarocks_dev_controls))]
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
    #[cfg(any(test, novarocks_dev_controls))]
    pub async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvProjectionError> {
        self.ensure_no_active_publications()?;
        for target in self.runtime.projection_targets() {
            self.invalidate_current(target, "MV Accelerator wipe".into())
                .await?;
        }
        self.repository.wipe_accelerator(operation_id).await?;
        Ok(())
    }
    pub async fn query_admission(
        &self,
        target: &MvTarget,
    ) -> Result<MvQueryAdmission, MvRepositoryError> {
        if self
            .repository
            .find_keyed_by_target(target, PersistenceDecodeBudget::default())
            .await?
            .is_none()
        {
            return Ok(MvQueryAdmission::NotAnMv);
        }
        Ok(match self.runtime.readiness(target) {
            TargetReadiness::Ready | TargetReadiness::ReadOnly(_) => MvQueryAdmission::Admitted,
            TargetReadiness::Unavailable(r) => MvQueryAdmission::Quarantined(r),
            TargetReadiness::Unobserved => MvQueryAdmission::Quarantined(
                "MV target has not been observed in this process".into(),
            ),
        })
    }
    pub async fn load_ready_settled(
        &self,
        target: &MvTarget,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        let order = self.runtime.projection_order(target.clone());
        let deadline = tokio::time::Instant::now() + OBSERVATION_SETTLE_WAIT;
        loop {
            let cell = order.lock().await;
            if cell.pending.is_none() {
                break;
            }
            let settled = Arc::clone(&cell.settled);
            let notified = settled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            drop(cell);
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }
        self.load_ready(target).await
    }
    pub async fn bounded_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<MvBoundedKeyedProjectionInventory, MvRepositoryError> {
        Ok(MvBoundedKeyedProjectionInventory {
            service: self.clone(),
            entries: self
                .repository
                .list_keyed_projection_inventory(bound)
                .await?
                .into_iter(),
            bound,
        })
    }
    pub async fn list_ready_projections(
        &self,
    ) -> Result<Vec<crate::state_store_repository::keyed::LoadedKeyedMvProjection>, MvRepositoryError>
    {
        let mut values = Vec::new();
        for loaded in self.repository.list_keyed_projections().await? {
            match self.load_ready(&loaded.projection.facts.target()).await {
                Ok(Some(v)) => values.push(v),
                Ok(None) => {}
                Err(e) if e.kind() == MvRepositoryErrorKind::Unavailable => {}
                Err(e) => return Err(e),
            }
        }
        Ok(values)
    }
    pub async fn list_ready_dependencies_by_downstream_bounded(
        &self,
        loaded: &crate::state_store_repository::keyed::LoadedKeyedMvProjection,
        bound: crate::repository::MvDependencyReadBound,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        let target = loaded.projection.facts.target();
        let order = self.runtime.projection_order(target.clone());
        let cell = order.lock().await;
        if !matches!(self.runtime.readiness(&target), TargetReadiness::Ready)
            || cell.installed.as_ref() != Some(&loaded.version)
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "keyed dependency root is not ready",
            ));
        }
        self.repository
            .list_keyed_dependencies_by_downstream_bounded(loaded, bound)
            .await
    }
    pub async fn ensure_no_ready_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectIdentity,
    ) -> Result<(), MvRepositoryError> {
        for loaded in self.list_ready_projections().await? {
            for r in &loaded.projection.facts.definition().relation_occurrences {
                if r.catalog_at_binding == upstream.catalog_instance.as_str()
                    && crate::persistence::exact_revision::persisted_object_names(
                        &r.object_id,
                        &upstream.object_id,
                    )
                    .map_err(|e| {
                        MvRepositoryError::new(MvRepositoryErrorKind::Corruption, e.to_string())
                    })?
                {
                    return Err(MvRepositoryError::new(
                        MvRepositoryErrorKind::Conflict,
                        "exact object has downstream materialized views",
                    ));
                }
            }
        }
        Ok(())
    }
    pub async fn reserve_projection_delete(
        &self,
        target: MvTarget,
    ) -> Result<MvKeyedProjectionDeleteGuard, MvProjectionError> {
        let order = self.runtime.projection_order(target.clone());
        let mut cell = order.lock().await;
        let generation = cell.advance()?;
        cell.installed = None;
        cell.pending = Some(generation);
        self.runtime
            .set_unavailable(target.clone(), "MV deletion pending".into());
        let expected = match self
            .repository
            .find_keyed_by_target(&target, PersistenceDecodeBudget::default())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                cell.settle(generation);
                return Err(e.into());
            }
        };
        drop(cell);
        Ok(MvKeyedProjectionDeleteGuard {
            request: self.clone(),
            target,
            generation,
            expected,
            order,
        })
    }
    pub async fn delete_after_provider_drop(
        &self,
        operation: Uuid,
        guard: MvKeyedProjectionDeleteGuard,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        let mut cell = guard.order.lock().await;
        // The guard retains its original weak borrower, even if a caller uses
        // another service entry to finish the provider-side deletion.
        let request = &guard.request;
        if cell.generation != guard.generation {
            return Ok(MvKeyedProjectionInstallOutcome::Superseded);
        }
        cell.settle(guard.generation);
        let current = request
            .repository
            .find_keyed_by_target(&guard.target, PersistenceDecodeBudget::default())
            .await?;
        if current.as_ref().map(|v| &v.version) != guard.expected.as_ref().map(|v| &v.version) {
            return Ok(MvKeyedProjectionInstallOutcome::Superseded);
        }
        let Some(expected) = guard.expected else {
            return Ok(MvKeyedProjectionInstallOutcome::AlreadyAbsent);
        };
        let removed = request
            .repository
            .delete_keyed_projection(operation, expected)
            .await?;
        cell.installed = None;
        request
            .runtime
            .set_unavailable(guard.target, "MV target was removed".into());
        Ok(if removed {
            MvKeyedProjectionInstallOutcome::Removed
        } else {
            MvKeyedProjectionInstallOutcome::AlreadyAbsent
        })
    }
    pub async fn prepare_drop(
        &self,
        target: &MvTarget,
        if_exists: bool,
    ) -> Result<MvKeyedDropReadiness, MvProjectionError> {
        let Some(loaded) = self.load_ready(target).await? else {
            return if if_exists {
                Ok(MvKeyedDropReadiness::AlreadyAbsent)
            } else {
                Err(MvProjectionError::new(
                    MvProjectionErrorKind::SourceConflict,
                    "materialized view does not exist",
                ))
            };
        };
        self.ensure_no_ready_downstream_dependencies(&MvDependencyObjectIdentity::new(
            loaded
                .projection
                .facts
                .source_revision()
                .target
                .instance_id
                .as_str(),
            loaded
                .projection
                .facts
                .source_revision()
                .target_object_id
                .clone(),
        ))
        .await?;
        Ok(MvKeyedDropReadiness::ReadyToDrop(
            self.reserve_projection_delete(target.clone()).await?,
        ))
    }
    pub async fn observe_current_read_only_and_install(
        &self,
        operation_id: Uuid,
        request: MvCurrentProjectionRequest,
        source: &dyn MvKeyedReadOnlyCurrentProjectionSource,
        reason: String,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        let target = request.target().clone();
        let order = self.runtime.projection_order(target.clone());
        let (generation, expected, managed, installed) = {
            let mut cell = order.lock().await;
            let generation = cell.advance()?;
            let managed = matches!(self.runtime.readiness(&target), TargetReadiness::Ready);
            let installed = cell.installed.take();
            cell.pending = Some(generation);
            self.runtime.set_unavailable(
                target.clone(),
                "fresh keyed read-only observation pending".into(),
            );
            let expected = self
                .repository
                .find_keyed_by_target(&target, request.decode_budget())
                .await?;
            (generation, expected, managed, installed)
        };
        let observed = source
            .observe_current_keyed_read_only(request.clone())
            .await;
        let mut cell = order.lock().await;
        if cell.generation != generation {
            return Ok(MvKeyedProjectionInstallOutcome::Superseded);
        }
        cell.settle(generation);
        let current = self
            .repository
            .find_keyed_by_target(&target, request.decode_budget())
            .await?;
        if current.as_ref().map(|v| &v.version) != expected.as_ref().map(|v| &v.version) {
            return Ok(MvKeyedProjectionInstallOutcome::Superseded);
        }
        let observed = observed?;
        check_context(request.context())?;
        let facts =
            crate::persistence::keyed_documents::MvKeyedDocumentProjection::try_from_current(
                observed,
            )
            .map_err(|e| MvProjectionError::new(MvProjectionErrorKind::CorruptDocument, e))?;
        if facts.target() != target {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::SourceConflict,
                "keyed read-only observation target changed",
            ));
        }
        let unchanged = expected
            .as_ref()
            .is_some_and(|v| v.projection.facts.source_revision() == facts.source_revision());
        if expected.is_none()
            && let Some(existing) = self
                .repository
                .find_keyed_by_target_object(&facts.source_revision().target_object_id)
                .await?
        {
            self.runtime.set_unavailable(
                target,
                "target object is already projected through another catalog".into(),
            );
            return Ok(MvKeyedProjectionInstallOutcome::AlreadyProjectedElsewhere(
                existing.projection.facts.target(),
            ));
        }
        let loaded = if unchanged {
            expected.expect("unchanged requires a root")
        } else {
            self.repository
                .install_keyed_projection(operation_id, facts, expected)
                .await?
        };
        check_context(request.context())?;
        if managed && unchanged && installed.as_ref() == Some(&loaded.version) {
            cell.installed = Some(loaded.version.clone());
            self.runtime.set_ready(target);
        } else {
            cell.installed = None;
            self.runtime.set_read_only(target, reason);
        }
        Ok(if unchanged {
            MvKeyedProjectionInstallOutcome::Unchanged(loaded)
        } else {
            MvKeyedProjectionInstallOutcome::Installed(loaded)
        })
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub async fn seed_projection(
        &self,
        operation: Uuid,
        facts: crate::persistence::keyed_documents::MvKeyedDocumentProjection,
    ) -> Result<MvKeyedProjectionInstallOutcome, MvProjectionError> {
        let target = facts.target();
        let order = self.runtime.projection_order(target.clone());
        let mut cell = order.lock().await;
        cell.advance()?;
        cell.supersede();
        let expected = self
            .repository
            .find_keyed_by_target(&target, PersistenceDecodeBudget::default())
            .await?;
        let loaded = self
            .repository
            .install_keyed_projection(operation, facts, expected)
            .await?;
        cell.installed = Some(loaded.version.clone());
        self.runtime.set_ready(target);
        Ok(MvKeyedProjectionInstallOutcome::Installed(loaded))
    }
}

#[cfg(test)]
mod keyed_tests {
    use super::*;
    use crate::persistence::keyed_documents::*;
    use crate::repository::MvDependencyReadBound;
    struct Source(MvKeyedObservedCurrentDocuments);
    #[async_trait::async_trait]
    impl MvKeyedCurrentProjectionSource for Source {
        async fn observe_current_keyed(
            &self,
            _: MvCurrentProjectionRequest,
        ) -> Result<
            (
                MvKeyedObservedCurrentDocuments,
                MvCurrentManagementAdmission,
            ),
            MvProjectionError,
        > {
            Ok((
                self.0.clone(),
                MvCurrentManagementAdmission::for_test_keyed(&self.0),
            ))
        }
    }
    #[async_trait::async_trait]
    impl MvKeyedReadOnlyCurrentProjectionSource for Source {
        async fn observe_current_keyed_read_only(
            &self,
            _: MvCurrentProjectionRequest,
        ) -> Result<MvKeyedObservedCurrentDocuments, MvProjectionError> {
            Ok(self.0.clone())
        }
    }
    async fn service() -> MvKeyedReadinessService {
        let store = Arc::new(
            novarocks_state_store_testkit::testing::InMemoryStateStore::new(format!(
                "keyed-readiness-{}",
                Uuid::now_v7()
            )),
        );
        let repository = crate::state_store_repository::StateStoreMvRepository::open(
            store,
            novarocks_state_store_runtime::StateStoreRunPolicy::default(),
        )
        .await
        .unwrap();
        MvKeyedReadinessService::new(repository, Arc::new(ProcessRuntime::default()))
    }
    fn input(published: bool) -> (MvCurrentProjectionRequest, Source) {
        let (catalog, view) = keyed_view_fixture(Some(17), published);
        let decoded = decode_current_keyed_documents(
            view,
            catalog.clone(),
            PersistenceDecodeBudget::default(),
        )
        .unwrap();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            stop.view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        (
            MvCurrentProjectionRequest::try_new(
                catalog,
                MvTarget::from_parts(Some("ice"), "sales", "mv"),
                context,
                PersistenceDecodeBudget::default(),
            )
            .unwrap(),
            Source(decoded),
        )
    }
    #[tokio::test]
    async fn keyed_ready_inventory_candidate_read_only_and_delete_share_one_root() {
        let service = service().await;
        let (request, source) = input(true);
        let target = request.target().clone();
        assert!(matches!(
            service
                .observe_and_install(Uuid::now_v7(), request.clone(), &source)
                .await
                .unwrap(),
            MvKeyedProjectionInstallOutcome::Installed(_)
        ));
        let loaded = service.load_ready_settled(&target).await.unwrap().unwrap();
        assert_eq!(loaded.projection.facts.publication_snapshot(), Some(17));
        assert!(matches!(
            service.query_admission(&target).await.unwrap(),
            MvQueryAdmission::Admitted
        ));
        assert_eq!(
            service
                .candidate_reader()
                .list_candidate_definitions()
                .await
                .unwrap()
                .len(),
            1
        );
        let bound = MvProjectionInventoryBound {
            entries: 4,
            snapshot_bytes: 64 * 1024,
            raw_page_bytes: 1024 * 1024,
            single_name_bytes: 1024,
            continuation_token_bytes: 4096,
            decode: PersistenceDecodeBudget::default(),
        };
        let mut inventory = service.bounded_projection_inventory(bound).await.unwrap();
        assert!(inventory.next_ready().await.unwrap().is_some());
        assert!(inventory.next_ready().await.unwrap().is_none());
        let dependencies = service
            .list_ready_dependencies_by_downstream_bounded(
                &loaded,
                MvDependencyReadBound {
                    inventory: bound,
                    entries: 8,
                    collection_bytes: 64 * 1024,
                },
            )
            .await
            .unwrap();
        assert_eq!(dependencies.len(), 2);
        assert_eq!(
            service
                .list_ready_dependencies_by_downstream(&loaded)
                .await
                .unwrap(),
            dependencies
        );
        assert!(matches!(
            service
                .observe_current_read_only_and_install(
                    Uuid::now_v7(),
                    request,
                    &source,
                    "foreign".into()
                )
                .await
                .unwrap(),
            MvKeyedProjectionInstallOutcome::Unchanged(_)
        ));
        assert!(service.load_ready(&target).await.unwrap().is_some());
        let guard = service
            .reserve_projection_delete(target.clone())
            .await
            .unwrap();
        assert!(matches!(
            service
                .delete_after_provider_drop(Uuid::now_v7(), guard)
                .await
                .unwrap(),
            MvKeyedProjectionInstallOutcome::Removed
        ));
        assert!(service.load_ready(&target).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn keyed_control_adapter_observes_the_same_active_publication() {
        let service = service().await;
        let (request, source) = input(false);
        let target = request.target().clone();
        service
            .observe_and_install(Uuid::now_v7(), request, &source)
            .await
            .unwrap();
        let hold = service
            .begin_publication(
                target,
                LakePublicationId::try_from_uuid(Uuid::now_v7()).unwrap(),
            )
            .unwrap();
        assert!(service.ensure_no_active_publications().is_err());
        assert!(service.ensure_no_active_publications().is_err());
        drop(hold);
        assert!(service.ensure_no_active_publications().is_ok());
    }
    #[tokio::test]
    async fn keyed_scheduler_consumes_only_actual_compact_publication_facts() {
        use crate::scheduler::{MvRefreshScheduler, MvSchedulerConfig};
        let (_, source) = input(true);
        let facts = MvKeyedDocumentProjection::try_from_current(source.0).unwrap();
        let due = facts.publication().unwrap().publication_prepared_at_ms
            + facts.configuration().refresh_interval_ms.unwrap();
        let mut scheduler = MvRefreshScheduler::new(MvSchedulerConfig::new(true, 1000, 1, 10, 40));
        scheduler.observe_projection(StoredKeyedMvProjection { mv_id: 7, facts }, due as i64);
        let ready = scheduler.take_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].projection().mv_id, 7);
    }
}

impl MvKeyedReadinessExecution {
    pub async fn list_ready_dependencies_by_downstream(
        &self,
        loaded: &crate::state_store_repository::keyed::LoadedKeyedMvProjection,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        let target = loaded.projection.facts.target();
        let order = self.runtime.projection_order(target.clone());
        let cell = order.lock().await;
        if !matches!(self.runtime.readiness(&target), TargetReadiness::Ready)
            || cell.installed.as_ref() != Some(&loaded.version)
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Unavailable,
                "keyed dependency root is not ready",
            ));
        }
        self.repository
            .list_keyed_dependencies_by_downstream(loaded)
            .await
    }
}

#[cfg(test)]
#[path = "readiness_tests.rs"]
mod readiness_tests;

// Only dispatch differs; both paths retain the repository's one algorithm.
impl ReadinessRepository {
    async fn find_keyed_by_target(
        &self,
        target: &MvTarget,
        budget: PersistenceDecodeBudget,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository.find_keyed_by_target(target, budget).await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .find_keyed_by_target(target, budget)
                    .await
            }
        }
    }
    async fn find_keyed_by_target_object(
        &self,
        object: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        match &self.execution {
            ReadinessExecution::Legacy => self.repository.find_keyed_by_target_object(object).await,
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .find_keyed_by_target_object(object)
                    .await
            }
        }
    }
    async fn find_keyed_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<
        Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
        MvRepositoryError,
    > {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository
                    .find_keyed_by_target_bounded(target, bound)
                    .await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .find_keyed_by_target_bounded(target, bound)
                    .await
            }
        }
    }
    async fn list_keyed_projections(
        &self,
    ) -> Result<Vec<crate::state_store_repository::keyed::LoadedKeyedMvProjection>, MvRepositoryError>
    {
        match &self.execution {
            ReadinessExecution::Legacy => self.repository.list_keyed_projections().await,
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .list_keyed_projections()
                    .await
            }
        }
    }
    async fn list_keyed_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository.list_keyed_projection_inventory(bound).await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .list_keyed_projection_inventory(bound)
                    .await
            }
        }
    }
    async fn list_keyed_dependencies_by_downstream_bounded(
        &self,
        loaded: &crate::state_store_repository::keyed::LoadedKeyedMvProjection,
        bound: crate::repository::MvDependencyReadBound,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository
                    .list_keyed_dependencies_by_downstream_bounded(loaded, bound)
                    .await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .list_keyed_dependencies_by_downstream_bounded(loaded, bound)
                    .await
            }
        }
    }
    async fn list_keyed_dependencies_by_downstream(
        &self,
        loaded: &crate::state_store_repository::keyed::LoadedKeyedMvProjection,
    ) -> Result<Vec<crate::persistence::dependency::StoredMvDependency>, MvRepositoryError> {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository
                    .list_keyed_dependencies_by_downstream(loaded)
                    .await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .list_keyed_dependencies_by_downstream(loaded)
                    .await
            }
        }
    }
    async fn install_keyed_projection(
        &self,
        operation_id: Uuid,
        facts: crate::persistence::keyed_documents::MvKeyedDocumentProjection,
        expected: Option<crate::state_store_repository::keyed::LoadedKeyedMvProjection>,
    ) -> Result<crate::state_store_repository::keyed::LoadedKeyedMvProjection, MvRepositoryError>
    {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository
                    .install_keyed_projection(operation_id, facts, expected)
                    .await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .install_keyed_projection(operation_id, facts, expected)
                    .await
            }
        }
    }
    async fn delete_keyed_projection(
        &self,
        operation_id: Uuid,
        expected: crate::state_store_repository::keyed::LoadedKeyedMvProjection,
    ) -> Result<bool, MvRepositoryError> {
        match &self.execution {
            ReadinessExecution::Legacy => {
                self.repository
                    .delete_keyed_projection(operation_id, expected)
                    .await
            }
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .delete_keyed_projection(operation_id, expected)
                    .await
            }
        }
    }
    #[cfg(any(test, novarocks_dev_controls))]
    async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError> {
        match &self.execution {
            ReadinessExecution::Legacy => self.repository.wipe_accelerator(operation_id).await,
            ReadinessExecution::Original(execution) => {
                self.repository
                    .in_requests(execution.as_ref())
                    .wipe_accelerator(operation_id)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod keyed_original_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause, domain};
    use novarocks_spi::connector::{ConnectorError, KeyedDocumentReadError};

    #[test]
    fn document_projection_keeps_connector_origin_for_both_actual_envelopes() {
        let error = ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::Cancelled,
        );
        let cause = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>())
            .unwrap()
            .clone();
        for envelope in [
            MvDocumentError::Connector(error.clone()),
            MvDocumentError::KeyedStorage(KeyedDocumentReadError::Invalid(error)),
        ] {
            let projected = MvProjectionError::from(envelope);
            assert_eq!(projected.kind(), MvProjectionErrorKind::Cancelled);
            assert_eq!(projected.original_failure(), Some(&cause));
            assert_eq!(
                projected.original_failure().unwrap().failure_fact(),
                cause.failure_fact()
            );
            assert_eq!(
                std::error::Error::source(&projected)
                    .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
                Some(&cause)
            );
        }
    }

    #[test]
    fn document_projection_keeps_the_actual_legacy_codec_source() {
        let legacy = KeyedDocumentReadError::LegacyFormat {
            key: "original-legacy-key".into(),
        };
        let projected = MvProjectionError::from(MvDocumentError::KeyedStorage(legacy.clone()));
        assert_eq!(projected.kind(), MvProjectionErrorKind::Unsupported);
        let cause = projected.original_failure().unwrap();
        assert_eq!(
            cause.downcast_ref::<KeyedDocumentReadError>(),
            Some(&legacy)
        );
        assert_eq!(cause.failure_fact(), legacy.failure_fact());
        assert_eq!(cause.failure_fact().class(), FailureClass::Unsupported);
        assert_eq!(
            cause.failure_fact().reason().domain(),
            domain::METADATA.get()
        );
        assert_eq!(cause.failure_fact().reason().code(), 916);
        assert_eq!(
            std::error::Error::source(&projected)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
            Some(cause)
        );
        assert_eq!(
            projected.to_string(),
            MvDocumentError::KeyedStorage(legacy).to_string()
        );
    }
}
