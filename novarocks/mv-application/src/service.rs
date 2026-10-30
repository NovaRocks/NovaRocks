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

//! Product-owned process lifecycle for materialized-view activity and workers.

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use crate::activity::{
    MvActivityAdmissionError, MvActivityGate, MvActivityLease, MvActivityOwner, MvActivityTicket,
};
use crate::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvProviderFailure,
    MvRefreshExecutionPort,
};
use crate::process_runtime::{
    MvBackgroundRuntimeLifecycleError, MvBackgroundRuntimeOwner, MvBackgroundRuntimeStart,
    ProcessRuntime,
};
use crate::product::{
    MvCommand, MvCreateCommand, MvOperationContext, MvProductError, MvProductErrorKind,
    MvProductResult, MvRefreshAttemptIdentity, MvStagedTarget, MvTarget,
};
use crate::publication::MvRefreshPublicationFinalizationFacts;
use crate::readiness::{MvDropReadiness, MvKeyedReadinessService, MvRuntimePublicationLease};
use crate::repository::{MvRepositoryError, MvRepositoryErrorKind};
use crate::scheduler::{MvRefreshScheduler, MvSchedulerConfig};

mod create_progress;
mod drop_progress;
pub use create_progress::MvCreateTransition;
pub use drop_progress::MvDropTransition;

/// The one process-local MV owner for activity admission and background-worker
/// lifecycle. Hosts may inject effect callbacks, but cannot own a parallel
/// gate, shutdown state, or worker supervisor.
pub struct MvKeyedProductService {
    operations: Arc<MvProductOperations>,
    background: MvBackgroundRuntimeOwner,
}

/// The same process-local business state borrowed by original Actions and
/// background Sources. It has no reference back to the background lifecycle.
/// Keeping an active operation alive must not keep its worker owner alive.
pub struct MvProductOperations {
    activity_gate: MvActivityGate,
    scheduler_config: MvSchedulerConfig,
    scheduler: Mutex<MvRefreshScheduler>,
    readiness: Option<MvKeyedReadinessService>,
    management: Option<Arc<crate::management::ManagementEntrance>>,
    continuation: Option<Arc<crate::management::ManagementContinuationService>>,
}

/// One product refresh transition stored in the original Action's step slot.
/// Dropping a borrowed polling future does not drop this state or its port.
/// On a foreign unwind the original Source retains it, including the lease;
/// the query application's existing health/loan kernel owns the raw panic.
pub struct MvRefreshTransition {
    target: MvTarget,
    attempt: MvRefreshAttemptIdentity,
    publication: Option<MvRuntimePublicationLease>,
    phase: RefreshPhase,
    result: Option<Result<MvProductResult, MvProductError>>,
}

#[derive(Clone, Copy)]
enum RefreshPhase {
    New,
    Executing,
    Projecting,
    Finished,
    Poisoned,
}

impl MvRefreshTransition {
    pub fn new(target: MvTarget, attempt: MvRefreshAttemptIdentity) -> Self {
        Self {
            target,
            attempt,
            publication: None,
            phase: RefreshPhase::New,
            result: None,
        }
    }

    pub fn result(&self) -> Option<&Result<MvProductResult, MvProductError>> {
        self.result.as_ref()
    }

    fn finish(&mut self, result: Result<MvProductResult, MvProductError>) {
        // Stable business classification precedes the original lease's Drop.
        // No Source lock is held. A destructor unwind leaves this poisoned.
        self.result = Some(result);
        self.phase = RefreshPhase::Poisoned;
        drop(self.publication.take());
        self.phase = RefreshPhase::Finished;
    }
}

impl Default for MvKeyedProductService {
    fn default() -> Self {
        Self::new(MvSchedulerConfig::default())
    }
}

impl MvKeyedProductService {
    /// Construct the one product owner for this process' MV scheduler state.
    /// The host may use the tick interval to arrange a wakeup, but the queue,
    /// source revisions, retry ledger and terminal transitions remain here.
    pub fn new(scheduler_config: MvSchedulerConfig) -> Self {
        Self {
            operations: Arc::new(MvProductOperations::new(scheduler_config)),
            background: MvBackgroundRuntimeOwner::default(),
        }
    }

    /// Retain the original business state without retaining the worker owner.
    /// This is the same gate, scheduler and publication state, not a snapshot.
    pub fn original_operations(&self) -> Arc<MvProductOperations> {
        Arc::clone(&self.operations)
    }
}

impl std::ops::Deref for MvKeyedProductService {
    type Target = MvProductOperations;

    fn deref(&self) -> &Self::Target {
        self.operations.as_ref()
    }
}

impl MvProductOperations {
    fn new(scheduler_config: MvSchedulerConfig) -> Self {
        Self {
            activity_gate: MvActivityGate::default(),
            scheduler: Mutex::new(MvRefreshScheduler::new(scheduler_config.clone())),
            scheduler_config,
            readiness: None,
            management: None,
            continuation: None,
        }
    }

    /// Borrow the product-owned authority for exact provider-port adaptation.
    pub fn management_entrance(&self) -> Option<Arc<crate::management::ManagementEntrance>> {
        self.management.clone()
    }

    /// The single process-local owner of management continuation. Absent on
    /// unit-only products, which have no management authority to continue.
    pub fn management_continuation(
        &self,
    ) -> Option<Arc<crate::management::ManagementContinuationService>> {
        self.continuation.clone()
    }

    pub fn scheduler_tick_interval_ms(&self) -> u64 {
        self.scheduler_config.tick_interval_ms()
    }

    /// Run one provider-observation adapter transition against the single
    /// product scheduler. The callback receives no clone or handle that could
    /// retain scheduler state outside this process product owner.
    pub fn with_refresh_scheduler<T>(
        &self,
        operation: impl FnOnce(&mut MvRefreshScheduler) -> T,
    ) -> T {
        let mut scheduler = self
            .scheduler
            .lock()
            .expect("MV product scheduler state lock poisoned");
        operation(&mut scheduler)
    }

    /// Reserve the one immutable identity for a product refresh publication.
    /// Query and provider adapters may derive their wire values from it but
    /// cannot mint a second identity for the same product transition.
    pub fn reserve_refresh_attempt(&self) -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity::reserve()
    }

    /// Begin the product-owned process publication lifetime before any
    /// provider effect. The returned lease releases the exact target and
    /// publication identity when the refresh transition unwinds.
    pub fn begin_refresh_publication(
        &self,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
    ) -> Result<MvRuntimePublicationLease, MvProductError> {
        let readiness = self.readiness.as_ref().ok_or_else(|| {
            MvProductError::new(
                MvProductErrorKind::Unavailable,
                "MV product has no configured readiness runtime",
            )
        })?;
        readiness
            .begin_publication(target.clone(), attempt.publication_id)
            .map_err(readiness_error)
    }

    /// Execute and finalize one product-owned refresh transition. The outer
    /// port is consumed once, so it cannot retain a parallel lifecycle or
    /// replay a publication after the product has classified its outcome.
    pub fn execute_refresh<'a>(
        &self,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
        execution: Box<dyn MvRefreshExecutionPort + 'a>,
    ) -> Result<MvProductResult, MvProductError> {
        let _publication = self.begin_refresh_publication(target, attempt)?;
        let known_committed = execution
            .execute_refresh(target, attempt)
            .map_err(MvProviderFailure::into_product_error)?;
        validate_published_refresh(target, attempt, known_committed.finalization_facts())?;
        known_committed
            .project_known_committed(target)
            .map_err(known_committed_finalize_failure)?;
        Ok(MvProductResult::Acknowledged)
    }

    /// Advance the original refresh capability without consuming its owner.
    /// Target and attempt are frozen in the stable transition before any poll.
    /// Foreign polls/getters run outside locks and are fenced against replay
    /// after unwind. Independent GC remains with the original residual.
    pub fn poll_refresh(
        &self,
        cx: &mut Context<'_>,
        transition: &mut MvRefreshTransition,
        execution: &mut dyn crate::ports::MvRefreshProgressPort,
    ) -> Poll<()> {
        loop {
            match transition.phase {
                RefreshPhase::New => {
                    transition.phase = RefreshPhase::Poisoned;
                    match self.begin_refresh_publication(&transition.target, &transition.attempt) {
                        Ok(publication) => {
                            // Install the original lease before crossing the port.
                            transition.publication = Some(publication);
                            transition.phase = RefreshPhase::Executing;
                        }
                        Err(error) => {
                            transition.finish(Err(error));
                            return Poll::Ready(());
                        }
                    }
                }
                RefreshPhase::Executing => {
                    transition.phase = RefreshPhase::Poisoned;
                    if execution
                        .poll_execute_refresh(cx, &transition.target, &transition.attempt)
                        .is_pending()
                    {
                        transition.phase = RefreshPhase::Executing;
                        return Poll::Pending;
                    }
                    let result = match execution.execution_outcome() {
                        Some(Ok(published)) => validate_published_refresh(
                            &transition.target,
                            &transition.attempt,
                            published,
                        ),
                        Some(Err(error)) => Err(error.clone().into_product_error()),
                        None => {
                            panic!("MV refresh execution is Ready without its original outcome")
                        }
                    };
                    if let Err(error) = result {
                        transition.finish(Err(error));
                        return Poll::Ready(());
                    }
                    transition.phase = RefreshPhase::Projecting;
                }
                RefreshPhase::Projecting => {
                    transition.phase = RefreshPhase::Poisoned;
                    if execution
                        .poll_project_known_committed(cx, &transition.target)
                        .is_pending()
                    {
                        transition.phase = RefreshPhase::Projecting;
                        return Poll::Pending;
                    }
                    let result = match execution.projection_outcome() {
                        Some(Ok(())) => Ok(MvProductResult::Acknowledged),
                        Some(Err(error)) => Err(known_committed_finalize_failure(error.clone())),
                        None => {
                            panic!("MV refresh projection is Ready without its original outcome")
                        }
                    };
                    transition.finish(result);
                    return Poll::Ready(());
                }
                RefreshPhase::Finished => return Poll::Ready(()),
                RefreshPhase::Poisoned => return Poll::Pending,
            }
        }
    }

    /// Execute the product-owned CREATE state machine. The outer adapter owns
    /// SQL lowering and provider capabilities, while this product owns the
    /// effect order and the distinction between pre-commit cleanup and a
    /// known-committed finalization failure.
    pub fn create(
        &self,
        operation: MvOperationContext,
        create: MvCreateCommand,
        provider: &dyn MvCreateProviderPort,
        catalog_registration: &dyn MvCreateCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        create_progress::create_immediate(self, operation, create, provider, catalog_registration)
    }

    pub fn acquire_foreground(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
        cancelled: impl Fn() -> bool,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        self.activity_gate
            .acquire_foreground(target, owner, cancelled)
    }

    /// Async migration entry on the same product FIFO. Legacy synchronous
    /// consumers keep the original method until their actual Action migrates.
    pub async fn acquire_foreground_async(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        self.activity_gate
            .acquire_foreground_async(target, owner, cancelled)
            .await
    }

    /// Register background work with the shared product FIFO gate. The host
    /// retains only its effect callback and must release the returned ticket or
    /// lease; the product retains the state machine itself.
    pub fn request_activity(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, crate::activity::MvActivityGateError> {
        self.activity_gate.request(target, owner)
    }
}

impl MvKeyedProductService {
    pub fn begin_background_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        self.background.begin_start()
    }

    pub fn begin_stopping(&self) {
        self.operations.activity_gate.begin_stopping();
    }

    pub async fn shutdown_background_workers_until(&self, deadline: Instant) -> Result<(), String> {
        self.background.shutdown_until(deadline).await
    }

    pub fn request_background_stop_for_process_exit(&self) {
        self.background.request_stop_for_process_exit();
    }
}

/// Settle a publish that did not report a committed target.
///
/// A staged target is discarded only when the publish proved it never became
/// visible. An unknown publication keeps its stage: aborting it could delete
/// the objects of a create that actually succeeded, and re-running it could
/// publish a second time. That case stays unsettled for the management owner
/// to adjudicate against the provider.
fn settle_stage_abort(
    primary: MvProductError,
    abort: Result<(), MvProviderFailure>,
) -> MvProductError {
    match abort {
        Ok(()) => primary,
        Err(abort) => {
            let mut failure = MvProductError::new(
                primary.kind(),
                format!("{}; staged target abort failed: {abort}", primary.message()),
            )
            .with_compile_control(primary.compile_control_error());
            if let Some(cause) = primary.original_failure() {
                failure = failure.with_original_failure(cause.clone());
            }
            failure
        }
    }
}

fn known_committed_finalize_failure(failure: MvProviderFailure) -> MvProductError {
    // Publication is already committed: this remains a finalization failure,
    // even when its diagnostic originated in SQL preparation.
    let mut error = MvProductError::new(
        MvProductErrorKind::KnownCommittedFinalizeFailed,
        failure.message(),
    )
    .with_compile_control(failure.compile_control_error());
    if let Some(cause) = failure.original_failure() {
        error = error.with_original_failure(cause.clone());
    }
    error
}

fn validate_published_refresh(
    target: &MvTarget,
    attempt: &MvRefreshAttemptIdentity,
    published: &MvRefreshPublicationFinalizationFacts,
) -> Result<(), MvProductError> {
    let intent = published.intent();
    if intent.publication_id() != attempt.publication_id {
        return Err(MvProductError::new(
            MvProductErrorKind::Corruption,
            "MV known-committed refresh facts do not match the reserved publication identity",
        ));
    }
    if target.catalog() != Some(intent.target_catalog())
        || target.namespace() != intent.target_namespace()
        || target.name() != intent.target_name()
    {
        return Err(MvProductError::new(
            MvProductErrorKind::Corruption,
            "MV known-committed refresh facts do not match the product target",
        ));
    }
    Ok(())
}

fn readiness_error(error: MvRepositoryError) -> MvProductError {
    let kind = match error.kind() {
        MvRepositoryErrorKind::InvalidRequest => MvProductErrorKind::InvalidRequest,
        MvRepositoryErrorKind::NotFound => MvProductErrorKind::TargetReplaced,
        MvRepositoryErrorKind::Conflict => MvProductErrorKind::Conflict,
        MvRepositoryErrorKind::Corruption => MvProductErrorKind::Corruption,
        MvRepositoryErrorKind::Unavailable => MvProductErrorKind::Unavailable,
        MvRepositoryErrorKind::CommitUnknown => MvProductErrorKind::CommitUnknown,
    };
    MvProductError::new(kind, error.to_string())
}

impl MvKeyedProductService {
    pub fn new_with_keyed_management_continuation(
        config: MvSchedulerConfig,
        repository: Arc<crate::state_store_repository::StateStoreMvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, novarocks_spi::connector::LakePublicationId>>,
        management: Arc<crate::management::ManagementEntrance>,
        policy: crate::management::RemoteEffectPolicy,
    ) -> Self {
        let mut operations = MvProductOperations::new(config);
        operations.readiness = Some(MvKeyedReadinessService::new(repository, runtime));
        operations.continuation = Some(Arc::new(
            crate::management::ManagementContinuationService::new(
                management.as_ref().clone(),
                policy,
            ),
        ));
        operations.management = Some(management);
        Self {
            operations: Arc::new(operations),
            background: MvBackgroundRuntimeOwner::default(),
        }
    }
}

impl MvProductOperations {
    pub fn keyed_readiness_service(&self) -> Option<crate::readiness::MvKeyedReadinessService> {
        self.readiness.clone()
    }
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;

#[cfg(test)]
mod original_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};

    #[test]
    fn committed_finalization_keeps_original_source_without_reclassifying_its_fact() {
        let source =
            SharedFailureCause::new(novarocks_type_contract::CompileControlError::Cancelled);
        let fact = source.failure_fact();
        let error = known_committed_finalize_failure(
            MvProviderFailure::new(
                crate::ports::MvProviderFailureKind::Unavailable,
                "original refusal",
            )
            .with_original_failure(source.clone()),
        );
        assert_eq!(
            error.kind(),
            MvProductErrorKind::KnownCommittedFinalizeFailed
        );
        assert_eq!(error.original_failure(), Some(&source));
        assert_eq!(error.original_failure().unwrap().failure_fact(), fact);
        assert_eq!(error.compile_control_error(), None);
        assert_eq!(
            std::error::Error::source(&error)
                .and_then(|cause| cause.downcast_ref::<SharedFailureCause>()),
            Some(&source),
        );
    }
}

#[cfg(test)]
mod incremental_admission_tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Waker};

    #[tokio::test(flavor = "current_thread")]
    async fn product_async_entry_uses_the_original_legacy_product_fifo() {
        let service = MvKeyedProductService::default();
        let target = MvTarget::from_parts(Some("ice"), "db", "mv");
        let first = service
            .acquire_foreground(target.clone(), MvActivityOwner::Create, || false)
            .unwrap();
        let mut waiting = Box::pin(service.acquire_foreground_async(
            target.clone(),
            MvActivityOwner::Alter,
            std::future::pending(),
        ));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let mut later = service
            .request_activity(target, MvActivityOwner::Drop)
            .unwrap();
        drop(first);
        assert!(later.try_acquire().unwrap().is_none());
        let lease = waiting.await.unwrap();
        assert!(later.try_acquire().unwrap().is_none());
        drop(lease);
        drop(later.try_acquire().unwrap().unwrap());
    }
}
