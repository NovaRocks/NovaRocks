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

//! Provider-neutral application bridge for one FE-only metadata maintenance operation.
//!
//! The bridge owns only exact-generation lease lifetime, dispatch certainty and
//! generic cache finalization. Provider-specific maintenance planning, durable
//! artifacts, markers and external mutation all remain behind the SPI.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorMaxCompactableDataFilesRequest, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceExecution, ConnectorMetadataMaintenanceLease,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenanceOutcome,
    ConnectorMetadataMaintenancePlan, ConnectorMetadataMaintenanceReceipt,
    ConnectorMetadataMaintenanceResolver, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    ExternalMutationEffect, ExternalMutationFinalization, ExternalMutationOutcome,
};

use novarocks_query_application::engine_error::EngineError;

/// Statement-level intent before loading a provider-owned table handle on the
/// exact maintenance lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataMaintenanceIntent {
    RewriteMetadataLayout,
    ExpireTableVersions {
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
}

impl MetadataMaintenanceIntent {
    pub const fn rewrite_metadata_layout() -> Self {
        Self::RewriteMetadataLayout
    }

    pub const fn expire_table_versions(
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    ) -> Self {
        Self::ExpireTableVersions {
            older_than_ms,
            retain_last,
        }
    }

    fn into_operation(
        self,
        table: ConnectorTableHandle,
    ) -> Result<ConnectorMetadataMaintenanceOperation, ConnectorError> {
        match self {
            Self::RewriteMetadataLayout => {
                ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(table)
            }
            Self::ExpireTableVersions {
                older_than_ms,
                retain_last,
            } => ConnectorMetadataMaintenanceOperation::expire_table_versions(
                table,
                older_than_ms,
                retain_last,
            ),
        }
    }
}

/// Invalidates application-owned catalog state after provider finalization.
///
/// Provider cache invalidation is represented by the provider's returned
/// finalization. This port is deliberately invoked only afterwards, preserving
/// provider-before-generic invalidation without importing provider state here.
pub trait MetadataMaintenanceCacheFinalizer: Send + Sync {
    fn invalidate_generic_table(
        &self,
        table: &ConnectorTableIdentity,
    ) -> Result<(), ConnectorError>;
}

#[derive(Clone, Debug)]
pub struct CompletedMetadataMaintenance {
    #[allow(dead_code)]
    pub effect: ExternalMutationEffect,
    pub receipt: ConnectorMetadataMaintenanceReceipt,
    pub finalization: ExternalMutationFinalization,
    /// Diagnostic cleanup, independent of necessary business finalization.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the independent cleanup diagnostic in the committed outcome"
        )
    )]
    pub cleanup: ExternalMutationFinalization,
}

/// Planning never crosses the dispatch boundary, whereas an explicit provider
/// outcome did. Preserve this distinction for the durable operation owner.
#[derive(Clone, Debug)]
pub enum KnownUncommittedMetadataMaintenance {
    Planning(ConnectorError),
    Provider(ConnectorMutationFailure),
}

impl fmt::Display for KnownUncommittedMetadataMaintenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planning(error) => write!(formatter, "{error}"),
            Self::Provider(failure) => write!(formatter, "{failure}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataMaintenanceDispatchState {
    ConfirmedNotDispatched,
    PossiblyDispatched,
}

/// Terminal application view after a single execute, or an exact-generation
/// reconcile. `CommitUnknown` remains durable-operation work; it is not an
/// invitation to dispatch the maintenance action again.
#[derive(Clone, Debug)]
pub enum ResolvedMetadataMaintenance {
    KnownCommitted(Box<CompletedMetadataMaintenance>),
    KnownUncommitted {
        failure: KnownUncommittedMetadataMaintenance,
        cleanup: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
    },
    ContractFailure {
        error: ConnectorError,
        dispatch: MetadataMaintenanceDispatchState,
    },
}

type MaintenancePreparation = Pin<
    Box<
        dyn Future<Output = Result<MetadataMaintenanceSession, ResolvedMetadataMaintenance>> + Send,
    >,
>;

/// The original durable-operation owner stores this call before polling it.
/// Cancelling a borrowed wait leaves preparation, the exact lease and execution
/// handle in this same field; it is not permission to create another call.
pub struct MetadataMaintenanceCall {
    lease: ConnectorMetadataMaintenanceLease,
    table: ConnectorTableIdentity,
    operation_id: ConnectorMutationOperationId,
    intent: MetadataMaintenanceIntent,
    #[expect(
        dead_code,
        reason = "Retains the original request scope and authority until this stable call is dropped"
    )]
    context: ConnectorRequestContext,
    preparation: Option<MaintenancePreparation>,
    preparation_poll_unproven: bool,
    session: Option<MetadataMaintenanceSession>,
    failure: Option<ResolvedMetadataMaintenance>,
}

impl MetadataMaintenanceCall {
    /// Pure lease/identity validation and stable future construction. No I/O is
    /// polled until this value has been installed in the caller's original row.
    pub fn try_new(
        resolver: &dyn ConnectorMetadataMaintenanceResolver,
        instance_id: &ConnectorInstanceId,
        operation_id: ConnectorMutationOperationId,
        table: ConnectorTableIdentity,
        intent: MetadataMaintenanceIntent,
        context: ConnectorRequestContext,
    ) -> Result<Self, ResolvedMetadataMaintenance> {
        if &table.instance_id != instance_id {
            return Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector metadata maintenance table does not belong to requested instance",
                ),
                MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            ));
        }
        let lease = resolver
            .acquire_current_metadata_maintenance(instance_id)
            .map_err(|error| {
                contract_failure(
                    error,
                    MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                )
            })?;
        let preparation = Box::pin(MetadataMaintenanceSession::plan_on_lease(
            lease.clone(),
            operation_id,
            table.clone(),
            intent.clone(),
            context.clone(),
        ));
        Ok(Self {
            lease,
            table,
            operation_id,
            intent,
            context,
            preparation: Some(preparation),
            preparation_poll_unproven: false,
            session: None,
            failure: None,
        })
    }

    pub(crate) fn has_applied_publication(&self) -> bool {
        self.session
            .as_ref()
            .and_then(|session| session.provider_outcome.as_ref())
            .is_some_and(|outcome| {
                matches!(
                    outcome,
                    Ok(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    })
                )
            })
    }

    pub(crate) fn cleanup_responsibility(
        &self,
    ) -> novarocks_spi::connector::ConnectorMetadataCleanupResponsibility {
        use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as R;
        if let Some(session) = &self.session {
            if let Some(execution) = &session.execution {
                return execution.cleanup_responsibility();
            }
            // Only a proven pre-dispatch failure without an execution can
            // have no produced objects. Absence alone is not that proof.
            return match &session.resolved {
                Some(
                    ResolvedMetadataMaintenance::KnownUncommitted {
                        cleanup: ExternalMutationFinalization::Complete,
                        ..
                    }
                    | ResolvedMetadataMaintenance::ContractFailure {
                        dispatch: MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                        ..
                    },
                ) => R::Disposed,
                _ => R::Retained,
            };
        }
        match &self.failure {
            Some(
                ResolvedMetadataMaintenance::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                    ..
                }
                | ResolvedMetadataMaintenance::ContractFailure {
                    dispatch: MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                    ..
                },
            ) if self.preparation.is_none() && !self.preparation_poll_unproven => R::Disposed,
            _ => R::Retained,
        }
    }

    pub fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }

    pub fn matches(
        &self,
        instance: &ConnectorInstanceId,
        operation_id: ConnectorMutationOperationId,
        table: &ConnectorTableIdentity,
        intent: &MetadataMaintenanceIntent,
    ) -> bool {
        self.lease.descriptor().instance_id == *instance
            && self.operation_id == operation_id
            && self.table == *table
            && self.intent == *intent
    }

    fn poll_prepared(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ResolvedMetadataMaintenance>> {
        if self.preparation_poll_unproven {
            return Poll::Ready(Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "metadata maintenance preparation did not prove actual exit",
                ),
                MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            )));
        }
        if let Some(failure) = &self.failure {
            return Poll::Ready(Err(failure.clone()));
        }
        if self.session.is_some() {
            return Poll::Ready(Ok(()));
        }
        // If foreign poll or Drop unwinds, the future/session remains in this
        // original call, and subsequent callers cannot repoll an unwound F.
        self.preparation_poll_unproven = true;
        let result = self
            .preparation
            .as_mut()
            .expect("preparation is installed")
            .as_mut()
            .poll(cx);
        match result {
            Poll::Pending => {
                self.preparation_poll_unproven = false;
                Poll::Pending
            }
            Poll::Ready(result) => {
                match result {
                    Ok(session) => self.session = Some(session),
                    Err(failure) => self.failure = Some(failure),
                }
                let retired = self.preparation.take();
                drop(retired);
                self.preparation_poll_unproven = false;
                Poll::Ready(self.failure.clone().map_or(Ok(()), Err))
            }
        }
    }

    pub async fn resolve(
        &mut self,
        cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    ) -> ResolvedMetadataMaintenance {
        if let Err(failure) = std::future::poll_fn(|cx| self.poll_prepared(cx)).await {
            return failure;
        }
        self.session
            .as_mut()
            .expect("prepared session is stable")
            .execute(cache_finalizer)
            .await
    }

    /// Original provider facts remain inspectable after a notification or
    /// finalizer panic. They never authorize another physical dispatch.
    pub fn retained_outcome(
        &self,
    ) -> Option<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> {
        self.session
            .as_ref()
            .and_then(MetadataMaintenanceSession::retained_outcome)
    }
    /// Publication classification from the original retained provider fact.
    /// It grants no cleanup/exit proof and never dispatches another operation.
    pub fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        use novarocks_spi::connector::LakePublicationDisposition as D;
        if let Some(Ok(outcome)) = self.retained_outcome() {
            return Some(match outcome {
                ExternalMutationOutcome::KnownCommitted { .. } => D::KnownCommitted,
                ExternalMutationOutcome::KnownUncommitted { .. } => D::KnownUncommitted,
                ExternalMutationOutcome::CommitUnknown { .. } => D::CommitUnknown,
            });
        }
        // Retained provider terminal above wins even after frame/Drop failure.
        // Installed execution or an interrupted begin is possibly dispatched.
        if self
            .session
            .as_ref()
            .is_some_and(|session| session.execution.is_some() || session.execution_begin_unproven)
        {
            return Some(D::CommitUnknown);
        }
        let resolved = self
            .failure
            .as_ref()
            .or_else(|| self.session.as_ref().and_then(|s| s.resolved.as_ref()))?;
        match resolved {
            ResolvedMetadataMaintenance::KnownCommitted(_) => Some(D::KnownCommitted),
            ResolvedMetadataMaintenance::KnownUncommitted { .. } => Some(D::KnownUncommitted),
            ResolvedMetadataMaintenance::CommitUnknown { .. } => Some(D::CommitUnknown),
            ResolvedMetadataMaintenance::ContractFailure {
                dispatch: MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                ..
            } => Some(D::KnownUncommitted),
            ResolvedMetadataMaintenance::ContractFailure { .. } => Some(D::CommitUnknown),
        }
    }
}

pub async fn resolve_metadata_maintenance(
    call: &mut MetadataMaintenanceCall,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
) -> ResolvedMetadataMaintenance {
    call.resolve(cache_finalizer).await
}

pub async fn execute_metadata_maintenance(
    call: &mut MetadataMaintenanceCall,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
) -> Result<CompletedMetadataMaintenance, String> {
    completed_projection(call.resolve(cache_finalizer).await)
}

fn completed_projection(
    result: ResolvedMetadataMaintenance,
) -> Result<CompletedMetadataMaintenance, String> {
    match result {
        ResolvedMetadataMaintenance::KnownCommitted(completed) => {
            let completed = *completed;
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedMetadataMaintenance::KnownUncommitted { failure, cleanup } => Err(
            EngineError::commit_known_uncommitted(super::mutation::known_uncommitted_message(
                failure, &cleanup,
            ))
            .to_string(),
        ),
        ResolvedMetadataMaintenance::CommitUnknown { failure } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_bracketed_user_message())
        }
        ResolvedMetadataMaintenance::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

#[cfg(test)]
pub async fn execute_planned_metadata_maintenance(
    session: &mut MetadataMaintenanceSession,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
) -> Result<CompletedMetadataMaintenance, String> {
    completed_projection(session.execute(cache_finalizer).await)
}

/// The original background source already formed this exact metadata
/// handle on its selected Provider metadata CPU. This constructor borrows
/// that same generation; it does not load or create another schema.
pub(crate) fn begin_captured_max_compactable_data_files(
    resolver: &dyn ConnectorMetadataMaintenanceResolver,
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    context: ConnectorRequestContext,
    cpu: std::sync::Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
) -> Result<novarocks_spi::connector::ConnectorMaxCompactableDataFilesExecution, ConnectorError> {
    let lease = resolver.acquire_exact_metadata_maintenance(planning.control_runtime_id())?;
    if metadata.identity.instance_id != lease.descriptor().instance_id
        || metadata.table.owner() != &lease.descriptor().instance_id
        || planning.control_runtime_id() != lease.control_runtime_id()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "captured maintenance metadata belongs to another exact generation",
        ));
    }
    let request =
        ConnectorMaxCompactableDataFilesRequest::try_new(metadata.table.clone(), context)?;
    lease.begin_read_max_compactable_data_files(request, cpu)
}

pub struct MetadataMaintenanceSession {
    lease: ConnectorMetadataMaintenanceLease,
    table: ConnectorTableIdentity,
    plan: ConnectorMetadataMaintenancePlan,
    context: ConnectorRequestContext,
    execution: Option<ConnectorMetadataMaintenanceExecution>,
    execution_begin_unproven: bool,
    provider_outcome: Option<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>>,
    resolved: Option<ResolvedMetadataMaintenance>,
    finalization_unproven: bool,
}

impl MetadataMaintenanceSession {
    #[cfg(test)]
    pub(crate) async fn plan(
        resolver: &dyn ConnectorMetadataMaintenanceResolver,
        instance_id: &ConnectorInstanceId,
        operation_id: ConnectorMutationOperationId,
        table: ConnectorTableIdentity,
        intent: MetadataMaintenanceIntent,
        context: ConnectorRequestContext,
    ) -> Result<Self, ResolvedMetadataMaintenance> {
        let mut call = MetadataMaintenanceCall::try_new(
            resolver,
            instance_id,
            operation_id,
            table,
            intent,
            context,
        )?;
        std::future::poll_fn(|cx| call.poll_prepared(cx)).await?;
        Ok(call.session.take().expect("prepared test session"))
    }
    async fn plan_on_lease(
        lease: ConnectorMetadataMaintenanceLease,
        operation_id: ConnectorMutationOperationId,
        table: ConnectorTableIdentity,
        intent: MetadataMaintenanceIntent,
        context: ConnectorRequestContext,
    ) -> Result<Self, ResolvedMetadataMaintenance> {
        let metadata = crate::connector::schema_creation::load_frontend_table_async(
            lease.metadata().as_ref(),
            ConnectorTableRequest {
                table: table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            },
        )
        .await
        .map_err(|error| ResolvedMetadataMaintenance::KnownUncommitted {
            failure: KnownUncommittedMetadataMaintenance::Planning(error),
            cleanup: ExternalMutationFinalization::Complete,
        })?;
        if metadata.identity != table || metadata.table.owner() != &lease.descriptor().instance_id {
            return Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector metadata returned a table handle for a different exact owner",
                ),
                MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            ));
        }
        let operation = intent.into_operation(metadata.table).map_err(|error| {
            ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Planning(error),
                cleanup: ExternalMutationFinalization::Complete,
            }
        })?;
        let plan = lease
            .plan_operation(operation_id, operation, context.clone())
            .await
            .map_err(|error| ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Planning(error),
                cleanup: ExternalMutationFinalization::Complete,
            })?;
        Ok(Self {
            lease,
            table,
            plan,
            context,
            execution: None,
            execution_begin_unproven: false,
            provider_outcome: None,
            resolved: None,
            finalization_unproven: false,
        })
    }
    #[cfg(test)]
    pub fn plan_ref(&self) -> &ConnectorMetadataMaintenancePlan {
        &self.plan
    }
    pub fn retained_outcome(
        &self,
    ) -> Option<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> {
        self.execution
            .as_ref()
            .and_then(|execution| execution.retained_result())
            .or_else(|| self.provider_outcome.clone())
    }
    /// Installs the original provider execution handle before the first wait.
    /// Re-entry resumes this exact handle; it never constructs another request.
    pub(crate) async fn execute(
        &mut self,
        cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    ) -> ResolvedMetadataMaintenance {
        if let Some(resolved) = &self.resolved {
            return resolved.clone();
        }
        if self.execution_begin_unproven {
            return contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "metadata maintenance begin did not prove non-dispatch",
                ),
                MetadataMaintenanceDispatchState::PossiblyDispatched,
            );
        }
        if self.execution.is_none() {
            let request = match ConnectorMetadataMaintenanceExecuteRequest::try_new(
                self.plan.clone(),
                self.context.clone(),
            ) {
                Ok(request) => request,
                Err(error) => {
                    let failure = contract_failure(
                        error,
                        MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                    );
                    self.resolved = Some(failure.clone());
                    return failure;
                }
            };
            // A panic after this call entered cannot prove non-dispatch.
            // The same session retains that fact before any foreign callback.
            self.execution_begin_unproven = true;
            match self.lease.begin_execute(request) {
                Ok(execution) => {
                    self.execution = Some(execution);
                    self.execution_begin_unproven = false;
                }
                Err(error) => {
                    let failure = contract_failure(
                        error,
                        MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                    );
                    self.resolved = Some(failure.clone());
                    self.execution_begin_unproven = false;
                    return failure;
                }
            }
        }
        if self.provider_outcome.is_none() {
            let result = self
                .execution
                .as_mut()
                .expect("execution is stable")
                .wait()
                .await;
            self.provider_outcome = Some(result);
        }
        let outcome = match self
            .provider_outcome
            .as_ref()
            .expect("outcome is stable")
            .clone()
        {
            Ok(outcome) => outcome,
            Err(error) => match self
                .execution
                .as_ref()
                .and_then(|execution| execution.retained_result())
            {
                Some(Ok(outcome)) => outcome_after_frame_failure(outcome, &error),
                _ => {
                    return contract_failure(
                        error,
                        MetadataMaintenanceDispatchState::PossiblyDispatched,
                    );
                }
            },
        };
        if self.finalization_unproven {
            // A cache callback panic cannot erase a known publication or call
            // that callback again. The original outer owner retains its panic.
            let outcome = outcome_after_frame_failure(
                outcome,
                &ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "generic cache finalization did not prove actual exit",
                ),
            );
            return project_terminal_without_cache(outcome);
        }
        self.finalization_unproven = true;
        let resolved = resolve_terminal_outcome(outcome, &self.table, cache_finalizer);
        self.resolved = Some(resolved.clone());
        self.finalization_unproven = false;
        resolved
    }
}

fn outcome_after_frame_failure(
    outcome: ConnectorMetadataMaintenanceOutcome,
    error: &ConnectorError,
) -> ConnectorMetadataMaintenanceOutcome {
    let frame_failure = ConnectorMutationFailure::new(
        ConnectorMutationFailureKind::Internal,
        format!("metadata maintenance execution frame failed: {error}"),
    );
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            cleanup,
            effect,
            receipt,
            finalization,
        } => {
            let finalization = match finalization {
                ExternalMutationFinalization::Complete => {
                    ExternalMutationFinalization::Failed(frame_failure)
                }
                ExternalMutationFinalization::Failed(failure) => {
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        failure.kind(),
                        format!("{}; {}", failure.message(), frame_failure.message()),
                    ))
                }
            };
            ExternalMutationOutcome::KnownCommitted {
                cleanup,
                effect,
                receipt,
                finalization,
            }
        }
        other => other,
    }
}

fn resolve_terminal_outcome(
    mut outcome: ConnectorMetadataMaintenanceOutcome,
    table: &ConnectorTableIdentity,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
) -> ResolvedMetadataMaintenance {
    if let ExternalMutationOutcome::KnownCommitted { finalization, .. } = &mut outcome {
        *finalization = merge_finalization(
            finalization.clone(),
            cache_finalizer.invalidate_generic_table(table),
        );
    }
    project_terminal_without_cache(outcome)
}

fn project_terminal_without_cache(
    outcome: ConnectorMetadataMaintenanceOutcome,
) -> ResolvedMetadataMaintenance {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            cleanup,
            effect,
            receipt,
            finalization,
        } => {
            if let ExternalMutationFinalization::Failed(error) = &cleanup {
                tracing::warn!(%error, "committed metadata maintenance retains incomplete cleanup");
            }
            ResolvedMetadataMaintenance::KnownCommitted(Box::new(CompletedMetadataMaintenance {
                effect,
                receipt,
                finalization,
                cleanup,
            }))
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Provider(failure),
                cleanup,
            }
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => {
            ResolvedMetadataMaintenance::CommitUnknown { failure }
        }
    }
}

fn merge_finalization(
    provider: ExternalMutationFinalization,
    generic: Result<(), ConnectorError>,
) -> ExternalMutationFinalization {
    match (provider, generic) {
        (ExternalMutationFinalization::Complete, Ok(())) => ExternalMutationFinalization::Complete,
        (ExternalMutationFinalization::Failed(failure), Ok(())) => {
            ExternalMutationFinalization::Failed(failure)
        }
        (ExternalMutationFinalization::Complete, Err(error)) => {
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                format!("generic catalog cache invalidation failed: {error}"),
            ))
        }
        (ExternalMutationFinalization::Failed(failure), Err(error)) => {
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                failure.kind(),
                format!(
                    "{}; generic catalog cache invalidation also failed: {error}",
                    failure.message()
                ),
            ))
        }
    }
}

fn contract_failure(
    error: ConnectorError,
    dispatch: MetadataMaintenanceDispatchState,
) -> ResolvedMetadataMaintenance {
    ResolvedMetadataMaintenance::ContractFailure { error, dispatch }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorControlRuntimeId, ConnectorInstanceDescriptor, ConnectorListTablesRequest,
        ConnectorMaxCompactableDataFiles, ConnectorMetadata, ConnectorMetadataMaintenance,
        ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
        ConnectorMetadataMaintenanceReceiptSummary, ConnectorNamespaceRequest,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorTableMetadata,
        ConnectorTablePlanningFacts, ExternalMutationEvidence, ProviderBindingEpoch,
    };

    use super::*;

    fn empty_table_metadata(
        header: novarocks_spi::connector::ConnectorTableMetadataHeader,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        // This fixture explicitly declares a zero-column table.
        let schema = creation.arc(Schema::empty())?;
        let storage = creation.storage();
        let execution = {
            let (request, work) = creation.observers();
            novarocks_connector_contract::ConnectorReadExecutionSchema::try_new_observed(
                schema.clone(),
                Vec::new().into_boxed_slice(),
                storage,
                |layout| {
                    request(layout)
                        .map_err(novarocks_connector_contract::PureProviderCompileError::Control)
                },
                |units| {
                    work(units)
                        .map_err(novarocks_connector_contract::PureProviderCompileError::Control)
                },
            )?
        };
        ConnectorTableMetadata::try_new_typed(header, schema, execution, creation)
    }

    #[derive(Clone, Copy)]
    enum Mode {
        Committed,
        CommittedCleanupFailed,
        UnknownStaysUnknown,
        PlanningFailure,
        Pending,
        BeginPanic,
    }

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorProviderBindingKey,
        mode: Mode,
        metadata_calls: AtomicUsize,
        plan_calls: AtomicUsize,
        execute_calls: AtomicUsize,
        events: Arc<Mutex<Vec<&'static str>>>,
        execution_ready: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FakeProvider {
        fn new(mode: Mode) -> Arc<Self> {
            let instance_id = ConnectorInstanceId::parse("catalog.maintenance").unwrap();
            Arc::new(Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("fake-maintenance").unwrap(),
                    instance_id: instance_id.clone(),
                },
                key: ConnectorProviderBindingKey {
                    instance_id,
                    incarnation: ProviderBindingEpoch::from_bytes([5; 16]),
                },
                mode,
                metadata_calls: AtomicUsize::new(0),
                plan_calls: AtomicUsize::new(0),
                execute_calls: AtomicUsize::new(0),
                events: Arc::new(Mutex::new(Vec::new())),
                execution_ready: Arc::new(std::sync::atomic::AtomicBool::new(!matches!(
                    mode,
                    Mode::Pending
                ))),
            })
        }

        fn receipt(
            &self,
            plan: &ConnectorMetadataMaintenancePlan,
        ) -> ConnectorMetadataMaintenanceReceipt {
            ConnectorMetadataMaintenanceReceipt::try_new(
                self.descriptor.clone(),
                self.key.incarnation,
                plan.operation_id(),
                plan.operation_kind(),
                plan.request_digest(),
                plan.plan_digest(),
                plan.intent_digest(),
                ConnectorMetadataMaintenanceReceiptSummary::default(),
                Bytes::new(),
            )
            .unwrap()
        }

        fn evidence(&self, plan: &ConnectorMetadataMaintenancePlan) -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                self.descriptor.clone(),
                self.key.incarnation,
                plan.operation_id(),
                plan.operation_kind(),
                Bytes::from_static(b"evidence"),
            )
            .unwrap()
        }
    }

    impl FakeProvider {
        fn create_test_table(
            &self,
            request: ConnectorTableRequest,
            creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            empty_table_metadata(
                novarocks_spi::connector::ConnectorTableMetadataHeader {
                    identity: request.table,
                    planning_facts: ConnectorTablePlanningFacts::empty(),
                    definition_facts:
                        novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                    version: None,
                    statistics_data_version: None,
                    table: ConnectorTableHandle::try_new(
                        self.descriptor.instance_id.clone(),
                        Bytes::from_static(b"table"),
                    )?,
                },
                creation,
            )
        }
    }

    impl ConnectorMetadata for FakeProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }
        fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
            unreachable!()
        }
        fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            unreachable!()
        }
        fn list_tables(
            &self,
            _: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            unreachable!()
        }
        fn prepare_table(
            &self,
            request: ConnectorTableRequest,
        ) -> novarocks_spi::connector::ConnectorTablePreparationFuture<'_> {
            Box::pin(async move {
                self.metadata_calls.fetch_add(1, Ordering::SeqCst);
                Ok(novarocks_spi::connector::ConnectorTablePreparation::new(
                    self.descriptor.instance_id.clone(),
                    request,
                ))
            })
        }
        fn create_prepared_table(
            &self,
            prepared: novarocks_spi::connector::ConnectorTablePreparation,
            creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            let request: ConnectorTableRequest =
                prepared.into_facts(&self.descriptor.instance_id)?;
            self.create_test_table(request, creation)
        }
        fn load_table(
            &self,
            request: ConnectorTableRequest,
            creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            novarocks_spi::connector::ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            self.metadata_calls.fetch_add(1, Ordering::SeqCst);
            self.create_test_table(request, creation)
        }
    }

    impl FakeProvider {
        fn test_outcome(
            &self,
            request: ConnectorMetadataMaintenanceExecuteRequest,
        ) -> Result<ConnectorMetadataMaintenanceOutcome, ConnectorError> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::UnknownStaysUnknown) {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "response lost",
                    ),
                    evidence: self.evidence(&request.plan),
                });
            }
            self.events.lock().unwrap().push("provider");
            Ok(ExternalMutationOutcome::KnownCommitted {
                cleanup: if matches!(self.mode, Mode::CommittedCleanupFailed) {
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "retired objects remain",
                    ))
                } else {
                    ExternalMutationFinalization::Complete
                },
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(&request.plan),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }
    impl ConnectorMetadataMaintenance for FakeProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.key
        }
        fn plan_maintenance(
            &self,
            request: ConnectorMetadataMaintenancePlanningRequest,
        ) -> novarocks_spi::connector::ConnectorMetadataMaintenanceFuture<
            '_,
            ConnectorMetadataMaintenancePlan,
        > {
            Box::pin(async move {
                self.plan_calls.fetch_add(1, Ordering::SeqCst);
                if matches!(self.mode, Mode::PlanningFailure) {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "planning rejected",
                    ));
                }
                ConnectorMetadataMaintenancePlan::try_new(
                    &request,
                    [3; 32],
                    ConnectorMetadataMaintenancePlanSummary::new(1, 2, 3, 4, 5),
                    Bytes::from_static(b"plan"),
                )
            })
        }
        fn begin_execute(
            &self,
            request: ConnectorMetadataMaintenanceExecuteRequest,
        ) -> Result<ConnectorMetadataMaintenanceExecution, ConnectorError> {
            if matches!(self.mode, Mode::BeginPanic) {
                self.execute_calls.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(71_u32);
            }
            Ok(ConnectorMetadataMaintenanceExecution::from_source(
                Arc::new(GatedExecution {
                    ready: self.execution_ready.clone(),
                    result: self.test_outcome(request),
                }),
            ))
        }
    }

    struct GatedExecution {
        ready: Arc<std::sync::atomic::AtomicBool>,
        result: Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>,
    }
    impl novarocks_spi::connector::ConnectorMetadataMaintenanceExecutionSource for GatedExecution {
        fn cleanup_responsibility(
            &self,
        ) -> novarocks_spi::connector::ConnectorMetadataCleanupResponsibility {
            use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as R;
            if !self.ready.load(Ordering::SeqCst) {
                return R::Retained;
            }
            match &self.result {
                Ok(
                    ExternalMutationOutcome::KnownCommitted {
                        cleanup: ExternalMutationFinalization::Complete,
                        ..
                    }
                    | ExternalMutationOutcome::KnownUncommitted {
                        cleanup: ExternalMutationFinalization::Complete,
                        ..
                    },
                ) => R::Disposed,
                _ => R::Retained,
            }
        }
        fn poll_result(
            &self,
            _: &mut Context<'_>,
        ) -> Poll<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> {
            if self.ready.load(Ordering::SeqCst) {
                Poll::Ready(self.result.clone())
            } else {
                Poll::Pending
            }
        }
        fn retained_result(
            &self,
        ) -> Option<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> {
            self.ready
                .load(Ordering::SeqCst)
                .then(|| self.result.clone())
        }
    }
    // Test-stack wrappers preserve the original behavior oracles. Production
    // callers must supply their stable named Call/Observation fields instead.
    async fn execute_metadata_maintenance(
        resolver: &dyn ConnectorMetadataMaintenanceResolver,
        finalizer: &dyn MetadataMaintenanceCacheFinalizer,
        instance: &ConnectorInstanceId,
        operation: ConnectorMutationOperationId,
        table: ConnectorTableIdentity,
        intent: MetadataMaintenanceIntent,
        context: ConnectorRequestContext,
    ) -> Result<CompletedMetadataMaintenance, String> {
        let mut call =
            MetadataMaintenanceCall::try_new(resolver, instance, operation, table, intent, context)
                .map_err(|failure| format!("{failure:?}"))?;
        super::execute_metadata_maintenance(&mut call, finalizer).await
    }
    async fn resolve_metadata_maintenance(
        resolver: &dyn ConnectorMetadataMaintenanceResolver,
        finalizer: &dyn MetadataMaintenanceCacheFinalizer,
        instance: &ConnectorInstanceId,
        operation: ConnectorMutationOperationId,
        table: ConnectorTableIdentity,
        intent: MetadataMaintenanceIntent,
        context: ConnectorRequestContext,
    ) -> ResolvedMetadataMaintenance {
        let mut call = match MetadataMaintenanceCall::try_new(
            resolver, instance, operation, table, intent, context,
        ) {
            Ok(call) => call,
            Err(failure) => return failure,
        };
        super::resolve_metadata_maintenance(&mut call, finalizer).await
    }
    struct Resolver {
        provider: Arc<FakeProvider>,
        releases: Arc<AtomicUsize>,
    }
    impl Resolver {
        fn new(provider: Arc<FakeProvider>) -> Self {
            Self {
                provider,
                releases: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn lease(&self) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            let releases = self.releases.clone();
            ConnectorMetadataMaintenanceLease::new(
                self.provider.descriptor.clone(),
                ConnectorControlRuntimeId::from_bytes([8; 16]),
                self.provider.key.incarnation,
                self.provider.clone(),
                self.provider.clone(),
                move || {
                    releases.fetch_add(1, Ordering::SeqCst);
                },
            )
        }
    }
    impl ConnectorMetadataMaintenanceResolver for Resolver {
        fn acquire_current_metadata_maintenance(
            &self,
            _: &ConnectorInstanceId,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            self.lease()
        }
        fn acquire_exact_metadata_maintenance(
            &self,
            control_runtime_id: ConnectorControlRuntimeId,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            if control_runtime_id == ConnectorControlRuntimeId::from_bytes([8; 16]) {
                self.lease()
            } else {
                Err(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "generation retired",
                ))
            }
        }
    }

    struct Finalizer {
        events: Arc<Mutex<Vec<&'static str>>>,
    }
    impl MetadataMaintenanceCacheFinalizer for Finalizer {
        fn invalidate_generic_table(
            &self,
            _: &ConnectorTableIdentity,
        ) -> Result<(), ConnectorError> {
            self.events.lock().unwrap().push("generic");
            Ok(())
        }
    }
    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            1024,
        )
        .unwrap()
    }
    fn table(instance_id: &ConnectorInstanceId) -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from("orders"),
        }
    }

    #[tokio::test]
    async fn exact_lease_plans_executes_once_and_finalizes_provider_before_generic() {
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let mut session = MetadataMaintenanceSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([9; 16]),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .await
        .unwrap();
        assert_eq!(
            session.plan_ref().operation_kind(),
            "rewrite-metadata-layout"
        );
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);
        assert!(matches!(
            session.execute(&finalizer).await,
            ResolvedMetadataMaintenance::KnownCommitted(_)
        ));
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        drop(session);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.events.lock().unwrap().as_slice(),
            ["provider", "generic"]
        );
    }

    #[tokio::test]
    async fn unknown_execute_is_not_reexecuted() {
        let provider = FakeProvider::new(Mode::UnknownStaysUnknown);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let mut session = MetadataMaintenanceSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([7; 16]),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .await
        .unwrap();
        assert!(matches!(
            session.execute(&finalizer).await,
            ResolvedMetadataMaintenance::CommitUnknown { .. }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unknown_maintenance_strings_preserve_the_canonical_outcome_without_reexecution() {
        for planned in [false, true] {
            let provider = FakeProvider::new(Mode::UnknownStaysUnknown);
            let resolver = Resolver::new(provider.clone());
            let finalizer = Finalizer {
                events: provider.events.clone(),
            };
            let operation = ConnectorMutationOperationId::new();
            let outcome = if planned {
                let mut session = MetadataMaintenanceSession::plan(
                    &resolver,
                    &provider.descriptor.instance_id,
                    operation,
                    table(&provider.descriptor.instance_id),
                    MetadataMaintenanceIntent::rewrite_metadata_layout(),
                    context(),
                )
                .await
                .unwrap();
                execute_planned_metadata_maintenance(&mut session, &finalizer).await
            } else {
                execute_metadata_maintenance(
                    &resolver,
                    &finalizer,
                    &provider.descriptor.instance_id,
                    operation,
                    table(&provider.descriptor.instance_id),
                    MetadataMaintenanceIntent::rewrite_metadata_layout(),
                    context(),
                )
                .await
            };
            assert_eq!(
                outcome.unwrap_err(),
                "[CommitUnknown] Unavailable: response lost"
            );
            assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
            assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
            assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
            assert_eq!(resolver.releases.load(Ordering::SeqCst), 1);
            assert!(provider.events.lock().unwrap().is_empty());
        }
    }

    /// A provider that only answers the read-only observation. Everything a
    /// mutation would need stays unreachable, which is exactly the point: the
    /// observation must not travel the operation path.
    struct ObservingProvider {
        inner: Arc<FakeProvider>,
        observation: Option<u64>,
    }

    impl ConnectorMetadata for ObservingProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            self.inner.instance_id()
        }
        fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
            unreachable!()
        }
        fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            unreachable!()
        }
        fn list_tables(
            &self,
            _: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            unreachable!()
        }
        fn prepare_table(
            &self,
            request: ConnectorTableRequest,
        ) -> novarocks_spi::connector::ConnectorTablePreparationFuture<'_> {
            assert_eq!(
                request.resolution,
                ConnectorTableResolution::StrictBaseTable
            );
            self.inner.prepare_table(request)
        }
        fn create_prepared_table(
            &self,
            prepared: novarocks_spi::connector::ConnectorTablePreparation,
            creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            self.inner.create_prepared_table(prepared, creation)
        }
        fn load_table(
            &self,
            request: ConnectorTableRequest,
            creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            assert_eq!(
                request.resolution,
                ConnectorTableResolution::StrictBaseTable
            );
            self.inner.load_table(request, creation)
        }
    }

    impl ConnectorMetadataMaintenance for ObservingProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.inner.descriptor
        }
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.inner.key
        }
        fn plan_maintenance(
            &self,
            _: ConnectorMetadataMaintenancePlanningRequest,
        ) -> novarocks_spi::connector::ConnectorMetadataMaintenanceFuture<
            '_,
            ConnectorMetadataMaintenancePlan,
        > {
            Box::pin(async move { unreachable!("observation must not plan") })
        }
        fn begin_execute(
            &self,
            _: ConnectorMetadataMaintenanceExecuteRequest,
        ) -> Result<ConnectorMetadataMaintenanceExecution, ConnectorError> {
            unreachable!("observation must not execute")
        }
        fn begin_read_max_compactable_data_files(
            &self,
            request: ConnectorMaxCompactableDataFilesRequest,
            _cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
        ) -> Result<
            novarocks_spi::connector::ConnectorMaxCompactableDataFilesExecution,
            ConnectorError,
        > {
            assert_eq!(request.table.owner(), &self.inner.descriptor.instance_id);
            Ok(
                novarocks_spi::connector::ConnectorMaxCompactableDataFilesExecution::from_source(
                    Arc::new(FiniteObservationSource {
                        _request: request,
                        value: self.observation,
                    }),
                ),
            )
        }
    }

    // This finite Provider fixture starts no Work, CPU input or foreign future.
    // Its close means no work existed; it does not model a production footer.
    struct FiniteObservationSource {
        _request: ConnectorMaxCompactableDataFilesRequest,
        value: Option<u64>,
    }
    impl novarocks_spi::connector::ConnectorMaxCompactableDataFilesSource for FiniteObservationSource {
        fn poll_result(
            &self,
            _: &mut Context<'_>,
        ) -> Poll<Result<ConnectorMaxCompactableDataFiles, ConnectorError>> {
            Poll::Ready(Ok(ConnectorMaxCompactableDataFiles::new(self.value)))
        }
        fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
    }

    // Neither the finite value Source nor the default refusal creates CPU work.
    // Any dispatch or exit observation through this port is a fixture error.
    struct UnusedObservationCpu;
    impl novarocks_spi::connector::ProviderMetadataCpuPort for UnusedObservationCpu {
        fn submit_metadata_in(
            &self,
            _: &mut Option<novarocks_spi::connector::ProviderMetadataCpuInput>,
        ) -> Result<(), ConnectorError> {
            panic!("the observation fixture starts no CPU work")
        }
        fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            panic!("the observation fixture has no CPU input")
        }
        fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            panic!("the observation fixture claims no CPU exit")
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            panic!("the observation fixture has no CPU source")
        }
    }

    struct ObservingResolver {
        planning: novarocks_spi::connector::ConnectorControlPlanningLease,
        provider: Arc<dyn ConnectorMetadataMaintenance>,
    }
    impl ObservingResolver {
        fn new(
            metadata: Arc<dyn ConnectorMetadata>,
            provider: Arc<dyn ConnectorMetadataMaintenance>,
        ) -> Self {
            use novarocks_spi::connector::ConnectorControlResolver;
            let descriptor = provider.descriptor().clone();
            // The existing catalog fixture supplies unused planning/distribution
            // facets. Metadata capture uses this observation Provider itself.
            let unused = novarocks_catalog_application::test_support::test_control_binding_for(
                descriptor.instance_id.clone(),
                5,
            );
            let binding = novarocks_spi::connector::ConnectorControlBinding::try_new(
                descriptor.clone(),
                provider.binding_key().incarnation,
                metadata,
                unused.planning().clone(),
                unused.execution_distribution().clone(),
                None,
            )
            .expect("observation control binding");
            let registry = crate::connector::fixture::FixtureConnectorRegistry::new();
            registry.register_fixture_control(binding);
            let controls = crate::connector::fixture::FixtureControlResolver::new(registry);
            let planning = controls
                .acquire_current(&descriptor.instance_id)
                .expect("current observation generation");
            Self { planning, provider }
        }
        fn lease(&self) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            ConnectorMetadataMaintenanceLease::new(
                self.provider.descriptor().clone(),
                self.planning.control_runtime_id(),
                self.provider.binding_key().incarnation,
                self.planning.binding().metadata().clone(),
                self.provider.clone(),
                || {},
            )
        }
    }
    impl ConnectorMetadataMaintenanceResolver for ObservingResolver {
        fn acquire_current_metadata_maintenance(
            &self,
            instance: &ConnectorInstanceId,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            if instance != &self.provider.descriptor().instance_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "observation generation not registered",
                ));
            }
            self.lease()
        }
        fn acquire_exact_metadata_maintenance(
            &self,
            runtime: ConnectorControlRuntimeId,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            if runtime != self.planning.control_runtime_id() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "observation generation retired",
                ));
            }
            self.lease()
        }
    }

    #[tokio::test]
    async fn observation_reads_the_provider_scalar_without_planning() {
        let inner = FakeProvider::new(Mode::Committed);
        let instance_id = inner.descriptor.instance_id.clone();
        let provider = Arc::new(ObservingProvider {
            inner: inner.clone(),
            observation: Some(7),
        });
        let resolver = ObservingResolver::new(provider.clone(), provider);
        let context = context();
        let metadata = crate::connector::schema_creation::load_frontend_table_async(
            resolver.planning.binding().metadata().as_ref(),
            ConnectorTableRequest {
                table: table(&instance_id),
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            },
        )
        .await
        .expect("captured observation metadata");
        let mut observation = begin_captured_max_compactable_data_files(
            &resolver,
            &resolver.planning,
            &metadata,
            context,
            Arc::new(UnusedObservationCpu),
        )
        .expect("observation source");
        let observed = std::future::poll_fn(|cx| observation.poll_result(cx))
            .await
            .expect("observation")
            .value();
        std::future::poll_fn(|cx| observation.poll_closed(cx))
            .await
            .expect("finite observation closed");

        assert_eq!(observed, Some(7));
        assert_eq!(inner.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.plan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner.execute_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn observation_is_unsupported_when_the_provider_does_not_implement_it() {
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = ObservingResolver::new(provider.clone(), provider.clone());
        let context = context();
        let metadata = crate::connector::schema_creation::load_frontend_table_async(
            resolver.planning.binding().metadata().as_ref(),
            ConnectorTableRequest {
                table: table(&provider.descriptor.instance_id),
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            },
        )
        .await
        .expect("captured observation metadata");
        let error = begin_captured_max_compactable_data_files(
            &resolver,
            &resolver.planning,
            &metadata,
            context,
            Arc::new(UnusedObservationCpu),
        )
        .err()
        .expect("default observation must fail closed");

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn observation_rejects_a_table_from_another_instance() {
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = ObservingResolver::new(provider.clone(), provider.clone());
        let foreign = ConnectorInstanceId::parse("catalog.other").unwrap();
        let context = context();
        // Author the same foreign identity without metadata I/O, then use the
        // real captured-helper owner gate before asking the Provider for a Source.
        let metadata =
            crate::connector::schema_creation::with_schema_creation(&context, |creation| {
                provider.create_test_table(
                    ConnectorTableRequest {
                        table: table(&foreign),
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: context.clone(),
                    },
                    creation,
                )
            })
            .expect("foreign metadata fixture");
        let error = begin_captured_max_compactable_data_files(
            &resolver,
            &resolver.planning,
            &metadata,
            context,
            Arc::new(UnusedObservationCpu),
        )
        .err()
        .expect("foreign instance must fail closed");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cleanup_failure_is_retained_without_skipping_necessary_cache_finalization() {
        struct CacheFailure;
        impl MetadataMaintenanceCacheFinalizer for CacheFailure {
            fn invalidate_generic_table(
                &self,
                _: &ConnectorTableIdentity,
            ) -> Result<(), ConnectorError> {
                Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "necessary cache barrier failed",
                ))
            }
        }
        let provider = FakeProvider::new(Mode::CommittedCleanupFailed);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let ResolvedMetadataMaintenance::KnownCommitted(completed) = resolve_metadata_maintenance(
            &resolver,
            &finalizer,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::new(),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::expire_table_versions(None, Some(1)),
            context(),
        )
        .await
        else {
            panic!("publication is known committed")
        };
        assert_eq!(
            completed.finalization,
            ExternalMutationFinalization::Complete
        );
        assert!(
            matches!(completed.cleanup, ExternalMutationFinalization::Failed(ref error) if error.message() == "retired objects remain")
        );
        assert_eq!(
            *provider.events.lock().unwrap(),
            vec!["provider", "generic"]
        );
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        let ResolvedMetadataMaintenance::KnownCommitted(completed) = resolve_metadata_maintenance(
            &resolver,
            &CacheFailure,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::new(),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::expire_table_versions(None, Some(1)),
            context(),
        )
        .await
        else {
            panic!("necessary cache failure cannot erase the commit")
        };
        assert!(
            matches!(completed.finalization, ExternalMutationFinalization::Failed(ref error) if error.message().contains("necessary cache barrier failed"))
        );
        assert!(
            matches!(completed.cleanup, ExternalMutationFinalization::Failed(ref error) if error.message() == "retired objects remain")
        );
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn original_cleanup_observation_keeps_failed_and_unknown_responsibility_without_reexecution()
     {
        use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as R;
        for (mode, expected, dispatches) in [
            (Mode::Committed, R::Disposed, 1),
            (Mode::CommittedCleanupFailed, R::Retained, 1),
            (Mode::UnknownStaysUnknown, R::Retained, 1),
            (Mode::PlanningFailure, R::Disposed, 0),
        ] {
            let provider = FakeProvider::new(mode);
            let resolver = Resolver::new(provider.clone());
            let finalizer = Finalizer {
                events: provider.events.clone(),
            };
            let mut call = MetadataMaintenanceCall::try_new(
                &resolver,
                &provider.descriptor.instance_id,
                ConnectorMutationOperationId::new(),
                table(&provider.descriptor.instance_id),
                MetadataMaintenanceIntent::expire_table_versions(None, Some(1)),
                context(),
            )
            .unwrap_or_else(|_| panic!("original Call construction"));
            assert_eq!(call.cleanup_responsibility(), R::Retained);
            call.resolve(&finalizer).await;
            for _ in 0..3 {
                assert_eq!(call.cleanup_responsibility(), expected);
            }
            assert_eq!(provider.execute_calls.load(Ordering::SeqCst), dispatches);
        }
    }

    #[tokio::test]
    async fn planning_failure_is_known_uncommitted_without_dispatch() {
        let provider = FakeProvider::new(Mode::PlanningFailure);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        assert!(matches!(
            resolve_metadata_maintenance(
                &resolver,
                &finalizer,
                &provider.descriptor.instance_id,
                ConnectorMutationOperationId::from_bytes([6; 16]),
                table(&provider.descriptor.instance_id),
                MetadataMaintenanceIntent::expire_table_versions(None, Some(1)),
                context()
            )
            .await,
            ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Planning(_),
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete
            }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn cancelled_execution_wait_resumes_same_installed_session_without_redispatch() {
        let provider = FakeProvider::new(Mode::Pending);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let mut session = MetadataMaintenanceSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::new(),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .await
        .unwrap();
        let mut wait = Box::pin(session.execute(&finalizer));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        assert!(session.execution.is_some());
        assert!(session.provider_outcome.is_none());
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);
        provider.execution_ready.store(true, Ordering::SeqCst);
        assert!(matches!(
            session.execute(&finalizer).await,
            ResolvedMetadataMaintenance::KnownCommitted(_)
        ));
        assert!(matches!(
            session.execute(&finalizer).await,
            ResolvedMetadataMaintenance::KnownCommitted(_)
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.events.lock().unwrap().as_slice(),
            ["provider", "generic"]
        );
        drop(session);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generic_finalizer_panic_keeps_original_provider_outcome_and_never_redispatches() {
        struct PanickingFinalizer(AtomicUsize);
        impl MetadataMaintenanceCacheFinalizer for PanickingFinalizer {
            fn invalidate_generic_table(
                &self,
                _: &ConnectorTableIdentity,
            ) -> Result<(), ConnectorError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("original generic finalizer panic");
            }
        }
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = Resolver::new(provider.clone());
        let finalizer = PanickingFinalizer(AtomicUsize::new(0));
        let mut call = MetadataMaintenanceCall::try_new(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::new(),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .unwrap();
        let mut wait = Box::pin(call.resolve(&finalizer));
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            wait.as_mut().poll(&mut cx)
        }))
        .expect_err("actual generic callback must unwind");
        drop(wait);
        assert!(matches!(
            call.retained_outcome().unwrap().unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert!(matches!(
            call.resolve(&finalizer).await,
            ResolvedMetadataMaintenance::KnownCommitted(completed)
                if matches!(
                    &completed.finalization,
                    ExternalMutationFinalization::Failed(_)
                )
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(finalizer.0.load(Ordering::SeqCst), 1);
        drop(failure);
    }
    #[tokio::test]
    async fn interrupted_begin_keeps_unknown_fact_and_never_begins_again() {
        use futures::FutureExt;
        let provider = FakeProvider::new(Mode::BeginPanic);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let mut call = MetadataMaintenanceCall::try_new(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::new(),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .unwrap();
        let Err(cause) = std::panic::AssertUnwindSafe(call.resolve(&finalizer))
            .catch_unwind()
            .await
        else {
            panic!("actual begin callback must panic")
        };
        assert_eq!(cause.downcast_ref::<u32>(), Some(&71));
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::CommitUnknown)
        );
        assert!(matches!(
            call.resolve(&finalizer).await,
            ResolvedMetadataMaintenance::ContractFailure {
                dispatch: MetadataMaintenanceDispatchState::PossiblyDispatched,
                ..
            }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pending_execution_publication_fact_becomes_actual_terminal_without_redispatch() {
        let provider = FakeProvider::new(Mode::Pending);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let mut call = MetadataMaintenanceCall::try_new(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::new(),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .unwrap();
        let mut wait = Box::pin(call.resolve(&finalizer));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::CommitUnknown)
        );
        provider.execution_ready.store(true, Ordering::SeqCst);
        assert!(matches!(
            call.resolve(&finalizer).await,
            ResolvedMetadataMaintenance::KnownCommitted(_)
        ));
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
    }
}
