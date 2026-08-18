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

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorMaxCompactableDataFilesRequest, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceLease, ConnectorMetadataMaintenanceOperation,
    ConnectorMetadataMaintenancePlan, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReconcileRequest,
    ConnectorMetadataMaintenanceResolver, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTablePlanningFacts, ConnectorTableRequest,
    ConnectorTableResolution, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::common::engine_error::EngineError;

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
pub trait MetadataMaintenanceCacheFinalizer {
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
    KnownCommitted(CompletedMetadataMaintenance),
    KnownUncommitted {
        failure: KnownUncommittedMetadataMaintenance,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
    ContractFailure {
        error: ConnectorError,
        dispatch: MetadataMaintenanceDispatchState,
    },
}

/// Compatibility projection for a synchronous statement caller.
#[allow(clippy::too_many_arguments)]
pub fn execute_metadata_maintenance(
    resolver: &dyn ConnectorMetadataMaintenanceResolver,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    instance_id: &ConnectorInstanceId,
    operation_id: ConnectorMutationOperationId,
    table: ConnectorTableIdentity,
    intent: MetadataMaintenanceIntent,
    context: ConnectorRequestContext,
) -> Result<CompletedMetadataMaintenance, String> {
    match resolve_metadata_maintenance(
        resolver,
        cache_finalizer,
        instance_id,
        operation_id,
        table,
        intent,
        context,
    ) {
        ResolvedMetadataMaintenance::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedMetadataMaintenance::KnownUncommitted { failure } => {
            Err(EngineError::commit_known_uncommitted(failure.to_string()).to_string())
        }
        ResolvedMetadataMaintenance::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_string())
        }
        ResolvedMetadataMaintenance::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

/// Execute a plan that a frontend durable-operation owner has already made
/// visible as RUNNING.  The session owns the exact-generation lease until the
/// provider returns a terminal outcome.
pub fn execute_planned_metadata_maintenance(
    session: MetadataMaintenanceSession,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
) -> Result<CompletedMetadataMaintenance, String> {
    match session.execute(cache_finalizer) {
        ResolvedMetadataMaintenance::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedMetadataMaintenance::KnownUncommitted { failure } => {
            Err(EngineError::commit_known_uncommitted(failure.to_string()).to_string())
        }
        ResolvedMetadataMaintenance::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_string())
        }
        ResolvedMetadataMaintenance::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn plan_metadata_maintenance_session(
    resolver: &dyn ConnectorMetadataMaintenanceResolver,
    instance_id: &ConnectorInstanceId,
    operation_id: ConnectorMutationOperationId,
    table: ConnectorTableIdentity,
    intent: MetadataMaintenanceIntent,
    context: ConnectorRequestContext,
) -> Result<MetadataMaintenanceSession, String> {
    MetadataMaintenanceSession::plan(resolver, instance_id, operation_id, table, intent, context)
        .map_err(resolved_error_message)
}

pub fn reconcile_metadata_maintenance_session(
    resolver: &dyn ConnectorMetadataMaintenanceResolver,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    table: ConnectorTableIdentity,
    plan: ConnectorMetadataMaintenancePlan,
    context: ConnectorRequestContext,
) -> Result<CompletedMetadataMaintenance, String> {
    let session = MetadataMaintenanceSession::recover(resolver, table, plan, None, context)
        .map_err(resolved_error_message)?;
    match session.reconcile(None, cache_finalizer) {
        ResolvedMetadataMaintenance::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedMetadataMaintenance::KnownUncommitted { failure } => {
            Err(EngineError::commit_known_uncommitted(failure.to_string()).to_string())
        }
        ResolvedMetadataMaintenance::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_string())
        }
        ResolvedMetadataMaintenance::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

fn resolved_error_message(value: ResolvedMetadataMaintenance) -> String {
    match value {
        ResolvedMetadataMaintenance::KnownUncommitted { failure } => failure.to_string(),
        ResolvedMetadataMaintenance::CommitUnknown { failure, .. } => failure.to_string(),
        ResolvedMetadataMaintenance::ContractFailure { error, .. } => error.to_string(),
        ResolvedMetadataMaintenance::KnownCommitted(_) => {
            "metadata maintenance planning unexpectedly committed".to_string()
        }
    }
}

/// Read one provider-owned maintenance observation on a current lease.
///
/// This deliberately does not reuse [`MetadataMaintenanceSession`]: an
/// observation has no operation id, no plan, no receipt and no cache
/// finalization, so it must not travel the durable operation path. The lease is
/// held only long enough to resolve the table handle and ask the provider.
///
/// The call is expensive by contract — the provider enumerates live table
/// state to answer it — so it belongs to background maintenance policy only,
/// never to SQL planning.
pub fn read_max_compactable_data_files(
    resolver: &dyn ConnectorMetadataMaintenanceResolver,
    instance_id: &ConnectorInstanceId,
    table: ConnectorTableIdentity,
    context: ConnectorRequestContext,
) -> Result<Option<u64>, ConnectorError> {
    if &table.instance_id != instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector maintenance observation table does not belong to requested instance",
        ));
    }
    let lease = resolver.acquire_current_metadata_maintenance(instance_id)?;
    let metadata = lease.metadata().load_table(ConnectorTableRequest {
        table: table.clone(),
        resolution: ConnectorTableResolution::StrictBaseTable,
        context: context.clone(),
    })?;
    if metadata.identity != table || metadata.table.owner() != &lease.binding_key().instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector metadata returned a table handle for a different exact owner",
        ));
    }
    let request = ConnectorMaxCompactableDataFilesRequest::try_new(metadata.table, context)?;
    Ok(lease.read_max_compactable_data_files(request)?.value())
}

/// Plan and execute one operation using a newly acquired current lease.
#[allow(clippy::too_many_arguments)]
pub fn resolve_metadata_maintenance(
    resolver: &dyn ConnectorMetadataMaintenanceResolver,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    instance_id: &ConnectorInstanceId,
    operation_id: ConnectorMutationOperationId,
    table: ConnectorTableIdentity,
    intent: MetadataMaintenanceIntent,
    context: ConnectorRequestContext,
) -> ResolvedMetadataMaintenance {
    let session = match MetadataMaintenanceSession::plan(
        resolver,
        instance_id,
        operation_id,
        table,
        intent,
        context,
    ) {
        Ok(session) => session,
        Err(failure) => return failure,
    };
    session.execute(cache_finalizer)
}

pub struct MetadataMaintenanceSession {
    lease: ConnectorMetadataMaintenanceLease,
    table: ConnectorTableIdentity,
    plan: ConnectorMetadataMaintenancePlan,
    context: ConnectorRequestContext,
}

impl MetadataMaintenanceSession {
    /// Acquire the current exact lease, strictly load the base table, and make
    /// one immutable provider plan. This is the only entry point that plans.
    #[allow(clippy::result_large_err)]
    pub(crate) fn plan(
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
        let metadata = lease
            .metadata()
            .load_table(ConnectorTableRequest {
                table: table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .map_err(|error| ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Planning(error),
            })?;
        if metadata.identity != table || metadata.table.owner() != &lease.binding_key().instance_id
        {
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
            }
        })?;
        let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
            operation_id,
            lease.binding_key().clone(),
            operation,
            context.clone(),
        )
        .map_err(|error| ResolvedMetadataMaintenance::KnownUncommitted {
            failure: KnownUncommittedMetadataMaintenance::Planning(error),
        })?;
        let plan = lease.plan_maintenance(request).map_err(|error| {
            ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Planning(error),
            }
        })?;
        Ok(Self {
            lease,
            table,
            plan,
            context,
        })
    }

    pub fn plan_ref(&self) -> &ConnectorMetadataMaintenancePlan {
        &self.plan
    }

    /// Consume the session, so callers cannot execute its immutable plan twice.
    pub(crate) fn execute(
        self,
        cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    ) -> ResolvedMetadataMaintenance {
        let request = match ConnectorMetadataMaintenanceExecuteRequest::try_new(
            self.plan.clone(),
            self.context.clone(),
        ) {
            Ok(request) => request,
            Err(error) => {
                return contract_failure(
                    error,
                    MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                );
            }
        };
        let outcome = match self.lease.execute(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                return contract_failure(
                    error,
                    MetadataMaintenanceDispatchState::PossiblyDispatched,
                );
            }
        };
        resolve_terminal_outcome(outcome, &self.table, cache_finalizer)
    }

    /// Restore an already persisted plan on its recorded exact generation.
    /// This intentionally never loads metadata, replans, or executes.
    #[allow(clippy::result_large_err)]
    pub(crate) fn recover(
        resolver: &dyn ConnectorMetadataMaintenanceResolver,
        table: ConnectorTableIdentity,
        plan: ConnectorMetadataMaintenancePlan,
        evidence: Option<ExternalMutationEvidence>,
        context: ConnectorRequestContext,
    ) -> Result<Self, ResolvedMetadataMaintenance> {
        let key = plan.owner().clone();
        plan.validate().map_err(|error| {
            contract_failure(
                error,
                MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            )
        })?;
        if table.instance_id != key.instance_id {
            return Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "metadata maintenance recovery table does not match plan owner",
                ),
                MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            ));
        }
        let lease = resolver
            .acquire_exact_metadata_maintenance(&key)
            .map_err(|error| {
                contract_failure(
                    error,
                    MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                )
            })?;
        if let Some(evidence) = &evidence
            && (evidence.operation_id() != plan.operation_id()
                || evidence.operation_kind() != plan.operation_kind())
        {
            return Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "metadata maintenance recovery evidence does not match plan",
                ),
                MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            ));
        }
        Ok(Self {
            lease,
            table,
            plan,
            context,
        })
    }

    /// Consume a recovered session by reconciling only. An unavailable exact
    /// generation is propagated as a contract failure for the frontend to mark
    /// unresolved; it must never be substituted with the current generation.
    pub(crate) fn reconcile(
        self,
        evidence: Option<ExternalMutationEvidence>,
        cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    ) -> ResolvedMetadataMaintenance {
        let request = match ConnectorMetadataMaintenanceReconcileRequest::try_new(
            self.plan.clone(),
            evidence,
            self.context,
        ) {
            Ok(request) => request,
            Err(error) => {
                return contract_failure(
                    error,
                    MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
                );
            }
        };
        let outcome = match self.lease.reconcile(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                return contract_failure(
                    error,
                    MetadataMaintenanceDispatchState::PossiblyDispatched,
                );
            }
        };
        resolve_terminal_outcome(outcome, &self.table, cache_finalizer)
    }
}

fn resolve_terminal_outcome(
    outcome: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
    table: &ConnectorTableIdentity,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
) -> ResolvedMetadataMaintenance {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
        } => {
            let generic_finalization = cache_finalizer.invalidate_generic_table(table);
            ResolvedMetadataMaintenance::KnownCommitted(CompletedMetadataMaintenance {
                effect,
                receipt,
                finalization: merge_finalization(finalization, generic_finalization),
            })
        }
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Provider(failure),
            }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ResolvedMetadataMaintenance::CommitUnknown { failure, evidence }
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
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
        ConnectorInstanceIncarnation, ConnectorListTablesRequest, ConnectorMaxCompactableDataFiles,
        ConnectorMetadata, ConnectorMetadataMaintenance, ConnectorMetadataMaintenancePlanSummary,
        ConnectorMetadataMaintenanceReceiptSummary, ConnectorNamespaceRequest, ConnectorProviderId,
        ConnectorTableMetadata,
    };

    use super::*;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Clone, Copy)]
    enum Mode {
        Committed,
        UnknownStaysUnknown,
        PlanningFailure,
    }

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
        mode: Mode,
        metadata_calls: AtomicUsize,
        plan_calls: AtomicUsize,
        execute_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl FakeProvider {
        fn new(mode: Mode) -> Arc<Self> {
            let instance_id = ConnectorInstanceId::parse("catalog.maintenance").unwrap();
            Arc::new(Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("fake-maintenance").unwrap(),
                    instance_id: instance_id.clone(),
                },
                key: ConnectorExecutionBindingKey {
                    instance_id,
                    incarnation: ConnectorInstanceIncarnation::from_bytes([5; 16]),
                },
                mode,
                metadata_calls: AtomicUsize::new(0),
                plan_calls: AtomicUsize::new(0),
                execute_calls: AtomicUsize::new(0),
                reconcile_calls: AtomicUsize::new(0),
                events: Arc::new(Mutex::new(Vec::new())),
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
                plan.state_digest(),
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
        fn load_table(
            &self,
            request: ConnectorTableRequest,
        ) -> Result<novarocks_spi::connector::ConnectorTableMetadata, ConnectorError> {
            self.metadata_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ConnectorTableMetadata {
                identity: request.table,
                schema: Arc::new(Schema::empty()),
                planning_facts: ConnectorTablePlanningFacts::empty(),
                definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(
                    self.descriptor.instance_id.clone(),
                    Bytes::from_static(b"table"),
                )?,
            })
        }
    }

    impl ConnectorMetadataMaintenance for FakeProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }
        fn plan_maintenance(
            &self,
            request: ConnectorMetadataMaintenancePlanningRequest,
        ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
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
        }
        fn execute(
            &self,
            request: ConnectorMetadataMaintenanceExecuteRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError>
        {
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
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(&request.plan),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn reconcile(
            &self,
            request: ConnectorMetadataMaintenanceReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError>
        {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::UnknownStaysUnknown) {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "marker absent",
                    ),
                    evidence: request
                        .evidence
                        .unwrap_or_else(|| self.evidence(&request.plan)),
                });
            }
            self.events.lock().unwrap().push("provider");
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(&request.plan),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
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
                self.provider.key.clone(),
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
            key: &ConnectorExecutionBindingKey,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            if key == &self.provider.key {
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
            Arc::new(NeverCancelled),
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

    #[test]
    fn exact_lease_plans_executes_once_and_finalizes_provider_before_generic() {
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let session = MetadataMaintenanceSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([9; 16]),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .unwrap();
        assert_eq!(
            session.plan_ref().operation_kind(),
            "rewrite-metadata-layout"
        );
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);
        assert!(matches!(
            session.execute(&finalizer),
            ResolvedMetadataMaintenance::KnownCommitted(_)
        ));
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.events.lock().unwrap().as_slice(),
            ["provider", "generic"]
        );
    }

    #[test]
    fn unknown_execute_is_not_reexecuted_and_exact_recovery_only_reconciles() {
        let provider = FakeProvider::new(Mode::UnknownStaysUnknown);
        let resolver = Resolver::new(provider.clone());
        let finalizer = Finalizer {
            events: provider.events.clone(),
        };
        let session = MetadataMaintenanceSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([7; 16]),
            table(&provider.descriptor.instance_id),
            MetadataMaintenanceIntent::rewrite_metadata_layout(),
            context(),
        )
        .unwrap();
        let plan = session.plan.clone();
        assert!(matches!(
            session.execute(&finalizer),
            ResolvedMetadataMaintenance::CommitUnknown { .. }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 0);
        assert!(
            reconcile_metadata_maintenance_session(
                &resolver,
                &finalizer,
                table(&provider.descriptor.instance_id),
                plan,
                context(),
            )
            .is_err()
        );
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 1);
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
        fn load_table(
            &self,
            request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            assert_eq!(
                request.resolution,
                ConnectorTableResolution::StrictBaseTable
            );
            self.inner.load_table(request)
        }
    }

    impl ConnectorMetadataMaintenance for ObservingProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.inner.descriptor
        }
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.inner.key
        }
        fn plan_maintenance(
            &self,
            _: ConnectorMetadataMaintenancePlanningRequest,
        ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
            unreachable!("observation must not plan")
        }
        fn execute(
            &self,
            _: ConnectorMetadataMaintenanceExecuteRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError>
        {
            unreachable!("observation must not execute")
        }
        fn reconcile(
            &self,
            _: ConnectorMetadataMaintenanceReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError>
        {
            unreachable!("observation must not reconcile")
        }
        fn read_max_compactable_data_files(
            &self,
            request: novarocks_spi::connector::ConnectorMaxCompactableDataFilesRequest,
        ) -> Result<ConnectorMaxCompactableDataFiles, ConnectorError> {
            assert_eq!(request.table.owner(), &self.inner.descriptor.instance_id);
            Ok(ConnectorMaxCompactableDataFiles::new(self.observation))
        }
    }

    struct ObservingResolver {
        provider: Arc<ObservingProvider>,
    }
    impl ConnectorMetadataMaintenanceResolver for ObservingResolver {
        fn acquire_current_metadata_maintenance(
            &self,
            _: &ConnectorInstanceId,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            ConnectorMetadataMaintenanceLease::new(
                self.provider.inner.descriptor.clone(),
                self.provider.inner.key.clone(),
                self.provider.clone(),
                self.provider.clone(),
                || {},
            )
        }
        fn acquire_exact_metadata_maintenance(
            &self,
            _: &ConnectorExecutionBindingKey,
        ) -> Result<ConnectorMetadataMaintenanceLease, ConnectorError> {
            unreachable!("observation only uses the current generation")
        }
    }

    #[test]
    fn observation_reads_the_provider_scalar_without_planning() {
        let inner = FakeProvider::new(Mode::Committed);
        let instance_id = inner.descriptor.instance_id.clone();
        let resolver = ObservingResolver {
            provider: Arc::new(ObservingProvider {
                inner: inner.clone(),
                observation: Some(7),
            }),
        };

        let observed = read_max_compactable_data_files(
            &resolver,
            &instance_id,
            table(&instance_id),
            context(),
        )
        .expect("observation");

        assert_eq!(observed, Some(7));
        assert_eq!(inner.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.plan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner.execute_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn observation_is_unsupported_when_the_provider_does_not_implement_it() {
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = Resolver::new(provider.clone());

        let error = read_max_compactable_data_files(
            &resolver,
            &provider.descriptor.instance_id,
            table(&provider.descriptor.instance_id),
            context(),
        )
        .expect_err("default observation must fail closed");

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn observation_rejects_a_table_from_another_instance() {
        let provider = FakeProvider::new(Mode::Committed);
        let resolver = Resolver::new(provider.clone());
        let foreign = ConnectorInstanceId::parse("catalog.other").unwrap();

        let error = read_max_compactable_data_files(
            &resolver,
            &provider.descriptor.instance_id,
            table(&foreign),
            context(),
        )
        .expect_err("foreign instance must fail closed");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn planning_failure_is_known_uncommitted_without_dispatch() {
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
            ),
            ResolvedMetadataMaintenance::KnownUncommitted {
                failure: KnownUncommittedMetadataMaintenance::Planning(_)
            }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 0);
    }
}
