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

//! Provider-neutral application bridge for one FE-only data mutation.

use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationLease,
    ConnectorDataMutationOperation, ConnectorDataMutationPlanningRequest,
    ConnectorDataMutationReceipt, ConnectorDataMutationReconcileRequest,
    ConnectorDataMutationResolver, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTablePlanningFacts,
    ConnectorTableRequest, ConnectorTableResolution, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::common::engine_error::EngineError;

/// The statement-level operation before its provider-owned table handle is
/// loaded through the exact-generation metadata capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataMutationIntent {
    RegisterExistingFiles { source_location: Arc<str> },
    Truncate { target_ref: Arc<str> },
}

impl DataMutationIntent {
    pub fn register_existing_files(source_location: impl Into<Arc<str>>) -> Self {
        Self::RegisterExistingFiles {
            source_location: source_location.into(),
        }
    }

    pub fn truncate(target_ref: impl Into<Arc<str>>) -> Self {
        Self::Truncate {
            target_ref: target_ref.into(),
        }
    }

    fn into_operation(
        self,
        table: novarocks_spi::connector::ConnectorTableHandle,
    ) -> Result<ConnectorDataMutationOperation, ConnectorError> {
        match self {
            Self::RegisterExistingFiles { source_location } => {
                ConnectorDataMutationOperation::register_existing_files(table, source_location)
            }
            Self::Truncate { target_ref } => {
                ConnectorDataMutationOperation::truncate(table, target_ref)
            }
        }
    }
}

/// Invalidates the application-owned catalog cache after provider finalization.
///
/// Provider-owned table-cache invalidation is part of the provider's returned
/// `ExternalMutationFinalization`. Calling this port only after that outcome is
/// received fixes the provider-before-generic ordering without exposing a
/// concrete provider registry to core.
pub trait DataMutationCacheFinalizer {
    fn invalidate_generic_table(
        &self,
        table: &ConnectorTableIdentity,
    ) -> Result<(), ConnectorError>;
}

#[derive(Clone, Debug)]
pub struct CompletedDataMutation {
    #[allow(dead_code)]
    pub effect: ExternalMutationEffect,
    pub receipt: ConnectorDataMutationReceipt,
    pub finalization: ExternalMutationFinalization,
}

/// A known-uncommitted result can originate either from read-only planning or
/// from an explicit provider outcome. Keeping those sources distinct avoids
/// pretending that a planning `ConnectorError` crossed the dispatch boundary.
#[derive(Clone, Debug)]
pub enum KnownUncommittedDataMutation {
    Planning(ConnectorError),
    Provider(ConnectorMutationFailure),
}

impl fmt::Display for KnownUncommittedDataMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planning(error) => write!(formatter, "{error}"),
            Self::Provider(failure) => write!(formatter, "{failure}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataMutationDispatchState {
    ConfirmedNotDispatched,
    PossiblyDispatched,
}

/// Application view after one execute or one explicit reconciliation.
#[derive(Clone, Debug)]
pub enum ResolvedDataMutation {
    KnownCommitted(CompletedDataMutation),
    KnownUncommitted {
        failure: KnownUncommittedDataMutation,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        #[allow(dead_code)]
        evidence: ExternalMutationEvidence,
    },
    ContractFailure {
        error: ConnectorError,
        #[allow(dead_code)]
        dispatch: DataMutationDispatchState,
    },
}

/// Execute a data mutation and project its typed truth state to the existing
/// user-facing engine error vocabulary.
#[allow(clippy::too_many_arguments)]
pub fn execute_data_mutation(
    resolver: &dyn ConnectorDataMutationResolver,
    cache_finalizer: &dyn DataMutationCacheFinalizer,
    instance_id: &ConnectorInstanceId,
    operation_id: ConnectorMutationOperationId,
    table: ConnectorTableIdentity,
    intent: DataMutationIntent,
    context: ConnectorRequestContext,
) -> Result<CompletedDataMutation, String> {
    match resolve_data_mutation(
        resolver,
        cache_finalizer,
        instance_id,
        operation_id,
        table,
        intent,
        context,
    ) {
        ResolvedDataMutation::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedDataMutation::KnownUncommitted { failure } => {
            Err(EngineError::commit_known_uncommitted(failure.to_string()).to_string())
        }
        ResolvedDataMutation::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_string())
        }
        ResolvedDataMutation::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

/// Plan and execute one operation on one exact-generation lease.
///
/// The operation ID is supplied by the caller. Planning is read-only and any
/// planning error is therefore known uncommitted. Once execute is called, an
/// outer SPI error is conservatively possibly dispatched. A provider unknown
/// is reconciled once through the retained lease and is never re-executed.
#[allow(clippy::too_many_arguments)]
pub fn resolve_data_mutation(
    resolver: &dyn ConnectorDataMutationResolver,
    cache_finalizer: &dyn DataMutationCacheFinalizer,
    instance_id: &ConnectorInstanceId,
    operation_id: ConnectorMutationOperationId,
    table: ConnectorTableIdentity,
    intent: DataMutationIntent,
    context: ConnectorRequestContext,
) -> ResolvedDataMutation {
    let mut session = match DataMutationSession::plan(
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
    let executed = session.execute_once(cache_finalizer);
    let ResolvedDataMutation::CommitUnknown { evidence, .. } = &executed else {
        return executed;
    };
    session.reconcile_once(evidence.clone(), cache_finalizer)
}

pub struct DataMutationSession {
    lease: ConnectorDataMutationLease,
    table: ConnectorTableIdentity,
    plan: novarocks_spi::connector::ConnectorDataMutationPlan,
    context: ConnectorRequestContext,
    phase: DataMutationSessionPhase,
}

enum DataMutationSessionPhase {
    Planned,
    AwaitingReconcile(ExternalMutationEvidence),
    Terminal,
}

impl DataMutationSession {
    #[allow(clippy::result_large_err)]
    pub fn plan(
        resolver: &dyn ConnectorDataMutationResolver,
        instance_id: &ConnectorInstanceId,
        operation_id: ConnectorMutationOperationId,
        table: ConnectorTableIdentity,
        intent: DataMutationIntent,
        context: ConnectorRequestContext,
    ) -> Result<Self, ResolvedDataMutation> {
        if &table.instance_id != instance_id {
            return Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector data mutation table does not belong to the requested instance",
                ),
                DataMutationDispatchState::ConfirmedNotDispatched,
            ));
        }
        let lease = resolver
            .acquire_current_data_mutation(instance_id)
            .map_err(|error| {
                contract_failure(error, DataMutationDispatchState::ConfirmedNotDispatched)
            })?;
        let metadata = lease
            .metadata()
            .load_table(ConnectorTableRequest {
                table: table.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .map_err(|error| ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Planning(error),
            })?;
        if metadata.identity != table || metadata.table.owner() != &lease.binding_key().instance_id
        {
            return Err(contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector metadata returned a table handle for a different exact owner",
                ),
                DataMutationDispatchState::ConfirmedNotDispatched,
            ));
        }
        let operation = intent.into_operation(metadata.table).map_err(|error| {
            ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Planning(error),
            }
        })?;
        let request = ConnectorDataMutationPlanningRequest::try_new(
            operation_id,
            lease.binding_key().clone(),
            operation,
            context.clone(),
        )
        .map_err(|error| ResolvedDataMutation::KnownUncommitted {
            failure: KnownUncommittedDataMutation::Planning(error),
        })?;
        let plan = lease.plan_mutation(request).map_err(|error| {
            ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Planning(error),
            }
        })?;
        Ok(Self {
            lease,
            table,
            plan,
            context,
            phase: DataMutationSessionPhase::Planned,
        })
    }

    /// Establish this attempt's external fence before anything is dispatched.
    ///
    /// TRUNCATE and ADD FILES destroy or extend table content, so a superseded
    /// owner's late execute has to be refused at the catalog rather than
    /// merely reported afterwards. The provider publishes the marker and
    /// returns the receipt that acknowledges it; the receipt travels back out
    /// so the frontend can journal proof of the fence before dispatch.
    pub fn establish_external_fence(
        &self,
        fence: novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> Result<
        novarocks_spi::connector::ConnectorExternalFenceReceipt,
        novarocks_spi::connector::ConnectorError,
    > {
        self.lease
            .establish_external_fence(fence, self.context.clone())
    }

    pub fn plan_ref(&self) -> &novarocks_spi::connector::ConnectorDataMutationPlan {
        &self.plan
    }

    pub fn descriptor_ref(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.lease.descriptor()
    }

    /// Dispatch the retained plan exactly once.
    ///
    /// A commit-unknown outcome is deliberately returned without reconciling.
    /// The caller must first durably persist the evidence and then explicitly
    /// pass that same evidence to [`Self::reconcile_once`].
    pub fn execute_once(
        &mut self,
        cache_finalizer: &dyn DataMutationCacheFinalizer,
    ) -> ResolvedDataMutation {
        if !matches!(self.phase, DataMutationSessionPhase::Planned) {
            return contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector data mutation session execute was already attempted",
                ),
                DataMutationDispatchState::ConfirmedNotDispatched,
            );
        }
        let request = match ConnectorDataMutationExecuteRequest::try_new(
            self.plan.clone(),
            // Whatever this authority established before dispatch. An
            // operation that never established one says so; it never invents a
            // fence at terminal time, which would look fenced while asserting
            // nothing.
            match self.lease.fencing() {
                Ok(fencing) => fencing,
                Err(error) => {
                    self.phase = DataMutationSessionPhase::Terminal;
                    return contract_failure(
                        error,
                        DataMutationDispatchState::ConfirmedNotDispatched,
                    );
                }
            },
            self.context.clone(),
        ) {
            Ok(request) => request,
            Err(error) => {
                self.phase = DataMutationSessionPhase::Terminal;
                return contract_failure(error, DataMutationDispatchState::ConfirmedNotDispatched);
            }
        };
        let outcome = match self.lease.execute(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.phase = DataMutationSessionPhase::Terminal;
                return contract_failure(error, DataMutationDispatchState::PossiblyDispatched);
            }
        };
        self.phase = match &outcome {
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                DataMutationSessionPhase::AwaitingReconcile(evidence.clone())
            }
            _ => DataMutationSessionPhase::Terminal,
        };
        resolve_terminal_outcome(outcome, &self.table, cache_finalizer)
    }

    /// Reconcile one previously returned unknown outcome on the retained exact
    /// lease. Passing the evidence back explicitly is the durable-barrier seam:
    /// core cannot silently reconcile before the frontend records it.
    pub fn reconcile_once(
        &mut self,
        evidence: ExternalMutationEvidence,
        cache_finalizer: &dyn DataMutationCacheFinalizer,
    ) -> ResolvedDataMutation {
        let DataMutationSessionPhase::AwaitingReconcile(expected) = &self.phase else {
            return contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector data mutation session is not awaiting reconciliation",
                ),
                DataMutationDispatchState::ConfirmedNotDispatched,
            );
        };
        if expected != &evidence {
            return contract_failure(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector data mutation evidence does not match the execute outcome",
                ),
                DataMutationDispatchState::ConfirmedNotDispatched,
            );
        }
        let reconcile = match ConnectorDataMutationReconcileRequest::try_new(
            &self.plan,
            evidence,
            self.context.clone(),
        ) {
            Ok(request) => request,
            Err(error) => {
                return contract_failure(error, DataMutationDispatchState::ConfirmedNotDispatched);
            }
        };
        let outcome = match self.lease.reconcile(reconcile) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.phase = DataMutationSessionPhase::Terminal;
                return contract_failure(error, DataMutationDispatchState::PossiblyDispatched);
            }
        };
        self.phase = DataMutationSessionPhase::Terminal;
        resolve_terminal_outcome(outcome, &self.table, cache_finalizer)
    }
}

fn resolve_terminal_outcome(
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    table: &ConnectorTableIdentity,
    cache_finalizer: &dyn DataMutationCacheFinalizer,
) -> ResolvedDataMutation {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
        } => {
            let generic_finalization = cache_finalizer.invalidate_generic_table(table);
            ResolvedDataMutation::KnownCommitted(CompletedDataMutation {
                effect,
                receipt,
                finalization: merge_finalization(finalization, generic_finalization),
            })
        }
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Provider(failure),
            }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ResolvedDataMutation::CommitUnknown { failure, evidence }
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
    dispatch: DataMutationDispatchState,
) -> ResolvedDataMutation {
    ResolvedDataMutation::ContractFailure { error, dispatch }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorDataMutation, ConnectorDataMutationPlan,
        ConnectorDataMutationPlanSummary, ConnectorExecutionBindingKey,
        ConnectorInstanceDescriptor, ConnectorInstanceIncarnation, ConnectorListTablesRequest,
        ConnectorMetadata, ConnectorNamespaceRequest, ConnectorProviderId, ConnectorTableMetadata,
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
        MetadataFailure,
        PlanningFailure,
        Uncommitted,
        UnknownThenCommitted,
        UnknownStaysUnknown,
        ExecuteContractFailure,
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
            let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance");
            let descriptor = ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake-data").expect("provider"),
                instance_id: instance_id.clone(),
            };
            Arc::new(Self {
                descriptor,
                key: ConnectorExecutionBindingKey {
                    instance_id,
                    incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
                },
                mode,
                metadata_calls: AtomicUsize::new(0),
                plan_calls: AtomicUsize::new(0),
                execute_calls: AtomicUsize::new(0),
                reconcile_calls: AtomicUsize::new(0),
                events: Arc::new(Mutex::new(Vec::new())),
            })
        }

        fn receipt(&self, plan: &ConnectorDataMutationPlan) -> ConnectorDataMutationReceipt {
            ConnectorDataMutationReceipt::try_new(
                self.descriptor.clone(),
                self.key.incarnation,
                plan.operation_id(),
                plan.operation_kind(),
                plan.request_digest(),
                plan.plan_digest(),
                plan.state_digest(),
                plan.summary(),
                Bytes::new(),
            )
            .expect("receipt")
        }

        fn evidence(&self, plan: &ConnectorDataMutationPlan) -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                self.descriptor.clone(),
                self.key.incarnation,
                plan.operation_id(),
                plan.operation_kind(),
                Bytes::from_static(b"evidence"),
            )
            .expect("evidence")
        }
    }

    impl ConnectorMetadata for FakeProvider {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.descriptor.instance_id
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!()
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            unreachable!()
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            unreachable!()
        }

        fn load_table(
            &self,
            request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            self.metadata_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::MetadataFailure) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "table metadata is unavailable",
                ));
            }
            Ok(ConnectorTableMetadata {
                identity: request.table,
                schema: Arc::new(Schema::empty()),
                planning_facts: ConnectorTablePlanningFacts::empty(),
                definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                    self.descriptor.instance_id.clone(),
                    Bytes::from_static(b"table"),
                )?,
            })
        }
    }

    impl ConnectorDataMutation for FakeProvider {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn plan_mutation(
            &self,
            request: ConnectorDataMutationPlanningRequest,
        ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::PlanningFailure) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "planning rejected the source",
                ));
            }
            ConnectorDataMutationPlan::try_new(
                &request,
                [3; 32],
                ConnectorDataMutationPlanSummary::try_new(1, 2, 3)?,
                match request.operation() {
                    ConnectorDataMutationOperation::RegisterExistingFiles { .. } => Some(
                        novarocks_spi::connector::ConnectorDataMutationSourceScope::try_new_directory(
                            [4; 32],
                        )?,
                    ),
                    ConnectorDataMutationOperation::Truncate { .. } => None,
                },
                Bytes::from_static(b"plan"),
            )
        }

        fn execute(
            &self,
            request: ConnectorDataMutationExecuteRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::ExecuteContractFailure) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "provider violated execute outcome contract",
                ));
            }
            if matches!(
                self.mode,
                Mode::UnknownThenCommitted | Mode::UnknownStaysUnknown
            ) {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "response lost",
                    ),
                    evidence: self.evidence(&request.plan),
                });
            }
            if matches!(self.mode, Mode::Uncommitted) {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "base state changed",
                    ),
                });
            }
            self.events.lock().expect("events").push("provider");
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(&request.plan),
                finalization: ExternalMutationFinalization::Complete,
            })
        }

        fn reconcile(
            &self,
            request: ConnectorDataMutationReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::UnknownStaysUnknown) {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "marker not visible",
                    ),
                    evidence: request.evidence,
                });
            }
            self.events.lock().expect("events").push("provider");
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: ConnectorDataMutationReceipt::try_new(
                    self.descriptor.clone(),
                    self.key.incarnation,
                    request.operation_id,
                    request.operation_kind,
                    request.request_digest,
                    request.plan_digest,
                    request.state_digest,
                    ConnectorDataMutationPlanSummary::try_new(1, 2, 3)?,
                    Bytes::new(),
                )?,
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
    }

    impl ConnectorDataMutationResolver for Resolver {
        fn acquire_current_data_mutation(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<ConnectorDataMutationLease, ConnectorError> {
            let releases = self.releases.clone();
            ConnectorDataMutationLease::new(
                self.provider.descriptor.clone(),
                self.provider.key.clone(),
                self.provider.clone(),
                self.provider.clone(),
                move || {
                    releases.fetch_add(1, Ordering::SeqCst);
                },
            )
        }

        fn acquire_exact_data_mutation(
            &self,
            _key: &ConnectorExecutionBindingKey,
        ) -> Result<ConnectorDataMutationLease, ConnectorError> {
            unreachable!()
        }
    }

    struct MissingResolver;

    impl ConnectorDataMutationResolver for MissingResolver {
        fn acquire_current_data_mutation(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<ConnectorDataMutationLease, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "data mutation capability is unavailable",
            ))
        }

        fn acquire_exact_data_mutation(
            &self,
            _key: &ConnectorExecutionBindingKey,
        ) -> Result<ConnectorDataMutationLease, ConnectorError> {
            unreachable!()
        }
    }

    struct Finalizer {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    impl DataMutationCacheFinalizer for Finalizer {
        fn invalidate_generic_table(
            &self,
            _table: &ConnectorTableIdentity,
        ) -> Result<(), ConnectorError> {
            self.events.lock().expect("events").push("generic");
            if self.fail {
                Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "generic cache unavailable",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            Arc::new(NeverCancelled),
            1024,
            1024,
        )
        .expect("context")
    }

    fn table(instance_id: &ConnectorInstanceId) -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from("orders"),
        }
    }

    fn truncate_intent() -> DataMutationIntent {
        DataMutationIntent::truncate("main")
    }

    fn resolve(provider: &Arc<FakeProvider>, finalizer: &Finalizer) -> ResolvedDataMutation {
        resolve_data_mutation(
            &Resolver::new(provider.clone()),
            finalizer,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([9; 16]),
            table(&provider.descriptor.instance_id),
            truncate_intent(),
            context(),
        )
    }

    #[test]
    fn exact_lease_loads_plans_and_executes_once_before_generic_finalization() {
        let provider = FakeProvider::new(Mode::Committed);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        let operation_id = ConnectorMutationOperationId::from_bytes([8; 16]);
        let resolver = Resolver::new(provider.clone());
        let mut session = DataMutationSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            operation_id,
            table(&provider.descriptor.instance_id),
            DataMutationIntent::register_existing_files("s3://bucket/import"),
            context(),
        )
        .expect("planned session");
        assert_eq!(session.plan_ref().operation_id(), operation_id);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);
        assert!(matches!(
            session.execute_once(&finalizer),
            ResolvedDataMutation::KnownCommitted(_)
        ));
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);
        drop(session);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 1);
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            provider.events.lock().expect("events").as_slice(),
            ["provider", "generic"]
        );
    }

    #[test]
    fn unknown_waits_for_explicit_reconcile_on_the_same_session() {
        let provider = FakeProvider::new(Mode::UnknownThenCommitted);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        let resolver = Resolver::new(provider.clone());
        let mut session = DataMutationSession::plan(
            &resolver,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([11; 16]),
            table(&provider.descriptor.instance_id),
            truncate_intent(),
            context(),
        )
        .expect("planned session");

        let ResolvedDataMutation::CommitUnknown { evidence, .. } = session.execute_once(&finalizer)
        else {
            panic!("expected commit unknown");
        };
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(resolver.releases.load(Ordering::SeqCst), 0);

        assert!(matches!(
            session.reconcile_once(evidence, &finalizer),
            ResolvedDataMutation::KnownCommitted(_)
        ));
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            session.execute_once(&finalizer),
            ResolvedDataMutation::ContractFailure {
                dispatch: DataMutationDispatchState::ConfirmedNotDispatched,
                ..
            }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mismatched_evidence_fails_before_reconcile_dispatch() {
        let provider = FakeProvider::new(Mode::UnknownThenCommitted);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        let mut session = DataMutationSession::plan(
            &Resolver::new(provider.clone()),
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([12; 16]),
            table(&provider.descriptor.instance_id),
            truncate_intent(),
            context(),
        )
        .expect("planned session");
        let ResolvedDataMutation::CommitUnknown { evidence, .. } = session.execute_once(&finalizer)
        else {
            panic!("expected commit unknown");
        };
        let mismatched = ExternalMutationEvidence::try_new(
            evidence.schema_version(),
            evidence.descriptor().clone(),
            evidence.incarnation(),
            ConnectorMutationOperationId::from_bytes([99; 16]),
            evidence.operation_kind(),
            evidence.provider_payload().clone(),
        )
        .expect("mismatched evidence");

        assert!(matches!(
            session.reconcile_once(mismatched, &finalizer),
            ResolvedDataMutation::ContractFailure {
                dispatch: DataMutationDispatchState::ConfirmedNotDispatched,
                ..
            }
        ));
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            session.reconcile_once(evidence, &finalizer),
            ResolvedDataMutation::KnownCommitted(_)
        ));
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn planning_failure_is_known_uncommitted_without_execute() {
        let provider = FakeProvider::new(Mode::PlanningFailure);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        assert!(matches!(
            resolve(&provider, &finalizer),
            ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Planning(_)
            }
        ));
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn metadata_failure_is_known_uncommitted_without_planning() {
        let provider = FakeProvider::new(Mode::MetadataFailure);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        assert!(matches!(
            resolve(&provider, &finalizer),
            ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Planning(_)
            }
        ));
        assert_eq!(provider.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unknown_reconciles_once_without_reexecuting() {
        let provider = FakeProvider::new(Mode::UnknownThenCommitted);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        assert!(matches!(
            resolve(&provider, &finalizer),
            ResolvedDataMutation::KnownCommitted(_)
        ));
        assert_eq!(provider.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn provider_uncommitted_does_not_finalize_cache() {
        let provider = FakeProvider::new(Mode::Uncommitted);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        assert!(matches!(
            resolve(&provider, &finalizer),
            ResolvedDataMutation::KnownUncommitted {
                failure: KnownUncommittedDataMutation::Provider(_)
            }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 0);
        assert!(provider.events.lock().expect("events").is_empty());
    }

    #[test]
    fn unresolved_unknown_is_returned_after_one_reconcile() {
        let provider = FakeProvider::new(Mode::UnknownStaysUnknown);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        assert!(matches!(
            resolve(&provider, &finalizer),
            ResolvedDataMutation::CommitUnknown { .. }
        ));
        assert_eq!(provider.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.reconcile_calls.load(Ordering::SeqCst), 1);
        assert!(provider.events.lock().expect("events").is_empty());
    }

    #[test]
    fn missing_lease_is_contract_failure_before_dispatch() {
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance");
        let finalizer = Finalizer {
            events: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        };
        assert!(matches!(
            resolve_data_mutation(
                &MissingResolver,
                &finalizer,
                &instance_id,
                ConnectorMutationOperationId::from_bytes([9; 16]),
                table(&instance_id),
                truncate_intent(),
                context(),
            ),
            ResolvedDataMutation::ContractFailure {
                dispatch: DataMutationDispatchState::ConfirmedNotDispatched,
                ..
            }
        ));
    }

    #[test]
    fn outer_execute_error_is_conservatively_possibly_dispatched() {
        let provider = FakeProvider::new(Mode::ExecuteContractFailure);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: false,
        };
        assert!(matches!(
            resolve(&provider, &finalizer),
            ResolvedDataMutation::ContractFailure {
                dispatch: DataMutationDispatchState::PossiblyDispatched,
                ..
            }
        ));
    }

    #[test]
    fn generic_cache_failure_stays_known_committed() {
        let provider = FakeProvider::new(Mode::Committed);
        let finalizer = Finalizer {
            events: provider.events.clone(),
            fail: true,
        };
        let ResolvedDataMutation::KnownCommitted(completed) = resolve(&provider, &finalizer) else {
            panic!("expected known committed");
        };
        assert!(matches!(
            completed.finalization,
            ExternalMutationFinalization::Failed(_)
        ));

        let error = execute_data_mutation(
            &Resolver::new(provider.clone()),
            &finalizer,
            &provider.descriptor.instance_id,
            ConnectorMutationOperationId::from_bytes([10; 16]),
            table(&provider.descriptor.instance_id),
            truncate_intent(),
            context(),
        )
        .expect_err("finalization failure must be projected");
        assert!(error.contains("generic catalog cache invalidation failed"));
    }
}
