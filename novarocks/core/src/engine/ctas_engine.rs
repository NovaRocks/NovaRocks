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

//! Statement-specific reverse port for frontend-owned CTAS orchestration.
//!
//! The frontend owns the durable saga. Core owns pure SQL preparation, the
//! exact admitted execution context, source execution, and connector calls.
//! Opaque handles ensure the frontend cannot inspect or reconstruct compiler,
//! writer, or provider-private staged-table state.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorMutationFailure, ConnectorPartitionTransform,
    ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateLease,
    ConnectorStagedCreateOperationId, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreateReceipt, ConnectorStagedCreateReconcileOutcome,
    ConnectorStagedCreateReconcilePhase, ConnectorStagedCreateReconcileRequest,
    ConnectorStagedTableHandle, ConnectorStagedWritePlanningRequest,
    ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationCompletion,
    ConnectorWriteOperationId, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    CreatePolicy, ExternalMutationEvidence, ExternalMutationFinalization,
};

use crate::query_execution::request_context::QueryExecutionContext;
use novarocks_execution::runtime::query_options::QueryOptions;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasCommand {
    pub target_parts: Vec<String>,
    pub if_not_exists: bool,
    pub source_sql: String,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CtasTargetPrecheck {
    Absent,
    ExistsNoOp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CtasFailureKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasFailure {
    pub kind: CtasFailureKind,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CtasPreparedSourceFacts {
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub source_catalog: Option<String>,
    pub source_database: String,
    pub plan_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    pub execution_identity: [u8; 32],
    pub output_columns: Vec<ConnectorColumnDefinition>,
}

pub struct PrepareCtasSourceRequest {
    pub command: CtasCommand,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub query_options: Option<QueryOptions>,
    pub execution: QueryExecutionContext,
}

pub trait CtasPreparedSource: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
}

pub struct PreparedCtasSource {
    pub facts: CtasPreparedSourceFacts,
    pub handle: Arc<dyn CtasPreparedSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasTargetFacts {
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub operation_id: [u8; 16],
    pub handle_digest: Option<[u8; 32]>,
}

/// Opaque target session. A concrete core implementation retains the same
/// exact `ConnectorStagedCreateLease` for prepare, publish, abort and explicit
/// reconcile; the frontend can persist only the neutral facts.
pub trait CtasPreparedTarget: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasTarget {
    pub facts: CtasTargetFacts,
    pub handle: Arc<dyn CtasPreparedTarget>,
}

pub enum CtasTargetPrepareOutcome {
    Prepared {
        target: PreparedCtasTarget,
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Conflict {
        failure: CtasFailure,
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        target: PreparedCtasTarget,
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
    /// The connector violated the staged-create call contract after dispatch
    /// may have occurred. The opaque target retains the exact leases for
    /// operator-driven inspection; core must not silently discard it.
    ContractUnknown {
        target: PreparedCtasTarget,
        failure: CtasFailure,
    },
}

pub trait CtasPreparedWrite: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
}

pub struct PreparedCtasWrite {
    pub target_facts: CtasTargetFacts,
    pub write_operation_id: ConnectorWriteOperationId,
    pub execution_identity: [u8; 32],
    pub handle: Arc<dyn CtasPreparedWrite>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CtasWriteOutcome {
    Completed {
        completion: ConnectorWriteOperationCompletion,
        execution_identity: [u8; 32],
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
    },
}

/// One-to-one core capability consumed by the frontend CTAS application
/// owner. It is intentionally not a generic connector DML facade.
pub trait CtasEngine: Send + Sync {
    fn classify_ctas(&self, sql: &str) -> Result<Option<CtasCommand>, String>;

    fn precheck_ctas_target(
        &self,
        command: &CtasCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<CtasTargetPrecheck, CtasFailure>;

    /// Pure analyze/optimize. It must not execute the source, reserve a
    /// writer, prepare a staged table, or consult a new live topology.
    fn prepare_ctas_source(
        &self,
        request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure>;

    fn prepare_ctas_target(
        &self,
        source: &dyn CtasPreparedSource,
        operation_id: ConnectorStagedCreateOperationId,
        policy: CreatePolicy,
    ) -> Result<CtasTargetPrepareOutcome, CtasFailure>;

    /// Bind the same optimized source artifact and admitted execution to the
    /// provider-issued staged table and SPI-4C1 sink. This call is inert.
    fn prepare_ctas_write(
        &self,
        source: &dyn CtasPreparedSource,
        target: &dyn CtasPreparedTarget,
        write_operation_id: ConnectorWriteOperationId,
    ) -> Result<PreparedCtasWrite, CtasFailure>;

    /// Consume the prepared source exactly once and return the sealed generic
    /// writer aggregate. It never publishes the target table.
    fn execute_ctas_write(&self, prepared: &dyn CtasPreparedWrite) -> CtasWriteOutcome;

    /// Explicit writer recovery against the exact retained writer session.
    /// This must never re-execute the source or substitute staged-create
    /// reconciliation for the generic writer protocol.
    fn reconcile_ctas_write(
        &self,
        prepared: &dyn CtasPreparedWrite,
        evidence: ExternalMutationEvidence,
    ) -> CtasWriteOutcome;

    fn publish_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<ConnectorStagedCreatePublishOutcome, CtasFailure>;

    fn abort_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        completion: Option<ConnectorWriteOperationCompletion>,
    ) -> Result<ConnectorStagedCreateAbortOutcome, CtasFailure>;

    /// Explicit recovery only. The normal engine path never invokes this
    /// automatically after an unknown outcome.
    fn reconcile_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        phase: ConnectorStagedCreateReconcilePhase,
        evidence: ExternalMutationEvidence,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, CtasFailure>;
}

/// Core-private guard embedded in concrete prepared source/write handles.
/// It proves preparation is inert, preserves one execution identity, and
/// rejects any second execution before reaching the coordinator.
pub(crate) struct CtasSourceExecutionGate {
    execution_identity: [u8; 32],
    executed: AtomicBool,
    source_artifact: Arc<dyn Any + Send + Sync>,
    retained_execution: Mutex<Option<QueryExecutionContext>>,
}

/// Pure CTAS source artifact. It retains the one optimized tree and the exact
/// scan bindings produced by analysis so target preparation cannot trigger a
/// second SQL compilation or a current-generation metadata lookup.
pub(crate) struct PlannedCtasSourceQuery {
    optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    output_columns: Vec<crate::sql::analysis::OutputColumn>,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    optimizer_settings: crate::sql::optimizer::options::SessionOptimizerSettings,
    connector_target_parallelism: std::num::NonZeroUsize,
}

#[allow(clippy::too_many_arguments)]
fn plan_query_for_ctas_source(
    state: &Arc<crate::engine::StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedCtasSourceQuery, String> {
    let mut query = query.clone();
    if super::has_time_travel_refs(&query) {
        super::rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut query,
            connector_context,
        )?;
    }
    let catalog_service_snapshot = super::catalog_service_snapshot(state);
    let analyzer_provider = super::build_catalog_service_provider(
        current_catalog,
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let table_bindings = analyzer_provider.query_table_bindings();
    let statistics =
        super::query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
            state,
            Arc::clone(&table_bindings),
        );
    let catalog_snapshot = crate::sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| "CTAS requires a frozen non-empty backend topology".to_string())?;
    let request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query)),
        crate::sql::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: crate::sql::compiler::RootDistributionRequirement::Any,
        },
        crate::sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let crate::sql::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err("CTAS source did not produce optimized SQL facts".to_string());
    };
    Ok(PlannedCtasSourceQuery {
        output_columns: compiled.optimized_tree.output_columns.clone(),
        optimized_tree: compiled.optimized_tree,
        table_bindings,
        optimizer_settings: execution.optimizer_settings().clone(),
        connector_target_parallelism: backend_count,
    })
}

fn prepare_planned_ctas_connector_write(
    state: &Arc<crate::engine::StandaloneState>,
    planned: &PlannedCtasSourceQuery,
    input_schema: arrow::datatypes::SchemaRef,
    query_options: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    template: crate::query_execution::contract::ConnectorWritePlanningTemplate,
) -> Result<crate::query_execution::prepared_write::PreparedDistributedWriteRequest, String> {
    let physical =
        crate::sql::planner::optimizer_bridge::to_physical_plan(&planned.optimized_tree)?;
    let distributed = crate::sql::planner::pipeline::build_connector_write_distributed_plan(
        physical,
        crate::sql::planner::distributed::write::sink::ConnectorWritePlanInput {
            target_schema: input_schema,
            input: crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
            root_output_exprs: None,
        },
        &planned.optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        state.connector_control.as_ref(),
        connector_context,
        Some(planned.table_bindings.as_ref()),
        None,
        crate::query_execution::preparation::ScanPreparationOptions::new(
            planned
                .optimizer_settings
                .connector_static_predicate_pushdown_enabled(),
            planned.connector_target_parallelism,
            None,
        ),
    )?;
    let native_bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&distributed, &prepared)?;
    let cohort_id = template.cohort_id();
    let exact_lease = template.lease();
    crate::query_execution::prepared_write::PreparedDistributedWriteRequest::new(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::ConnectorWriteOperationRegistration::single(template),
        cohort_id,
        exact_lease,
    )
    .map_err(|error| error.to_string())
}

/// Concrete opaque target session used by the core implementation. The exact
/// staged-create lease is retained for the full saga; no method reacquires a
/// current generation. The SPI lease itself enforces unresolved-operation
/// lockout after any unknown outcome.
pub(crate) struct CoreCtasTargetSession {
    lease: ConnectorStagedCreateLease,
    write_lease: ConnectorWriteLease,
    operation_id: ConnectorStagedCreateOperationId,
    handle: Mutex<Option<ConnectorStagedTableHandle>>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    write_plan_started: AtomicBool,
    write_unknown_latched: AtomicBool,
}

impl CtasPreparedTarget for CoreCtasTargetSession {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CoreCtasTargetSession {
    pub(crate) fn prepare(
        lease: ConnectorStagedCreateLease,
        write_lease: ConnectorWriteLease,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<
        (
            Arc<Self>,
            Result<ConnectorStagedCreatePrepareOutcome, novarocks_spi::connector::ConnectorError>,
        ),
        novarocks_spi::connector::ConnectorError,
    > {
        let operation_id = request.operation_id;
        if lease.owner() != write_lease.binding_key() {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "CTAS staged-create and writer leases do not share one exact generation",
            ));
        }
        let context = request.context.clone();
        let outcome = lease.prepare(request);
        let handle = match &outcome {
            Ok(ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. }) => {
                Some(handle.clone())
            }
            Err(_)
            | Ok(ConnectorStagedCreatePrepareOutcome::Conflict { .. })
            | Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { .. })
            | Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }) => None,
        };
        Ok((
            Arc::new(Self {
                lease,
                write_lease,
                operation_id,
                handle: Mutex::new(handle),
                context,
                write_plan_started: AtomicBool::new(false),
                write_unknown_latched: AtomicBool::new(false),
            }),
            outcome,
        ))
    }

    pub(crate) fn owner(&self) -> &novarocks_spi::connector::ConnectorExecutionBindingKey {
        self.lease.owner()
    }

    pub(crate) const fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.operation_id
    }

    pub(crate) fn handle_digest(&self) -> Option<[u8; 32]> {
        self.handle
            .lock()
            .ok()
            .and_then(|handle| handle.as_ref().map(ConnectorStagedTableHandle::digest))
    }

    pub(crate) fn publish(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        completion: ConnectorWriteOperationCompletion,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorStagedCreatePublishOutcome, novarocks_spi::connector::ConnectorError> {
        self.require_write_resolved()?;
        let handle = self.exact_handle()?;
        self.lease.publish(
            novarocks_spi::connector::ConnectorStagedCreatePublishRequest {
                operation_id,
                handle,
                completion,
                context,
            },
        )
    }

    pub(crate) fn bind_write(
        &self,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.require_write_resolved()?;
        self.lease.bind_write(self.exact_handle()?, completion)
    }

    pub(crate) fn reconcile_write_completion(
        &self,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        if !self.write_unknown_latched.load(Ordering::Acquire) {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "CTAS writer recovery requires a locally latched unknown outcome",
            ));
        }
        self.lease
            .reconcile_write_completion(self.exact_handle()?, completion)?;
        self.write_unknown_latched.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn plan_write(
        &self,
        operation_id: ConnectorWriteOperationId,
        input_schema: arrow::datatypes::SchemaRef,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<
        crate::query_execution::contract::ConnectorWritePlanningTemplate,
        novarocks_spi::connector::ConnectorError,
    > {
        self.require_write_resolved()?;
        if self
            .write_plan_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "CTAS staged target write has already been prepared",
            ));
        }
        // Staged-create remains the provider-only bridge that proves this
        // invisible table belongs to the retained create lease.  Immediately
        // turn that bridge result into a normal Provider-signed preparation:
        // generic CTAS orchestration must not retain its table payload or
        // provider-private plan payload.
        let binding = self.lease.plan_write(ConnectorStagedWritePlanningRequest {
            handle: self.exact_handle()?,
            operation_id,
            intent: ConnectorWriteIntent::Append,
            input_schema: Arc::clone(&input_schema),
            context,
        })?;
        let outcome =
            self.write_lease
                .control()
                .prepare_write(ConnectorWritePreparationRequest {
                    table: binding.table().clone(),
                    target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
                    intent: binding.intent(),
                    purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                    input: ConnectorWriteInputRequest::Data {
                        fields: input_schema
                            .fields()
                            .iter()
                            .map(|field| ConnectorWriteFieldRequest::new(field.as_ref().clone()))
                            .collect(),
                    },
                    context: binding.context().clone(),
                })?;
        let preparation = match outcome {
            ConnectorWritePreparationOutcome::Prepared(preparation) => preparation,
            ConnectorWritePreparationOutcome::Denied(error) => return Err(error),
        };
        crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared(
            binding.operation_id(),
            preparation,
            binding.context().clone(),
            self.write_lease.clone(),
        )
    }

    pub(crate) fn write_lease(&self) -> ConnectorWriteLease {
        self.write_lease.clone()
    }

    pub(crate) fn context(&self) -> novarocks_spi::connector::ConnectorRequestContext {
        self.context.clone()
    }

    pub(crate) fn mark_write_unknown(
        &self,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.write_unknown_latched.store(true, Ordering::Release);
        self.lease.mark_write_unknown(&self.exact_handle()?)
    }

    pub(crate) fn abort(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        completion: Option<ConnectorWriteOperationCompletion>,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorStagedCreateAbortOutcome, novarocks_spi::connector::ConnectorError> {
        self.require_write_resolved()?;
        self.lease.abort(
            novarocks_spi::connector::ConnectorStagedCreateAbortRequest {
                operation_id,
                handle: self.exact_handle()?,
                completion,
                context,
            },
        )
    }

    pub(crate) fn reconcile(
        &self,
        phase: ConnectorStagedCreateReconcilePhase,
        evidence: ExternalMutationEvidence,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, novarocks_spi::connector::ConnectorError>
    {
        let outcome = self
            .lease
            .reconcile(ConnectorStagedCreateReconcileRequest {
                target_operation_id: self.operation_id,
                phase,
                evidence,
                context,
            })?;
        if let ConnectorStagedCreateReconcileOutcome::Prepared { handle, .. } = &outcome {
            *self.handle.lock().map_err(|error| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    format!("CTAS staged target handle lock: {error}"),
                )
            })? = Some(handle.clone());
        }
        Ok(outcome)
    }

    fn exact_handle(
        &self,
    ) -> Result<ConnectorStagedTableHandle, novarocks_spi::connector::ConnectorError> {
        self.handle
            .lock()
            .map_err(|error| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    format!("CTAS staged target handle lock: {error}"),
                )
            })?
            .clone()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "CTAS staged target has no prepared provider handle",
                )
            })
    }

    fn require_write_resolved(&self) -> Result<(), novarocks_spi::connector::ConnectorError> {
        if self.write_unknown_latched.load(Ordering::Acquire) {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                "CTAS writer outcome is unresolved; publish and abort are forbidden",
            ));
        }
        Ok(())
    }
}

impl CtasSourceExecutionGate {
    pub(crate) fn new(
        execution_identity: [u8; 32],
        source_artifact: Arc<dyn Any + Send + Sync>,
        execution: QueryExecutionContext,
    ) -> Self {
        Self {
            execution_identity,
            executed: AtomicBool::new(false),
            source_artifact,
            retained_execution: Mutex::new(Some(execution)),
        }
    }

    pub(crate) const fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }

    pub(crate) fn source_artifact(&self) -> &(dyn Any + Send + Sync) {
        self.source_artifact.as_ref()
    }

    pub(crate) fn execute_once<T>(
        &self,
        expected_identity: [u8; 32],
        execute: impl FnOnce(&(dyn Any + Send + Sync), QueryExecutionContext) -> Result<T, String>,
    ) -> Result<T, String> {
        if expected_identity != self.execution_identity {
            return Err("CTAS prepared write execution identity mismatch".to_string());
        }
        if self
            .executed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("CTAS prepared source has already been executed".to_string());
        }
        let execution = self
            .retained_execution
            .lock()
            .map_err(|error| format!("CTAS execution context lock: {error}"))?
            .take()
            .ok_or_else(|| "CTAS admitted execution context was already consumed".to_string())?;
        execute(self.source_artifact(), execution)
    }
}

pub(crate) fn mutation_failure(failure: ConnectorMutationFailure) -> CtasFailure {
    let kind = match failure.kind() {
        novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest => {
            CtasFailureKind::InvalidRequest
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::NotFound => {
            CtasFailureKind::NotFound
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists => {
            CtasFailureKind::AlreadyExists
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Conflict => {
            CtasFailureKind::Conflict
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported => {
            CtasFailureKind::Unsupported
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled => {
            CtasFailureKind::Cancelled
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded => {
            CtasFailureKind::DeadlineExceeded
        }
        novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable => {
            CtasFailureKind::Unavailable
        }
        _ => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: failure.message().to_string(),
    }
}

fn connector_failure(error: novarocks_spi::connector::ConnectorError) -> CtasFailure {
    use novarocks_spi::connector::ConnectorErrorKind;
    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => CtasFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => CtasFailureKind::NotFound,
        ConnectorErrorKind::Unsupported => CtasFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => CtasFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => CtasFailureKind::DeadlineExceeded,
        ConnectorErrorKind::Unavailable => CtasFailureKind::Unavailable,
        ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::ResourceExhausted
        | ConnectorErrorKind::CorruptData
        | ConnectorErrorKind::Internal => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: error.to_string(),
    }
}

fn internal_failure(message: impl Into<String>) -> CtasFailure {
    CtasFailure {
        kind: CtasFailureKind::Internal,
        message: message.into(),
    }
}

struct CorePreparedCtasSource {
    gate: Arc<CtasSourceExecutionGate>,
    command: CtasCommand,
    target: crate::engine::backend_resolver::TargetBackend,
    current_catalog: Option<String>,
    current_database: String,
    query_options: Option<QueryOptions>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    output_schema: arrow::datatypes::SchemaRef,
    output_columns: Vec<ConnectorColumnDefinition>,
    target_session: Mutex<Option<Arc<CoreCtasTargetSession>>>,
    target_prepare_started: AtomicBool,
}

impl CtasPreparedSource for CorePreparedCtasSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.gate.execution_identity()
    }
}

struct CorePreparedCtasWrite {
    state: Arc<crate::engine::StandaloneState>,
    gate: Arc<CtasSourceExecutionGate>,
    target: Arc<CoreCtasTargetSession>,
    prepared:
        Mutex<Option<crate::query_execution::prepared_write::PreparedDistributedWriteRequest>>,
    completion: Mutex<Option<crate::query_execution::ConnectorWriteCompletion>>,
    write_session:
        Mutex<Option<crate::query_execution::write_operation::ConnectorWriteOperationSession>>,
    write_unknown: Mutex<Option<ExternalMutationEvidence>>,
    execution_identity: [u8; 32],
}

impl CtasPreparedWrite for CorePreparedCtasWrite {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }
}

fn downcast_source(
    source: &dyn CtasPreparedSource,
) -> Result<&CorePreparedCtasSource, CtasFailure> {
    source
        .as_any()
        .downcast_ref::<CorePreparedCtasSource>()
        .ok_or_else(|| internal_failure("CTAS source handle does not belong to the core engine"))
}

fn downcast_target(target: &dyn CtasPreparedTarget) -> Result<&CoreCtasTargetSession, CtasFailure> {
    target
        .as_any()
        .downcast_ref::<CoreCtasTargetSession>()
        .ok_or_else(|| internal_failure("CTAS target handle does not belong to the core engine"))
}

fn downcast_write(write: &dyn CtasPreparedWrite) -> Result<&CorePreparedCtasWrite, CtasFailure> {
    write
        .as_any()
        .downcast_ref::<CorePreparedCtasWrite>()
        .ok_or_else(|| internal_failure("CTAS write handle does not belong to the core engine"))
}

fn target_facts(target: &CoreCtasTargetSession) -> CtasTargetFacts {
    CtasTargetFacts {
        provider_id: "iceberg".to_string(),
        instance_id: target.owner().instance_id.as_str().to_string(),
        incarnation: target.owner().incarnation.to_bytes(),
        operation_id: target.operation_id().to_bytes(),
        handle_digest: target.handle_digest(),
    }
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.finalize().into()
}

fn optimized_capture_fingerprint(node: &crate::sql::optimizer::OptimizedOperatorNode) -> [u8; 32] {
    // This is a versioned capture fingerprint of the exact in-memory optimized
    // artifact, not a cross-release canonical serialization or replay format.
    // The complete Debug payload is intentional here: it includes every
    // operator field, scalar-arena node and execution property retained by the
    // optimizer. Hashing only variant/tree shape would allow different
    // predicates, join keys or distributions to share a CTAS plan identity.
    let material = format!("{node:#?}");
    sha256(&[b"novarocks.ctas-optimized-capture.v1", material.as_bytes()])
}

fn write_staging_evidence(
    session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
) -> ExternalMutationEvidence {
    let mut payload = Vec::with_capacity(8 + 16 + 16 + 32);
    payload.extend_from_slice(b"CTASWS1\0");
    payload.extend_from_slice(&session.operation_id().to_bytes());
    payload.extend_from_slice(&session.owner().incarnation.to_bytes());
    payload.extend_from_slice(&session.sealed().digest());
    ExternalMutationEvidence::try_new(
        1,
        novarocks_spi::connector::ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static Iceberg provider ID is valid"),
            instance_id: session.owner().instance_id.clone(),
        },
        session.owner().incarnation,
        novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
            session.operation_id().to_bytes(),
        ),
        "ctas-write-staging",
        Bytes::from(payload),
    )
    .expect("bounded CTAS writer evidence is valid")
}

fn write_commit_unknown(
    prepared: &CorePreparedCtasWrite,
    session: &crate::query_execution::write_operation::ConnectorWriteOperationSession,
    message: impl Into<String>,
) -> CtasWriteOutcome {
    let mut failure_message = message.into();
    let evidence = {
        let mut stored = prepared
            .write_unknown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stored.is_none() {
            let evidence = write_staging_evidence(session);
            if let Err(error) = prepared.target.mark_write_unknown() {
                failure_message.push_str(&format!(
                    "; retained staged-create lease also rejected write-unknown transition: {error}"
                ));
            }
            *stored = Some(evidence);
        }
        stored
            .as_ref()
            .expect("CTAS evidence was installed")
            .clone()
    };
    CtasWriteOutcome::CommitUnknown {
        failure: CtasFailure {
            kind: CtasFailureKind::Unavailable,
            message: failure_message,
        },
        evidence,
    }
}

impl CtasEngine for Arc<crate::engine::StandaloneState> {
    fn classify_ctas(&self, sql: &str) -> Result<Option<CtasCommand>, String> {
        use crate::sql::parser::ast::CreateTableKind;
        use crate::sql::parser::dialect::{
            StarRocksDialect, create_table::parse_create_table_statement, looks_like_create_table,
        };
        use sqlparser::keywords::Keyword;
        use sqlparser::tokenizer::Token;

        // Classification must stay inert for ordinary CREATE TABLE. Its
        // established DDL parser accepts nested complex types such as
        // ARRAY<STRUCT<...>>, while sqlparser tokenizes the closing `>>` as a
        // shift token before the StarRocks create-table adapter can split it.
        // Only invoke that adapter after a token-level scan proves that this
        // statement has a top-level AS clause and therefore belongs to CTAS.
        let mut classifier = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(sql)
            .map_err(|error| error.to_string())?;
        if !looks_like_create_table(&classifier) {
            return Ok(None);
        }
        let mut depth = 0_u32;
        let is_ctas = loop {
            match classifier.next_token().token {
                Token::LParen => depth = depth.saturating_add(1),
                Token::RParen => depth = depth.saturating_sub(1),
                Token::Word(word) if depth == 0 && word.keyword == Keyword::AS => break true,
                Token::EOF | Token::SemiColon => break false,
                _ => {}
            }
        };
        if !is_ctas {
            return Ok(None);
        }

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(sql)
            .map_err(|error| error.to_string())?;
        let statement = parse_create_table_statement(&mut parser)?;
        let Some(source) = statement.as_select else {
            return Ok(None);
        };
        let CreateTableKind::Iceberg {
            partition_fields,
            properties,
            ..
        } = statement.kind;
        let mut normalized_properties: BTreeMap<Arc<str>, Arc<str>> = properties
            .into_iter()
            .filter(|(key, _)| {
                !key.eq_ignore_ascii_case("format-version")
                    && !key.eq_ignore_ascii_case("write.row-lineage")
            })
            .map(|(key, value)| (Arc::from(key), Arc::from(value)))
            .collect();
        normalized_properties.insert(Arc::from("format-version"), Arc::from("3"));
        normalized_properties.insert(Arc::from("write.row-lineage"), Arc::from("true"));
        Ok(Some(CtasCommand {
            target_parts: statement.name.parts,
            if_not_exists: statement.if_not_exists,
            source_sql: source.to_string(),
            partitioning: partition_fields
                .iter()
                .map(crate::engine::statement::connector_partition_transform)
                .collect(),
            properties: normalized_properties,
        }))
    }

    fn precheck_ctas_target(
        &self,
        command: &CtasCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<CtasTargetPrecheck, CtasFailure> {
        let target = crate::engine::backend_resolver::resolve_table_target(
            self,
            &crate::sql::parser::ast::ObjectName {
                parts: command.target_parts.clone(),
            },
            current_catalog,
            current_database,
        )
        .map_err(internal_failure)?;
        let context =
            crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))
                .map_err(internal_failure)?;
        match crate::connector::metadata_table_exists(
            self.connector_control.as_ref(),
            context,
            &target.catalog,
            &target.namespace,
            &target.table,
        ) {
            Ok(true) if command.if_not_exists => Ok(CtasTargetPrecheck::ExistsNoOp),
            Ok(true) => Err(CtasFailure {
                kind: CtasFailureKind::AlreadyExists,
                message: format!("table {}.{} already exists", target.namespace, target.table),
            }),
            Ok(false) => Ok(CtasTargetPrecheck::Absent),
            Err(error) => Err(internal_failure(format!(
                "check CTAS target {}.{}: {error}",
                target.namespace, target.table
            ))),
        }
    }

    fn prepare_ctas_source(
        &self,
        request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure> {
        let target = crate::engine::backend_resolver::resolve_table_target(
            self,
            &crate::sql::parser::ast::ObjectName {
                parts: request.command.target_parts.clone(),
            },
            request.current_catalog.as_deref(),
            &request.current_database,
        )
        .map_err(internal_failure)?;
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(&request.command.source_sql)
            .map_err(|error| internal_failure(error.to_string()))?;
        let query = parser
            .parse_query()
            .map_err(|error| internal_failure(error.to_string()))?;
        let connector_context = crate::connector::connector_request_context_for_execution(
            request.query_options.as_ref(),
            &request.execution,
        )
        .map_err(internal_failure)?;
        let planned = plan_query_for_ctas_source(
            self,
            request.current_catalog.as_deref(),
            &request.current_database,
            &query,
            &request.execution,
            &connector_context,
        )
        .map_err(internal_failure)?;
        if planned.output_columns.is_empty() {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS source has no output columns".to_string(),
            });
        }
        let output_schema = Arc::new(arrow::datatypes::Schema::new(
            planned
                .output_columns
                .iter()
                .map(|column| {
                    arrow::datatypes::Field::new(
                        &column.name,
                        column.data_type.clone(),
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let table_columns =
            crate::engine::iceberg_ctas::arrow_schema_to_table_column_defs(output_schema.as_ref())
                .map_err(internal_failure)?;
        let output_columns = table_columns
            .iter()
            .map(crate::engine::statement::connector_column)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_failure)?;
        let schema_text = format!("{output_schema:?}");
        let optimized_fingerprint = optimized_capture_fingerprint(&planned.optimized_tree);
        let settings_material = planned.optimizer_settings.stable_digest_material();
        let binding_material = planned.table_bindings.stable_digest_material();
        let execution_nonce = uuid::Uuid::now_v7();
        let execution_identity =
            sha256(&[b"novarocks.ctas-execution.v1", execution_nonce.as_bytes()]);
        let plan_digest = sha256(&[
            b"novarocks.ctas-plan.v1",
            request.command.source_sql.as_bytes(),
            optimized_fingerprint.as_slice(),
            settings_material.as_slice(),
            binding_material.as_slice(),
        ]);
        let schema_digest = sha256(&[schema_text.as_bytes()]);
        let planned: Arc<PlannedCtasSourceQuery> = Arc::new(planned);
        let gate = Arc::new(CtasSourceExecutionGate::new(
            execution_identity,
            planned,
            request.execution,
        ));
        let facts = CtasPreparedSourceFacts {
            target_catalog: target.catalog.clone(),
            target_namespace: target.namespace.clone(),
            target_table: target.table.clone(),
            source_catalog: request.current_catalog.clone(),
            source_database: request.current_database.clone(),
            plan_digest,
            schema_digest,
            execution_identity,
            output_columns: output_columns.clone(),
        };
        Ok(PreparedCtasSource {
            facts,
            handle: Arc::new(CorePreparedCtasSource {
                gate,
                command: request.command,
                target,
                current_catalog: request.current_catalog,
                current_database: request.current_database,
                query_options: request.query_options,
                connector_context,
                output_schema,
                output_columns,
                target_session: Mutex::new(None),
                target_prepare_started: AtomicBool::new(false),
            }),
        })
    }

    fn prepare_ctas_target(
        &self,
        source: &dyn CtasPreparedSource,
        operation_id: ConnectorStagedCreateOperationId,
        policy: CreatePolicy,
    ) -> Result<CtasTargetPrepareOutcome, CtasFailure> {
        let source = downcast_source(source)?;
        if source
            .target_prepare_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS source target preparation has already been attempted".to_string(),
            });
        }
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&source.target.catalog)
                .map_err(connector_failure)?;
        let planning = self
            .connector_control
            .acquire_current(&instance_id)
            .map_err(connector_failure)?;
        let staged_lease = planning
            .derive_staged_create_lease()
            .map_err(connector_failure)?;
        let write_lease = planning.derive_write_lease().map_err(connector_failure)?;
        let owner = staged_lease.owner().clone();
        let request = ConnectorStagedCreatePrepareRequest {
            owner,
            operation_id,
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(source.target.namespace.as_str()),
                table: Arc::from(source.target.table.as_str()),
            },
            columns: source.output_columns.clone(),
            partitioning: source.command.partitioning.clone(),
            properties: source.command.properties.clone(),
            policy,
            context: source.connector_context.clone(),
        };
        let (target, outcome) = CoreCtasTargetSession::prepare(staged_lease, write_lease, request)
            .map_err(connector_failure)?;
        *source
            .target_session
            .lock()
            .map_err(|error| internal_failure(format!("CTAS target session lock: {error}")))? =
            Some(Arc::clone(&target));
        let prepared_target = || PreparedCtasTarget {
            facts: target_facts(target.as_ref()),
            handle: target.clone(),
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                return Ok(CtasTargetPrepareOutcome::ContractUnknown {
                    target: prepared_target(),
                    failure: connector_failure(error),
                });
            }
        };
        Ok(match outcome {
            ConnectorStagedCreatePrepareOutcome::Prepared {
                receipt,
                finalization,
                ..
            } => CtasTargetPrepareOutcome::Prepared {
                target: prepared_target(),
                receipt,
                finalization,
            },
            ConnectorStagedCreatePrepareOutcome::Conflict { failure } => {
                CtasTargetPrepareOutcome::Conflict {
                    failure: mutation_failure(failure),
                }
            }
            ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } => {
                CtasTargetPrepareOutcome::KnownUncommitted {
                    failure: mutation_failure(failure),
                }
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { failure, evidence } => {
                CtasTargetPrepareOutcome::CommitUnknown {
                    target: prepared_target(),
                    failure: mutation_failure(failure),
                    evidence,
                }
            }
        })
    }

    fn prepare_ctas_write(
        &self,
        source: &dyn CtasPreparedSource,
        target: &dyn CtasPreparedTarget,
        write_operation_id: ConnectorWriteOperationId,
    ) -> Result<PreparedCtasWrite, CtasFailure> {
        let source = downcast_source(source)?;
        let target = downcast_target(target)?;
        if source.gate.execution_identity() != source.execution_identity() {
            return Err(internal_failure("CTAS source execution identity drift"));
        }
        let target_arc = source
            .target_session
            .lock()
            .map_err(|error| internal_failure(format!("CTAS target session lock: {error}")))?
            .clone()
            .ok_or_else(|| internal_failure("CTAS source has no retained target session"))?;
        if !std::ptr::eq(target_arc.as_ref(), target) {
            return Err(internal_failure(
                "CTAS target does not match the source-retained exact session",
            ));
        }
        let template = target
            .plan_write(
                write_operation_id,
                Arc::clone(&source.output_schema),
                source.connector_context.clone(),
            )
            .map_err(connector_failure)?;
        let planned = source
            .gate
            .source_artifact()
            .downcast_ref::<PlannedCtasSourceQuery>()
            .ok_or_else(|| internal_failure("CTAS retained source artifact type mismatch"))?;
        let prepared = prepare_planned_ctas_connector_write(
            self,
            planned,
            Arc::clone(&source.output_schema),
            source.query_options.clone(),
            &source.connector_context,
            template,
        )
        .map_err(internal_failure)?;
        let target = target_arc;
        let facts = target_facts(target.as_ref());
        let identity = source.gate.execution_identity();
        Ok(PreparedCtasWrite {
            target_facts: facts,
            write_operation_id,
            execution_identity: identity,
            handle: Arc::new(CorePreparedCtasWrite {
                state: Arc::clone(self),
                gate: Arc::clone(&source.gate),
                target,
                prepared: Mutex::new(Some(prepared)),
                completion: Mutex::new(None),
                write_session: Mutex::new(None),
                write_unknown: Mutex::new(None),
                execution_identity: identity,
            }),
        })
    }

    fn execute_ctas_write(&self, prepared: &dyn CtasPreparedWrite) -> CtasWriteOutcome {
        let prepared = match downcast_write(prepared) {
            Ok(prepared) => prepared,
            Err(failure) => return CtasWriteOutcome::KnownUncommitted { failure },
        };
        let result = prepared
            .gate
            .execute_once(prepared.execution_identity, |_, execution| {
                let request = prepared
                    .prepared
                    .lock()
                    .map_err(|error| format!("CTAS prepared write lock: {error}"))?
                    .take()
                    .ok_or_else(|| "CTAS prepared write was already consumed".to_string())?;
                let cohort_id = request.write_cohort_id();
                let exact_lease = request.lease();
                let session = prepared
                    .state
                    .query_execution
                    .begin_write_operation(request.registration(), exact_lease)
                    .map_err(|error| error.to_string())?;
                *prepared
                    .write_session
                    .lock()
                    .map_err(|error| format!("CTAS write session lock: {error}"))? =
                    Some(session.clone());
                let registration =
                    crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                        session.clone(),
                        cohort_id,
                    )
                    .map_err(|error| error.to_string())?;
                let request = request
                    .into_request(&execution, registration)
                    .map_err(|error| error.to_string())?;
                let outcome = match prepared.state.query_execution.execute(request) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        return Ok(write_commit_unknown(
                            prepared,
                            &session,
                            format!("CTAS writer dispatch outcome is unknown: {error}"),
                        ));
                    }
                };
                let write = match outcome.into_write() {
                    Ok(write) => write,
                    Err(error) => {
                        return Ok(write_commit_unknown(
                            prepared,
                            &session,
                            format!("CTAS writer terminal outcome is unknown: {error}"),
                        ));
                    }
                };
                let (_, _, _, completion) = write.into_parts_with_connector();
                let Some(completion) = completion else {
                    return Ok(write_commit_unknown(
                        prepared,
                        &session,
                        "CTAS writer returned no complete generic completion",
                    ));
                };
                let sealed = match completion.sealed_operation_completion() {
                    Ok(sealed) => sealed,
                    Err(error) => {
                        return Ok(write_commit_unknown(
                            prepared,
                            &session,
                            format!("CTAS writer aggregate is incomplete: {error}"),
                        ));
                    }
                };
                if let Err(error) = prepared.target.bind_write(sealed.clone()) {
                    return Ok(write_commit_unknown(
                        prepared,
                        &session,
                        format!("CTAS target write binding outcome is unknown: {error}"),
                    ));
                }
                *prepared
                    .completion
                    .lock()
                    .map_err(|error| format!("CTAS completion lock: {error}"))? = Some(completion);
                Ok(CtasWriteOutcome::Completed {
                    completion: sealed,
                    execution_identity: prepared.execution_identity,
                })
            });
        match result {
            Ok(outcome) => outcome,
            Err(message) => CtasWriteOutcome::KnownUncommitted {
                failure: internal_failure(message),
            },
        }
    }

    fn reconcile_ctas_write(
        &self,
        prepared: &dyn CtasPreparedWrite,
        evidence: ExternalMutationEvidence,
    ) -> CtasWriteOutcome {
        let prepared = match downcast_write(prepared) {
            Ok(prepared) => prepared,
            Err(failure) => return CtasWriteOutcome::KnownUncommitted { failure },
        };
        let stored_evidence = match prepared.write_unknown.lock() {
            Ok(evidence) => evidence.clone(),
            Err(error) => {
                return CtasWriteOutcome::CommitUnknown {
                    failure: internal_failure(format!("CTAS write evidence lock: {error}")),
                    evidence,
                };
            }
        };
        let Some(stored_evidence) = stored_evidence else {
            return CtasWriteOutcome::KnownUncommitted {
                failure: CtasFailure {
                    kind: CtasFailureKind::InvalidRequest,
                    message: "CTAS writer has no prior CommitUnknown decision to reconcile"
                        .to_string(),
                },
            };
        };
        if evidence != stored_evidence {
            return CtasWriteOutcome::CommitUnknown {
                failure: CtasFailure {
                    kind: CtasFailureKind::InvalidRequest,
                    message: "CTAS writer reconcile evidence does not match the exact unresolved operation"
                        .to_string(),
                },
                evidence: stored_evidence,
            };
        }
        let session = match prepared.write_session.lock() {
            Ok(session) => session.clone(),
            Err(error) => {
                return CtasWriteOutcome::CommitUnknown {
                    failure: internal_failure(format!("CTAS write session lock: {error}")),
                    evidence: stored_evidence,
                };
            }
        };
        let Some(session) = session else {
            return CtasWriteOutcome::CommitUnknown {
                failure: internal_failure("CTAS unresolved writer lost its exact retained session"),
                evidence: stored_evidence,
            };
        };
        let expected = write_staging_evidence(&session);
        if expected != stored_evidence {
            return CtasWriteOutcome::CommitUnknown {
                failure: internal_failure(
                    "CTAS stored writer evidence failed exact session validation",
                ),
                evidence: stored_evidence,
            };
        }
        let completion = match session.sealed_operation_completion() {
            Ok(completion) => completion,
            Err(error) => {
                return CtasWriteOutcome::CommitUnknown {
                    failure: CtasFailure {
                        kind: CtasFailureKind::Unavailable,
                        message: format!("CTAS writer is still incomplete: {error}"),
                    },
                    evidence: stored_evidence,
                };
            }
        };
        if let Err(error) = prepared
            .target
            .reconcile_write_completion(completion.clone())
        {
            return CtasWriteOutcome::CommitUnknown {
                failure: connector_failure(error),
                evidence: stored_evidence,
            };
        }
        CtasWriteOutcome::Completed {
            completion,
            execution_identity: prepared.execution_identity,
        }
    }

    fn publish_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<ConnectorStagedCreatePublishOutcome, CtasFailure> {
        let target = downcast_target(target)?;
        target
            .publish(operation_id, completion, target.context())
            .map_err(connector_failure)
    }

    fn abort_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        completion: Option<ConnectorWriteOperationCompletion>,
    ) -> Result<ConnectorStagedCreateAbortOutcome, CtasFailure> {
        let target = downcast_target(target)?;
        target
            .abort(operation_id, completion, target.context())
            .map_err(connector_failure)
    }

    fn reconcile_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        phase: ConnectorStagedCreateReconcilePhase,
        evidence: ExternalMutationEvidence,
    ) -> Result<ConnectorStagedCreateReconcileOutcome, CtasFailure> {
        let target = downcast_target(target)?;
        target
            .reconcile(phase, evidence, target.context())
            .map_err(connector_failure)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::common::app_config::ClusterRole;
    use crate::query_execution::backend::BackendTopologySnapshot;
    use crate::query_execution::backend::LiveBackendTarget;
    use crate::query_execution::cancellation::QueryCancellationSource;
    use crate::sql::optimizer::options::SessionOptimizerSettings;
    use bytes::Bytes;
    use novarocks_spi::connector::*;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn connector_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    fn execution() -> QueryExecutionContext {
        QueryExecutionContext::new(
            ClusterRole::AllInOne,
            BackendTopologySnapshot::empty(0),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
        )
    }

    fn fe_execution(settings: SessionOptimizerSettings) -> QueryExecutionContext {
        let topology = BackendTopologySnapshot::try_new(
            19,
            vec![LiveBackendTarget::new(
                7,
                "127.0.0.1:19048".parse().expect("backend endpoint"),
                41,
            )],
        )
        .expect("frozen topology");
        QueryExecutionContext::new(
            ClusterRole::Fe,
            topology,
            None,
            QueryCancellationSource::new().view(),
            settings,
        )
    }

    fn planned_source(sql: &str) -> PlannedCtasSourceQuery {
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let query = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("parser init")
            .parse_query()
            .expect("source query");
        let execution = fe_execution(SessionOptimizerSettings::default());
        plan_query_for_ctas_source(
            &Arc::new(crate::engine::StandaloneState::default()),
            None,
            "analytics",
            &query,
            &execution,
            &connector_context(),
        )
        .expect("plan CTAS source")
    }

    #[test]
    fn classifier_preserves_ifne_partitioning_and_properties() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        let command = CtasEngine::classify_ctas(
            &state,
            "CREATE TABLE IF NOT EXISTS ice.sales.dst PARTITION BY (region) \
             TBLPROPERTIES('owner'='dml3') AS SELECT 1 AS region",
        )
        .expect("classify CTAS")
        .expect("CTAS command");

        assert_eq!(command.target_parts, ["ice", "sales", "dst"]);
        assert!(command.if_not_exists);
        assert_eq!(command.partitioning.len(), 1);
        assert_eq!(
            command.properties.get("owner").map(AsRef::as_ref),
            Some("dml3")
        );
        assert_eq!(
            command.properties.get("format-version").map(AsRef::as_ref),
            Some("3")
        );
        assert_eq!(
            command
                .properties
                .get("write.row-lineage")
                .map(AsRef::as_ref),
            Some("true")
        );
        assert_eq!(command.source_sql, "SELECT 1 AS region");
    }

    #[test]
    fn classifier_leaves_nested_complex_type_create_table_to_ddl() {
        let state = Arc::new(crate::engine::StandaloneState::default());
        let command = CtasEngine::classify_ctas(
            &state,
            "CREATE TABLE ice.sales.nested (\
                 items ARRAY<STRUCT<id INT, labels ARRAY<STRING>>>\
             ) COMMENT 'AS SELECT is text, not a CTAS clause'",
        )
        .expect("ordinary CREATE TABLE classification is inert");

        assert!(command.is_none());
    }

    fn same_operator_shape(
        left: &crate::sql::optimizer::OptimizedOperatorNode,
        right: &crate::sql::optimizer::OptimizedOperatorNode,
    ) -> bool {
        std::mem::discriminant(&left.op) == std::mem::discriminant(&right.op)
            && left.children.len() == right.children.len()
            && left
                .children
                .iter()
                .zip(&right.children)
                .all(|(left, right)| same_operator_shape(left, right))
    }

    fn set_first_join_distribution(
        node: &mut crate::sql::optimizer::OptimizedOperatorNode,
        distribution: crate::sql::optimizer::optimized_tree::JoinExecutionDistribution,
    ) -> bool {
        if matches!(
            node.op,
            crate::sql::optimizer::Operator::PhysicalHashJoin(_)
                | crate::sql::optimizer::Operator::PhysicalNestLoopJoin(_)
        ) {
            node.execution_props.join_distribution = Some(distribution);
            return true;
        }
        node.children
            .iter_mut()
            .any(|child| set_first_join_distribution(child, distribution))
    }

    #[test]
    fn optimized_capture_fingerprint_distinguishes_same_shape_predicates_and_join_keys() {
        let predicate_left = planned_source("SELECT x FROM (VALUES (1), (2)) AS v(x) WHERE x > 0");
        let predicate_right = planned_source("SELECT x FROM (VALUES (1), (2)) AS v(x) WHERE x > 1");
        assert!(same_operator_shape(
            &predicate_left.optimized_tree,
            &predicate_right.optimized_tree
        ));
        assert_ne!(
            optimized_capture_fingerprint(&predicate_left.optimized_tree),
            optimized_capture_fingerprint(&predicate_right.optimized_tree),
            "same-shape predicates must not share a CTAS plan fingerprint"
        );

        let join_left = planned_source(
            "SELECT a.x FROM (VALUES (1, 2)) AS a(x, y) JOIN (VALUES (1, 2)) AS b(x, y) ON a.x = b.x",
        );
        let join_right = planned_source(
            "SELECT a.x FROM (VALUES (1, 2)) AS a(x, y) JOIN (VALUES (1, 2)) AS b(x, y) ON a.y = b.y",
        );
        assert!(same_operator_shape(
            &join_left.optimized_tree,
            &join_right.optimized_tree
        ));
        assert_ne!(
            optimized_capture_fingerprint(&join_left.optimized_tree),
            optimized_capture_fingerprint(&join_right.optimized_tree),
            "same-shape join keys must not share a CTAS plan fingerprint"
        );
    }

    #[test]
    fn optimized_capture_fingerprint_includes_join_distribution() {
        use crate::sql::optimizer::optimized_tree::JoinExecutionDistribution;

        let planned = planned_source(
            "SELECT a.x FROM (VALUES (1)) AS a(x) JOIN (VALUES (1)) AS b(x) ON a.x = b.x",
        );
        let mut broadcast = planned.optimized_tree.clone();
        let mut partitioned = planned.optimized_tree;
        assert!(set_first_join_distribution(
            &mut broadcast,
            JoinExecutionDistribution::Broadcast
        ));
        assert!(set_first_join_distribution(
            &mut partitioned,
            JoinExecutionDistribution::Partitioned
        ));
        assert!(same_operator_shape(&broadcast, &partitioned));
        assert_ne!(
            optimized_capture_fingerprint(&broadcast),
            optimized_capture_fingerprint(&partitioned),
            "join distribution must participate in the CTAS plan fingerprint"
        );
    }

    #[test]
    fn prepared_source_is_inert_and_executes_same_artifact_exactly_once() {
        let prepare_count = AtomicUsize::new(0);
        let execute_count = AtomicUsize::new(0);
        prepare_count.fetch_add(1, Ordering::SeqCst);
        let artifact: Arc<dyn Any + Send + Sync> = Arc::new("optimized-source".to_string());
        let identity = [7; 32];
        let gate = CtasSourceExecutionGate::new(identity, artifact, execution());
        assert_eq!(prepare_count.load(Ordering::SeqCst), 1);
        assert_eq!(execute_count.load(Ordering::SeqCst), 0);
        let observed = gate
            .execute_once(identity, |artifact, _| {
                execute_count.fetch_add(1, Ordering::SeqCst);
                Ok(artifact.downcast_ref::<String>().unwrap().clone())
            })
            .unwrap();
        assert_eq!(observed, "optimized-source");
        assert_eq!(execute_count.load(Ordering::SeqCst), 1);
        assert!(gate.execute_once(identity, |_, _| Ok(())).is_err());
        assert_eq!(execute_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn prepared_source_rejects_different_execution_identity_before_dispatch() {
        let execute_count = AtomicUsize::new(0);
        let gate = CtasSourceExecutionGate::new([3; 32], Arc::new(()), execution());
        assert!(
            gate.execute_once([4; 32], |_, _| {
                execute_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .is_err()
        );
        assert_eq!(execute_count.load(Ordering::SeqCst), 0);
        gate.execute_once([3; 32], |_, _| {
            execute_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        assert_eq!(execute_count.load(Ordering::SeqCst), 1);
    }

    struct RecoveringCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        reconciles: AtomicUsize,
        publishes: AtomicUsize,
        aborts: AtomicUsize,
    }

    impl RecoveringCapability {
        fn owner(&self) -> ConnectorExecutionBindingKey {
            ConnectorExecutionBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            }
        }

        fn receipt(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
            phase: ConnectorStagedCreateReceiptPhase,
        ) -> ConnectorStagedCreateReceipt {
            ConnectorStagedCreateReceipt::try_new(
                self.owner(),
                operation_id,
                phase,
                ExternalMutationEffect::Applied,
                Bytes::new(),
            )
            .unwrap()
        }

        fn evidence(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
        ) -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                self.descriptor.clone(),
                self.incarnation,
                operation_id,
                "staged-create-prepare",
                Bytes::from_static(b"prepare-unknown"),
            )
            .unwrap()
        }
    }

    impl ConnectorStagedCreate for RecoveringCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }
        fn prepare(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "unknown",
                ),
                evidence: self.evidence(request.operation_id),
            })
        }
        fn publish(
            &self,
            request: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            Ok(ConnectorStagedCreatePublishOutcome::Applied {
                receipt: self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Published,
                ),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn plan_write(
            &self,
            request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            let table = ConnectorTableHandle::try_new(
                request.handle.owner().instance_id.clone(),
                Bytes::new(),
            )?;
            ConnectorStagedWritePlanningBinding::try_new(
                &request.handle,
                request.operation_id,
                request.intent,
                request.input_schema,
                table,
                Bytes::new(),
                request.context,
            )
        }
        fn abort(
            &self,
            request: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(ConnectorStagedCreateAbortOutcome::Aborted {
                receipt: self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Aborted,
                ),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn reconcile(
            &self,
            request: ConnectorStagedCreateReconcileRequest,
        ) -> Result<ConnectorStagedCreateReconcileOutcome, ConnectorError> {
            self.reconciles.fetch_add(1, Ordering::SeqCst);
            let operation_id = request.evidence.operation_id();
            Ok(ConnectorStagedCreateReconcileOutcome::Prepared {
                handle: ConnectorStagedTableHandle::try_new(
                    self.owner(),
                    operation_id,
                    Bytes::from_static(b"recovered-handle"),
                )?,
                receipt: self.receipt(operation_id, ConnectorStagedCreateReceiptPhase::Prepared),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    struct NoopWriteControl {
        owner: ConnectorExecutionBindingKey,
    }

    impl ConnectorWriteControl for NoopWriteControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.owner
        }

        fn plan_write(
            &self,
            _: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            unreachable!("write planning is not used by this target-session test")
        }

        fn commit(
            &self,
            _: ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!("ordinary write commit is forbidden for CTAS")
        }

        fn abort(
            &self,
            _: ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            unreachable!("writer abort is not used by this target-session test")
        }

        fn reconcile(
            &self,
            _: ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!("write reconcile is not used by this target-session test")
        }
    }

    fn completion(owner: ConnectorExecutionBindingKey) -> ConnectorWriteOperationCompletion {
        let operation_id = ConnectorWriteOperationId::new();
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([11; 16], 1);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [12; 16],
            1,
            0,
            0,
            owner.clone(),
        );
        let report = ConnectorStagedReport::try_new(
            writer,
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"report"),
        )
        .unwrap();
        let accepted = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            execution_id,
            [13; 32],
            vec![report],
            Bytes::new(),
        )
        .unwrap();
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                [14; 32],
            )],
        )
        .unwrap();
        ConnectorWriteOperationCompletion::try_new(
            owner,
            sealed,
            vec![
                ConnectorWriteCohortCompletion::try_new(cohort_id, Some(accepted), vec![]).unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn prepare_unknown_reconcile_prepared_retains_same_lease_for_publish() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let capability = Arc::new(RecoveringCapability {
            descriptor: descriptor.clone(),
            incarnation,
            reconciles: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
        });
        let lease =
            ConnectorStagedCreateLease::new(capability.owner(), capability.clone(), || {}).unwrap();
        let operation_id = ConnectorStagedCreateOperationId::new();
        let request = ConnectorStagedCreatePrepareRequest {
            owner: capability.owner(),
            operation_id,
            table: ConnectorTableIdentity {
                instance_id: descriptor.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            columns: vec![],
            partitioning: vec![],
            properties: Default::default(),
            policy: CreatePolicy::FailIfExists,
            context: connector_context(),
        };
        let write_lease = ConnectorWriteLease::new(
            capability.owner(),
            Arc::new(NoopWriteControl {
                owner: capability.owner(),
            }),
            || {},
        )
        .unwrap();
        let (session, outcome) =
            CoreCtasTargetSession::prepare(lease, write_lease, request).unwrap();
        let outcome = outcome.unwrap();
        let ConnectorStagedCreatePrepareOutcome::CommitUnknown { evidence, .. } = outcome else {
            panic!("expected unknown")
        };
        assert!(
            session
                .abort(
                    ConnectorMutationOperationId::new(),
                    None,
                    connector_context()
                )
                .is_err(),
            "unknown must forbid abort"
        );
        let outcome = session
            .reconcile(
                ConnectorStagedCreateReconcilePhase::Prepare,
                evidence,
                connector_context(),
            )
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreateReconcileOutcome::Prepared { .. }
        ));
        assert!(session.handle_digest().is_some());
        let completion = completion(capability.owner());
        session.bind_write(completion.clone()).unwrap();
        session
            .publish(
                ConnectorMutationOperationId::new(),
                completion,
                connector_context(),
            )
            .unwrap();
        assert_eq!(capability.reconciles.load(Ordering::SeqCst), 1);
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 1);
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 0);
    }
}
