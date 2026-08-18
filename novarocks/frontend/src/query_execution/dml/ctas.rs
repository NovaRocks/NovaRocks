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
    ConnectorColumnDefinition, ConnectorCtasAbortRequest, ConnectorCtasAbortResult,
    ConnectorCtasActionId, ConnectorCtasAdvanceFenceRequest, ConnectorCtasFailure,
    ConnectorCtasOperationId, ConnectorCtasPublicationFence, ConnectorCtasPublicationFenceReceipt,
    ConnectorCtasPublicationProof, ConnectorCtasPublishRequest, ConnectorCtasPublishResult,
    ConnectorCtasStageRequest, ConnectorCtasStagedLocator, ConnectorCtasStagedPublicationLease,
    ConnectorCtasStagedTableDefinition, ConnectorHistoricalCtasCleanupReceipt,
    ConnectorHistoricalCtasCleanupRequest, ConnectorHistoricalCtasDescriptor,
    ConnectorHistoricalCtasObservation, ConnectorMutationFailure, ConnectorPartitionTransform,
    ConnectorStagedTableHandle, ConnectorStagedWritePlanningRequest,
    ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationCompletion,
    ConnectorWriteOperationId, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    CreatePolicy, ExternalMutationEvidence,
};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::kernels::DmlExecutionKernel;
use novarocks_protocol::lifecycle::QueryOptions;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasCommand {
    pub target_parts: Vec<String>,
    pub if_not_exists: bool,
    pub source_sql: String,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
}

pub enum CtasTargetPreflightOutcome {
    Ready(PreparedCtasTargetPreflight),
    ExistsNoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CtasTargetPreflightFacts {
    pub provider_id: String,
    pub instance_id: String,
    pub incarnation: [u8; 16],
    pub capability_version: u32,
    pub target_namespace: String,
    pub target_table: String,
}

pub trait CtasPreparedTargetPreflight: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasTargetPreflight {
    pub facts: CtasTargetPreflightFacts,
    pub handle: Arc<dyn CtasPreparedTargetPreflight>,
}

pub trait CtasPreparedCatalogAction: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasCatalogAction {
    pub input_digest: [u8; 32],
    pub handle: Arc<dyn CtasPreparedCatalogAction>,
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
    pub fence_digest: [u8; 32],
    pub locator_digest: [u8; 32],
}

/// Opaque target session. A concrete core implementation retains the same
/// exact fenced-publication lease, fence, ordinary writer handle, opaque
/// locator and proof for foreground stage, publish and abort.
pub trait CtasPreparedTarget: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct PreparedCtasTarget {
    pub facts: CtasTargetFacts,
    pub handle: Arc<dyn CtasPreparedTarget>,
}

pub struct CtasTargetStageResult {
    pub target: PreparedCtasTarget,
    pub locator: ConnectorCtasStagedLocator,
    pub receipt: novarocks_spi::connector::ConnectorCtasPublicationReceipt,
    pub proof: ConnectorCtasPublicationProof,
}

pub trait CtasPreparedWrite: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn execution_identity(&self) -> [u8; 32];
    fn native_encoding(&self) -> Result<CtasNativeEncoding<'_>, CtasFailure>;
}

/// Borrowed access to the exact Core-retained encoding input. Frontend may
/// inspect it only for the native encoder call; Core consumes the same input
/// when the resulting bundle is bound for dispatch.
pub struct CtasNativeEncoding<'a> {
    encoding: std::sync::MutexGuard<
        'a,
        Option<crate::query_execution::compiler::NativeFragmentEncodingInput>,
    >,
}

impl CtasNativeEncoding<'_> {
    pub fn input(
        &self,
    ) -> Result<&crate::query_execution::compiler::NativeFragmentEncodingInput, CtasFailure> {
        self.encoding
            .as_ref()
            .ok_or_else(|| internal_failure("CTAS native encoding input was already consumed"))
    }
}

pub struct PreparedCtasWrite {
    pub target_facts: CtasTargetFacts,
    pub write_operation_id: ConnectorWriteOperationId,
    /// Digest of the exact provider-signed writer cohort set. Frontend keeps
    /// this neutral value before dispatch so a later generation can build the
    /// CP-3B historical descriptor without decoding opaque evidence.
    pub cohort_set_digest: [u8; 32],
    pub execution_identity: [u8; 32],
    pub handle: Arc<dyn CtasPreparedWrite>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CtasWriteOutcome {
    Completed {
        completion: ConnectorWriteOperationCompletion,
        execution_identity: [u8; 32],
        established_fence: Option<novarocks_spi::connector::ConnectorEstablishedWriteFence>,
    },
    KnownUncommitted {
        failure: CtasFailure,
    },
    CommitUnknown {
        failure: CtasFailure,
        evidence: ExternalMutationEvidence,
        established_fence: Option<novarocks_spi::connector::ConnectorEstablishedWriteFence>,
    },
}

/// One-to-one core capability consumed by the frontend CTAS application
/// owner. It is intentionally not a generic connector DML facade.
pub trait CtasEngine: Send + Sync {
    fn classify_ctas(&self, sql: &str) -> Result<Option<CtasCommand>, String>;

    /// Resolve the exact target and retain the current fenced-publication
    /// generation before source preparation. Unsupported catalogs fail here;
    /// Core never falls back to ordinary staged create.
    fn preflight_ctas_target(
        &self,
        command: &CtasCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<CtasTargetPreflightOutcome, CtasFailure>;

    /// Pure analyze/optimize. It must not execute the source, reserve a
    /// writer, prepare a staged table, or consult a new live topology.
    fn prepare_ctas_source(
        &self,
        preflight: &dyn CtasPreparedTargetPreflight,
        request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure>;

    /// Prepare an inert catalog action so Frontend can durably checkpoint the
    /// exact request digest before dispatch.
    fn prepare_ctas_fence_advance(
        &self,
        preflight: &dyn CtasPreparedTargetPreflight,
        fence: ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure>;

    fn advance_ctas_fence(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure>;

    fn prepare_ctas_target(
        &self,
        source: &dyn CtasPreparedSource,
        fence: ConnectorCtasPublicationFence,
        stage_action_id: ConnectorCtasActionId,
        policy: CreatePolicy,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure>;

    fn stage_ctas_target(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<CtasTargetStageResult, ConnectorCtasFailure>;

    /// Bind the same optimized source artifact and admitted execution to the
    /// provider-issued staged table and SPI-4C1 sink. This call is inert.
    fn prepare_ctas_write(
        &self,
        source: &dyn CtasPreparedSource,
        target: &dyn CtasPreparedTarget,
        write_operation_id: ConnectorWriteOperationId,
    ) -> Result<PreparedCtasWrite, CtasFailure>;

    /// Bind the one Frontend-encoded native bundle to the retained CTAS
    /// writer. Core validates the write registration and creates the neutral
    /// distributed request; it never encodes native bytes for this flow.
    fn bind_ctas_write_native_bundle(
        &self,
        prepared: &dyn CtasPreparedWrite,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<(), CtasFailure>;

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

    fn prepare_publish_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        action_id: ConnectorCtasActionId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure>;

    fn publish_ctas(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure>;

    fn prepare_abort_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        action_id: ConnectorCtasActionId,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure>;

    fn abort_ctas(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure>;

    /// Current-generation recovery. No method accepts or reconstructs an old
    /// foreground target session.
    fn inspect_historical_ctas(
        &self,
        descriptor: ConnectorHistoricalCtasDescriptor,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure>;

    fn advance_historical_ctas_fence(
        &self,
        request: ConnectorCtasAdvanceFenceRequest,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure>;

    fn cleanup_historical_ctas(
        &self,
        request: ConnectorHistoricalCtasCleanupRequest,
    ) -> Result<ConnectorHistoricalCtasCleanupReceipt, ConnectorCtasFailure>;
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
    source: novarocks_sql::planning::dml::DmlCtasSourcePlan,
    table_bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings,
    connector_target_parallelism: std::num::NonZeroUsize,
}

#[allow(clippy::too_many_arguments)]
fn plan_query_for_ctas_source(
    state: &DmlExecutionKernel,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedCtasSourceQuery, String> {
    let mut query = query.clone();
    if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut query,
            connector_context,
        )?;
    }
    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(state);
    let analyzer_provider =
        crate::catalog_application::query_materializer::build_catalog_service_provider(
            current_catalog,
            &catalog_service_snapshot,
            state.connector_control().as_ref(),
            connector_context.clone(),
            novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
            state.catalog_application().map(Arc::as_ref),
        );
    let table_bindings = analyzer_provider.query_table_bindings();
    let catalog_snapshot =
        novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| "CTAS requires a frozen non-empty backend topology".to_string())?;
    let request = novarocks_sql::compiler::SqlAnalyzeRequest::new(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        novarocks_sql::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: novarocks_sql::compiler::RootDistributionRequirement::Any,
        },
        novarocks_sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        novarocks_sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        novarocks_sql::compiler::builtin_sql_function_catalog(),
        None,
        novarocks_sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::query_execution::planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let analyzed = novarocks_sql::compiler::SqlCompiler::analyze(request)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
    let statistics =
        crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
            state,
            Arc::clone(&table_bindings),
            connector_context,
        )?;
    let source = novarocks_sql::planning::dml::compile_ctas_source(
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics),
    )?;
    Ok(PlannedCtasSourceQuery {
        source,
        table_bindings,
        optimizer_settings: execution.optimizer_settings().clone(),
        connector_target_parallelism: backend_count,
    })
}

fn prepare_planned_ctas_connector_write(
    state: &DmlExecutionKernel,
    planned: &PlannedCtasSourceQuery,
    input_schema: arrow::datatypes::SchemaRef,
    query_options: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    template: crate::query_execution::contract::ConnectorWritePlanningTemplate,
) -> Result<
    (
        crate::query_execution::compiler::NativeFragmentEncodingInput,
        PendingCtasDistributedWrite,
    ),
    String,
> {
    let distributed = novarocks_sql::planning::dml::build_ctas_connector_write_distributed_plan(
        &planned.source,
        input_schema,
        &planned.optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        state.connector_control().as_ref(),
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
    let cohort_id = template.cohort_id();
    let exact_lease = template.lease();
    Ok((
        crate::query_execution::compiler::NativeFragmentEncodingInput::new(distributed, prepared),
        PendingCtasDistributedWrite {
            query_options,
            registration:
                crate::query_execution::contract::ConnectorWriteOperationRegistration::single(
                    template,
                ),
            cohort_id,
            lease: exact_lease,
        },
    ))
}

#[derive(Clone)]
struct CoreCtasTargetPreflight {
    target: crate::catalog_application::resolver::TargetBackend,
    lease: ConnectorCtasStagedPublicationLease,
    write_lease: ConnectorWriteLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    established_fence: Arc<Mutex<Option<ConnectorCtasPublicationFence>>>,
}

impl CtasPreparedTargetPreflight for CoreCtasTargetPreflight {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

enum CoreCtasCatalogActionKind {
    Advance {
        preflight: CoreCtasTargetPreflight,
        request: ConnectorCtasAdvanceFenceRequest,
    },
    Stage {
        preflight: CoreCtasTargetPreflight,
        request: ConnectorCtasStageRequest,
        target_slot: Arc<Mutex<Option<Arc<CoreCtasTargetSession>>>>,
    },
    Publish {
        lease: ConnectorCtasStagedPublicationLease,
        request: ConnectorCtasPublishRequest,
        state: Arc<Mutex<CoreCtasCatalogState>>,
    },
    Abort {
        lease: ConnectorCtasStagedPublicationLease,
        request: ConnectorCtasAbortRequest,
        state: Arc<Mutex<CoreCtasCatalogState>>,
    },
}

struct CoreCtasCatalogAction {
    kind: CoreCtasCatalogActionKind,
    dispatched: AtomicBool,
}

impl CtasPreparedCatalogAction for CoreCtasCatalogAction {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CoreCtasCatalogAction {
    fn begin_dispatch(&self) -> Result<(), ConnectorCtasFailure> {
        self.dispatched
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                local_ctas_failure(CtasFailure {
                    kind: CtasFailureKind::InvalidRequest,
                    message: "prepared CTAS catalog action has already been dispatched".into(),
                })
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreCtasCatalogState {
    Active,
    NoOpCleanup,
    Published,
    Aborted,
    HistoricalOnly,
}

/// Concrete opaque foreground target. It retains the exact fenced capability,
/// writer lease, established fence, provider locator and stage proof. No
/// method reacquires a current generation.
pub(crate) struct CoreCtasTargetSession {
    lease: ConnectorCtasStagedPublicationLease,
    write_lease: ConnectorWriteLease,
    fence: ConnectorCtasPublicationFence,
    locator: ConnectorCtasStagedLocator,
    proof: ConnectorCtasPublicationProof,
    handle: ConnectorStagedTableHandle,
    policy: CreatePolicy,
    context: novarocks_spi::connector::ConnectorRequestContext,
    write_plan_started: AtomicBool,
    write_unknown_latched: AtomicBool,
    catalog_state: Arc<Mutex<CoreCtasCatalogState>>,
}

impl CtasPreparedTarget for CoreCtasTargetSession {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CoreCtasTargetSession {
    fn from_stage(
        preflight: &CoreCtasTargetPreflight,
        request: &ConnectorCtasStageRequest,
        result: &novarocks_spi::connector::ConnectorCtasStageResult,
    ) -> Arc<Self> {
        // Both invariants were validated before any source work when the
        // preflight derived its exact-generation leases, and the SPI lease has
        // already validated `result` before returning it. Keep post-dispatch
        // session materialization infallible.
        Arc::new(Self {
            lease: preflight.lease.clone(),
            write_lease: preflight.write_lease.clone(),
            fence: request.fence.clone(),
            locator: result.locator.clone(),
            proof: result.proof.clone(),
            handle: result.handle.clone(),
            policy: request.create_policy,
            context: request.context.clone(),
            write_plan_started: AtomicBool::new(false),
            write_unknown_latched: AtomicBool::new(false),
            catalog_state: Arc::new(Mutex::new(CoreCtasCatalogState::Active)),
        })
    }

    pub(crate) fn owner(&self) -> &novarocks_spi::connector::ConnectorExecutionBindingKey {
        self.lease.owner()
    }

    pub(crate) const fn operation_id(&self) -> ConnectorCtasOperationId {
        self.fence.operation_id()
    }

    pub(crate) const fn fence_digest(&self) -> [u8; 32] {
        self.fence.digest()
    }

    pub(crate) fn prepare_publish(
        &self,
        action_id: ConnectorCtasActionId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<ConnectorCtasPublishRequest, novarocks_spi::connector::ConnectorError> {
        self.require_catalog_state(CoreCtasCatalogState::Active)?;
        ConnectorCtasPublishRequest::try_new(
            self.owner().clone(),
            self.fence.clone(),
            action_id,
            self.locator.clone(),
            completion.aggregate_digest(),
            self.policy,
            self.context.clone(),
        )
    }

    pub(crate) fn bind_write(
        &self,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.require_write_resolved()?;
        self.lease
            .bind_write(self.handle.clone(), completion)
            .map_err(ctas_spi_error)
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
            .bind_write(self.handle.clone(), completion)
            .map_err(ctas_spi_error)?;
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
        let binding = self
            .lease
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle: self.handle.clone(),
                operation_id,
                intent: ConnectorWriteIntent::Append,
                input_schema: Arc::clone(&input_schema),
                context,
            })
            .map_err(ctas_spi_error)?;
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
        Ok(())
    }

    pub(crate) fn prepare_abort(
        &self,
        action_id: ConnectorCtasActionId,
    ) -> Result<ConnectorCtasAbortRequest, novarocks_spi::connector::ConnectorError> {
        self.require_catalog_cleanup_state()?;
        ConnectorCtasAbortRequest::try_new(
            self.owner().clone(),
            self.fence.clone(),
            action_id,
            self.locator.clone(),
            self.proof.clone(),
            self.context.clone(),
        )
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

    fn require_catalog_state(
        &self,
        expected: CoreCtasCatalogState,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.require_write_resolved()?;
        let actual = *self
            .catalog_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if actual != expected {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                format!(
                    "CTAS catalog action requires {expected:?} state, current state is {actual:?}"
                ),
            ));
        }
        Ok(())
    }

    fn require_catalog_cleanup_state(
        &self,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.require_write_resolved()?;
        let actual = *self
            .catalog_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            actual,
            CoreCtasCatalogState::Active | CoreCtasCatalogState::NoOpCleanup
        ) {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                format!("CTAS staged cleanup is forbidden in {actual:?} state"),
            ));
        }
        Ok(())
    }
}

fn ctas_spi_error(failure: ConnectorCtasFailure) -> novarocks_spi::connector::ConnectorError {
    novarocks_spi::connector::ConnectorError::new(
        match failure.failure().kind() {
            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest => {
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::NotFound => {
                novarocks_spi::connector::ConnectorErrorKind::NotFound
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists => {
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::PermissionDenied => {
                novarocks_spi::connector::ConnectorErrorKind::PermissionDenied
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::Unauthenticated => {
                novarocks_spi::connector::ConnectorErrorKind::PermissionDenied
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::Conflict => {
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported => {
                novarocks_spi::connector::ConnectorErrorKind::Unsupported
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled => {
                novarocks_spi::connector::ConnectorErrorKind::Cancelled
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded => {
                novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::ResourceExhausted => {
                novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable => {
                novarocks_spi::connector::ConnectorErrorKind::Unavailable
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::CorruptData => {
                novarocks_spi::connector::ConnectorErrorKind::CorruptData
            }
            novarocks_spi::connector::ConnectorMutationFailureKind::Internal => {
                novarocks_spi::connector::ConnectorErrorKind::Internal
            }
        },
        failure.failure().message(),
    )
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
    preflight: CoreCtasTargetPreflight,
    command: CtasCommand,
    target: crate::catalog_application::resolver::TargetBackend,
    current_catalog: Option<String>,
    current_database: String,
    query_options: Option<QueryOptions>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    output_schema: arrow::datatypes::SchemaRef,
    output_columns: Vec<ConnectorColumnDefinition>,
    target_session: Arc<Mutex<Option<Arc<CoreCtasTargetSession>>>>,
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
    state: DmlExecutionKernel,
    gate: Arc<CtasSourceExecutionGate>,
    target: Arc<CoreCtasTargetSession>,
    native_encoding: Mutex<Option<crate::query_execution::compiler::NativeFragmentEncodingInput>>,
    pending: Mutex<Option<PendingCtasDistributedWrite>>,
    prepared:
        Mutex<Option<crate::query_execution::prepared_write::PreparedDistributedWriteRequest>>,
    completion: Mutex<Option<crate::query_execution::ConnectorWriteCompletion>>,
    write_session:
        Mutex<Option<crate::query_execution::write_operation::ConnectorWriteOperationSession>>,
    write_unknown: Mutex<Option<ExternalMutationEvidence>>,
    execution_identity: [u8; 32],
}

/// Core-retained write facts that are not part of the Frontend-owned native
/// encoding step. They are consumed exactly once when Frontend returns the
/// native bundle for the sealed plan/preparation pair.
struct PendingCtasDistributedWrite {
    query_options: Option<QueryOptions>,
    registration: crate::query_execution::contract::ConnectorWriteOperationRegistration,
    cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
    lease: ConnectorWriteLease,
}

impl CtasPreparedWrite for CorePreparedCtasWrite {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn execution_identity(&self) -> [u8; 32] {
        self.execution_identity
    }

    fn native_encoding(&self) -> Result<CtasNativeEncoding<'_>, CtasFailure> {
        let encoding = self
            .native_encoding
            .lock()
            .map_err(|error| internal_failure(format!("CTAS native encoding lock: {error}")))?;
        if encoding.is_none() {
            return Err(internal_failure(
                "CTAS native encoding input was already consumed",
            ));
        }
        Ok(CtasNativeEncoding { encoding })
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

fn downcast_preflight(
    preflight: &dyn CtasPreparedTargetPreflight,
) -> Result<&CoreCtasTargetPreflight, CtasFailure> {
    preflight
        .as_any()
        .downcast_ref::<CoreCtasTargetPreflight>()
        .ok_or_else(|| internal_failure("CTAS target preflight does not belong to the core engine"))
}

fn downcast_catalog_action(
    action: &dyn CtasPreparedCatalogAction,
) -> Result<&CoreCtasCatalogAction, CtasFailure> {
    action
        .as_any()
        .downcast_ref::<CoreCtasCatalogAction>()
        .ok_or_else(|| internal_failure("CTAS catalog action does not belong to the core engine"))
}

fn prepared_catalog_action(
    input_digest: [u8; 32],
    kind: CoreCtasCatalogActionKind,
) -> PreparedCtasCatalogAction {
    PreparedCtasCatalogAction {
        input_digest,
        handle: Arc::new(CoreCtasCatalogAction {
            kind,
            dispatched: AtomicBool::new(false),
        }),
    }
}

fn validate_fence_for_preflight(
    preflight: &CoreCtasTargetPreflight,
    fence: &ConnectorCtasPublicationFence,
) -> Result<(), CtasFailure> {
    fence.validate().map_err(connector_failure)?;
    if fence.target().instance_id != preflight.lease.owner().instance_id
        || fence.target().namespace.as_ref() != preflight.target.namespace
        || fence.target().table.as_ref() != preflight.target.table
    {
        return Err(CtasFailure {
            kind: CtasFailureKind::InvalidRequest,
            message: "CTAS publication fence names a foreign preflight target".to_string(),
        });
    }
    Ok(())
}

fn derive_ctas_foreground_leases(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<(ConnectorCtasStagedPublicationLease, ConnectorWriteLease), CtasFailure> {
    // The fenced capability is intentionally derived first. An unsupported
    // catalog therefore fails before writer admission or source preparation.
    let lease = planning
        .derive_ctas_staged_publication_lease()
        .map_err(connector_failure)?;
    let write_lease = planning.derive_write_lease().map_err(connector_failure)?;
    if lease.owner() != write_lease.binding_key() {
        return Err(CtasFailure {
            kind: CtasFailureKind::Internal,
            message: "CTAS fenced publication and writer leases do not share one exact generation"
                .to_string(),
        });
    }
    Ok((lease, write_lease))
}

fn historical_ctas_recovery(
    planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<
    Arc<dyn novarocks_spi::connector::ConnectorHistoricalCtasStagedPublicationRecovery>,
    ConnectorCtasFailure,
> {
    planning
        .binding()
        .historical_ctas_staged_publication_recovery()
        .cloned()
        .ok_or_else(|| {
            local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::Unsupported,
                message: "current connector generation has no historical CTAS recovery capability"
                    .to_string(),
            })
        })
}

fn require_established_fence(
    preflight: &CoreCtasTargetPreflight,
    fence: &ConnectorCtasPublicationFence,
) -> Result<(), CtasFailure> {
    let established = preflight
        .established_fence
        .lock()
        .map_err(|error| internal_failure(format!("CTAS established fence lock: {error}")))?;
    if established.as_ref() != Some(fence) {
        return Err(CtasFailure {
            kind: CtasFailureKind::InvalidRequest,
            message: "CTAS action requires the latest established publication fence".to_string(),
        });
    }
    Ok(())
}

fn local_ctas_failure(failure: CtasFailure) -> ConnectorCtasFailure {
    let kind = match failure.kind {
        CtasFailureKind::InvalidRequest => {
            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest
        }
        CtasFailureKind::NotFound => {
            novarocks_spi::connector::ConnectorMutationFailureKind::NotFound
        }
        CtasFailureKind::AlreadyExists | CtasFailureKind::Conflict => {
            novarocks_spi::connector::ConnectorMutationFailureKind::Conflict
        }
        CtasFailureKind::Unsupported => {
            novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported
        }
        CtasFailureKind::Cancelled => {
            novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled
        }
        CtasFailureKind::DeadlineExceeded => {
            novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded
        }
        CtasFailureKind::Unavailable => {
            novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable
        }
        CtasFailureKind::Internal => {
            novarocks_spi::connector::ConnectorMutationFailureKind::Internal
        }
    };
    ConnectorCtasFailure::KnownNotDispatched(
        novarocks_spi::connector::ConnectorMutationFailure::new(kind, failure.message),
    )
}

fn invalid_ctas_provider_response(
    operation: &'static str,
    error: novarocks_spi::connector::ConnectorError,
) -> ConnectorCtasFailure {
    ConnectorCtasFailure::CommittedResponseInvalid(ConnectorMutationFailure::new(
        novarocks_spi::connector::ConnectorMutationFailureKind::CorruptData,
        format!("historical CTAS {operation} returned an invalid response: {error}"),
    ))
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
        fence_digest: target.fence_digest(),
        locator_digest: target.locator.digest(),
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
                    "; retained fenced CTAS session also rejected write-unknown transition: {error}"
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
        established_fence: session.established_external_fence().ok().flatten(),
    }
}

impl CtasEngine for DmlExecutionKernel {
    fn classify_ctas(&self, sql: &str) -> Result<Option<CtasCommand>, String> {
        use novarocks_sql::syntax::{
            CreateTableKind, StarRocksDialect, looks_like_create_table,
            parse_create_table_statement,
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
                .map(crate::catalog_application::statement::connector_partition_transform)
                .collect(),
            properties: normalized_properties,
        }))
    }

    fn preflight_ctas_target(
        &self,
        command: &CtasCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<CtasTargetPreflightOutcome, CtasFailure> {
        let target = crate::catalog_application::resolver::resolve_table_target(
            self,
            &novarocks_sql::syntax::ObjectName {
                parts: command.target_parts.clone(),
            },
            current_catalog,
            current_database,
        )
        .map_err(internal_failure)?;
        let context =
            novarocks::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))
                .map_err(internal_failure)?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(connector_failure)?;
        let planning = self
            .connector_control()
            .acquire_current(&instance_id)
            .map_err(connector_failure)?;
        // Derive the mandatory capability before any source preparation. The
        // retained lease is the only foreground route; ordinary staged create
        // is intentionally not consulted.
        let (lease, write_lease) = derive_ctas_foreground_leases(&planning)?;
        let exists = novarocks::connector::metadata_table_exists_with_planning_lease(
            planning.clone(),
            context.clone(),
            &target.namespace,
            &target.table,
        )
        .map_err(internal_failure)?;
        match exists {
            true if command.if_not_exists => Ok(CtasTargetPreflightOutcome::ExistsNoOp),
            true => Err(CtasFailure {
                kind: CtasFailureKind::AlreadyExists,
                message: format!("table {}.{} already exists", target.namespace, target.table),
            }),
            false => {
                let binding = planning.binding();
                let facts = CtasTargetPreflightFacts {
                    provider_id: binding.descriptor().provider_id.as_str().to_string(),
                    instance_id: binding.descriptor().instance_id.as_str().to_string(),
                    incarnation: binding.incarnation().to_bytes(),
                    capability_version: lease.capability().protocol_version(),
                    target_namespace: target.namespace.clone(),
                    target_table: target.table.clone(),
                };
                Ok(CtasTargetPreflightOutcome::Ready(
                    PreparedCtasTargetPreflight {
                        facts,
                        handle: Arc::new(CoreCtasTargetPreflight {
                            target,
                            lease,
                            write_lease,
                            context,
                            established_fence: Arc::new(Mutex::new(None)),
                        }),
                    },
                ))
            }
        }
    }

    fn prepare_ctas_source(
        &self,
        preflight: &dyn CtasPreparedTargetPreflight,
        request: PrepareCtasSourceRequest,
    ) -> Result<PreparedCtasSource, CtasFailure> {
        let preflight = downcast_preflight(preflight)?;
        let target = crate::catalog_application::resolver::resolve_table_target(
            self,
            &novarocks_sql::syntax::ObjectName {
                parts: request.command.target_parts.clone(),
            },
            request.current_catalog.as_deref(),
            &request.current_database,
        )
        .map_err(internal_failure)?;
        if target != preflight.target {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS source target does not match its exact preflight".to_string(),
            });
        }
        let dialect = novarocks_sql::syntax::StarRocksDialect;
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(&request.command.source_sql)
            .map_err(|error| internal_failure(error.to_string()))?;
        let query = parser
            .parse_query()
            .map_err(|error| internal_failure(error.to_string()))?;
        let connector_context = novarocks::connector::connector_request_context_for_execution(
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
        let source_columns = planned.source.output_columns();
        if source_columns.is_empty() {
            return Err(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS source has no output columns".to_string(),
            });
        }
        let output_schema = Arc::new(arrow::datatypes::Schema::new(
            source_columns
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
            crate::query_execution::dml::iceberg_ctas::arrow_schema_to_table_column_defs(
                output_schema.as_ref(),
            )
            .map_err(internal_failure)?;
        let output_columns = table_columns
            .iter()
            .map(crate::catalog_application::statement::connector_column)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_failure)?;
        let schema_text = format!("{output_schema:?}");
        let optimized_fingerprint = planned.source.capture_fingerprint();
        let settings_material =
            novarocks_sql::planning::dml::optimizer_settings_stable_digest_material(
                &planned.optimizer_settings,
            );
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
                preflight: preflight.clone(),
                command: request.command,
                target,
                current_catalog: request.current_catalog,
                current_database: request.current_database,
                query_options: request.query_options,
                connector_context,
                output_schema,
                output_columns,
                target_session: Arc::new(Mutex::new(None)),
                target_prepare_started: AtomicBool::new(false),
            }),
        })
    }

    fn prepare_ctas_fence_advance(
        &self,
        preflight: &dyn CtasPreparedTargetPreflight,
        fence: ConnectorCtasPublicationFence,
        action_id: ConnectorCtasActionId,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
        let preflight = downcast_preflight(preflight)?;
        validate_fence_for_preflight(preflight, &fence)?;
        let request =
            ConnectorCtasAdvanceFenceRequest::try_new(fence, action_id, preflight.context.clone())
                .map_err(connector_failure)?;
        Ok(prepared_catalog_action(
            request.input_digest,
            CoreCtasCatalogActionKind::Advance {
                preflight: preflight.clone(),
                request,
            },
        ))
    }

    fn advance_ctas_fence(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
        let action = downcast_catalog_action(action).map_err(local_ctas_failure)?;
        let CoreCtasCatalogActionKind::Advance { preflight, request } = &action.kind else {
            return Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS catalog action is not an advance-fence action".to_string(),
            }));
        };
        action.begin_dispatch()?;
        // Serialize one preflight's successor check, catalog dispatch and
        // local writeback. Otherwise a slower lower-generation reply could
        // overwrite a newer established fence after both catalog calls had
        // already succeeded.
        let mut established = preflight
            .established_fence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = established.as_ref() {
            request
                .fence
                .validate_monotonic_successor_of(previous)
                .map_err(|error| local_ctas_failure(connector_failure(error)))?;
        }
        let receipt = preflight.lease.advance_fence(request.clone())?;
        *established = Some(request.fence.clone());
        Ok(receipt)
    }

    fn prepare_ctas_target(
        &self,
        source: &dyn CtasPreparedSource,
        fence: ConnectorCtasPublicationFence,
        stage_action_id: ConnectorCtasActionId,
        policy: CreatePolicy,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
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
        validate_fence_for_preflight(&source.preflight, &fence)?;
        require_established_fence(&source.preflight, &fence)?;
        let definition = ConnectorCtasStagedTableDefinition::try_new(
            fence.target().clone(),
            source.output_columns.clone(),
            source.command.partitioning.clone(),
            source.command.properties.clone(),
        )
        .map_err(connector_failure)?;
        let request = ConnectorCtasStageRequest::try_new(
            source.preflight.lease.owner().clone(),
            fence,
            stage_action_id,
            definition,
            policy,
            Bytes::new(),
            source.connector_context.clone(),
        )
        .map_err(connector_failure)?;
        Ok(prepared_catalog_action(
            request.input_digest,
            CoreCtasCatalogActionKind::Stage {
                preflight: source.preflight.clone(),
                request,
                target_slot: Arc::clone(&source.target_session),
            },
        ))
    }

    fn stage_ctas_target(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<CtasTargetStageResult, ConnectorCtasFailure> {
        let action = downcast_catalog_action(action).map_err(local_ctas_failure)?;
        let CoreCtasCatalogActionKind::Stage {
            preflight,
            request,
            target_slot,
        } = &action.kind
        else {
            return Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS catalog action is not a stage action".to_string(),
            }));
        };
        action.begin_dispatch()?;
        require_established_fence(preflight, &request.fence).map_err(local_ctas_failure)?;
        let result = preflight.lease.stage(request.clone())?;
        let target = CoreCtasTargetSession::from_stage(preflight, request, &result);
        *target_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&target));
        Ok(CtasTargetStageResult {
            target: PreparedCtasTarget {
                facts: target_facts(target.as_ref()),
                handle: target,
            },
            locator: result.locator,
            receipt: result.receipt,
            proof: result.proof,
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
        let (native_encoding, pending) = prepare_planned_ctas_connector_write(
            self,
            planned,
            Arc::clone(&source.output_schema),
            source.query_options.clone(),
            &source.connector_context,
            template,
        )
        .map_err(internal_failure)?;
        let cohort_set_digest = pending
            .registration
            .clone()
            .sealed_cohorts()
            .map_err(connector_failure)?
            .digest();
        let target = target_arc;
        let facts = target_facts(target.as_ref());
        let identity = source.gate.execution_identity();
        Ok(PreparedCtasWrite {
            target_facts: facts,
            write_operation_id,
            cohort_set_digest,
            execution_identity: identity,
            handle: Arc::new(CorePreparedCtasWrite {
                state: self.clone(),
                gate: Arc::clone(&source.gate),
                target,
                native_encoding: Mutex::new(Some(native_encoding)),
                pending: Mutex::new(Some(pending)),
                prepared: Mutex::new(None),
                completion: Mutex::new(None),
                write_session: Mutex::new(None),
                write_unknown: Mutex::new(None),
                execution_identity: identity,
            }),
        })
    }

    fn bind_ctas_write_native_bundle(
        &self,
        prepared: &dyn CtasPreparedWrite,
        native_bundle: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<(), CtasFailure> {
        let prepared = downcast_write(prepared)?;
        let pending = prepared
            .pending
            .lock()
            .map_err(|error| internal_failure(format!("CTAS pending write lock: {error}")))?
            .take()
            .ok_or_else(|| internal_failure("CTAS native bundle was already bound"))?;
        let encoding = prepared
            .native_encoding
            .lock()
            .map_err(|error| internal_failure(format!("CTAS native encoding lock: {error}")))?
            .take()
            .ok_or_else(|| internal_failure("CTAS native encoding input was already consumed"))?;
        if !encoding.matches_native_attachment(&native_bundle) {
            return Err(internal_failure(
                "native fragment bundle does not match the sealed CTAS encoding input",
            ));
        }
        let (_, prepared_fragments) = encoding.into_parts();
        let request = crate::query_execution::prepared_write::PreparedDistributedWriteRequest::new(
            prepared_fragments,
            native_bundle,
            pending.query_options,
            pending.registration,
            pending.cohort_id,
            pending.lease,
        )
        .map_err(|error| internal_failure(error.to_string()))?;
        *prepared
            .prepared
            .lock()
            .map_err(|error| internal_failure(format!("CTAS prepared write lock: {error}")))? =
            Some(request);
        Ok(())
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
                    .query_execution()
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
                let outcome = match prepared.state.query_execution().execute(request) {
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
                    established_fence: session.established_external_fence().ok().flatten(),
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
                    established_fence: None,
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
                established_fence: None,
            };
        }
        let session = match prepared.write_session.lock() {
            Ok(session) => session.clone(),
            Err(error) => {
                return CtasWriteOutcome::CommitUnknown {
                    failure: internal_failure(format!("CTAS write session lock: {error}")),
                    evidence: stored_evidence,
                    established_fence: None,
                };
            }
        };
        let Some(session) = session else {
            return CtasWriteOutcome::CommitUnknown {
                failure: internal_failure("CTAS unresolved writer lost its exact retained session"),
                evidence: stored_evidence,
                established_fence: None,
            };
        };
        let expected = write_staging_evidence(&session);
        if expected != stored_evidence {
            return CtasWriteOutcome::CommitUnknown {
                failure: internal_failure(
                    "CTAS stored writer evidence failed exact session validation",
                ),
                evidence: stored_evidence,
                established_fence: session.established_external_fence().ok().flatten(),
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
                    established_fence: session.established_external_fence().ok().flatten(),
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
                established_fence: session.established_external_fence().ok().flatten(),
            };
        }
        CtasWriteOutcome::Completed {
            completion,
            execution_identity: prepared.execution_identity,
            established_fence: session.established_external_fence().ok().flatten(),
        }
    }

    fn prepare_publish_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        action_id: ConnectorCtasActionId,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
        let target = downcast_target(target)?;
        let request = target
            .prepare_publish(action_id, completion)
            .map_err(connector_failure)?;
        Ok(prepared_catalog_action(
            request.input_digest,
            CoreCtasCatalogActionKind::Publish {
                lease: target.lease.clone(),
                request,
                state: Arc::clone(&target.catalog_state),
            },
        ))
    }

    fn publish_ctas(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
        let action = downcast_catalog_action(action).map_err(local_ctas_failure)?;
        let CoreCtasCatalogActionKind::Publish {
            lease,
            request,
            state,
        } = &action.kind
        else {
            return Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS catalog action is not a publish action".to_string(),
            }));
        };
        action.begin_dispatch()?;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *state != CoreCtasCatalogState::Active {
            return Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: format!("CTAS publish is forbidden in {:?} state", *state),
            }));
        }
        match lease.publish(request.clone()) {
            Ok(result) => {
                *state = match result.disposition {
                    novarocks_spi::connector::ConnectorCtasPublishDisposition::Published => {
                        CoreCtasCatalogState::Published
                    }
                    novarocks_spi::connector::ConnectorCtasPublishDisposition::NoOp => {
                        CoreCtasCatalogState::NoOpCleanup
                    }
                };
                Ok(result)
            }
            Err(failure) => {
                if !matches!(failure, ConnectorCtasFailure::KnownNotDispatched(_)) {
                    *state = CoreCtasCatalogState::HistoricalOnly;
                }
                Err(failure)
            }
        }
    }

    fn prepare_abort_ctas(
        &self,
        target: &dyn CtasPreparedTarget,
        action_id: ConnectorCtasActionId,
    ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
        let target = downcast_target(target)?;
        let request = target.prepare_abort(action_id).map_err(connector_failure)?;
        Ok(prepared_catalog_action(
            request.input_digest,
            CoreCtasCatalogActionKind::Abort {
                lease: target.lease.clone(),
                request,
                state: Arc::clone(&target.catalog_state),
            },
        ))
    }

    fn abort_ctas(
        &self,
        action: &dyn CtasPreparedCatalogAction,
    ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
        let action = downcast_catalog_action(action).map_err(local_ctas_failure)?;
        let CoreCtasCatalogActionKind::Abort {
            lease,
            request,
            state,
        } = &action.kind
        else {
            return Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: "CTAS catalog action is not an abort action".to_string(),
            }));
        };
        action.begin_dispatch()?;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            *state,
            CoreCtasCatalogState::Active | CoreCtasCatalogState::NoOpCleanup
        ) {
            return Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::InvalidRequest,
                message: format!("CTAS staged cleanup is forbidden in {:?} state", *state),
            }));
        }
        match lease.abort(request.clone()) {
            Ok(result) => {
                *state = CoreCtasCatalogState::Aborted;
                Ok(result)
            }
            Err(failure) => {
                if !matches!(failure, ConnectorCtasFailure::KnownNotDispatched(_)) {
                    *state = CoreCtasCatalogState::HistoricalOnly;
                }
                Err(failure)
            }
        }
    }

    fn inspect_historical_ctas(
        &self,
        descriptor: ConnectorHistoricalCtasDescriptor,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
        let planning = self
            .connector_control()
            .acquire_current(&descriptor.fence.target().instance_id)
            .map_err(|error| local_ctas_failure(connector_failure(error)))?;
        let recovery = historical_ctas_recovery(&planning)?;
        let observation = recovery.inspect(descriptor.clone(), context)?;
        observation
            .validate_for(&descriptor)
            .map_err(|error| invalid_ctas_provider_response("inspection", error))?;
        Ok(observation)
    }

    fn advance_historical_ctas_fence(
        &self,
        request: ConnectorCtasAdvanceFenceRequest,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
        let planning = self
            .connector_control()
            .acquire_current(&request.fence.target().instance_id)
            .map_err(|error| local_ctas_failure(connector_failure(error)))?;
        let recovery = historical_ctas_recovery(&planning)?;
        let receipt = recovery.advance_fence(request.clone())?;
        receipt
            .validate_for(&request)
            .map_err(|error| invalid_ctas_provider_response("advance fence", error))?;
        Ok(receipt)
    }

    fn cleanup_historical_ctas(
        &self,
        request: ConnectorHistoricalCtasCleanupRequest,
    ) -> Result<ConnectorHistoricalCtasCleanupReceipt, ConnectorCtasFailure> {
        let planning = self
            .connector_control()
            .acquire_current(&request.descriptor.fence.target().instance_id)
            .map_err(|error| local_ctas_failure(connector_failure(error)))?;
        let recovery = historical_ctas_recovery(&planning)?;
        let receipt = recovery.cleanup(request.clone())?;
        receipt
            .validate_for(&request)
            .map_err(|error| invalid_ctas_provider_response("cleanup", error))?;
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::backend_topology::LiveBackendTarget;
    use crate::common::query_cancellation::QueryCancellationSource;
    use bytes::Bytes;
    use novarocks_spi::connector::*;
    use novarocks_sql::compiler::SessionOptimizerSettings;
    use novarocks_types::ClusterRole;

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

    fn connector_context_with_deadline(deadline: Instant) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            deadline,
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    fn ctas_operation_id() -> ConnectorCtasOperationId {
        ConnectorCtasOperationId::try_from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap()
    }

    fn ctas_action_id() -> ConnectorCtasActionId {
        ConnectorCtasActionId::try_from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap()
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

    fn test_dml_kernel() -> DmlExecutionKernel {
        let connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> =
            Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default());
        DmlExecutionKernel::new(
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            Arc::clone(&connector_control),
            Arc::new(
                novarocks::connector::unified_statistics::UnifiedStatisticsResolver::default(),
            ),
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
            crate::query_execution::compiler::test_query_execution_service(),
        )
    }

    fn planned_source(sql: &str) -> PlannedCtasSourceQuery {
        let dialect = novarocks_sql::syntax::StarRocksDialect;
        let query = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("parser init")
            .parse_query()
            .expect("source query");
        let execution = fe_execution(SessionOptimizerSettings::default());
        plan_query_for_ctas_source(
            &test_dml_kernel(),
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
        let state = test_dml_kernel();
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
        let state = test_dml_kernel();
        let command = CtasEngine::classify_ctas(
            &state,
            "CREATE TABLE ice.sales.nested (\
                 items ARRAY<STRUCT<id INT, labels ARRAY<STRING>>>\
             ) COMMENT 'AS SELECT is text, not a CTAS clause'",
        )
        .expect("ordinary CREATE TABLE classification is inert");

        assert!(command.is_none());
    }

    #[test]
    fn opaque_ctas_source_fingerprint_distinguishes_predicates_and_join_keys() {
        let predicate_left = planned_source("SELECT x FROM (VALUES (1), (2)) AS v(x) WHERE x > 0");
        let predicate_right = planned_source("SELECT x FROM (VALUES (1), (2)) AS v(x) WHERE x > 1");
        assert_ne!(
            predicate_left.source.capture_fingerprint(),
            predicate_right.source.capture_fingerprint(),
            "same-shape predicates must not share a CTAS plan fingerprint"
        );

        let join_left = planned_source(
            "SELECT a.x FROM (VALUES (1, 2)) AS a(x, y) JOIN (VALUES (1, 2)) AS b(x, y) ON a.x = b.x",
        );
        let join_right = planned_source(
            "SELECT a.x FROM (VALUES (1, 2)) AS a(x, y) JOIN (VALUES (1, 2)) AS b(x, y) ON a.y = b.y",
        );
        assert_ne!(
            join_left.source.capture_fingerprint(),
            join_right.source.capture_fingerprint(),
            "same-shape join keys must not share a CTAS plan fingerprint"
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

    struct CountingFencedCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        calls: AtomicUsize,
    }

    struct SerialAdvanceCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl SerialAdvanceCapability {
        fn new(
            descriptor: ConnectorInstanceDescriptor,
            incarnation: ConnectorInstanceIncarnation,
        ) -> Self {
            Self {
                descriptor,
                incarnation,
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
            }
        }

        fn owner(&self) -> ConnectorExecutionBindingKey {
            ConnectorExecutionBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            }
        }
    }

    impl CountingFencedCapability {
        fn new(
            descriptor: ConnectorInstanceDescriptor,
            incarnation: ConnectorInstanceIncarnation,
        ) -> Self {
            Self {
                descriptor,
                incarnation,
                calls: AtomicUsize::new(0),
            }
        }

        fn owner(&self) -> ConnectorExecutionBindingKey {
            ConnectorExecutionBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            }
        }

        fn total_calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ConnectorCtasStagedPublication for CountingFencedCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }

        fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
            ConnectorCtasStagedPublicationCapability::try_new(
                CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION,
            )
            .unwrap()
        }

        fn advance_fence(
            &self,
            _: ConnectorCtasAdvanceFenceRequest,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorCtasFailure::PossiblyDispatched(
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "injected uncertain CTAS mutation",
                ),
            ))
        }

        fn stage(
            &self,
            _: ConnectorCtasStageRequest,
        ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorCtasFailure::PossiblyDispatched(
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "injected uncertain CTAS mutation",
                ),
            ))
        }

        fn plan_write(
            &self,
            _: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            unreachable!("test must reject before dispatch")
        }

        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            unreachable!("test must reject before dispatch")
        }

        fn publish(
            &self,
            _: ConnectorCtasPublishRequest,
        ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorCtasFailure::PossiblyDispatched(
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "injected uncertain CTAS mutation",
                ),
            ))
        }

        fn abort(
            &self,
            _: ConnectorCtasAbortRequest,
        ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorCtasFailure::PossiblyDispatched(
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "injected uncertain CTAS mutation",
                ),
            ))
        }
    }

    impl ConnectorCtasStagedPublication for SerialAdvanceCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }

        fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
            ConnectorCtasStagedPublicationCapability::try_new(
                CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION,
            )
            .unwrap()
        }

        fn advance_fence(
            &self,
            request: ConnectorCtasAdvanceFenceRequest,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(ConnectorCtasPublicationFenceReceipt::try_new(
                &request,
                Bytes::from_static(b"fence-receipt"),
            )
            .expect("static test fence receipt"))
        }

        fn stage(
            &self,
            _: ConnectorCtasStageRequest,
        ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure> {
            unreachable!()
        }

        fn plan_write(
            &self,
            _: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure> {
            unreachable!()
        }

        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorWriteOperationCompletion,
        ) -> Result<(), ConnectorCtasFailure> {
            unreachable!()
        }

        fn publish(
            &self,
            _: ConnectorCtasPublishRequest,
        ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
            unreachable!()
        }

        fn abort(
            &self,
            _: ConnectorCtasAbortRequest,
        ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
            unreachable!()
        }
    }

    struct TestNoopWriteControl {
        owner: ConnectorExecutionBindingKey,
    }

    impl ConnectorWriteControl for TestNoopWriteControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.owner
        }

        fn plan_write(
            &self,
            _: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            unreachable!()
        }

        fn commit(
            &self,
            _: ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!()
        }

        fn abort(
            &self,
            _: ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            unreachable!()
        }

        fn reconcile(
            &self,
            _: ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!()
        }
    }

    fn test_preflight(capability: Arc<CountingFencedCapability>) -> CoreCtasTargetPreflight {
        let owner = capability.owner();
        test_preflight_with_capability(owner, capability)
    }

    fn test_preflight_with_capability(
        owner: ConnectorExecutionBindingKey,
        capability: Arc<dyn ConnectorCtasStagedPublication>,
    ) -> CoreCtasTargetPreflight {
        CoreCtasTargetPreflight {
            target: crate::catalog_application::resolver::TargetBackend {
                backend_name: "iceberg",
                catalog: owner.instance_id.as_str().to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
            },
            lease: ConnectorCtasStagedPublicationLease::new(owner.clone(), capability, || {})
                .unwrap(),
            write_lease: ConnectorWriteLease::new(
                owner.clone(),
                Arc::new(TestNoopWriteControl { owner }),
                || {},
            )
            .unwrap(),
            context: connector_context(),
            established_fence: Arc::new(Mutex::new(None)),
        }
    }

    fn test_fence(preflight: &CoreCtasTargetPreflight) -> ConnectorCtasPublicationFence {
        ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).unwrap(),
            ctas_operation_id(),
            ConnectorTableIdentity {
                instance_id: preflight.lease.owner().instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
        )
        .unwrap()
    }

    fn test_stage_authority(
        preflight: &CoreCtasTargetPreflight,
        fence: &ConnectorCtasPublicationFence,
    ) -> (ConnectorCtasStagedLocator, ConnectorCtasPublicationProof) {
        let stage_action_id = ctas_action_id();
        let stage_input_digest = [7; 32];
        let locator = ConnectorCtasStagedLocator::try_new(
            preflight.lease.owner().clone(),
            fence,
            stage_action_id,
            fence.digest(),
            Bytes::from_static(b"staged-locator"),
        )
        .unwrap();
        let proof = ConnectorCtasPublicationProof::try_new(
            preflight.lease.owner().clone(),
            fence,
            ConnectorCtasProofPurpose::Stage,
            Some(stage_action_id),
            stage_input_digest,
            Some(&locator),
            Bytes::from_static(b"stage-proof"),
        )
        .unwrap();
        (locator, proof)
    }

    struct CountingHistoricalCapability {
        owner: ConnectorExecutionBindingKey,
        calls: Arc<AtomicUsize>,
    }

    impl ConnectorHistoricalCtasStagedPublicationRecovery for CountingHistoricalCapability {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.owner
        }

        fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
            ConnectorCtasStagedPublicationCapability::try_new(
                CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION,
            )
            .unwrap()
        }

        fn advance_fence(
            &self,
            _: ConnectorCtasAdvanceFenceRequest,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::Unavailable,
                message: "historical fixture".to_string(),
            }))
        }

        fn inspect(
            &self,
            _: ConnectorHistoricalCtasDescriptor,
            _: ConnectorRequestContext,
        ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::Unavailable,
                message: "historical fixture".to_string(),
            }))
        }

        fn cleanup(
            &self,
            _: ConnectorHistoricalCtasCleanupRequest,
        ) -> Result<ConnectorHistoricalCtasCleanupReceipt, ConnectorCtasFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(local_ctas_failure(CtasFailure {
                kind: CtasFailureKind::Unavailable,
                message: "historical fixture".to_string(),
            }))
        }
    }

    #[test]
    fn foreign_fence_is_rejected_before_catalog_dispatch() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let capability = Arc::new(CountingFencedCapability::new(
            descriptor.clone(),
            incarnation,
        ));
        let preflight = test_preflight(capability.clone());
        let foreign = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).unwrap(),
            ctas_operation_id(),
            ConnectorTableIdentity {
                instance_id: descriptor.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("other"),
            },
        )
        .unwrap();
        assert!(validate_fence_for_preflight(&preflight, &foreign).is_err());
        assert_eq!(capability.total_calls(), 0);
    }

    #[test]
    fn missing_established_fence_does_not_dispatch_stage() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let capability = Arc::new(CountingFencedCapability::new(
            descriptor.clone(),
            incarnation,
        ));
        let preflight = test_preflight(capability.clone());
        let fence = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).unwrap(),
            ctas_operation_id(),
            ConnectorTableIdentity {
                instance_id: descriptor.instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
        )
        .unwrap();
        assert!(require_established_fence(&preflight, &fence).is_err());
        assert_eq!(capability.total_calls(), 0);
    }

    #[test]
    fn concurrent_fence_advances_cannot_regress_the_local_established_generation() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let capability = Arc::new(SerialAdvanceCapability::new(
            descriptor.clone(),
            incarnation,
        ));
        let preflight = test_preflight_with_capability(capability.owner(), capability.clone());
        let state = test_dml_kernel();
        let operation_id = ctas_operation_id();
        let target = ConnectorTableIdentity {
            instance_id: descriptor.instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("t"),
        };
        let lower = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).unwrap(),
            operation_id,
            target.clone(),
        )
        .unwrap();
        let higher = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 2).unwrap(),
            operation_id,
            target,
        )
        .unwrap();
        let lower_action =
            CtasEngine::prepare_ctas_fence_advance(&state, &preflight, lower, ctas_action_id())
                .unwrap();
        let higher_action = CtasEngine::prepare_ctas_fence_advance(
            &state,
            &preflight,
            higher.clone(),
            ctas_action_id(),
        )
        .unwrap();

        let lower_state = state.clone();
        let lower_thread = std::thread::spawn(move || {
            CtasEngine::advance_ctas_fence(&lower_state, &*lower_action.handle).unwrap()
        });
        while capability.active.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        let higher_state = state.clone();
        let higher_thread = std::thread::spawn(move || {
            CtasEngine::advance_ctas_fence(&higher_state, &*higher_action.handle).unwrap()
        });

        lower_thread.join().unwrap();
        higher_thread.join().unwrap();
        assert_eq!(capability.calls.load(Ordering::SeqCst), 2);
        assert_eq!(capability.max_active.load(Ordering::SeqCst), 1);
        assert_eq!(
            preflight
                .established_fence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref(),
            Some(&higher)
        );
    }

    #[test]
    fn prepared_catalog_action_dispatches_at_most_once_after_unknown_outcome() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let capability = Arc::new(CountingFencedCapability::new(
            descriptor,
            ConnectorInstanceIncarnation::new(),
        ));
        let preflight = test_preflight(capability.clone());
        let fence = test_fence(&preflight);
        let request =
            ConnectorCtasAdvanceFenceRequest::try_new(fence, ctas_action_id(), connector_context())
                .unwrap();
        let action = prepared_catalog_action(
            request.input_digest,
            CoreCtasCatalogActionKind::Advance { preflight, request },
        );
        let state = test_dml_kernel();

        assert!(matches!(
            CtasEngine::advance_ctas_fence(&state, &*action.handle),
            Err(ConnectorCtasFailure::PossiblyDispatched(_))
        ));
        assert!(matches!(
            CtasEngine::advance_ctas_fence(&state, &*action.handle),
            Err(ConnectorCtasFailure::KnownNotDispatched(_))
        ));
        assert_eq!(capability.total_calls(), 1);
    }

    #[test]
    fn unknown_publish_latches_historical_only_and_blocks_abort_dispatch() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let capability = Arc::new(CountingFencedCapability::new(
            descriptor,
            ConnectorInstanceIncarnation::new(),
        ));
        let preflight = test_preflight(capability.clone());
        let fence = test_fence(&preflight);
        let (locator, stage_proof) = test_stage_authority(&preflight, &fence);
        let catalog_state = Arc::new(Mutex::new(CoreCtasCatalogState::Active));
        let publish_request = ConnectorCtasPublishRequest::try_new(
            preflight.lease.owner().clone(),
            fence.clone(),
            ctas_action_id(),
            locator.clone(),
            [8; 32],
            CreatePolicy::FailIfExists,
            connector_context(),
        )
        .unwrap();
        let publish_action = prepared_catalog_action(
            publish_request.input_digest,
            CoreCtasCatalogActionKind::Publish {
                lease: preflight.lease.clone(),
                request: publish_request,
                state: Arc::clone(&catalog_state),
            },
        );
        let state = test_dml_kernel();

        assert!(matches!(
            CtasEngine::publish_ctas(&state, &*publish_action.handle),
            Err(ConnectorCtasFailure::PossiblyDispatched(_))
        ));
        assert_eq!(
            *catalog_state.lock().unwrap(),
            CoreCtasCatalogState::HistoricalOnly
        );
        assert!(matches!(
            CtasEngine::publish_ctas(&state, &*publish_action.handle),
            Err(ConnectorCtasFailure::KnownNotDispatched(_))
        ));

        let abort_request = ConnectorCtasAbortRequest::try_new(
            preflight.lease.owner().clone(),
            fence,
            ctas_action_id(),
            locator,
            stage_proof,
            connector_context(),
        )
        .unwrap();
        let abort_action = prepared_catalog_action(
            abort_request.input_digest,
            CoreCtasCatalogActionKind::Abort {
                lease: preflight.lease.clone(),
                request: abort_request,
                state: Arc::clone(&catalog_state),
            },
        );
        assert!(matches!(
            CtasEngine::abort_ctas(&state, &*abort_action.handle),
            Err(ConnectorCtasFailure::KnownNotDispatched(_))
        ));
        assert_eq!(capability.total_calls(), 1);
    }

    #[test]
    fn staged_target_uses_the_live_stage_context_not_expired_preflight_context() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let capability = Arc::new(CountingFencedCapability::new(
            descriptor,
            ConnectorInstanceIncarnation::new(),
        ));
        let mut preflight = test_preflight(capability);
        preflight.context =
            connector_context_with_deadline(Instant::now() - Duration::from_secs(1));
        let fence = test_fence(&preflight);
        let stage_action_id = ctas_action_id();
        let live_context =
            connector_context_with_deadline(Instant::now() + Duration::from_secs(120));
        let definition = ConnectorCtasStagedTableDefinition::try_new(
            fence.target().clone(),
            vec![ConnectorColumnDefinition {
                name: Arc::from("v"),
                data_type: ConnectorDataType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            Vec::new(),
            Default::default(),
        )
        .unwrap();
        let request = ConnectorCtasStageRequest::try_new(
            preflight.lease.owner().clone(),
            fence.clone(),
            stage_action_id,
            definition,
            CreatePolicy::FailIfExists,
            Bytes::new(),
            live_context.clone(),
        )
        .unwrap();
        let locator = ConnectorCtasStagedLocator::try_new(
            preflight.lease.owner().clone(),
            &fence,
            stage_action_id,
            request.target_digest,
            Bytes::from_static(b"staged-locator"),
        )
        .unwrap();
        let result = ConnectorCtasStageResult::try_new(
            &request,
            locator.clone(),
            ConnectorStagedTableHandle::try_new(
                preflight.lease.owner().clone(),
                ConnectorMutationOperationId::from_bytes(stage_action_id.to_bytes()),
                Bytes::from_static(b"staged-handle"),
            )
            .unwrap(),
            ConnectorCtasPublicationReceipt::try_new(
                &fence,
                stage_action_id,
                request.input_digest,
                Bytes::from_static(b"stage-receipt"),
            )
            .unwrap(),
            ConnectorCtasPublicationProof::try_new(
                preflight.lease.owner().clone(),
                &fence,
                ConnectorCtasProofPurpose::Stage,
                Some(stage_action_id),
                request.input_digest,
                Some(&locator),
                Bytes::from_static(b"stage-proof"),
            )
            .unwrap(),
        )
        .unwrap();

        let target = CoreCtasTargetSession::from_stage(&preflight, &request, &result);
        assert_eq!(target.context().deadline(), live_context.deadline());
        assert_eq!(
            target
                .prepare_abort(ctas_action_id())
                .unwrap()
                .context
                .deadline(),
            live_context.deadline()
        );
    }

    #[test]
    fn foreign_locator_is_rejected_before_publish_dispatch() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let capability = Arc::new(CountingFencedCapability::new(
            descriptor.clone(),
            incarnation,
        ));
        let preflight = test_preflight(capability.clone());
        let fence = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).unwrap(),
            ctas_operation_id(),
            ConnectorTableIdentity {
                instance_id: descriptor.instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
        )
        .unwrap();
        let foreign_owner = ConnectorExecutionBindingKey {
            instance_id: preflight.lease.owner().instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::new(),
        };
        let locator = ConnectorCtasStagedLocator::try_new(
            foreign_owner,
            &fence,
            ctas_action_id(),
            fence.digest(),
            Bytes::from_static(b"foreign-locator"),
        )
        .unwrap();
        let request = ConnectorCtasPublishRequest::try_new(
            preflight.lease.owner().clone(),
            fence,
            ctas_action_id(),
            locator,
            [3; 32],
            CreatePolicy::FailIfExists,
            connector_context(),
        );
        assert!(request.is_err());
        assert_eq!(capability.total_calls(), 0);
    }

    #[test]
    fn historical_inspection_never_calls_ordinary_capability() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("rest").unwrap(),
        };
        let incarnation = ConnectorInstanceIncarnation::from_bytes([0; 16]);
        let ordinary = Arc::new(CountingFencedCapability::new(
            descriptor.clone(),
            incarnation,
        ));
        let historical_calls = Arc::new(AtomicUsize::new(0));
        let historical = Arc::new(CountingHistoricalCapability {
            owner: ordinary.owner(),
            calls: Arc::clone(&historical_calls),
        });
        let binding = novarocks::connector::scan_model::planned_files_fixture_binding_for_provider(
            descriptor.provider_id.clone(),
            descriptor.instance_id.as_str(),
            Default::default(),
            None,
        )
        .try_with_ctas_staged_publication(Some(ordinary.clone()))
        .unwrap()
        .try_with_historical_ctas_staged_publication_recovery(Some(historical))
        .unwrap();
        let planning = ConnectorControlPlanningLease::new(Arc::new(binding), || {});
        let fence = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").unwrap(),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).unwrap(),
            ctas_operation_id(),
            ConnectorTableIdentity {
                instance_id: descriptor.instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
        )
        .unwrap();
        let descriptor = ConnectorHistoricalCtasDescriptor::try_new(
            ordinary.owner(),
            fence.clone(),
            [1; 32],
            fence.digest(),
            CreatePolicy::FailIfExists,
            None,
            vec![ConnectorHistoricalCtasCheckpoint {
                action_id: ctas_action_id(),
                action: ConnectorHistoricalCtasAction::Stage,
                dispatch: ConnectorHistoricalCtasDispatchState::Unknown,
                input_digest: [2; 32],
                evidence_digest: None,
            }],
            None,
        )
        .unwrap();
        let recovery = historical_ctas_recovery(&planning).unwrap();
        assert!(recovery.inspect(descriptor, connector_context()).is_err());
        assert_eq!(historical_calls.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary.total_calls(), 0);
    }
}
