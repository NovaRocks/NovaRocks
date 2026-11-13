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

//! Table-maintenance application ports shared with the Frontend role application.
//!
//! This dependency-inversion boundary exposes only the typed engine
//! capabilities and application results needed by the frontend owner. It does
//! not expose standalone engine state or connector handles.

use std::sync::Arc;

mod action_state;
pub(crate) mod cleanup_action;
pub mod command;
pub(crate) mod io_admission;
mod rewrite_capture;
pub(crate) mod rewrite_computation;
pub(crate) mod rewrite_job;
pub(crate) mod rewrite_preparation;
pub use action_state::MaintenanceActionState;
pub(crate) mod iceberg;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use crate::connector::cleanup_maintenance::{CleanupBatchExecution, CleanupMaintenanceSession};
use novarocks_query_application::api::QueryResult;
use novarocks_spi::connector::{
    CandidatePage, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefSelection,
    ConnectorRewriteCohortRead, ConnectorWriteCohortId, ConnectorWriteInputShape, PreparedBatch,
};
use novarocks_table_maintenance::runtime::TerminalError;
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceEffectId, MaintenanceTarget,
};

pub const TABLE_MAINTENANCE_SERVICE_UNAVAILABLE: &str = "table maintenance service is not injected";

/// One exact rewrite cohort, planned and encoded, waiting to be submitted.
///
/// The plan and the session that commits it are held together because they
/// are one decision: the writer handle the plan states is the handle this
/// session sealed, and a plan paired with a different session would stage
/// rows a commit never takes over.
pub struct PreparedDistributedRewriteCohort {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedDistributedRewriteCohort {
    /// Consume this exact frozen cohort through the common governed Write
    /// consumer. Its receiving port appends once to this SAME session; the
    /// returned Ready is not an input to legacy accumulate(prepared).
    pub(crate) fn prepare_computation(
        self,
        initial: crate::query_execution::logical_execution::InitialComputationReservation,
        runtime: crate::query_execution::internal_consumer::decoded::DecodedConsumerRuntime,
    ) -> Result<
        (
            crate::query_execution::logical_execution::PreparedLogicalComputation,
            novarocks_query_application::api::ReservedExecutionIdentity,
            crate::query_execution::internal_consumer::write::WriteComputationOutput,
        ),
        novarocks_query_application::api::QueryExecutionError,
    > {
        let options =
            crate::query_execution::contract::synthetic_statement_query_options(&self.execution);
        crate::query_execution::internal_consumer::write::prepare_write_computation(
            self.description,
            self.template,
            Arc::new(
                crate::query_execution::contract::ResolvedQueryOptions::from_upstream(Some(
                    options,
                )),
            ),
            self.write_session,
            initial,
            runtime,
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MaintenanceRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

/// Write capability for one frontend-owned maintenance execution attempt.
///
/// This cancellation is deliberately provider-neutral. The frontend keeps the
/// source and passes only [`MaintenanceAttemptContext`] through the engine
/// port, so a lost coordination lease can stop subsequent connector work
/// without exposing a lease, repository, or provider object to Core.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct MaintenanceAttemptCancellationSource {
    cancelled: Arc<AtomicBool>,
    stop: novarocks_spi::connector::ConnectorStopOwner,
}

#[cfg(test)]
impl MaintenanceAttemptCancellationSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn context(&self) -> MaintenanceAttemptContext {
        MaintenanceAttemptContext {
            stop: self.stop.view(),
        }
    }

    /// Returns true only for the first cancellation request.
    pub fn cancel(&self) -> bool {
        let first = !self.cancelled.swap(true, Ordering::AcqRel);
        self.stop.request_stop();
        first
    }
}

/// Read-only cancellation view shared by all provider calls in one durable
/// maintenance attempt.
#[derive(Clone, Debug)]
pub struct MaintenanceAttemptContext {
    stop: novarocks_spi::connector::ConnectorStopView,
}

impl Default for MaintenanceAttemptContext {
    fn default() -> Self {
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        Self { stop: stop.view() }
    }
}

impl MaintenanceAttemptContext {
    pub fn uncancelled() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn is_cancelled(&self) -> bool {
        self.stop.is_stopped()
    }

    #[cfg(test)]
    fn connector_request_context(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        crate::connector::connector_request_context(None, self.stop.clone())
    }

    /// Preserve the statement's admitted connector deadline and cancellation
    /// while also stopping subsequent provider work when this maintenance
    /// attempt loses its frontend fence.
    ///
    /// A durable maintenance service owns the fence cancellation; it must not
    /// replace the request cancellation captured by the SQL admission path.
    pub(crate) fn connector_request_context_with_attempt(
        &self,
        request: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        Ok(request
            .clone()
            .without_attempt_capabilities()
            .with_request_scope(novarocks_spi::connector::ConnectorRequestScope::new())
            .with_additional_stop(self.stop.clone()))
    }
}

#[derive(Clone, Debug)]
pub enum MaintenanceStatementResult {
    Ok,
    Query(QueryResult),
}

/// CLS-R2 boundary: the implementation moves to the frontend with the rest of
/// the maintenance application; this stable Core domain port remains available
/// to the Frontend MV background runtime until CLS-R3.
// Design: ADR-0083 (docs/adr/ADR-0083-frontend-owns-table-maintenance-execution-port.md)
#[async_trait::async_trait]
pub trait TableMaintenanceEngine: Send + Sync {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, crate::catalog_application::resolver::TargetResolutionError>;

    async fn reject_user_action_on_mv(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<(), novarocks_query_application::session_error::QueryServiceError>;

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String>;

    async fn execute_action(
        &self,
        state: &MaintenanceActionState,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String>;

    /// Automatic metadata effects retain the MV owner's exact identity and
    /// the provider's typed terminal state. A missing implementation closes
    /// management before a provider dispatch.
    async fn execute_automatic_metadata_action(
        &self,
        _state: &MaintenanceActionState,
        _request: MaintenanceActionRequest,
        _effect_id: MaintenanceEffectId,
    ) -> Result<novarocks_table_maintenance::AutomaticMaintenanceOutcome, TerminalError> {
        Err(TerminalError::pre_dispatch_failed(
            "automatic metadata maintenance identity is unsupported",
        ))
    }

    /// Plan a provider-neutral FE-only orphan cleanup operation. The returned
    /// session owns its exact connector generation for the durable frontend
    /// operation; no BE or generic action route participates.
    fn plan_cleanup_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _operation_id: ConnectorCleanupOperationId,
        _older_than_ms: i64,
    ) -> Result<CleanupMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn plan_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_cleanup_maintenance(target, operation_id, older_than_ms)
    }

    /// Plan the second cleanup pass after the frontend has durably selected
    /// exact mature owned refs. An empty selection is a valid ref-only plan;
    /// it must never become an object sweep.
    fn plan_selected_owned_ref_cleanup_maintenance(
        &self,
        _target: &MaintenanceTarget,
        _operation_id: ConnectorCleanupOperationId,
        _older_than_ms: i64,
        _selection: ConnectorCleanupOwnedRefSelection,
    ) -> Result<CleanupMaintenanceSession, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
        _attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_selected_owned_ref_cleanup_maintenance(
            target,
            operation_id,
            older_than_ms,
            selection,
        )
    }

    fn prepare_cleanup_batch(
        &self,
        _session: &CleanupMaintenanceSession,
        _batch_ordinal: u32,
    ) -> Result<PreparedBatch, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn execute_cleanup_batch(
        &self,
        _session: &CleanupMaintenanceSession,
        _prepared: PreparedBatch,
    ) -> Result<CleanupBatchExecution, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn read_cleanup_candidate_page(
        &self,
        _session: &CleanupMaintenanceSession,
        _offset: u64,
        _limit: u32,
    ) -> Result<CandidatePage, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }

    fn finalize_cleanup_terminal(
        &self,
        _session: &CleanupMaintenanceSession,
    ) -> Result<(), String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_string())
    }
}

#[async_trait::async_trait]
pub trait TableMaintenanceService: Send + Sync {
    /// Executes one already-lowered maintenance write statement. The parser
    /// owner has consumed SQL syntax before this boundary, so implementations
    /// must not reparse or probe raw source text.
    async fn handle_typed_statement(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _state: &MaintenanceActionState,
        _statement: crate::table_maintenance::ParsedMaintenanceStatement,
        _spark_procedure: bool,
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, TerminalError> {
        Err(TerminalError::pre_dispatch_failed(
            TABLE_MAINTENANCE_SERVICE_UNAVAILABLE,
        ))
    }

    #[cfg(novarocks_dev_controls)]
    fn statement_test_controls(
        &self,
    ) -> Option<Arc<crate::table_maintenance::MaintenanceTestControls>> {
        None
    }
    fn original_optimize_host(&self) -> Result<OriginalOptimizeHost, String> {
        Err("original Optimize statement host is unavailable".into())
    }
}

#[doc(hidden)]
pub struct OriginalOptimizeHost {
    pub(crate) product: Arc<novarocks_table_maintenance::product::TableMaintenanceProduct>,
}

/// One foreground SQL maintenance command bound to the exact request admitted
/// by Frontend.  It deliberately contains only the maintenance kernel and
/// immutable request facts; it cannot recover an application facade or capture
/// a second topology, deadline, or cancellation scope.
#[derive(Clone)]
pub(crate) struct RequestScopedMaintenanceEngine {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl RequestScopedMaintenanceEngine {
    pub(crate) async fn reject_direct_rewrite_on_mv(
        &self,
        target: &MaintenanceTarget,
        exact_lease: &novarocks_spi::connector::ConnectorControlPlanningPin,
    ) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context_async(
            self.kernel.mv_storage_observation().as_ref(),
            exact_lease,
            self.kernel.mv_readiness().as_ref(),
            &crate::catalog_application::resolver::TargetBackend {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .expect("static Iceberg provider ID"),
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Maintenance,
            self.connector_context.clone(),
        ).await
    }

    pub(crate) async fn reject_direct_rewrite_on_mv_governed(
        &self,
        target: &MaintenanceTarget,
        exact_lease: &novarocks_spi::connector::ConnectorControlPlanningPin,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
        crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_governed_planning_pin_and_context_async(
            self.kernel.mv_storage_observation().as_ref(),
            exact_lease,
            self.kernel.mv_readiness().as_ref(),
            &crate::catalog_application::resolver::TargetBackend {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .expect("static Iceberg provider ID"),
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Maintenance,
            self.connector_context.clone(),
            cpu,
        ).await
    }

    pub fn new(
        kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
        execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            kernel,
            execution,
            connector_context,
        }
    }

    fn connector_context_for_attempt(
        &self,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<novarocks_spi::connector::ConnectorRequestContext, String> {
        attempt.connector_request_context_with_attempt(&self.connector_context)
    }

    fn target_identity(
        target: &MaintenanceTarget,
    ) -> Result<novarocks_spi::connector::ConnectorTableIdentity, String> {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        Ok(novarocks_spi::connector::ConnectorTableIdentity {
            instance_id,
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        })
    }
}

#[cfg(test)]
fn maintenance_target_rebind_from_connector_result(
    binding: Result<(), novarocks_spi::connector::ConnectorError>,
) -> Result<novarocks_table_maintenance::MaintenanceTargetRebind, String> {
    use novarocks_spi::connector::ConnectorTableObjectBindingFailure;
    use novarocks_table_maintenance::MaintenanceTargetRebind;
    match binding {
        Ok(_) => Ok(MaintenanceTargetRebind::Bound),
        Err(error) => match error.table_object_binding_failure() {
            Some(ConnectorTableObjectBindingFailure::Replaced) => {
                Ok(MaintenanceTargetRebind::Replaced)
            }
            Some(ConnectorTableObjectBindingFailure::Missing) => {
                Ok(MaintenanceTargetRebind::Missing)
            }
            None => Err(format!(
                "rebind maintenance target object identity: {error}"
            )),
        },
    }
}

impl crate::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer
    for RequestScopedMaintenanceEngine
{
    fn invalidate_generic_table(
        &self,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        crate::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer::invalidate_generic_table(
            &self.kernel,
            table,
        )
    }
}

#[async_trait::async_trait]
impl TableMaintenanceEngine for RequestScopedMaintenanceEngine {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, crate::catalog_application::resolver::TargetResolutionError>
    {
        let target = crate::catalog_application::resolver::resolve_existing_table_target(
            &self.kernel,
            &novarocks_sql::semantic::ObjectName {
                parts: name_parts.to_vec(),
            },
            context.current_catalog,
            context.current_database,
        )?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        self.kernel
            .connector_control()
            .acquire_current_metadata_maintenance(&instance_id)
            .map_err(|error| error.to_string())?;
        Ok(MaintenanceTarget {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
        })
    }

    async fn reject_user_action_on_mv(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
        use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};
        let instance_id = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
            crate::mv::domain::readiness::application_error(format!(
                "parse Iceberg catalog identity for MV guard: {error}"
            ))
        })?;
        let exact_lease = ConnectorControlResolver::acquire_current(
            self.kernel.connector_control().as_ref(),
            &instance_id,
        )
        .map_err(crate::mv::domain::readiness::connector_application_error)?;
        self.reject_direct_rewrite_on_mv(target, &exact_lease).await
    }

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String> {
        self::iceberg::current_snapshot_id_with_ports(
            self.kernel.connector_control().as_ref(),
            target,
            self.connector_context.clone(),
        )
    }

    async fn execute_action(
        &self,
        state: &MaintenanceActionState,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        state.install_engine(self.clone())?;
        state.execute(request).await
    }

    async fn execute_automatic_metadata_action(
        &self,
        state: &MaintenanceActionState,
        request: MaintenanceActionRequest,
        effect_id: MaintenanceEffectId,
    ) -> Result<novarocks_table_maintenance::AutomaticMaintenanceOutcome, TerminalError> {
        state
            .install_engine(self.clone())
            .map_err(TerminalError::pre_dispatch_failed)?;
        state.execute_automatic(request, effect_id).await
    }

    fn plan_cleanup_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_cleanup_maintenance_with_attempt_context(
            target,
            operation_id,
            older_than_ms,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn plan_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        let identity = Self::target_identity(target)?;
        CleanupMaintenanceSession::plan(
            self.kernel.connector_control().as_ref(),
            &identity.instance_id.clone(),
            operation_id,
            identity,
            older_than_ms,
            self.connector_context_for_attempt(attempt)?,
        )
        .map_err(|error| format!("plan orphan cleanup operation: {error}"))
    }

    fn plan_selected_owned_ref_cleanup_maintenance(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
    ) -> Result<CleanupMaintenanceSession, String> {
        self.plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
            target,
            operation_id,
            older_than_ms,
            selection,
            &MaintenanceAttemptContext::uncancelled(),
        )
    }

    fn plan_selected_owned_ref_cleanup_maintenance_with_attempt_context(
        &self,
        target: &MaintenanceTarget,
        operation_id: ConnectorCleanupOperationId,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
        attempt: &MaintenanceAttemptContext,
    ) -> Result<CleanupMaintenanceSession, String> {
        let identity = Self::target_identity(target)?;
        CleanupMaintenanceSession::plan_selected_owned_refs(
            self.kernel.connector_control().as_ref(),
            &identity.instance_id.clone(),
            operation_id,
            identity,
            older_than_ms,
            selection,
            self.connector_context_for_attempt(attempt)?,
        )
        .map_err(|error| format!("plan selected owned-ref cleanup operation: {error}"))
    }

    fn prepare_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        batch_ordinal: u32,
    ) -> Result<PreparedBatch, String> {
        session
            .prepare_batch(batch_ordinal)
            .map_err(|error| format!("prepare orphan cleanup batch: {error}"))
    }

    fn execute_cleanup_batch(
        &self,
        session: &CleanupMaintenanceSession,
        prepared: PreparedBatch,
    ) -> Result<CleanupBatchExecution, String> {
        session
            .execute_batch(prepared)
            .map_err(|error| format!("execute orphan cleanup batch: {error}"))
    }

    fn read_cleanup_candidate_page(
        &self,
        session: &CleanupMaintenanceSession,
        offset: u64,
        limit: u32,
    ) -> Result<CandidatePage, String> {
        session
            .read_candidate_page(offset, limit)
            .map_err(|error| format!("read orphan cleanup candidate page: {error}"))
    }

    fn finalize_cleanup_terminal(&self, session: &CleanupMaintenanceSession) -> Result<(), String> {
        session
            .finalize_terminal()
            .map_err(|error| format!("finalize orphan cleanup artifacts: {error}"))
    }
}

pub(super) struct RewriteCohortRecipe {
    bindings: Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    physical_plan: novarocks_sql::planning::query_execution::FrozenConnectorScanPlan,
    sink: novarocks_sql::planning::dml::DmlWritePlanInput,
    need: novarocks_sql::compiler::ProviderReadNeed,
    session: crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    execution: novarocks_query_application::admitted_query_context::QueryExecutionContext,
}
struct RewriteCohortPlanParts {
    encoded: crate::query_execution::physical_encoding::PreparedStaticPlan,
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    version: novarocks_physical_plan::PlanVersionId,
}
fn prepare_rewrite_cohort_recipe(
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RewriteCohortRecipe, novarocks_sql::compiler::SqlCompileError> {
    let cohort = session
        .plan()
        .cohorts()
        .iter()
        .find(|candidate| candidate.cohort_id() == cohort_id)
        .ok_or_else(|| "distributed rewrite execution names an unknown cohort".to_string())?;
    let table_bindings =
        Arc::new(crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()?);
    // A cohort that rewrites table rows names exactly the data files its
    // commit replaces, so its read is frozen from that set. A cohort that
    // rewrites delete artifacts reads no table rows at all, so it has no data
    // file set to be frozen from and names its frozen group instead -- the
    // same group its commit resolves the replaced artifacts from.
    let (cohort_read, source_binding, physical_plan) = match cohort.read() {
        ConnectorRewriteCohortRead::PinnedFileSet(pinned) => {
            let read = crate::query_execution::cohort_read::QueryPinnedFileSetRead {
                pinned: pinned.clone(),
                frozen_source: None,
                planning_pin: session.rewrite().planning_pin(),
                _original: None,
            };
            let source_binding =
                crate::query_execution::distributed_rewrite::admit_pinned_rewrite_scan_binding(
                    table_bindings.as_ref(),
                    cohort.scan_execution_schema(),
                    read.clone(),
                )?;
            let physical_plan =
                crate::query_execution::distributed_rewrite::pinned_rewrite_scan_physical_plan(
                    cohort.scan_execution_schema(),
                    source_binding,
                )?;
            (
                crate::catalog_application::query_bindings::QueryFrozenCohortRead::PinnedFileSet(
                    read,
                ),
                source_binding,
                physical_plan,
            )
        }
        ConnectorRewriteCohortRead::DeleteArtifactGroup(group) => {
            let read = crate::query_execution::cohort_read::QueryRewriteGroupRead {
                group: group.clone(),
                group_digest: cohort.group_digest(),
                planning_pin: session.rewrite().planning_pin(),
            };
            let source_binding =
                crate::query_execution::distributed_rewrite::admit_rewrite_group_scan_binding(
                    table_bindings.as_ref(),
                    cohort.scan_execution_schema(),
                    read.clone(),
                )?;
            let physical_plan =
                crate::query_execution::distributed_rewrite::rewrite_group_scan_physical_plan(
                    cohort.scan_execution_schema(),
                    source_binding,
                )?;
            (
                crate::catalog_application::query_bindings::QueryFrozenCohortRead::TableExecute(
                    read,
                ),
                source_binding,
                physical_plan,
            )
        }
    };
    // The session sealed one logical target per frozen group, so this group's
    // writer recipe and its ordinal both come from there. Deriving the ordinal
    // from anything else would let a group's artifacts be attributed to another
    // group's writer, which the prepared set cannot catch.
    let write_target = session
        .write_target(cohort_id)
        .map_err(|error| format!("resolve frozen rewrite group write target: {error}"))?;
    let target_binding =
        crate::query_execution::planning::write_sink::admit_session_connector_write_target(
            table_bindings.as_ref(),
            rewrite_target_identity(session, cohort_id),
            write_target,
            session.rewrite().planning_pin(),
        )?;
    let sink =
        crate::query_execution::planning::write_sink::dml_write_plan_input_for_admitted_target(
            table_bindings.as_ref(),
            target_binding,
            rewrite_sink_mode(write_target.input())?,
            novarocks_sql::planning::dml::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )?;
    crate::connector::validate_request_context(context)?;

    // Freeze the read. The capability it leaves is deposited as it is taken,
    // so the plan and what performs its read are accounted for together.
    let need = crate::query_execution::distributed_rewrite::rewrite_cohort_provider_read_need(
        source_binding,
        cohort.scan_execution_schema(),
        &cohort_read,
    )?;
    Ok(RewriteCohortRecipe {
        bindings: table_bindings,
        physical_plan,
        sink,
        need,
        session: session.clone(),
        cohort_id,
        execution: execution.clone(),
    })
}

fn freeze_rewrite_cohort_recipe(
    recipe: &RewriteCohortRecipe,
    read_fact: novarocks_sql::compiler::ProviderReadFact,
    access: &novarocks_query_application::preparation::FinalPlanRuntimeAccess<
        crate::query_execution::provider_read_facts::FrozenProviderRead,
    >,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    static_plan_carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &novarocks_sql::compiler::SqlCompileControl,
    capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) -> Result<RewriteCohortPlanParts, novarocks_sql::compiler::SqlCompileError> {
    let session = &recipe.session;
    let execution = &recipe.execution;
    let write_session = session
        .write_session()
        .ok_or("rewrite no-op has no write session")?;
    let write_target = session
        .write_target(recipe.cohort_id)
        .map_err(|e| e.to_string())?;
    let optimizer_settings = execution.optimizer_settings().clone();
    // The handle the plan states is the same one the session sealed, encoded
    // as the payload a plan carries rather than as the wire form the encoder
    // stamps. Both come from this session's own handle encoder, so a plan
    // cannot name a target the commit does not.
    let _sealed_write_targets = write_session
        .seal_write_targets()
        .map_err(|error| format!("seal distributed rewrite write targets: {error}"))?;
    let write_handle = write_session
        .encode_writer_handle_payload(write_target.handle())
        .map_err(|error| format!("encode distributed rewrite writer handle: {error}"))?;
    // The plan names each written field by the token the provider issued, and
    // the provider matches its own schema by name, so the names travel beside
    // the sealed handle -- from the very bindings the tokens came from.
    let write_target_facts = crate::query_execution::physical_encoding::WriteTargetFacts {
        session: write_session.as_ref(),
    };

    let plan = novarocks_sql::planning::dml::build_final_frozen_connector_write_plan(
        recipe.physical_plan.clone(),
        recipe.sink.clone(),
        write_target.ordinal(),
        write_target.statistics().requirements(),
        function_catalog.clone(),
        &optimizer_settings,
        novarocks_sql::planning::dml::DmlFinalWritePlanContext::new(
            novarocks_sql::planning::dml::DmlFinalPlanContext::new(
                crate::query_execution::physical_encoding::mint_plan_version(),
                crate::query_execution::contract::completed_plan_dop_domain(None)?,
                novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new([
                    novarocks_sql::planning::dml::DmlFinalizedProviderRead {
                        fact: read_fact,
                        read_budget: rewrite_cohort_scan_read_budget(),
                    },
                ])?,
            ),
            novarocks_sql::planning::dml::DmlFinalizedWriteTargetSet::try_new([
                novarocks_sql::planning::dml::DmlFinalizedWriteTarget {
                    ordinal: write_target.ordinal(),
                    handle: write_handle,
                },
            ])?,
        ),
        execution
            .sql_semantics()
            .sql_mode()
            .decimal_overflow_policy(),
        execution.sql_semantics().sql_mode().allow_throw_exception(),
        constant_policy,
        control,
    )?;
    let version = plan.plan().version();
    let candidate =
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_sql_program(
            plan, control,
        )
        .and_then(|candidate| {
            candidate.freeze_root_output(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
                ),
            )
        })
        .map_err(|error| error.to_string())?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_write_plan_borrowed(
        candidate,
        access,
        static_plan_carrier,
        constant_policy,
        &write_target_facts,
        control,
        capacity,
    )
    .map_err(crate::query_execution::mv_native_write::encode_compile_error)?;
    // Every fallible pairing/description check still borrows the unique access
    // held in the original CPU Source. The final receiving step only moves it.
    let encoded = encoded
        .prepare_access(access)
        .map_err(crate::query_execution::mv_native_write::encode_compile_error)?;
    let scheduling = encoded.scheduling_facts(version)?;
    let description =
        novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Write,
            encoded.candidate().clone(),
            scheduling
                .fragments
                .iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
            novarocks_query_application::preparation::OutputContract::CompletionOnly,
            novarocks_coordination_contract::ExecutionEffect::External,
            novarocks_coordination_contract::RecoveryMode::NoRecovery,
            Vec::new(),
            novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
            ),
            novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
            ),
        )?;
    Ok(RewriteCohortPlanParts {
        encoded,
        description,
        version,
    })
}

fn finish_rewrite_cohort_recipe(
    recipe: &RewriteCohortRecipe,
    parts: RewriteCohortPlanParts,
    access: novarocks_query_application::preparation::FinalPlanRuntimeAccess<
        crate::query_execution::provider_read_facts::FrozenProviderRead,
    >,
) -> Result<PreparedDistributedRewriteCohort, novarocks_sql::compiler::SqlCompileError> {
    // Same access was already checked by prepare_access; bind is infallible.
    let template = parts
        .encoded
        .bind_access(access)
        .into_attempt_template(parts.version);
    Ok(PreparedDistributedRewriteCohort {
        description: parts.description,
        template,
        execution: recipe.execution.clone(),
        write_session: recipe
            .session
            .write_session()
            .expect("same non-noop Rewrite recipe")
            .clone(),
    })
}

/// How much one rewrite cohort's scan may return in a batch.
///
/// A cohort has no session to lower this, so it states the contract's own
/// maximum, which means "unconstrained" rather than a number someone chose.
const fn rewrite_cohort_scan_read_budget() -> novarocks_physical_plan::ScanReadBudget {
    novarocks_physical_plan::ScanReadBudget {
        max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
        max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
    }
}

fn rewrite_target_identity(
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
) -> novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity {
    novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::new(
        session
            .rewrite()
            .descriptor()
            .instance_id
            .as_str()
            .to_string(),
        "__connector_rewrite",
        format!("cohort_{}", hex::encode(cohort_id.to_bytes())),
    )
}

fn rewrite_sink_mode(
    input: &ConnectorWriteInputShape,
) -> Result<novarocks_sql::planning::dml::DmlWriteSinkMode, String> {
    match input {
        ConnectorWriteInputShape::Data { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::Data)
        }
        ConnectorWriteInputShape::RowLineage { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::RowLineageData)
        }
        ConnectorWriteInputShape::PositionDelete { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::PositionDeletes)
        }
        ConnectorWriteInputShape::DeletionVector { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::DeletionVectors)
        }
        ConnectorWriteInputShape::EqualityDelete { .. } => {
            Ok(novarocks_sql::planning::dml::DmlWriteSinkMode::EqualityDeletes)
        }
    }
}

#[cfg(test)]
mod maintenance_attempt_context_tests {
    use std::time::Duration;

    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorTableObjectBindingFailure,
    };

    use super::{
        MaintenanceAttemptCancellationSource, MaintenanceAttemptContext,
        maintenance_target_rebind_from_connector_result,
    };
    use novarocks_table_maintenance::MaintenanceTargetRebind;
    use novarocks_table_maintenance::runtime::MaintenanceJobState;

    #[test]
    fn source_context_and_connector_request_share_one_cancellation_flag() {
        let source = MaintenanceAttemptCancellationSource::new();
        let attempt = source.context();
        let cloned = attempt.clone();
        let connector = attempt
            .connector_request_context()
            .expect("connector request context");

        assert!(!attempt.is_cancelled());
        assert!(!connector.is_cancelled());
        assert!(source.cancel());
        assert!(!source.cancel());
        assert!(attempt.is_cancelled());
        assert!(cloned.is_cancelled());
        assert!(connector.is_cancelled());
    }

    #[tokio::test]
    async fn maintenance_fence_wakes_existing_and_new_stop_waiters() {
        let source = MaintenanceAttemptCancellationSource::new();
        let context = source.context();
        let waiting = context.stop.stopped();
        assert!(source.cancel());
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("maintenance stop must wake an existing waiter");
        tokio::time::timeout(Duration::from_secs(1), context.stop.stopped())
            .await
            .expect("maintenance stop must wake a new waiter");
        assert!(context.is_cancelled());
    }

    #[test]
    fn uncancelled_attempts_do_not_share_state() {
        let first = MaintenanceAttemptContext::uncancelled();
        let second_source = MaintenanceAttemptCancellationSource::new();
        let second = second_source.context();

        assert!(second_source.cancel());
        assert!(!first.is_cancelled());
        assert!(second.is_cancelled());
    }

    #[test]
    fn attempt_context_preserves_request_cancellation_and_deadline() {
        let request_stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = crate::connector::connector_request_context(None, request_stop.view())
            .expect("request connector context");
        let source = MaintenanceAttemptCancellationSource::new();
        let combined = source
            .context()
            .connector_request_context_with_attempt(&request)
            .expect("combined connector context");

        assert_eq!(combined.deadline(), request.deadline());
        assert_eq!(
            combined.max_handle_payload_bytes(),
            request.max_handle_payload_bytes()
        );
        assert_eq!(
            combined.max_total_payload_bytes(),
            request.max_total_payload_bytes()
        );
        assert!(!combined.is_cancelled());

        request_stop.request_stop();
        assert!(combined.is_cancelled());

        let request = crate::connector::connector_request_context(
            None,
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
        )
        .expect("fresh request connector context");
        let source = MaintenanceAttemptCancellationSource::new();
        let combined = source
            .context()
            .connector_request_context_with_attempt(&request)
            .expect("combined connector context");
        assert!(source.cancel());
        assert!(combined.is_cancelled());
    }

    #[tokio::test]
    async fn maintenance_attempt_combines_request_and_fence_stop() {
        let request_owner = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = crate::connector::connector_request_context(None, request_owner.view())
            .expect("request connector context");
        let source = MaintenanceAttemptCancellationSource::new();
        let combined = source
            .context()
            .connector_request_context_with_attempt(&request)
            .expect("combined connector context");
        let stop = combined.stop().clone();
        request_owner.request_stop();
        tokio::time::timeout(Duration::from_secs(1), stop.stopped())
            .await
            .expect("request stop must wake maintenance provider");
        assert!(combined.is_cancelled());

        let request_owner = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = crate::connector::connector_request_context(None, request_owner.view())
            .expect("request connector context");
        let source = MaintenanceAttemptCancellationSource::new();
        let combined = source
            .context()
            .connector_request_context_with_attempt(&request)
            .expect("combined connector context");
        let stop = combined.stop().clone();
        assert!(source.cancel());
        tokio::time::timeout(Duration::from_secs(1), stop.stopped())
            .await
            .expect("fence stop must wake maintenance provider");
        assert!(!request_owner.is_stopped());
    }

    #[test]
    fn rebind_accepts_the_same_physical_object() {
        assert_eq!(
            maintenance_target_rebind_from_connector_result(Ok(())).expect("bound"),
            MaintenanceTargetRebind::Bound
        );
    }

    #[test]
    fn rebind_classifies_replacement_without_reading_the_error_message() {
        let error = ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "unrelated provider wording",
        );

        assert_eq!(
            maintenance_target_rebind_from_connector_result(Err(error)).expect("replacement"),
            MaintenanceTargetRebind::Replaced
        );
    }

    #[test]
    fn rebind_classifies_missing_without_reading_the_error_message() {
        let error = ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Missing,
            "unrelated provider wording",
        );

        assert_eq!(
            maintenance_target_rebind_from_connector_result(Err(error)).expect("missing"),
            MaintenanceTargetRebind::Missing
        );
    }

    #[test]
    fn rebind_keeps_provider_unsupported_as_an_error() {
        let error = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "provider does not implement target rebinding",
        );

        let error = maintenance_target_rebind_from_connector_result(Err(error))
            .expect_err("unsupported is not replacement");

        assert!(error.contains("Unsupported"));
    }

    #[test]
    fn optimize_target_replacement_has_a_stable_show_value() {
        assert_eq!(
            MaintenanceJobState::TargetReplaced.as_str(),
            "TARGET_REPLACED"
        );
    }
}

#[cfg(test)]
mod rewrite_selected_failure_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_query_application::{
        cancellation::QueryCancellationReason,
        session_error::{QueryServiceError, QueryServiceErrorKind},
    };

    #[test]
    fn rewrite_guard_retains_selected_input_but_does_not_infer_one_from_text() {
        let input = novarocks_query_application::session_outcome::cancellation_failure_input(
            QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(17)
                        .expect("nonzero fixture requester"),
            },
        );
        let terminal = rewrite_job::guard_terminal_error(
            QueryServiceError::from_execution_failure(input.clone()),
        );
        assert_eq!(terminal.execution_failure, Some(input.clone()));
        assert_eq!(terminal.compile_control, None);
        let projected = rewrite_job::computation_service_error(terminal);
        assert_eq!(projected.execution_failure(), Some(&input));
        let plain = rewrite_job::guard_terminal_error(QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            input.user_error().to_string(),
        ));
        assert_eq!(plain.execution_failure, None);
        assert_eq!(plain.compile_control, None);
        assert_eq!(plain.failure_cause, None);
    }

    #[test]
    fn rewrite_guard_preserves_original_provider_cause_and_fact_through_service_projection() {
        let source = novarocks_failure_contract::SharedFailureCause::new(
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                "private connector source diagnostic",
            ),
        );
        let terminal = rewrite_job::guard_terminal_error(QueryServiceError::from_failure_cause(
            source.clone(),
        ));
        assert_eq!(terminal.failure_cause.as_ref(), Some(&source));
        assert_eq!(
            terminal.selected_failure_fact(),
            Some(source.failure_fact())
        );
        assert_eq!(terminal.compile_control, None);
        let projected = rewrite_job::computation_service_error(terminal);
        assert_eq!(projected.failure_cause(), Some(&source));
        assert_eq!(
            projected.execution_failure().unwrap().fact(),
            source.failure_fact()
        );
    }
}
