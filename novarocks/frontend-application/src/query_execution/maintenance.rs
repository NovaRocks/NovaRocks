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

use novarocks_query_application::api::QueryResult;
use novarocks_spi::connector::{
    ConnectorRewriteCohortRead, ConnectorWriteCohortId, ConnectorWriteInputShape,
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

    async fn execute_action(
        &self,
        state: &MaintenanceActionState,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, TerminalError>;

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
    metadata_target: Option<(
        MaintenanceTarget,
        novarocks_spi::connector::ConnectorControlPlanningPin,
    )>,
}

impl RequestScopedMaintenanceEngine {
    /// Retain the selected provider fact from the same original target gate.
    pub(crate) fn resolve_target_with_provider(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<
        (
            MaintenanceTarget,
            novarocks_spi::connector::ConnectorProviderId,
        ),
        crate::catalog_application::resolver::TargetResolutionError,
    > {
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
        Ok((
            MaintenanceTarget {
                catalog: target.catalog,
                namespace: target.namespace,
                table: target.table,
            },
            target.provider_id,
        ))
    }

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
            metadata_target: None,
        }
    }

    fn with_metadata_target(
        mut self,
        target: MaintenanceTarget,
        planning: novarocks_spi::connector::ConnectorControlPlanningPin,
    ) -> Self {
        self.metadata_target = Some((target, planning));
        self
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

#[async_trait::async_trait]
impl TableMaintenanceEngine for RequestScopedMaintenanceEngine {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, crate::catalog_application::resolver::TargetResolutionError>
    {
        if let Some((target, _)) = &self.metadata_target {
            let retained_parts = [
                target.catalog.as_str(),
                target.namespace.as_str(),
                target.table.as_str(),
            ];
            if !name_parts.iter().map(String::as_str).eq(retained_parts) {
                return Err("metadata action changed its retained target"
                    .to_string()
                    .into());
            }
            return Ok(target.clone());
        }
        self.resolve_target_with_provider(name_parts, context)
            .map(|(target, _provider)| target)
    }

    async fn reject_user_action_on_mv(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
        use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};
        if let Some((original, planning)) = &self.metadata_target {
            if original != target {
                return Err(crate::mv::domain::readiness::application_error(
                    "metadata guard changed its retained target".to_owned(),
                ));
            }
            return self.reject_direct_rewrite_on_mv(target, planning).await;
        }
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

    async fn execute_action(
        &self,
        state: &MaintenanceActionState,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, TerminalError> {
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
                planning_pin: session.rewrite().planning_pin(),
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
    sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
    static_plan_carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
    constant_policy: novarocks_functions::ConstantValidation,
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
        sealed_functions,
        &optimizer_settings,
        novarocks_sql::planning::dml::DmlFinalWritePlanContext::new(
            novarocks_sql::planning::dml::DmlFinalPlanContext::new(
                crate::query_execution::physical_encoding::mint_plan_version(),
                crate::query_execution::contract::completed_plan_dop_domain(None)?,
                novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new([read_fact])?,
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
        _ => Err("OPTIMIZE requires a data-file writer input".to_string()),
    }
}

#[cfg(test)]
mod maintenance_target_rebind_tests {
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorTableObjectBindingFailure,
    };
    use novarocks_table_maintenance::MaintenanceTargetRebind;
    use novarocks_table_maintenance::runtime::MaintenanceJobState;

    use super::maintenance_target_rebind_from_connector_result;

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
    use novarocks_query_application::api::ExecutionFailurePresentation;
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
