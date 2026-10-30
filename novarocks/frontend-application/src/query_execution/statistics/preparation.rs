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

//! Statistics preparation borrows the original statement's fact and CPU slots.

use super::{
    StatisticsCollectionProgram, StatisticsRelationIdentity, admit_statistics_scan_binding,
    contract_violation, statistics_scan_read_budget,
};
use crate::query_execution::{
    artifact::PreparedDistributedAttemptTemplate,
    completion_facts::{CompletionFactOwners, FrontendCompletionCustody, StatementFactScope},
    contract::{DistributedQueryError, DistributedQueryErrorKind},
    internal_consumer::decoded::{DecodedConsumerRuntime, StatisticsComputationOutput},
    logical_execution::{InitialComputationReservation, PreparedLogicalComputation},
    package_freeze::CompiledPackageCarrier,
    provider_read_facts::FrozenProviderRead,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::{QueryExecutionError, ReservedExecutionIdentity},
    preparation::{
        CompletedPhysicalPlanCandidate, FinalPlanRuntimeAccess, FrozenExecutionDescription,
        ReadAccessSink,
    },
};
use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorRequestContext};
use novarocks_workload_control::{WorkError, WorkScope};
use std::sync::Arc;

/// Process handles only. The original action owns both stable receiving slots
/// and its CompletionCustody; this value creates no second statement owner.
pub(crate) struct StatisticsComputationPlanningServices<'a> {
    pub(crate) completion: CompletionFactOwners,
    pub(crate) blocking: &'a ConnectorBlockingIoSupervisor,
    pub(crate) functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    pub(crate) constant_policy: novarocks_functions::ConstantPolicy,
    pub(crate) static_plan_carrier: CompiledPackageCarrier,
    pub(crate) wake: Arc<dyn crate::task_execution::status_intake::StatusIntakeWake>,
}

/// Inline in the original Action PlanSlot, before any borrowed resolution.
/// No capability belongs to the disposable planning Future. After actual
/// Provider close the same unpaired capabilities remain here for its owner.
#[derive(Default)]
pub(crate) struct StatisticsPreparationCustody {
    sink: Option<ReadAccessSink<FrozenProviderRead>>,
    access: Option<FinalPlanRuntimeAccess<FrozenProviderRead>>,
    returned:
        Option<Vec<novarocks_query_application::preparation::FrozenReadAccess<FrozenProviderRead>>>,
    started: bool,
    closed: bool,
}
impl StatisticsPreparationCustody {
    pub(crate) fn receive_access_in(
        &mut self,
        source: &mut Option<FinalPlanRuntimeAccess<FrozenProviderRead>>,
    ) {
        if source.is_some() {
            assert!(self.sink.is_none() && self.access.is_none() && self.returned.is_none());
            self.access = source.take();
        }
    }
    pub(crate) async fn close(
        &mut self,
        provider: &FrontendCompletionCustody,
    ) -> Result<(), novarocks_query_application::api::StatementNativeHealthFailure> {
        provider.close().await?;
        // The Provider has truly joined: a late freeze can no longer deposit.
        // Take into the SAME original receiving slot, never clear/drop entries
        // in a temporary Future or a ReadAccessSink destructor under its lock.
        if let Some(sink) = self.sink.take() {
            assert!(self.returned.is_none() && self.access.is_none());
            self.returned = Some(sink.into_taken());
        }
        if let Some(access) = self.access.take() {
            assert!(self.returned.is_none());
            self.returned = Some(access.into_taken());
        }
        self.closed = true;
        Ok(())
    }
}

pub(crate) type StatisticsPreparationJob =
    crate::query_execution::internal_result_cpu::StatisticsPlanCpuJob;
pub(crate) type StatisticsPreparationOutcome =
    Result<PreparedStatisticsComputationAssembly, StatisticsPreparationFailure>;

/// Domain refusal and the original CPU admission/control fact stay distinct.
/// Raw worker/forwarder/Drop failures remain in the same job's health packet.
pub(crate) enum StatisticsPreparationFailure {
    Domain(DistributedQueryError),
    Cpu(Box<novarocks_query_application::execution_error::ExecutionFailureInput>),
}

/// A signed plan and its exact Provider program. This is not a computation
/// result: the original decoded consumer must still prove both receipts.
pub(crate) struct PreparedStatisticsComputationAssembly {
    description: FrozenExecutionDescription,
    template: PreparedDistributedAttemptTemplate,
    options: novarocks_proto_codec::lifecycle::QueryOptions,
    program: StatisticsCollectionProgram,
}
impl PreparedStatisticsComputationAssembly {
    pub(crate) fn prepare_computation(
        self,
        initial: InitialComputationReservation,
        runtime: DecodedConsumerRuntime,
    ) -> Result<
        (
            PreparedLogicalComputation,
            ReservedExecutionIdentity,
            StatisticsComputationOutput,
        ),
        QueryExecutionError,
    > {
        let (prepared, output) =
            crate::query_execution::internal_consumer::decoded::prepare_statistics_computation(
                self.description,
                self.template,
                Arc::new(
                    crate::query_execution::contract::ResolvedQueryOptions::from_upstream(Some(
                        self.options,
                    )),
                ),
                &self.program,
                runtime,
            )?;
        let (prepared, reserved) = prepared
            .with_initial_reservation(initial)
            .map_err(|error| {
                QueryExecutionError::from_execution(error.into_execution_failure_input())
            })?;
        Ok((prepared, reserved, output))
    }
}

/// Mutable borrows of the original Action PlanSlot's stable receiving cells.
pub(crate) struct StatisticsPreparationCells<'a> {
    pub(crate) custody: &'a mut StatisticsPreparationCustody,
    pub(crate) job: &'a mut Option<StatisticsPreparationJob>,
    pub(crate) outcome: &'a mut Option<StatisticsPreparationOutcome>,
}

/// The borrowed future owns neither the Provider join nor the CPU job. Its
/// caller keeps `provider`, `job` and `outcome` in the same stable action and
/// joins their actual exits even when this preparation future is cancelled.
/// The outer error is pre-CPU preparation; the original typed CPU failure stays
/// in `outcome`, and is never reclassified from diagnostic text.
pub(crate) async fn prepare_statistics_computation_in(
    services: StatisticsComputationPlanningServices<'_>,
    execution: &QueryExecutionContext,
    context: ConnectorRequestContext,
    identity: &StatisticsRelationIdentity,
    program: StatisticsCollectionProgram,
    planning_lease: ConnectorControlPlanningLease,
    stage: &WorkScope,
    provider: &FrontendCompletionCustody,
    cells: StatisticsPreparationCells<'_>,
) -> Result<(), DistributedQueryError> {
    let StatisticsPreparationCells {
        custody,
        job,
        outcome,
    } = cells;
    if job.is_some() || outcome.is_some() || custody.started || custody.closed {
        return Err(contract_violation(
            "statistics preparation already owns its original CPU slot",
        ));
    }
    let capacity = computation_capacity(execution, stage).map_err(work_failure)?;
    let (bindings, scan) = prepare_statistics_scan(execution, identity, &program, planning_lease)?;
    let need = novarocks_sql::planning::dml::statistics_provider_read_need(&scan)
        .map_err(contract_violation)?;
    let scope = StatementFactScope::governed_read_io(
        bindings,
        context,
        Some(&identity.catalog),
        stage,
        capacity.try_clone().map_err(work_failure)?,
    )
    .map_err(work_failure)?;
    crate::query_execution::completion_facts::install_frontend_fact_source(
        provider,
        services.completion,
        scope,
        crate::query_execution::compiler::typed_connector_session().map_err(contract_violation)?,
    )
    .map_err(contract_violation)?;
    custody.sink = Some(ReadAccessSink::new());
    custody.started = true;
    let mut facts = provider
        .resolve_provider_reads(
            &[need],
            custody.sink.as_ref().expect("stable original Stats sink"),
        )
        .await
        .map_err(contract_violation)?;
    if facts.len() != 1 {
        return Err(contract_violation(
            "statistics preparation requires its one exact scan fact",
        ));
    }
    let fact = facts.pop().expect("one original scan fact");
    match custody
        .sink
        .take()
        .expect("same original Stats sink")
        .try_into_access()
    {
        Ok(access) => custody.access = Some(access),
        Err((error, returned)) => {
            custody.returned = Some(returned);
            return Err(contract_violation(error.to_string()));
        }
    }
    // The installed Provider remains in its original stable custody; all
    // potentially expensive pure work is in the existing finite CPU owner.
    let cpu = services
        .blocking
        .provider_creator_cpu()
        .map_err(work_failure)?;
    let source = cpu.provider_creator_source(stage).map_err(work_failure)?;
    let window = capacity.try_window_alias().map_err(work_failure)?;
    *job = Some(cpu.submit_statistics_plan(
        StatisticsPlanCpuInput {
            execution: execution.clone(),
            scan,
            fact,
            access: custody.access.take(),
            program: Some(program),
            functions: services.functions,
            constant_policy: services.constant_policy,
            static_plan_carrier: services.static_plan_carrier,
            staged: None,
            started: false,
        },
        window,
        source,
        execution.cancellation().clone(),
        services.blocking.runtime(),
        services.wake,
    ));
    // Borrowed cancellation never owns either the worker receipt or this job.
    // Actual physical exit and the same forwarder join precede result transfer.
    std::future::poll_fn(|cx| {
        job.as_mut()
            .expect("same statistics CPU job")
            .poll_finish_in(cx, outcome, custody)
    })
    .await
    .map_err(|_original| {
        DistributedQueryError::new(
            DistributedQueryErrorKind::Failed,
            "original Statistics CPU health remains in its source",
        )
    })?;
    Ok(())
}

fn computation_capacity(
    execution: &QueryExecutionContext,
    stage: &WorkScope,
) -> Result<QueryResultCapacityBinding, WorkError> {
    let capacity = execution.result_capacity().ok_or(WorkError::NotReady)?;
    if !capacity.is_root_backed_internal()
        || !capacity.window_alias().is_for_scope(stage)
        || capacity
            .window_alias()
            .internal_manifest_binding()?
            .is_none()
    {
        return Err(WorkError::Conflict);
    }
    stage.check()?;
    Ok(capacity.clone())
}
fn work_failure(error: WorkError) -> DistributedQueryError {
    DistributedQueryError::from_source(
        DistributedQueryErrorKind::Rejected,
        novarocks_failure_contract::SharedFailureCause::new(error),
    )
}

pub(crate) struct StatisticsPlanCpuInput {
    execution: QueryExecutionContext,
    scan: novarocks_sql::planning::dml::StatisticsConnectorScan,
    fact: novarocks_sql::compiler::ProviderReadFact,
    access: Option<FinalPlanRuntimeAccess<FrozenProviderRead>>,
    program: Option<StatisticsCollectionProgram>,
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: CompiledPackageCarrier,
    staged: Option<Result<StatisticsPlanParts, DistributedQueryError>>,
    started: bool,
}
impl StatisticsPlanCpuInput {
    #[cfg(test)]
    pub(crate) fn fixture_with_access(
        binding: novarocks_sql::binding::SqlTableBindingId,
        fact: novarocks_sql::compiler::ProviderReadFact,
        access: FinalPlanRuntimeAccess<FrozenProviderRead>,
    ) -> Self {
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorTableHandle, StatisticsArtifactIdentity,
            StatisticsRequiredAggregation, StatisticsScanColumn,
        };
        let program = StatisticsCollectionProgram::try_new(
            ConnectorTableHandle::try_new(
                ConnectorInstanceId::parse("catalog.analytics").unwrap(),
                bytes::Bytes::from_static(b"orders"),
            )
            .unwrap(),
            // Pin the fixture's exact version so its intended failure is the
            // unavailable aggregate, after the original read was captured.
            Some(1),
            vec![
                StatisticsRequiredAggregation::try_new(
                    StatisticsScanColumn::try_new(
                        0,
                        "k",
                        novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),
                    )
                    .unwrap(),
                    "$uninstalled_statistics_aggregate",
                    StatisticsArtifactIdentity::try_new(vec![1], "test/blob").unwrap(),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        Self {
            execution: QueryExecutionContext::new(
                novarocks_types::ClusterRole::Fe,
                novarocks_query_application::api::BackendTopologySnapshot::empty(1),
                None,
                cancellation.view(),
                novarocks_sql::compiler::SessionOptimizerSettings::default(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            ),
            scan: novarocks_sql::planning::dml::StatisticsConnectorScan {
                binding,
                catalog: "catalog.analytics".into(),
                namespace: "db".into(),
                table: "orders".into(),
                version_ordinal: program.read_version_ordinal(),
                columns: program.scan_columns(),
            },
            fact,
            access: Some(access),
            program: Some(program),
            functions: Arc::new(
                novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                    .unwrap(),
            ),
            constant_policy: crate::application::test_constant_policy(),
            static_plan_carrier: CompiledPackageCarrier::for_test(
                novarocks_physical_plan::FragmentPackageAdmission {
                    plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
                    source_retained_bytes: 64 << 20,
                    property_projection_limits:
                        novarocks_physical_plan::PropertyProofProjectionLimits {
                            max_request_bytes: 16 << 20,
                            max_coexisting_bytes: 256 << 20,
                            max_projection_work: 16 << 20,
                        },
                },
                novarocks_plan_codec::physical_package_v2::test_support::encode_limits(),
            ),
            staged: None,
            started: false,
        }
    }
    /// Every original capability/context remains in this stable Source. The
    /// closed worker only borrows it; panic cannot destroy its unique access.
    pub(crate) fn run_once(&mut self) {
        assert!(!self.started, "one original statistics planner invocation");
        self.started = true;
        self.staged = Some(build_statistics_plan_parts(
            &self.execution,
            self.scan.clone(),
            self.fact.clone(),
            self.access.as_ref().expect("original read access"),
            self.program.as_ref().expect("original statistics program"),
            self.functions.as_ref(),
            self.constant_policy,
            &self.static_plan_carrier,
        ));
    }
    /// Return unused linear capabilities before any original input retirement.
    pub(crate) fn return_access_in(&mut self, custody: &mut StatisticsPreparationCustody) {
        custody.receive_access_in(&mut self.access);
    }
    /// Called only after the same actual physical receipt and forwarder join.
    pub(crate) fn take_result_in(
        &mut self,
        destination: &mut Option<StatisticsPreparationOutcome>,
    ) -> Result<(), &'static str> {
        if destination.is_some() {
            return Err("statistics destination is occupied");
        }
        let parts = self
            .staged
            .take()
            .ok_or("statistics worker has no result")?;
        *destination = Some(
            parts
                .map(|parts| {
                    finish_statistics_assembly(
                        parts,
                        self.access.take().expect("same original access"),
                        self.program.take().expect("same original program"),
                    )
                })
                .map_err(StatisticsPreparationFailure::Domain),
        );
        Ok(())
    }
}

pub(super) fn prepare_statistics_scan(
    execution: &QueryExecutionContext,
    identity: &StatisticsRelationIdentity,
    program: &StatisticsCollectionProgram,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<
    (
        Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
        novarocks_sql::planning::dml::StatisticsConnectorScan,
    ),
    DistributedQueryError,
> {
    let live = execution.topology().targets().len();
    if live == 0 {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "statistics collection requires at least one live backend",
        ));
    }
    if planning_lease.binding().descriptor().instance_id != *program.table().owner() {
        return Err(contract_violation(
            "statistics collection planning lease does not own its resolved table handle",
        ));
    }
    if planning_lease.binding().descriptor().instance_id.as_str() != identity.catalog {
        return Err(contract_violation(format!(
            "statistics collection of `{}` was planned through connector instance `{}`",
            identity.fqn(),
            planning_lease.binding().descriptor().instance_id.as_str()
        )));
    }
    let bindings = Arc::new(
        crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
            .map_err(contract_violation)?,
    );
    let binding =
        admit_statistics_scan_binding(bindings.as_ref(), identity, program, planning_lease)?;
    let scan = novarocks_sql::planning::dml::StatisticsConnectorScan {
        binding,
        catalog: identity.catalog.clone(),
        namespace: identity.namespace.clone(),
        table: identity.table.clone(),
        version_ordinal: program.read_version_ordinal(),
        columns: program.scan_columns(),
    };

    Ok((bindings, scan))
}

struct StatisticsPlanParts {
    encoded: crate::query_execution::physical_encoding::PreparedStaticPlan,
    description: FrozenExecutionDescription,
    options: novarocks_proto_codec::lifecycle::QueryOptions,
    version: novarocks_physical_plan::PlanVersionId,
}
fn build_statistics_plan_parts(
    execution: &QueryExecutionContext,
    scan: novarocks_sql::planning::dml::StatisticsConnectorScan,
    fact: novarocks_sql::compiler::ProviderReadFact,
    access: &FinalPlanRuntimeAccess<FrozenProviderRead>,
    program: &StatisticsCollectionProgram,
    functions: &novarocks_functions::EngineFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: &CompiledPackageCarrier,
) -> Result<StatisticsPlanParts, DistributedQueryError> {
    let completion_control =
        crate::query_execution::planning::sql_compile_control_from_execution(execution);
    let plan = novarocks_sql::planning::dml::build_final_statistics_connector_plan(
        scan,
        program.required_aggregations(),
        functions,
        execution.optimizer_settings(),
        novarocks_sql::planning::dml::DmlFinalPlanContext::new(
            crate::query_execution::physical_encoding::mint_plan_version(),
            crate::query_execution::contract::completed_plan_dop_domain(None)
                .map_err(contract_violation)?,
            novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new([
                novarocks_sql::planning::dml::DmlFinalizedProviderRead {
                    fact,
                    read_budget: statistics_scan_read_budget(),
                },
            ])
            .map_err(contract_violation)?,
        ),
        execution
            .sql_semantics()
            .sql_mode()
            .decimal_overflow_policy(),
        execution.sql_semantics().sql_mode().allow_throw_exception(),
        constant_policy,
        &completion_control,
    )
    .map_err(DistributedQueryError::from_compile)?;
    let version = plan.plan().version();
    let candidate = CompletedPhysicalPlanCandidate::for_sql_program(plan, &completion_control)
        .and_then(|candidate| {
            candidate.freeze_root_output(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                ),
            )
        })
        .map_err(|error| contract_violation(error.to_string()))?;
    let output =
        novarocks_query_application::preparation::OutputContract::from_completed_candidate(
            novarocks_query_application::api::QueryExecutionKind::Statistics,
            &candidate,
        )
        .map_err(contract_violation)?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan_borrowed(
        candidate,
        access,
        static_plan_carrier,
        constant_policy,
        &completion_control,
    )
    .map_err(DistributedQueryError::from_encode)?;
    // All fallible signed-description/control work precedes the linear access
    // move. The CPU input remains its sole owner on any ordinary error/panic.
    let encoded = encoded
        .prepare_access(access)
        .map_err(DistributedQueryError::from_encode)?;
    let candidate = encoded.candidate().clone();
    let scheduling = encoded
        .scheduling_facts(version)
        .map_err(contract_violation)?;
    let description = FrozenExecutionDescription::for_completed_plan(
        novarocks_query_application::api::QueryExecutionKind::Statistics,
        candidate,
        scheduling
            .fragments
            .iter()
            .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
            .collect(),
        output,
        novarocks_coordination_contract::ExecutionEffect::None,
        novarocks_coordination_contract::RecoveryMode::NoRecovery,
        Vec::new(),
        novarocks_query_application::preparation::FrozenCostEstimate::unknown(
            novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
        ),
        novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
            novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
        ),
    )
    .map_err(contract_violation)?;
    let options = crate::query_execution::contract::synthetic_statement_query_options(execution);
    let resolved = crate::query_execution::contract::ResolvedQueryOptions::from_upstream(Some(
        options.clone(),
    ));
    description
        .validate_legacy_intrinsic_allow_throw_exception(
            resolved.runtime_options().allow_throw_exception,
            &completion_control,
        )
        .map_err(DistributedQueryError::from_compile)?;
    Ok(StatisticsPlanParts {
        encoded,
        description,
        options,
        version,
    })
}
fn finish_statistics_assembly(
    parts: StatisticsPlanParts,
    access: FinalPlanRuntimeAccess<FrozenProviderRead>,
    program: StatisticsCollectionProgram,
) -> PreparedStatisticsComputationAssembly {
    let StatisticsPlanParts {
        encoded,
        description,
        options,
        version,
    } = parts;
    // The exact pairing was validated while borrowing this same access. No
    // fallible validation or Provider callback follows its one ownership move.
    let template = encoded.bind_access(access).into_attempt_template(version);
    PreparedStatisticsComputationAssembly {
        description,
        template,
        program,
        options,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_preparation_rejects_a_legacy_window_without_the_original_computation() {
        use crate::task_execution::blocking_io::tests::{admitted, settle_control};
        use novarocks_query_application::{
            api::BackendTopologySnapshot, cancellation::QueryCancellationSource,
        };
        let (control, root, window) = admitted();
        let stage = root.owner.scope();
        let cancellation = QueryCancellationSource::new();
        let execution = QueryExecutionContext::new(
            novarocks_types::ClusterRole::Fe,
            BackendTopologySnapshot::empty(1),
            None,
            cancellation.view(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        );
        assert!(matches!(
            computation_capacity(&execution, &stage),
            Err(WorkError::NotReady)
        ));
        let binding = QueryResultCapacityBinding::try_new(&stage, window.retain_alias()).unwrap();
        let execution = execution.with_result_capacity(binding).unwrap();
        assert!(matches!(
            computation_capacity(&execution, &stage),
            Err(WorkError::Conflict)
        ));
        // This is a real admitted legacy window, not an invalid handle. It
        // cannot substitute for the stage's original positive P1 grant.
        assert_eq!(control.snapshot().result_windows.held_positions[2], 1);
        drop(execution);
        drop(window);
        root.owner.complete();
        root.business.release();
        settle_control(&control);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }
}
