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

//! Request/result pairing for one admitted distributed query.
//!
//! The application can submit the request once and then consume its paired
//! completion. It cannot construct a replacement completion for a different
//! query intent or rehydrate planning inputs after admission.

use crate::query_execution::lifecycle_plan::{
    AttemptCredentialLeaseCollector, AttemptTerminalCredentialResolver, QueryCredentialLeases,
};
use novarocks_query_application::api::build_string_query_result;
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorRequestScope};
use novarocks_types::QueryExecutionId;
#[cfg(test)]
use novarocks_types::{AttemptId, QueryId};
use std::sync::Arc;

/// FE-local identity for one distributed attempt.
///
/// The QA process owner reserves the actual identity; `from_reserved` binds
/// this first-attempt credential custody to that same move-only reservation.
/// Credential collection and terminal material remain Frontend-owned.
pub struct QueryAttemptReservation {
    credential_collector: Arc<AttemptCredentialLeaseCollector>,
    request_scope: ConnectorRequestScope,
    #[cfg(novarocks_dev_controls)]
    test_nomination:
        std::sync::OnceLock<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
}

impl QueryAttemptReservation {
    #[cfg(test)]
    pub(crate) fn retry(
        query_id: QueryId,
        attempt: u32,
    ) -> Result<Self, crate::query_execution::contract::DistributedQueryError> {
        Self::for_round(query_id, attempt)
    }

    #[cfg(test)]
    fn for_round(
        query_id: QueryId,
        attempt: u32,
    ) -> Result<Self, crate::query_execution::contract::DistributedQueryError> {
        let execution_id = QueryExecutionId::new(
            query_id,
            AttemptId::new(u64::from(attempt)).map_err(|error| {
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                    error.to_string(),
                )
            })?,
        )
        .map_err(|error| {
            crate::query_execution::contract::DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            )
        })?;
        Ok(Self::for_execution(execution_id))
    }

    /// Retains the exact initial identity allocated by the QA process owner.
    /// The original move-only token still has to pass QA's issuer check at start.
    pub(super) fn from_reserved(
        reserved: &novarocks_query_application::api::ReservedExecutionIdentity,
    ) -> Result<Self, crate::query_execution::contract::DistributedQueryError> {
        let execution_id = reserved.execution();
        if execution_id.attempt_id().get() != 1 {
            return Err(
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
                    "initial computation reservation must name the original first attempt",
                ),
            );
        }
        Ok(Self::for_execution(execution_id))
    }

    fn for_execution(execution_id: QueryExecutionId) -> Self {
        Self {
            credential_collector: AttemptCredentialLeaseCollector::new(execution_id),
            request_scope: ConnectorRequestScope::new(),
            #[cfg(novarocks_dev_controls)]
            test_nomination: std::sync::OnceLock::new(),
        }
    }

    pub(super) fn terminal_storage_resolver(&self) -> Arc<AttemptTerminalCredentialResolver> {
        self.credential_collector.terminal_storage_resolver()
    }

    pub(super) fn seal_for_terminal(
        &self,
    ) -> Result<(), crate::query_execution::contract::DistributedQueryError> {
        self.credential_collector.seal_for_terminal().map(|_| ())
    }

    pub(crate) fn credential_lease_sink(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorVendedCredentialLeaseSink> {
        self.credential_collector.sink()
    }

    pub(crate) fn credential_storage_resolver(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorStorageResolver> {
        self.credential_collector.storage_resolver()
    }

    /// Bind one application request context to this exact candidate attempt.
    /// The sidecar is local-only and fresh for every retry, while the lease
    /// collector remains the sole owner of response-local secret material.
    pub(crate) fn connector_request_context(
        &self,
        context: ConnectorRequestContext,
        #[cfg(novarocks_dev_controls)] nomination: Option<
            &Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
        >,
    ) -> Result<ConnectorRequestContext, String> {
        let context = context
            .with_request_scope(self.request_scope.clone())
            .with_storage_resolver(self.credential_storage_resolver())
            .with_vended_credential_lease_sink(self.credential_lease_sink());
        #[cfg(novarocks_dev_controls)]
        if let Some(nomination) = nomination {
            nomination.validate().map_err(|error| error.to_string())?;
            if let Err(candidate) = self.test_nomination.set(nomination.clone())
                && self.test_nomination.get() != Some(&candidate)
            {
                return Err("attempt reservation already names another statement".into());
            }
            let installed =
                context.request_scope_extension_or_insert_with(|| nomination.as_ref().clone());
            if installed != *nomination {
                return Err("attempt scope has a foreign statement nomination".into());
            }
        }
        Ok(context)
    }

    pub(crate) fn has_collected_credential_leases(&self) -> bool {
        self.credential_collector.has_collected_leases()
    }

    /// The initializer borrows its original reservation through the one-shot
    /// Native drain. The reservation continues to anchor terminal material.
    pub(crate) fn drain_credential_leases(
        &self,
    ) -> Result<QueryCredentialLeases, crate::query_execution::contract::DistributedQueryError>
    {
        self.credential_collector.drain_credential_leases()
    }
}

impl Drop for QueryAttemptReservation {
    fn drop(&mut self) {
        // Abandonment closes collection/Native transfer rights. It does not
        // prove any asynchronous initializer or external operation has exited.
        // An explicit terminal owner observes validation errors before Drop.
        let _ = self.seal_for_terminal();
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "Prepared query operations preserve their direct immediate and distributed boundary payloads."
)]
pub enum PreparedQueryOperation {
    Immediate(PreparedImmediateQuery),
    /// Plain read query owned by Query Application. The carrier retains the
    /// frozen logical description and the reusable Native attempt template;
    /// it cannot be downgraded to one legacy coordinator round.
    LogicalRead(PreparedLogicalRead),
}

impl PreparedQueryOperation {
    /// Build a completed statement result without exposing the immediate
    /// operation constructor to application callers.
    pub fn immediate(result: StatementResult) -> Self {
        Self::Immediate(PreparedImmediateQuery::new(result))
    }
}

/// Move-only plain SELECT handoff into the Query Application runtime.
///
/// The three fields originate in one semantic/native finalization. Keeping
/// them private prevents callers from pairing an immutable description with a
/// different attempt template or resolved option set.
pub struct PreparedLogicalRead {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    attempt_template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    options: Arc<crate::query_execution::contract::ResolvedQueryOptions>,
}

impl PreparedLogicalRead {
    pub(crate) fn new(
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        attempt_template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
        options: Arc<crate::query_execution::contract::ResolvedQueryOptions>,
    ) -> Self {
        Self {
            description,
            attempt_template,
            options,
        }
    }

    pub(crate) fn validate_read_start(&self) -> Result<(), &'static str> {
        use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
        use novarocks_query_application::preparation::ExecutionFinishBoundary;
        if self.description.finish_boundary() != ExecutionFinishBoundary::ClientEof
            || self.description.effect() != ExecutionEffect::None
            || self.description.recovery() != RecoveryMode::RestartAttemptBeforeVisibility
            || !self
                .description
                .matches_plan_seal(self.attempt_template.native_manifest_template().plan())
        {
            return Err("ordinary Read has a foreign output, recovery, or plan seal");
        }
        Ok(())
    }
    pub(crate) fn row_carrier(
        &self,
    ) -> Result<novarocks_query_application::api::ResultRowCarrier, String> {
        self.description.row_carrier()
    }

    pub(crate) fn scalar_schema(&self) -> Option<&novarocks_result_contract::ScalarSchema> {
        self.description.scalar_schema()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        novarocks_query_application::preparation::FrozenExecutionDescription,
        crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
        Arc<crate::query_execution::contract::ResolvedQueryOptions>,
    ) {
        (self.description, self.attempt_template, self.options)
    }
}

pub struct PreparedImmediateQuery {
    result: StatementResult,
}

impl PreparedImmediateQuery {
    pub(crate) fn new(result: StatementResult) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> StatementResult {
        self.result
    }
}

/// Core-owned completion formatter paired with a distributed request.
pub struct PreparedQueryCompletion {
    formatter: PreparedQueryFormatter,
}

enum PreparedQueryFormatter {
    Profile(PreparedProfileFormatter),
}

struct PreparedProfileFormatter {
    plan: ProfilePlan,
    control: novarocks_sql::compiler::SqlCompileControl,
    planning_elapsed: std::time::Duration,
    execution_started_at: std::time::Instant,
}

enum ProfilePlan {
    Completed {
        plan: std::sync::Arc<novarocks_physical_plan::PhysicalPlan>,
        annotations: std::sync::Arc<[novarocks_sql::compiler::SqlDisplayAnnotation]>,
    },
}

impl PreparedQueryCompletion {
    pub(crate) fn completed_profile(
        plan: std::sync::Arc<novarocks_physical_plan::PhysicalPlan>,
        annotations: std::sync::Arc<[novarocks_sql::compiler::SqlDisplayAnnotation]>,
        planning_elapsed: std::time::Duration,
        execution_started_at: std::time::Instant,
        control: novarocks_sql::compiler::SqlCompileControl,
    ) -> Self {
        Self {
            formatter: PreparedQueryFormatter::Profile(PreparedProfileFormatter {
                plan: ProfilePlan::Completed { plan, annotations },
                control,
                planning_elapsed,
                execution_started_at,
            }),
        }
    }
}

fn render_profile_result(
    formatter: PreparedProfileFormatter,
    output_rows: u64,
    fragment_profiles: Vec<crate::query_execution::profile::FragmentProfileTree>,
    diagnostics: Vec<String>,
) -> Result<novarocks_query_application::api::QueryResult, String> {
    // Per-fragment attribution is read while each tree still carries the
    // fragment its producer named; every other summary below is a counter sum
    // over the trees themselves and needs no attribution.
    let per_fragment =
        crate::query_execution::profile::collect_per_fragment_profile_summaries(&fragment_profiles);
    let reported_runtime = fragment_profiles.iter().any(|profile| {
        profile.fragment_root_plan_node_id().is_some() && profile.has_runtime_statistics()
    });
    let fragment_profiles = fragment_profiles
        .into_iter()
        .map(crate::query_execution::profile::FragmentProfileTree::into_tree)
        .collect::<Vec<_>>();
    let actuals =
        crate::query_execution::profile::collect_actuals_by_plan_node_id_from_profile_trees(
            &fragment_profiles,
        );
    let profile_summary =
        crate::query_execution::profile::collect_distributed_profile_summary_from_profile_trees(
            &fragment_profiles,
        );
    let mut lines = Vec::new();
    lines.push(format!(
        "Planning: {} / Execution: {} / Rows: {}",
        format_explain_analyze_duration(formatter.planning_elapsed),
        format_explain_analyze_duration(formatter.execution_started_at.elapsed()),
        output_rows
    ));
    if reported_runtime || diagnostics.is_empty() {
        lines.push(format_distributed_profile_summary(&profile_summary));
    }
    lines.extend(diagnostics);
    if let Some(apply) =
        crate::query_execution::profile::collect_native_runtime_filter_apply_from_profile_trees(
            &fragment_profiles,
        )
    {
        lines.push(apply.to_string());
    }
    if let Some(apply) =
        crate::query_execution::profile::collect_native_scan_conjunct_apply_from_profile_trees(
            &fragment_profiles,
        )
    {
        lines.push(apply.to_string());
    }
    for (names, label) in [
        (
            ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES,
            "ProfileCounters",
        ),
        (CONNECTOR_FILE_COUNTER_NAMES, "ConnectorFileMetrics"),
        (TYPED_CONNECTOR_COUNTER_NAMES, "TypedConnectorMetrics"),
        (
            FRONTEND_SPLIT_ASSIGNMENT_COUNTER_NAMES,
            "FrontendSplitAssignmentMetrics",
        ),
    ] {
        if let Some(counters) =
            crate::query_execution::profile::format_counter_sums_from_profile_trees(
                &fragment_profiles,
                names,
                label,
            )
        {
            lines.push(counters);
        }
    }
    lines.extend(match formatter.plan {
        ProfilePlan::Completed { plan, annotations } => render_completed_profile(
            &plan,
            &annotations,
            &actuals,
            &per_fragment,
            &formatter.control,
        )?,
    });
    build_string_query_result("Explain String", lines)
}

impl PreparedQueryCompletion {
    /// Render the accepted computation with its independent, exact Native
    /// diagnostic snapshot. A missing diagnostic is not a missing Ready cut.
    pub(crate) fn complete_profile_observation(
        self,
        ready: &crate::query_execution::internal_consumer::profile::ReadyProfileComputation,
        observation: Result<
            &crate::task_execution::profile_diagnostics::ProfileDiagnosticSnapshot,
            crate::query_execution::internal_consumer::profile::ProfileSnapshotMissing,
        >,
    ) -> Result<novarocks_query_application::api::QueryResult, String> {
        use crate::task_execution::profile_diagnostics::ProfileTaskDiagnostic;
        let PreparedQueryFormatter::Profile(formatter) = self.formatter;
        let ProfilePlan::Completed { plan, .. } = &formatter.plan;
        let mut profiles = crate::query_execution::profile::ProfileTerminalBuilder::new();
        let mut diagnostics = Vec::new();
        match observation {
            Ok(snapshot) => {
                if snapshot.ready != ready.ready_token()
                    || !snapshot
                        .tasks
                        .iter()
                        .map(|(task, _)| *task)
                        .eq(ready.expected_tasks().iter().copied())
                    || snapshot.fragments.len() != snapshot.tasks.len()
                    || !snapshot
                        .fragments
                        .iter()
                        .map(|(task, _)| *task)
                        .eq(snapshot.tasks.iter().map(|(task, _)| *task))
                {
                    return Err(
                        "EXPLAIN ANALYZE diagnostics do not match the accepted computation"
                            .to_string(),
                    );
                }
                if !snapshot.complete() {
                    diagnostics.push(format!("Diagnostics: {:?}; complete=false", snapshot.closure));
                }
                for ((task, fact), (_, fragment)) in
                    snapshot.tasks.iter().zip(snapshot.fragments.iter())
                {
                    let fragment = plan
                        .fragments()
                        .get(&novarocks_physical_plan::FragmentId::new(*fragment))
                        .ok_or_else(|| "EXPLAIN ANALYZE diagnostic Task names an absent plan fragment".to_string())?;
                    let root = i32::try_from(fragment.root().get()).map_err(|_| "EXPLAIN ANALYZE fragment root exceeds native profile range".to_string())?;
                    match fact {
                        ProfileTaskDiagnostic::Available(info) => {
                            if info.final_status().identity() != *task {
                                return Err(
                                    "EXPLAIN ANALYZE final info has a foreign Task identity"
                                        .to_string(),
                                );
                            }
                            profiles
                                .apply_task_operator_statistics(info, root)
                                .map_err(|error| error.to_string())?;
                        }
                        ProfileTaskDiagnostic::Missing(reason) => {
                            profiles.apply_missing_task_information(*task, root);
                            diagnostics.push(format!(
                                "Diagnostics missing: stage={} task={}; reason={reason:?}",
                                task.stage_id(), task.task_id()
                            ));
                        }
                    }
                }
                if !snapshot.runtime_filters.is_complete() {
                    diagnostics.push("RuntimeFilter diagnostics unavailable: context release incomplete".to_string());
                }
                for (process, telemetry) in snapshot.runtime_filters.contributions() {
                    if matches!(telemetry.as_proto().telemetry, Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(_))) {
                        diagnostics.push(format!("RuntimeFilter diagnostics unavailable: backend={process}"));
                    }
                    profiles.apply_runtime_filter_contribution(ready.ready_token().root().query_execution_id(), *process, telemetry)
                        .map_err(|error| error.to_string())?;
                }
                match snapshot.split {
                    crate::task_execution::profile_diagnostics::ProfileSplitDiagnostic::Available(profile) => profiles.apply_split_assignment_profile(profile),
                    crate::task_execution::profile_diagnostics::ProfileSplitDiagnostic::Missing(reason) => diagnostics.push(format!("FrontendSplitAssignment diagnostics unavailable: {reason:?}")),
                }
            }
            Err(crate::query_execution::internal_consumer::profile::ProfileSnapshotMissing::ForeignComputationCut) => {
                return Err("EXPLAIN ANALYZE diagnostic observer has a foreign computation cut".to_string());
            }
            Err(reason) => diagnostics.push(format!("Diagnostics unavailable: {reason:?}")),
        }
        render_profile_result(
            formatter,
            ready.output_rows(),
            profiles.finish().into_profiles(),
            diagnostics,
        )
    }
}

fn render_completed_profile(
    plan: &novarocks_physical_plan::PhysicalPlan,
    annotations: &[novarocks_sql::compiler::SqlDisplayAnnotation],
    actuals: &std::collections::HashMap<i32, crate::query_execution::profile::ActualMetrics>,
    per_fragment: &std::collections::HashMap<
        i32,
        crate::query_execution::profile::DistributedProfileSummary,
    >,
    control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<Vec<String>, String> {
    use novarocks_sql::compiler::{
        SqlCompletedExplainProfile, SqlExplainFragmentMetrics, SqlExplainNodeKey,
        SqlExplainObservation, SqlExplainOperatorMetrics, SqlExplainUnavailableReason,
    };

    let operators = plan
        .fragments()
        .iter()
        .flat_map(|(fragment_id, fragment)| {
            fragment.nodes().keys().map(move |node_id| {
                let wire_id = i32::try_from(node_id.get()).map_err(|_| {
                    "completed EXPLAIN ANALYZE node ID exceeds native profile range".to_string()
                })?;
                let observation = actuals.get(&wire_id).map_or(
                    SqlExplainObservation::Unavailable(
                        SqlExplainUnavailableReason::RuntimeDidNotReport,
                    ),
                    |metrics| {
                        SqlExplainObservation::Available(
                            SqlExplainOperatorMetrics::new(
                                metrics.output_rows,
                                metrics.total_time_ns,
                                metrics.peak_mem_bytes,
                            )
                            .with_time_range(metrics.total_time_min_ns, metrics.total_time_max_ns)
                            .with_hash_join_times(
                                metrics.build_ht_ns,
                                metrics.search_ns,
                                metrics.out_build_ns,
                                metrics.out_probe_ns,
                            )
                            .with_dictionary_counts(
                                metrics.dict_input_rows,
                                metrics.dict_input_columns,
                                metrics.dict_kept_rows,
                                metrics.dict_kept_columns,
                                metrics.dict_hydrated_rows,
                                metrics.dict_hydrated_columns,
                                metrics.dict_unsupported_columns,
                            ),
                        )
                    },
                );
                Ok((SqlExplainNodeKey::new(*fragment_id, *node_id), observation))
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let fragments = plan
        .fragments()
        .iter()
        .map(|(fragment_id, fragment)| {
            let root_id = i32::try_from(fragment.root().get()).map_err(|_| {
                "completed EXPLAIN ANALYZE fragment root exceeds native profile range".to_string()
            })?;
            let observation = per_fragment.get(&root_id).map_or(
                SqlExplainObservation::Unavailable(
                    SqlExplainUnavailableReason::RuntimeDidNotReport,
                ),
                |summary| {
                    SqlExplainObservation::Available(
                        SqlExplainFragmentMetrics::new(
                            summary.operator_active_time_ns,
                            summary.driver_blocked_time_ns,
                        )
                        .with_wait_times(
                            summary.dependency_wait_time_ns,
                            summary.exchange_wait_time_ns,
                            summary.network_time_ns,
                            summary.scan_io_time_ns,
                        ),
                    )
                },
            );
            Ok((*fragment_id, observation))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let profile = SqlCompletedExplainProfile::try_new(plan.version(), operators, fragments)
        .map_err(|error| error.to_string())?;
    novarocks_sql::compiler::render_completed_plan(
        plan,
        annotations,
        novarocks_sql::compiler::ExplainLevel::Analyze,
        Some(&profile),
        novarocks_sql::compiler::ExplainRenderBudget::default(),
        control,
    )
    .map_err(|error| error.to_string())
}

const ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES: &[&str] = &[
    "IcebergRuntimeFilePruning/FilesTotal",
    "IcebergRuntimeFilePruning/FilesSelected",
    "IcebergRuntimeFilePruning/FilesPruned",
    "IcebergRuntimeFilePruning/Predicates",
    "IcebergRuntimeFilePruning/Unsupported",
    "IcebergRuntimeFilePruning/Unavailable",
];
const CONNECTOR_FILE_COUNTER_NAMES: &[&str] = &[
    "ConnectorFileCacheHits",
    "ConnectorFileCacheMisses",
    "ConnectorFileRowGroupsRead",
    "ConnectorFileRowGroupsPruned",
    "ConnectorUnitReadersOpened",
    "ConnectorFilePageIndexAttempts",
    "ConnectorFilePageIndexFallbacks",
    "ConnectorFilePageIndexRowsConsidered",
    "ConnectorFilePageIndexRowsPruned",
];
const TYPED_CONNECTOR_COUNTER_NAMES: &[&str] = &[
    "TypedConnectorPageSourcesOpened",
    "TypedConnectorSplitsRead",
];
const FRONTEND_SPLIT_ASSIGNMENT_COUNTER_NAMES: &[&str] = &[
    "ConnectorFilesConsidered",
    "ConnectorWholeFilesPruned",
    "ConnectorFilesExpanded",
    "ConnectorSplitsEmitted",
];

fn format_distributed_profile_summary(
    summary: &crate::query_execution::profile::DistributedProfileSummary,
) -> String {
    format!(
        "Profile: fragments={} fragment_wall_max={} fragment_wall_sum={} driver_total={} driver_blocked={} source_wait={} sink_wait={} dependency_wait={} operator_active={} exchange_wait={} exchange_process={} network={} scan_io={}",
        summary.fragment_instance_count,
        format_explain_analyze_duration_ns(summary.fragment_wall_max_ns),
        format_explain_analyze_duration_ns(summary.fragment_wall_sum_ns),
        format_explain_analyze_duration_ns(summary.driver_total_time_ns),
        format_explain_analyze_duration_ns(summary.driver_blocked_time_ns),
        format_explain_analyze_duration_ns(summary.source_wait_time_ns),
        format_explain_analyze_duration_ns(summary.sink_wait_time_ns),
        format_explain_analyze_duration_ns(summary.dependency_wait_time_ns),
        format_explain_analyze_duration_ns(summary.operator_active_time_ns),
        format_explain_analyze_duration_ns(summary.exchange_wait_time_ns),
        format_explain_analyze_duration_ns(summary.exchange_process_time_ns),
        format_explain_analyze_duration_ns(summary.network_time_ns),
        format_explain_analyze_duration_ns(summary.scan_io_time_ns)
    )
}

fn format_explain_analyze_duration_ns(ns: i64) -> String {
    format_explain_analyze_duration(std::time::Duration::from_nanos(ns.max(0) as u64))
}

fn format_explain_analyze_duration(duration: std::time::Duration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{ms:.3}ms")
    } else if ms < 1000.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn connector_file_summary_includes_cache_and_page_index_effect_counters() {
        assert_eq!(
            super::CONNECTOR_FILE_COUNTER_NAMES,
            [
                "ConnectorFileCacheHits",
                "ConnectorFileCacheMisses",
                "ConnectorFileRowGroupsRead",
                "ConnectorFileRowGroupsPruned",
                "ConnectorUnitReadersOpened",
                "ConnectorFilePageIndexAttempts",
                "ConnectorFilePageIndexFallbacks",
                "ConnectorFilePageIndexRowsConsidered",
                "ConnectorFilePageIndexRowsPruned",
            ]
        );
    }

    #[test]
    fn typed_connector_summary_includes_carrier_native_counters() {
        assert_eq!(
            super::TYPED_CONNECTOR_COUNTER_NAMES,
            [
                "TypedConnectorPageSourcesOpened",
                "TypedConnectorSplitsRead"
            ]
        );
    }

    #[test]
    fn frontend_split_assignment_summary_includes_dynamic_file_pruning_counters() {
        assert_eq!(
            super::FRONTEND_SPLIT_ASSIGNMENT_COUNTER_NAMES,
            [
                "ConnectorFilesConsidered",
                "ConnectorWholeFilesPruned",
                "ConnectorFilesExpanded",
                "ConnectorSplitsEmitted",
            ]
        );
    }
}
