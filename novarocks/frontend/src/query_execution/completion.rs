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
    AttemptCredentialLeaseCollector, QueryCredentialLeases,
};
use crate::runtime::query_result::build_string_query_result;
use crate::runtime::statement_result::StatementResult;
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorRequestScope};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use std::sync::Arc;

/// Move-only ownership of one logical query identity.
///
/// Preparation uses this identity only to correlate diagnostics. Attempt
/// identity, credentials and request scope are minted later by the
/// coordinator, after the immutable logical operation has crossed the
/// execution boundary.
pub struct LogicalQueryReservation {
    query_id: QueryId,
}

impl LogicalQueryReservation {
    pub(crate) const fn new(query_id: QueryId) -> Self {
        Self { query_id }
    }

    pub(crate) const fn into_query_id(self) -> QueryId {
        self.query_id
    }

    #[cfg(test)]
    pub(crate) const fn for_test(query_id: QueryId) -> Self {
        Self::new(query_id)
    }
}

/// FE-local identity for one distributed attempt.
///
/// Read-only query preparation cannot construct this value. The coordinator
/// mints it only after receiving the frozen logical operation. Effectful
/// statement wrappers may reserve it earlier when their staged business
/// protocol must own attempt-scoped capabilities before Core request
/// construction.
pub struct QueryAttemptReservation {
    query_id: QueryId,
    execution_id: QueryExecutionId,
    credential_collector: Arc<AttemptCredentialLeaseCollector>,
    request_scope: ConnectorRequestScope,
}

impl QueryAttemptReservation {
    pub(crate) fn first(
        query_id: QueryId,
    ) -> Result<Self, crate::query_execution::contract::DistributedQueryError> {
        Self::for_round(query_id, 1)
    }

    pub(crate) fn retry(
        query_id: QueryId,
        attempt: u32,
    ) -> Result<Self, crate::query_execution::contract::DistributedQueryError> {
        Self::for_round(query_id, attempt)
    }

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
        Ok(Self {
            query_id,
            execution_id,
            credential_collector: AttemptCredentialLeaseCollector::new(execution_id),
            request_scope: ConnectorRequestScope::new(),
        })
    }

    pub(crate) const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
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
    ) -> ConnectorRequestContext {
        context
            .with_request_scope(self.request_scope.clone())
            .with_storage_resolver(self.credential_storage_resolver())
            .with_vended_credential_lease_sink(self.credential_lease_sink())
    }

    pub(crate) fn has_collected_credential_leases(&self) -> bool {
        self.credential_collector.has_collected_leases()
    }

    pub(crate) fn into_credential_leases(
        self,
    ) -> Result<QueryCredentialLeases, crate::query_execution::contract::DistributedQueryError>
    {
        self.credential_collector.into_credential_leases()
    }
}

/// Frontend-owned factory for a replacement *whole* distributed round. It
/// receives a fresh topology snapshot and must return request artifacts and a
/// matching completion formatter that belong to that attempt alone.
///
/// The rule this replaces said no existing fragment, split, writer, RF,
/// schedule, manifest, or profile artifact may be reused. That was written
/// when the only way to get a second round was to plan one from scratch, and
/// it conflated two different things. What must not be reused is anything an
/// attempt *owns*: no fragment instance, split source, writer, RF artifact,
/// schedule, manifest or profile may cross from a spent attempt into a fresh
/// one, and no credential or lease may either.
///
/// The sealed *description* is the opposite case. It is semantic input to
/// every attempt of one logical execution, it names no backend and holds no
/// capability, and re-deriving it per attempt is how two rounds of one
/// statement came to disagree about plan shape whenever the statistics under
/// them moved. A replacement round therefore rebinds the same sealed plan and
/// re-derives only what the new attempt owns.
pub(crate) trait PreReadyRetryBoundary {
    fn permit_pre_ready_retry(
        &self,
    ) -> Result<(), crate::query_execution::contract::DistributedQueryError>;

    fn close_after_control_ready(&self) {}

    fn close_after_stage_or_start(&self) {}
}

pub(crate) trait PreparedDistributedAttemptFactory: Send + PreReadyRetryBoundary {
    fn instantiate(
        &mut self,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<PreparedDistributedAttempt, crate::query_execution::contract::DistributedQueryError>;
}

/// Request and completion artifacts for a replacement attempt of an already
/// owned logical query.
///
/// This deliberately carries no logical or attempt reservation. The
/// coordinator keeps the sole logical identity and mints a fresh attempt only
/// after these immutable replacement artifacts have been produced.
pub(crate) struct PreparedDistributedAttempt {
    request: crate::query_execution::contract::DistributedQueryRequest,
    completion: PreparedQueryCompletion,
}

impl PreparedDistributedAttempt {
    pub(crate) fn new(
        request: crate::query_execution::contract::DistributedQueryRequest,
        completion: PreparedQueryCompletion,
    ) -> Self {
        Self {
            request,
            completion,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::query_execution::contract::DistributedQueryRequest,
        PreparedQueryCompletion,
    ) {
        (self.request, self.completion)
    }
}

/// Frontend-owned factory for a replacement whole distributed round whose
/// caller retains the raw outcome (for example, a DML transaction runner
/// still needs its exact commit/abort handles). It has the same no-reuse and
/// one-way effect boundary as [`PreparedDistributedAttemptFactory`], but it
/// intentionally has no statement-result formatter.
pub(crate) trait PreparedDistributedRequestFactory: Send + PreReadyRetryBoundary {
    fn replan(
        &mut self,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryRequest,
        crate::query_execution::contract::DistributedQueryError,
    >;
}

/// One raw-outcome distributed operation plus the sole owner capable of
/// generating its replacement round. The request remains move-only; the
/// factory can only return a wholly new request from stable semantics.
pub struct PreparedRetriableDistributedRequest {
    request: crate::query_execution::contract::DistributedQueryRequest,
    round_factory: Box<dyn PreparedDistributedRequestFactory>,
    /// A DML operation can observe vended storage credentials while it is
    /// still building its first native request.  Keep the matching attempt
    /// reservation move-only with that request so lifecycle Init, rather than
    /// a later coordinator-generated identity, owns those credentials.
    reservation: Option<QueryAttemptReservation>,
}

impl PreparedRetriableDistributedRequest {
    pub(crate) fn new(
        request: crate::query_execution::contract::DistributedQueryRequest,
        round_factory: Box<dyn PreparedDistributedRequestFactory>,
    ) -> Self {
        Self {
            request,
            round_factory,
            reservation: None,
        }
    }

    pub(crate) fn with_attempt_reservation(mut self, reservation: QueryAttemptReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::query_execution::contract::DistributedQueryRequest,
        Box<dyn PreparedDistributedRequestFactory>,
        Option<QueryAttemptReservation>,
    ) {
        (self.request, self.round_factory, self.reservation)
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
    Distributed(PreparedDistributedQuery),
}

impl PreparedQueryOperation {
    /// Build a completed statement result without exposing the immediate
    /// operation constructor to application callers.
    pub fn immediate(result: StatementResult) -> Self {
        Self::Immediate(PreparedImmediateQuery::new(result))
    }

    /// Build the canonical single-column response used by EXPLAIN.
    pub fn explain_lines(lines: Vec<String>) -> Result<Self, String> {
        Ok(Self::immediate(StatementResult::Query(
            build_string_query_result("Explain String", lines)?,
        )))
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
    pub(super) fn new(
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

pub struct PreparedDistributedQuery {
    request: crate::query_execution::contract::DistributedQueryRequest,
    completion: PreparedQueryCompletion,
    attempt_factory: Option<Box<dyn PreparedDistributedAttemptFactory>>,
    logical_reservation: LogicalQueryReservation,
}

impl PreparedDistributedQuery {
    /// Pair the Core-validated request with its exact completion formatter.
    /// Only Frontend native assembly receives both values from the same
    /// sealed query preparation.
    pub fn new(
        request: crate::query_execution::contract::DistributedQueryRequest,
        completion: PreparedQueryCompletion,
        logical_reservation: LogicalQueryReservation,
    ) -> Self {
        Self {
            request,
            completion,
            attempt_factory: None,
            logical_reservation,
        }
    }

    pub(crate) fn with_attempt_factory(
        mut self,
        attempt_factory: Box<dyn PreparedDistributedAttemptFactory>,
    ) -> Self {
        self.attempt_factory = Some(attempt_factory);
        self
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::query_execution::contract::DistributedQueryRequest,
        PreparedQueryCompletion,
        Option<Box<dyn PreparedDistributedAttemptFactory>>,
        LogicalQueryReservation,
    ) {
        (
            self.request,
            self.completion,
            self.attempt_factory,
            self.logical_reservation,
        )
    }
}

/// Core-owned completion formatter paired with a distributed request.
pub struct PreparedQueryCompletion {
    formatter: PreparedQueryFormatter,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Completion formatting retains the exact profile payload paired with the distributed request."
)]
enum PreparedQueryFormatter {
    Result,
    Profile(PreparedProfileFormatter),
}

struct PreparedProfileFormatter {
    distributed_plan: std::sync::Arc<novarocks_sql::plan_read::DistributedPlan>,
    planning_elapsed: std::time::Duration,
    execution_started_at: std::time::Instant,
}

impl PreparedQueryCompletion {
    pub(crate) fn result() -> Self {
        Self {
            formatter: PreparedQueryFormatter::Result,
        }
    }

    pub(crate) fn profile(
        distributed_plan: std::sync::Arc<novarocks_sql::plan_read::DistributedPlan>,
        planning_elapsed: std::time::Duration,
        execution_started_at: std::time::Instant,
    ) -> Self {
        Self {
            formatter: PreparedQueryFormatter::Profile(PreparedProfileFormatter {
                distributed_plan,
                planning_elapsed,
                execution_started_at,
            }),
        }
    }

    pub fn complete(
        self,
        outcome: crate::query_execution::contract::DistributedQueryOutcome,
    ) -> Result<StatementResult, String> {
        match self.formatter {
            PreparedQueryFormatter::Result => outcome
                .into_result()
                .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
                .map(StatementResult::Query)
                .map_err(|error| error.to_string()),
            PreparedQueryFormatter::Profile(formatter) => complete_profile(formatter, outcome),
        }
    }
}

fn complete_profile(
    formatter: PreparedProfileFormatter,
    outcome: crate::query_execution::contract::DistributedQueryOutcome,
) -> Result<StatementResult, String> {
    let outcome = outcome
        .into_profile()
        .map(crate::query_execution::outcome::ProfileExecutionOutcome::into_parts)
        .map_err(|error| error.to_string())?;
    let (query_result, fragment_profiles) = outcome;
    let fragment_profiles = fragment_profiles.into_profiles();
    if fragment_profiles.is_empty() {
        return Err("EXPLAIN ANALYZE completed without fragment runtime profiles".into());
    }
    // Per-fragment attribution is read while each tree still carries the
    // fragment its producer named; every other summary below is a counter sum
    // over the trees themselves and needs no attribution.
    let per_fragment =
        crate::query_execution::profile::collect_per_fragment_profile_summaries(&fragment_profiles);
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
        query_result.row_count()
    ));
    lines.push(format_distributed_profile_summary(&profile_summary));
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
        (
            RUNTIME_FILTER_SCAN_UNIT_COUNTER_NAMES,
            "RuntimeFilterScanUnits",
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
    let operator_facts = actuals
        .into_iter()
        .map(|(node_id, metrics)| {
            novarocks_sql::compiler::SqlExplainAnalyzeOperatorFacts::try_new(
                node_id,
                metrics.output_rows,
                metrics.total_time_ns,
                metrics.peak_mem_bytes,
                metrics.total_time_max_ns,
                metrics.total_time_min_ns,
                metrics.build_ht_ns,
                metrics.search_ns,
                metrics.out_build_ns,
                metrics.out_probe_ns,
                metrics.dict_input_rows,
                metrics.dict_input_columns,
                metrics.dict_kept_rows,
                metrics.dict_kept_columns,
                metrics.dict_hydrated_rows,
                metrics.dict_hydrated_columns,
                metrics.dict_unsupported_columns,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let fragment_facts = per_fragment
        .into_iter()
        .map(|(root_node_id, summary)| {
            novarocks_sql::compiler::SqlExplainAnalyzeFragmentFacts::try_new(
                root_node_id,
                summary.operator_active_time_ns,
                summary.driver_blocked_time_ns,
                summary.dependency_wait_time_ns,
                summary.exchange_wait_time_ns,
                summary.network_time_ns,
                summary.scan_io_time_ns,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let profile =
        novarocks_sql::compiler::SqlExplainAnalyzeProfile::try_new(operator_facts, fragment_facts)
            .map_err(|error| error.to_string())?;
    lines.extend(
        novarocks_sql::compiler::render_distributed_explain_analyze(
            &formatter.distributed_plan,
            &profile,
        )
        .map_err(|error| error.to_string())?,
    );
    build_string_query_result("Explain String", lines).map(StatementResult::Query)
}

const ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES: &[&str] = &[
    "IcebergRuntimeFilePruning/FilesTotal",
    "IcebergRuntimeFilePruning/FilesSelected",
    "IcebergRuntimeFilePruning/FilesPruned",
    "IcebergRuntimeFilePruning/Predicates",
    "IcebergRuntimeFilePruning/Unsupported",
    "IcebergRuntimeFilePruning/Unavailable",
];
const RUNTIME_FILTER_SCAN_UNIT_COUNTER_NAMES: &[&str] = &[
    "RuntimeFilterScanUnitsPruned",
    "RuntimeFilterScanUnitsKept",
    "RuntimeFilterScanUnitsNotEvaluated",
    "RuntimeFilterScanUnitsNotEvaluatedUnitFactsMissing",
    "RuntimeFilterScanUnitsNotEvaluatedColumnFactsMissing",
    "RuntimeFilterScanUnitsNotEvaluatedDataTypeUnsupported",
    "RuntimeFilterScanUnitsNotEvaluatedPredicateCapabilityUnsupported",
    "RuntimeFilterScanUnitsNotEvaluatedResourceUnavailable",
    "RuntimeFilterScanUnitsNotEvaluatedSnapshotUnavailable",
    "RuntimeFilterScanUnitsNotEvaluatedSnapshotTimedOut",
    "RuntimeFilterScanUnitsNotEvaluatedSnapshotNotPublished",
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
    fn logical_query_identity_is_separate_from_attempt_identity() {
        let query_id = novarocks_types::QueryId::new(7, 41);
        let logical = super::LogicalQueryReservation::for_test(query_id);

        assert_eq!(logical.into_query_id(), query_id);

        let first = super::QueryAttemptReservation::first(query_id).expect("first attempt");
        let replacement =
            super::QueryAttemptReservation::retry(query_id, 2).expect("replacement attempt");
        assert_eq!(first.query_id(), query_id);
        assert_eq!(replacement.query_id(), query_id);
        assert_eq!(first.execution_id().attempt_id().get(), 1);
        assert_eq!(replacement.execution_id().attempt_id().get(), 2);
    }

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
