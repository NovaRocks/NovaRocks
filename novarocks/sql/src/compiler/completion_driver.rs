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

//! Owned driver for the final physical-plan completion protocol.
//!
//! Every pending state in this module contains SQL values only. Catalog and
//! provider capabilities remain with the application that answers a typed
//! need batch. The optimizer is run only after the corresponding immutable
//! catalog, MV and statistics facts have been installed.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId,
    ProviderReadOccurrenceId, ScanReadBudget, ValueType,
};
use novarocks_spi::connector::StatisticsMetric;

use super::completion::{
    CatalogRelationFact, CompileNeedId, CompilerContinuation, CompilerStep, CompletionLimits,
    MaterializedViewFact, MaterializedViewNeed, MaterializedViewOutcome, ProviderReadColumnNeed,
    ProviderReadFact, ProviderReadNeed, ProviderReadStaticContract, SqlCompileRequest,
    SqlDisplayIntent, SqlNeedBatch, StatisticsFact, StatisticsNeed,
    provider_connector_type_for_engine, provider_relation_need_from_sql_scan,
};
use super::completion_catalog::CatalogCompletionState;
use super::completion_predicate::{ProviderPredicateColumn, lower_provider_predicates};
use super::mv_rewrite::{MvRewriteDefinitionIndex, SqlMvRewriteDefinitionFacts};
use super::{
    SqlAnalyzeOutput, SqlAnalyzeRequest, SqlAnalyzedQuery, SqlCompileControl, SqlCompileError,
    SqlCompileIntent, SqlCompiler, SqlConstantEvaluator, SqlFunctionCatalog,
    SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use crate::binding::SqlTableBindingId;
use crate::catalog::TableLookupMode;
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::physical::PhysicalPlanNode;
use crate::planner::table::{ScanSource, TableDef};
use crate::planning::dml::DmlStatisticsSnapshot;

/// Fully owned SQL input for producing one immutable final physical plan.
///
/// The function catalog is snapshotted at construction. The constant evaluator
/// is the existing pure optimizer port; it cannot resolve catalog or provider
/// state. Neither a catalog nor a provider callback is accepted here.
pub struct SqlFinalPlanCompileRequest {
    version: PlanVersionId,
    statement: SqlStatementInput,
    intent: SqlCompileIntent,
    session: SqlSessionContext,
    environment: SqlPlanningEnvironment,
    functions: Arc<dyn SqlFunctionCatalog>,
    constant_evaluator: &'static dyn SqlConstantEvaluator,
    control: SqlCompileControl,
    dop_domain: PipelineDopDomain,
    scan_read_budget: ScanReadBudget,
    limits: CompletionLimits,
}

impl fmt::Debug for SqlFinalPlanCompileRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlFinalPlanCompileRequest")
            .field("version", &self.version)
            .field("statement", &self.statement)
            .field("intent", &self.intent)
            .field("session", &self.session)
            .field("environment", &self.environment)
            .field("dop_domain", &self.dop_domain)
            .field("scan_read_budget", &self.scan_read_budget)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl SqlFinalPlanCompileRequest {
    #[expect(
        clippy::too_many_arguments,
        reason = "Each argument is an independently frozen final-plan input."
    )]
    pub fn new(
        version: PlanVersionId,
        statement: SqlStatementInput,
        intent: SqlCompileIntent,
        session: SqlSessionContext,
        environment: SqlPlanningEnvironment,
        functions: Arc<dyn SqlFunctionCatalog>,
        constant_evaluator: &'static dyn SqlConstantEvaluator,
        control: SqlCompileControl,
        dop_domain: PipelineDopDomain,
        scan_read_budget: ScanReadBudget,
        limits: CompletionLimits,
    ) -> Self {
        Self {
            version,
            statement,
            intent,
            session,
            environment,
            functions: functions.snapshot(),
            constant_evaluator,
            control,
            dop_domain,
            scan_read_budget,
            limits,
        }
    }

    /// Returns the immutable request control that must govern every fact round.
    /// The application may clone it before consuming this request, but cannot
    /// replace it with a broader deadline or a different cancellation view.
    pub const fn control(&self) -> &SqlCompileControl {
        &self.control
    }

    /// Parse the owned statement and publish the first exact observation need,
    /// or complete immediately when the query has no external relation.
    pub fn try_into_completion(self) -> Result<SqlCompileRequest, SqlCompileError> {
        let Self {
            version,
            statement,
            intent,
            session,
            environment,
            functions,
            constant_evaluator,
            control,
            dop_domain,
            scan_read_budget,
            limits,
        } = self;
        control.check()?;
        validate_final_intent(&intent)?;
        validate_dop_domain(dop_domain)?;
        validate_scan_read_budget(scan_read_budget)?;
        let display_intent = display_intent(&intent);
        let query = crate::sql_mode::normalize_concat_query(
            super::parse_query(&statement)?,
            &session.sql_semantics,
        );
        let common = FinalPlanCommon {
            version,
            intent,
            session,
            environment,
            functions,
            constant_evaluator,
            dop_domain,
            scan_read_budget,
            display_intent,
        };

        // A persisted semantic snapshot is required before an optional MV
        // definition can be replayed. Decide before publishing discovery or
        // catalog needs so an unrelated candidate cannot fail the base query.
        let consumer_requires_semantic_snapshot =
            crate::sql_mode::query_uses_group_concat_legacy(&common.session.sql_semantics, &query)
                .map_err(SqlCompileError::Analyze)?;
        let mv_enabled = common.session.optimizer_settings.mv_rewrite_enabled()
            && !consumer_requires_semantic_snapshot;
        let initial_catalog = CatalogCompletionState::try_new(
            query.clone(),
            common.session.current_catalog.as_deref(),
            &common.session.current_database,
            TableLookupMode::SchemaOnly,
            if mv_enabled { 1 } else { 0 },
        )?;
        let mut seen_relations = HashSet::new();
        let referenced_relations = initial_catalog
            .needs()
            .iter()
            .map(|need| need.relation().clone())
            .filter(|relation| seen_relations.insert(relation.clone()))
            .collect::<Vec<_>>();

        let step = if mv_enabled && !referenced_relations.is_empty() {
            let need = MaterializedViewNeed::try_new(CompileNeedId::new(0), referenced_relations)
                .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
            CompilerStep::need(
                SqlNeedBatch::MaterializedViews(vec![need].into_boxed_slice()),
                CompilerContinuation::materialized_view(SqlMaterializedViewCompletionState {
                    common,
                    query: Box::new(query),
                }),
            )
        } else {
            catalog_or_analyze_step(common, initial_catalog, None, &control)?
        };
        Ok(SqlCompileRequest::pending(step, limits))
    }
}

fn validate_final_intent(intent: &SqlCompileIntent) -> Result<(), SqlCompileError> {
    match intent {
        SqlCompileIntent::Query | SqlCompileIntent::Explain { .. } => Ok(()),
        SqlCompileIntent::AnalyzeOnly | SqlCompileIntent::LogicalOnly => {
            Err(SqlCompileError::InvalidRequest(
                "final physical-plan completion does not accept a pre-physical terminal intent"
                    .to_string(),
            ))
        }
        SqlCompileIntent::IcebergWrite { .. } | SqlCompileIntent::ChangeStreamWrite => {
            Err(SqlCompileError::InvalidRequest(
                "final physical-plan completion for write intents is not installed".to_string(),
            ))
        }
        SqlCompileIntent::DmlInternalRead => Err(SqlCompileError::InvalidRequest(
            "internal DML reads complete through their statement owner".to_string(),
        )),
    }
}

fn validate_dop_domain(domain: PipelineDopDomain) -> Result<(), SqlCompileError> {
    if domain.min == 0 || domain.max < domain.min {
        return Err(SqlCompileError::InvalidRequest(
            "final physical-plan DOP domain is invalid".to_string(),
        ));
    }
    if domain.requires_power_of_two
        && (!domain.min.is_power_of_two() || !domain.max.is_power_of_two())
    {
        return Err(SqlCompileError::InvalidRequest(
            "power-of-two final physical-plan DOP bounds must both be powers of two".to_string(),
        ));
    }
    Ok(())
}

fn validate_scan_read_budget(budget: ScanReadBudget) -> Result<(), SqlCompileError> {
    if budget.max_batch_rows == 0 || budget.max_batch_bytes == 0 {
        return Err(SqlCompileError::InvalidRequest(
            "final physical-plan scan read budget must be non-zero".to_string(),
        ));
    }
    if budget.max_batch_rows > MAX_SCAN_BATCH_ROWS || budget.max_batch_bytes > MAX_SCAN_BATCH_BYTES
    {
        return Err(SqlCompileError::InvalidRequest(
            "final physical-plan scan read budget exceeds the contract limit".to_string(),
        ));
    }
    Ok(())
}

fn display_intent(intent: &SqlCompileIntent) -> SqlDisplayIntent {
    match intent {
        SqlCompileIntent::Explain { level, analyze } => SqlDisplayIntent::Explain {
            level: *level,
            analyze: *analyze,
        },
        _ => SqlDisplayIntent::Execute,
    }
}

struct FinalPlanCommon {
    version: PlanVersionId,
    intent: SqlCompileIntent,
    session: SqlSessionContext,
    environment: SqlPlanningEnvironment,
    functions: Arc<dyn SqlFunctionCatalog>,
    constant_evaluator: &'static dyn SqlConstantEvaluator,
    dop_domain: PipelineDopDomain,
    scan_read_budget: ScanReadBudget,
    display_intent: SqlDisplayIntent,
}

pub(crate) struct SqlMaterializedViewCompletionState {
    common: FinalPlanCommon,
    query: Box<novarocks_parser::ast::Query>,
}

pub(crate) struct SqlCatalogCompletionState {
    common: FinalPlanCommon,
    catalog: CatalogCompletionState,
    mv_definitions: Option<MvRewriteDefinitionIndex>,
}

pub(crate) struct SqlStatisticsCompletionState {
    common: FinalPlanCommon,
    analyzed: SqlAnalyzedQuery,
    needs: Box<[StatisticsNeed]>,
    next_need_ordinal: u32,
}

pub(crate) struct SqlProviderReadCompletionState {
    common: FinalPlanCommon,
    physical: PhysicalPlanNode,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    needs: Box<[ProviderReadNeed]>,
}

/// Exact provider results paired with the scan occurrences that requested
/// them. The lowering visitor consumes every entry once. A binding alone is
/// insufficient because a self join has two independently negotiated reads.
pub(crate) struct FinalizedProviderReadSet {
    entries: BTreeMap<ProviderReadOccurrenceId, FinalizedProviderRead>,
}

pub(crate) struct FinalizedProviderRead {
    pub(crate) binding: SqlTableBindingId,
    pub(crate) contract: ProviderReadStaticContract,
    pub(crate) read_budget: ScanReadBudget,
}

impl FinalizedProviderReadSet {
    pub(crate) fn try_from_facts(
        facts: impl IntoIterator<Item = (ProviderReadFact, ScanReadBudget)>,
    ) -> Result<Self, SqlCompileError> {
        let mut entries = BTreeMap::new();
        for (fact, read_budget) in facts {
            validate_scan_read_budget(read_budget)?;
            let occurrence = fact.occurrence();
            let binding = fact.binding();
            if entries
                .insert(
                    occurrence,
                    FinalizedProviderRead {
                        binding,
                        contract: fact.into_contract(),
                        read_budget,
                    },
                )
                .is_some()
            {
                return Err(SqlCompileError::Compilation(format!(
                    "finalized provider reads repeat scan occurrence {}",
                    occurrence.get()
                )));
            }
        }
        Ok(Self { entries })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn single_occurrence(&self) -> Result<ProviderReadOccurrenceId, SqlCompileError> {
        let mut occurrences = self.entries.keys().copied();
        let occurrence = occurrences.next().ok_or_else(|| {
            SqlCompileError::Compilation(
                "single-scan final plan requires one finalized provider read".to_string(),
            )
        })?;
        if occurrences.next().is_some() {
            return Err(SqlCompileError::Compilation(
                "single-scan final plan received more than one finalized provider read".to_string(),
            ));
        }
        Ok(occurrence)
    }

    #[cfg(test)]
    pub(crate) fn single_for_test(
        binding: SqlTableBindingId,
        occurrence: ProviderReadOccurrenceId,
        contract: ProviderReadStaticContract,
        read_budget: ScanReadBudget,
    ) -> Self {
        Self {
            entries: BTreeMap::from([(
                occurrence,
                FinalizedProviderRead {
                    binding,
                    contract,
                    read_budget,
                },
            )]),
        }
    }

    pub(crate) fn take(
        &mut self,
        binding: SqlTableBindingId,
        occurrence: ProviderReadOccurrenceId,
    ) -> Result<FinalizedProviderRead, SqlCompileError> {
        let read = self.entries.remove(&occurrence).ok_or_else(|| {
                SqlCompileError::Compilation(format!(
                    "physical scan occurrence {} for binding {binding:?} has no finalized provider read",
                    occurrence.get()
                ))
            })?;
        if read.binding != binding {
            return Err(SqlCompileError::Compilation(format!(
                "physical scan occurrence {} expects binding {binding:?} but its finalized provider read carries {:?}",
                occurrence.get(),
                read.binding
            )));
        }
        Ok(read)
    }

    pub(crate) fn ensure_consumed(self) -> Result<(), SqlCompileError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        Err(SqlCompileError::Compilation(format!(
            "{} finalized provider reads were not consumed by physical lowering",
            self.entries.len()
        )))
    }
}

pub(super) fn resume_materialized_view(
    state: SqlMaterializedViewCompletionState,
    facts: Box<[MaterializedViewFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    control.check()?;
    let mut definitions = Vec::<SqlMvRewriteDefinitionFacts>::new();
    for fact in facts {
        match fact.outcome() {
            MaterializedViewOutcome::Observed(observed) => {
                definitions.extend(observed.iter().cloned());
            }
            MaterializedViewOutcome::Missing { .. } => {}
        }
    }
    let additional_relations = definitions
        .iter()
        // Keep rejected definitions for the late eligibility diagnostic, but
        // never publish their additional source/target catalog obligations.
        .filter(|definition| definition.completion_query_semantics_supported())
        .flat_map(SqlMvRewriteDefinitionFacts::completion_catalog_relations)
        .collect::<Vec<_>>();
    let mv_definitions = MvRewriteDefinitionIndex::try_new(definitions)
        .map_err(|error| SqlCompileError::Compilation(format!("MV completion facts: {error}")))?;
    let catalog = CatalogCompletionState::try_new_with_additional_queries(
        *state.query,
        &[],
        &additional_relations,
        state.common.session.current_catalog.as_deref(),
        &state.common.session.current_database,
        TableLookupMode::SchemaOnly,
        1,
    )?;
    catalog_or_analyze_step(state.common, catalog, Some(mv_definitions), control)
}

pub(super) fn resume_catalog(
    state: SqlCatalogCompletionState,
    facts: Box<[CatalogRelationFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    control.check()?;
    let fact_catalog = state.catalog.fact_catalog(&facts)?;
    let analyzed = analyze_with_catalog(
        &state.common,
        state.catalog.query().clone(),
        &fact_catalog,
        state.mv_definitions.as_ref(),
        control,
    )?;
    statistics_or_optimize_step(
        state.common,
        analyzed,
        state.catalog.next_need_ordinal(),
        control,
    )
}

fn catalog_or_analyze_step(
    common: FinalPlanCommon,
    catalog: CatalogCompletionState,
    mv_definitions: Option<MvRewriteDefinitionIndex>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    if !catalog.needs().is_empty() {
        let needs = catalog.needs().to_vec().into_boxed_slice();
        let state = SqlCatalogCompletionState {
            common,
            catalog,
            mv_definitions,
        };
        return Ok(CompilerStep::need(
            SqlNeedBatch::CatalogRelations(needs),
            CompilerContinuation::catalog(state),
        ));
    }

    let fact_catalog = catalog.fact_catalog(&[])?;
    let analyzed = analyze_with_catalog(
        &common,
        catalog.query().clone(),
        &fact_catalog,
        mv_definitions.as_ref(),
        control,
    )?;
    statistics_or_optimize_step(common, analyzed, catalog.next_need_ordinal(), control)
}

fn analyze_with_catalog(
    common: &FinalPlanCommon,
    query: novarocks_parser::ast::Query,
    catalog: &dyn crate::catalog::PlannerTableProvider,
    mv_definitions: Option<&MvRewriteDefinitionIndex>,
    control: &SqlCompileControl,
) -> Result<SqlAnalyzedQuery, SqlCompileError> {
    let catalog = SqlPlannerTableSnapshot::new(catalog);
    let request = SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        common.intent.clone(),
        common.session.clone(),
        common.environment,
        &catalog,
        common.functions.as_ref(),
        common.constant_evaluator,
        mv_definitions,
        control.clone(),
    );
    match SqlCompiler::analyze(request)? {
        SqlAnalyzeOutput::Pending(analyzed) => Ok(analyzed),
        SqlAnalyzeOutput::Complete(_) => Err(SqlCompileError::InvalidRequest(
            "final physical-plan analysis terminated before optimization".to_string(),
        )),
    }
}

fn statistics_or_optimize_step(
    common: FinalPlanCommon,
    analyzed: SqlAnalyzedQuery,
    first_need_ordinal: u32,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    let (needs, next_need_ordinal) = collect_statistics_needs(&analyzed, first_need_ordinal)?;
    if needs.is_empty() {
        return optimize_and_prepare_provider(
            common,
            analyzed,
            DmlStatisticsSnapshot::empty(),
            next_need_ordinal,
            control,
        );
    }
    Ok(CompilerStep::need(
        SqlNeedBatch::Statistics(needs.clone()),
        CompilerContinuation::statistics(SqlStatisticsCompletionState {
            common,
            analyzed,
            needs,
            next_need_ordinal,
        }),
    ))
}

pub(super) fn resume_statistics(
    state: SqlStatisticsCompletionState,
    facts: Box<[StatisticsFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    control.check()?;
    let expected = state
        .needs
        .iter()
        .map(|need| (need.id(), need.binding()))
        .collect::<BTreeMap<_, _>>();
    let mut evidence = Vec::with_capacity(facts.len());
    for fact in facts {
        if expected.get(&fact.id()).copied() != Some(fact.binding()) {
            return Err(SqlCompileError::Compilation(format!(
                "statistics completion fact {} does not match the requested binding",
                fact.id().get()
            )));
        }
        evidence.push(fact.into_evidence());
    }
    optimize_and_prepare_provider(
        state.common,
        state.analyzed,
        DmlStatisticsSnapshot::from_evidence(evidence),
        state.next_need_ordinal,
        control,
    )
}

fn collect_statistics_needs(
    analyzed: &SqlAnalyzedQuery,
    mut next_need_ordinal: u32,
) -> Result<(Box<[StatisticsNeed]>, u32), SqlCompileError> {
    let mut tables = BTreeMap::<SqlTableBindingId, TableDef>::new();
    collect_logical_scan_tables(&analyzed.logical_plan, &mut tables)?;
    for (_, table) in super::mv_rewrite::completion_statistics_tables(&analyzed.mv_rewrite) {
        insert_statistics_table(&mut tables, table)?;
    }
    let mut needs = Vec::with_capacity(tables.len());
    for (binding, table) in tables {
        let id = CompileNeedId::new(next_need_ordinal);
        next_need_ordinal = next_need_ordinal.checked_add(1).ok_or_else(|| {
            SqlCompileError::Compilation("statistics completion need identity overflow".to_string())
        })?;
        needs.push(
            StatisticsNeed::try_new(id, binding, statistics_metrics(&table.columns))
                .map_err(|error| SqlCompileError::Compilation(error.to_string()))?,
        );
    }
    Ok((needs.into_boxed_slice(), next_need_ordinal))
}

fn collect_logical_scan_tables(
    plan: &LogicalPlanNode,
    output: &mut BTreeMap<SqlTableBindingId, TableDef>,
) -> Result<(), SqlCompileError> {
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        if let LogicalPlanKind::Scan(scan) = &node.kind {
            insert_statistics_table(output, scan.table.clone())?;
        }
        pending.extend(node.children.iter().rev());
    }
    Ok(())
}

fn insert_statistics_table(
    output: &mut BTreeMap<SqlTableBindingId, TableDef>,
    table: TableDef,
) -> Result<(), SqlCompileError> {
    let ScanSource::Sql(source) = &table.source;
    match output.get(&source.binding) {
        Some(existing) if !same_statistics_table(existing, &table) => {
            Err(SqlCompileError::Compilation(format!(
                "SQL table binding {:?} identifies conflicting statistics inputs",
                source.binding
            )))
        }
        Some(_) => Ok(()),
        None => {
            output.insert(source.binding, table);
            Ok(())
        }
    }
}

fn same_statistics_table(left: &TableDef, right: &TableDef) -> bool {
    let (ScanSource::Sql(left_source), ScanSource::Sql(right_source)) =
        (&left.source, &right.source);
    left_source.binding == right_source.binding
        && left_source.table == right_source.table
        && left.columns == right.columns
}

fn statistics_metrics(columns: &[novarocks_types::schema::ColumnDef]) -> Box<[StatisticsMetric]> {
    let mut metrics = Vec::with_capacity(1 + columns.len().saturating_mul(5));
    metrics.push(StatisticsMetric::RowCount);
    for column in columns {
        let name = Arc::<str>::from(column.name.as_str());
        metrics.push(StatisticsMetric::NullCount {
            column: Arc::clone(&name),
        });
        if statistics_scalar_bounds_supported(&column.data_type) {
            metrics.push(StatisticsMetric::Minimum {
                column: Arc::clone(&name),
            });
            metrics.push(StatisticsMetric::Maximum {
                column: Arc::clone(&name),
            });
        }
        metrics.push(StatisticsMetric::AverageSize {
            column: Arc::clone(&name),
        });
        metrics.push(StatisticsMetric::ThetaNdv { column: name });
    }
    metrics.into_boxed_slice()
}

fn statistics_scalar_bounds_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

fn optimize_and_prepare_provider(
    common: FinalPlanCommon,
    analyzed: SqlAnalyzedQuery,
    statistics_snapshot: DmlStatisticsSnapshot,
    next_need_ordinal: u32,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    let optimized = optimize_to_physical(analyzed, &statistics_snapshot, control)?;
    provider_or_ready_step(common, optimized, next_need_ordinal)
}

/// The optimizer result and the immutable base-table facts it consumed travel
/// together until final-plan completion. Provider negotiation cannot replace them.
struct OptimizedPhysicalPlan {
    physical: PhysicalPlanNode,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
}

fn optimize_to_physical(
    analyzed: SqlAnalyzedQuery,
    statistics_snapshot: &DmlStatisticsSnapshot,
    control: &SqlCompileControl,
) -> Result<OptimizedPhysicalPlan, SqlCompileError> {
    let SqlAnalyzedQuery {
        logical_plan,
        factory,
        intent,
        settings,
        change_stream: _,
        mv_rewrite,
        function_catalog,
        constant_evaluator,
    } = analyzed;
    control.check()?;
    let mut scalar_arena = crate::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )
    .map_err(SqlCompileError::Compilation)?;
    let mut statistics = super::collect_statistics(statistics_snapshot, &mut optimizer_expr)?;
    control.check()?;
    let (mv_rewrite, factory) = super::mv_rewrite::attach_candidate_statistics(
        mv_rewrite,
        statistics_snapshot,
        &mut statistics,
        factory,
    )?;
    let super::mv_rewrite::SqlMvRewritePreparation {
        candidates,
        diagnostics: _,
    } = mv_rewrite;
    let root_distribution = match &intent {
        SqlCompileIntent::IcebergWrite { root_distribution } => {
            super::resolve_root_distribution_requirement(&logical_plan, root_distribution)?
        }
        _ => None,
    };
    let environment = crate::optimizer::OptimizerEnvironment::new(
        &settings,
        constant_evaluator,
        Arc::clone(&function_catalog),
    );
    let optimized = match root_distribution {
        Some(distribution) => crate::optimizer::optimize_with_root_distribution(
            optimizer_expr,
            scalar_arena,
            &statistics.snapshot,
            factory,
            distribution,
            environment,
        ),
        None => crate::optimizer::optimize(
            optimizer_expr,
            scalar_arena,
            &statistics.snapshot,
            factory,
            candidates,
            environment,
        ),
    }
    .map_err(SqlCompileError::Compilation)?;
    control.check()?;
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized)
        .map_err(SqlCompileError::Compilation)?;
    Ok(OptimizedPhysicalPlan {
        physical,
        query_statistics: statistics.snapshot,
    })
}

fn provider_or_ready_step(
    common: FinalPlanCommon,
    optimized: OptimizedPhysicalPlan,
    next_need_ordinal: u32,
) -> Result<CompilerStep, SqlCompileError> {
    let OptimizedPhysicalPlan {
        mut physical,
        query_statistics,
    } = optimized;
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        &common.session.optimizer_settings,
    );
    let offer_predicates = common
        .session
        .optimizer_settings
        .connector_static_predicate_pushdown_enabled();
    let (physical, needs) = collect_provider_needs(physical, next_need_ordinal, offer_predicates)?;
    if !needs.is_empty() {
        return Ok(CompilerStep::need(
            SqlNeedBatch::ProviderReads(needs.clone()),
            CompilerContinuation::provider_read(SqlProviderReadCompletionState {
                common,
                physical,
                query_statistics,
                needs,
            }),
        ));
    }
    let mut builder = crate::planner::distributed::build::lower_final_physical_plan(
        &physical,
        common.version,
        common.dop_domain,
    )
    .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
    query_statistics.annotate_final_plan(&mut builder);
    Ok(CompilerStep::ready(
        common.version,
        builder,
        common.display_intent,
        [],
    ))
}

/// State every provider read one physical plan performs, and address each of
/// its scans by the occurrence that read will be accounted for under.
///
/// A statement reaches this through the completion protocol. A write reaches
/// it directly, because a write is compiled by the owner that sealed its
/// target rather than driven need-by-need -- but it states the same needs, so
/// it states them the same way.
pub(crate) fn collect_provider_needs(
    plan: PhysicalPlanNode,
    mut next_need_ordinal: u32,
    offer_predicates: bool,
) -> Result<(PhysicalPlanNode, Box<[ProviderReadNeed]>), SqlCompileError> {
    #[derive(Default)]
    struct ProviderReadOccurrenceAllocator {
        next: u32,
    }

    impl ProviderReadOccurrenceAllocator {
        fn mint(&mut self) -> Result<ProviderReadOccurrenceId, SqlCompileError> {
            let occurrence = ProviderReadOccurrenceId::new(self.next);
            self.next = self.next.checked_add(1).ok_or_else(|| {
                SqlCompileError::Compilation("physical scan occurrence overflow".to_string())
            })?;
            Ok(occurrence)
        }
    }

    fn walk(
        plan: PhysicalPlanNode,
        next_need_ordinal: &mut u32,
        occurrence_allocator: &mut ProviderReadOccurrenceAllocator,
        needs: &mut Vec<ProviderReadNeed>,
        offer_predicates: bool,
    ) -> Result<PhysicalPlanNode, SqlCompileError> {
        let PhysicalPlanNode {
            kind,
            children,
            output_columns,
            stats,
            probe_runtime_filters,
        } = plan;
        let children = children
            .into_iter()
            .map(|child| {
                walk(
                    child,
                    next_need_ordinal,
                    occurrence_allocator,
                    needs,
                    offer_predicates,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let kind = match kind {
            crate::planner::physical::PhysicalPlanKind::Scan(scan) => {
                let ScanSource::Sql(source) = &scan.table.source;
                let id = CompileNeedId::new(*next_need_ordinal);
                *next_need_ordinal = next_need_ordinal.checked_add(1).ok_or_else(|| {
                    SqlCompileError::Compilation(
                        "provider completion need identity overflow".to_string(),
                    )
                })?;
                let occurrence = occurrence_allocator.mint()?;
                let relation = provider_relation_need_from_sql_scan(
                    id,
                    novarocks_types::naming::TableIdentity::new(
                        &source.table.catalog,
                        &source.table.namespace,
                        &source.table.table,
                    ),
                    &source.kind,
                )
                .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
                let (columns, predicate_columns) = provider_columns(&output_columns, &scan)?;
                let predicates = if offer_predicates {
                    lower_provider_predicates(&scan, &predicate_columns)
                } else {
                    Box::default()
                };
                let need = ProviderReadNeed::try_new(
                    id,
                    occurrence,
                    source.binding,
                    relation,
                    columns,
                    predicates,
                    None,
                )
                .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
                needs.push(need);
                crate::planner::physical::PhysicalPlanKind::Scan(
                    scan.finalize_provider_read_occurrence(occurrence)
                        .map_err(SqlCompileError::Compilation)?,
                )
            }
            kind => kind,
        };
        Ok(PhysicalPlanNode {
            kind,
            children,
            output_columns,
            stats,
            probe_runtime_filters,
        })
    }

    let mut needs = Vec::new();
    let mut occurrence_allocator = ProviderReadOccurrenceAllocator::default();
    let plan = walk(
        plan,
        &mut next_need_ordinal,
        &mut occurrence_allocator,
        &mut needs,
        offer_predicates,
    )?;
    Ok((plan, needs.into_boxed_slice()))
}

type ProviderColumnProjection = (
    Box<[ProviderReadColumnNeed]>,
    BTreeMap<crate::column_id::ColumnId, ProviderPredicateColumn>,
);

fn provider_columns(
    output_columns: &[crate::analysis::OutputColumn],
    scan: &crate::planner::payload::PlanScanNode,
) -> Result<ProviderColumnProjection, SqlCompileError> {
    let synthetic = scan
        .variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<BTreeSet<_>>();
    let source_columns = scan
        .table
        .columns
        .iter()
        .chain(&scan.table.iceberg_row_lineage_metadata_columns)
        .collect::<Vec<_>>();
    let source_scan_columns = scan
        .columns
        .iter()
        .filter(|column| !synthetic.contains(&column.column_id))
        .collect::<Vec<_>>();
    // A scan names the provider fields it reads, which need not be all of
    // them: a statement rewritten onto a materialized view reads the columns
    // that view was matched for. So each one is found by the name it carries
    // rather than by standing at the field's position.
    let mut source_by_name = BTreeMap::new();
    for column in &source_columns {
        if source_by_name
            .insert(column.name.as_str(), *column)
            .is_some()
        {
            return Err(SqlCompileError::Compilation(format!(
                "provider schema repeats the column name '{}'",
                column.name
            )));
        }
    }
    let mut columns = Vec::new();
    let mut predicate_columns = BTreeMap::new();
    for output in output_columns {
        if synthetic.contains(&output.column_id) {
            continue;
        }
        let mut matches = source_scan_columns
            .iter()
            .filter(|column| column.column_id == output.column_id);
        let logical = matches.next().ok_or_else(|| {
            SqlCompileError::Compilation(format!(
                "scan output column id {} has no exact source-column binding",
                output.column_id
            ))
        })?;
        if matches.next().is_some() {
            return Err(SqlCompileError::Compilation(format!(
                "scan output column id {} repeats its source-column binding",
                output.column_id
            )));
        }
        let source = source_by_name.get(logical.name.as_str()).ok_or_else(|| {
            SqlCompileError::Compilation(format!(
                "scan source column '{}' is not a field of the provider schema",
                logical.name
            ))
        })?;
        if logical.name != source.name
            || logical.data_type != source.data_type
            || logical.nullable != source.nullable
            || output.name != source.name
            || output.data_type != source.data_type
            || output.nullable != source.nullable
        {
            return Err(SqlCompileError::Compilation(format!(
                "scan output '{}' differs from its exact provider schema column",
                output.name
            )));
        }
        let connector_type =
            provider_connector_type_for_engine(&source.data_type).ok_or_else(|| {
                SqlCompileError::Compilation(format!(
                    "scan output '{}' has no exact provider value type",
                    output.name
                ))
            })?;
        let ordinal = u32::try_from(columns.len()).map_err(|_| {
            SqlCompileError::Compilation("provider projection exceeds u32".to_string())
        })?;
        columns.push(
            ProviderReadColumnNeed::try_new(
                ordinal,
                source.name.clone(),
                ValueType::new(source.data_type.clone(), source.nullable),
                connector_type,
            )
            .map_err(|error| SqlCompileError::Compilation(error.to_string()))?,
        );
        if predicate_columns
            .insert(
                output.column_id,
                ProviderPredicateColumn {
                    ordinal,
                    value_type: connector_type,
                },
            )
            .is_some()
        {
            return Err(SqlCompileError::Compilation(format!(
                "scan output column id {} is repeated in the provider projection",
                output.column_id
            )));
        }
    }
    Ok((columns.into_boxed_slice(), predicate_columns))
}

pub(super) fn resume_provider_read(
    state: SqlProviderReadCompletionState,
    facts: Box<[ProviderReadFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    control.check()?;
    let expected = state
        .needs
        .iter()
        .map(|need| (need.id(), (need.binding(), need.occurrence())))
        .collect::<BTreeMap<_, _>>();
    for fact in &facts {
        if expected.get(&fact.id()).copied() != Some((fact.binding(), fact.occurrence())) {
            return Err(SqlCompileError::Compilation(format!(
                "provider completion fact {} does not match the requested binding and occurrence",
                fact.id().get()
            )));
        }
    }
    let reads = FinalizedProviderReadSet::try_from_facts(
        facts
            .into_vec()
            .into_iter()
            .map(|fact| (fact, state.common.scan_read_budget)),
    )?;
    let mut builder =
        crate::planner::distributed::build::lower_final_physical_plan_with_provider_reads(
            &state.physical,
            state.common.version,
            state.common.dop_domain,
            reads,
        )
        .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
    state.query_statistics.annotate_final_plan(&mut builder);
    Ok(CompilerStep::ready(
        state.common.version,
        builder,
        state.common.display_intent,
        [],
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        ExactInputVersion, NodeKind, PredicateGuaranteeKind, ProviderColumnReference,
        ProviderReadReference,
    };
    use novarocks_spi::connector::read_stack::{ConnectorReadBinding, ConnectorReadWorkSource};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
    };

    use super::*;
    use crate::catalog::ResolvedAnalyzerTable;
    use crate::compiler::{
        CatalogRelationFact, CatalogRelationNeed, DEFAULT_COMPLETION_LIMITS, MaterializedViewFact,
        ProviderReadColumnFact, ProviderReadFact, ProviderReadLimitFact, ProviderReadPredicateFact,
        ProviderReadProperties, ProviderReadRequestBinding, SessionOptimizerSettings,
        SqlCancellationObservation, SqlCompilation, SqlCompileProgress, SqlCompileProgressError,
        SqlFactBatch, StatisticsFact, builtin_sql_function_catalog, noop_constant_evaluator,
    };
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
    };
    use crate::planning::dml::DmlStatisticsEvidence;

    fn request(sql: &str, intent: SqlCompileIntent) -> SqlFinalPlanCompileRequest {
        request_with_mv(sql, intent, false)
    }

    fn request_with_mv(
        sql: &str,
        intent: SqlCompileIntent,
        mv_enabled: bool,
    ) -> SqlFinalPlanCompileRequest {
        request_with_control(sql, intent, mv_enabled, SqlCompileControl::unbounded())
    }

    fn request_with_control(
        sql: &str,
        intent: SqlCompileIntent,
        mv_enabled: bool,
        control: SqlCompileControl,
    ) -> SqlFinalPlanCompileRequest {
        let optimizer_settings = SessionOptimizerSettings {
            enable_materialized_view_rewrite: Some(mv_enabled),
            ..SessionOptimizerSettings::default()
        };
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([17; 16]).expect("plan version"),
            SqlStatementInput::sql(sql),
            intent,
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings,
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            control,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }

    struct CancelOnSecondObservation(AtomicUsize);

    impl SqlCancellationObservation for CancelOnSecondObservation {
        fn is_cancelled(&self) -> bool {
            self.0.fetch_add(1, Ordering::AcqRel) >= 1
        }
    }

    struct NeverCancelled;

    impl SqlCancellationObservation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct DelayOnSecondObservation(AtomicUsize);

    impl SqlCancellationObservation for DelayOnSecondObservation {
        fn is_cancelled(&self) -> bool {
            if self.0.fetch_add(1, Ordering::AcqRel) == 1 {
                std::thread::sleep(Duration::from_millis(75));
            }
            false
        }
    }

    fn incomplete(progress: SqlCompileProgress) -> SqlCompilation {
        match progress {
            SqlCompileProgress::Incomplete(compilation) => {
                assert_eq!(
                    compilation.usage().exchange_bytes,
                    compilation
                        .needs()
                        .accounted_bytes()
                        .expect("need exchange accounting"),
                    "completion must retain only the currently published need batch",
                );
                compilation
            }
            SqlCompileProgress::Complete(_) => panic!("expected an observation need"),
        }
    }

    fn resolved_table(need: &CatalogRelationNeed) -> ResolvedAnalyzerTable {
        let relation = need.relation();
        ResolvedAnalyzerTable::from_planner(
            Some(&relation.catalog),
            &relation.namespace,
            TableDef {
                name: relation.table.clone(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "order_key".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    SqlTableBindingId::new_for_test(41),
                    SqlTableIdentity::try_new(
                        relation.catalog.clone(),
                        relation.namespace.clone(),
                        relation.table.clone(),
                    )
                    .expect("table identity"),
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            },
        )
    }

    fn connector_binding() -> ConnectorReadBinding {
        let provider_id = ConnectorProviderId::parse("iceberg").expect("provider id");
        let instance_id = ConnectorInstanceId::parse("lakehouse").expect("instance id");
        ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        )
    }

    fn encoded(
        binding: &ConnectorReadBinding,
        category: ConnectorCodecCategory,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).expect("codec revision"),
            ),
            vec![category as u8 + 1].into(),
        )
    }

    fn provider_contract(need: &ProviderReadNeed) -> ProviderReadStaticContract {
        let binding = connector_binding();
        ProviderReadStaticContract {
            sql_binding: need.binding(),
            request: ProviderReadRequestBinding::from_need(need),
            read: ProviderReadReference {
                binding: binding.clone(),
                input_version: ExactInputVersion::try_new([9]).expect("input version"),
                relation: ConnectorReadRelationPayload::new(
                    need.relation().relation_kind(),
                    encoded(&binding, ConnectorCodecCategory::ReadTable),
                    encoded(&binding, ConnectorCodecCategory::ReadView),
                ),
            },
            work_source: ConnectorReadWorkSource::RuntimeSplits,
            selection_digest: [8; 32],
            schema: need
                .columns()
                .iter()
                .map(|column| {
                    ProviderReadColumnFact::new(
                        column.ordinal(),
                        ProviderColumnReference {
                            column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn),
                        },
                        column.engine_type().clone(),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            predicates: need
                .predicates()
                .iter()
                .map(|predicate| {
                    ProviderReadPredicateFact::new(
                        predicate.occurrence(),
                        PredicateGuaranteeKind::Exact,
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            limit: need.limit().map_or(
                ProviderReadLimitFact::NotRequested,
                ProviderReadLimitFact::Exact,
            ),
            provided_properties: ProviderReadProperties::unconstrained(),
            artifact_inputs: Box::default(),
            artifact_refs: Box::default(),
            coverage_evidence: Box::default(),
        }
    }

    fn catalog_facts(compilation: &SqlCompilation) -> SqlFactBatch {
        let needs = match compilation.needs() {
            SqlNeedBatch::CatalogRelations(needs) => needs.to_vec(),
            other => panic!("expected catalog needs, got {other:?}"),
        };
        let facts = needs
            .iter()
            .map(|need| {
                CatalogRelationFact::resolved(need, resolved_table(need))
                    .expect("resolved catalog fact")
            })
            .collect::<Vec<_>>();
        SqlFactBatch::CatalogRelations(facts.into_boxed_slice())
    }

    fn answer_catalog(compilation: SqlCompilation) -> SqlCompileProgress {
        let facts = catalog_facts(&compilation);
        SqlCompiler::finish(compilation, facts, &SqlCompileControl::unbounded())
            .expect("catalog round")
    }

    fn answer_statistics(compilation: SqlCompilation) -> SqlCompileProgress {
        let needs = match compilation.needs() {
            SqlNeedBatch::Statistics(needs) => needs.to_vec(),
            other => panic!("expected statistics needs, got {other:?}"),
        };
        let facts = needs
            .iter()
            .map(|need| {
                StatisticsFact::try_new(
                    need,
                    need.metrics().to_vec(),
                    DmlStatisticsEvidence::Missing {
                        binding: need.binding(),
                        label: "iceberg.db.orders".to_string(),
                        reason: "test fixture has no statistics".to_string(),
                    },
                )
                .expect("statistics fact")
            })
            .collect::<Vec<_>>();
        SqlCompiler::finish(
            compilation,
            SqlFactBatch::Statistics(facts.into_boxed_slice()),
            &SqlCompileControl::unbounded(),
        )
        .expect("statistics round")
    }

    fn answer_provider(compilation: SqlCompilation) -> SqlCompileProgress {
        let needs = match compilation.needs() {
            SqlNeedBatch::ProviderReads(needs) => needs.to_vec(),
            other => panic!("expected provider needs, got {other:?}"),
        };
        let facts = needs
            .iter()
            .map(|need| {
                ProviderReadFact::negotiated(need, provider_contract(need))
                    .expect("provider read fact")
            })
            .collect::<Vec<_>>();
        SqlCompiler::finish(
            compilation,
            SqlFactBatch::ProviderReads(facts.into_boxed_slice()),
            &SqlCompileControl::unbounded(),
        )
        .expect("provider round")
    }

    #[test]
    fn values_query_completes_without_an_observation_round() {
        let seed = request("select 1", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");

        let progress = SqlCompiler::start(seed).expect("completed values plan");

        assert!(matches!(progress, SqlCompileProgress::Complete(_)));
    }

    #[test]
    fn explain_uses_the_same_completion_path_and_records_only_display_intent() {
        let seed = request(
            "select 1",
            SqlCompileIntent::Explain {
                level: crate::explain::ExplainLevel::Verbose,
                analyze: false,
            },
        )
        .try_into_completion()
        .expect("completion seed");

        let SqlCompileProgress::Complete(completed) =
            SqlCompiler::start(seed).expect("completed explain plan")
        else {
            panic!("values explain must not request external observations");
        };
        assert_eq!(
            completed.display_intent(),
            SqlDisplayIntent::Explain {
                level: crate::explain::ExplainLevel::Verbose,
                analyze: false,
            }
        );
    }

    #[test]
    fn external_read_cannot_complete_without_its_catalog_fact() {
        let seed = request("select order_key from orders", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");

        let progress = SqlCompiler::start(seed).expect("catalog need");

        assert!(matches!(
            &progress,
            SqlCompileProgress::Incomplete(compilation)
                if matches!(compilation.needs(), SqlNeedBatch::CatalogRelations(_))
        ));
        assert!(progress.into_complete().is_err());
    }

    #[test]
    fn completion_state_does_not_retain_initial_runtime_control() {
        let cancellation = Arc::new(NeverCancelled);
        let weak = Arc::downgrade(&cancellation);
        let seed = request_with_control(
            "select order_key from orders",
            SqlCompileIntent::Query,
            false,
            SqlCompileControl::new(
                None,
                Arc::clone(&cancellation) as Arc<dyn SqlCancellationObservation>,
            ),
        )
        .try_into_completion()
        .expect("completion seed");

        drop(cancellation);

        assert!(
            weak.upgrade().is_none(),
            "the completion request must release its initial runtime control"
        );
        let _ = SqlCompiler::start(seed).expect("the pure completion state remains usable");
    }

    #[test]
    fn catalog_resume_observes_invocation_cancellation() {
        let seed = request("select order_key from orders", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");
        let compilation = incomplete(SqlCompiler::start(seed).expect("catalog need"));
        let facts = catalog_facts(&compilation);
        let control = SqlCompileControl::new(
            None,
            Arc::new(CancelOnSecondObservation(AtomicUsize::new(0))),
        );

        assert!(matches!(
            SqlCompiler::finish(compilation, facts, &control),
            Err(SqlCompileProgressError::Compile(SqlCompileError::Cancelled))
        ));
    }

    #[test]
    fn catalog_resume_observes_invocation_deadline() {
        let seed = request("select order_key from orders", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");
        let compilation = incomplete(SqlCompiler::start(seed).expect("catalog need"));
        let facts = catalog_facts(&compilation);
        let control = SqlCompileControl::new(
            Some(Instant::now() + Duration::from_millis(50)),
            Arc::new(DelayOnSecondObservation(AtomicUsize::new(0))),
        );

        assert!(matches!(
            SqlCompiler::finish(compilation, facts, &control),
            Err(SqlCompileProgressError::Compile(
                SqlCompileError::DeadlineExceeded
            ))
        ));
    }

    // Counters represent observation-port invocations, driven by the real
    // completion need batches rather than a duplicate eligibility predicate.
    fn complete_base_query_with_counters(
        mut progress: SqlCompileProgress,
        discovery_calls: &AtomicUsize,
        target_catalog_reads: &AtomicUsize,
    ) {
        loop {
            let compilation = match progress {
                SqlCompileProgress::Complete(_) => break,
                SqlCompileProgress::Incomplete(compilation) => compilation,
            };
            progress = match compilation.needs() {
                SqlNeedBatch::MaterializedViews(_) => {
                    discovery_calls.fetch_add(1, Ordering::AcqRel);
                    panic!("unsupported consumer must not request optional MV discovery");
                }
                SqlNeedBatch::CatalogRelations(needs) => {
                    for need in needs {
                        if need.relation().table != "orders" {
                            target_catalog_reads.fetch_add(1, Ordering::AcqRel);
                            panic!(
                                "unavailable optional MV relation must not block the base query"
                            );
                        }
                    }
                    answer_catalog(compilation)
                }
                SqlNeedBatch::Statistics(_) => answer_statistics(compilation),
                SqlNeedBatch::ProviderReads(_) => answer_provider(compilation),
            };
        }
    }

    #[test]
    fn unsupported_caller_never_discovers_optional_mv_definitions() {
        use crate::sql_mode::SqlMode;
        for (sql, connection_mode) in [
            ("SELECT order_key FROM orders", "GROUP_CONCAT_LEGACY"),
            (
                "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ order_key FROM orders",
                "32",
            ),
            (
                "SELECT order_key FROM orders UNION ALL SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ order_key FROM orders",
                "32",
            ),
            (
                "SELECT order_key FROM (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ order_key FROM orders) d",
                "32",
            ),
        ] {
            let mut request = request_with_mv(sql, SqlCompileIntent::Query, true);
            request.session.sql_semantics = request
                .session
                .sql_semantics
                .clone()
                .with_sql_mode(SqlMode::from_assignment(connection_mode));
            let seed = request.try_into_completion().expect("eligible base query");
            let discovery_calls = AtomicUsize::new(0);
            let target_catalog_reads = AtomicUsize::new(0);
            complete_base_query_with_counters(
                SqlCompiler::start(seed).expect("start base query"),
                &discovery_calls,
                &target_catalog_reads,
            );
            assert_eq!(
                discovery_calls.load(Ordering::Acquire),
                0,
                "an unrelated bad MV cannot be observed for this query"
            );
            assert_eq!(target_catalog_reads.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn modern_root_override_keeps_existing_mv_discovery_policy() {
        use crate::sql_mode::SqlMode;
        let mut request = request_with_mv(
            "SELECT /*+ SET_VAR(sql_mode=32) */ order_key FROM orders",
            SqlCompileIntent::Query,
            true,
        );
        request.session.sql_semantics = request
            .session
            .sql_semantics
            .clone()
            .with_sql_mode(SqlMode::from_assignment("GROUP_CONCAT_LEGACY"));
        let seed = request.try_into_completion().unwrap();
        let compilation = incomplete(SqlCompiler::start(seed).unwrap());
        assert!(matches!(
            compilation.needs(),
            SqlNeedBatch::MaterializedViews(_)
        ));
    }

    #[test]
    fn stored_unsupported_definition_never_requests_optional_target_catalog() {
        use super::super::mv_rewrite::{
            SqlMvDefinitionResolutionContext, SqlMvRelationOccurrenceId,
            SqlMvRewriteBaseTableFacts, SqlMvRewriteSourceOccurrenceFacts,
        };
        let seed = request_with_mv(
            "SELECT order_key FROM orders",
            SqlCompileIntent::Query,
            true,
        )
        .try_into_completion()
        .unwrap();
        let mv = incomplete(SqlCompiler::start(seed).unwrap());
        let need = match mv.needs() {
            SqlNeedBatch::MaterializedViews(needs) => needs[0].clone(),
            other => panic!("modern query retains MV discovery, got {other:?}"),
        };
        let discovery_calls = AtomicUsize::new(0);
        discovery_calls.fetch_add(1, Ordering::AcqRel);
        let table = novarocks_types::naming::TableIdentity {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let mut statements = novarocks_parser::parse(
            "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ order_key FROM orders",
        )
        .unwrap();
        let novarocks_parser::ast::Statement::Query(query) = statements.remove(0) else {
            panic!("query")
        };
        let definition = SqlMvRewriteDefinitionFacts::try_new(
            91,
            [11; 32],
            query,
            SqlMvDefinitionResolutionContext::try_new("iceberg".to_string(), "db".to_string())
                .unwrap(),
            "iceberg".to_string(),
            Some(novarocks_types::naming::TableIdentity {
                table: "missing_mv_target".to_string(),
                ..table.clone()
            }),
            vec![
                SqlMvRewriteSourceOccurrenceFacts::try_new(
                    SqlMvRelationOccurrenceId::new(0),
                    table,
                    "orders".to_string(),
                    None,
                    SqlMvRewriteBaseTableFacts::unavailable("no publication".to_string()),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let progress = SqlCompiler::finish(
            mv,
            SqlFactBatch::MaterializedViews(Box::from([MaterializedViewFact::observed(
                &need,
                Box::from([definition]),
            )])),
            &SqlCompileControl::unbounded(),
        )
        .expect("stored unsupported candidate must not fail base query");
        let target_catalog_reads = AtomicUsize::new(0);
        complete_base_query_with_counters(progress, &discovery_calls, &target_catalog_reads);
        assert_eq!(
            discovery_calls.load(Ordering::Acquire),
            1,
            "modern discovery policy is unchanged"
        );
        assert_eq!(
            target_catalog_reads.load(Ordering::Acquire),
            0,
            "no optional target read before late eligibility diagnostic"
        );
    }

    #[test]
    fn materialized_view_enabled_external_read_completes_in_four_exact_rounds() {
        let seed = request_with_mv(
            "select order_key from orders",
            SqlCompileIntent::Query,
            true,
        )
        .try_into_completion()
        .expect("completion seed");
        let mv = incomplete(SqlCompiler::start(seed).expect("MV need"));
        let mv_need = match mv.needs() {
            SqlNeedBatch::MaterializedViews(needs) if needs.len() == 1 => needs[0].clone(),
            other => panic!("expected one MV need, got {other:?}"),
        };
        let catalog = incomplete(
            SqlCompiler::finish(
                mv,
                SqlFactBatch::MaterializedViews(Box::from([MaterializedViewFact::missing(
                    &mv_need,
                    "no matching MV",
                )
                .expect("missing MV fact")])),
                &SqlCompileControl::unbounded(),
            )
            .expect("MV round"),
        );
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_statistics(statistics));

        let completed = answer_provider(provider);

        assert!(matches!(completed, SqlCompileProgress::Complete(_)));
    }

    #[test]
    fn materialized_view_disabled_external_read_completes_in_three_exact_rounds() {
        let seed = request("select order_key from orders", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");
        let catalog = incomplete(SqlCompiler::start(seed).expect("catalog need"));
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_statistics(statistics));

        let completed = answer_provider(provider);

        assert!(matches!(completed, SqlCompileProgress::Complete(_)));
    }

    #[test]
    fn self_join_preserves_two_occurrences_for_one_provider_reference() {
        let seed = request(
            "select lhs.order_key from orders lhs join orders rhs on lhs.order_key = rhs.order_key",
            SqlCompileIntent::Query,
        )
        .try_into_completion()
        .expect("completion seed");
        let catalog = incomplete(SqlCompiler::start(seed).expect("catalog need"));
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_statistics(statistics));
        let provider_needs = match provider.needs() {
            SqlNeedBatch::ProviderReads(needs) => needs,
            other => panic!("expected provider needs, got {other:?}"),
        };
        assert_eq!(provider_needs.len(), 2);
        assert_eq!(provider_needs[0].binding(), provider_needs[1].binding());
        assert_ne!(
            provider_needs[0].occurrence(),
            provider_needs[1].occurrence(),
            "self-join reads need distinct query-local occurrence identities"
        );
        let replayed_occurrences = match provider.needs() {
            SqlNeedBatch::ProviderReads(needs) => needs
                .iter()
                .map(ProviderReadNeed::occurrence)
                .collect::<Vec<_>>(),
            other => panic!("expected provider needs on replay, got {other:?}"),
        };
        assert_eq!(
            replayed_occurrences,
            provider_needs
                .iter()
                .map(ProviderReadNeed::occurrence)
                .collect::<Vec<_>>(),
            "replaying the same continuation must preserve occurrence identities"
        );

        let completed = answer_provider(provider)
            .into_complete()
            .expect("repeated relation completes");
        let scans = completed
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .filter_map(|node| match &node.kind {
                NodeKind::Scan {
                    occurrence,
                    relation,
                    ..
                } => Some((*occurrence, relation.read().clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(scans.len(), 2);
        assert_ne!(scans[0].0, scans[1].0);
        assert_eq!(
            scans[0].1, scans[1].1,
            "occurrence identity must not split a reusable provider relation identity"
        );
    }

    #[test]
    fn finalized_provider_reads_reject_one_occurrence_across_distinct_bindings() {
        let seed = request("select order_key from orders", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");
        let catalog = incomplete(SqlCompiler::start(seed).expect("catalog need"));
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_statistics(statistics));
        let original = match provider.needs() {
            SqlNeedBatch::ProviderReads(needs) => needs[0].clone(),
            other => panic!("expected provider needs, got {other:?}"),
        };
        let conflicting = ProviderReadNeed::try_new(
            CompileNeedId::new(99),
            original.occurrence(),
            SqlTableBindingId::new_for_test(99),
            original.relation().clone(),
            original.columns().to_vec(),
            original.predicates().to_vec(),
            original.limit(),
        )
        .expect("conflicting test need");
        let original_fact =
            ProviderReadFact::negotiated(&original, provider_contract(&original)).unwrap();
        let conflicting_fact =
            ProviderReadFact::negotiated(&conflicting, provider_contract(&conflicting)).unwrap();
        let budget = ScanReadBudget {
            max_batch_rows: MAX_SCAN_BATCH_ROWS,
            max_batch_bytes: MAX_SCAN_BATCH_BYTES,
        };

        assert!(matches!(
            FinalizedProviderReadSet::try_from_facts([
                (original_fact, budget),
                (conflicting_fact, budget),
            ]),
            Err(SqlCompileError::Compilation(message))
                if message.contains("repeat scan occurrence")
        ));
    }

    fn available_statistics_evidence(rows: u64) -> DmlStatisticsEvidence {
        available_statistics_evidence_with_average_size(rows, None)
    }

    fn available_statistics_evidence_with_average_size(
        rows: u64,
        average_size: Option<f64>,
    ) -> DmlStatisticsEvidence {
        use novarocks_spi::connector::{
            StatisticsBasisRelation, StatisticsDataVersion, StatisticsEvidence,
            StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricObservation,
            StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
            StatisticsNumericNature, StatisticsRowCoverage,
        };
        let basis = StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"frozen-data"))
            .expect("data version");
        let mut metrics = std::collections::BTreeMap::from([(
            StatisticsMetric::RowCount,
            StatisticsMetricState::Available(StatisticsMetricObservation::new(
                StatisticsMetricValue::U64(rows),
                basis.clone(),
                StatisticsMetricSource::CurrentManifest,
                StatisticsNumericNature::Exact,
                StatisticsBasisRelation::Identical,
            )),
        )]);
        if let Some(width) = average_size {
            metrics.insert(
                StatisticsMetric::AverageSize {
                    column: "order_key".into(),
                },
                StatisticsMetricState::Available(StatisticsMetricObservation::new(
                    StatisticsMetricValue::F64(width),
                    basis.clone(),
                    StatisticsMetricSource::CurrentManifest,
                    StatisticsNumericNature::TwoSidedApproximate,
                    StatisticsBasisRelation::Identical,
                )),
            );
        }
        DmlStatisticsEvidence::Available {
            binding: SqlTableBindingId::new_for_test(41),
            label: "iceberg.db.orders".to_string(),
            columns: vec![novarocks_types::schema::ColumnDef {
                name: "order_key".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            evidence: StatisticsEvidence::try_new(
                basis.clone(),
                StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"frozen-revision"))
                    .expect("evidence revision"),
                StatisticsRowCoverage::AllVisibleRows,
                metrics,
            )
            .expect("frozen row count"),
        }
    }

    fn answer_exact_statistics(compilation: SqlCompilation, rows: u64) -> SqlCompileProgress {
        answer_frozen_statistics(compilation, available_statistics_evidence(rows))
    }

    fn answer_frozen_statistics(
        compilation: SqlCompilation,
        frozen: DmlStatisticsEvidence,
    ) -> SqlCompileProgress {
        use novarocks_spi::connector::{
            StatisticsMetricState, StatisticsMissing, StatisticsMissingKind,
        };
        let needs = match compilation.needs() {
            SqlNeedBatch::Statistics(needs) => needs.to_vec(),
            other => panic!("expected statistics needs, got {other:?}"),
        };
        let DmlStatisticsEvidence::Available {
            binding,
            label,
            columns,
            evidence,
        } = frozen
        else {
            unreachable!()
        };
        let facts = needs
            .iter()
            .map(|need| {
                assert_eq!(need.binding(), binding);
                // Answer exactly the requested metrics; unavailable column metrics
                // remain missing instead of borrowing defaults from another query.
                let metrics = need
                    .metrics()
                    .iter()
                    .map(|metric| {
                        (
                            metric.clone(),
                            evidence.metrics().get(metric).cloned().unwrap_or_else(|| {
                                StatisticsMetricState::Missing(StatisticsMissing {
                                    kind: StatisticsMissingKind::NotCollected,
                                    message: "not part of the frozen fixture".into(),
                                })
                            }),
                        )
                    })
                    .collect();
                let answer = novarocks_spi::connector::StatisticsEvidence::try_new(
                    evidence.data_version().clone(),
                    evidence.evidence_revision().clone(),
                    evidence.row_coverage(),
                    metrics,
                )
                .expect("complete requested metrics");
                StatisticsFact::try_new(
                    need,
                    need.metrics().to_vec(),
                    DmlStatisticsEvidence::Available {
                        binding,
                        label: label.clone(),
                        columns: columns.clone(),
                        evidence: answer,
                    },
                )
                .expect("statistics fact")
            })
            .collect::<Vec<_>>();
        SqlCompiler::finish(
            compilation,
            SqlFactBatch::Statistics(facts.into_boxed_slice()),
            &SqlCompileControl::unbounded(),
        )
        .expect("statistics round")
    }

    fn complete_with_exact_statistics(
        sql: &str,
        intent: SqlCompileIntent,
        rows: u64,
    ) -> SqlCompletedPlan {
        let catalog = incomplete(
            SqlCompiler::start(
                request(sql, intent)
                    .try_into_completion()
                    .expect("completion seed"),
            )
            .expect("catalog need"),
        );
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_exact_statistics(statistics, rows));
        answer_provider(provider)
            .into_complete()
            .expect("completed plan")
    }

    fn frozen_table_statistics(plan: &novarocks_physical_plan::PhysicalPlan) -> Vec<&str> {
        plan.annotations()
            .iter()
            .filter_map(|annotation| {
                (annotation.subject == novarocks_physical_plan::AnnotationSubject::Plan
                    && annotation.key.as_ref()
                        == crate::optimizer::stats_input::TABLE_STATISTICS_ANNOTATION_KEY)
                    .then_some(annotation.value.as_ref())
            })
            .collect()
    }

    #[test]
    fn select_and_explain_retain_only_the_optimizer_consumed_statistics() {
        for rows in [7, 13] {
            let query = complete_with_exact_statistics(
                "select order_key from orders",
                SqlCompileIntent::Query,
                rows,
            );
            let explain = complete_with_exact_statistics(
                "select order_key from orders",
                SqlCompileIntent::Explain {
                    level: crate::explain::ExplainLevel::Costs,
                    analyze: false,
                },
                rows,
            );
            let expected = format!(
                "TABLE STATS ref=0 table=iceberg.db.orders rows={rows} confidence=Exact source=IcebergManifest"
            );
            assert_eq!(
                frozen_table_statistics(query.plan()),
                vec![expected.as_str()]
            );
            assert_eq!(
                frozen_table_statistics(explain.plan()),
                vec![expected.as_str()]
            );
            for plan in [query.plan(), explain.plan()] {
                // Exact source cardinality must have reached optimization as
                // well as the final-plan provenance; a second display-only
                // catalog lookup could not satisfy this relationship.
                assert!(plan.annotations().iter().any(|annotation| {
                    annotation.key.as_ref() == "optimizer.statistics"
                        && (annotation.value.as_ref() == format!("rows={rows}")
                            || annotation.value.starts_with(&format!("rows={rows},")))
                }));
                let costs = crate::explain::completed_tree::render_completed_plan_tree(
                    plan,
                    crate::explain::ExplainLevel::Costs,
                )
                .expect("costs text");
                assert!(costs.iter().any(|line| line == &expected));
                let contract = crate::explain::completed::render_completed_plan(
                    plan,
                    &[],
                    crate::explain::ExplainLevel::Contract,
                    None,
                    crate::explain::completed::ExplainRenderBudget::default(),
                )
                .expect("contract text");
                assert!(contract.iter().any(|line| line.contains(&expected)));
                let normal = crate::explain::completed_tree::render_completed_plan_tree(
                    plan,
                    crate::explain::ExplainLevel::Normal,
                )
                .expect("normal text");
                assert!(!normal.iter().any(|line| line.starts_with("TABLE STATS")));
            }
        }
    }

    #[test]
    fn self_join_keeps_distinct_statistics_refs_and_relation_aliases() {
        let completed = complete_with_exact_statistics(
            "select lhs.order_key from orders lhs join orders rhs on lhs.order_key = rhs.order_key",
            SqlCompileIntent::Explain {
                level: crate::explain::ExplainLevel::Costs,
                analyze: false,
            },
            17,
        );
        assert_eq!(
            frozen_table_statistics(completed.plan()),
            vec![
                "TABLE STATS ref=0 table=iceberg.db.orders rows=17 confidence=Exact source=IcebergManifest",
                "TABLE STATS ref=1 table=iceberg.db.orders rows=17 confidence=Exact source=IcebergManifest",
            ]
        );
        let names = completed
            .plan()
            .annotations()
            .iter()
            .filter(|annotation| annotation.key.as_ref() == "sql.display_name")
            .map(|annotation| annotation.value.as_ref())
            .collect::<Vec<_>>();
        assert!(names.contains(&"lhs.order_key"));
        assert!(names.contains(&"rhs.order_key"));
    }

    #[test]
    fn missing_and_source_free_statistics_are_never_fabricated() {
        let catalog = incomplete(
            SqlCompiler::start(
                request("select order_key from orders", SqlCompileIntent::Query)
                    .try_into_completion()
                    .expect("seed"),
            )
            .expect("catalog"),
        );
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_statistics(statistics));
        let completed = answer_provider(provider)
            .into_complete()
            .expect("completed");
        let provenance = frozen_table_statistics(completed.plan());
        assert_eq!(provenance.len(), 1);
        assert!(provenance[0].contains("rows=missing"));
        assert!(provenance[0].contains("test fixture has no statistics"));
        assert!(!provenance[0].contains("source=IcebergManifest"));
        let values = SqlCompiler::start(
            request("select 1", SqlCompileIntent::Query)
                .try_into_completion()
                .expect("seed"),
        )
        .expect("values")
        .into_complete()
        .expect("complete");
        assert!(frozen_table_statistics(values.plan()).is_empty());
    }

    struct FrozenOrdersCatalog;

    impl crate::catalog::PlannerTableProvider for FrozenOrdersCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            Ok(ResolvedAnalyzerTable::from_planner(
                catalog,
                database,
                TableDef {
                    name: table.to_string(),
                    columns: vec![novarocks_types::schema::ColumnDef {
                        name: "order_key".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::Sql(SqlScanSource::new(
                        SqlTableBindingId::new_for_test(41),
                        SqlTableIdentity::try_new(
                            "iceberg".to_string(),
                            database.to_string(),
                            table.to_string(),
                        )
                        .expect("identity"),
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
                },
            ))
        }
    }
    impl crate::compiler::SqlCatalogSnapshot for FrozenOrdersCatalog {
        fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
            self
        }
    }

    #[test]
    fn dml_read_completion_retains_the_admitted_snapshot_after_optimization() {
        let catalog = FrozenOrdersCatalog;
        let functions = builtin_sql_function_catalog();
        let analyzed = SqlCompiler::analyze(crate::compiler::SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select order_key from orders"),
            SqlCompileIntent::Query,
            request("select 1", SqlCompileIntent::Query).session,
            SqlPlanningEnvironment::Distributed,
            &catalog,
            functions,
            noop_constant_evaluator(),
            None,
            SqlCompileControl::unbounded(),
        ))
        .expect("analysis")
        .into_pending()
        .expect("analyzed source");
        let statistics = DmlStatisticsSnapshot::from_evidence([available_statistics_evidence(23)]);
        let (completion, needs) = crate::planning::dml::begin_final_dml_read_plan(
            crate::compiler::SqlOptimizeRequest::new(
                analyzed,
                &statistics,
                SqlCompileControl::unbounded(),
            ),
            &SessionOptimizerSettings::default(),
        )
        .expect("optimized DML source");
        drop(statistics);
        let reads =
            crate::planning::dml::DmlFinalizedProviderReadSet::try_new(needs.iter().map(|need| {
                crate::planning::dml::DmlFinalizedProviderRead {
                    fact: ProviderReadFact::negotiated(need, provider_contract(need))
                        .expect("provider fact"),
                    read_budget: ScanReadBudget {
                        max_batch_rows: MAX_SCAN_BATCH_ROWS,
                        max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                    },
                }
            }))
            .expect("frozen reads");
        let plan = completion
            .finish(
                PlanVersionId::try_new([24; 16]).expect("version"),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                reads,
            )
            .expect("DML final plan");
        let expected = "TABLE STATS ref=0 table=iceberg.db.orders rows=23 confidence=Exact source=IcebergManifest";
        assert_eq!(frozen_table_statistics(&plan), vec![expected]);
        assert!(
            crate::explain::completed_tree::render_completed_plan_tree(
                &plan,
                crate::explain::ExplainLevel::Costs
            )
            .expect("DML costs")
            .iter()
            .any(|line| line == expected)
        );
    }

    #[test]
    fn final_broadcast_annotation_uses_the_same_frozen_cardinality_and_byte_formula() {
        let mut compile_request = request(
            "select lhs.order_key from orders lhs join orders rhs on lhs.order_key = rhs.order_key",
            SqlCompileIntent::Query,
        );
        compile_request
            .session
            .optimizer_settings
            .effective_backend_count = Some(3.0);
        compile_request
            .session
            .optimizer_settings
            .cbo_broadcast_node_mem_budget_bytes = Some(268435456.0);
        let catalog = incomplete(
            SqlCompiler::start(compile_request.try_into_completion().expect("seed"))
                .expect("catalog"),
        );
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_exact_statistics(statistics, 10));
        let completed = answer_provider(provider).into_complete().expect("complete");
        let provenance = frozen_table_statistics(completed.plan());
        assert_eq!(provenance.len(), 2);
        assert!(
            provenance
                .iter()
                .all(|row| row.contains("rows=10 confidence=Exact source=IcebergManifest"))
        );
        let decision = completed
            .plan()
            .annotations()
            .iter()
            .find(|annotation| annotation.key.as_ref() == "optimizer.broadcast")
            .expect("small exact build has a frozen broadcast decision");
        let fields = decision
            .value
            .split(", ")
            .map(|part| part.split_once('=').expect("field"))
            .collect::<std::collections::BTreeMap<_, _>>();
        let number = |name: &str| fields[name].parse::<f64>().expect("numeric decision fact");
        assert_eq!(fields["verdict"], "feasible");
        assert_eq!(number("backends"), 3.0);
        assert_eq!(number("per_node_budget_bytes"), 268435456.0);
        // This fixture leaves AverageSize missing: the existing unknown-column
        // statistics policy supplies eight bytes per row, not a measurement
        // inferred from Int64. The supplied-width test below replaces it.
        assert_eq!(number("build_bytes"), 10.0 * 8.0);
        assert_eq!(number("hash_table_bytes"), 80.0 / 0.75 + 10.0 * 16.0);
        assert_eq!(
            number("fanout_bytes"),
            80.0 * 3.0 * number("risk_multiplier")
        );
        assert!(
            number("hash_table_bytes") * number("risk_multiplier")
                <= number("per_node_budget_bytes")
        );
    }

    #[test]
    fn final_broadcast_consumes_supplied_approximate_width_and_keeps_exact_row_provenance() {
        for supplied_width in [None, Some(20.25), Some(42.5)] {
            let mut compile_request = request(
                "select lhs.order_key from orders lhs join orders rhs on lhs.order_key = rhs.order_key",
                SqlCompileIntent::Explain {
                    level: crate::explain::ExplainLevel::Costs,
                    analyze: false,
                },
            );
            compile_request
                .session
                .optimizer_settings
                .effective_backend_count = Some(3.0);
            compile_request
                .session
                .optimizer_settings
                .cbo_broadcast_node_mem_budget_bytes = Some(268435456.0);
            let catalog = incomplete(
                SqlCompiler::start(compile_request.try_into_completion().expect("seed"))
                    .expect("catalog"),
            );
            let statistics = incomplete(answer_catalog(catalog));
            let provider = incomplete(answer_frozen_statistics(
                statistics,
                available_statistics_evidence_with_average_size(10, supplied_width),
            ));
            // The owned continuation is the only remaining statistics holder.
            // No catalog object or mutable external evidence exists at finish.
            let completed = answer_provider(provider).into_complete().expect("complete");
            let provenance = frozen_table_statistics(completed.plan());
            assert_eq!(provenance.len(), 2);
            assert!(
                provenance
                    .iter()
                    .all(|line| line.contains("rows=10 confidence=Exact source=IcebergManifest"))
            );
            let decision = completed
                .plan()
                .annotations()
                .iter()
                .find(|annotation| annotation.key.as_ref() == "optimizer.broadcast")
                .expect("frozen broadcast decision");
            let fields = decision
                .value
                .split(", ")
                .map(|part| part.split_once('=').expect("decision field"))
                .collect::<std::collections::BTreeMap<_, _>>();
            let number = |name: &str| fields[name].parse::<f64>().expect("finite decision number");
            // Missing width keeps the existing ColumnStatistic::unknown(8)
            // policy. Actual approximate width replaces that policy per metric;
            // its inexact nature does not turn an exact RowCount into missing.
            let width = supplied_width.unwrap_or(8.0);
            let payload = 10.0 * width;
            assert_eq!(fields["verdict"], "feasible");
            assert_eq!(fields["forced"], "false");
            assert_eq!(number("build_bytes"), payload);
            assert_eq!(number("hash_table_bytes"), payload / 0.75 + 10.0 * 16.0);
            assert_eq!(
                number("fanout_bytes"),
                payload * 3.0 * number("risk_multiplier")
            );
            assert_eq!(number("backends"), 3.0);
            assert_eq!(number("per_node_budget_bytes"), 268435456.0);
            assert!(number("hash_table_bytes") * number("risk_multiplier") <= 268435456.0);
            assert!(number("fanout_bytes") <= 268435456.0);
            let text = crate::explain::completed_tree::render_completed_plan_tree(
                completed.plan(),
                crate::explain::ExplainLevel::Costs,
            )
            .expect("completed costs");
            assert!(
                text.iter()
                    .any(|line| line.contains(decision.value.as_ref()))
            );
        }
    }

    #[test]
    fn source_free_ctes_do_not_fabricate_base_statistics_or_known_zero_rows() {
        let scenarios = [
            (
                "WITH big_probe AS (SELECT generate_series AS k FROM TABLE(generate_series(1, 1000000))), no_stats AS (SELECT k FROM (SELECT generate_series + 0 AS k FROM TABLE(generate_series(1, 100000000))) projected WHERE k > 0) SELECT COUNT(*) AS cnt FROM big_probe p JOIN no_stats b ON p.k = b.k",
                false,
            ),
            (
                "WITH p AS (SELECT generate_series AS k FROM TABLE(generate_series(1, 1000))), b AS (SELECT generate_series AS k FROM TABLE(generate_series(1, 10))) SELECT COUNT(*) AS cnt FROM p JOIN b ON p.k = b.k",
                true,
            ),
        ];
        for (sql, small_build) in scenarios {
            let mut compile_request = request(
                sql,
                SqlCompileIntent::Explain {
                    level: crate::explain::ExplainLevel::Costs,
                    analyze: false,
                },
            );
            compile_request
                .session
                .optimizer_settings
                .effective_backend_count = Some(3.0);
            compile_request
                .session
                .optimizer_settings
                .cbo_broadcast_node_mem_budget_bytes = Some(268435456.0);
            let completed =
                SqlCompiler::start(compile_request.try_into_completion().expect("seed"))
                    .expect("source-free compilation")
                    .into_complete()
                    .expect("no catalog/statistics/provider needs");
            assert!(frozen_table_statistics(completed.plan()).is_empty());
            let text = crate::explain::completed_tree::render_completed_plan_tree(
                completed.plan(),
                crate::explain::ExplainLevel::Costs,
            )
            .expect("completed costs");
            if small_build {
                let decision = completed
                    .plan()
                    .annotations()
                    .iter()
                    .find(|annotation| annotation.key.as_ref() == "optimizer.broadcast")
                    .expect("ten i64 keys permit broadcast");
                let fields = decision
                    .value
                    .split(", ")
                    .map(|part| part.split_once('=').expect("decision field"))
                    .collect::<std::collections::BTreeMap<_, _>>();
                let number = |name: &str| fields[name].parse::<f64>().expect("number");
                assert_eq!(fields["verdict"], "feasible");
                assert_eq!(number("risk_multiplier"), 2.0);
                assert_eq!(number("build_bytes"), 80.0);
                assert_eq!(number("hash_table_bytes"), 80.0 / 0.75 + 10.0 * 16.0);
                assert_eq!(number("fanout_bytes"), 480.0);
                assert!(
                    text.iter()
                        .any(|line| line.contains("HASH JOIN (BROADCAST, INNER"))
                );
            } else {
                // These expressions have no external base-row observation.
                // Generated cardinalities remain positive derived facts: the
                // absence of a QueryStatsSnapshot is not a known-zero table.
                assert!(
                    text.iter()
                        .any(|line| line.contains("HASH JOIN (PARTITIONED, INNER"))
                );
                assert!(
                    !text
                        .iter()
                        .any(|line| line.contains("HASH JOIN (BROADCAST"))
                );
                assert!(
                    text.iter()
                        .any(|line| line.contains("GENERATE_SERIES(1, 100000000, 1)"))
                );
                assert!(completed.plan().annotations().iter().any(|annotation| {
                    annotation.key.as_ref() == "optimizer.statistics"
                        && annotation.value.starts_with("rows=100000000")
                }));
            }
        }
    }
}
