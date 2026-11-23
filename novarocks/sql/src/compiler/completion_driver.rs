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
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ProviderReadOccurrenceId};
use novarocks_spi::connector::StatisticsMetric;

use super::completion::{
    CatalogRelationFact, CompileNeedId, CompilerContinuation, CompilerStep, CompletionLimits,
    MaterializedViewFact, MaterializedViewNeed, MaterializedViewOutcome, ProviderReadColumnNeed,
    ProviderReadFact, ProviderReadNeed, ProviderReadStaticContract, SqlCompileRequest,
    SqlDisplayIntent, SqlNeedBatch, StatisticsFact, StatisticsNeed,
    provider_relation_need_from_sql_scan,
};
use super::completion_catalog::CatalogCompletionState;
use super::completion_predicate::{ProviderPredicateColumn, lower_provider_predicates};
use super::mv_rewrite::{MvRewriteDefinitionIndex, SqlMvRewriteDefinitionFacts};
use super::table_sequence::{
    TableSequenceArgumentFact, TableSequenceOccurrence, TableSequencePass,
};
use super::{
    SqlAnalyzeOutput, SqlAnalyzeRequest, SqlAnalyzedQuery, SqlCompileControl, SqlCompileError,
    SqlCompileIntent, SqlCompiler, SqlFunctionCatalog, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
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

    sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: SqlCompileControl,
    dop_domain: PipelineDopDomain,
    limits: CompletionLimits,
    catalog_qualifications: super::CatalogQualifications,
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
        sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
        constant_policy: novarocks_functions::ConstantPolicy,
        control: SqlCompileControl,
        dop_domain: PipelineDopDomain,
        limits: CompletionLimits,
    ) -> Self {
        Self {
            version,
            statement,
            intent,
            session,
            environment,
            functions: functions.snapshot(),

            sealed_functions,
            constant_policy,
            control,
            dop_domain,
            limits,
            catalog_qualifications: super::CatalogQualifications::default(),
        }
    }

    pub fn with_catalog_qualifications(
        mut self,
        qualifications: super::CatalogQualifications,
    ) -> Self {
        self.catalog_qualifications = qualifications;
        self
    }

    pub fn quoted_metadata_catalogs(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorInstanceId>, SqlCompileError> {
        self.statement
            .quoted_metadata_catalogs(&self.session, &self.control)
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

            sealed_functions,
            constant_policy,
            control,
            dop_domain,
            limits,
            catalog_qualifications,
        } = self;
        control.check()?;
        validate_final_intent(&intent)?;
        validate_dop_domain(dop_domain)?;
        let display_intent = display_intent(&intent);
        let root_session_admitted = statement.root_session_admitted();
        let query = super::parse_query(&statement)?;
        let query_semantics;
        let effective = if root_session_admitted {
            &session.sql_semantics
        } else {
            query_semantics = crate::sql_mode::query_sql_semantics(&session.sql_semantics, &query)
                .map_err(SqlCompileError::from)?;
            &query_semantics
        };
        let consumer_requires_semantic_snapshot =
            crate::sql_mode::query_requires_semantic_snapshot(effective, &query);
        // Retain the producer's actual checked constants across discovery and
        // catalog rounds. The loan is rebuilt against the admitted query;
        // syntax transformations must validate the original structural sites.
        let materialized_source = match statement.kind {
            super::SqlStatementInputKind::MaterializedValuesQuery { source, .. } => {
                source.loan(&query, &control)?;
                Some(source)
            }
            _ => None,
        };
        let common = FinalPlanCommon {
            version,
            intent,
            session,
            environment,
            functions,

            sealed_functions,
            constant_policy,
            dop_domain,
            display_intent,
            materialized_source,
            root_session_admitted,
            catalog_qualifications,
        };

        // A persisted semantic snapshot is required before an optional MV
        // definition can be replayed. Decide before publishing discovery or
        // catalog needs so an unrelated candidate cannot fail the base query.
        let mv_enabled = common.session.optimizer_settings.mv_rewrite_enabled()
            && !consumer_requires_semantic_snapshot;
        let initial_catalog = CatalogCompletionState::try_new_with_qualifications(
            query.clone(),
            &[],
            &[],
            common.session.current_catalog.as_deref(),
            &common.session.current_database,
            TableLookupMode::SchemaOnly,
            if mv_enabled { 1 } else { 0 },
            &common.session.sql_semantics,
            common.catalog_qualifications.clone(),
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

    sealed_functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    dop_domain: PipelineDopDomain,
    display_intent: SqlDisplayIntent,
    // Both variants hold Arc-backed constant collections. Cloning this source
    // for analysis preserves original pool/ordinal facts, not AST pointers.
    materialized_source: Option<super::materialized_values::MaterializedValues>,
    root_session_admitted: bool,
    catalog_qualifications: super::CatalogQualifications,
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
impl SqlCatalogCompletionState {
    pub(super) fn is_discovery_round(&self) -> bool {
        self.catalog.is_discovery_round()
    }
}

pub(crate) struct SqlTableSequenceCompletionState {
    catalog: SqlCatalogCompletionState,
    occurrences: Vec<TableSequenceOccurrence>,
    next_need_ordinal: u32,
}

pub(crate) struct SqlStatisticsCompletionState {
    common: FinalPlanCommon,
    analyzed: SqlAnalyzedQuery,
    needs: Box<[StatisticsNeed]>,
    next_need_ordinal: u32,
}

pub(crate) struct SqlProviderReadCompletionState {
    root_allow_throw_exception: bool,
    common: FinalPlanCommon,
    root_semantics: super::root_output::RootOutputSemantics,
    physical: PhysicalPlanNode,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    needs: Box<[ProviderReadNeed]>,
    system_needs: Box<[super::completion::SystemTableSnapshotNeed]>,
}

impl SqlProviderReadCompletionState {
    pub(super) const fn kind(&self) -> super::completion::NeedKind {
        if self.system_needs.is_empty() {
            super::completion::NeedKind::ProviderRead
        } else {
            super::completion::NeedKind::ProviderInputs
        }
    }
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
}

impl FinalizedProviderReadSet {
    pub(crate) fn try_from_facts(
        facts: impl IntoIterator<Item = ProviderReadFact>,
    ) -> Result<Self, SqlCompileError> {
        let mut entries = BTreeMap::new();
        for fact in facts {
            let occurrence = fact.occurrence();
            let binding = fact.binding();
            if entries
                .insert(
                    occurrence,
                    FinalizedProviderRead {
                        binding,
                        contract: fact.into_contract(),
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
    ) -> Self {
        Self {
            entries: BTreeMap::from([(occurrence, FinalizedProviderRead { binding, contract })]),
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
    let catalog = CatalogCompletionState::try_new_with_qualifications(
        *state.query,
        &[],
        &additional_relations,
        state.common.session.current_catalog.as_deref(),
        &state.common.session.current_database,
        TableLookupMode::SchemaOnly,
        1,
        &state.common.session.sql_semantics,
        state.common.catalog_qualifications.clone(),
    )?;
    catalog_or_analyze_step(state.common, catalog, Some(mv_definitions), control)
}

pub(super) fn resume_catalog(
    mut state: SqlCatalogCompletionState,
    facts: Box<[CatalogRelationFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    control.check()?;
    state.catalog.accept_facts(facts, control)?;
    if !state.catalog.needs().is_empty() {
        let needs = state.catalog.needs().to_vec().into_boxed_slice();
        return Ok(CompilerStep::need(
            SqlNeedBatch::CatalogRelations(needs),
            CompilerContinuation::catalog(state),
        ));
    }
    analyze_completed_catalog(state, control)
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

    analyze_completed_catalog(
        SqlCatalogCompletionState {
            common,
            catalog,
            mv_definitions,
        },
        control,
    )
}

fn analyze_completed_catalog(
    state: SqlCatalogCompletionState,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    let pass = TableSequencePass::request(
        Arc::clone(&state.common.sealed_functions),
        state.catalog.next_need_ordinal(),
    );
    let analyzed = analyze_with_completed_catalog(&state, &pass, control)?;
    let (occurrences, next_need_ordinal) = pass.into_requests();
    if occurrences.is_empty() {
        let analyzed = analyzed.ok_or_else(|| {
            SqlCompileError::InvalidRequest("Catalog analysis did not publish a plan".into())
        })?;
        return statistics_or_optimize_step(state.common, analyzed, next_need_ordinal, control);
    }
    let needs = TableSequencePass::needs(&occurrences);
    Ok(CompilerStep::need(
        SqlNeedBatch::TableSequenceArguments(needs),
        CompilerContinuation::TableSequence(Box::new(SqlTableSequenceCompletionState {
            catalog: state,
            occurrences,
            next_need_ordinal,
        })),
    ))
}

pub(super) fn resume_table_sequence(
    state: SqlTableSequenceCompletionState,
    facts: Box<[TableSequenceArgumentFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    let pass = TableSequencePass::resume(state.occurrences, facts, control)?;
    let analyzed =
        analyze_with_completed_catalog(&state.catalog, &pass, control)?.ok_or_else(|| {
            SqlCompileError::InvalidRequest("Table sequence analysis still needs arguments".into())
        })?;
    pass.check_consumed()?;
    statistics_or_optimize_step(
        state.catalog.common,
        analyzed,
        state.next_need_ordinal,
        control,
    )
}

fn analyze_with_completed_catalog(
    state: &SqlCatalogCompletionState,
    pass: &TableSequencePass,
    control: &SqlCompileControl,
) -> Result<Option<SqlAnalyzedQuery>, SqlCompileError> {
    let fact_catalog = state.catalog.completed_catalog()?;
    analyze_with_catalog(
        &state.common,
        state.catalog.query().clone(),
        &fact_catalog,
        state.mv_definitions.as_ref(),
        Some(pass),
        control,
    )
}

fn analyze_with_catalog(
    common: &FinalPlanCommon,
    query: novarocks_parser::ast::Query,
    catalog: &dyn crate::catalog::PlannerTableProvider,
    mv_definitions: Option<&MvRewriteDefinitionIndex>,
    table_sequence: Option<&TableSequencePass>,
    control: &SqlCompileControl,
) -> Result<Option<SqlAnalyzedQuery>, SqlCompileError> {
    let catalog = SqlPlannerTableSnapshot::new(catalog);
    let query = Box::new(query);
    let statement = match &common.materialized_source {
        Some(source) => {
            // Resolve a fresh loan only over the actual query entering this
            // analysis round, under the original request control. No pointer
            // map or inferred literal is carried through AST cloning.
            source.loan(&query, control)?;
            SqlStatementInput {
                kind: super::SqlStatementInputKind::MaterializedValuesQuery {
                    query,
                    source: source.clone(),
                },
            }
        }
        None if common.root_session_admitted => {
            SqlStatementInput::query_with_admitted_session(query)
        }
        None => SqlStatementInput::parsed_query(query),
    };
    let mut request = SqlAnalyzeRequest::new(
        statement,
        common.intent.clone(),
        common.session.clone(),
        common.environment,
        &catalog,
        common.functions.as_ref(),
        Arc::clone(&common.sealed_functions),
        mv_definitions,
        common.constant_policy,
        control.clone(),
    );
    request.table_sequence = table_sequence;
    match SqlCompiler::analyze_with_table_sequence(request)? {
        None => Ok(None),
        Some(SqlAnalyzeOutput::Pending(analyzed)) => Ok(Some(analyzed)),
        Some(SqlAnalyzeOutput::Complete(_)) => Err(SqlCompileError::InvalidRequest(
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
    provider_or_ready_step(common, optimized, next_need_ordinal, control)
}

/// The optimizer result and the immutable base-table facts it consumed travel
/// together until final-plan completion. Provider negotiation cannot replace them.
struct OptimizedPhysicalPlan {
    functions: Arc<dyn SqlFunctionCatalog>,
    root_allow_throw_exception: bool,
    physical: PhysicalPlanNode,
    root_semantics: super::root_output::RootOutputSemantics,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
}

fn optimize_to_physical(
    analyzed: SqlAnalyzedQuery,
    statistics_snapshot: &DmlStatisticsSnapshot,
    control: &SqlCompileControl,
) -> Result<OptimizedPhysicalPlan, SqlCompileError> {
    let SqlAnalyzedQuery {
        root_semantics,
        logical_plan,
        factory,
        intent,
        settings,
        semantic_settings: _,
        decimal_overflow_policy,
        root_allow_throw_exception,
        change_stream: _,
        mv_rewrite,
        function_catalog,

        sealed_functions,
        constant_policy,
    } = analyzed;
    control.check()?;
    let mut scalar_arena =
        crate::optimizer::scalar::ScalarArena::with_constant_policy(constant_policy);
    let mut optimizer_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
        control,
    )?;
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
        constant_policy: mv_constant_policy,
    } = mv_rewrite;
    if mv_constant_policy != constant_policy {
        return Err(SqlCompileError::InvalidRequest(
            "MV constant policy differs from its analyzed request".into(),
        ));
    }
    let root_distribution = match &intent {
        SqlCompileIntent::IcebergWrite { root_distribution } => {
            super::resolve_root_distribution_requirement(&logical_plan, root_distribution)?
        }
        _ => None,
    };
    let environment = crate::optimizer::OptimizerEnvironment::new(
        &settings,
        Arc::clone(&function_catalog),
        Arc::clone(&sealed_functions),
        decimal_overflow_policy,
        root_allow_throw_exception,
        control,
    )
    .with_fold_dependency_observer(control.fold_dependency_observer().cloned());
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
    }?;
    control.check()?;
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized)
        .map_err(SqlCompileError::Compilation)?;
    root_semantics
        .domains(&physical.output_columns)
        .map_err(SqlCompileError::Compilation)?;
    Ok(OptimizedPhysicalPlan {
        functions: function_catalog,
        root_allow_throw_exception,
        root_semantics,
        physical,
        query_statistics: statistics.snapshot,
    })
}

fn provider_or_ready_step(
    mut common: FinalPlanCommon,
    optimized: OptimizedPhysicalPlan,
    next_need_ordinal: u32,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    let OptimizedPhysicalPlan {
        functions,
        root_allow_throw_exception,
        mut physical,
        root_semantics,
        query_statistics,
    } = optimized;
    // Retain the exact catalogue snapshot that authored optimizer bindings,
    // rather than the earlier request snapshot whose outer owner may differ.
    common.functions = functions;
    crate::planner::physical::runtime_filter_placement::place_runtime_filters_for_compiled(
        &mut physical,
        &common.session.optimizer_settings,
    );
    let offer_predicates = common
        .session
        .optimizer_settings
        .connector_static_predicate_pushdown_enabled();
    let (physical, needs, system_needs) = collect_provider_inputs(
        physical,
        next_need_ordinal,
        offer_predicates,
        None,
        !matches!(
            common.display_intent,
            SqlDisplayIntent::Explain { analyze: false, .. }
        ),
        control,
    )?;
    if !needs.is_empty() || !system_needs.is_empty() {
        return Ok(CompilerStep::need(
            if system_needs.is_empty() {
                SqlNeedBatch::ProviderReads(needs.clone())
            } else {
                SqlNeedBatch::ProviderInputs {
                    reads: needs.clone(),
                    system_tables: system_needs.clone(),
                }
            },
            CompilerContinuation::provider_read(SqlProviderReadCompletionState {
                root_allow_throw_exception,
                common,
                physical,
                root_semantics,
                query_statistics,
                needs,
                system_needs,
            }),
        ));
    }
    let mut draft =
        crate::planner::distributed::build::lower_final_physical_plan_with_root_semantics(
            &physical,
            common.version,
            common.dop_domain,
            None,
            root_semantics,
            common.functions,
            Arc::clone(&common.sealed_functions),
            root_allow_throw_exception,
            common.constant_policy,
            control,
        )
        .map_err(SqlCompileError::from)?;
    query_statistics.annotate_final_plan(&mut draft);
    Ok(CompilerStep::ready(
        common.version,
        draft,
        common.display_intent,
        [],
    ))
}

type CollectedProviderInputs = (
    PhysicalPlanNode,
    Box<[ProviderReadNeed]>,
    Box<[super::completion::SystemTableSnapshotNeed]>,
);

/// State every provider read one physical plan performs, and address each of
/// its scans by the occurrence that read will be accounted for under.
///
/// A statement reaches this through the completion protocol. A write reaches
/// it directly, because a write is compiled by the owner that sealed its
/// target rather than driven need-by-need -- but it states the same needs, so
/// it states them the same way.
pub(crate) fn collect_provider_inputs(
    plan: PhysicalPlanNode,
    next_need_ordinal: u32,
    offer_predicates: bool,
    row_mutation_match: Option<crate::binding::SqlTableBindingId>,
    materialize_system_tables: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<CollectedProviderInputs, SqlCompileError> {
    let (physical, reads, mut next_need) = collect_provider_needs_with_next(
        plan,
        next_need_ordinal,
        offer_predicates,
        row_mutation_match,
        control,
    )?;
    let (physical, system_tables) = super::system_table_completion::collect(
        physical,
        &mut next_need,
        materialize_system_tables,
        control,
    )?;
    Ok((physical, reads, system_tables))
}

pub(crate) fn collect_provider_needs(
    plan: PhysicalPlanNode,
    next_need_ordinal: u32,
    offer_predicates: bool,
    row_mutation_match: Option<crate::binding::SqlTableBindingId>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(PhysicalPlanNode, Box<[ProviderReadNeed]>), SqlCompileError> {
    let (plan, needs, _) = collect_provider_needs_with_next(
        plan,
        next_need_ordinal,
        offer_predicates,
        row_mutation_match,
        control,
    )?;
    Ok((plan, needs))
}

fn collect_provider_needs_with_next(
    plan: PhysicalPlanNode,
    mut next_need_ordinal: u32,
    offer_predicates: bool,
    row_mutation_match: Option<crate::binding::SqlTableBindingId>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(PhysicalPlanNode, Box<[ProviderReadNeed]>, u32), SqlCompileError> {
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
        row_mutation_match: Option<crate::binding::SqlTableBindingId>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<PhysicalPlanNode, SqlCompileError> {
        work.step()?;
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
                    row_mutation_match,
                    work,
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
                let (columns, predicate_columns) = provider_columns(&output_columns, &scan, work)?;
                let predicates = if offer_predicates {
                    lower_provider_predicates(
                        &scan,
                        &predicate_columns,
                        row_mutation_match == Some(source.binding),
                        work,
                    )?
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

    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::ProviderValidation,
    )?;
    let mut needs = Vec::new();
    let mut occurrence_allocator = ProviderReadOccurrenceAllocator::default();
    let plan = walk(
        plan,
        &mut next_need_ordinal,
        &mut occurrence_allocator,
        &mut needs,
        offer_predicates,
        row_mutation_match,
        &mut work,
    )?;
    work.finish()?;
    Ok((plan, needs.into_boxed_slice(), next_need_ordinal))
}

enum ProviderColumnProjectionError {
    Source(novarocks_types::ColumnValueTypeError),
    Control(novarocks_type_contract::CompileControlError),
}
impl From<novarocks_types::ColumnValueTypeError> for ProviderColumnProjectionError {
    fn from(error: novarocks_types::ColumnValueTypeError) -> Self {
        Self::Source(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for ProviderColumnProjectionError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(error.into())
    }
}
impl ProviderColumnProjectionError {
    fn into_compile_error(self) -> SqlCompileError {
        match self {
            Self::Control(error) => SqlCompileError::from(error),
            Self::Source(error) => SqlCompileError::Compilation(error.to_string()),
        }
    }
}

type ProviderColumnProjection = (
    Box<[ProviderReadColumnNeed]>,
    BTreeMap<crate::column_id::ColumnId, ProviderPredicateColumn>,
);

fn preflight_provider_value_type(
    value_type: &novarocks_type_contract::FunctionValueType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), SqlCompileError> {
    use novarocks_functions::KernelFailure;
    novarocks_functions::validate_function_value_type_observed(value_type, work).map_err(|error| {
        match error {
            KernelFailure::Cancelled => SqlCompileError::Cancelled,
            KernelFailure::DeadlineExceeded => SqlCompileError::DeadlineExceeded,
            KernelFailure::ResourceExhausted => SqlCompileError::ResourceExhausted,
            error => SqlCompileError::Compilation(error.to_string()),
        }
    })
}

fn provider_columns(
    output_columns: &[crate::analysis::OutputColumn],
    scan: &crate::planner::payload::PlanScanNode,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<ProviderColumnProjection, SqlCompileError> {
    let mut synthetic = BTreeSet::new();
    for column in &scan.variant_columns {
        work.step()?;
        synthetic.insert(column.synthetic_column_id());
    }
    let source_columns = scan
        .table
        .columns
        .iter()
        .chain(&scan.table.iceberg_row_lineage_metadata_columns);
    let mut source_scan_columns = BTreeMap::<_, Vec<_>>::new();
    for column in &scan.columns {
        work.step()?;
        if !synthetic.contains(&column.column_id) {
            source_scan_columns
                .entry(column.column_id)
                .or_default()
                .push(column);
        }
    }
    // A scan names the provider fields it reads, which need not be all of
    // them: a statement rewritten onto a materialized view reads the columns
    // that view was matched for. So each one is found by the name it carries
    // rather than by standing at the field's position.
    let mut source_by_name = BTreeMap::new();
    for column in source_columns {
        work.step()?;
        if source_by_name
            .insert(column.name.as_str(), column)
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
        work.step()?;
        if synthetic.contains(&output.column_id) {
            continue;
        }
        let matches = source_scan_columns.get(&output.column_id).ok_or_else(|| {
            SqlCompileError::Compilation(format!(
                "scan output column id {} has no exact source-column binding",
                output.column_id,
            ))
        })?;
        if matches.len() != 1 {
            return Err(SqlCompileError::Compilation(format!(
                "scan output column id {} repeats its source-column binding",
                output.column_id,
            )));
        }
        let logical = matches[0];
        let source = source_by_name.get(logical.name.as_str()).ok_or_else(|| {
            SqlCompileError::Compilation(format!(
                "scan source column '{}' is not a field of the provider schema",
                logical.name
            ))
        })?;
        let source_value_type = source
            .declared_value_type_observed(|| {
                work.step().map_err(ProviderColumnProjectionError::Control)
            })
            .map_err(ProviderColumnProjectionError::into_compile_error)?;
        // Validate the three complete execution types before comparing them.
        // Independent provider attributes do not establish execution identity.
        for value_type in [&source_value_type, &logical.value_type, &output.value_type] {
            preflight_provider_value_type(value_type, work)?;
        }
        if logical.name != source.name
            || !logical
                .value_type
                .exactly_equals_observed(&source_value_type, || {
                    work.step().map_err(ProviderColumnProjectionError::Control)
                })
                .map_err(ProviderColumnProjectionError::into_compile_error)?
            || output.name != source.name
            || !output
                .value_type
                .exactly_equals_observed(&source_value_type, || {
                    work.step().map_err(ProviderColumnProjectionError::Control)
                })
                .map_err(ProviderColumnProjectionError::into_compile_error)?
        {
            return Err(SqlCompileError::Compilation(format!(
                "scan output '{}' differs from its exact provider schema column",
                output.name
            )));
        }
        let connector_type =
            novarocks_connector_contract::connector_type_for_value_type(&source_value_type)
                .ok_or_else(|| {
                    SqlCompileError::Compilation(format!(
                        "scan output '{}' has no exact provider value type",
                        output.name,
                    ))
                })?;
        let ordinal = u32::try_from(columns.len()).map_err(|_| {
            SqlCompileError::Compilation("provider projection exceeds u32".to_string())
        })?;
        columns.push(
            ProviderReadColumnNeed::try_new(
                ordinal,
                source.name.clone(),
                source_value_type,
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

pub(super) fn resume_provider_inputs(
    mut state: SqlProviderReadCompletionState,
    facts: Box<[ProviderReadFact]>,
    system_facts: Box<[super::completion::SystemTableSnapshotFact]>,
    control: &SqlCompileControl,
) -> Result<CompilerStep, SqlCompileError> {
    super::system_table_completion::install(
        &mut state.physical,
        &state.system_needs,
        system_facts,
        control,
    )?;
    resume_provider_read(state, facts, control)
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
    let reads = FinalizedProviderReadSet::try_from_facts(facts.into_vec())?;
    let mut draft =
        crate::planner::distributed::build::lower_final_physical_plan_with_root_semantics(
            &state.physical,
            state.common.version,
            state.common.dop_domain,
            Some(reads),
            state.root_semantics,
            state.common.functions,
            Arc::clone(&state.common.sealed_functions),
            state.root_allow_throw_exception,
            state.common.constant_policy,
            control,
        )
        .map_err(SqlCompileError::from)?;
    state.query_statistics.annotate_final_plan(&mut draft);
    Ok(CompilerStep::ready(
        state.common.version,
        draft,
        state.common.display_intent,
        [],
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;
    use novarocks_physical_plan::ValueType;
    use novarocks_physical_plan::{
        ExactInputVersion, NodeKind, PredicateGuaranteeKind, ProviderColumnReference,
        ProviderReadReference,
    };
    use novarocks_spi::connector::read_stack::ConnectorReadBinding;
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
        SqlFactBatch, StatisticsFact, builtin_sql_function_catalog,
    };
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
    };
    use crate::planning::dml::DmlStatisticsEvidence;

    pub(super) fn request(sql: &str, intent: SqlCompileIntent) -> SqlFinalPlanCompileRequest {
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
            disabled_rules: vec!["FoldConstant".into()],
            ..SessionOptimizerSettings::default()
        };
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([17; 16]).expect("plan version"),
            SqlStatementInput::parsed_query(crate::compiler::test_query(sql)),
            intent,
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings,
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            crate::constant::test_constant_policy(),
            control,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }

    #[test]
    fn completion_optimizer_carries_the_analyzed_root_allow_override() {
        for (session_mode, sql, expected_allow) in [
            ("ERROR_IF_OVERFLOW", "SELECT 1", false),
            (
                "ERROR_IF_OVERFLOW",
                "WITH SESSION sql_mode = 'ALLOW_THROW_EXCEPTION' SELECT 1",
                true,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "WITH SESSION sql_mode = 32 SELECT 1",
                false,
            ),
        ] {
            let mut input = request(sql, SqlCompileIntent::Query);
            input.session.sql_semantics = input
                .session
                .sql_semantics
                .clone()
                .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session_mode));
            let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
            let snapshot = SqlPlannerTableSnapshot::new(&catalog);
            let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
                input.statement,
                input.intent,
                input.session,
                input.environment,
                &snapshot,
                input.functions.as_ref(),
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
                None,
                crate::constant::test_constant_policy(),
                input.control.clone(),
            ))
            .unwrap()
            .into_pending()
            .unwrap();
            assert_eq!(analyzed.root_allow_throw_exception(), expected_allow);
            let optimized =
                optimize_to_physical(analyzed, &DmlStatisticsSnapshot::empty(), &input.control)
                    .unwrap();
            assert_eq!(optimized.root_allow_throw_exception, expected_allow);
        }
    }

    fn provider_type_scan(value_type: ValueType, count: usize) -> PhysicalPlanNode {
        use crate::analysis::OutputColumn;
        use crate::binding::SqlTableBindingScopeId;
        use crate::column_id::ColumnId;
        use crate::planner::payload::PlanScanNode;
        use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanStats, PlannerConfidence};
        use std::num::{NonZeroU32, NonZeroU64};
        let columns = (0..count)
            .map(|index| OutputColumn {
                column_id: ColumnId(index as u32 + 1),
                name: format!("c{index}"),
                value_type: value_type.clone(),
                is_internal: false,
            })
            .collect::<Vec<_>>();
        let scan = PlanScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: columns
                    .iter()
                    .map(|column| {
                        novarocks_types::schema::ColumnDef::from_value_type(
                            column.name.clone(),
                            column.value_type.clone(),
                            None,
                        )
                        .unwrap()
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::Sql(SqlScanSource::new(
                    SqlTableBindingId::new(
                        SqlTableBindingScopeId::new(NonZeroU64::new(1).unwrap()),
                        NonZeroU32::new(1).unwrap(),
                    ),
                    SqlTableIdentity {
                        catalog: "iceberg".into(),
                        namespace: "db".into(),
                        table: "t".into(),
                    },
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            },
            alias: None,
            columns: columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        };
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(scan.into()),
            children: vec![],
            output_columns: columns,
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            probe_runtime_filters: vec![],
        }
    }

    #[test]
    fn provider_completion_keeps_source_uuid_and_rejects_same_carrier_root_forgery() {
        use novarocks_type_contract::ValueLogicalType;
        for logical in [ValueLogicalType::Uuid, ValueLogicalType::Physical] {
            let value_type =
                ValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                    .unwrap();
            let plan = provider_type_scan(value_type.clone(), 1);
            let (_, needs) = collect_provider_needs(
                plan.clone(),
                7,
                false,
                None,
                &SqlCompileControl::unbounded(),
            )
            .unwrap();
            assert_eq!(needs[0].columns()[0].engine_type(), &value_type);
            let mut forged = plan;
            forged.output_columns[0].value_type =
                novarocks_type_contract::FunctionValueType::try_with_logical_type(
                    forged.output_columns[0].value_type.data_type().clone(),
                    forged.output_columns[0].value_type.nullable(),
                    if logical == ValueLogicalType::Uuid {
                        ValueLogicalType::Physical
                    } else {
                        ValueLogicalType::Uuid
                    },
                )
                .unwrap();
            assert!(matches!(
                collect_provider_needs(forged, 7, false, None, &SqlCompileControl::unbounded()),
                Err(SqlCompileError::Compilation(_))
            ));
        }
    }

    #[test]
    fn provider_completion_controls_actual_work_and_never_returns_partial_needs() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Owner {
            observations: std::sync::Mutex<Vec<u32>>,
            failure: Option<CompileControlError>,
            fail_at: usize,
        }
        impl PureCompileControl for Owner {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::ProviderValidation);
                let mut observations = self.observations.lock().unwrap();
                observations.push(units);
                if observations.len() == self.fail_at
                    && let Some(error) = self.failure.clone()
                {
                    return Err(error);
                }
                Ok(())
            }
        }
        let plan = provider_type_scan(ValueType::new(DataType::Int32, false), 320);
        let owner = Owner {
            observations: Default::default(),
            failure: None,
            fail_at: usize::MAX,
        };
        let (_, needs) = collect_provider_needs(plan.clone(), 0, false, None, &owner).unwrap();
        assert_eq!(needs[0].columns().len(), 320);
        let observations = owner.observations.lock().unwrap().clone();
        assert_eq!(observations[0], 0);
        assert!(observations.iter().all(|units| *units <= 256));
        assert!(observations.iter().sum::<u32>() >= 320 * 3);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            // Entry, real interior work and final publication all preserve the
            // exact typed error even if the owner would allow another call.
            for fail_at in [1, 2, observations.len()] {
                let owner = Owner {
                    observations: Default::default(),
                    failure: Some(error.clone()),
                    fail_at,
                };
                let result = collect_provider_needs(plan.clone(), 0, false, None, &owner);
                assert!(
                    matches!(result, Err(actual) if actual == SqlCompileError::from(error.clone()))
                );
                assert_eq!(owner.observations.lock().unwrap().len(), fail_at);
            }
        }
    }

    #[test]
    fn provider_completion_controls_interior_of_one_nested_column_comparison() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Owner {
            failure: CompileControlError,
            fail_at: usize,
            observations: std::sync::Mutex<Vec<u32>>,
        }
        impl PureCompileControl for Owner {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::ProviderValidation);
                let mut observations = self.observations.lock().unwrap();
                observations.push(units);
                if observations.len() == self.fail_at {
                    Err(self.failure.clone())
                } else {
                    Ok(())
                }
            }
        }
        let value_type = ValueType::new(
            DataType::Struct(
                (0..100)
                    .map(|index| {
                        arrow::datatypes::Field::new(
                            format!("child{index}"),
                            DataType::Int32,
                            false,
                        )
                    })
                    .collect(),
            ),
            false,
        );
        let mut plan = provider_type_scan(value_type, 1);
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &mut plan.kind else {
            panic!("scan fixture");
        };
        // The actual physical source has no declaration to project. Count its
        // traversal separately so the failure below must occur in comparison.
        scan.table.columns[0].logical_type = None;
        let mut source_work = 0;
        let source_value_type = scan.table.columns[0]
            .declared_value_type_observed::<novarocks_types::ColumnValueTypeError>(|| {
                source_work += 1;
                Ok(())
            })
            .unwrap();
        let counter = Owner {
            failure: CompileControlError::Cancelled,
            fail_at: usize::MAX,
            observations: Default::default(),
        };
        let mut preflight_work = novarocks_type_contract::CompileCheckpoints::try_new(
            &counter,
            CompilePhase::ProviderValidation,
        )
        .unwrap();
        for value_type in [
            &source_value_type,
            &scan.columns[0].value_type,
            &plan.output_columns[0].value_type,
        ] {
            super::preflight_provider_value_type(value_type, &mut preflight_work).unwrap();
        }
        preflight_work.finish().unwrap();
        let preflight_units = counter.observations.lock().unwrap().iter().sum::<u32>() as usize;
        // Four outer steps precede the source projection and frozen preflight.
        // Their first following full checkpoint must be inside exact Eq.
        let fail_at = (source_work + preflight_units + 4) / 256 + 2;
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let owner = Owner {
                failure: failure.clone(),
                fail_at,
                observations: Default::default(),
            };
            assert!(
                matches!(collect_provider_needs(plan.clone(), 0, false, None, &owner),
                Err(actual) if actual == SqlCompileError::from(failure))
            );
            let observations = owner.observations.lock().unwrap();
            assert_eq!(observations.len(), fail_at);
            assert_eq!(observations[0], 0);
            assert!(observations[1..].iter().all(|units| *units == 256));
        }
        let (_, needs) =
            collect_provider_needs(plan, 0, false, None, &SqlCompileControl::unbounded()).unwrap();
        assert_eq!(needs.len(), 1);
        assert_eq!(needs[0].columns().len(), 1);
    }

    #[test]
    fn provider_completion_uses_complete_types_without_source_attribute_identity() {
        let near = ValueType::new(
            DataType::Struct(
                vec![arrow::datatypes::Field::new(
                    "nested",
                    DataType::Int32,
                    false,
                )]
                .into(),
            ),
            false,
        );
        for length in [
            1,
            novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1,
        ] {
            let source = arrow::datatypes::Field::new("nested", DataType::Int32, false)
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    "provider.attribute".into(),
                    "v".repeat(length),
                )]));
            let same = ValueType::new(DataType::Struct(vec![source.clone()].into()), false);
            assert_eq!(same, near);
            assert_eq!(source.metadata()["provider.attribute"].len(), length);
            let (_, needs) = collect_provider_needs(
                provider_type_scan(same, 1),
                0,
                false,
                None,
                &SqlCompileControl::unbounded(),
            )
            .unwrap();
            assert_eq!(needs[0].columns()[0].engine_type(), &near);
        }
        let changed = ValueType::new(
            DataType::Struct(
                vec![arrow::datatypes::Field::new(
                    "different",
                    DataType::Int32,
                    false,
                )]
                .into(),
            ),
            false,
        );
        for position in 0..3 {
            let mut plan = provider_type_scan(near.clone(), 1);
            let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &mut plan.kind else {
                panic!("scan fixture")
            };
            match position {
                0 => scan.table.columns[0].data_type = changed.data_type().clone(),
                1 => scan.columns[0].value_type = changed.clone(),
                2 => plan.output_columns[0].value_type = changed.clone(),
                _ => unreachable!(),
            }
            assert!(matches!(
                collect_provider_needs(plan, 0, false, None, &SqlCompileControl::unbounded()),
                Err(SqlCompileError::Compilation(_))
            ));
        }
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

    pub(super) fn incomplete(progress: SqlCompileProgress) -> SqlCompilation {
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

    pub(super) fn resolved_table(need: &CatalogRelationNeed) -> ResolvedAnalyzerTable {
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
                    match need.target() {
                        crate::compiler::CatalogLookupTarget::SystemTable => {
                            panic!("ordinary table fixture cannot provide a system declaration")
                        }
                        crate::compiler::CatalogLookupTarget::Table { .. } => SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                        crate::compiler::CatalogLookupTarget::IcebergMetadata { kind } => {
                            SqlScanKind::Metadata {
                                kind,
                                version: SqlTableVersionSelector::Current,
                            }
                        }
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

    pub(super) fn provider_contract(need: &ProviderReadNeed) -> ProviderReadStaticContract {
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
            metadata_kind: matches!(
                need.relation(),
                crate::compiler::ProviderReadRelationNeed::Metadata { .. }
            )
            .then(|| {
                novarocks_spi::connector::read_stack::ConnectorReadMetadataKind::try_new(
                    "$fixture-metadata",
                )
                .expect("fixture metadata kind")
            }),
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
            coverage_evidence: if matches!(
                need.relation(),
                crate::compiler::ProviderReadRelationNeed::Metadata { .. }
            ) {
                Box::from(*b"fixture-metadata-coverage")
            } else {
                Box::default()
            },
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

    pub(super) fn answer_catalog(compilation: SqlCompilation) -> SqlCompileProgress {
        let facts = catalog_facts(&compilation);
        SqlCompiler::finish(compilation, facts, &SqlCompileControl::unbounded())
            .expect("catalog round")
    }

    pub(super) fn answer_statistics(compilation: SqlCompilation) -> SqlCompileProgress {
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

    pub(super) fn answer_provider(compilation: SqlCompilation) -> SqlCompileProgress {
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
    fn explain_delta_projection_requests_exact_provider_window_without_private_action() {
        for (from, to) in [(0, 0), (7, 11)] {
            let seed = request(
                &format!("SELECT order_key FROM TABLE(__nr_ivm_delta('iceberg.db.orders', {from}, {to})) WHERE order_key > 0"),
                SqlCompileIntent::Explain { level: crate::explain::ExplainLevel::Verbose, analyze: false },
            ).try_into_completion().unwrap();
            let mut progress = SqlCompiler::start(seed, &SqlCompileControl::unbounded()).unwrap();
            loop {
                let compilation = incomplete(progress);
                progress = match compilation.needs() {
                    SqlNeedBatch::CatalogRelations(needs) => {
                        let facts = needs
                            .iter()
                            .map(|need| {
                                let mut resolved = resolved_table(need);
                                let metadata = ["_row_id", "_last_updated_sequence_number"]
                                    .map(|name| novarocks_types::schema::ColumnDef {
                                        name: name.into(),
                                        data_type: DataType::Int64,
                                        nullable: false,
                                        write_default: None,
                                        logical_type: None,
                                    })
                                    .to_vec();
                                resolved.catalog.hidden_columns = metadata.clone();
                                resolved.planner.iceberg_row_lineage_metadata_columns = metadata;
                                CatalogRelationFact::resolved(need, resolved).unwrap()
                            })
                            .collect::<Vec<_>>();
                        SqlCompiler::finish(
                            compilation,
                            SqlFactBatch::CatalogRelations(facts.into_boxed_slice()),
                            &SqlCompileControl::unbounded(),
                        )
                        .expect("catalog round must admit the ordinary delta TVF")
                    }
                    SqlNeedBatch::Statistics(_) => answer_statistics(compilation),
                    SqlNeedBatch::ProviderReads(needs) => {
                        let [need] = needs.as_ref() else {
                            panic!("one original Delta scan must request provider facts")
                        };
                        assert!(
                            matches!(need.relation(), crate::compiler::ProviderReadRelationNeed::Delta { from_snapshot_id, to_snapshot_id, .. } if (*from_snapshot_id, *to_snapshot_id) == (from, to))
                        );
                        assert_eq!(need.columns().len(), 1);
                        assert_eq!(need.columns()[0].name(), "order_key");
                        // Even EXPLAIN cannot become Ready until its exact
                        // provider window is admitted. In particular, 0..0
                        // is not an optimizer-authored empty/no-op window.
                        break;
                    }
                    other => panic!("unexpected delta projection need {other:?}"),
                };
            }
        }
    }

    #[test]
    fn metadata_count_uses_unconstrained_scan_and_plan_dop_with_global_gather() {
        use novarocks_physical_plan::{AggregatePhase, Distribution, NodeKind, Relation};
        let intent = SqlCompileIntent::Explain {
            level: crate::explain::ExplainLevel::Verbose,
            analyze: false,
        };
        let seed = request("select count(*) from \"orders$snapshots\"", intent)
            .with_catalog_qualifications(crate::compiler::iceberg_metadata_qualifications_for_test(
                "iceberg",
            ))
            .try_into_completion()
            .unwrap();
        let mut progress = SqlCompiler::start(seed, &SqlCompileControl::unbounded()).unwrap();
        let completed = loop {
            match progress {
                SqlCompileProgress::Complete(completed) => break completed,
                SqlCompileProgress::Incomplete(compilation) => {
                    progress = match compilation.needs() {
                        SqlNeedBatch::CatalogRelations(_) => answer_catalog(compilation),
                        SqlNeedBatch::Statistics(_) => answer_statistics(compilation),
                        SqlNeedBatch::ProviderReads(_) => answer_provider(compilation),
                        other => panic!("unexpected metadata count need {other:?}"),
                    };
                }
            }
        };
        let plan = completed.plan();
        let (scan_fragment, scan) = plan
            .fragments()
            .values()
            .find_map(|fragment| {
                fragment.nodes().values().find_map(|node| {
                    matches!(node.kind, NodeKind::Scan { .. }).then_some((fragment, node))
                })
            })
            .expect("metadata scan");
        let NodeKind::Scan { relation, .. } = &scan.kind else {
            unreachable!()
        };
        let Relation::Metadata(metadata) = relation.as_ref() else {
            panic!("metadata relation must remain explicit")
        };
        assert_eq!(
            metadata.read.relation.kind(),
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::SystemTable
        );
        assert!(matches!(
            metadata.provided_properties.distribution,
            Distribution::Unconstrained
        ));
        assert_eq!(scan_fragment.dop_domain().max, 8);
        assert!(scan_fragment.nodes().values().any(|node| {
            match &node.kind {
                NodeKind::Aggregate { calls, .. } => calls
                    .iter()
                    .any(|call| matches!(call.binding.phase, AggregatePhase::Partial { .. })),
                _ => false,
            }
        }));
        let gather = plan
            .edges()
            .values()
            .find(|edge| {
                edge.source.fragment == scan_fragment.id()
                    && matches!(edge.partitioning.destination, Distribution::Singleton)
            })
            .expect("metadata partial states must cross a gather boundary");
        let final_fragment = &plan.fragments()[&gather.destination.fragment];
        assert!(final_fragment.nodes().values().any(|node| {
            match &node.kind {
                NodeKind::Aggregate { calls, .. } => calls
                    .iter()
                    .any(|call| matches!(call.binding.phase, AggregatePhase::Final { .. })),
                _ => false,
            }
        }));
        let verbose = completed.render_explain_lines().unwrap().join("\n");
        assert!(verbose.contains("HASH AGGREGATE"));
        assert!(verbose.contains("EXCHANGE"));
        assert!(!verbose.contains("work-source="));
    }

    #[test]
    fn values_query_completes_without_an_observation_round() {
        let seed = request("select 1", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");

        let progress = SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
            .expect("completed values plan");

        assert!(matches!(progress, SqlCompileProgress::Complete(_)));
    }

    #[test]
    fn admitted_session_completion_uses_frozen_settings_for_expression_assignments() {
        let source = "WITH SESSION sql_mode = concat('3', '2') SELECT 1 AS a";
        let query = Box::new(novarocks_parser::parser::parse_query(source).unwrap());
        let mut admitted = request("SELECT 1 AS a", SqlCompileIntent::Query);
        admitted.statement = SqlStatementInput::query_with_admitted_session(query);
        admitted.session.sql_semantics = crate::sql_mode::SqlSemanticSettings::default()
            .with_sql_mode(crate::sql_mode::SqlMode::from_assignment("32"));
        let seed = admitted.try_into_completion().unwrap();
        let completed = SqlCompiler::start(seed, &SqlCompileControl::unbounded())
            .unwrap()
            .into_complete()
            .unwrap();
        assert!(matches!(
            completed.display_intent(),
            SqlDisplayIntent::Execute
        ));
        let result = completed.plan().result_port().expect("query result port");
        let [field] = result.fields.as_ref() else {
            panic!("expected one projected column");
        };
        assert_eq!(field.alias.as_deref().unwrap_or(field.name.as_ref()), "a");
    }

    fn complete_root_intrinsic_fixture(
        sql: &str,
        session_mode: &str,
    ) -> super::super::SqlCompletedPlan {
        let mut input = request(sql, SqlCompileIntent::Query);
        input.session.sql_semantics = input
            .session
            .sql_semantics
            .clone()
            .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session_mode));
        let mut progress = SqlCompiler::start(
            input.try_into_completion().unwrap(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        loop {
            match progress {
                SqlCompileProgress::Complete(completed) => return completed,
                SqlCompileProgress::Incomplete(compilation) => {
                    progress = match compilation.needs() {
                        SqlNeedBatch::CatalogRelations(_) => answer_catalog(compilation),
                        SqlNeedBatch::Statistics(_) => answer_statistics(compilation),
                        SqlNeedBatch::ProviderReads(_) => answer_provider(compilation),
                        SqlNeedBatch::MaterializedViews(_) => {
                            panic!("fixture disabled optional MV discovery")
                        }
                        SqlNeedBatch::ProviderInputs { .. } => {
                            panic!("root intrinsic fixture has no system tables")
                        }
                        SqlNeedBatch::TableSequenceArguments(_) => {
                            panic!("root intrinsic fixture has no table arguments")
                        }
                    };
                }
            }
        }
    }

    #[test]
    fn completed_casts_and_standard_arithmetic_do_not_consume_legacy_root_modes() {
        use novarocks_expression_contract::ExprKind;
        use novarocks_type_contract::{OperatorFunction, TrinoCastMode};

        let ordinary =
            "SELECT order_key + 1 AS a, CAST(order_key AS DECIMAL(18,6)) AS d FROM orders";
        for (mode, sql) in [
            ("32", ordinary),
            ("ALLOW_THROW_EXCEPTION", ordinary),
            ("ERROR_IF_OVERFLOW", ordinary),
            ("ALLOW_THROW_EXCEPTION,ERROR_IF_OVERFLOW", ordinary),
            (
                "ALLOW_THROW_EXCEPTION",
                "WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' SELECT order_key + 1 AS a, CAST(order_key AS DECIMAL(18,6)) AS d FROM orders",
            ),
            (
                "ERROR_IF_OVERFLOW",
                "WITH SESSION sql_mode = 'ALLOW_THROW_EXCEPTION' SELECT order_key + 1 AS a, CAST(order_key AS DECIMAL(18,6)) AS d FROM orders",
            ),
        ] {
            let completed = complete_root_intrinsic_fixture(sql, mode);
            let plan = completed.plan();
            assert!(plan.parameters().entries().is_empty());
            let mut arithmetic = 0;
            let mut casts = 0;
            for fragment in plan.fragments().values() {
                for expression in fragment
                    .nodes()
                    .values()
                    .filter_map(|node| node.expressions.as_ref())
                    .flat_map(|unit| unit.definitions())
                {
                    assert!(
                        expression
                            .kind
                            .intrinsic_parameter_references()
                            .next()
                            .is_none()
                    );
                    match &expression.kind {
                        ExprKind::FunctionCall { function, .. }
                            if OperatorFunction::from_function_id(&function.function_id)
                                == Some(OperatorFunction::Add) =>
                        {
                            arithmetic += 1;
                        }
                        ExprKind::Cast {
                            cast_mode: TrinoCastMode::Explicit,
                            ..
                        } => casts += 1,
                        _ => {}
                    }
                }
            }
            assert!(arithmetic > 0, "column arithmetic must not be folded away");
            assert!(casts > 0, "column conversion must retain its actual cast");
        }
    }

    #[test]
    fn completed_derived_cast_keeps_its_mode_without_a_legacy_parameter() {
        use novarocks_expression_contract::ExprKind;
        use novarocks_type_contract::{OperatorFunction, TrinoCastMode};

        let completed = complete_root_intrinsic_fixture(
            "SELECT x + 1 AS a FROM (SELECT CAST(order_key AS DECIMAL(18,6)) AS x FROM orders) s",
            "ALLOW_THROW_EXCEPTION",
        );
        let plan = completed.plan();
        assert!(plan.parameters().entries().is_empty());
        let mut saw_root_arithmetic = false;
        let mut saw_nested_cast = false;
        for fragment in plan.fragments().values() {
            for expression in fragment
                .nodes()
                .values()
                .filter_map(|node| node.expressions.as_ref())
                .flat_map(|unit| unit.definitions())
            {
                assert!(
                    expression
                        .kind
                        .intrinsic_parameter_references()
                        .next()
                        .is_none()
                );
                match &expression.kind {
                    ExprKind::FunctionCall { function, .. }
                        if OperatorFunction::from_function_id(&function.function_id)
                            == Some(OperatorFunction::Add) =>
                    {
                        saw_root_arithmetic = true;
                    }
                    ExprKind::Cast {
                        cast_mode: TrinoCastMode::Explicit,
                        ..
                    } => {
                        saw_nested_cast = true;
                    }
                    _ => {}
                }
            }
        }
        assert!(saw_root_arithmetic);
        assert!(saw_nested_cast);
    }

    #[test]
    fn completed_plans_without_intrinsic_consumers_do_not_invent_an_allow_parameter() {
        for mode in ["32", "ALLOW_THROW_EXCEPTION"] {
            for sql in ["SELECT 1", "SELECT order_key FROM orders"] {
                let completed = complete_root_intrinsic_fixture(sql, mode);
                let plan = completed.plan();
                assert!(plan.parameters().entries().is_empty());
                assert!(plan.fragments().values().all(|fragment| {
                    fragment
                        .nodes()
                        .values()
                        .filter_map(|node| node.expressions.as_ref())
                        .flat_map(|unit| unit.definitions())
                        .all(|expression| {
                            expression
                                .kind
                                .intrinsic_parameter_references()
                                .next()
                                .is_none()
                        })
                }));
            }
        }
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
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("completed explain plan")
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

        let progress = SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
            .expect("catalog need");

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
        let _ = SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
            .expect("the pure completion state remains usable");
    }

    #[test]
    fn catalog_resume_observes_invocation_cancellation() {
        let seed = request("select order_key from orders", SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed");
        let compilation = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("catalog need"),
        );
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
        let compilation = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("catalog need"),
        );
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
                SqlNeedBatch::ProviderInputs { .. } => {
                    panic!("base query fixture has no system tables")
                }
                SqlNeedBatch::TableSequenceArguments(_) => {
                    panic!("base query fixture has no table arguments")
                }
            };
        }
    }

    #[test]
    fn unsupported_caller_never_discovers_optional_mv_definitions() {
        use crate::sql_mode::SqlMode;
        for (sql, connection_mode) in [
            ("SELECT order_key FROM orders", "ERROR_IF_OVERFLOW"),
            (
                "WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' SELECT order_key FROM orders",
                "32",
            ),
            (
                "SELECT order_key FROM orders UNION ALL SELECT order_key FROM orders",
                "ERROR_IF_OVERFLOW",
            ),
            (
                "SELECT order_key FROM (SELECT order_key FROM orders) d",
                "ERROR_IF_OVERFLOW",
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
                SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                    .expect("start base query"),
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
    fn modern_root_preserves_mv_discovery_across_query_shapes() {
        use crate::sql_mode::SqlMode;
        for (sql, connection_mode) in [
            (
                "WITH SESSION sql_mode = 32 SELECT order_key FROM orders",
                "ERROR_IF_OVERFLOW",
            ),
            (
                "SELECT order_key FROM orders UNION ALL SELECT order_key FROM orders",
                "32",
            ),
            (
                "SELECT order_key FROM (SELECT order_key FROM orders) d",
                "32",
            ),
            (
                "WITH d AS (SELECT order_key FROM orders) SELECT order_key FROM d",
                "32",
            ),
        ] {
            let mut request = request_with_mv(sql, SqlCompileIntent::Query, true);
            request.session.sql_semantics = request
                .session
                .sql_semantics
                .clone()
                .with_sql_mode(SqlMode::from_assignment(connection_mode));
            let seed = request.try_into_completion().unwrap();
            let compilation = incomplete(
                SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            );
            assert!(matches!(
                compilation.needs(),
                SqlNeedBatch::MaterializedViews(_)
            ));
        }
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
        let mv = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
        );
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
        let query = novarocks_parser::parser::parse_query(
            "WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' SELECT order_key FROM orders",
        )
        .unwrap();
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
        let mv = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("MV need"),
        );
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
        let catalog = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("catalog need"),
        );
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
        let catalog = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("catalog need"),
        );
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
        let catalog = incomplete(
            SqlCompiler::start(seed, &crate::compiler::SqlCompileControl::unbounded())
                .expect("catalog need"),
        );
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

        assert!(matches!(
            FinalizedProviderReadSet::try_from_facts([
                original_fact,
                conflicting_fact,
            ]),
            Err(SqlCompileError::Compilation(message))
                if message.contains("repeat scan occurrence")
        ));
    }

    pub(super) fn available_statistics_evidence(rows: u64) -> DmlStatisticsEvidence {
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

    pub(super) fn answer_exact_statistics(
        compilation: SqlCompilation,
        rows: u64,
    ) -> SqlCompileProgress {
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

    pub(super) fn complete_with_exact_statistics(
        sql: &str,
        intent: SqlCompileIntent,
        rows: u64,
    ) -> crate::compiler::SqlCompletedPlan {
        let catalog = incomplete(
            SqlCompiler::start(
                request(sql, intent)
                    .try_into_completion()
                    .expect("completion seed"),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("catalog need"),
        );
        let statistics = incomplete(answer_catalog(catalog));
        let provider = incomplete(answer_exact_statistics(statistics, rows));
        answer_provider(provider)
            .into_complete()
            .expect("completed plan")
    }

    // SQL owner regression for a real DISTINCT/Rows TopN compilation refusal.
    // The oracle is the existing complete-group/sequence contract, rather than
    // an optimizer node count. Inputs are the original nullable Int8/Int16
    // columns and DOP 2; row count 30_004 follows the original fixture inserts,
    // while uncollected column metrics remain explicitly Missing. Provider
    // properties are unconstrained. This pure component owns no external
    // resources, needs no cleanup, and runs in the short SQL owner test lane
    // whenever DISTINCT/TopN planning changes. The offset query below is a
    // derived two-column boundary, not the original ten-column query 114.
    fn distinct_rows_topn_sequence_fixture(
        sql: &str,
    ) -> Result<SqlCompileProgress, super::super::SqlCompileProgressError> {
        let columns = vec![
            novarocks_types::schema::ColumnDef {
                name: "c1".into(),
                data_type: DataType::Int8,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            novarocks_types::schema::ColumnDef {
                name: "c2".into(),
                data_type: DataType::Int16,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        let mut input = request(sql, SqlCompileIntent::Query);
        input.dop_domain = PipelineDopDomain {
            min: 2,
            max: 2,
            requires_power_of_two: true,
        };
        let control = SqlCompileControl::unbounded();
        let catalog = incomplete(SqlCompiler::start(
            input.try_into_completion().unwrap(),
            &control,
        )?);
        let SqlNeedBatch::CatalogRelations(needs) = catalog.needs() else {
            panic!("expected catalog needs");
        };
        let facts = needs
            .iter()
            .map(|need| {
                let relation = need.relation();
                assert_eq!(relation.table, "all_numbers_t0");
                let mut table = resolved_table(need).planner;
                table.columns = columns.clone();
                CatalogRelationFact::resolved(
                    need,
                    ResolvedAnalyzerTable::from_planner(
                        Some(&relation.catalog),
                        &relation.namespace,
                        table,
                    ),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let statistics = incomplete(SqlCompiler::finish(
            catalog,
            SqlFactBatch::CatalogRelations(facts.into_boxed_slice()),
            &control,
        )?);
        let mut frozen = available_statistics_evidence(30_004);
        let DmlStatisticsEvidence::Available {
            label,
            columns: statistics_columns,
            ..
        } = &mut frozen
        else {
            unreachable!();
        };
        *label = "iceberg.db.all_numbers_t0".into();
        *statistics_columns = columns;
        let provider = incomplete(answer_frozen_statistics(statistics, frozen));
        let SqlNeedBatch::ProviderReads(needs) = provider.needs() else {
            panic!("expected provider needs");
        };
        let facts = needs
            .iter()
            .map(|need| ProviderReadFact::negotiated(need, provider_contract(need)).unwrap())
            .collect::<Vec<_>>();
        SqlCompiler::finish(
            provider,
            SqlFactBatch::ProviderReads(facts.into_boxed_slice()),
            &control,
        )
    }

    // SQL owner regression for complete-key state TopN authorship.
    // The original all_t0 declares all 24 columns; its three key columns are
    // nullable Int8/Int16/Int32 and the three inserts declare 30_002 rows.
    // Uncollected metrics remain Missing. This pure completion fixture owns
    // no external resource and needs no cleanup. Run in the short SQL owner
    // lane when grouped TopN authorship changes. The inner query is a separate
    // compilation assertion, not evidence that the outer MD5 query executed.
    // The oracle checks new Intermediate state identities, unchanged complete
    // keys/bindings, and the same captured producer request; old Rows fixtures
    // cannot exercise this state lifecycle. Native owns actual result/exit proof.
    fn grouped_state_topn_sequence_fixture(
        sql: &str,
    ) -> Result<SqlCompileProgress, super::super::SqlCompileProgressError> {
        grouped_state_topn_sequence_fixture_with_disabled(sql, false)
    }

    fn grouped_state_topn_sequence_fixture_with_disabled(
        sql: &str,
        disable_preagg: bool,
    ) -> Result<SqlCompileProgress, super::super::SqlCompileProgressError> {
        grouped_state_topn_sequence_fixture_with_options(sql, disable_preagg, &[])
    }

    fn grouped_state_topn_sequence_fixture_with_observed_metrics(
        sql: &str,
        additional_metrics: &[(&str, u64, f64, f64)],
    ) -> Result<SqlCompileProgress, super::super::SqlCompileProgressError> {
        grouped_state_topn_sequence_fixture_with_options(sql, false, additional_metrics)
    }

    fn grouped_state_topn_sequence_fixture_with_options(
        sql: &str,
        disable_preagg: bool,
        additional_metrics: &[(&str, u64, f64, f64)],
    ) -> Result<SqlCompileProgress, super::super::SqlCompileProgressError> {
        use arrow::datatypes::TimeUnit;
        use novarocks_types::schema::{ColumnDef, SqlType};

        let declarations = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Float32,
            DataType::Float64,
        ];
        let columns = declarations
            .iter()
            .cycle()
            .take(24)
            .enumerate()
            .map(|(ordinal, ty)| ColumnDef {
                name: format!("c{}", ordinal + 1),
                data_type: ty.clone(),
                nullable: ordinal < 12,
                write_default: None,
                logical_type: matches!(ordinal, 4 | 16).then_some(SqlType::LargeInt),
            })
            .collect::<Vec<_>>();
        let mut input = request(sql, SqlCompileIntent::Query);
        if disable_preagg {
            input
                .session
                .optimizer_settings
                .disabled_rules
                .push("PushDownTopNToPreAgg".into());
        }
        input.dop_domain = PipelineDopDomain {
            min: 2,
            max: 2,
            requires_power_of_two: true,
        };
        let control = SqlCompileControl::unbounded();
        let catalog = incomplete(SqlCompiler::start(
            input.try_into_completion().unwrap(),
            &control,
        )?);
        let SqlNeedBatch::CatalogRelations(needs) = catalog.needs() else {
            panic!("expected catalog needs");
        };
        let facts = needs
            .iter()
            .map(|need| {
                let relation = need.relation();
                assert_eq!(relation.table, "all_t0");
                let mut table = resolved_table(need).planner;
                table.columns = columns.clone();
                CatalogRelationFact::resolved(
                    need,
                    ResolvedAnalyzerTable::from_planner(
                        Some(&relation.catalog),
                        &relation.namespace,
                        table,
                    ),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let statistics = incomplete(SqlCompiler::finish(
            catalog,
            SqlFactBatch::CatalogRelations(facts.into_boxed_slice()),
            &control,
        )?);
        let SqlNeedBatch::Statistics(needs) = statistics.needs() else {
            panic!("expected statistics needs");
        };
        assert_eq!(needs.len(), 1);
        let mut frozen = available_statistics_evidence(30_002);
        let DmlStatisticsEvidence::Available {
            binding,
            label,
            columns: statistics_columns,
            evidence,
            ..
        } = &mut frozen
        else {
            unreachable!();
        };
        *binding = needs[0].binding();
        *label = "iceberg.db.all_t0".into();
        *statistics_columns = columns;
        // Historical evidence-model key metrics captured after three INSERTs,
        // before TRC-5d2 retired their SQL projection. These are explicit inputs.
        // Missing bounds and other omitted metrics stay explicitly Missing.
        use novarocks_spi::connector::{
            StatisticsBasisRelation, StatisticsEvidence, StatisticsMetricObservation,
            StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
            StatisticsNumericNature,
        };
        let mut metrics = evidence.metrics().clone();
        for (column, nulls, average_size, ndv) in [
            ("c1", 10_801, 0.7430171321911873, 129.0),
            ("c2", 1, 1.230151323245117, 201.0),
            ("c3", 1, 1.230151323245117, 201.0),
        ]
        .into_iter()
        .chain(additional_metrics.iter().copied())
        {
            for (metric, value, source, nature) in [
                (
                    StatisticsMetric::NullCount {
                        column: column.into(),
                    },
                    StatisticsMetricValue::U64(nulls),
                    StatisticsMetricSource::CurrentManifest,
                    StatisticsNumericNature::Exact,
                ),
                (
                    StatisticsMetric::AverageSize {
                        column: column.into(),
                    },
                    StatisticsMetricValue::F64(average_size),
                    StatisticsMetricSource::CurrentManifest,
                    StatisticsNumericNature::TwoSidedApproximate,
                ),
                (
                    StatisticsMetric::ThetaNdv {
                        column: column.into(),
                    },
                    StatisticsMetricValue::F64(ndv),
                    StatisticsMetricSource::ProviderArtifact,
                    StatisticsNumericNature::TwoSidedApproximate,
                ),
            ] {
                metrics.insert(
                    metric,
                    StatisticsMetricState::Available(StatisticsMetricObservation::new(
                        value,
                        evidence.data_version().clone(),
                        source,
                        nature,
                        StatisticsBasisRelation::Identical,
                    )),
                );
            }
        }
        *evidence = StatisticsEvidence::try_new(
            evidence.data_version().clone(),
            evidence.evidence_revision().clone(),
            evidence.row_coverage(),
            metrics,
        )
        .unwrap();
        let provider = incomplete(answer_frozen_statistics(statistics, frozen));
        let SqlNeedBatch::ProviderReads(needs) = provider.needs() else {
            panic!("expected provider needs");
        };
        let facts = needs
            .iter()
            .map(|need| ProviderReadFact::negotiated(need, provider_contract(need)).unwrap())
            .collect::<Vec<_>>();
        SqlCompiler::finish(
            provider,
            SqlFactBatch::ProviderReads(facts.into_boxed_slice()),
            &control,
        )
    }

    #[test]
    fn ordered_aggregate_topn_compile_preserves_exact_endpoints() {
        let suite =
            include_str!("../../../../tests/sql/correctness/aggregate/sql/compressed_key.sql");
        let original = suite
            .split("-- query 140\n")
            .nth(1)
            .expect("original query 140 is present")
            .split("-- query 141\n")
            .next()
            .expect("original query 140 body is present")
            .trim()
            .strip_prefix("USE ${case_db};")
            .expect("original query 140 selects its case namespace")
            .trim()
            .strip_suffix(';')
            .expect("the source contains one terminated query");
        // SQL owns this component regression; no external resource is used.
        // Historical source statistics select a real preaggregation path, not
        // a forced physical winner. Verify the actual filter and endpoints,
        // since a successful compilation with no filter misses the regression.
        let observed_c9 = [("c9", 2, 10.140857276181588, 30_002.0)];
        type LaneObservation = (&'static str, u64, f64, f64);
        type StatisticsLane<'a> = (&'static str, &'a [LaneObservation]);
        let lanes: [StatisticsLane<'_>; 2] = [("Missing", &[]), ("HistoricalC9", &observed_c9)];
        for (statistics, metrics) in lanes {
            {
                for (label, sql) in [
                    (
                        "inner-140",
                        "SELECT c9, COUNT(*) AS cnt FROM all_t0 GROUP BY c9 ORDER BY c9 LIMIT 100",
                    ),
                    ("original-140", original),
                ] {
                    let progress =
                        grouped_state_topn_sequence_fixture_with_observed_metrics(sql, metrics)
                            .unwrap_or_else(|error| {
                                panic!("{statistics} compiled {label}: {error:#?}")
                            });
                    let SqlCompileProgress::Complete(completed) = progress else {
                        panic!("unresolved completion needs for {statistics} compiled {label}");
                    };
                    let owner = completed.into_plan();
                    let plan = owner.plan();
                    if metrics.is_empty() {
                        assert!(plan.runtime_filters().is_empty(), "compiled {label}");
                        continue;
                    }
                    use novarocks_physical_plan::{
                        AggregateGrouping, NodeKind, RuntimeFilterConsumerTarget,
                        RuntimeFilterDomain, RuntimeFilterProducerTarget, TopNReduction,
                    };
                    assert_eq!(plan.runtime_filters().len(), 1, "compiled {label}");
                    let filter = plan.runtime_filters().values().next().unwrap();
                    let RuntimeFilterDomain::Ordered { key, inclusive, .. } = &filter.domain else {
                        panic!("expected the original ordered domain");
                    };
                    assert!(*inclusive);
                    assert_eq!(key.ty.data_type(), &DataType::Utf8);
                    assert!(key.ty.nullable());
                    assert_eq!(filter.producers.len(), 1);
                    assert_eq!(filter.consumers.len(), 1);
                    let producer = &filter.producers[0];
                    let fragment = &plan.fragments()[&producer.endpoint.fragment];
                    let aggregate = &fragment.nodes()[&producer.endpoint.node];
                    let NodeKind::Aggregate {
                        grouping,
                        group_by,
                        calls,
                    } = &aggregate.kind
                    else {
                        panic!("producer must remain on its original aggregate");
                    };
                    assert!(!calls.is_empty());
                    assert_eq!(group_by.len(), 1);
                    assert_eq!(
                        fragment
                            .root_expression(aggregate.id, group_by[0].0)
                            .unwrap()
                            .ty,
                        key.ty
                    );
                    let RuntimeFilterProducerTarget::AggregateTopNKey {
                        topn,
                        limit,
                        offset,
                        ..
                    } = producer.target
                    else {
                        panic!("expected the original Aggregate TopN target");
                    };
                    assert_eq!(limit, 100);
                    assert_eq!(offset, 0);
                    let parent = &fragment.nodes()[&topn];
                    assert_eq!(parent.inputs.as_ref(), &[aggregate.id]);
                    let NodeKind::TopN { reduction, .. } = &parent.kind else {
                        panic!("expected the exact parent TopN");
                    };
                    let consumer = &filter.consumers[0];
                    assert!(matches!(
                        consumer.target,
                        RuntimeFilterConsumerTarget::AggregateTopNScanField { .. }
                    ));
                    let scan_fragment = &plan.fragments()[&consumer.endpoint.fragment];
                    let scan = &scan_fragment.nodes()[&consumer.endpoint.node];
                    assert!(matches!(scan.kind, NodeKind::Scan { .. }));
                    {
                        assert_eq!(*grouping, AggregateGrouping::Partial);
                        assert!(matches!(reduction, TopNReduction::GroupedStates { .. }));
                        assert_eq!(consumer.endpoint.fragment, producer.endpoint.fragment);
                        assert_eq!(aggregate.inputs.as_ref(), &[scan.id]);
                        assert_eq!(producer.endpoint.values, consumer.endpoint.values);
                    }
                }
            }
        }
    }

    fn assert_grouped_state_topn_sequence() {
        let queries = [
            (
                "inner",
                "SELECT c1,c2,c3,COUNT(*) AS cnt FROM all_t0 GROUP BY c1,c2,c3 ORDER BY c1,c2,c3 LIMIT 100",
            ),
            (
                "original-137",
                "WITH result AS (SELECT c1,c2,c3,COUNT(*) AS cnt FROM all_t0 GROUP BY c1,c2,c3 ORDER BY c1,c2,c3 LIMIT 100) SELECT 'Test Case 39' AS test_name, MD5(LISTAGG(CAST(c1 AS VARCHAR), ':') WITHIN GROUP (ORDER BY c1, c2, c3)) AS result_hash FROM result",
            ),
        ];
        let mut failures = Vec::new();
        for (label, sql) in queries {
            match grouped_state_topn_sequence_fixture(sql) {
                Ok(SqlCompileProgress::Complete(completed)) => {
                    let owner = completed.into_plan();
                    {
                        assert_grouped_state_topn_sources(&owner);
                    }
                }
                Ok(SqlCompileProgress::Incomplete(_)) => {
                    failures.push(format!("{label}: unresolved completion needs"));
                }
                Err(error) => failures.push(format!("{label}: {error:?}")),
            }
        }
        assert!(failures.is_empty(), "compiled: {failures:?}");
    }

    fn assert_complete_group_topn_sources(owner: &super::super::SqlAuthoredPhysicalPlan) {
        use novarocks_physical_plan::{AggregateGrouping, NodeKind, TopNPhase, TopNReduction};

        let mut topn_count = 0;
        for fragment in owner.plan().fragments().values() {
            for node in fragment.nodes().values() {
                if let NodeKind::TopN {
                    reduction, phase, ..
                } = &node.kind
                {
                    topn_count += 1;
                    assert!(matches!(reduction, TopNReduction::Rows));
                    if matches!(phase, TopNPhase::Partial { .. }) {
                        let input = &fragment.nodes()[&node.inputs[0]];
                        assert!(
                            matches!(
                                input.kind,
                                NodeKind::Aggregate {
                                    grouping: AggregateGrouping::Complete,
                                    ..
                                }
                            ),
                            "an original row budget must read complete groups"
                        );
                    }
                }
            }
        }
        assert!(
            topn_count > 0,
            "the original completed graph must retain its TopN"
        );
    }

    fn assert_grouped_state_topn_sources(owner: &super::super::SqlAuthoredPhysicalPlan) {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        use {
            novarocks_expression_contract::ExprKind as PhysicalExprKind,
            novarocks_physical_plan::{
                AggregateGrouping, AggregatePhase, PhysicalCallSite, TopNReduction,
            },
        };
        let control = SqlCompileControl::unbounded();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        let mut reductions = 0;
        for fragment in owner.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::TopN {
                    reduction:
                        TopNReduction::GroupedStates {
                            group_by, calls, ..
                        },
                    ..
                } = &node.kind
                else {
                    continue;
                };
                reductions += 1;
                let producer = &fragment.nodes()[&node.inputs[0]];
                let NodeKind::Aggregate {
                    group_by: original_keys,
                    calls: originals,
                    grouping: AggregateGrouping::Partial,
                } = &producer.kind
                else {
                    panic!("expected actual direct partial aggregate");
                };
                assert_eq!(
                    group_by.iter().map(|(_, value)| value).collect::<Vec<_>>(),
                    original_keys
                        .iter()
                        .map(|(_, value)| value)
                        .collect::<Vec<_>>()
                );
                assert_eq!(calls.len(), originals.len());
                for (ordinal, (call, original)) in calls.iter().zip(originals).enumerate() {
                    let AggregatePhase::Intermediate { sequence } = call.binding.phase else {
                        panic!("expected intermediate state reduction");
                    };
                    assert_eq!(Some(sequence), original.binding.phase.sequence());
                    assert_ne!(call.id, original.id);
                    assert_ne!(call.output, original.output);
                    let mut binding = original.binding.clone();
                    binding.phase = call.binding.phase;
                    assert_eq!(call.binding, binding);
                    assert!(
                        matches!(fragment.root_expression(node.id, call.arguments[0]).unwrap().kind, PhysicalExprKind::Value(input) if input == original.output)
                    );
                    let ordinal = u32::try_from(ordinal).unwrap();
                    let consumer = owner
                        .checked_aggregate_source_observed(
                            fragment,
                            node,
                            PhysicalCallSite::TopNState {
                                node: node.id,
                                call: ordinal,
                            },
                            call,
                            &mut work,
                        )
                        .unwrap();
                    let source = owner
                        .checked_aggregate_source_observed(
                            fragment,
                            producer,
                            PhysicalCallSite::Aggregate {
                                node: producer.id,
                                call: ordinal,
                            },
                            original,
                            &mut work,
                        )
                        .unwrap();
                    assert!(std::ptr::eq(consumer.captured(), source.captured()));
                    let input = consumer.state_inputs_observed(&mut work).unwrap();
                    assert_eq!(input.root().node, producer.id);
                    assert_eq!(input.root().value, original.output);
                }
            }
        }
        assert!(
            reductions > 0,
            "actual completion must author a state reduction"
        );
        work.finish().unwrap();
    }

    #[test]
    fn grouped_state_topn_sequence_respects_user_disable_for_compiled() {
        {
            let completed = grouped_state_topn_sequence_fixture_with_disabled(
                "SELECT c1,c2,c3,COUNT(*) AS cnt FROM all_t0 GROUP BY c1,c2,c3 ORDER BY c1,c2,c3 LIMIT 100",true,
            ).unwrap().into_complete().expect("all facts are supplied");
            assert_complete_group_topn_sources(&completed.into_plan());
        }
    }

    #[test]
    fn grouped_state_topn_sequence_candidate() {
        assert_grouped_state_topn_sequence();
    }

    #[test]
    fn grouped_state_topn_sequence_multiple_aggregate_sources() {
        let sql = "SELECT c1,c2,c3,COUNT(*) AS cnt,SUM(c4) AS total,MIN(c4) AS lo,MAX(c4) AS hi FROM all_t0 GROUP BY c1,c2,c3 ORDER BY c1,c2,c3 LIMIT 100";
        let SqlCompileProgress::Complete(completed) =
            grouped_state_topn_sequence_fixture(sql).unwrap()
        else {
            panic!("expected complete multi-aggregate state reduction");
        };
        let owner = completed.into_plan();
        assert!(owner.plan().fragments().values().any(|fragment| {
            fragment.nodes().values().any(|node| matches!(
                &node.kind,
                NodeKind::TopN {
                    reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                    ..
                } if calls.len() == 4
            ))
        }));
        assert_grouped_state_topn_sources(&owner);
    }

    fn assert_distinct_rows_topn_sequence() {
        let mut failures = Vec::new();
        for (sql, offset) in [
            (
                "select distinct c2,c1 from all_numbers_t0 where c2 = 7 order by 1,2 limit 1",
                0,
            ),
            (
                "select distinct c2,c1 from all_numbers_t0 where c2 = 7 order by 1,2 OFFSET 30 LIMIT 1",
                30,
            ),
        ] {
            match distinct_rows_topn_sequence_fixture(sql) {
                Ok(SqlCompileProgress::Complete(completed)) => {
                    let plan = completed.plan();
                    let result = plan.result_port().expect("result port");
                    let fields = result
                        .fields
                        .iter()
                        .map(|field| (field.name.as_ref(), field.ty.clone()))
                        .collect::<Vec<_>>();
                    assert_eq!(
                        fields,
                        vec![
                            ("c2", ValueType::new(DataType::Int16, true)),
                            ("c1", ValueType::new(DataType::Int8, true)),
                        ],
                    );
                    assert!(plan.fragments().values().all(|fragment| {
                        fragment.dop_domain().min == 2 && fragment.dop_domain().max == 2
                    }));
                    let fragment = &plan.fragments()[&result.fragment];
                    let root = &fragment.nodes()[&fragment.root()];
                    if offset == 0 {
                        // Completed publication already proves the exact
                        // reduction path; retain ordinary parallel Rows split.
                        assert!(matches!(
                            root.kind,
                            NodeKind::TopN {
                                phase: novarocks_physical_plan::TopNPhase::Final { .. },
                                reduction: novarocks_physical_plan::TopNReduction::Rows,
                                limit: 1,
                                offset: 0,
                                ..
                            }
                        ));
                        assert!(plan.fragments().values().any(|fragment| {
                            fragment.nodes().values().any(|node| {
                                matches!(
                                    node.kind,
                                    NodeKind::TopN {
                                        phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                                        reduction: novarocks_physical_plan::TopNReduction::Rows,
                                        ..
                                    }
                                )
                            })
                        }));
                    } else {
                        assert!(matches!(
                            root.kind,
                            NodeKind::TopN {
                                phase: novarocks_physical_plan::TopNPhase::Single,
                                limit: 1,
                                offset: 30,
                                ..
                            }
                        ));
                    }
                }
                Ok(SqlCompileProgress::Incomplete(_)) => {
                    failures.push(format!("unexpected incomplete plan for {sql}"));
                }
                Err(error) => {
                    failures.push(error.to_string());
                }
            }
        }
        assert!(failures.is_empty(), "compiled: {failures:?}");
    }

    #[test]
    fn distinct_rows_topn_sequence_candidate() {
        assert_distinct_rows_topn_sequence();
    }

    pub(super) fn frozen_table_statistics(
        plan: &novarocks_physical_plan::PhysicalPlan,
    ) -> Vec<&str> {
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
                    &SqlCompileControl::unbounded(),
                )
                .expect("costs text");
                assert!(costs.iter().any(|line| line == &expected));
                let contract = crate::explain::completed::render_completed_plan(
                    plan,
                    &[],
                    crate::explain::ExplainLevel::Contract,
                    None,
                    crate::explain::completed::ExplainRenderBudget::default(),
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .expect("contract text");
                assert!(contract.iter().any(|line| line.contains(&expected)));
                let normal = crate::explain::completed_tree::render_completed_plan_tree(
                    plan,
                    crate::explain::ExplainLevel::Normal,
                    &SqlCompileControl::unbounded(),
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
                &crate::compiler::SqlCompileControl::unbounded(),
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
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("values")
        .into_complete()
        .expect("complete");
        assert!(frozen_table_statistics(values.plan()).is_empty());
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
            SqlCompiler::start(
                compile_request.try_into_completion().expect("seed"),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
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
                SqlCompiler::start(
                    compile_request.try_into_completion().expect("seed"),
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
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
                &SqlCompileControl::unbounded(),
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
                "WITH big_probe AS (SELECT sequential_number AS k FROM TABLE(sequence(1, 1000000))), no_stats AS (SELECT k FROM (SELECT sequential_number + 0 AS k FROM TABLE(sequence(1, 100000000))) projected WHERE k > 0) SELECT COUNT(*) AS cnt FROM big_probe p JOIN no_stats b ON p.k = b.k",
                false,
            ),
            (
                "WITH p AS (SELECT sequential_number AS k FROM TABLE(sequence(1, 1000))), b AS (SELECT sequential_number AS k FROM TABLE(sequence(1, 10))) SELECT COUNT(*) AS cnt FROM p JOIN b ON p.k = b.k",
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
            // Match the SQL fixtures' fixed probe/build roles. Otherwise join
            // commutativity may legally broadcast the smaller probe relation.
            compile_request.session.optimizer_settings.disabled_rules =
                vec!["JoinReorder".to_string(), "JoinCommutativity".to_string()];
            compile_request
                .session
                .optimizer_settings
                .effective_backend_count = Some(3.0);
            compile_request
                .session
                .optimizer_settings
                .cbo_broadcast_node_mem_budget_bytes = Some(268435456.0);
            let completed = SqlCompiler::start(
                compile_request.try_into_completion().expect("seed"),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("source-free compilation")
            .into_complete()
            .expect("no catalog/statistics/provider needs");
            assert!(frozen_table_statistics(completed.plan()).is_empty());
            let text = crate::explain::completed_tree::render_completed_plan_tree(
                completed.plan(),
                crate::explain::ExplainLevel::Costs,
                &SqlCompileControl::unbounded(),
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

    #[test]
    fn m07_scalar_root_identity_survives_owned_final_completion() {
        use novarocks_physical_plan::ResultValueDomain as D;
        use novarocks_result_contract::ScalarValueType as V;
        for (sql, domain, value) in [
            ("select json_object('k': 1) as j", D::Json, V::Json),
            (
                "select coalesce(json_object('k': 1), null) as j",
                D::Json,
                V::Json,
            ),
            (
                "select if(true, json_object('k': 1), null) as j",
                D::Json,
                V::Json,
            ),
            (
                "select case when true then json_object('k': 1) else null end as j",
                D::Json,
                V::Json,
            ),
            (
                "with q as (select json_object('k': 1) as j) select j from q",
                D::Json,
                V::Json,
            ),
            (
                "select cast(json_object('k': 1) as varchar) as j",
                D::Plain,
                V::String,
            ),
            (
                "select coalesce(cast(json_object('k': 1) as varchar), 'plain') as j",
                D::Plain,
                V::String,
            ),
            (
                "select json_object('k': 1) as j union all select null",
                D::Json,
                V::Json,
            ),
        ] {
            let progress = SqlCompiler::start(
                request(sql, SqlCompileIntent::Query)
                    .try_into_completion()
                    .unwrap(),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
            let SqlCompileProgress::Complete(completed) = progress else {
                panic!("source-free scalar unexpectedly needs observations: {sql}");
            };
            let result = completed.plan().result_port().unwrap();
            assert_eq!(result.fields[0].domain, domain, "{sql}");
            let schema = completed.scalar_schema().unwrap();
            assert_eq!(schema.field().value_type, value, "{sql}");
            assert_eq!(
                schema.field().nullable,
                result.fields[0].ty.nullable(),
                "{sql}"
            );
            assert_eq!(schema.source_slot(), None);
            let plan = completed
                .into_plan()
                .with_root_output(novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::ScalarValue(schema),
                ))
                .unwrap();
            novarocks_physical_plan::validate_plan(plan.plan()).unwrap();
        }
    }

    #[test]
    fn m07_scalar_schema_refuses_multiple_root_occurrences() {
        let SqlCompileProgress::Complete(completed) = SqlCompiler::start(
            request("select 1, 2", SqlCompileIntent::Query)
                .try_into_completion()
                .unwrap(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap() else {
            panic!("unexpected observation");
        };
        assert!(completed.scalar_schema().is_err());
    }
}

#[cfg(test)]
pub(crate) fn compile_authored_aggregate_for_test(sql: &str) -> super::SqlAuthoredPhysicalPlan {
    tests::complete_with_exact_statistics(sql, SqlCompileIntent::Query, 13).into_plan()
}

#[cfg(test)]
#[path = "owned_plan_movement_tests.rs"]
mod owned_plan_movement_tests;

#[cfg(test)]
#[path = "package_semantics_tests.rs"]
mod package_semantics_tests;

#[cfg(test)]
#[path = "catalogue_retention_tests.rs"]
mod catalogue_retention_tests;

#[cfg(test)]
#[path = "view_completion_tests.rs"]
mod view_completion_tests;

#[cfg(test)]
mod system_tables_tests;
