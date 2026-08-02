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

//! The statement-scoped SQL compiler boundary.
//!
//! This module deliberately owns only neutral compiler inputs and outputs.
//! Application admission, connector execution preparation, native encoding,
//! and query lifecycle orchestration remain outside this boundary.
// Design: ADR-0025 (docs/adr/ADR-0025-sql-compiler-explicit-input-boundary.md)

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use crate::query_execution::cancellation::QueryCancellationView;
use crate::sql::explain::ExplainLevel;
use crate::sql::optimizer::options::SessionOptimizerSettings;

/// A narrow catalog capability available to one compiler request.
///
/// Concrete connector clients and registry mutation APIs are intentionally not
/// part of this contract. The provider is a query-scoped snapshot and owns the
/// one binding store shared by analysis, statistics, and scan preparation.
pub(crate) trait SqlCatalogSnapshot {
    fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider;
}

pub(crate) struct SqlPlannerTableSnapshot<'a> {
    provider: &'a dyn crate::sql::catalog::PlannerTableProvider,
}

impl<'a> SqlPlannerTableSnapshot<'a> {
    pub(crate) fn new(provider: &'a dyn crate::sql::catalog::PlannerTableProvider) -> Self {
        Self { provider }
    }
}

impl SqlCatalogSnapshot for SqlPlannerTableSnapshot<'_> {
    fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider {
        self.provider
    }
}

/// Query statistics facts collected during one compilation.  This is a SQL
/// value: it contains no provider, control host, or mutable cache handle.
pub(crate) struct SqlStatisticsPlan {
    pub(crate) snapshot: crate::sql::optimizer::stats_input::QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl SqlStatisticsPlan {
    pub(crate) fn empty() -> Self {
        Self {
            snapshot: crate::sql::optimizer::stats_input::QueryStatsSnapshot::empty(),
            next_stats_ref: 0,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: crate::sql::optimizer::stats_input::BaseTableStatistics,
    ) -> crate::sql::optimizer::stats_input::StatsRef {
        let stats_ref = crate::sql::optimizer::stats_input::StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }

    pub(crate) fn set_next_stats_ref(&mut self, next_stats_ref: u32) {
        self.next_stats_ref = next_stats_ref;
    }
}

/// A narrow, exact statistics capability available to one request.  It reads
/// only bindings captured by the paired catalog snapshot; it cannot ask for a
/// newer connector generation.
pub(crate) trait SqlStatisticsSnapshot {
    fn collect_table_statistics(
        &self,
        database: &str,
        table: &crate::sql::planner::table::TableDef,
    ) -> (
        String,
        crate::sql::optimizer::stats_input::BaseTableStatistics,
    );
}

/// Immutable MV rewrite facts frozen by application admission.  The compiler
/// never enumerates repositories or opens catalog/provider clients itself.
pub(crate) trait SqlMvRewriteSnapshot {
    fn prepare_candidates(
        &self,
        catalog: &dyn crate::sql::catalog::PlannerTableProvider,
        current_database: &str,
        logical: &crate::sql::planner::logical::LogicalPlanNode,
        factory: &mut crate::sql::column_id::ColumnRefFactory,
        functions: &dyn SqlFunctionCatalog,
        statistics: &dyn SqlStatisticsSnapshot,
        query_stats: &mut SqlStatisticsPlan,
        optimizer_settings: &SessionOptimizerSettings,
    ) -> Vec<crate::sql::optimizer::cascades_rules::mv_rewrite::MvRewriteCandidate>;
}

/// Application-frozen input for incremental-MV rewrite.  SQL invokes this at
/// the logical boundary, but does not receive an application request context
/// or an untyped callback.  The implementation owns any refresh-specific
/// validation and returns only SQL facts.
pub(crate) trait SqlImvRewriteInput {
    fn rewrite(
        &self,
        logical_plan: crate::sql::planner::logical::LogicalPlanNode,
        factory: crate::sql::column_id::ColumnRefFactory,
        optimizer_settings: &SessionOptimizerSettings,
    ) -> Result<SqlImvRewriteOutput, String>;
}

pub(crate) struct SqlImvRewriteOutput {
    pub(crate) logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    pub(crate) factory: crate::sql::column_id::ColumnRefFactory,
    pub(crate) change_stream:
        crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
}

/// Immutable SQL function semantics used by analysis and optimization.
///
/// The function implementation and its execution kernels are explicitly out
/// of scope for this compiler-facing contract.
pub(crate) trait SqlFunctionCatalog: Send + Sync {
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<crate::sql::functions::ResolvedScalarFunction, crate::sql::functions::ResolveError>;

    fn volatility(&self, name: &str) -> crate::sql::functions::FunctionVolatility;
}

/// Statement material already owned by the SQL boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlStatementInput {
    Sql(String),
    ParsedQuery(Box<sqlparser::ast::Query>),
}

/// The compiler result shape required by the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlCompileIntent {
    Query,
    Explain {
        level: ExplainLevel,
        analyze: bool,
    },
    AnalyzeOnly,
    LogicalOnly,
    IcebergWrite {
        root_distribution: RootDistributionRequirement,
    },
    ChangeStreamWrite,
}

/// A SQL-owned replacement for write-planning callbacks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RootDistributionRequirement {
    Any,
    ShuffleOutputOrdinal(usize),
    ShuffleOutputName(String),
}

/// SQL-relevant session state frozen at statement admission.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlSessionContext {
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) optimizer_settings: SessionOptimizerSettings,
}

/// Deployment facts consumed by planning without exposing topology objects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlPlanningEnvironment {
    Distributed { backend_count: NonZeroUsize },
    NotApplicable,
}

/// Read-only request control observed by compiler phases.
#[derive(Clone)]
pub(crate) struct SqlCompileControl {
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
}

impl SqlCompileControl {
    pub(crate) fn new(deadline: Option<Instant>, cancellation: QueryCancellationView) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    pub(crate) fn check(&self) -> Result<(), SqlCompileError> {
        if self.cancellation.is_cancelled() {
            return Err(SqlCompileError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(SqlCompileError::DeadlineExceeded);
        }
        Ok(())
    }
}

/// Complete immutable input consumed by the pure SQL compiler.
pub(crate) struct SqlCompileRequest<'a> {
    pub(crate) statement: SqlStatementInput,
    pub(crate) intent: SqlCompileIntent,
    pub(crate) session: SqlSessionContext,
    pub(crate) environment: SqlPlanningEnvironment,
    pub(crate) catalog: &'a dyn SqlCatalogSnapshot,
    pub(crate) statistics: &'a dyn SqlStatisticsSnapshot,
    pub(crate) functions: &'a dyn SqlFunctionCatalog,
    pub(crate) mv_rewrite: Option<&'a dyn SqlMvRewriteSnapshot>,
    pub(crate) imv_rewrite: Option<&'a dyn SqlImvRewriteInput>,
    pub(crate) control: SqlCompileControl,
}

impl<'a> SqlCompileRequest<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        statement: SqlStatementInput,
        intent: SqlCompileIntent,
        session: SqlSessionContext,
        environment: SqlPlanningEnvironment,
        catalog: &'a dyn SqlCatalogSnapshot,
        statistics: &'a dyn SqlStatisticsSnapshot,
        functions: &'a dyn SqlFunctionCatalog,
        mv_rewrite: Option<&'a dyn SqlMvRewriteSnapshot>,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement,
            intent,
            session,
            environment,
            catalog,
            statistics,
            functions,
            mv_rewrite,
            imv_rewrite: None,
            control,
        }
    }

    pub(crate) fn check_control(&self) -> Result<(), SqlCompileError> {
        self.control.check()
    }

    pub(crate) fn with_imv_rewrite(mut self, input: &'a dyn SqlImvRewriteInput) -> Self {
        self.imv_rewrite = Some(input);
        self
    }
}

/// Application-owned facts used only after SQL has produced a plan.  Keeping
/// this separate makes it impossible for parse/analyze/optimize to reach a
/// connector registry, native encoder, or query lifecycle object.
pub(crate) struct PostCompilePlanningContext<'a> {
    pub(crate) table_bindings: Arc<crate::sql::catalog::provider::QueryTableBindingStore>,
    pub(crate) connector_controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    pub(crate) connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
}

/// One application admission's complete planning input.  The exact binding
/// store is deliberately shared by compiler catalog/statistics input and
/// post-compile scan preparation.
pub(crate) struct QueryPlanningInputs<'a> {
    pub(crate) compile_request: SqlCompileRequest<'a>,
    pub(crate) post_compile: PostCompilePlanningContext<'a>,
}

pub(crate) struct SqlAnalysisOutput {
    pub(crate) logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    pub(crate) factory: crate::sql::column_id::ColumnRefFactory,
}

pub(crate) struct SqlOptimizedOutput {
    pub(crate) optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    pub(crate) statistics: SqlStatisticsPlan,
    pub(crate) change_stream:
        crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
}

pub(crate) struct SqlDistributedOutput {
    pub(crate) distributed_plan: crate::sql::planner::distributed::DistributedPlan,
    pub(crate) statistics: SqlStatisticsPlan,
}

/// SQL-owned compiler facts. Native DTOs/bytes, lifecycle state and result
/// buffers are intentionally absent; application owns post-compile assembly.
pub(crate) enum SqlCompileOutput {
    Analysis(SqlAnalysisOutput),
    Logical(SqlAnalysisOutput),
    Optimized(SqlOptimizedOutput),
    ImmediateExplain(Vec<String>),
    Distributed(SqlDistributedOutput),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlCompileError {
    Cancelled,
    DeadlineExceeded,
    InvalidRequest(String),
    Compilation(String),
}

impl std::fmt::Display for SqlCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("SQL compilation was cancelled"),
            Self::DeadlineExceeded => f.write_str("SQL compilation deadline exceeded"),
            Self::InvalidRequest(error) | Self::Compilation(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for SqlCompileError {}

/// The canonical parse/analyze/logical/optimize/physical/distributed kernel.
/// It accepts only immutable SQL facts and checks request control at every
/// externally visible phase boundary.
pub(crate) struct SqlCompiler;

impl SqlCompiler {
    pub(crate) fn compile(
        request: SqlCompileRequest<'_>,
    ) -> Result<SqlCompileOutput, SqlCompileError> {
        request.check_control()?;
        let query = parse_query(&request.statement)?;
        let catalog = request.catalog.planner_table_provider();
        let (resolved, ctes, mut factory) = crate::sql::analyzer::analyze_with_function_catalog(
            &query,
            catalog,
            &request.session.current_database,
            request.functions,
        )
        .map_err(SqlCompileError::Compilation)?;
        request.check_control()?;
        let mut logical_plan = crate::sql::planner::plan_query(resolved, ctes, &mut factory)
            .map_err(SqlCompileError::Compilation)?;
        request.check_control()?;

        if matches!(request.intent, SqlCompileIntent::AnalyzeOnly) {
            return Ok(SqlCompileOutput::Analysis(SqlAnalysisOutput {
                logical_plan,
                factory,
            }));
        }
        if matches!(request.intent, SqlCompileIntent::LogicalOnly) {
            return Ok(SqlCompileOutput::Logical(SqlAnalysisOutput {
                logical_plan,
                factory,
            }));
        }
        let mut settings = request.session.optimizer_settings.clone();
        apply_planning_environment(&mut settings, request.environment)?;
        let mut change_stream =
            crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor::default();
        if let Some(input) = request.imv_rewrite {
            if !matches!(request.intent, SqlCompileIntent::ChangeStreamWrite) {
                return Err(SqlCompileError::InvalidRequest(
                    "incremental MV rewrite requires ChangeStreamWrite intent".to_string(),
                ));
            }
            let rewritten = input
                .rewrite(logical_plan, factory, &settings)
                .map_err(SqlCompileError::Compilation)?;
            logical_plan = rewritten.logical_plan;
            factory = rewritten.factory;
            change_stream = rewritten.change_stream;
            request.check_control()?;
        }
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let mut optimizer_expr =
            crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                &logical_plan,
                &mut scalar_arena,
            )
            .map_err(SqlCompileError::Compilation)?;
        let mut statistics = collect_statistics(request.statistics, &mut optimizer_expr);
        request.check_control()?;
        let mv_candidates = request
            .mv_rewrite
            .map(|snapshot| {
                snapshot.prepare_candidates(
                    catalog,
                    &request.session.current_database,
                    &logical_plan,
                    &mut factory,
                    request.functions,
                    request.statistics,
                    &mut statistics,
                    &settings,
                )
            })
            .unwrap_or_default();
        request.check_control()?;
        let root_distribution = match &request.intent {
            SqlCompileIntent::IcebergWrite { root_distribution } => {
                resolve_root_distribution_requirement(&logical_plan, root_distribution)?
            }
            _ => None,
        };
        let optimized_tree = match root_distribution {
            Some(root_distribution) => crate::sql::optimizer::optimize_with_root_distribution(
                optimizer_expr,
                scalar_arena,
                &statistics.snapshot,
                factory,
                root_distribution,
                &settings,
            ),
            None => crate::sql::optimizer::optimize(
                optimizer_expr,
                scalar_arena,
                &statistics.snapshot,
                factory,
                mv_candidates,
                &settings,
            ),
        }
        .map_err(SqlCompileError::Compilation)?;
        request.check_control()?;

        if let SqlCompileIntent::Explain {
            level,
            analyze: false,
        } = request.intent
        {
            let mut lines = Vec::new();
            if matches!(level, ExplainLevel::Costs) {
                lines.extend(statistics.snapshot.display_rows());
            }
            let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .map_err(SqlCompileError::Compilation)?;
            let distributed = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
                physical, &settings,
            )
            .map_err(SqlCompileError::Compilation)?;
            lines.extend(crate::sql::explain::distributed::explain_distributed_plan(
                &distributed,
                level,
            ));
            return Ok(SqlCompileOutput::ImmediateExplain(lines));
        }

        if matches!(
            request.intent,
            SqlCompileIntent::IcebergWrite { .. } | SqlCompileIntent::ChangeStreamWrite
        ) {
            return Ok(SqlCompileOutput::Optimized(SqlOptimizedOutput {
                optimized_tree,
                statistics,
                change_stream,
            }));
        }

        let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
            .map_err(SqlCompileError::Compilation)?;
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
            physical, &settings,
        )
        .map_err(SqlCompileError::Compilation)?;
        request.check_control()?;
        Ok(SqlCompileOutput::Distributed(SqlDistributedOutput {
            distributed_plan,
            statistics,
        }))
    }
}

fn parse_query(statement: &SqlStatementInput) -> Result<sqlparser::ast::Query, SqlCompileError> {
    let sql = match statement {
        SqlStatementInput::Sql(sql) => sql,
        SqlStatementInput::ParsedQuery(query) => return Ok((**query).clone()),
    };
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(SqlCompileError::Compilation)?;
    match crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| SqlCompileError::Compilation(error.to_string()))?
    {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        _ => Err(SqlCompileError::InvalidRequest(
            "SQL compiler requires a query statement after application preprocessing".to_string(),
        )),
    }
}

fn resolve_root_distribution_requirement(
    logical_plan: &crate::sql::planner::logical::LogicalPlanNode,
    requirement: &RootDistributionRequirement,
) -> Result<Option<crate::sql::optimizer::property::DistributionSpec>, SqlCompileError> {
    let output_columns = crate::sql::planner::plan_output_columns(logical_plan)
        .map_err(SqlCompileError::Compilation)?;
    let column = match requirement {
        RootDistributionRequirement::Any => return Ok(None),
        RootDistributionRequirement::ShuffleOutputOrdinal(index) => {
            output_columns.get(*index).ok_or_else(|| {
                SqlCompileError::InvalidRequest(format!(
                    "cannot derive Iceberg write root shuffle: output column index {index} out of range ({} columns)",
                    output_columns.len()
                ))
            })?
        }
        RootDistributionRequirement::ShuffleOutputName(name) => {
            let mut matches = output_columns.iter().filter(|column| column.name == *name);
            let column = matches.next().ok_or_else(|| {
                SqlCompileError::InvalidRequest(format!(
                    "cannot derive Iceberg write root shuffle: output column '{name}' not found"
                ))
            })?;
            if matches.next().is_some() {
                return Err(SqlCompileError::InvalidRequest(format!(
                    "cannot derive Iceberg write root shuffle: output column '{name}' is ambiguous"
                )));
            }
            column
        }
    };
    if column.column_id == crate::sql::column_id::ColumnId::UNSET {
        return Err(SqlCompileError::InvalidRequest(format!(
            "cannot derive Iceberg write root shuffle: output column '{}' has no ColumnId",
            column.name
        )));
    }
    Ok(Some(
        crate::sql::optimizer::property::DistributionSpec::shuffle_agg([column.column_id]),
    ))
}

fn collect_statistics(
    snapshot: &dyn SqlStatisticsSnapshot,
    expr: &mut crate::sql::optimizer::opt_expr::OptExpr,
) -> SqlStatisticsPlan {
    fn walk(
        snapshot: &dyn SqlStatisticsSnapshot,
        expr: &mut crate::sql::optimizer::opt_expr::OptExpr,
        plan: &mut SqlStatisticsPlan,
    ) {
        if let crate::sql::optimizer::operator::Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = crate::sql::optimizer::stats_input::StatsRef::new(plan.next_stats_ref);
            plan.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);
            let (label, stats) = snapshot.collect_table_statistics(&scan.database, &scan.table);
            plan.snapshot.insert(stats_ref, label, stats);
        }
        for child in &mut expr.children {
            walk(snapshot, child, plan);
        }
    }

    let mut plan = SqlStatisticsPlan::empty();
    walk(snapshot, expr, &mut plan);
    plan
}

fn apply_planning_environment(
    settings: &mut SessionOptimizerSettings,
    environment: SqlPlanningEnvironment,
) -> Result<(), SqlCompileError> {
    match environment {
        SqlPlanningEnvironment::Distributed { backend_count } => {
            settings.effective_backend_count = Some(backend_count.get() as f64);
            Ok(())
        }
        SqlPlanningEnvironment::NotApplicable => Err(SqlCompileError::InvalidRequest(
            "distributed SQL compilation requires a frozen non-zero backend count".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};

    use super::*;

    struct Catalog;
    impl SqlCatalogSnapshot for Catalog {
        fn planner_table_provider(&self) -> &dyn crate::sql::catalog::PlannerTableProvider {
            panic!("control tests must not reach catalog resolution")
        }
    }
    struct Statistics;
    impl SqlStatisticsSnapshot for Statistics {
        fn collect_table_statistics(
            &self,
            _database: &str,
            _table: &crate::sql::planner::table::TableDef,
        ) -> (
            String,
            crate::sql::optimizer::stats_input::BaseTableStatistics,
        ) {
            panic!("control tests must not reach statistics collection")
        }
    }
    struct Functions;
    impl SqlFunctionCatalog for Functions {
        fn resolve_scalar_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<
            crate::sql::functions::ResolvedScalarFunction,
            crate::sql::functions::ResolveError,
        > {
            Err(crate::sql::functions::ResolveError::UnknownFunction)
        }

        fn volatility(&self, _name: &str) -> crate::sql::functions::FunctionVolatility {
            crate::sql::functions::FunctionVolatility::Immutable
        }
    }

    static CATALOG: Catalog = Catalog;
    static STATISTICS: Statistics = Statistics;
    static FUNCTIONS: Functions = Functions;

    fn request(control: SqlCompileControl) -> SqlCompileRequest<'static> {
        SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1".to_string()),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).unwrap(),
            },
            &CATALOG,
            &STATISTICS,
            &FUNCTIONS,
            None,
            control,
        )
    }

    #[test]
    fn sqlx1_request_keeps_named_capabilities_without_application_context() {
        let cancellation = QueryCancellationSource::new();
        let request = request(SqlCompileControl::new(None, cancellation.view()));
        assert_eq!(request.check_control(), Ok(()));
        assert!(matches!(
            request.environment,
            SqlPlanningEnvironment::Distributed { backend_count } if backend_count.get() == 3
        ));
    }

    #[test]
    fn sqlx1_request_rejects_cancelled_control_before_compilation() {
        let cancellation = QueryCancellationSource::new();
        cancellation.request(QueryCancellationReason::ServerShutdown);
        assert!(matches!(
            SqlCompiler::compile(request(SqlCompileControl::new(None, cancellation.view()))),
            Err(SqlCompileError::Cancelled)
        ));
    }

    #[test]
    fn sqlx1_request_rejects_expired_deadline_before_compilation() {
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            SqlCompiler::compile(request(SqlCompileControl::new(
                Some(deadline),
                cancellation.view(),
            ))),
            Err(SqlCompileError::DeadlineExceeded)
        ));
    }

    #[test]
    fn sqlx1_request_models_metadata_planning_without_a_fake_backend() {
        let cancellation = QueryCancellationSource::new();
        let mut request = request(SqlCompileControl::new(None, cancellation.view()));
        request.environment = SqlPlanningEnvironment::NotApplicable;
        assert_eq!(request.check_control(), Ok(()));
    }

    #[test]
    fn sqlx1_kernel_compiles_a_query_without_application_state() {
        let catalog = crate::sql::catalog::local::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = QueryCancellationSource::new();
        let request = SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1".to_string()),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).expect("non-zero fixture topology"),
            },
            &catalog_snapshot,
            &STATISTICS,
            crate::sql::functions::builtin_sql_function_catalog(),
            None,
            SqlCompileControl::new(None, cancellation.view()),
        );

        assert!(matches!(
            SqlCompiler::compile(request),
            Ok(SqlCompileOutput::Distributed(_))
        ));
    }

    #[test]
    fn parsed_query_input_preserves_complex_types_and_escapes_without_sql_round_trip() {
        let statement =
            crate::sql::parser::parse_sql_raw(r"SELECT CAST('{}' AS MAP<STRING, INT>), 'e\\f'")
                .expect("query fixture must parse");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query fixture");
        };

        assert_eq!(
            parse_query(&SqlStatementInput::ParsedQuery(query.clone())),
            Ok(*query)
        );
    }

    #[test]
    fn sqlx1_kernel_write_root_requirement_is_validated_in_sql() {
        let catalog = crate::sql::catalog::local::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = QueryCancellationSource::new();
        let request = SqlCompileRequest::new(
            SqlStatementInput::Sql("select 1 as payload".to_string()),
            SqlCompileIntent::IcebergWrite {
                root_distribution: RootDistributionRequirement::ShuffleOutputName(
                    "missing".to_string(),
                ),
            },
            SqlSessionContext {
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).expect("non-zero fixture topology"),
            },
            &catalog_snapshot,
            &STATISTICS,
            crate::sql::functions::builtin_sql_function_catalog(),
            None,
            SqlCompileControl::new(None, cancellation.view()),
        );
        assert!(matches!(
            SqlCompiler::compile(request),
            Err(SqlCompileError::InvalidRequest(error)) if error.contains("output column 'missing' not found")
        ));
    }
}
pub(crate) mod mv_rewrite;
