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
// Design: ADR-0073 (docs/adr/ADR-0073-sql-compilation-freezes-statistics-after-analysis.md)
// Design: ADR-0040 (docs/adr/ADR-0040-sql-compiler-dependency-inversion.md)

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use crate::analyze_error::AnalyzeError;
pub use crate::explain::ExplainLevel;
pub use crate::functions::{
    build_builtin_engine_function_catalog, builtin_engine_function_catalog,
    builtin_sql_function_catalog, contribute_builtin_functions,
};
pub use crate::optimizer::options::SessionOptimizerSettings;
pub use mv_rewrite::{
    MvRewriteDefinitionIndex, SqlImvAggregateContractFacts, SqlImvAggregateExecutionFacts,
    SqlImvAggregateExecutionStateColumnFacts, SqlImvAggregateStateColumnFacts,
    SqlImvAggregateStateRoleFacts, SqlImvAggregateVisibleColumnFacts, SqlImvApplyKeySourceFacts,
    SqlImvBaseContractFacts, SqlImvBaseFieldFacts, SqlImvBaseSnapshotFacts,
    SqlImvBranchContractFacts, SqlImvExpressionFacts, SqlImvExpressionKindFacts,
    SqlImvJoinContractFacts, SqlImvJoinKindFacts, SqlImvJoinPredicateFacts,
    SqlImvOutputColumnFacts, SqlImvPartitionFacts, SqlImvPartitionFieldFacts,
    SqlImvPartitionTransformFacts, SqlImvQualifiedFieldFacts, SqlImvRefreshHistoryFacts,
    SqlImvRewriteSnapshotBuilder, SqlImvRewriteSnapshotHandle, SqlImvSchemaContractFacts,
    SqlImvTargetColumnsFacts, SqlImvTargetContractFacts, SqlImvTargetVisibleColumnFacts,
    SqlMvRewriteBaseTableFacts, SqlMvRewriteDefinitionFacts,
};

/// SQL's read-only observation of statement cancellation.
///
/// The application owns cancellation reasons and sources.  Compiler phases
/// only need this single immutable fact, so the SQL boundary never imports
/// query-lifecycle cancellation state.
pub trait SqlCancellationObservation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

/// A narrow catalog capability available to one compiler request.
///
/// Concrete connector clients and registry mutation APIs are intentionally not
/// part of this contract. The provider is a query-scoped snapshot and owns the
/// one binding store shared by analysis, statistics, and scan preparation.
pub trait SqlCatalogSnapshot {
    fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider;
}

/// Borrowed adapter over the application-owned query-local table snapshot.
///
/// It exposes only the catalog vocabulary required by `SqlAnalyzeRequest` and
/// cannot construct or mutate planner tables itself.
pub struct SqlPlannerTableSnapshot<'a> {
    provider: &'a dyn crate::catalog::PlannerTableProvider,
}

impl<'a> SqlPlannerTableSnapshot<'a> {
    pub fn new(provider: &'a dyn crate::catalog::PlannerTableProvider) -> Self {
        Self { provider }
    }
}

impl SqlCatalogSnapshot for SqlPlannerTableSnapshot<'_> {
    fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
        self.provider
    }
}

/// Query statistics facts collected during one compilation.  This is a SQL
/// value: it contains no provider, control host, or mutable cache handle.
pub(crate) struct SqlStatisticsPlan {
    pub(crate) snapshot: crate::optimizer::stats_input::QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl SqlStatisticsPlan {
    pub(crate) fn empty() -> Self {
        Self {
            snapshot: crate::optimizer::stats_input::QueryStatsSnapshot::empty(),
            next_stats_ref: 0,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: crate::optimizer::stats_input::BaseTableStatistics,
    ) -> crate::optimizer::stats_input::StatsRef {
        let stats_ref = crate::optimizer::stats_input::StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }

    #[allow(
        dead_code,
        reason = "The next statistics reference is retained for the pending distributed explain handoff."
    )]
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
        table: &crate::planner::table::TableDef,
    ) -> Result<(String, crate::optimizer::stats_input::BaseTableStatistics), SqlCompileError>;
}

impl SqlStatisticsSnapshot for crate::planning::dml::DmlStatisticsSnapshot {
    fn collect_table_statistics(
        &self,
        database: &str,
        table: &crate::planner::table::TableDef,
    ) -> Result<(String, crate::optimizer::stats_input::BaseTableStatistics), SqlCompileError> {
        let crate::planner::table::ScanSource::Sql(source) = &table.source;
        let label = format!(
            "{}.{}.{}",
            source.table.catalog, source.table.namespace, source.table.table
        );
        let _ = database;
        self.0
            .get(source.binding)
            .map(|entry| (entry.label.clone(), entry.statistics.clone()))
            .map_err(|error| {
                SqlCompileError::Compilation(format!(
                    "frozen SQL statistics binding `{label}` is invalid: {error}"
                ))
            })
    }
}

/// Required evidence for an incremental-MV rewrite.  This is data frozen by
/// application admission, not a callback that can re-enter application code
/// while the compiler is running.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlImvRewriteValidation {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

/// Application-frozen incremental-MV planning input.  The compiler owns the
/// rewrite pipeline and its validation; application provides only the exact
/// immutable refresh facts captured for this statement.
#[derive(Clone)]
pub struct SqlImvPlanningInput {
    snapshot: SqlImvRewriteSnapshotHandle,
    pub(crate) validation: SqlImvRewriteValidation,
}

impl SqlImvPlanningInput {
    pub fn new(snapshot: SqlImvRewriteSnapshotHandle, validation: SqlImvRewriteValidation) -> Self {
        Self {
            snapshot,
            validation,
        }
    }

    pub(crate) fn snapshot(&self) -> &Arc<mv_rewrite::SqlImvRewriteSnapshot> {
        self.snapshot.snapshot()
    }
}

/// Immutable SQL function semantics used by analysis and optimization.
///
/// The function implementation and its execution kernels are explicitly out
/// of scope for this compiler-facing contract.
pub trait SqlFunctionCatalog: Send + Sync + std::fmt::Debug {
    /// Freeze an owned handle to exactly the immutable catalog used for
    /// analysis so optimizer rewrites cannot consult ambient state.
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog>;

    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    >;

    fn contains_aggregate(&self, name: &str) -> bool;

    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    >;

    /// Resolve the declared logical overload, then materialize the exact
    /// executable update signature. The latter may append function ORDER BY
    /// channels without changing logical SQL arity.
    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[arrow::datatypes::DataType],
        update_arg_types: &[arrow::datatypes::DataType],
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        if logical_arg_types != update_arg_types {
            return Err(novarocks_functions::FunctionResolutionError::BadSignature(
                "function catalog does not support ordered aggregate update signatures".into(),
            ));
        }
        self.resolve_aggregate_signature(name, logical_arg_types)
    }

    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    >;

    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility;
}

pub use crate::common::expr::{BinOp, LiteralValue, UnOp};

/// One scalar node shape the optimizer may ask a constant evaluator to fold.
///
/// The vocabulary is deliberately SQL-owned: it never names an execution
/// expression node, kernel, or arena handle. Growing it is a compiler-boundary
/// decision, not an execution-layer one.
#[derive(Clone, Debug, PartialEq)]
pub enum FoldNodeKind {
    BinaryOp(BinOp),
    UnaryOp(UnOp),
    Cast,
    Function { name: String },
}

/// One already-folded argument of a fold request.
#[derive(Clone, Debug, PartialEq)]
pub struct FoldArg {
    pub value: LiteralValue,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

/// A single-node constant evaluation request.
///
/// Every argument is already a literal: recursion, volatility gating and the
/// foldable-shape policy stay on the SQL side, so an evaluator is a pure
/// per-node calculator with no traversal knowledge.
#[derive(Clone, Debug, PartialEq)]
pub struct FoldRequest {
    pub kind: FoldNodeKind,
    pub args: Vec<FoldArg>,
    pub out_type: arrow::datatypes::DataType,
    pub out_nullable: bool,
}

/// Evaluates one constant scalar node on behalf of the optimizer.
///
/// The implementation must reuse the same kernels the runtime uses, so a
/// folded literal is bit-identical to what execution would have produced.
/// Implementations are process-lifetime, stateless singletons: the analyzed
/// query carries the reference into the optimize phase, which outlives the
/// analyze request.
///
/// Fail-open contract:
/// - `Ok(Some(literal))` — folded; the literal has type `request.out_type`.
/// - `Ok(None)` — evaluator declines (unmapped node shape or literal type).
/// - `Err(_)` — evaluation failed. Callers keep the original expression and
///   must never surface this as a planning error, because the runtime is
///   still allowed to produce a value or its own error for that expression.
// Design: ADR-0100 (docs/adr/ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md)
pub trait SqlConstantEvaluator: Send + Sync {
    fn eval_scalar(&self, request: &FoldRequest) -> Result<Option<LiteralValue>, String>;
}

/// A constant evaluator that never folds.
///
/// Used by compiler paths that have no execution capability attached; the
/// folding rule degrades to a no-op rather than changing plan semantics.
#[derive(Debug, Default)]
struct NoopConstantEvaluator;

impl SqlConstantEvaluator for NoopConstantEvaluator {
    fn eval_scalar(&self, _request: &FoldRequest) -> Result<Option<LiteralValue>, String> {
        Ok(None)
    }
}

static NOOP_CONSTANT_EVALUATOR: NoopConstantEvaluator = NoopConstantEvaluator;

pub fn noop_constant_evaluator() -> &'static dyn SqlConstantEvaluator {
    &NOOP_CONSTANT_EVALUATOR
}

/// Statement material already owned by the SQL boundary.
///
/// The public constructors accept only source syntax. SQL-internal logical
/// re-entry is intentionally represented by a private variant so application
/// code cannot smuggle a mutable logical tree or column factory back across
/// the compiler boundary.
#[derive(Clone, Debug)]
pub struct SqlStatementInput {
    kind: SqlStatementInputKind,
}

#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "The inline SQL plan payload avoids an allocation at the compiler handoff boundary."
)]
enum SqlStatementInputKind {
    Sql(String),
    ParsedQuery(Box<novarocks_parser::ast::Query>),
    /// A SQL-owned logical transformation that must re-enter the canonical
    /// optimizer kernel without reopening catalog resolution.  Application
    /// code uses this only after compiler-produced logical facts have been
    /// transformed by SQL-owned MV planning.
    LogicalPlan {
        plan: crate::planner::logical::LogicalPlanNode,
        factory: crate::column_id::ColumnRefFactory,
    },
}

impl SqlStatementInput {
    pub fn sql(sql: impl Into<String>) -> Self {
        Self {
            kind: SqlStatementInputKind::Sql(sql.into()),
        }
    }

    pub fn parsed_query(query: Box<novarocks_parser::ast::Query>) -> Self {
        Self {
            kind: SqlStatementInputKind::ParsedQuery(query),
        }
    }

    pub(crate) fn logical_plan(
        plan: crate::planner::logical::LogicalPlanNode,
        factory: crate::column_id::ColumnRefFactory,
    ) -> Self {
        Self {
            kind: SqlStatementInputKind::LogicalPlan { plan, factory },
        }
    }
}

/// The compiler result shape required by the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlCompileIntent {
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
pub enum RootDistributionRequirement {
    Any,
    ShuffleOutputOrdinal(usize),
    ShuffleOutputName(String),
}

/// SQL-relevant session state frozen at statement admission.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlSessionContext {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: SessionOptimizerSettings,
}

/// Deployment facts consumed by planning without exposing topology objects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlPlanningEnvironment {
    Distributed { backend_count: NonZeroUsize },
    NotApplicable,
}

/// Read-only request control observed by compiler phases.
#[derive(Clone)]
pub struct SqlCompileControl {
    deadline: Option<Instant>,
    cancellation: Arc<dyn SqlCancellationObservation>,
}

struct SqlNeverCancelled;

impl SqlCancellationObservation for SqlNeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

impl SqlCompileControl {
    pub fn new(
        deadline: Option<Instant>,
        cancellation: Arc<dyn SqlCancellationObservation>,
    ) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    /// Explicitly unbounded control for an already-admitted SQL logical
    /// transformation.  It is not an execution fallback: callers that have
    /// a request control must still pass its deadline and cancellation view.
    pub fn unbounded() -> Self {
        Self {
            deadline: None,
            cancellation: Arc::new(SqlNeverCancelled),
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

    fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Immutable input for the catalog-touching analysis/materialization phase.
/// Statistics are deliberately absent because exact binding tokens do not
/// exist until this phase has completed.
pub struct SqlAnalyzeRequest<'a> {
    pub(crate) statement: SqlStatementInput,
    pub(crate) intent: SqlCompileIntent,
    pub(crate) session: SqlSessionContext,
    pub(crate) environment: SqlPlanningEnvironment,
    pub(crate) catalog: Option<&'a dyn SqlCatalogSnapshot>,
    pub(crate) functions: Option<&'a dyn SqlFunctionCatalog>,
    pub(crate) owned_functions: Option<Arc<dyn SqlFunctionCatalog>>,
    pub(crate) constant_evaluator: Option<&'static dyn SqlConstantEvaluator>,
    pub(crate) mv_rewrite: Option<&'a MvRewriteDefinitionIndex>,
    pub(crate) imv_rewrite: Option<&'a SqlImvPlanningInput>,
    pub(crate) control: SqlCompileControl,
}

impl<'a> SqlAnalyzeRequest<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        statement: SqlStatementInput,
        intent: SqlCompileIntent,
        session: SqlSessionContext,
        environment: SqlPlanningEnvironment,
        catalog: &'a dyn SqlCatalogSnapshot,
        functions: &'a dyn SqlFunctionCatalog,
        constant_evaluator: &'static dyn SqlConstantEvaluator,
        mv_rewrite: Option<&'a MvRewriteDefinitionIndex>,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement,
            intent,
            session,
            environment,
            catalog: Some(catalog),
            functions: Some(functions),
            owned_functions: None,
            constant_evaluator: Some(constant_evaluator),
            mv_rewrite,
            imv_rewrite: None,
            control,
        }
    }

    /// Build a phase-one request for an already SQL-owned logical plan. It has
    /// no catalog, function, or MV-candidate capability and therefore cannot
    /// materialize another binding.
    ///
    /// Constant evaluation stays available: it materializes no binding and
    /// touches no catalog, and withholding it would make a re-entered plan
    /// fold differently from the same plan compiled directly.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_logical(
        plan: crate::planner::logical::LogicalPlanNode,
        factory: crate::column_id::ColumnRefFactory,
        intent: SqlCompileIntent,
        session: SqlSessionContext,
        environment: SqlPlanningEnvironment,
        constant_evaluator: Option<&'static dyn SqlConstantEvaluator>,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement: SqlStatementInput::logical_plan(plan, factory),
            intent,
            session,
            environment,
            catalog: None,
            functions: None,
            owned_functions: None,
            constant_evaluator,
            mv_rewrite: None,
            imv_rewrite: None,
            control,
        }
    }

    pub(crate) fn check_control(&self) -> Result<(), SqlCompileError> {
        self.control.check()
    }

    pub(crate) fn with_function_catalog(mut self, functions: Arc<dyn SqlFunctionCatalog>) -> Self {
        self.owned_functions = Some(functions);
        self
    }

    fn function_catalog(&self) -> Option<&dyn SqlFunctionCatalog> {
        self.functions.or(self.owned_functions.as_deref())
    }

    fn deadline(&self) -> Option<Instant> {
        self.control.deadline()
    }

    pub(crate) fn with_imv_rewrite(mut self, input: &'a SqlImvPlanningInput) -> Self {
        self.imv_rewrite = Some(input);
        self
    }
}

/// Opaque, move-only compiler state returned after every catalog-touching
/// analysis path has closed its request-local binding set.
pub struct SqlAnalyzedQuery {
    logical_plan: crate::planner::logical::LogicalPlanNode,
    factory: crate::column_id::ColumnRefFactory,
    intent: SqlCompileIntent,
    settings: SessionOptimizerSettings,
    change_stream: crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    mv_rewrite: mv_rewrite::SqlMvRewriteAnalysis,
    function_catalog: Arc<dyn SqlFunctionCatalog>,
    /// Carried from the analyze request because folding runs in the optimize
    /// phase, which outlives the analyze request borrow.
    constant_evaluator: Option<&'static dyn SqlConstantEvaluator>,
    control: SqlCompileControl,
}

/// Typed phase-one outcome. Analyze-only and logical-only requests terminate
/// here; every optimizer or distributed terminal returns one opaque handle.
pub enum SqlAnalyzeOutput {
    Complete(SqlCompileOutput),
    Pending(SqlAnalyzedQuery),
}

impl SqlAnalyzeOutput {
    pub fn into_complete(self) -> Result<SqlCompileOutput, SqlCompileError> {
        match self {
            Self::Complete(output) => Ok(output),
            Self::Pending(_) => Err(SqlCompileError::InvalidRequest(
                "SQL analysis requires a frozen statistics snapshot before optimization"
                    .to_string(),
            )),
        }
    }

    pub fn into_pending(self) -> Result<SqlAnalyzedQuery, SqlCompileError> {
        match self {
            Self::Pending(analyzed) => Ok(analyzed),
            Self::Complete(_) => Err(SqlCompileError::InvalidRequest(
                "SQL analysis already produced a terminal result".to_string(),
            )),
        }
    }
}

/// Immutable input for the no-catalog optimize/seal phase. The analyzed
/// handle is consumed so the same compiler state cannot be sealed twice.
pub struct SqlOptimizeRequest<'a> {
    analyzed: SqlAnalyzedQuery,
    statistics: &'a crate::planning::dml::DmlStatisticsSnapshot,
}

impl<'a> SqlOptimizeRequest<'a> {
    pub fn new(
        analyzed: SqlAnalyzedQuery,
        statistics: &'a crate::planning::dml::DmlStatisticsSnapshot,
    ) -> Self {
        Self {
            analyzed,
            statistics,
        }
    }
}

pub(crate) struct SqlAnalysisOutput {
    pub(crate) logical_plan: crate::planner::logical::LogicalPlanNode,
    pub(crate) factory: crate::column_id::ColumnRefFactory,
}

#[allow(
    dead_code,
    reason = "Optimizer metadata remains part of the compiler terminal until the lifecycle handoff consumes it."
)]
pub(crate) struct SqlOptimizedOutput {
    pub(crate) optimized_tree: crate::optimizer::OptimizedOperatorNode,
    pub(crate) function_catalog: Arc<dyn SqlFunctionCatalog>,
    pub(crate) statistics: SqlStatisticsPlan,
    pub(crate) change_stream: crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    pub(crate) mv_rewrite_diagnostics: Vec<mv_rewrite::SqlMvRewriteDiagnostic>,
}

#[allow(
    dead_code,
    reason = "Distributed compiler metadata remains available for the later explain and lifecycle handoff."
)]
pub(crate) struct SqlDistributedOutput {
    pub(crate) distributed_plan: crate::planner::distributed::DistributedPlan,
    pub(crate) statistics: SqlStatisticsPlan,
    pub(crate) mv_rewrite_diagnostics: Vec<mv_rewrite::SqlMvRewriteDiagnostic>,
}

/// SQL-owned compiler facts. Native DTOs/bytes, lifecycle state and result
/// buffers are intentionally absent; application owns post-compile assembly.
///
/// The carrier is intentionally opaque outside SQL. Its public terminals
/// expose only a sealed distributed plan or rendered EXPLAIN lines; compiler
/// internal logical/optimized graphs never become a cross-owner API.
pub struct SqlCompileOutput {
    kind: SqlCompileOutputKind,
}

#[expect(
    clippy::large_enum_variant,
    reason = "The inline SQL plan payload avoids an allocation at the compiler handoff boundary."
)]
enum SqlCompileOutputKind {
    #[allow(
        dead_code,
        reason = "The opaque analysis terminal is retained for callers that stop before physical planning."
    )]
    Analysis(SqlAnalysisOutput),
    Logical(SqlAnalysisOutput),
    Optimized(SqlOptimizedOutput),
    ImmediateExplain(Vec<String>),
    Distributed(SqlDistributedOutput),
}

/// Immutable application facts for rendering one IMV refresh EXPLAIN result.
/// SQL consumes the opaque rewrite snapshot and keeps logical-plan ownership
/// internal; Core receives only the rendered lines.
pub struct SqlImvRefreshExplainContext<'a> {
    pub canonical_query: Box<novarocks_parser::ast::Query>,
    pub imv_rewrite: SqlImvPlanningInput,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: SessionOptimizerSettings,
    pub environment: SqlPlanningEnvironment,
    pub catalog: &'a dyn SqlCatalogSnapshot,
    pub functions: &'a dyn SqlFunctionCatalog,
    pub constant_evaluator: &'static dyn SqlConstantEvaluator,
    pub control: SqlCompileControl,
    pub level: ExplainLevel,
}

/// Immutable input for SQL-owned MV query analysis. The application may copy
/// parsed syntax and its frozen catalog snapshot in, but receives only the
/// opaque analyzed-MV carrier back.
pub struct SqlMvRefreshAnalysisContext<'a> {
    pub query: Box<novarocks_parser::ast::Query>,
    pub current_database: String,
    pub catalog: &'a dyn SqlCatalogSnapshot,
    pub functions: &'a dyn SqlFunctionCatalog,
}

/// Analyze a prepared MV query without exposing analyzer nodes, CTE state, or
/// the column-id factory to application code.
pub fn analyze_mv_refresh_input(
    context: SqlMvRefreshAnalysisContext<'_>,
) -> Result<crate::planning::mv::SqlResolvedMvRefreshInput, String> {
    let SqlMvRefreshAnalysisContext {
        query,
        current_database,
        catalog,
        functions,
    } = context;
    // The public MV-refresh facade still returns String while its frontend
    // owner is outside SQLP-7. Keep the typed parser rejection intact until
    // that boundary; no category is inferred from this message.
    crate::planning::mv::validate_imv_aggregate_star_arguments(&query)
        .map_err(|error| error.to_string())?;
    let (resolved, _, _) = crate::analyzer::analyze_with_function_catalog(
        &query,
        catalog.planner_table_provider(),
        &current_database,
        functions,
    )
    .map_err(|error| error.to_string())?;
    Ok(crate::planning::mv::SqlResolvedMvRefreshInput::from_analysis(resolved))
}

/// Compile and render an IMV refresh EXPLAIN request without exposing its
/// logical plan, rewrite trace, or column factory to application code.
pub fn compile_imv_refresh_explain_lines(
    context: SqlImvRefreshExplainContext<'_>,
) -> Result<Vec<String>, SqlCompileError> {
    let SqlImvRefreshExplainContext {
        canonical_query,
        imv_rewrite,
        current_catalog,
        current_database,
        optimizer_settings,
        environment,
        catalog,
        functions,
        constant_evaluator,
        control,
        level,
    } = context;
    let mut query = *canonical_query;
    crate::planning::mv::strip_catalog_from_three_part_names(&mut query);
    let request = imv_refresh_explain_request(
        query,
        &imv_rewrite,
        current_catalog,
        current_database,
        optimizer_settings,
        environment,
        catalog,
        functions,
        constant_evaluator,
        control,
    );
    let output = SqlCompiler::analyze(request)?.into_complete()?;
    output.into_explain_lines(level, true)
}

#[allow(clippy::too_many_arguments)]
fn imv_refresh_explain_request<'a>(
    query: novarocks_parser::ast::Query,
    imv_rewrite: &'a SqlImvPlanningInput,
    current_catalog: Option<String>,
    current_database: String,
    optimizer_settings: SessionOptimizerSettings,
    environment: SqlPlanningEnvironment,
    catalog: &'a dyn SqlCatalogSnapshot,
    functions: &'a dyn SqlFunctionCatalog,
    constant_evaluator: &'static dyn SqlConstantEvaluator,
    control: SqlCompileControl,
) -> SqlAnalyzeRequest<'a> {
    SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::LogicalOnly,
        SqlSessionContext {
            current_catalog,
            current_database,
            optimizer_settings,
        },
        environment,
        catalog,
        functions,
        constant_evaluator,
        None,
        control,
    )
    .with_imv_rewrite(imv_rewrite)
}

impl SqlCompileOutput {
    fn analysis(output: SqlAnalysisOutput) -> Self {
        Self {
            kind: SqlCompileOutputKind::Analysis(output),
        }
    }

    fn logical(output: SqlAnalysisOutput) -> Self {
        Self {
            kind: SqlCompileOutputKind::Logical(output),
        }
    }

    fn optimized(output: SqlOptimizedOutput) -> Self {
        Self {
            kind: SqlCompileOutputKind::Optimized(output),
        }
    }

    fn immediate_explain(lines: Vec<String>) -> Self {
        Self {
            kind: SqlCompileOutputKind::ImmediateExplain(lines),
        }
    }

    fn distributed(output: SqlDistributedOutput) -> Self {
        Self {
            kind: SqlCompileOutputKind::Distributed(output),
        }
    }

    pub(crate) fn into_logical_output(self) -> Result<SqlAnalysisOutput, SqlCompileError> {
        match self.kind {
            SqlCompileOutputKind::Logical(output) => Ok(output),
            _ => Err(SqlCompileError::InvalidRequest(
                "SQL compilation did not produce logical SQL facts".to_string(),
            )),
        }
    }

    pub(crate) fn into_optimized_output(self) -> Result<SqlOptimizedOutput, SqlCompileError> {
        match self.kind {
            SqlCompileOutputKind::Optimized(output) => Ok(output),
            _ => Err(SqlCompileError::InvalidRequest(
                "SQL compilation did not produce optimized SQL facts".to_string(),
            )),
        }
    }

    #[cfg(test)]
    fn is_distributed(&self) -> bool {
        matches!(self.kind, SqlCompileOutputKind::Distributed(_))
    }

    /// Consume the only output shape that may cross into Core post-compile
    /// preparation.  The plan remains sealed and callers receive no mutable
    /// builder or validation constructor.
    pub fn into_distributed_plan(
        self,
    ) -> Result<crate::plan_read::DistributedPlan, SqlCompileError> {
        match self.kind {
            SqlCompileOutputKind::Distributed(output) => Ok(output.distributed_plan),
            _ => Err(SqlCompileError::InvalidRequest(
                "SQL compilation did not produce a distributed plan".to_string(),
            )),
        }
    }

    /// Render an EXPLAIN result without exposing the compiler-private logical
    /// plan tree to application code.
    pub fn into_explain_lines(
        self,
        level: ExplainLevel,
        logical: bool,
    ) -> Result<Vec<String>, SqlCompileError> {
        match self.kind {
            SqlCompileOutputKind::Logical(output) if logical => {
                crate::explain::explain_plan_checked(&output.logical_plan, level)
                    .map_err(SqlCompileError::Compilation)
            }
            SqlCompileOutputKind::ImmediateExplain(lines) if !logical => Ok(lines),
            _ => Err(SqlCompileError::InvalidRequest(
                "EXPLAIN intent produced unexpected SQL facts".to_string(),
            )),
        }
    }
}

/// Immutable runtime observations for one sealed distributed-plan node.
///
/// This copied value deliberately contains no profile tree, runtime handle, or
/// lifecycle state. SQL only consumes it while rendering EXPLAIN ANALYZE.
pub struct SqlExplainAnalyzeOperatorFacts {
    node_id: i32,
    output_rows: i64,
    total_time_ns: i64,
    peak_mem_bytes: i64,
    total_time_max_ns: i64,
    total_time_min_ns: i64,
    build_ht_ns: i64,
    search_ns: i64,
    out_build_ns: i64,
    out_probe_ns: i64,
    dict_input_rows: i64,
    dict_input_columns: i64,
    dict_kept_rows: i64,
    dict_kept_columns: i64,
    dict_hydrated_rows: i64,
    dict_hydrated_columns: i64,
    dict_unsupported_columns: i64,
}

impl SqlExplainAnalyzeOperatorFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        node_id: i32,
        output_rows: i64,
        total_time_ns: i64,
        peak_mem_bytes: i64,
        total_time_max_ns: i64,
        total_time_min_ns: i64,
        build_ht_ns: i64,
        search_ns: i64,
        out_build_ns: i64,
        out_probe_ns: i64,
        dict_input_rows: i64,
        dict_input_columns: i64,
        dict_kept_rows: i64,
        dict_kept_columns: i64,
        dict_hydrated_rows: i64,
        dict_hydrated_columns: i64,
        dict_unsupported_columns: i64,
    ) -> Result<Self, SqlCompileError> {
        if node_id < 0 {
            return Err(SqlCompileError::InvalidRequest(
                "EXPLAIN ANALYZE operator facts require a non-negative node id".to_string(),
            ));
        }
        Ok(Self {
            node_id,
            output_rows,
            total_time_ns,
            peak_mem_bytes,
            total_time_max_ns,
            total_time_min_ns,
            build_ht_ns,
            search_ns,
            out_build_ns,
            out_probe_ns,
            dict_input_rows,
            dict_input_columns,
            dict_kept_rows,
            dict_kept_columns,
            dict_hydrated_rows,
            dict_hydrated_columns,
            dict_unsupported_columns,
        })
    }
}

/// Immutable fragment-runtime summary for one sealed distributed-plan root.
pub struct SqlExplainAnalyzeFragmentFacts {
    root_node_id: i32,
    operator_active_time_ns: i64,
    driver_blocked_time_ns: i64,
    dependency_wait_time_ns: i64,
    exchange_wait_time_ns: i64,
    network_time_ns: i64,
    scan_io_time_ns: i64,
}

impl SqlExplainAnalyzeFragmentFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        root_node_id: i32,
        operator_active_time_ns: i64,
        driver_blocked_time_ns: i64,
        dependency_wait_time_ns: i64,
        exchange_wait_time_ns: i64,
        network_time_ns: i64,
        scan_io_time_ns: i64,
    ) -> Result<Self, SqlCompileError> {
        if root_node_id < 0 {
            return Err(SqlCompileError::InvalidRequest(
                "EXPLAIN ANALYZE fragment facts require a non-negative root node id".to_string(),
            ));
        }
        Ok(Self {
            root_node_id,
            operator_active_time_ns,
            driver_blocked_time_ns,
            dependency_wait_time_ns,
            exchange_wait_time_ns,
            network_time_ns,
            scan_io_time_ns,
        })
    }
}

/// SQL-owned, opaque runtime profile for rendering one sealed EXPLAIN ANALYZE
/// plan. Application code may provide copied observations but cannot inspect or
/// mutate SQL's formatter state.
pub struct SqlExplainAnalyzeProfile {
    profile: crate::explain::distributed::SqlExplainProfile,
}

impl SqlExplainAnalyzeProfile {
    pub fn try_new(
        operator_facts: Vec<SqlExplainAnalyzeOperatorFacts>,
        fragment_facts: Vec<SqlExplainAnalyzeFragmentFacts>,
    ) -> Result<Self, SqlCompileError> {
        let mut operators = HashMap::with_capacity(operator_facts.len());
        for facts in operator_facts {
            let node_id = facts.node_id;
            let metrics = crate::explain::distributed::SqlOperatorMetrics {
                output_rows: facts.output_rows,
                total_time_ns: facts.total_time_ns,
                peak_mem_bytes: facts.peak_mem_bytes,
                total_time_max_ns: facts.total_time_max_ns,
                total_time_min_ns: facts.total_time_min_ns,
                build_ht_ns: facts.build_ht_ns,
                search_ns: facts.search_ns,
                out_build_ns: facts.out_build_ns,
                out_probe_ns: facts.out_probe_ns,
                dict_input_rows: facts.dict_input_rows,
                dict_input_columns: facts.dict_input_columns,
                dict_kept_rows: facts.dict_kept_rows,
                dict_kept_columns: facts.dict_kept_columns,
                dict_hydrated_rows: facts.dict_hydrated_rows,
                dict_hydrated_columns: facts.dict_hydrated_columns,
                dict_unsupported_columns: facts.dict_unsupported_columns,
            };
            if operators.insert(node_id, metrics).is_some() {
                return Err(SqlCompileError::InvalidRequest(format!(
                    "EXPLAIN ANALYZE profile has duplicate operator node id {node_id}"
                )));
            }
        }

        let mut fragments = HashMap::with_capacity(fragment_facts.len());
        for facts in fragment_facts {
            let root_node_id = facts.root_node_id;
            let profile = crate::explain::distributed::SqlFragmentProfile {
                operator_active_time_ns: facts.operator_active_time_ns,
                driver_blocked_time_ns: facts.driver_blocked_time_ns,
                dependency_wait_time_ns: facts.dependency_wait_time_ns,
                exchange_wait_time_ns: facts.exchange_wait_time_ns,
                network_time_ns: facts.network_time_ns,
                scan_io_time_ns: facts.scan_io_time_ns,
            };
            if fragments.insert(root_node_id, profile).is_some() {
                return Err(SqlCompileError::InvalidRequest(format!(
                    "EXPLAIN ANALYZE profile has duplicate fragment root node id {root_node_id}"
                )));
            }
        }

        Ok(Self {
            profile: crate::explain::distributed::SqlExplainProfile {
                operators,
                fragments,
            },
        })
    }
}

/// Render EXPLAIN ANALYZE for a sealed distributed plan and copied runtime
/// observations. The plan remains read-only and profile facts fail closed if
/// they name nodes that are absent from the sealed plan.
pub fn render_distributed_explain_analyze(
    plan: &crate::plan_read::DistributedPlan,
    profile: &SqlExplainAnalyzeProfile,
) -> Result<Vec<String>, SqlCompileError> {
    let mut plan_node_ids = HashSet::new();
    let fragment_root_ids = plan
        .fragments()
        .iter()
        .map(|fragment| {
            collect_distributed_plan_node_ids(&fragment.root, &mut plan_node_ids);
            fragment.root.node_id
        })
        .collect::<HashSet<_>>();

    if let Some(node_id) = profile
        .profile
        .operators
        .keys()
        .find(|node_id| !plan_node_ids.contains(node_id))
    {
        return Err(SqlCompileError::InvalidRequest(format!(
            "EXPLAIN ANALYZE operator facts reference unknown sealed-plan node id {node_id}"
        )));
    }
    if let Some(root_node_id) = profile
        .profile
        .fragments
        .keys()
        .find(|node_id| !fragment_root_ids.contains(node_id))
    {
        return Err(SqlCompileError::InvalidRequest(format!(
            "EXPLAIN ANALYZE fragment facts reference unknown sealed-plan root node id {root_node_id}"
        )));
    }

    Ok(
        crate::explain::distributed::explain_distributed_plan_with_profile(
            plan,
            ExplainLevel::Analyze,
            &profile.profile,
        ),
    )
}

fn collect_distributed_plan_node_ids(
    node: &crate::plan_read::DistributedNode,
    node_ids: &mut HashSet<i32>,
) {
    node_ids.insert(node.node_id);
    for child in &node.children {
        collect_distributed_plan_node_ids(child, node_ids);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlCompileError {
    Cancelled,
    DeadlineExceeded,
    InvalidRequest(String),
    Analyze(AnalyzeError),
    Compilation(String),
}

impl std::fmt::Display for SqlCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("SQL compilation was cancelled"),
            Self::DeadlineExceeded => f.write_str("SQL compilation deadline exceeded"),
            Self::InvalidRequest(error) | Self::Compilation(error) => f.write_str(error),
            Self::Analyze(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SqlCompileError {}

/// The canonical two-phase SQL compiler. Catalog materialization belongs only
/// to [`SqlCompiler::analyze`]; optimization and plan sealing belong only to
/// [`SqlCompiler::optimize`] after the application freezes statistics.
pub struct SqlCompiler;

impl SqlCompiler {
    pub fn analyze(request: SqlAnalyzeRequest<'_>) -> Result<SqlAnalyzeOutput, SqlCompileError> {
        request.check_control()?;
        let (mut logical_plan, mut factory, logical_input) = match &request.statement.kind {
            SqlStatementInputKind::LogicalPlan { plan, factory } => {
                (plan.clone(), factory.clone(), true)
            }
            _ => {
                let query = parse_query(&request.statement)?;
                let catalog = request
                    .catalog
                    .ok_or_else(|| {
                        SqlCompileError::InvalidRequest(
                            "SQL analysis requires a catalog snapshot".to_string(),
                        )
                    })?
                    .planner_table_provider();
                let functions = request.function_catalog().ok_or_else(|| {
                    SqlCompileError::InvalidRequest(
                        "SQL analysis requires a function catalog".to_string(),
                    )
                })?;
                let (resolved, ctes, mut factory) = crate::analyzer::analyze_with_function_catalog(
                    &query,
                    catalog,
                    &request.session.current_database,
                    functions,
                )
                .map_err(SqlCompileError::Analyze)?;
                request.check_control()?;
                let logical_plan = crate::planner::plan_query(resolved, ctes, &mut factory)
                    .map_err(SqlCompileError::Compilation)?;
                (logical_plan, factory, false)
            }
        };
        request.check_control()?;

        if matches!(request.intent, SqlCompileIntent::AnalyzeOnly) {
            return Ok(SqlAnalyzeOutput::Complete(SqlCompileOutput::analysis(
                SqlAnalysisOutput {
                    logical_plan,
                    factory,
                },
            )));
        }
        if matches!(request.intent, SqlCompileIntent::LogicalOnly) && request.imv_rewrite.is_none()
        {
            return Ok(SqlAnalyzeOutput::Complete(SqlCompileOutput::logical(
                SqlAnalysisOutput {
                    logical_plan,
                    factory,
                },
            )));
        }
        let mut settings = request.session.optimizer_settings.clone();
        if !matches!(request.intent, SqlCompileIntent::LogicalOnly)
            && (!logical_input
                || !matches!(request.environment, SqlPlanningEnvironment::NotApplicable))
        {
            apply_planning_environment(&mut settings, request.environment)?;
        }
        let mut change_stream = if logical_input {
            {
                crate::planner::imv_rewrite::change_stream::build_change_stream_descriptor(
                    &logical_plan,
                )
            }
        } else {
            Default::default()
        };
        if let Some(input) = request.imv_rewrite {
            if !matches!(
                request.intent,
                SqlCompileIntent::ChangeStreamWrite
                    | SqlCompileIntent::Explain { analyze: false, .. }
                    | SqlCompileIntent::LogicalOnly
            ) {
                return Err(SqlCompileError::InvalidRequest(
                    "incremental MV rewrite requires ChangeStreamWrite, LogicalOnly, or non-analyze EXPLAIN intent"
                        .to_string(),
                ));
            }
            let factory_cell = std::rc::Rc::new(std::cell::RefCell::new(factory));
            let outcome = crate::planner::imv_rewrite::entrypoint::run_imv_rewrite(
                crate::planner::imv_rewrite::entrypoint::ImvRewriteInput {
                    plan:
                        crate::planner::imv_rewrite::entrypoint::normalize_imv_rewrite_root_project(
                            logical_plan,
                        ),
                    snapshot: Arc::clone(input.snapshot()),
                    disabled_rules: settings.disabled_rules.clone(),
                    deadline: request.deadline(),
                    column_ref_factory: std::rc::Rc::clone(&factory_cell),
                    #[cfg(not(test))]
                    function_catalog: request
                        .function_catalog()
                        .ok_or_else(|| {
                            SqlCompileError::InvalidRequest(
                                "IMV rewrite requires a function catalog".to_string(),
                            )
                        })?
                        .snapshot(),
                },
            )
            .map_err(|error| SqlCompileError::Compilation(format!("imv rewrite: {error}")))?;
            validate_imv_rewrite_outcome(input, &outcome)?;
            logical_plan = outcome.plan;
            change_stream = outcome.annotation.change_stream;
            factory = std::rc::Rc::try_unwrap(factory_cell)
                .map_err(|_| {
                    SqlCompileError::Compilation(
                        "IMV rewrite leaked ColumnRefFactory references".to_string(),
                    )
                })?
                .into_inner();
            request.check_control()?;
        }
        if matches!(request.intent, SqlCompileIntent::LogicalOnly) {
            return Ok(SqlAnalyzeOutput::Complete(SqlCompileOutput::logical(
                SqlAnalysisOutput {
                    logical_plan,
                    factory,
                },
            )));
        }

        let mv_rewrite = if let Some(definitions) = request.mv_rewrite {
            let catalog = request
                .catalog
                .ok_or_else(|| {
                    SqlCompileError::InvalidRequest(
                        "MV rewrite analysis requires a catalog snapshot".to_string(),
                    )
                })?
                .planner_table_provider();
            let functions = request.function_catalog().ok_or_else(|| {
                SqlCompileError::InvalidRequest(
                    "MV rewrite analysis requires a function catalog".to_string(),
                )
            })?;
            mv_rewrite::analyze_candidates(
                definitions,
                catalog,
                &request.session.current_database,
                &logical_plan,
                &factory,
                functions,
                &settings,
                &request.control,
            )?
        } else {
            mv_rewrite::SqlMvRewriteAnalysis::empty()
        };
        request.check_control()?;
        let function_catalog = request
            .function_catalog()
            .ok_or_else(|| {
                SqlCompileError::InvalidRequest(
                    "SQL optimization requires an immutable function catalog".to_string(),
                )
            })?
            .snapshot();

        Ok(SqlAnalyzeOutput::Pending(SqlAnalyzedQuery {
            logical_plan,
            factory,
            intent: request.intent,
            settings,
            change_stream,
            mv_rewrite,
            function_catalog,
            constant_evaluator: request.constant_evaluator,
            control: request.control,
        }))
    }

    pub fn optimize(request: SqlOptimizeRequest<'_>) -> Result<SqlCompileOutput, SqlCompileError> {
        let SqlAnalyzedQuery {
            logical_plan,
            factory,
            intent,
            settings,
            change_stream,
            mv_rewrite,
            function_catalog,
            constant_evaluator,
            control,
        } = request.analyzed;
        control.check()?;
        let mut scalar_arena = crate::optimizer::scalar::ScalarArena::new();
        let mut optimizer_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
            &logical_plan,
            &mut scalar_arena,
        )
        .map_err(SqlCompileError::Compilation)?;
        let mut statistics = collect_statistics(request.statistics, &mut optimizer_expr)?;
        control.check()?;
        let (mv_rewrite, factory) = mv_rewrite::attach_candidate_statistics(
            mv_rewrite,
            request.statistics,
            &mut statistics,
            factory,
        )?;
        let mv_rewrite::SqlMvRewritePreparation {
            candidates: mv_candidates,
            diagnostics: mv_rewrite_diagnostics,
        } = mv_rewrite;
        control.check()?;
        let root_distribution = match &intent {
            SqlCompileIntent::IcebergWrite { root_distribution } => {
                resolve_root_distribution_requirement(&logical_plan, root_distribution)?
            }
            _ => None,
        };
        let optimized_tree = match root_distribution {
            Some(root_distribution) => crate::optimizer::optimize_with_root_distribution(
                optimizer_expr,
                scalar_arena,
                &statistics.snapshot,
                factory,
                root_distribution,
                crate::optimizer::OptimizerEnvironment::new(
                    &settings,
                    constant_evaluator,
                    Arc::clone(&function_catalog),
                ),
            ),
            None => crate::optimizer::optimize(
                optimizer_expr,
                scalar_arena,
                &statistics.snapshot,
                factory,
                mv_candidates,
                crate::optimizer::OptimizerEnvironment::new(
                    &settings,
                    constant_evaluator,
                    Arc::clone(&function_catalog),
                ),
            ),
        }
        .map_err(SqlCompileError::Compilation)?;
        control.check()?;

        if let SqlCompileIntent::Explain {
            level,
            analyze: false,
        } = intent
        {
            let mut lines = Vec::new();
            if matches!(level, ExplainLevel::Costs) {
                lines.extend(statistics.snapshot.display_rows());
            }
            let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .map_err(SqlCompileError::Compilation)?;
            let distributed =
                crate::planner::pipeline::build_distributed_plan_with_settings(physical, &settings)
                    .map_err(SqlCompileError::Compilation)?;
            lines.extend(crate::explain::distributed::explain_distributed_plan(
                &distributed,
                level,
            ));
            return Ok(SqlCompileOutput::immediate_explain(lines));
        }

        if matches!(
            intent,
            SqlCompileIntent::IcebergWrite { .. } | SqlCompileIntent::ChangeStreamWrite
        ) {
            return Ok(SqlCompileOutput::optimized(SqlOptimizedOutput {
                optimized_tree,
                function_catalog,
                statistics,
                change_stream,
                mv_rewrite_diagnostics,
            }));
        }

        let physical = crate::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
            .map_err(SqlCompileError::Compilation)?;
        let distributed_plan =
            crate::planner::pipeline::build_distributed_plan_with_settings(physical, &settings)
                .map_err(SqlCompileError::Compilation)?;
        control.check()?;
        Ok(SqlCompileOutput::distributed(SqlDistributedOutput {
            distributed_plan,
            statistics,
            mv_rewrite_diagnostics,
        }))
    }
}

fn parse_query(
    statement: &SqlStatementInput,
) -> Result<novarocks_parser::ast::Query, SqlCompileError> {
    let sql = match &statement.kind {
        SqlStatementInputKind::Sql(sql) => sql,
        SqlStatementInputKind::ParsedQuery(query) => return Ok((**query).clone()),
        SqlStatementInputKind::LogicalPlan { .. } => {
            return Err(SqlCompileError::InvalidRequest(
                "logical SQL compiler input must bypass parsing".to_string(),
            ));
        }
    };
    match novarocks_parser::parse(sql)
        .map_err(|error| SqlCompileError::Compilation(error.to_string()))?
    {
        mut statements if statements.len() == 1 => match statements.remove(0) {
            novarocks_parser::ast::Statement::Query(query) => Ok(query),
            _ => Err(SqlCompileError::InvalidRequest(
                "SQL compiler requires a query statement after application preprocessing"
                    .to_string(),
            )),
        },
        _ => Err(SqlCompileError::InvalidRequest(
            "SQL compiler requires a query statement after application preprocessing".to_string(),
        )),
    }
}

pub(crate) fn validate_imv_rewrite_outcome(
    input: &SqlImvPlanningInput,
    outcome: &crate::planner::imv_rewrite::entrypoint::ImvRewriteOutcome,
) -> Result<(), SqlCompileError> {
    let target = input.snapshot().target.fqn();
    let rule_changed = |rule_name: &str| {
        outcome.trace.events().iter().any(|event| {
            matches!(
                event,
                crate::optimizer::rewrite::trace::RewriteTraceEvent::RuleChanged { rule, .. }
                    if *rule == rule_name
            )
        })
    };
    if input.validation == SqlImvRewriteValidation::JoinAggregate
        && !rule_changed("RewriteJoinDelta")
    {
        return Err(SqlCompileError::Compilation(format!(
            "iceberg join aggregate MV {target} incremental refresh rewrite did not apply RewriteJoinDelta"
        )));
    }
    if input.validation == SqlImvRewriteValidation::BranchUnionAggregate
        && !rule_changed("RewriteBranchUnion")
    {
        return Err(SqlCompileError::Compilation(format!(
            "iceberg branch UNION ALL aggregate MV {target} incremental refresh rewrite did not apply RewriteBranchUnion"
        )));
    }
    if input.validation != SqlImvRewriteValidation::None
        && input.validation != SqlImvRewriteValidation::BranchUnionAggregate
        && !rule_changed("RewriteAggregateState")
    {
        let label = match input.validation {
            SqlImvRewriteValidation::JoinAggregate => "join aggregate",
            _ => "aggregate",
        };
        return Err(SqlCompileError::Compilation(format!(
            "iceberg {label} MV {target} incremental refresh rewrite did not apply RewriteAggregateState"
        )));
    }
    if input.validation != SqlImvRewriteValidation::None
        && !outcome.annotation.change_stream.has_aggregate()
    {
        let label = match input.validation {
            SqlImvRewriteValidation::JoinAggregate => "join aggregate",
            SqlImvRewriteValidation::BranchUnionAggregate => "branch UNION ALL aggregate",
            _ => "aggregate",
        };
        return Err(SqlCompileError::Compilation(format!(
            "iceberg {label} MV {target} incremental refresh rewrite plan does not contain aggregate state change stream"
        )));
    }
    Ok(())
}

fn resolve_root_distribution_requirement(
    logical_plan: &crate::planner::logical::LogicalPlanNode,
    requirement: &RootDistributionRequirement,
) -> Result<Option<crate::optimizer::property::DistributionSpec>, SqlCompileError> {
    let output_columns =
        crate::planner::plan_output_columns(logical_plan).map_err(SqlCompileError::Compilation)?;
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
    if column.column_id == crate::column_id::ColumnId::UNSET {
        return Err(SqlCompileError::InvalidRequest(format!(
            "cannot derive Iceberg write root shuffle: output column '{}' has no ColumnId",
            column.name
        )));
    }
    Ok(Some(
        crate::optimizer::property::DistributionSpec::shuffle_agg([column.column_id]),
    ))
}

fn collect_statistics(
    snapshot: &dyn SqlStatisticsSnapshot,
    expr: &mut crate::optimizer::opt_expr::OptExpr,
) -> Result<SqlStatisticsPlan, SqlCompileError> {
    fn walk(
        snapshot: &dyn SqlStatisticsSnapshot,
        expr: &mut crate::optimizer::opt_expr::OptExpr,
        plan: &mut SqlStatisticsPlan,
    ) -> Result<(), SqlCompileError> {
        if let crate::optimizer::operator::Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = crate::optimizer::stats_input::StatsRef::new(plan.next_stats_ref);
            plan.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);
            let (label, stats) = snapshot.collect_table_statistics(&scan.database, &scan.table)?;
            plan.snapshot.insert(stats_ref, label, stats);
        }
        for child in &mut expr.children {
            walk(snapshot, child, plan)?;
        }
        Ok(())
    }

    let mut plan = SqlStatisticsPlan::empty();
    walk(snapshot, expr, &mut plan)?;
    Ok(plan)
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use std::time::{Duration, Instant};

    use super::*;

    struct Catalog;
    impl SqlCatalogSnapshot for Catalog {
        fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
            panic!("control tests must not reach catalog resolution")
        }
    }
    #[derive(Debug)]
    struct Functions;
    impl SqlFunctionCatalog for Functions {
        fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
            Arc::new(Self)
        }

        fn resolve_scalar_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<crate::functions::ResolvedScalarFunction, crate::functions::ResolveError>
        {
            Err(crate::functions::ResolveError::UnknownFunction)
        }

        fn contains_aggregate(&self, _name: &str) -> bool {
            false
        }

        fn resolve_aggregate_signature(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<novarocks_functions::ResolvedAggregateSignature, crate::functions::ResolveError>
        {
            Err(crate::functions::ResolveError::UnknownFunction)
        }

        fn resolve_aggregate_trusted(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
        ) -> Result<novarocks_functions::ResolvedAggregateSignature, crate::functions::ResolveError>
        {
            Err(crate::functions::ResolveError::UnknownFunction)
        }

        fn volatility(&self, _name: &str) -> crate::functions::FunctionVolatility {
            crate::functions::FunctionVolatility::Immutable
        }
    }

    static CATALOG: Catalog = Catalog;
    static STATISTICS: LazyLock<crate::planning::dml::DmlStatisticsSnapshot> =
        LazyLock::new(crate::planning::dml::DmlStatisticsSnapshot::empty);
    static FUNCTIONS: Functions = Functions;

    struct CountingTableCatalog {
        resolutions: AtomicUsize,
    }

    impl CountingTableCatalog {
        fn new() -> Self {
            Self {
                resolutions: AtomicUsize::new(0),
            }
        }

        fn resolution_count(&self) -> usize {
            self.resolutions.load(Ordering::Acquire)
        }
    }

    impl crate::catalog::PlannerTableProvider for CountingTableCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            self.resolutions.fetch_add(1, Ordering::AcqRel);
            if table != "orders" {
                return Err(format!("unknown test table `{table}`"));
            }
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog,
                database,
                crate::planner::table::TableDef {
                    name: table.to_string(),
                    columns: vec![novarocks_types::schema::ColumnDef {
                        name: "order_id".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::planner::table::test_sql_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
            ))
        }
    }

    impl SqlCatalogSnapshot for CountingTableCatalog {
        fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
            self
        }
    }

    #[derive(Default)]
    struct Cancellation(AtomicBool);

    impl Cancellation {
        fn request(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl SqlCancellationObservation for Cancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    fn control(deadline: Option<Instant>, cancellation: &Arc<Cancellation>) -> SqlCompileControl {
        SqlCompileControl::new(
            deadline,
            Arc::clone(cancellation) as Arc<dyn SqlCancellationObservation>,
        )
    }

    fn request(control: SqlCompileControl) -> SqlAnalyzeRequest<'static> {
        SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1"),
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
            &FUNCTIONS,
            noop_constant_evaluator(),
            None,
            control,
        )
    }

    fn analyze_then_optimize(
        request: SqlAnalyzeRequest<'_>,
    ) -> Result<SqlCompileOutput, SqlCompileError> {
        let analyzed = SqlCompiler::analyze(request)?.into_pending()?;
        SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &STATISTICS))
    }

    fn table_request<'a>(
        catalog: &'a CountingTableCatalog,
        control: SqlCompileControl,
    ) -> SqlAnalyzeRequest<'a> {
        SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select order_id from orders"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed {
                backend_count: NonZeroUsize::new(3).unwrap(),
            },
            catalog,
            &FUNCTIONS,
            noop_constant_evaluator(),
            None,
            control,
        )
    }

    fn missing_table_statistics() -> crate::planning::dml::DmlStatisticsSnapshot {
        crate::planning::dml::DmlStatisticsSnapshot::from_evidence([
            crate::planning::dml::DmlStatisticsEvidence::Missing {
                binding: crate::binding::SqlTableBindingId::new_for_test(1),
                label: "iceberg.db.orders".to_string(),
                reason: "statistics are not published".to_string(),
            },
        ])
    }

    fn imv_validation_input(validation: SqlImvRewriteValidation) -> SqlImvPlanningInput {
        SqlImvPlanningInput::new(
            crate::compiler::mv_rewrite::test_incremental_snapshot_handle(),
            validation,
        )
    }

    fn imv_validation_outcome(
        changed_rules: &[&'static str],
        aggregate_change_stream: bool,
    ) -> crate::planner::imv_rewrite::entrypoint::ImvRewriteOutcome {
        let mut trace = crate::optimizer::rewrite::trace::RewriteTrace::default();
        for rule in changed_rules {
            trace.rule_changed(
                crate::optimizer::rewrite::phase::RewritePhase::SemanticRewrite,
                rule,
                0,
            );
        }
        let mut annotation = crate::planner::imv_rewrite::annotation::ImvPlanAnnotation::default();
        if aggregate_change_stream {
            annotation.change_stream = crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor {
                aggregate: Some(
                    crate::planner::imv_rewrite::change_stream::AggregateChangeStreamDescriptor {
                        action_column_id: crate::column_id::ColumnId::new_for_test(1),
                        action_column_name: crate::common::CHANGE_OP_COLUMN.to_string(),
                        shape: crate::planner::imv_rewrite::change_stream::AggregateChangeStreamShape::UnionChangeStream,
                        target_state: crate::planner::imv_rewrite::change_stream::TargetStateProof {
                            present: true,
                        },
                        signed_state_aggregate: crate::planner::imv_rewrite::change_stream::SignedStateAggregateProof {
                            present: true,
                        },
                    },
                ),
                ..Default::default()
            };
        }
        crate::planner::imv_rewrite::entrypoint::ImvRewriteOutcome {
            plan: crate::planner::logical::LogicalPlanNode::new(
                crate::planner::logical::LogicalPlanKind::Values(
                    crate::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    },
                ),
                Vec::new(),
                None,
            ),
            trace,
            annotation,
        }
    }

    #[test]
    fn sqlx2_request_keeps_sql_owned_cancellation_observation() {
        let cancellation = Arc::new(Cancellation::default());
        let request = request(control(None, &cancellation));
        assert_eq!(request.check_control(), Ok(()));
        assert!(matches!(
            request.environment,
            SqlPlanningEnvironment::Distributed { backend_count } if backend_count.get() == 3
        ));
    }

    #[test]
    fn sqlx2_request_rejects_cancelled_control_before_compilation() {
        let cancellation = Arc::new(Cancellation::default());
        cancellation.request();
        assert!(matches!(
            analyze_then_optimize(request(control(None, &cancellation))),
            Err(SqlCompileError::Cancelled)
        ));
    }

    #[test]
    fn sqlx2_request_rejects_expired_deadline_before_compilation() {
        let cancellation = Arc::new(Cancellation::default());
        let deadline = Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            analyze_then_optimize(request(control(Some(deadline), &cancellation))),
            Err(SqlCompileError::DeadlineExceeded)
        ));
    }

    #[test]
    fn two_phase_compiler_uses_typed_missing_without_reentering_catalog() {
        let catalog = CountingTableCatalog::new();
        let cancellation = Arc::new(Cancellation::default());
        let analyzed = SqlCompiler::analyze(table_request(&catalog, control(None, &cancellation)))
            .expect("phase one analyzes the table")
            .into_pending()
            .expect("query requires frozen statistics");
        assert_eq!(catalog.resolution_count(), 1);

        let statistics = missing_table_statistics();
        let output = SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
            .expect("typed Missing is conservative, not fatal");
        assert!(output.is_distributed());
        assert_eq!(
            catalog.resolution_count(),
            1,
            "phase two must not resolve catalog tables"
        );
    }

    #[test]
    fn phase_two_fails_closed_when_snapshot_omits_analyzed_binding() {
        let catalog = CountingTableCatalog::new();
        let cancellation = Arc::new(Cancellation::default());
        let analyzed = SqlCompiler::analyze(table_request(&catalog, control(None, &cancellation)))
            .expect("phase one analyzes the table")
            .into_pending()
            .expect("query requires frozen statistics");

        let error = match SqlCompiler::optimize(SqlOptimizeRequest::new(
            analyzed,
            &crate::planning::dml::DmlStatisticsSnapshot::empty(),
        )) {
            Ok(_) => panic!("an omitted binding token must be fatal"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("binding is missing"),
            "unexpected missing-binding error: {error}"
        );
        assert_eq!(catalog.resolution_count(), 1);
    }

    #[test]
    fn phase_two_preserves_all_fatal_statistics_evidence() {
        use crate::planning::dml::{DmlStatisticsEvidence, DmlStatisticsFailure};

        let failures = [
            DmlStatisticsFailure::OwnerMismatch,
            DmlStatisticsFailure::IncarnationMismatch,
            DmlStatisticsFailure::DataVersionMismatch,
            DmlStatisticsFailure::CorruptEvidence("invalid bounds".to_string()),
        ];
        for failure in failures {
            let catalog = CountingTableCatalog::new();
            let cancellation = Arc::new(Cancellation::default());
            let analyzed =
                SqlCompiler::analyze(table_request(&catalog, control(None, &cancellation)))
                    .expect("phase one analyzes the table")
                    .into_pending()
                    .expect("query requires frozen statistics");
            let statistics = crate::planning::dml::DmlStatisticsSnapshot::from_evidence([
                DmlStatisticsEvidence::Fatal {
                    binding: crate::binding::SqlTableBindingId::new_for_test(1),
                    label: "iceberg.db.orders".to_string(),
                    failure: failure.clone(),
                },
            ]);

            let error = match SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
            {
                Ok(_) => panic!("fatal statistics evidence must fail compilation: {failure:?}"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("invalid"),
                "fatal evidence must retain an explicit statistics error: {error}"
            );
            assert_eq!(catalog.resolution_count(), 1);
        }
    }

    #[test]
    fn phase_two_observes_cancellation_after_analysis() {
        let catalog = CountingTableCatalog::new();
        let cancellation = Arc::new(Cancellation::default());
        let analyzed = SqlCompiler::analyze(table_request(&catalog, control(None, &cancellation)))
            .expect("phase one completes before cancellation")
            .into_pending()
            .expect("query requires frozen statistics");
        cancellation.request();

        assert!(matches!(
            SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &STATISTICS)),
            Err(SqlCompileError::Cancelled)
        ));
    }

    #[test]
    fn phase_two_observes_deadline_after_analysis() {
        let catalog = CountingTableCatalog::new();
        let cancellation = Arc::new(Cancellation::default());
        let deadline = Instant::now() + Duration::from_millis(100);
        let analyzed = SqlCompiler::analyze(table_request(
            &catalog,
            control(Some(deadline), &cancellation),
        ))
        .expect("phase one completes before the deadline")
        .into_pending()
        .expect("query requires frozen statistics");
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));

        assert!(matches!(
            SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &STATISTICS)),
            Err(SqlCompileError::DeadlineExceeded)
        ));
    }

    #[test]
    fn sqlx2_request_models_metadata_planning_without_a_fake_backend() {
        let cancellation = Arc::new(Cancellation::default());
        let mut request = request(control(None, &cancellation));
        request.environment = SqlPlanningEnvironment::NotApplicable;
        assert_eq!(request.check_control(), Ok(()));
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn compiler_session_settings_expose_static_predicate_pushdown_policy() {
        assert!(SessionOptimizerSettings::default().connector_static_predicate_pushdown_enabled());

        let mut disabled = SessionOptimizerSettings::default();
        disabled.enable_connector_static_predicate_pushdown = Some(false);
        assert!(!disabled.connector_static_predicate_pushdown_enabled());

        let mut enabled = SessionOptimizerSettings::default();
        enabled.enable_connector_static_predicate_pushdown = Some(true);
        assert!(enabled.connector_static_predicate_pushdown_enabled());
    }

    #[test]
    fn explain_output_terminal_rejects_the_wrong_output_shape() {
        let error = SqlCompileOutput::immediate_explain(vec!["EXPLAIN".to_string()])
            .into_explain_lines(ExplainLevel::Normal, true)
            .expect_err("logical explain must not accept immediate explain facts");
        assert!(matches!(error, SqlCompileError::InvalidRequest(_)));

        let _: fn(SqlCompileOutput, ExplainLevel, bool) -> Result<Vec<String>, SqlCompileError> =
            SqlCompileOutput::into_explain_lines;
    }

    #[test]
    fn imv_refresh_explain_terminal_keeps_logical_rewrite_inside_sql() {
        let mut statements = novarocks_parser::parse("SELECT 1").expect("query fixture parses");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query fixture");
        };
        let cancellation = Arc::new(Cancellation::default());
        let input = imv_validation_input(SqlImvRewriteValidation::None);
        let request = imv_refresh_explain_request(
            query.clone(),
            &input,
            Some("ice".to_string()),
            "db".to_string(),
            SessionOptimizerSettings::default(),
            SqlPlanningEnvironment::NotApplicable,
            &CATALOG,
            &FUNCTIONS,
            noop_constant_evaluator(),
            control(None, &cancellation),
        );

        assert!(matches!(request.intent, SqlCompileIntent::LogicalOnly));
        assert!(matches!(
            request.environment,
            SqlPlanningEnvironment::NotApplicable
        ));
        assert!(request.imv_rewrite.is_some());
        let _: fn(SqlImvRefreshExplainContext<'_>) -> Result<Vec<String>, SqlCompileError> =
            compile_imv_refresh_explain_lines;
    }

    fn mv_analysis_query(sql: &str) -> Box<novarocks_parser::ast::Query> {
        let mut statements = novarocks_parser::parse(sql).expect("MV analysis query parses");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected MV analysis query");
        };
        Box::new(query.clone())
    }

    #[test]
    fn mv_refresh_analysis_terminal_returns_only_opaque_analysis_input() {
        let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        catalog
            .create_database("db")
            .expect("create MV analysis database");
        catalog
            .register(
                "db",
                crate::planner::table::TableDef {
                    name: "orders".to_string(),
                    columns: vec![novarocks_types::schema::ColumnDef {
                        name: "order_id".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::planner::table::test_sql_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
            )
            .expect("register catalog-visible MV table");
        let catalog = SqlPlannerTableSnapshot::new(&catalog);
        let functions = crate::functions::build_builtin_engine_function_catalog()
            .expect("builtin function catalog");
        let input = analyze_mv_refresh_input(SqlMvRefreshAnalysisContext {
            query: mv_analysis_query("SELECT order_id FROM orders"),
            current_database: "db".to_string(),
            catalog: &catalog,
            functions: &functions,
        })
        .expect("analyze MV query through opaque terminal");

        let facts = input.analysis_facts();
        assert_eq!(facts.output_columns.len(), 1);
        assert_eq!(facts.output_columns[0].name, "order_id");
        let _: fn(
            SqlMvRefreshAnalysisContext<'_>,
        ) -> Result<crate::planning::mv::SqlResolvedMvRefreshInput, String> =
            analyze_mv_refresh_input;
    }

    #[test]
    fn mv_refresh_analysis_terminal_fails_closed_for_missing_table() {
        let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        catalog
            .create_database("db")
            .expect("create MV analysis database");
        let catalog = SqlPlannerTableSnapshot::new(&catalog);
        let functions = crate::functions::build_builtin_engine_function_catalog()
            .expect("builtin function catalog");
        let error = analyze_mv_refresh_input(SqlMvRefreshAnalysisContext {
            query: mv_analysis_query("SELECT order_id FROM missing_orders"),
            current_database: "db".to_string(),
            catalog: &catalog,
            functions: &functions,
        })
        .expect_err("unregistered MV table must not analyze");
        assert!(
            error.contains("missing_orders"),
            "missing-table error must retain SQL analyzer context: {error}"
        );
    }

    fn explain_operator_facts(node_id: i32) -> SqlExplainAnalyzeOperatorFacts {
        SqlExplainAnalyzeOperatorFacts::try_new(
            node_id, 7, 10_000, 64, 11_000, 9_000, 2_000, 3_000, 4_000, 5_000, 6, 2, 5, 1, 4, 1, 0,
        )
        .expect("valid operator facts")
    }

    fn explain_fragment_facts(root_node_id: i32) -> SqlExplainAnalyzeFragmentFacts {
        SqlExplainAnalyzeFragmentFacts::try_new(
            root_node_id,
            20_000,
            1_000,
            2_000,
            3_000,
            4_000,
            5_000,
        )
        .expect("valid fragment facts")
    }

    #[test]
    fn explain_analyze_profile_rejects_invalid_and_duplicate_facts() {
        assert!(matches!(
            SqlExplainAnalyzeOperatorFacts::try_new(
                -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ),
            Err(SqlCompileError::InvalidRequest(_))
        ));
        assert!(matches!(
            SqlExplainAnalyzeFragmentFacts::try_new(-1, 0, 0, 0, 0, 0, 0),
            Err(SqlCompileError::InvalidRequest(_))
        ));
        assert!(matches!(
            SqlExplainAnalyzeProfile::try_new(
                vec![explain_operator_facts(1), explain_operator_facts(1)],
                Vec::new(),
            ),
            Err(SqlCompileError::InvalidRequest(_))
        ));
        assert!(matches!(
            SqlExplainAnalyzeProfile::try_new(
                Vec::new(),
                vec![explain_fragment_facts(1), explain_fragment_facts(1)],
            ),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }

    #[test]
    fn explain_analyze_renderer_accepts_only_facts_for_the_sealed_plan() {
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let plan = analyze_then_optimize(SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1"),
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
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            control(None, &cancellation),
        ))
        .expect("compile distributed query")
        .into_distributed_plan()
        .expect("sealed distributed plan");
        let root_node_id = plan
            .fragments()
            .first()
            .expect("fixture has a fragment")
            .root
            .node_id;
        let profile = SqlExplainAnalyzeProfile::try_new(
            vec![explain_operator_facts(root_node_id)],
            vec![explain_fragment_facts(root_node_id)],
        )
        .expect("sealed profile");
        let rendered = render_distributed_explain_analyze(&plan, &profile)
            .expect("render sealed profile")
            .join("\n");
        assert!(rendered.contains("PLAN FRAGMENT"), "{rendered}");
        assert!(rendered.contains("act={rows=7"), "{rendered}");
        assert!(rendered.contains("Profile: active=20us"), "{rendered}");

        let unknown =
            SqlExplainAnalyzeProfile::try_new(vec![explain_operator_facts(i32::MAX)], Vec::new())
                .expect("well-formed but mismatched facts");
        assert!(matches!(
            render_distributed_explain_analyze(&plan, &unknown),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }

    #[test]
    fn sqlx1_kernel_compiles_a_query_without_application_state() {
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1"),
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
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            control(None, &cancellation),
        );

        assert!(
            analyze_then_optimize(request)
                .expect("query compile")
                .is_distributed()
        );
    }

    #[test]
    fn sqlx2_kernel_compiles_a_query_from_sql_owned_inputs() {
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1"),
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
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            control(None, &cancellation),
        );

        assert!(
            analyze_then_optimize(request)
                .expect("query compile")
                .is_distributed()
        );
    }

    #[test]
    fn parsed_query_input_preserves_complex_types_and_escapes_without_sql_round_trip() {
        let mut statements =
            novarocks_parser::parse(r"SELECT CAST('{}' AS MAP<STRING, INT>), 'e\\f'")
                .expect("query fixture must parse");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query fixture");
        };

        assert_eq!(
            parse_query(&SqlStatementInput::parsed_query(Box::new(query.clone()))),
            Ok(query.clone())
        );
    }

    #[test]
    fn sqlx1_kernel_write_root_requirement_is_validated_in_sql() {
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1 as payload"),
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
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            control(None, &cancellation),
        );
        assert!(matches!(
            analyze_then_optimize(request),
            Err(SqlCompileError::InvalidRequest(error)) if error.contains("output column 'missing' not found")
        ));
    }

    #[test]
    fn sqlx2_kernel_rejects_missing_aggregate_rewrite_evidence() {
        let input = SqlImvPlanningInput::new(
            crate::compiler::mv_rewrite::test_incremental_snapshot_handle(),
            SqlImvRewriteValidation::Aggregate,
        );
        let outcome = crate::planner::imv_rewrite::entrypoint::ImvRewriteOutcome {
            plan: crate::planner::logical::LogicalPlanNode::new(
                crate::planner::logical::LogicalPlanKind::Values(
                    crate::planner::payload::PlanValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    },
                ),
                Vec::new(),
                None,
            ),
            trace: crate::optimizer::rewrite::trace::RewriteTrace::default(),
            annotation: crate::planner::imv_rewrite::annotation::ImvPlanAnnotation::default(),
        };

        assert!(matches!(
            validate_imv_rewrite_outcome(&input, &outcome),
            Err(SqlCompileError::Compilation(error)) if error.contains("RewriteAggregateState")
        ));
    }

    #[test]
    fn aggregate_refresh_rejects_unchanged_rewrite_outcome() {
        let error = validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::Aggregate),
            &imv_validation_outcome(&[], false),
        )
        .expect_err("aggregate refresh must not continue with unchanged rewrite outcome");
        assert!(
            error
                .to_string()
                .contains("did not apply RewriteAggregateState")
        );
    }

    #[test]
    fn aggregate_refresh_rejects_missing_merge_plan_evidence() {
        let error = validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::Aggregate),
            &imv_validation_outcome(&["RewriteAggregateState"], false),
        )
        .expect_err("aggregate refresh must require change stream in the rewrite plan");
        assert!(
            error
                .to_string()
                .contains("does not contain aggregate state change stream")
        );
    }

    #[test]
    fn join_aggregate_refresh_rejects_missing_join_rewrite_evidence() {
        let error = validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::JoinAggregate),
            &imv_validation_outcome(&["RewriteAggregateState"], true),
        )
        .expect_err("join aggregate refresh must require join rewrite evidence");
        assert!(error.to_string().contains("did not apply RewriteJoinDelta"));
    }

    #[test]
    fn join_aggregate_refresh_missing_merge_plan_uses_join_label() {
        let error = validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::JoinAggregate),
            &imv_validation_outcome(&["RewriteJoinDelta", "RewriteAggregateState"], false),
        )
        .expect_err("join aggregate refresh must require change stream in the rewrite plan");
        assert!(
            error.to_string().contains("iceberg join aggregate MV")
                && error
                    .to_string()
                    .contains("does not contain aggregate state change stream")
        );
    }

    #[test]
    fn branch_union_aggregate_refresh_rejects_missing_branch_union_rewrite_evidence() {
        let error = validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::BranchUnionAggregate),
            &imv_validation_outcome(&["RewriteAggregateState"], true),
        )
        .expect_err("branch UNION ALL aggregate refresh must require branch rewrite evidence");
        assert!(
            error.to_string().contains("branch UNION ALL aggregate")
                && error
                    .to_string()
                    .contains("did not apply RewriteBranchUnion")
        );
    }

    #[test]
    fn branch_union_aggregate_refresh_requires_state_merge_plan_evidence() {
        let error = validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::BranchUnionAggregate),
            &imv_validation_outcome(&["RewriteBranchUnion"], false),
        )
        .expect_err("branch UNION ALL aggregate refresh must require change-stream plan evidence");
        assert!(
            error
                .to_string()
                .contains("iceberg branch UNION ALL aggregate MV")
                && error
                    .to_string()
                    .contains("does not contain aggregate state change stream")
        );
    }

    #[test]
    fn branch_union_aggregate_refresh_accepts_branch_rewrite_with_change_stream_plan() {
        validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::BranchUnionAggregate),
            &imv_validation_outcome(&["RewriteBranchUnion"], true),
        )
        .expect("branch UNION ALL aggregate refresh should accept aggregate-state change-stream evidence");
    }

    #[test]
    fn aggregate_refresh_accepts_change_stream_descriptor_evidence() {
        validate_imv_rewrite_outcome(
            &imv_validation_input(SqlImvRewriteValidation::Aggregate),
            &imv_validation_outcome(&["RewriteAggregateState"], true),
        )
        .expect(
            "aggregate refresh should accept aggregate-state change-stream descriptor evidence",
        );
    }
}
pub(crate) mod mv_rewrite;
