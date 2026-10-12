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

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::analyze_error::AnalyzeError;
pub use crate::explain::ExplainLevel;
pub use crate::functions::{
    build_builtin_engine_function_catalog, builtin_engine_function_catalog,
    builtin_sql_function_catalog, contribute_builtin_functions,
};
pub use crate::optimizer::options::SessionOptimizerSettings;
mod emission_mode;
mod scalar_presence_catalog;
pub use crate::planner::distributed::build::{
    AggregateRuntimeDemand, CheckedSqlResultDeclaration, ResultDeclarationError,
    SqlAuthoredPhysicalPlan, SqlCallDependencyLoan, SqlCallDependencyProvenance,
    SqlCallDependencySite, SqlCanonicalDependencyLoan, SqlExpressionCallKind,
    SqlSourceJournalError,
};
pub use crate::planner::distributed::build::{
    FragmentPackageSemantics, PackageSemanticsError, author_fragment_package_semantics,
    check_pure_call_definitions_observed,
};
#[cfg(test)]
pub(crate) use completion_driver::compile_authored_aggregate_for_test;
pub use emission_mode::SqlPhysicalEmissionMode;
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
    SqlMvDefinitionResolutionContext, SqlMvRelationOccurrenceId, SqlMvRewriteBaseTableFacts,
    SqlMvRewriteDefinitionFacts, SqlMvRewritePublicationInput, SqlMvRewritePublicationRelation,
    SqlMvRewriteSelectionFacts, SqlMvRewriteSourceOccurrenceFacts,
};

// The request profile has one owner; application test fixtures may author
// their explicit ceiling without a second default or translation vocabulary.
pub use novarocks_functions::ConstantPolicy;

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
/// Selected pure preparation borrows the same installed owner as analysis.
/// Mutable runtime instances remain outside this compiler-facing contract.
pub trait SqlFunctionCatalog: Send + Sync + std::fmt::Debug {
    /// Freeze an owned handle to exactly the immutable catalog used for
    /// analysis so optimizer rewrites cannot consult ambient state.
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog>;

    /// Candidate-only admission of the original selected call and its actual
    /// invocation lifecycle. Original catalogues perform no lookup or work.
    /// This does not prepare data or supply absent environment/source facts.
    fn admit_bound_lifecycle_observed(
        &self,
        _binding: &novarocks_functions::ResolvedFunctionBinding,
        _lifecycle: novarocks_functions::PureCallLifecycle,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        Ok(())
    }

    /// Actual same-source frozen environment admission. Original catalogues
    /// retain their original path without lookup, preparation or added work.
    fn admit_bound_environment_observed(
        &self,
        _binding: &novarocks_functions::ResolvedFunctionBinding,
        _environment: &[novarocks_type_contract::SemanticParameterRef],
        _parameters: &novarocks_type_contract::SemanticParameters,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        Ok(())
    }

    /// Borrow the original lexical call facts at the existing fold-parent frontier.
    /// Original catalogues do no work and construct no semantic table here.
    fn admit_authored_environment_observed(
        &self,
        _binding: &crate::binding::SqlFunctionBinding,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        Ok(())
    }

    /// Original catalogues retain the original intrinsic input domain. Only
    /// the explicit Exact request scope installs this source-shape admission.
    /// No query mode is inferred from a runtime value or constant payload.
    fn admit_native_bitnot_source_observed(
        &self,
        _source: &novarocks_functions::FunctionValueType,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        Ok(())
    }

    /// Retain this same immutable catalogue with candidate-only scalar presence
    /// admission. No preparation, data evaluation or supported-profile seal is
    /// granted. An already scoped snapshot preserves its one projection.
    fn snapshot_for_scalar_presence(&self) -> Arc<dyn SqlFunctionCatalog> {
        scalar_presence_catalog::scope(self.snapshot())
    }

    /// Instantiate only this exact installed identity and overload. This does
    /// not publish a final physical snapshot or grant call effects/capability.
    fn select_exact_overload_observed(
        &self,
        _function: &novarocks_functions::FunctionId,
        _kind: novarocks_functions::FunctionKind,
        _overload: &novarocks_functions::FunctionOverloadId,
        _request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        Arc<novarocks_functions::FunctionBindingSelection>,
        novarocks_functions::FunctionBindingError,
    > {
        let work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
        )?;
        work.finish()?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    /// Borrow the installed exact overload's base to author control domains.
    /// Full selected preparation must still validate/refine types, constants,
    /// environment and the actual implementation. Metadata-only snapshots
    /// refuse rather than substituting a declaration from a name lookup.
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        _function_id: &novarocks_functions::FunctionId,
        _kind: novarocks_functions::FunctionKind,
        _overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(novarocks_functions::FunctionSpecializationFailure::Control)?;
        Err(
            novarocks_functions::FunctionSpecializationFailure::InvalidInput(
                "SQL function snapshot has no installed pure overload declaration owner",
            ),
        )
    }

    /// Prepare the exact selected implementation from this immutable snapshot.
    /// There is no SQL name to resolve again and no declaration-only effect
    /// fallback. An adapter without an installed owner refuses the capability.
    fn prepare_fresh_selected(
        &self,
        _input: novarocks_functions::CallEffectInput<'_>,
        _selected: Arc<novarocks_functions::FunctionBindingSelection>,
        _options: novarocks_functions::PureCallPreparation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureCallSpecialization,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(novarocks_functions::FunctionSpecializationFailure::Control)?;
        Err(
            novarocks_functions::FunctionSpecializationFailure::InvalidInput(
                "SQL function snapshot has no installed selected preparation owner",
            ),
        )
    }

    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    >;

    fn resolve_scalar_binding(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    /// Explicit result syntax requires a declaring binder capability.
    fn resolve_scalar_binding_with_expected_result(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        _expected: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    fn resolve_value_conversion_binding(
        &self,
        _argument: &novarocks_functions::FunctionArgument,
        _target: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    fn resolve_window_binding(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    fn resolve_table_binding(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    fn contains_aggregate(&self, name: &str) -> bool;

    fn resolve_aggregate_binding(
        &self,
        _name: &str,
        _logical_argument_count: usize,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        Err(novarocks_functions::FunctionBindingError::MissingBindingDeclaration)
    }

    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.resolve_aggregate_binding(name, logical_argument_count, arguments, control)
    }

    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
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
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        control.checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )?;
        if logical_arg_types != update_arg_types {
            return Err(novarocks_functions::FunctionResolutionError::BadSignature(
                "function catalog does not support ordered aggregate update signatures".into(),
            ));
        }
        self.resolve_aggregate_signature(name, logical_arg_types, control)
    }

    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    >;

    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility;
}

pub use crate::common::expr::{BinOp, LiteralValue, UnOp};
mod fold_observation;
pub use fold_observation::{
    SqlFoldDependencyInput, SqlFoldDependencyObserver, SqlFoldDependencySource,
    SqlFoldEvaluationOutcome,
};
pub(crate) use fold_observation::{SqlFoldEvaluatorLoan, evaluate_fold_dependency_observed};

/// One scalar node shape the optimizer may ask a constant evaluator to fold.
///
/// The vocabulary is deliberately SQL-owned: it never names an execution
/// expression node, kernel, or arena handle. Growing it is a compiler-boundary
/// decision, not an execution-layer one.
#[derive(Clone, Debug, PartialEq)]
pub enum FoldNodeKind {
    BinaryOp(BinOp, novarocks_type_contract::DecimalOverflowPolicy),
    UnaryOp(UnOp),
    Cast(novarocks_type_contract::DecimalOverflowPolicy),
    Function { name: String },
}

/// One already-folded argument of a fold request.
#[derive(Clone, Debug)]
pub struct FoldArg {
    pub value: novarocks_functions::ConstantValue,
    /// The authored logical domain and exact Arrow field vocabulary travel
    /// together; a carrier alone cannot authorize a logical reinterpretation.
    pub value_type: novarocks_type_contract::FunctionValueType,
}

/// A single-node constant evaluation request.
///
/// Every argument is already a literal: recursion, volatility gating and the
/// foldable-shape policy stay on the SQL side, so an evaluator is a pure
/// per-node calculator with no traversal knowledge.
#[derive(Clone, Debug)]
pub struct FoldRequest {
    pub kind: FoldNodeKind,
    pub args: Vec<FoldArg>,
    /// Explicit host-admitted policy for the newly evaluated backing.
    pub constant_policy: novarocks_functions::ConstantPolicy,
    pub result_type: novarocks_type_contract::FunctionValueType,
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
/// - `Ok(Some(literal))` — folded; the literal has the complete type `request.result_type`.
/// - `Ok(None)` — evaluator declines (unmapped node shape or literal type).
/// - `Err(Evaluation(_))` — evaluation failed. Callers keep the original expression and
///   must never surface this as a planning error, because the runtime is
///   still allowed to produce a value or its own error for that expression.
/// - `Err(InvalidType(_))` — invalid frozen type facts; compilation stops.
/// - `Err(Preparation(_))` — a pure preparation failure; compilation stops.
///   Resource, internal and static failures cannot become optional row errors.
/// - `Err(Control(_))` — request cancellation, deadline or resource exhaustion;
///   compilation stops with that exact category, never an optimization decline.
// Design: ADR-0100 (docs/adr/ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md)
pub trait SqlConstantEvaluator: Send + Sync {
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>;

    /// Static selected-call admission at the original parent node, before any
    /// child is folded. The request loan supplies the actual immutable catalog;
    /// legacy calculators retain their original behavior by default.
    fn admit_fold_parent_observed(
        &self,
        _binding: &crate::binding::SqlFunctionBinding,
        _lifecycle: novarocks_functions::PureCallLifecycle,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        Ok(())
    }

    /// Optional observation before the original evaluator performs any work.
    /// An observer must admit and reserve its complete receipt storage here.
    /// A refusal aborts compilation with this exact control cause, without eval.
    /// Existing evaluators observe nothing and introduce no extra checkpoints.
    fn before_fold_dependency_observed(
        &self,
        _input: SqlFoldDependencyInput<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }

    /// Complete only the receipt admitted by the corresponding start callback.
    /// No allocation/admission, failure, binding, evaluation, or retry is allowed.
    /// Borrowed error text is complete; this hook cannot replace the primary cause.
    fn after_fold_dependency_observed(
        &self,
        _input: SqlFoldDependencyInput<'_>,
        _outcome: SqlFoldEvaluationOutcome<'_>,
    ) {
    }
}

/// Request failures stay distinct from a failed optional legacy evaluation.
/// Kernel strings are not interpreted to infer a control category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlConstantEvaluationError {
    Control(novarocks_type_contract::CompileControlError),
    Constant(novarocks_functions::ConstantError),
    InvalidType(novarocks_type_contract::ValueTypeError),
    Preparation(novarocks_functions::KernelFailure),
    Evaluation(String),
}

impl From<novarocks_type_contract::CompileControlError> for SqlConstantEvaluationError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl From<novarocks_functions::ConstantError> for SqlConstantEvaluationError {
    fn from(error: novarocks_functions::ConstantError) -> Self {
        match error {
            novarocks_functions::ConstantError::Control(error) => Self::Control(error),
            error => Self::Constant(error),
        }
    }
}

impl From<novarocks_type_contract::ValueTypeError> for SqlConstantEvaluationError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::InvalidType(error)
    }
}

impl From<novarocks_functions::KernelFailure> for SqlConstantEvaluationError {
    fn from(error: novarocks_functions::KernelFailure) -> Self {
        use novarocks_functions::KernelFailure;
        use novarocks_type_contract::CompileControlError;
        match error {
            KernelFailure::Cancelled => Self::Control(CompileControlError::Cancelled),
            KernelFailure::DeadlineExceeded => Self::Control(CompileControlError::DeadlineExceeded),
            KernelFailure::ResourceExhausted => {
                Self::Control(CompileControlError::ResourceExhausted)
            }
            other => Self::Preparation(other),
        }
    }
}

impl From<String> for SqlConstantEvaluationError {
    fn from(error: String) -> Self {
        Self::Evaluation(error)
    }
}

impl fmt::Display for SqlConstantEvaluationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::InvalidType(error) => error.fmt(f),
            Self::Preparation(error) => error.fmt(f),
            Self::Evaluation(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for SqlConstantEvaluationError {}

/// A constant evaluator that never folds.
///
/// Used by compiler paths that have no execution capability attached; the
/// folding rule degrades to a no-op rather than changing plan semantics.
#[derive(Debug, Default)]
struct NoopConstantEvaluator;

impl SqlConstantEvaluator for NoopConstantEvaluator {
    fn eval_scalar(
        &self,
        _request: &FoldRequest,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError> {
        let work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
        )?;
        work.finish()?;
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
    /// A DML-owned result read whose provider facts are completed by its
    /// statement owner before native encoding.
    DmlInternalRead,
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
    pub sql_semantics: crate::sql_mode::SqlSemanticSettings,
}

/// The execution environment selected for SQL planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlPlanningEnvironment {
    Distributed,
    NotApplicable,
}

/// Read-only request control observed by compiler phases.
#[derive(Clone)]
pub struct SqlCompileControl {
    deadline: Option<Instant>,
    cancellation: Arc<dyn SqlCancellationObservation>,
    fold_dependency_observer: Option<Arc<dyn SqlFoldDependencyObserver>>,
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
            fold_dependency_observer: None,
        }
    }

    /// Explicitly unbounded control for an already-admitted SQL logical
    /// transformation.  It is not an execution fallback: callers that have
    /// a request control must still pass its deadline and cancellation view.
    pub fn unbounded() -> Self {
        Self {
            deadline: None,
            cancellation: Arc::new(SqlNeverCancelled),
            fold_dependency_observer: None,
        }
    }

    /// Attach this request's observer; preserve its original control sources.
    /// The host owns admission. This attachment grants no capacity.
    pub fn with_fold_dependency_observer(
        mut self,
        observer: Arc<dyn SqlFoldDependencyObserver>,
    ) -> Self {
        self.fold_dependency_observer = Some(observer);
        self
    }
    pub(crate) fn fold_dependency_observer(&self) -> Option<&Arc<dyn SqlFoldDependencyObserver>> {
        self.fold_dependency_observer.as_ref()
    }
    pub(crate) fn check(&self) -> Result<(), SqlCompileError> {
        novarocks_type_contract::PureCompileControl::checkpoint(
            self,
            novarocks_type_contract::CompilePhase::Validate,
            0,
        )
        .map_err(Self::compile_error)
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    fn compile_error(error: novarocks_type_contract::CompileControlError) -> SqlCompileError {
        match error {
            novarocks_type_contract::CompileControlError::Cancelled => SqlCompileError::Cancelled,
            novarocks_type_contract::CompileControlError::DeadlineExceeded => {
                SqlCompileError::DeadlineExceeded
            }
            novarocks_type_contract::CompileControlError::ResourceExhausted => {
                SqlCompileError::ResourceExhausted
            }
        }
    }
}

impl novarocks_type_contract::PureCompileControl for SqlCompileControl {
    fn checkpoint(
        &self,
        _phase: novarocks_type_contract::CompilePhase,
        _work_units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        use novarocks_type_contract::CompileControlError;
        // Request control observes the existing cancellation/deadline source.
        // Structural/work admission remains with the operation's policy; no
        // independent optimizer or constant-factory wallet is created here.
        if self.cancellation.is_cancelled() {
            return Err(CompileControlError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(CompileControlError::DeadlineExceeded);
        }
        Ok(())
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
    pub(crate) emission_mode: SqlPhysicalEmissionMode,
    pub(crate) catalog: Option<&'a dyn SqlCatalogSnapshot>,
    pub(crate) functions: Option<&'a dyn SqlFunctionCatalog>,
    pub(crate) owned_functions: Option<Arc<dyn SqlFunctionCatalog>>,
    pub(crate) constant_evaluator: Option<&'static dyn SqlConstantEvaluator>,
    pub(crate) constant_policy: novarocks_functions::ConstantPolicy,
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
        constant_policy: novarocks_functions::ConstantPolicy,
        emission_mode: SqlPhysicalEmissionMode,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement,
            intent,
            session,
            environment,
            emission_mode,
            catalog: Some(catalog),
            functions: Some(functions),
            owned_functions: None,
            constant_evaluator: Some(constant_evaluator),
            constant_policy,
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
        constant_policy: novarocks_functions::ConstantPolicy,
        emission_mode: SqlPhysicalEmissionMode,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            statement: SqlStatementInput::logical_plan(plan, factory),
            intent,
            session,
            environment,
            emission_mode,
            catalog: None,
            functions: None,
            owned_functions: None,
            constant_evaluator,
            constant_policy,
            mv_rewrite: None,
            imv_rewrite: None,
            control,
        }
    }

    fn freeze_scalar_presence_scope(&mut self) {
        if self.emission_mode == SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration {
            // Retain the same original catalogue after the original entry control check.
            let scoped = self
                .function_catalog()
                .map(|functions| functions.snapshot_for_scalar_presence());
            self.functions = None;
            self.owned_functions = scoped;
        }
    }

    pub(crate) fn check_control(&self) -> Result<(), SqlCompileError> {
        self.control.check()
    }

    /// Borrow the original request control for subsequent diagnostics.
    pub fn control(&self) -> &SqlCompileControl {
        &self.control
    }

    pub(crate) fn with_function_catalog(mut self, functions: Arc<dyn SqlFunctionCatalog>) -> Self {
        self.owned_functions = Some(functions);
        self
    }

    fn function_catalog(&self) -> Option<&dyn SqlFunctionCatalog> {
        self.functions.or(self.owned_functions.as_deref())
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
    /// Policy of the admitted root scope for calls synthesized after analysis.
    /// Existing calls retain their own scope policy in `SqlFunctionBinding`.
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    /// The admitted root option, independent of lexical decimal policies.
    root_allow_throw_exception: bool,
    change_stream: crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    mv_rewrite: mv_rewrite::SqlMvRewriteAnalysis,
    function_catalog: Arc<dyn SqlFunctionCatalog>,
    /// Carried from the analyze request because folding runs in the optimize
    /// phase, which outlives the analyze request borrow.
    constant_evaluator: Option<&'static dyn SqlConstantEvaluator>,
    constant_policy: novarocks_functions::ConstantPolicy,
}

impl SqlAnalyzedQuery {
    pub(crate) fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }
    pub(crate) fn root_allow_throw_exception(&self) -> bool {
        self.root_allow_throw_exception
    }

    pub(crate) fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
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
    control: SqlCompileControl,
}

impl<'a> SqlOptimizeRequest<'a> {
    pub fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.analyzed.constant_policy
    }

    pub fn root_allow_throw_exception(&self) -> bool {
        self.analyzed.root_allow_throw_exception()
    }

    pub fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        self.analyzed.decimal_overflow_policy()
    }

    pub(crate) fn control(&self) -> &SqlCompileControl {
        &self.control
    }

    pub fn new(
        analyzed: SqlAnalyzedQuery,
        statistics: &'a crate::planning::dml::DmlStatisticsSnapshot,
        control: SqlCompileControl,
    ) -> Self {
        Self {
            analyzed,
            statistics,
            control,
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
    pub(crate) root_allow_throw_exception: bool,
    pub(crate) root_semantics: root_output::RootOutputSemantics,
    pub(crate) optimized_tree: crate::optimizer::OptimizedOperatorNode,
    pub(crate) function_catalog: Arc<dyn SqlFunctionCatalog>,
    pub(crate) statistics: SqlStatisticsPlan,
    pub(crate) change_stream: crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    pub(crate) mv_rewrite_diagnostics: Vec<mv_rewrite::SqlMvRewriteDiagnostic>,
}

/// SQL-owned compiler facts. Native DTOs/bytes, lifecycle state and result
/// buffers are intentionally absent; application owns post-compile assembly.
///
/// The carrier is intentionally opaque outside SQL. Its public terminal
/// renders logical EXPLAIN lines; final-plan completion consumes optimized
/// facts inside SQL.
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
}

/// Immutable application facts for rendering one IMV refresh EXPLAIN result.
/// SQL consumes the opaque rewrite snapshot and keeps logical-plan ownership
/// internal; Core receives only the rendered lines.
pub struct SqlImvRefreshExplainContext<'a> {
    pub emission_mode: SqlPhysicalEmissionMode,
    pub canonical_query: Box<novarocks_parser::ast::Query>,
    pub imv_rewrite: SqlImvPlanningInput,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub optimizer_settings: SessionOptimizerSettings,
    pub environment: SqlPlanningEnvironment,
    pub catalog: &'a dyn SqlCatalogSnapshot,
    pub functions: &'a dyn SqlFunctionCatalog,
    pub constant_evaluator: &'static dyn SqlConstantEvaluator,
    pub constant_policy: novarocks_functions::ConstantPolicy,
    pub control: SqlCompileControl,
    pub level: ExplainLevel,
}

/// Immutable input for SQL-owned MV query analysis. The application may copy
/// parsed syntax and its frozen catalog snapshot in, but receives only the
/// opaque analyzed-MV carrier back.
pub struct SqlMvRefreshAnalysisContext<'a> {
    pub emission_mode: SqlPhysicalEmissionMode,
    pub query: Box<novarocks_parser::ast::Query>,
    pub current_database: String,
    pub catalog: &'a dyn SqlCatalogSnapshot,
    pub functions: &'a dyn SqlFunctionCatalog,
    pub constant_policy: novarocks_functions::ConstantPolicy,
    pub control: SqlCompileControl,
}

/// Analyze a prepared MV query without exposing analyzer nodes, CTE state, or
/// the column-id factory to application code.
pub fn analyze_mv_refresh_input(
    context: SqlMvRefreshAnalysisContext<'_>,
) -> Result<crate::planning::mv::SqlResolvedMvRefreshInput, SqlCompileError> {
    let SqlMvRefreshAnalysisContext {
        emission_mode,
        query,
        current_database,
        catalog,
        functions,
        constant_policy,
        control,
    } = context;
    control.check()?;
    // Keep typed analysis/control failures intact at the compiler boundary.
    crate::sql_mode::validate_persisted_query_semantics(
        &query,
        &crate::sql_mode::SqlSemanticSettings::default(),
    )
    .map_err(SqlCompileError::Compilation)?;
    crate::planning::mv::validate_imv_aggregate_star_arguments(&query)
        .map_err(SqlCompileError::from)?;
    let scoped_functions = match emission_mode {
        SqlPhysicalEmissionMode::OriginalNativeV1 => None,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration => {
            Some(functions.snapshot_for_scalar_presence())
        }
    };
    let functions = scoped_functions.as_deref().unwrap_or(functions);
    let (resolved, _, factory) = crate::analyzer::analyze_with_function_catalog(
        &query,
        catalog.planner_table_provider(),
        &current_database,
        functions,
        constant_policy,
        &control,
    )
    .map_err(SqlCompileError::from)?;
    crate::planning::mv::validate_persistable_output(&resolved, &factory)
        .map_err(SqlCompileError::Compilation)?;
    Ok(crate::planning::mv::SqlResolvedMvRefreshInput::from_analysis(resolved))
}

/// Compile and render an IMV refresh EXPLAIN request without exposing its
/// logical plan, rewrite trace, or column factory to application code.
pub fn compile_imv_refresh_explain_lines(
    context: SqlImvRefreshExplainContext<'_>,
) -> Result<Vec<String>, SqlCompileError> {
    let SqlImvRefreshExplainContext {
        emission_mode,
        canonical_query,
        imv_rewrite,
        current_catalog,
        current_database,
        optimizer_settings,
        environment,
        catalog,
        functions,
        constant_evaluator,
        constant_policy,
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
        constant_policy,
        emission_mode,
        control,
    );
    let explain_control = request.control().clone();
    let output = SqlCompiler::analyze(request)?.into_complete()?;
    output.into_explain_lines(level, true, &explain_control)
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
    constant_policy: novarocks_functions::ConstantPolicy,
    emission_mode: SqlPhysicalEmissionMode,
    control: SqlCompileControl,
) -> SqlAnalyzeRequest<'a> {
    SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::LogicalOnly,
        SqlSessionContext {
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
            current_catalog,
            current_database,
            optimizer_settings,
        },
        environment,
        catalog,
        functions,
        constant_evaluator,
        None,
        constant_policy,
        emission_mode,
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
    fn is_optimized(&self) -> bool {
        matches!(self.kind, SqlCompileOutputKind::Optimized(_))
    }

    /// Render an EXPLAIN result without exposing the compiler-private logical
    /// plan tree to application code.
    pub fn into_explain_lines(
        self,
        level: ExplainLevel,
        logical: bool,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<String>, SqlCompileError> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::LowerProgram,
        )?;
        let result = match self.kind {
            SqlCompileOutputKind::Logical(output) if logical => {
                work.step()?;
                work.flush()?;
                crate::explain::explain_plan_checked(&output.logical_plan, level, control)
            }
            _ => {
                work.step()?;
                Err(SqlCompileError::InvalidRequest(
                    "EXPLAIN intent produced unexpected SQL facts".to_string(),
                ))
            }
        };
        if matches!(
            &result,
            Err(SqlCompileError::Cancelled
                | SqlCompileError::DeadlineExceeded
                | SqlCompileError::ResourceExhausted)
        ) {
            return result;
        }
        work.finish()?;
        result
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlCompileError {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    InvalidRequest(String),
    Analyze(AnalyzeError),
    Compilation(String),
}

impl From<String> for SqlCompileError {
    fn from(error: String) -> Self {
        Self::Compilation(error)
    }
}

impl From<novarocks_functions::FunctionBindingError> for SqlCompileError {
    fn from(error: novarocks_functions::FunctionBindingError) -> Self {
        match error {
            novarocks_functions::FunctionBindingError::Control(error) => error.into(),
            error @ novarocks_functions::FunctionBindingError::UnavailableImplementation(_) => {
                Self::Analyze(AnalyzeError::function_binding(error))
            }
            other => Self::Compilation(other.to_string()),
        }
    }
}

impl From<novarocks_functions::ConstantError> for SqlCompileError {
    fn from(error: novarocks_functions::ConstantError) -> Self {
        match error {
            novarocks_functions::ConstantError::Control(error) => error.into(),
            novarocks_functions::ConstantError::Limit(_) => Self::ResourceExhausted,
            novarocks_functions::ConstantError::Type(error) => {
                Self::InvalidRequest(error.to_string())
            }
            error => Self::Compilation(error.to_string()),
        }
    }
}

impl From<novarocks_type_contract::CompileControlError> for SqlCompileError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        SqlCompileControl::compile_error(error)
    }
}

impl From<AnalyzeError> for SqlCompileError {
    fn from(error: AnalyzeError) -> Self {
        match error.control_error() {
            Some(error) => SqlCompileControl::compile_error(error),
            None => Self::Analyze(error),
        }
    }
}

impl std::fmt::Display for SqlCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("SQL compilation was cancelled"),
            Self::DeadlineExceeded => f.write_str("SQL compilation deadline exceeded"),
            Self::ResourceExhausted => f.write_str("SQL compilation resource budget was exhausted"),
            Self::InvalidRequest(error) | Self::Compilation(error) => f.write_str(error),
            Self::Analyze(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SqlCompileError {}

/// Keep legacy runtime ALLOW_THROW semantics out of stateless numeric folding.
/// The no-op port still permits structural reductions such as Nested(literal).
/// This choice only controls optimization; it neither changes nor transports the
/// separately frozen legacy runtime option.
pub(crate) fn constant_evaluator_for_legacy_mode(
    evaluator: Option<&'static dyn SqlConstantEvaluator>,
    allow_throw_exception: bool,
) -> Option<&'static dyn SqlConstantEvaluator> {
    if allow_throw_exception {
        evaluator.map(|_| noop_constant_evaluator())
    } else {
        evaluator
    }
}

/// The canonical two-phase SQL compiler. Catalog materialization belongs only
/// to [`SqlCompiler::analyze`]; optimization and plan sealing belong only to
/// [`SqlCompiler::optimize`] after the application freezes statistics.
pub struct SqlCompiler;

pub use crate::explain::completed::{
    ExplainRenderBudget, SqlCompletedExplainProfile, SqlExplainFragmentMetrics, SqlExplainNodeKey,
    SqlExplainObservation, SqlExplainOperatorMetrics, SqlExplainUnavailableReason,
    render_completed_plan,
};
pub use crate::explain::completed_tree::render_completed_plan_tree;
pub use completion_driver::SqlFinalPlanCompileRequest;
pub(crate) use completion_driver::{
    FinalizedProviderRead, FinalizedProviderReadSet, collect_provider_needs,
};
use completion_driver::{
    SqlCatalogCompletionState, SqlMaterializedViewCompletionState, SqlProviderReadCompletionState,
    SqlStatisticsCompletionState, resume_catalog, resume_materialized_view, resume_provider_read,
    resume_statistics,
};

impl SqlCompiler {
    pub fn analyze(
        mut request: SqlAnalyzeRequest<'_>,
    ) -> Result<SqlAnalyzeOutput, SqlCompileError> {
        request.check_control()?;
        request.freeze_scalar_presence_scope();
        let (
            mut logical_plan,
            mut factory,
            logical_input,
            consumer_requires_semantic_snapshot,
            legacy_allow_throw_exception,
            decimal_overflow_policy,
        ) = match &request.statement.kind {
            SqlStatementInputKind::LogicalPlan { plan, factory } => (
                plan.clone(),
                factory.clone(),
                true,
                (request
                    .session
                    .sql_semantics
                    .sql_mode()
                    .group_concat_legacy()
                    || request.session.sql_semantics.decimal_overflow_to_double()
                    || request.session.sql_semantics.sql_mode().error_if_overflow()),
                request
                    .session
                    .sql_semantics
                    .sql_mode()
                    .allow_throw_exception(),
                request
                    .session
                    .sql_semantics
                    .sql_mode()
                    .decimal_overflow_policy(),
            ),
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
                let (resolved, ctes, mut factory) =
                    crate::analyzer::analyze_with_function_catalog_and_sql_semantics(
                        &query,
                        catalog,
                        &request.session.current_database,
                        functions,
                        &request.session.sql_semantics,
                        request.constant_policy,
                        &request.control,
                    )
                    .map_err(SqlCompileError::from)?;
                request.check_control()?;
                let logical_plan =
                    crate::planner::plan_query(resolved, ctes, &mut factory, &request.control)?;
                let consumer_requires_semantic_snapshot =
                    crate::sql_mode::query_uses_group_concat_legacy(
                        &request.session.sql_semantics,
                        &query,
                    )
                    .map_err(SqlCompileError::from)?
                        || crate::sql_mode::query_uses_decimal_overflow_to_double(
                            &request.session.sql_semantics,
                            &query,
                        )
                        .map_err(SqlCompileError::from)?
                        || crate::sql_mode::query_uses_error_if_overflow(
                            &request.session.sql_semantics,
                            &query,
                        )
                        .map_err(SqlCompileError::from)?;
                (
                    logical_plan,
                    factory,
                    false,
                    consumer_requires_semantic_snapshot,
                    crate::sql_mode::query_sql_semantics(&request.session.sql_semantics, &query)
                        .map_err(SqlCompileError::from)?
                        .sql_mode()
                        .allow_throw_exception(),
                    crate::sql_mode::query_sql_semantics(&request.session.sql_semantics, &query)
                        .map_err(SqlCompileError::from)?
                        .sql_mode()
                        .decimal_overflow_policy(),
                )
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
        let settings = request.session.optimizer_settings.clone();
        if !matches!(request.intent, SqlCompileIntent::LogicalOnly)
            && (!logical_input
                || !matches!(request.environment, SqlPlanningEnvironment::NotApplicable))
        {
            validate_planning_environment(request.environment)?;
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
                    decimal_overflow_policy,
                    constant_policy: request.constant_policy,
                    control: &request.control,
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
            )?;
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
                request.constant_policy,
                &request.control,
                consumer_requires_semantic_snapshot,
            )?
        } else {
            mv_rewrite::SqlMvRewriteAnalysis::empty(request.constant_policy)
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
            decimal_overflow_policy,
            root_allow_throw_exception: legacy_allow_throw_exception,
            change_stream,
            mv_rewrite,
            function_catalog,
            constant_policy: request.constant_policy,
            constant_evaluator: constant_evaluator_for_legacy_mode(
                request.constant_evaluator,
                legacy_allow_throw_exception,
            ),
        }))
    }

    pub fn optimize(request: SqlOptimizeRequest<'_>) -> Result<SqlCompileOutput, SqlCompileError> {
        let SqlAnalyzedQuery {
            logical_plan,
            factory,
            intent,
            settings,
            decimal_overflow_policy,
            root_allow_throw_exception,
            change_stream,
            mv_rewrite,
            function_catalog,
            constant_evaluator,
            constant_policy,
        } = request.analyzed;
        let control = request.control;
        control.check()?;
        let root_semantics = root_output::RootOutputSemantics::capture(
            &crate::planner::plan_output_columns(&logical_plan)
                .map_err(SqlCompileError::Compilation)?,
            &factory,
        )
        .map_err(SqlCompileError::Compilation)?;
        let mut scalar_arena =
            crate::optimizer::scalar::ScalarArena::with_constant_policy(constant_policy);
        let mut optimizer_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
            &logical_plan,
            &mut scalar_arena,
            &control,
        )?;
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
            constant_policy: mv_constant_policy,
        } = mv_rewrite;
        if mv_constant_policy != constant_policy {
            return Err(SqlCompileError::InvalidRequest(
                "MV constant policy differs from its analyzed request".into(),
            ));
        }
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
                    decimal_overflow_policy,
                    &control,
                )
                .with_fold_dependency_observer(control.fold_dependency_observer().cloned()),
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
                    decimal_overflow_policy,
                    &control,
                )
                .with_fold_dependency_observer(control.fold_dependency_observer().cloned()),
            ),
        }?;
        control.check()?;

        root_semantics
            .domains(&optimized_tree.output_columns)
            .map_err(SqlCompileError::Compilation)?;
        Ok(SqlCompileOutput::optimized(SqlOptimizedOutput {
            root_allow_throw_exception,
            root_semantics,
            optimized_tree,
            function_catalog,
            statistics,
            change_stream,
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
        // A writer consumes the distributed source directly. Falling back to
        // the query default would gather rows before the TableWriter.
        RootDistributionRequirement::Any => {
            return Ok(Some(crate::optimizer::property::DistributionSpec::Any));
        }
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

fn validate_planning_environment(
    environment: SqlPlanningEnvironment,
) -> Result<(), SqlCompileError> {
    match environment {
        SqlPlanningEnvironment::Distributed => Ok(()),
        SqlPlanningEnvironment::NotApplicable => Err(SqlCompileError::InvalidRequest(
            "distributed SQL compilation requires a distributed planning environment".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use std::time::{Duration, Instant};

    use super::*;

    struct Catalog;
    impl crate::catalog::PlannerTableProvider for Catalog {
        fn resolve_table_for_analysis(
            &self,
            _catalog: Option<&str>,
            _database: &str,
            _table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            Err("control tests must not resolve a table".to_string())
        }
    }
    impl SqlCatalogSnapshot for Catalog {
        fn planner_table_provider(&self) -> &dyn crate::catalog::PlannerTableProvider {
            self
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
            _control: &dyn novarocks_type_contract::PureCompileControl,
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
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<novarocks_functions::ResolvedAggregateSignature, crate::functions::ResolveError>
        {
            Err(crate::functions::ResolveError::UnknownFunction)
        }

        fn resolve_aggregate_trusted(
            &self,
            _name: &str,
            _arg_types: &[arrow::datatypes::DataType],
            _control: &dyn novarocks_type_contract::PureCompileControl,
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

    #[test]
    fn sql_request_control_is_the_pure_control_observation() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        let cancellation = Arc::new(Cancellation::default());
        let request_control = control(None, &cancellation);
        for units in [0, 256] {
            request_control
                .checkpoint(CompilePhase::FunctionSpecialization, units)
                .unwrap();
        }
        cancellation.request();
        assert_eq!(
            request_control.checkpoint(CompilePhase::Validate, 256),
            Err(CompileControlError::Cancelled)
        );
        assert_eq!(request_control.check(), Err(SqlCompileError::Cancelled));
        let active = Arc::new(Cancellation::default());
        let expired = control(Some(Instant::now() - Duration::from_secs(1)), &active);
        assert_eq!(
            expired.checkpoint(CompilePhase::Validate, 0),
            Err(CompileControlError::DeadlineExceeded)
        );
        assert_eq!(expired.check(), Err(SqlCompileError::DeadlineExceeded));
    }

    #[test]
    fn analyzer_control_keeps_all_outer_categories_and_semantic_errors() {
        use novarocks_type_contract::CompileControlError;
        for (cause, expected) in [
            (CompileControlError::Cancelled, SqlCompileError::Cancelled),
            (
                CompileControlError::DeadlineExceeded,
                SqlCompileError::DeadlineExceeded,
            ),
            (
                CompileControlError::ResourceExhausted,
                SqlCompileError::ResourceExhausted,
            ),
        ] {
            let error = AnalyzeError::control(cause);
            assert_eq!(error.control_error(), Some(cause));
            assert!(error.span().is_none());
            assert_eq!(SqlCompileError::from(error), expected);
        }
        let error = AnalyzeError::internal("ordinary analysis invariant");
        assert_eq!(error.control_error(), None);
        assert_eq!(
            SqlCompileError::from(error.clone()),
            SqlCompileError::Analyze(error)
        );
    }

    fn request(control: SqlCompileControl) -> SqlAnalyzeRequest<'static> {
        SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &CATALOG,
            &FUNCTIONS,
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            control,
        )
    }

    fn analyze_then_optimize(
        request: SqlAnalyzeRequest<'_>,
    ) -> Result<SqlCompileOutput, SqlCompileError> {
        let control = request.control.clone();
        let analyzed = SqlCompiler::analyze(request)?.into_pending()?;
        SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &STATISTICS, control))
    }

    fn table_request<'a>(
        catalog: &'a CountingTableCatalog,
        control: SqlCompileControl,
    ) -> SqlAnalyzeRequest<'a> {
        SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select order_id from orders"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            catalog,
            &FUNCTIONS,
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
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
        assert_eq!(request.environment, SqlPlanningEnvironment::Distributed);
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
        let output = SqlCompiler::optimize(SqlOptimizeRequest::new(
            analyzed,
            &statistics,
            SqlCompileControl::unbounded(),
        ))
        .expect("typed Missing is conservative, not fatal");
        assert!(output.is_optimized());
        assert_eq!(
            catalog.resolution_count(),
            1,
            "phase two must not resolve catalog tables"
        );
    }

    #[test]
    fn analyzed_query_does_not_retain_runtime_control() {
        let cancellation = Arc::new(Cancellation::default());
        let weak = Arc::downgrade(&cancellation);
        let analyzed = SqlCompiler::analyze(request(control(None, &cancellation)))
            .expect("analysis completes")
            .into_pending()
            .expect("query requires optimization");

        drop(cancellation);

        assert!(
            weak.upgrade().is_none(),
            "the analyzed value must not retain the cancellation capability"
        );
        drop(analyzed);
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
            SqlCompileControl::unbounded(),
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

            let error = match SqlCompiler::optimize(SqlOptimizeRequest::new(
                analyzed,
                &statistics,
                SqlCompileControl::unbounded(),
            )) {
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
            SqlCompiler::optimize(SqlOptimizeRequest::new(
                analyzed,
                &STATISTICS,
                control(None, &cancellation),
            )),
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
            SqlCompiler::optimize(SqlOptimizeRequest::new(
                analyzed,
                &STATISTICS,
                control(Some(deadline), &cancellation),
            )),
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
    fn ordinary_explain_optimization_requires_final_completion() {
        let cancellation = Arc::new(Cancellation::default());
        let mut request = request(control(None, &cancellation));
        request.intent = SqlCompileIntent::Explain {
            level: ExplainLevel::Normal,
            analyze: false,
        };
        let output = analyze_then_optimize(request).expect("compile ordinary EXPLAIN");
        assert!(output.is_optimized());
        let _: fn(
            SqlCompileOutput,
            ExplainLevel,
            bool,
            &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Vec<String>, SqlCompileError> = SqlCompileOutput::into_explain_lines;
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
            crate::constant::test_constant_policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
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
    fn mv_refresh_rejects_stored_legacy_hint_before_table_analysis() {
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog = SqlPlannerTableSnapshot::new(&catalog);
        let functions = crate::functions::build_builtin_engine_function_catalog().unwrap();
        let error = analyze_mv_refresh_input(SqlMvRefreshAnalysisContext {
            emission_mode: SqlPhysicalEmissionMode::OriginalNativeV1,
            constant_policy: crate::constant::test_constant_policy(),
            query: mv_analysis_query(
                "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ k FROM missing_table",
            ),
            current_database: "db".to_string(),
            catalog: &catalog,
            functions: &functions,
            control: SqlCompileControl::unbounded(),
        })
        .err()
        .expect("unsupported persisted semantics");
        assert!(error.to_string().starts_with("Unsupported:"));
        assert!(error.to_string().contains("GROUP_CONCAT_LEGACY"));
        assert!(
            !error.to_string().contains("missing_table"),
            "guard precedes analyzer table materialization"
        );
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
            emission_mode: SqlPhysicalEmissionMode::OriginalNativeV1,
            constant_policy: crate::constant::test_constant_policy(),
            query: mv_analysis_query("SELECT order_id FROM orders"),
            current_database: "db".to_string(),
            catalog: &catalog,
            functions: &functions,
            control: SqlCompileControl::unbounded(),
        })
        .expect("analyze MV query through opaque terminal");

        let facts = input.analysis_facts();
        assert_eq!(facts.output_columns.len(), 1);
        assert_eq!(facts.output_columns[0].name, "order_id");
        let _: fn(
            SqlMvRefreshAnalysisContext<'_>,
        )
            -> Result<crate::planning::mv::SqlResolvedMvRefreshInput, SqlCompileError> =
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
            emission_mode: SqlPhysicalEmissionMode::OriginalNativeV1,
            constant_policy: crate::constant::test_constant_policy(),
            query: mv_analysis_query("SELECT order_id FROM missing_orders"),
            current_database: "db".to_string(),
            catalog: &catalog,
            functions: &functions,
            control: SqlCompileControl::unbounded(),
        })
        .expect_err("unregistered MV table must not analyze");
        assert!(
            error.to_string().contains("missing_orders"),
            "missing-table error must retain SQL analyzer context: {error}"
        );
    }

    #[test]
    fn array_agg_json_semantic_schema_survives_optimizer_and_exact_final_plan() {
        use arrow::datatypes::DataType;
        use novarocks_types::logical::{LogicalType, logical_type_of_field};
        for sql in [
            "select array_agg(json_object('2:3')) as j, array_agg(cast(json_object('2:3') as varchar)) as s",
            "with q as (select cast(json_object('k',1) as json) as j) select array_agg(j) as j, array_agg(cast(j as varchar)) as s from q",
            "select array_sortby([json_object('k',1), json_object('k',2)],[2,1]) as j, array_sortby(cast([json_object('k',1), json_object('k',2)] as array<varchar>),[2,1]) as s",
            "with q as (select [json_object('k',1),json_object('k',2)] as j) select array_sortby(j,[2,1]) as j, array_sortby(cast(j as array<varchar>),[2,1]) as s from q",
        ] {
            let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
            let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
            let cancellation = Arc::new(Cancellation::default());
            let request = SqlAnalyzeRequest::new(
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                    current_catalog: None,
                    current_database: "default".into(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                &catalog_snapshot,
                crate::functions::builtin_sql_function_catalog(),
                noop_constant_evaluator(),
                None,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                control(None, &cancellation),
            );
            let optimized = analyze_then_optimize(request)
                .unwrap()
                .into_optimized_output()
                .unwrap();
            let physical =
                crate::planner::optimizer_bridge::to_physical_plan(&optimized.optimized_tree)
                    .unwrap();
            let plan = crate::planner::distributed::build::lower_final_physical_plan(
                &physical,
                novarocks_physical_plan::PlanVersionId::try_new([41; 16]).unwrap(),
                novarocks_physical_plan::PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                optimized.function_catalog,
                optimized.root_allow_throw_exception,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()
            .finish_observed(&crate::compiler::SqlCompileControl::unbounded())
            .unwrap();
            let result = plan.plan().result_port().unwrap();
            assert_eq!(result.fields.len(), 2);
            let DataType::List(json_item) = &result.fields[0].ty.data_type else {
                panic!("expected JSON List");
            };
            let DataType::List(string_item) = &result.fields[1].ty.data_type else {
                panic!("expected STRING List");
            };
            assert_eq!(logical_type_of_field(json_item), Some(LogicalType::Json));
            assert_eq!(logical_type_of_field(string_item), None);
            let field = arrow::datatypes::Field::new(
                "j",
                result.fields[0].ty.data_type.clone(),
                result.fields[0].ty.nullable,
            );
            assert!(
                novarocks_types::FieldRenderSchema::from_field(&field)
                    .list_item()
                    .unwrap()
                    .is_json_value()
            );
            let mut aggregate_count = 0;
            for fragment in plan.plan().fragments().values() {
                for node in fragment.nodes().values() {
                    if let novarocks_physical_plan::NodeKind::Aggregate { calls, .. } = &node.kind {
                        for call in calls {
                            aggregate_count += 1;
                            assert_eq!(
                                call.binding.function.function_id.as_str(),
                                "builtin.aggregate/array_agg/v1"
                            );
                            let DataType::List(item) = &call.binding.function.result_type.data_type
                            else {
                                panic!("expected physical aggregate List");
                            };
                            let novarocks_functions::FunctionArgumentType::Value(input) =
                                &call.binding.function.argument_types[0]
                            else {
                                panic!("aggregate input must be a value");
                            };
                            let expected = (input.logical_type
                                == novarocks_type_contract::ValueLogicalType::Json)
                                .then_some(LogicalType::Json);
                            assert_eq!(logical_type_of_field(item), expected);
                            let DataType::List(item) = &call.binding.intermediate_type.data_type
                            else {
                                panic!("expected physical state List");
                            };
                            assert_eq!(logical_type_of_field(item), None);
                        }
                    }
                }
            }
            if sql.contains("array_agg") {
                assert!(aggregate_count >= 1);
            }
        }
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
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &catalog_snapshot,
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            control(None, &cancellation),
        );

        assert!(
            analyze_then_optimize(request)
                .expect("query compile")
                .is_optimized()
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
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &catalog_snapshot,
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            control(None, &cancellation),
        );

        assert!(
            analyze_then_optimize(request)
                .expect("query compile")
                .is_optimized()
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
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &catalog_snapshot,
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            control(None, &cancellation),
        );
        assert!(matches!(
            analyze_then_optimize(request),
            Err(SqlCompileError::InvalidRequest(error)) if error.contains("output column 'missing' not found")
        ));
    }

    #[test]
    fn iceberg_write_any_root_does_not_gather_before_the_writer() {
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let cancellation = Arc::new(Cancellation::default());
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select 1 as payload"),
            SqlCompileIntent::IcebergWrite {
                root_distribution: RootDistributionRequirement::Any,
            },
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &catalog_snapshot,
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            control(None, &cancellation),
        );
        let optimized = analyze_then_optimize(request)
            .expect("write source compiles")
            .into_optimized_output()
            .expect("write source is optimized");
        let physical =
            crate::planner::optimizer_bridge::to_physical_plan(&optimized.optimized_tree)
                .expect("write source lowers");
        assert!(
            !matches!(
                physical.kind,
                crate::planner::physical::PhysicalPlanKind::Redistribute(
                    crate::planner::physical::RedistributeNode {
                        mode: crate::planner::physical::RedistributeMode::Gather,
                        ..
                    }
                )
            ),
            "an Iceberg writer must receive the source before a query-result gather"
        );
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
    #[test]
    fn field_common_type_survives_physical_planning() {
        use crate::analysis::{ExprKind, TypedExpr};
        use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
        use arrow::datatypes::DataType;
        fn find_field(plan: &PhysicalPlanNode) -> Option<&TypedExpr> {
            let expressions = match &plan.kind {
                PhysicalPlanKind::Project(project) => project
                    .items
                    .iter()
                    .map(|item| &item.expr)
                    .collect::<Vec<_>>(),
                PhysicalPlanKind::Values(values) => values.rows.iter().flatten().collect(),
                _ => Vec::new(),
            };
            expressions.into_iter().find(|expr| matches!(&expr.kind, ExprKind::FunctionCall { name, .. } if name == "field"))
                .or_else(|| plan.children.iter().find_map(find_field))
        }
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let request = SqlAnalyzeRequest::new(
            SqlStatementInput::sql("select field('01', '1', 1) as ordinal"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: None,
                current_database: "default".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            crate::functions::builtin_sql_function_catalog(),
            noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            SqlCompileControl::unbounded(),
        );
        let optimized = analyze_then_optimize(request)
            .unwrap()
            .into_optimized_output()
            .unwrap();
        let physical =
            crate::planner::optimizer_bridge::to_physical_plan(&optimized.optimized_tree).unwrap();
        let field = find_field(&physical)
            .expect("FIELD must remain typed when constant evaluation is disabled");
        let ExprKind::FunctionCall { args, binding, .. } = &field.kind else {
            unreachable!()
        };
        assert!(
            args.iter()
                .all(|arg| arg.value_type.data_type == DataType::Float64)
        );
        assert!(binding.selected.argument_types.iter().all(|arg| matches!(arg,
            novarocks_functions::FunctionArgumentType::Value(value) if value.data_type == DataType::Float64)));
        assert_eq!(field.value_type.data_type, DataType::Int32);
        assert!(!field.value_type.nullable);
    }

    #[test]
    fn slice_calls_retain_fixed_temporal_types_in_strict_final_plan() {
        use arrow::datatypes::{DataType, TimeUnit};
        for (sql, expected) in [
            (
                "select time_slice('2023-12-31 03:12:04',interval 2147483647 year) as sliced",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "select time_slice(cast('2020-01-01' as date),interval 1 day,ceil) as sliced",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "select date_slice('2020-01-01 12:00:00',interval 1 day) as sliced",
                DataType::Date32,
            ),
            (
                "select date_slice(cast('2020-01-01' as datetime),1,'month','ceil') as sliced",
                DataType::Date32,
            ),
        ] {
            let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
            let catalog_snapshot = SqlPlannerTableSnapshot::new(&catalog);
            let cancellation = Arc::new(Cancellation::default());
            let request = SqlAnalyzeRequest::new(
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                    current_catalog: None,
                    current_database: "default".into(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                &catalog_snapshot,
                crate::functions::builtin_sql_function_catalog(),
                noop_constant_evaluator(),
                None,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                control(None, &cancellation),
            );
            let optimized = analyze_then_optimize(request)
                .unwrap()
                .into_optimized_output()
                .unwrap();
            let physical =
                crate::planner::optimizer_bridge::to_physical_plan(&optimized.optimized_tree)
                    .unwrap();
            let plan = crate::planner::distributed::build::lower_final_physical_plan(
                &physical,
                novarocks_physical_plan::PlanVersionId::try_new([42; 16]).unwrap(),
                novarocks_physical_plan::PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                optimized.function_catalog,
                optimized.root_allow_throw_exception,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()
            .finish_observed(&crate::compiler::SqlCompileControl::unbounded())
            .unwrap();
            let result = plan.plan().result_port().unwrap();
            assert_eq!(result.fields.len(), 1);
            assert_eq!(result.fields[0].ty.data_type, expected);
            assert!(result.fields[0].ty.nullable);
        }
    }

    #[test]
    fn noop_constant_evaluator_observes_request_entry_and_decline_completion() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Owner {
            checks: std::sync::Mutex<Vec<u32>>,
            failure: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Owner {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::FunctionSpecialization);
                let mut checks = self.checks.lock().unwrap();
                checks.push(units);
                if let Some((at, error)) = self.failure
                    && checks.len() == at
                {
                    return Err(error);
                }
                Ok(())
            }
        }
        let request = FoldRequest {
            constant_policy: crate::constant::test_constant_policy(),
            kind: FoldNodeKind::Function {
                name: "fixture/no-op".into(),
            },
            args: Vec::new(),
            result_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                true,
            ),
        };
        let owner = Owner {
            checks: Default::default(),
            failure: None,
        };
        assert!(
            noop_constant_evaluator()
                .eval_scalar(&request, &owner)
                .unwrap()
                .is_none()
        );
        assert_eq!(*owner.checks.lock().unwrap(), [0, 0]);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in [1, 2] {
                let owner = Owner {
                    checks: Default::default(),
                    failure: Some((at, error)),
                };
                assert!(
                    matches!(noop_constant_evaluator().eval_scalar(&request, &owner), Err(SqlConstantEvaluationError::Control(actual)) if actual == error)
                );
                assert_eq!(owner.checks.lock().unwrap().len(), at);
            }
        }
    }

    #[test]
    fn analyzed_root_policy_keeps_admitted_hint_across_optimize_handoff() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        for (session, sql, expected) in [
            ("32", "SELECT 1", OutputNull),
            (
                "32",
                "SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1",
                ReportError,
            ),
            (
                "ERROR_IF_OVERFLOW",
                "SELECT /*+ SET_VAR(sql_mode=32) */ 1",
                OutputNull,
            ),
            ("ERROR_IF_OVERFLOW", "SELECT 1", ReportError),
            (
                "32",
                "SELECT x FROM (SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1 AS x) s",
                OutputNull,
            ),
        ] {
            let cancellation = Arc::new(Cancellation::default());
            let mut input = request(control(None, &cancellation));
            input.statement = SqlStatementInput::sql(sql);
            input.session.sql_semantics = input
                .session
                .sql_semantics
                .clone()
                .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session));
            let analyzed = SqlCompiler::analyze(input).unwrap().into_pending().unwrap();
            assert_eq!(analyzed.decimal_overflow_policy(), expected);
            let statistics = crate::planning::dml::DmlStatisticsSnapshot::default();
            let optimize =
                SqlOptimizeRequest::new(analyzed, &statistics, control(None, &cancellation));
            assert_eq!(optimize.decimal_overflow_policy(), expected);
        }
    }

    #[test]
    fn analyzed_root_allow_mode_survives_optimization_independently_of_decimal_policy() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        for (session_mode, sql, expected_allow, expected_policy) in [
            ("32", "SELECT 1", false, OutputNull),
            ("ALLOW_THROW_EXCEPTION", "SELECT 1", true, OutputNull),
            ("ERROR_IF_OVERFLOW", "SELECT 1", false, ReportError),
            (
                "ALLOW_THROW_EXCEPTION,ERROR_IF_OVERFLOW",
                "SELECT 1",
                true,
                ReportError,
            ),
            (
                "ERROR_IF_OVERFLOW",
                "SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ 1",
                true,
                OutputNull,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1",
                false,
                ReportError,
            ),
        ] {
            let cancellation = Arc::new(Cancellation::default());
            let request_control = control(None, &cancellation);
            let mut input = request(request_control.clone());
            input.statement = SqlStatementInput::sql(sql);
            input.session.sql_semantics = input
                .session
                .sql_semantics
                .clone()
                .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session_mode));
            let analyzed = SqlCompiler::analyze(input).unwrap().into_pending().unwrap();
            assert_eq!(analyzed.root_allow_throw_exception(), expected_allow);
            assert_eq!(analyzed.decimal_overflow_policy(), expected_policy);
            let optimize = SqlOptimizeRequest::new(analyzed, &STATISTICS, request_control);
            assert_eq!(optimize.root_allow_throw_exception(), expected_allow);
            assert_eq!(optimize.decimal_overflow_policy(), expected_policy);
            let optimized = SqlCompiler::optimize(optimize)
                .unwrap()
                .into_optimized_output()
                .unwrap();
            assert_eq!(optimized.root_allow_throw_exception, expected_allow);
        }
    }

    #[test]
    fn nested_decimal_policy_does_not_replace_the_admitted_root_allow_mode() {
        use crate::analysis::ExprKind;
        use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        fn cast_policies(
            node: &LogicalPlanNode,
            out: &mut Vec<novarocks_type_contract::DecimalOverflowPolicy>,
        ) {
            if let LogicalPlanKind::Project(project) = &node.kind {
                for item in &project.items {
                    if let ExprKind::Cast {
                        decimal_overflow_policy,
                        ..
                    } = &item.expr.kind
                    {
                        out.push(*decimal_overflow_policy);
                    }
                }
            }
            for child in &node.children {
                cast_policies(child, out);
            }
        }
        let cancellation = Arc::new(Cancellation::default());
        let request_control = control(None, &cancellation);
        let mut input = request(request_control.clone());
        input.statement = SqlStatementInput::sql(
            "SELECT x FROM (SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(1 AS DECIMAL(18,6)) AS x) s",
        );
        input.session.sql_semantics = input.session.sql_semantics.clone().with_sql_mode(
            crate::sql_mode::SqlMode::from_assignment("ALLOW_THROW_EXCEPTION"),
        );
        let analyzed = SqlCompiler::analyze(input).unwrap().into_pending().unwrap();
        assert!(analyzed.root_allow_throw_exception());
        assert_eq!(analyzed.decimal_overflow_policy(), OutputNull);
        let mut policies = Vec::new();
        cast_policies(&analyzed.logical_plan, &mut policies);
        assert_eq!(policies, [ReportError]);
        let optimized = SqlCompiler::optimize(SqlOptimizeRequest::new(
            analyzed,
            &STATISTICS,
            request_control,
        ))
        .unwrap()
        .into_optimized_output()
        .unwrap();
        assert!(optimized.root_allow_throw_exception);
    }

    #[test]
    fn analyzed_root_allow_mode_freezes_only_the_numeric_fold_eligibility() {
        struct NullableEvaluator;
        impl SqlConstantEvaluator for NullableEvaluator {
            fn eval_scalar(
                &self,
                request: &FoldRequest,
                control: &dyn novarocks_type_contract::PureCompileControl,
            ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
            {
                Ok(Some(crate::constant::admit_syntax_constant(
                    &LiteralValue::Null,
                    &request.result_type,
                    request.constant_policy,
                    control,
                )?))
            }
        }
        static EVALUATOR: NullableEvaluator = NullableEvaluator;
        for (session_mode, sql, disabled) in [
            ("32", "SELECT CAST(1000 AS DECIMAL(3,0))", false),
            (
                "32",
                "SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ CAST(1000 AS DECIMAL(3,0))",
                true,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT CAST(1000 AS DECIMAL(3,0))",
                true,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT /*+ SET_VAR(sql_mode=32) */ CAST(1000 AS DECIMAL(3,0))",
                false,
            ),
        ] {
            let cancellation = Arc::new(Cancellation::default());
            let request_control = control(None, &cancellation);
            let mut input = request(request_control.clone());
            input.statement = SqlStatementInput::sql(sql);
            input.constant_evaluator = Some(&EVALUATOR);
            input.session.sql_semantics = input
                .session
                .sql_semantics
                .clone()
                .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session_mode));
            let pending = SqlCompiler::analyze(input).unwrap().into_pending().unwrap();
            assert_eq!(pending.root_allow_throw_exception(), disabled);
            let result = pending
                .constant_evaluator
                .unwrap()
                .eval_scalar(
                    &FoldRequest {
                        constant_policy: crate::constant::test_constant_policy(),
                        kind: FoldNodeKind::Cast(
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                        ),
                        args: Vec::new(),
                        result_type: novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Decimal128(3, 0),
                            true,
                        ),
                    },
                    &request_control,
                )
                .unwrap();
            assert_eq!(result.is_none(), disabled);
        }
    }
}
mod completion;
mod completion_catalog;
mod completion_driver;
mod completion_predicate;
pub(crate) mod root_output;
mod root_render_type;
mod root_scalar_type;
pub use completion::*;
pub use root_render_type::{client_render_schema, client_render_schema_from_port};
pub(crate) mod mv_rewrite;

impl From<&str> for SqlCompileError {
    fn from(error: &str) -> Self {
        Self::Compilation(error.into())
    }
}

#[cfg(feature = "test-support")]
pub use crate::planner::distributed::build::state_source_fixture::ordinary_union_source as ordinary_union_source_for_test;

#[cfg(feature = "test-support")]
pub use crate::planner::distributed::build::state_author_fixture::{
    OrdinaryStateObservation, ordinary_state_observe_for_test,
};
#[cfg(feature = "test-support")]
pub use crate::planner::distributed::build::writer_state_fixture::{
    WriterStateSourceObservation, writer_state_observe_for_test, writer_state_source_for_test,
};

#[cfg(feature = "test-support")]
pub use crate::planner::distributed::build::state_source_fixture::ordinary_extrema_constant_state_source_for_test;

#[cfg(feature = "test-support")]
pub use crate::planner::distributed::build::state_source_fixture::ordered_array_state_source_for_test;

#[cfg(test)]
mod c16_p1_json_object_error_effect_tests;
