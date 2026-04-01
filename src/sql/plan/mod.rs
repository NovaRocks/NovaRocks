//! Logical Plan — a tree of relational algebra operators.
//!
//! This is the layer where a future optimizer would operate.
//! Expressions use [`TypedExpr`] from [`crate::sql::ir`].

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;

use crate::sql::ir::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr};

// ---------------------------------------------------------------------------
// Logical plan tree
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum LogicalPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    Sort(SortNode),
    Limit(LimitNode),
    Union(UnionNode),
    Intersect(IntersectNode),
    Except(ExceptNode),
    Values(ValuesNode),
    GenerateSeries(GenerateSeriesNode),
    Window(WindowNode),
    /// Wraps a subquery plan with an alias, so that the physical emitter
    /// can register qualified columns (e.g., `ctr1.ctr_customer_sk` for
    /// a CTE referenced as `FROM customer_total_return ctr1`).
    SubqueryAlias(SubqueryAliasNode),
    /// Repeat node for ROLLUP/CUBE/GROUPING SETS.
    /// Replicates each input row N times with different null patterns.
    Repeat(RepeatPlanNode),
    /// Reference to a shared CTE. Leaf node — execution receives data via exchange.
    CTEConsume(CTEConsumeNode),
}

/// Repeat node for ROLLUP/CUBE/GROUPING SETS.
/// Replicates each input row N times with different null patterns.
#[derive(Clone, Debug)]
pub(crate) struct RepeatPlanNode {
    pub input: Box<LogicalPlan>,
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}

/// Subquery alias node: wraps an inlined subquery (CTE or derived table)
/// with the alias name and output column metadata.
#[derive(Clone, Debug)]
pub(crate) struct SubqueryAliasNode {
    pub input: Box<LogicalPlan>,
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEConsumeNode {
    pub cte_id: crate::sql::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}

/// A query plan with optional shared CTE subtrees.
#[derive(Clone, Debug)]
pub(crate) struct QueryPlan {
    pub cte_plans: Vec<CTEProducePlan>,
    pub main_plan: LogicalPlan,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEProducePlan {
    pub cte_id: crate::sql::cte::CteId,
    pub plan: LogicalPlan,
    pub output_columns: Vec<crate::sql::ir::OutputColumn>,
}

/// Analytic/window function evaluation node.
#[derive(Clone, Debug)]
pub(crate) struct WindowNode {
    pub input: Box<LogicalPlan>,
    pub window_exprs: Vec<WindowExpr>,
    /// All output columns: base columns from input + window function results.
    pub output_columns: Vec<OutputColumn>,
}

/// A single window function expression with its OVER specification.
#[derive(Clone, Debug)]
pub(crate) struct WindowExpr {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortItem>,
    pub window_frame: Option<crate::sql::ir::WindowFrame>,
    pub result_type: DataType,
    pub output_name: String,
}

/// Inline table function: `TABLE(generate_series(start, end, step))`.
/// Materialized to a temporary parquet file at emission time.
#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
}

// ---------------------------------------------------------------------------
// Leaf nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    /// Predicates pushed down from Filter nodes by the optimizer.
    pub predicates: Vec<TypedExpr>,
    /// Columns actually required by upstream operators (set by column pruning).
    /// `None` means all columns are required (no pruning applied).
    pub required_columns: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

// ---------------------------------------------------------------------------
// Unary nodes (single input)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: TypedExpr,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<ProjectItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateCall {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: DataType,
    pub order_by: Vec<SortItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct SortNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<SortItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

// ---------------------------------------------------------------------------
// Binary nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinKind,
    /// `None` for CROSS JOIN.
    pub condition: Option<TypedExpr>,
}

// ---------------------------------------------------------------------------
// N-ary set operation nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct UnionNode {
    pub inputs: Vec<LogicalPlan>,
    /// `true` = UNION ALL, `false` = UNION DISTINCT.
    pub all: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct IntersectNode {
    pub inputs: Vec<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExceptNode {
    pub inputs: Vec<LogicalPlan>,
}
