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

//! Semantic analysis output types.
//!
//! All names are resolved, all expressions carry their Arrow DataType.
//! This layer does NOT contain physical concepts (tuple_id, slot_id).

pub(crate) mod cte;
pub(crate) mod expr_display;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
pub use crate::sql::common::LiteralValue;
pub(crate) use crate::sql::common::{
    BinOp, JoinKind, LambdaParam, OutputColumn, UnOp, WindowBound, WindowFrame, WindowFrameType,
};
use crate::sql::planner::table::TableDef;

// ---------------------------------------------------------------------------
// Top-level query
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ResolvedQuery {
    pub body: QueryBody,
    pub order_by: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub output_columns: Vec<OutputColumn>,
    /// CTE ids declared by this query block's WITH clause, in declaration order.
    pub local_cte_ids: Vec<cte::CteId>,
}

#[derive(Clone, Debug)]
pub(crate) struct SortItem {
    pub expr: TypedExpr,
    pub asc: bool,
    pub nulls_first: bool,
}

// ---------------------------------------------------------------------------
// Query body
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum QueryBody {
    Select(ResolvedSelect),
    SetOperation(ResolvedSetOp),
    Values(ResolvedValues),
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedSelect {
    /// FROM clause. `None` means `SELECT expr` without FROM (dual table).
    pub from: Option<Relation>,
    /// WHERE clause.
    pub filter: Option<TypedExpr>,
    /// GROUP BY expressions.
    pub group_by: Vec<TypedExpr>,
    /// HAVING clause.
    pub having: Option<TypedExpr>,
    /// SELECT list items.
    pub projection: Vec<ProjectItem>,
    /// Whether the SELECT contains aggregate functions.
    pub has_aggregation: bool,
    /// Whether SELECT DISTINCT is used.
    pub distinct: bool,
    /// Repeat metadata for ROLLUP/CUBE/GROUPING SETS expansion.
    pub repeat: Option<RepeatInfo>,
    /// Scalar subqueries routed to the Apply framework. Consumed by the
    /// planner to emit `LogicalPlanKind::Apply`.
    pub apply_specs: Vec<ApplyScalarSpec>,
    /// EXISTS/IN subqueries routed to the Apply framework. Consumed by the
    /// planner alongside `apply_specs` to emit `LogicalPlanKind::Apply`.
    #[allow(dead_code)]
    pub predicate_apply_specs: Vec<ApplyPredicateSpec>,
}

/// Metadata for ROLLUP/CUBE/GROUPING SETS repeat execution.
#[derive(Clone, Debug)]
pub(crate) struct RepeatInfo {
    /// For each repeat level, the column names that are NON-null.
    pub repeat_column_ref_list: Vec<Vec<String>>,
    /// For each repeat level, the ColumnIds that are NON-null.
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    /// Grouping ID bitmap for each level. Bit=1 means column is NULLed.
    pub grouping_ids: Vec<u64>,
    /// All rollup column names.
    pub all_rollup_columns: Vec<String>,
    /// All rollup ColumnIds.
    pub all_rollup_column_ids: Vec<ColumnId>,
    /// GROUPING() function calls: (output_name, arg_column_names).
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    /// GROUPING() function argument ColumnIds, aligned with `grouping_fn_args`.
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    /// GROUPING() virtual output ids: (output_name, analyzer-minted ColumnId).
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectItem {
    pub expr: TypedExpr,
    pub output_name: String,
    /// The [`ColumnId`] that the analyzer minted for this output column.
    /// Always equals the corresponding entry in the parallel `output_columns`
    /// vec so that parent plans can address this output by `ColumnId`.
    /// Set to [`ColumnId::UNSET`] only at optimizer/planner construction sites
    /// that are never reached by the column-pruning pass.
    pub output_column_id: ColumnId,
}

// ---------------------------------------------------------------------------
// FROM clause (relational tree, supports subqueries)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum Relation {
    /// A base table scan.
    Scan(ScanRelation),
    /// An Iceberg metadata table scan: `t$snapshots`, `t$history`, etc.
    /// Produced by `resolve_from` after `__nr_meta_<type>__` suffix detection.
    IcebergMetadataScan(IcebergMetadataScanRelation),
    /// IVM-A1 plan-time delta scan: `__nr_ivm_delta('cat.ns.tbl', from, to)`.
    /// Produced by the analyzer when it recognizes the `__nr_ivm_delta`
    /// table function. Lowered by the planner into a regular `Scan` over a
    /// synthetic `TableDef` whose storage is `ScanSource::IcebergDeltaTable`,
    /// and emitted by codegen as `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`.
    IcebergDeltaScan(IcebergDeltaScanRelation),
    /// A subquery in FROM: `(SELECT ...) AS alias`.
    Subquery {
        query: Box<ResolvedQuery>,
        alias: String,
        output_columns: Vec<OutputColumn>,
    },
    /// A join between two relations.
    Join(Box<JoinRelation>),
    /// `TABLE(generate_series(start, end[, step]))`.
    GenerateSeries(GenerateSeriesRelation),
    /// `LATERAL UNNEST(array_expr[, ...])`.
    Unnest(UnnestRelation),
    /// Reference to an analyzed non-recursive CTE definition.
    /// Inline vs reuse is decided later by Cascades.
    CTEConsume {
        cte_id: cte::CteId,
        alias: String,
        output_columns: Vec<OutputColumn>,
        producer_column_ids: Vec<ColumnId>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesRelation {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: crate::sql::column_id::ColumnId,
}

#[derive(Clone, Debug)]
pub(crate) struct UnnestRelation {
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ScanRelation {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    /// G1: ColumnId assigned by the analyzer when this table was added to a
    /// scope. For Iceberg v3 row-lineage scans, ids are base columns first,
    /// then hidden metadata columns. The planner reuses these instead of
    /// minting fresh ones so the scan output's ColumnIds match the
    /// analyzer-produced `ColumnRef`s in the rest of the plan (filters,
    /// GROUP BY, ORDER BY, Window PARTITION BY, etc.).
    pub column_ids: Vec<ColumnId>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergMetadataScanRelation {
    /// The underlying iceberg table being inspected.
    pub database: String,
    pub table: TableDef,
    pub metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    /// FROM-clause alias (e.g., `t$snapshots AS s` → `Some("s")`).
    pub alias: Option<String>,
    /// Analyzer-allocated ColumnIds for each metadata column, in schema order.
    /// The planner reuses these to keep ColumnRef ids in the rest of the plan
    /// consistent with the scan's output column ids (same pattern as `Relation::Scan`).
    pub column_ids: Vec<crate::sql::column_id::ColumnId>,
}

/// IVM-A1 plan-time delta-scan reference: the analyzer's output for a
/// `__nr_ivm_delta('cat.ns.tbl', from_snap, to_snap)` table function call.
/// Carries the base table's `TableDef` (with v3 row-lineage metadata
/// columns already populated by the catalog) so the planner can emit a
/// synthetic `LogicalPlanKind::Scan` whose storage tag dispatches codegen to
/// `ICEBERG_DELTA_SCAN_NODE`.
#[derive(Clone, Debug)]
pub(crate) struct IcebergDeltaScanRelation {
    /// Three-part identifier of the base Iceberg table.
    pub catalog: String,
    pub namespace: String,
    pub table_name: String,
    /// The base table definition resolved through the catalog. Includes the
    /// `iceberg_row_lineage_metadata_columns` that delta-scan exposes as
    /// resolvable virtual columns (`_row_id`, etc).
    pub table: TableDef,
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    /// Optional FROM-clause alias (`__nr_ivm_delta(...) AS t` → `Some("t")`).
    pub alias: Option<String>,
    /// Analyzer-allocated ColumnIds for base table columns + row-lineage metadata
    /// columns, in schema order (base columns first, then metadata columns). The
    /// planner reuses these instead of minting fresh ids so ColumnRef ids in the
    /// rest of the plan (SELECT list, WHERE, GROUP BY, etc.) match the scan's
    /// output column ids (same pattern as `Relation::Scan`).
    pub column_ids: Vec<crate::sql::column_id::ColumnId>,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinRelation {
    pub left: Relation,
    pub right: Relation,
    pub join_type: JoinKind,
    /// `None` for CROSS JOIN.
    pub condition: Option<TypedExpr>,
}

// ---------------------------------------------------------------------------
// Set operations (UNION / INTERSECT / EXCEPT)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ResolvedSetOp {
    pub kind: SetOpKind,
    pub all: bool,
    pub left: Box<ResolvedQuery>,
    pub right: Box<ResolvedQuery>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetOpKind {
    Union,
    Intersect,
    Except,
}

// ---------------------------------------------------------------------------
// VALUES clause
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ResolvedValues {
    pub rows: Vec<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
}

// ---------------------------------------------------------------------------
// Typed expressions (all nodes carry resolved DataType)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct TypedExpr {
    pub kind: ExprKind,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub enum ExprKind {
    /// Resolved column reference.
    ColumnRef {
        column_id: ColumnId,
        qualifier: Option<String>,
        column: String,
    },
    /// Resolved lambda parameter reference.
    LambdaParamRef { name: String, slot_id: i32 },
    /// Literal value.
    Literal(LiteralValue),
    /// Binary operation (arithmetic, comparison, logical).
    BinaryOp {
        left: Box<TypedExpr>,
        op: BinOp,
        right: Box<TypedExpr>,
    },
    /// Unary operation.
    UnaryOp { op: UnOp, expr: Box<TypedExpr> },
    /// Scalar function call (non-aggregate).
    FunctionCall {
        name: String,
        args: Vec<TypedExpr>,
        distinct: bool,
        /// Semantics resolved by the request's immutable function catalog.
        /// The optimizer bridge preserves this value instead of reclassifying
        /// by name from ambient process state.
        volatility: crate::sql::functions::FunctionVolatility,
    },
    /// Higher-order function lambda expression.
    LambdaFunction {
        params: Vec<LambdaParam>,
        body: Box<TypedExpr>,
    },
    /// Aggregate function call.
    AggregateCall {
        name: String,
        args: Vec<TypedExpr>,
        distinct: bool,
        order_by: Vec<SortItem>,
    },
    /// CAST expression.
    Cast {
        expr: Box<TypedExpr>,
        target: DataType,
    },
    /// IS [NOT] NULL.
    IsNull { expr: Box<TypedExpr>, negated: bool },
    /// [NOT] IN (list).
    InList {
        expr: Box<TypedExpr>,
        list: Vec<TypedExpr>,
        negated: bool,
    },
    /// [NOT] BETWEEN low AND high.
    Between {
        expr: Box<TypedExpr>,
        low: Box<TypedExpr>,
        high: Box<TypedExpr>,
        negated: bool,
    },
    /// [NOT] LIKE pattern.
    Like {
        expr: Box<TypedExpr>,
        pattern: Box<TypedExpr>,
        negated: bool,
    },
    /// CASE [operand] WHEN ... THEN ... [ELSE ...] END.
    Case {
        operand: Option<Box<TypedExpr>>,
        when_then: Vec<(TypedExpr, TypedExpr)>,
        else_expr: Option<Box<TypedExpr>>,
    },
    /// IS [NOT] TRUE / IS [NOT] FALSE.
    IsTruthValue {
        expr: Box<TypedExpr>,
        value: bool,
        negated: bool,
    },
    /// Parenthesized expression (preserved for display fidelity).
    Nested(Box<TypedExpr>),
    /// Window function call: `func(...) OVER (PARTITION BY ... ORDER BY ... frame)`.
    WindowCall {
        name: String,
        args: Vec<TypedExpr>,
        distinct: bool,
        partition_by: Vec<TypedExpr>,
        order_by: Vec<SortItem>,
        window_frame: Option<WindowFrame>,
        /// `IGNORE NULLS` modifier (for first_value/last_value/lead/lag).
        /// `false` means default (RESPECT NULLS).
        ignore_nulls: bool,
    },
    /// Placeholder for a subquery that will be rewritten into a JOIN.
    /// This is an intermediate representation created during expression analysis
    /// and consumed by the subquery rewriting pass before planning.
    #[allow(dead_code)]
    SubqueryPlaceholder {
        id: usize,
        kind: SubqueryKind,
        data_type: DataType,
    },
    /// Lambda expression used by higher-order functions (e.g. array_map,
    /// array_filter). Produced only inside a higher-order function call's
    /// arguments; not a free-standing expression elsewhere.
    Lambda {
        /// Parameter names in declaration order (lower-cased).
        params: Vec<String>,
        /// Lambda body, analyzed under a scope that binds each `params[i]` to
        /// the corresponding higher-order function's element type.
        body: Box<TypedExpr>,
    },
}

/// The kind of subquery encountered in an expression.
#[derive(Clone, Debug)]
pub(crate) enum SubqueryKind {
    /// Scalar subquery: `col op (SELECT agg(...) FROM ...)`
    /// Stores the subquery AST, comparison operator, and the LHS expression.
    Scalar,
    /// EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)
    Exists { negated: bool },
    /// col [NOT] IN (SELECT ...)
    InSubquery { negated: bool },
}

/// Which clause of the enclosing SELECT a scalar subquery was found in.
/// Determines where the planner inserts the Apply node relative to the
/// WHERE filter, the aggregate, and the projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyClause {
    Where,
    /// A scalar subquery nested in an aggregate argument. Its output must be
    /// produced before the Aggregate consumes that argument.
    AggregateInput,
    Having,
    Projection,
}

/// A scalar subquery the analyzer routed to the Apply framework.
/// The planner consumes these to emit `LogicalPlanKind::Apply`. The inner query is
/// left INTACT — correlation predicates remain in its WHERE; M1b's
/// PushDownApplyFilter rule extracts them into the Apply's correlation_conjuncts.
// All fields are consumed by the M1a planner (Task 4) and M1b runtime (Task 5+).
// The dead_code allows on individual fields drop when each is first read in
// production code (the test build reads them but cfg(test) code is excluded
// from the non-test dead-code analysis).
#[derive(Clone, Debug)]
pub(crate) struct ApplyScalarSpec {
    /// Placeholder id this spec replaced (matches the original SubqueryInfo.id).
    /// Read by M1b (AssertOneRow error message); unused in M1a production code.
    #[allow(dead_code)]
    pub subquery_id: usize,
    /// Which clause of the enclosing SELECT the subquery was found in.
    /// Read by the planner to select the insertion point.
    pub clause: ApplyClause,
    /// Fresh column representing the subquery's scalar value in outer exprs.
    /// Read by the planner to build the Apply node.
    pub output_column: OutputColumn,
    /// Fully-analyzed inner subquery, with outer references carrying the outer
    /// column ids (via merged-scope analysis). Becomes the Apply's right child.
    /// Read by the planner to plan Apply.right.
    pub inner: ResolvedQuery,
    /// Outer columns referenced inside the subquery (their ids are the outer
    /// factory's ids, since the outer scope was merged into the inner analysis).
    /// Read by the planner to populate Apply.correlation_column_ids.
    pub correlation_column_ids: Vec<ColumnId>,
    /// Scalar subqueries must yield <= 1 row; M1b discharges this when the inner
    /// is a scalar aggregate grouped by the correlation key.
    /// Read by the planner to set Apply.need_check_max_rows.
    pub need_check_max_rows: bool,
    /// Original subquery SQL text, for the M1b AssertOneRow runtime message.
    /// Read by M1b; unused in M1a production code.
    #[allow(dead_code)]
    pub subquery_text: String,
}

/// An EXISTS / NOT EXISTS / IN / NOT IN subquery routed to the Apply framework
/// Parallel to `ApplyScalarSpec`; the planner consumes these to
/// emit `LogicalPlanKind::Apply` with `ApplyKind::Exists` / `ApplyKind::In`. The
/// inner query is left INTACT — its WHERE (correlation + residual) is read by
/// the M3 to-join rules (`ExistentialApplyToJoin` / `QuantifiedApplyToJoin`).
// Constructed and read by analyzer collection and planner in Task 2/3.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ApplyPredicateSpec {
    /// Placeholder id this spec replaced (matches the original SubqueryInfo.id).
    pub subquery_id: usize,
    /// EXISTS{negated} or InSubquery{negated}. Maps to the planner ApplyKind.
    pub kind: SubqueryKind,
    /// Which clause the placeholder lived in. M3 only records `Where`.
    pub clause: ApplyClause,
    /// Fresh Boolean indicator column for the subquery in the Apply schema.
    /// Removed from the outer filter (semantics carried by the semi/anti join),
    /// so it is never referenced; it disappears when the join replaces the Apply.
    pub output_column: OutputColumn,
    /// Fully-analyzed inner subquery (outer refs carry outer column ids).
    pub inner: ResolvedQuery,
    /// Outer columns referenced inside the subquery (the correlation keys).
    pub correlation_column_ids: Vec<ColumnId>,
    /// For IN/NOT IN: the analyzed single-column LHS. None for EXISTS.
    pub in_lhs: Option<TypedExpr>,
    /// True iff the subquery is a top-level AND conjunct of WHERE (always true
    /// for an M3-recorded spec; carried for the planner and EXPLAIN parity).
    pub use_semi_anti: bool,
    /// Original subquery SQL text (diagnostics).
    pub subquery_text: String,
}

/// A collected subquery from expression analysis, ready for rewriting.
#[derive(Clone, Debug)]
pub(crate) struct SubqueryInfo {
    pub id: usize,
    pub kind: SubqueryKind,
    pub subquery: Box<sqlparser::ast::Query>,
    /// The resolved data type of the subquery result (scalar).
    #[allow(dead_code)]
    pub data_type: DataType,
    /// For IN subquery: the left-hand expression from the outer query.
    pub in_expr: Option<Box<sqlparser::ast::Expr>>,
}
