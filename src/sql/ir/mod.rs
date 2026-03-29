//! Resolved Query IR — the output of semantic analysis.
//!
//! All names are resolved, all expressions carry their Arrow DataType.
//! This layer does NOT contain physical concepts (tuple_id, slot_id).

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;

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
}

#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
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
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectItem {
    pub expr: TypedExpr,
    pub output_name: String,
}

// ---------------------------------------------------------------------------
// FROM clause (relational tree, supports subqueries)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum Relation {
    /// A base table scan.
    Scan(ScanRelation),
    /// A subquery in FROM: `(SELECT ...) AS alias`.
    Subquery {
        query: Box<ResolvedQuery>,
        alias: String,
    },
    /// A join between two relations.
    Join(Box<JoinRelation>),
    /// `TABLE(generate_series(start, end[, step]))`.
    GenerateSeries(GenerateSeriesRelation),
}

#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesRelation {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ScanRelation {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinRelation {
    pub left: Relation,
    pub right: Relation,
    pub join_type: JoinKind,
    /// `None` for CROSS JOIN.
    pub condition: Option<TypedExpr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

// ---------------------------------------------------------------------------
// Set operations (UNION / INTERSECT / EXCEPT)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ResolvedSetOp {
    pub kind: SetOpKind,
    pub all: bool,
    pub left: Box<QueryBody>,
    pub right: Box<QueryBody>,
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
    pub column_types: Vec<DataType>,
}

// ---------------------------------------------------------------------------
// Typed expressions (all nodes carry resolved DataType)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct TypedExpr {
    pub kind: ExprKind,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum ExprKind {
    /// Resolved column reference.
    ColumnRef {
        qualifier: Option<String>,
        column: String,
    },
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
    },
    /// Placeholder for a subquery that will be rewritten into a JOIN.
    /// This is an intermediate representation created during expression analysis
    /// and consumed by the subquery rewriting pass before planning.
    SubqueryPlaceholder {
        id: usize,
        kind: SubqueryKind,
        data_type: DataType,
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

/// A collected subquery from expression analysis, ready for rewriting.
#[derive(Clone, Debug)]
pub(crate) struct SubqueryInfo {
    pub id: usize,
    pub kind: SubqueryKind,
    pub subquery: Box<sqlparser::ast::Query>,
    /// The resolved data type of the subquery result (scalar).
    pub data_type: DataType,
    /// For IN subquery: the left-hand expression from the outer query.
    pub in_expr: Option<Box<sqlparser::ast::Expr>>,
}

/// Window frame specification for analytic functions.
#[derive(Clone, Debug)]
pub(crate) struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub start: WindowBound,
    pub end: WindowBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowFrameType {
    Rows,
    Range,
}

#[derive(Clone, Debug)]
pub(crate) enum WindowBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

// ---------------------------------------------------------------------------
// Expression primitives
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Decimal literal with its original string representation (e.g. "100.00").
    /// Precision and scale are inferred from the text: `100.00` → precision=5, scale=2.
    Decimal(String),
    String(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    EqForNull,
    // Logical
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnOp {
    Not,
    Negate,
    BitwiseNot,
}
