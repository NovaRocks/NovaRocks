#![allow(dead_code)]

pub mod iceberg_ref;
pub(crate) use iceberg_ref::{AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor};

use crate::sql::catalog::LegacyRangePartition;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreateCatalogStmt {
    pub name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreateDatabaseStmt {
    pub name: ObjectName,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateTableStmt {
    pub name: ObjectName,
    pub kind: CreateTableKind,
    pub legacy_range_partitions: Vec<LegacyRangePartition>,
    /// Present when the SQL was `CREATE TABLE ... AS <select>`. Schema and
    /// (optionally) partition spec are inferred from the query at engine
    /// time. `None` for plain `CREATE TABLE` (the existing path).
    pub as_select: Option<Box<sqlparser::ast::Query>>,
    /// Set to `true` when the SQL was `CREATE TABLE IF NOT EXISTS ...`.
    /// For CTAS, the engine skips table creation and data write when the
    /// target table already exists.
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropCatalogStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropDatabaseStmt {
    pub name: ObjectName,
    pub if_exists: bool,
    pub force: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CreateTableKind {
    Iceberg {
        columns: Vec<TableColumnDef>,
        key_desc: Option<TableKeyDesc>,
        bucket_count: Option<u32>,
        partition_fields: Vec<IcebergPartitionFieldExpr>,
        properties: Vec<(String, String)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergPartitionFieldExpr {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AlterIcebergPartitionSpecStmt {
    AddPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
    DropPartitionColumn {
        table: ObjectName,
        field: IcebergPartitionFieldExpr,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropTableStmt {
    pub name: ObjectName,
    pub if_exists: bool,
    pub force: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaterializedViewDistribution {
    pub hash_columns: Vec<String>,
    pub bucket_count: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateMaterializedViewStmt {
    pub name: ObjectName,
    pub if_not_exists: bool,
    /// Simple `PARTITION BY col[, ...]` compatibility clause. NovaRocks keeps
    /// this in the AST so semantic validation can ensure referenced columns are
    /// real MV outputs.
    pub partition_by: Option<Vec<String>>,
    pub distribution: Option<MaterializedViewDistribution>,
    pub refresh_manual_explicit: bool,
    /// Raw SQL text of the SELECT body after `AS`. Produced by re-serializing
    /// the parsed `sqlparser::ast::Query`; used for storage and for
    /// re-parsing on every REFRESH in Phase 1.
    pub select_sql: String,
    pub select_query: sqlparser::ast::Query,
    /// Key-value pairs from `PROPERTIES(...)`, retained for later semantic
    /// interpretation (e.g. `storage_engine`). Empty when the clause is
    /// absent.
    pub properties: Vec<(String, String)>,
    /// Columns named in `PRIMARY KEY (col, ...)`. `None` when the clause is
    /// absent. The clause is the IVM Phase-2 opt-in marker; columns must
    /// reference the iceberg base table and satisfy the constraints checked
    /// by `mv_ddl::validate_ivm_primary_key`.
    pub primary_key: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropMaterializedViewStmt {
    pub name: ObjectName,
    pub if_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshMaterializedViewStmt {
    pub name: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShowMaterializedViewsStmt {
    pub database: Option<String>,
}

/// Top-level statement variants produced by the custom dialect `parse_sql`
/// entry point. Phase 1 only covers materialized-view DDL; other statements
/// still flow through the legacy `parse_sql_raw` path.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Statement {
    CreateMaterializedView(CreateMaterializedViewStmt),
    DropMaterializedView(DropMaterializedViewStmt),
    RefreshMaterializedView(RefreshMaterializedViewStmt),
    ShowMaterializedViews(ShowMaterializedViewsStmt),
    AlterIcebergRef(AlterIcebergRefStmt),
    Truncate {
        name: ObjectName,
        /// `"main"` by default; branch name when the SQL uses `t.branch_<name>`.
        target_ref: String,
    },
}

/// Describes the overwrite semantics of an INSERT statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverwriteMode {
    /// `INSERT INTO ...` — append.
    None,
    /// `INSERT OVERWRITE [TABLE] ...` — replace all rows in the table.
    FullTable,
    /// `INSERT OVERWRITE PARTITIONS [TABLE] ...` — replace only the partitions
    /// touched by the new data; other partitions preserved. v3 row-lineage only.
    DynamicPartitions,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InsertStmt {
    pub table: ObjectName,
    pub columns: Vec<String>,
    pub source: InsertSource,
    /// Overwrite semantics for this INSERT statement. `OverwriteMode::None` for
    /// `INSERT INTO`; `OverwriteMode::FullTable` for `INSERT OVERWRITE [TABLE]`.
    /// Phase 1 only honors non-None for iceberg backends — non-iceberg backends
    /// reject overwrite at the engine layer.
    pub overwrite_mode: OverwriteMode,
}

/// `DELETE FROM <table> WHERE <predicate>`. Phase 1 only supports iceberg
/// backends; the engine layer rejects other backends. WHERE is required;
/// `DELETE FROM <table>` (no filter) is rejected — the spec recommends
/// `INSERT OVERWRITE t SELECT * FROM t WHERE FALSE` for the truncate use case.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeleteStmt {
    pub table: ObjectName,
    pub where_clause: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateStmt {
    pub table: ObjectName,
    pub alias: Option<String>,
    pub assignments: Vec<UpdateAssignment>,
    pub source: Option<MutationSource>,
    pub where_clause: Option<sqlparser::ast::Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateAssignment {
    pub column: String,
    pub value: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MutationSource {
    Table {
        name: ObjectName,
        alias: Option<String>,
    },
    Query {
        query: Box<sqlparser::ast::Query>,
        alias: Option<String>,
    },
}

/// `MERGE INTO <target> USING <source> ON <pred> WHEN ...`. The first
/// implementation supports at most one `WHEN MATCHED` clause and at most one
/// `WHEN NOT MATCHED` clause; each clause may carry an optional `AND`
/// predicate. `WHEN NOT MATCHED BY SOURCE` and lateral source subqueries are
/// rejected at conversion time.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MergeStmt {
    pub table: ObjectName,
    pub target_alias: Option<String>,
    pub source: MutationSource,
    pub on: sqlparser::ast::Expr,
    pub matched: Option<MergeWhenClause<MergeMatchedAction>>,
    pub not_matched: Option<MergeWhenClause<MergeNotMatchedAction>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MergeWhenClause<A> {
    /// Optional `AND <expr>` predicate refining the clause.
    pub predicate: Option<sqlparser::ast::Expr>,
    pub action: A,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MergeMatchedAction {
    Update { assignments: Vec<UpdateAssignment> },
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MergeNotMatchedAction {
    /// Target columns named in `INSERT (a, b, c)`. Empty when omitted (callers
    /// must align the values with the target schema in column order).
    pub columns: Vec<String>,
    /// Per-column value expressions from the `VALUES (...)` clause. The
    /// element count must match `columns` (or the target schema when
    /// `columns` is empty).
    pub values: Vec<sqlparser::ast::Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InsertSource {
    Values(Vec<Vec<Literal>>),
    SelectLiteralRow(Vec<Literal>),
    /// `a UNION ALL b` and chains thereof. Each sub-source is evaluated in
    /// order and their rows are concatenated. UNION (distinct) is not
    /// supported: INSERT-level deduplication would need table-side semantics
    /// we don't want to replicate at the parser layer.
    UnionAll(Vec<InsertSource>),
    /// A full SELECT query that cannot be collapsed into literal rows. Carrying
    /// the raw sqlparser AST lets us hand the SELECT back to the normal
    /// analyzer/planner/pipeline stack at execution time instead of evaluating
    /// it in the parser layer.
    FromQuery(Box<sqlparser::ast::Query>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
    pub aggregation: Option<ColumnAggregation>,
    pub default: Option<DefaultLiteral>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableKeyDesc {
    pub kind: TableKeyKind,
    pub columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TableKeyKind {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnAggregation {
    Sum,
    Min,
    Max,
    Replace,
}

/// Literal that may appear in `DEFAULT <literal>` clauses for Iceberg v3
/// columns.  `Null` is the sentinel for `DEFAULT NULL` and is NOT persisted
/// into the Iceberg metadata; it only suppresses duplicate-DEFAULT diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DefaultLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal { unscaled: i128, scale: i8 },
    String(String),
    Date(i32),     // days since 1970-01-01
    DateTime(i64), // microseconds since 1970-01-01T00:00:00Z
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Decimal {
        precision: u8,
        scale: i8,
    },
    String,
    Binary,
    Boolean,
    Date,
    DateTime,
    Time,
    Array(Box<SqlType>),
    Map(Box<SqlType>, Box<SqlType>),
    Struct(Vec<(String, SqlType)>),
    /// Iceberg v3 unshredded variant. Carried as Arrow `LargeBinary`
    /// in execution; persisted as a parquet group with `LogicalType::Variant`.
    Variant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectName {
    pub parts: Vec<String>,
}

impl ObjectName {
    pub(crate) fn leaf(&self) -> &str {
        self.parts
            .last()
            .map(String::as_str)
            .expect("object name must have at least one part")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColumnRef {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    Column(ColumnRef),
    Literal(Literal),
    Arithmetic {
        left: Box<Expr>,
        op: ArithmeticOp,
        right: Box<Expr>,
    },
    Comparison {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    Logical {
        left: Box<Expr>,
        op: LogicalOp,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Aggregate(AggregateExpr),
    ScalarFunction(ScalarFunctionExpr),
    Array(Vec<Expr>),
    Cast {
        expr: Box<Expr>,
        data_type: SqlType,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AggregateExpr {
    pub name: String,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub order_by: Vec<FunctionOrderByExpr>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScalarFunctionExpr {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FunctionOrderByExpr {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogicalOp {
    And,
    Or,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<Literal>),
    Map(Vec<(Literal, Literal)>),
    Struct(Vec<Literal>),
}
