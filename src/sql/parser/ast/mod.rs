#![allow(dead_code)]

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreateCatalogStmt {
    pub name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreateDatabaseStmt {
    pub name: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreateTableStmt {
    pub name: ObjectName,
    pub kind: CreateTableKind,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CreateTableKind {
    LocalParquet {
        path: String,
    },
    Iceberg {
        columns: Vec<TableColumnDef>,
        key_desc: Option<TableKeyDesc>,
        properties: Vec<(String, String)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DropTableStmt {
    pub name: ObjectName,
    pub if_exists: bool,
    pub force: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InsertStmt {
    pub table: ObjectName,
    pub columns: Vec<String>,
    pub source: InsertSource,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InsertSource {
    Values(Vec<Vec<Literal>>),
    SelectLiteralRow(Vec<Literal>),
    GenerateSeriesSelect(GenerateSeriesSelect),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GenerateSeriesSelect {
    pub column_name: String,
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub projection: Vec<Expr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub aggregation: Option<ColumnAggregation>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SqlType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Decimal { precision: u8, scale: i8 },
    String,
    Boolean,
    Date,
    DateTime,
    Time,
    Array(Box<SqlType>),
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
}
