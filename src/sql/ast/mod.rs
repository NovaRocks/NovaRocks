#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Statement {
    Query(QueryStmt),
    CreateCatalog(CreateCatalogStmt),
    CreateDatabase(CreateDatabaseStmt),
    CreateTable(CreateTableStmt),
    DropCatalog(DropCatalogStmt),
    DropDatabase(DropDatabaseStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryStmt {
    pub projection: Vec<ProjectionItem>,
    pub from: TableRef,
    pub selection: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

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
    pub source: InsertSource,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InsertSource {
    Values(Vec<Vec<Literal>>),
    SelectLiteralRow(Vec<Literal>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    String,
    Boolean,
    Date,
    DateTime,
    Time,
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjectionItem {
    Wildcard,
    Column(ColumnRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColumnRef {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableRef {
    pub name: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderByExpr {
    pub column: ColumnRef,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    Comparison {
        left: ColumnRef,
        op: CompareOp,
        right: Literal,
    },
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
}
