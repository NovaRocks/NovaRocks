use arrow::datatypes::DataType;

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
    NullAwareLeftAnti,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LambdaParam {
    pub name: String,
    pub slot_id: i32,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub start: WindowBound,
    pub end: WindowBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WindowFrameType {
    Rows,
    Range,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WindowBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    LargeInt(i128),
    Float(f64),
    Decimal(String),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    EqForNull,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnOp {
    Not,
    Negate,
    BitwiseNot,
}
