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

use crate::{
    Span,
    ast::{Ident, Literal, ObjectName, Query, TypeName, window::WindowSpec},
};

/// A SQL expression with source spans retained at every syntax node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Identifier(Ident),
    CompoundIdentifier(CompoundIdentifier),
    Literal(Literal),
    FunctionCall(FunctionCall),
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Nested(NestedExpr),
    Between(BetweenExpr),
    InList(InListExpr),
    InSubquery(InSubqueryExpr),
    Exists(ExistsExpr),
    Like(LikeExpr),
    IsPredicate(IsPredicateExpr),
    Case(CaseExpr),
    Cast(CastExpr),
    Interval(IntervalExpr),
    Subquery(SubqueryExpr),
    Tuple(TupleExpr),
    Array(ArrayExpr),
    Map(MapExpr),
    Struct(StructExpr),
    Lambda(LambdaExpr),
    Access(AccessExpr),
    TypedString(TypedStringExpr),
}

impl Expr {
    pub const fn span(&self) -> Span {
        match self {
            Self::Identifier(ident) => ident.span,
            Self::CompoundIdentifier(ident) => ident.span,
            Self::Literal(literal) => literal.span,
            Self::FunctionCall(call) => call.span,
            Self::Unary(expression) => expression.span,
            Self::Binary(expression) => expression.span,
            Self::Nested(expression) => expression.span,
            Self::Between(expression) => expression.span,
            Self::InList(expression) => expression.span,
            Self::InSubquery(expression) => expression.span,
            Self::Exists(expression) => expression.span,
            Self::Like(expression) => expression.span,
            Self::IsPredicate(expression) => expression.span,
            Self::Case(expression) => expression.span,
            Self::Cast(expression) => expression.span,
            Self::Interval(expression) => expression.span,
            Self::Subquery(expression) => expression.span,
            Self::Tuple(expression) => expression.span,
            Self::Array(expression) => expression.span,
            Self::Map(expression) => expression.span,
            Self::Struct(expression) => expression.span,
            Self::Lambda(expression) => expression.span,
            Self::Access(expression) => expression.span,
            Self::TypedString(expression) => expression.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompoundIdentifier {
    pub parts: Vec<Ident>,
    pub span: Span,
}

/// A function name and its argument expressions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionCall {
    pub name: ObjectName,
    pub arguments: Vec<Expr>,
    pub quantifier: FunctionQuantifier,
    pub order_by: Vec<FunctionOrderBy>,
    pub separator: Option<Box<Expr>>,
    pub filter: Option<Box<Expr>>,
    pub null_treatment: Option<NullTreatment>,
    pub over: Option<Box<WindowSpec>>,
    /// Whether SUBSTRING used its `value FROM start [FOR length]` spelling.
    pub substring_from_syntax: bool,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FunctionQuantifier {
    None,
    Distinct,
    All,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionOrderBy {
    pub expr: Expr,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullTreatment {
    IgnoreNulls,
    RespectNulls,
}

/// A prefix unary expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnaryExpr {
    pub operator: UnaryOperator,
    pub expression: Box<Expr>,
    pub span: Span,
}

/// A binary expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub operator: BinaryOperator,
    pub right: Box<Expr>,
    pub span: Span,
}

/// A parenthesized expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NestedExpr {
    pub expression: Box<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BetweenExpr {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub low: Box<Expr>,
    pub high: Box<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InListExpr {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub list: Vec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InSubqueryExpr {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExistsExpr {
    pub negated: bool,
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LikeExpr {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub operator: LikeOperator,
    pub pattern: Box<Expr>,
    pub escape: Option<Box<Expr>>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LikeOperator {
    Like,
    ILike,
    RLike,
    SimilarTo,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsPredicateExpr {
    pub expr: Box<Expr>,
    pub predicate: IsPredicate,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IsPredicate {
    Null,
    NotNull,
    True,
    NotTrue,
    False,
    NotFalse,
    Unknown,
    NotUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseExpr {
    pub operand: Option<Box<Expr>>,
    pub conditions: Vec<Expr>,
    pub results: Vec<Expr>,
    pub else_result: Option<Box<Expr>>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CastExpr {
    pub expr: Box<Expr>,
    pub data_type: TypeName,
    pub kind: CastKind,
    pub format: Option<Box<Expr>>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CastKind {
    Cast,
    TryCast,
    Convert,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalExpr {
    pub value: Box<Expr>,
    pub leading_field: IntervalField,
    pub leading_precision: Option<Box<Expr>>,
    pub last_field: Option<IntervalField>,
    pub fractional_seconds_precision: Option<Box<Expr>>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalField {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubqueryExpr {
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TupleExpr {
    pub expressions: Vec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrayExpr {
    /// Optional element type from StarRocks' `ARRAY<type>[...]` literal
    /// spelling. Bare `[ ... ]` literals leave this absent for inference.
    pub element_type: Option<TypeName>,
    pub elements: Vec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MapExpr {
    pub entries: Vec<MapEntry>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MapEntry {
    pub key: Expr,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructExpr {
    pub fields: Vec<StructExprField>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructExprField {
    pub name: Option<Ident>,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LambdaExpr {
    pub parameters: Vec<Ident>,
    /// Whether a single parameter appeared in parentheses in the source.
    pub parenthesized_single_parameter: bool,
    pub body: Box<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessExpr {
    pub expr: Box<Expr>,
    pub kind: AccessKind,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessKind {
    Field(Ident),
    Subscript(Box<Expr>),
    Json {
        operator: JsonOperator,
        path: Box<Expr>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonOperator {
    Arrow,
    ArrowText,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedStringExpr {
    pub data_type: TypeName,
    pub value: Literal,
    pub span: Span,
}

/// Prefix operators initially recognized by the Pratt parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    Not,
    Plus,
    Minus,
    BitwiseNot,
}

/// Infix operators initially recognized by the Pratt parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    NamedArgument,
    Or,
    And,
    Equal,
    NullSafeEqual,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
    StringConcat,
    IsDistinctFrom,
    IsNotDistinctFrom,
}
