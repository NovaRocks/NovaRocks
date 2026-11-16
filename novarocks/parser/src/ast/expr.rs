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
    ast::{AstBox, AstVec, Ident, Literal, ObjectName, OrderBy, Query, window::WindowSpec},
};

/// A SQL expression with source spans retained at every syntax node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    JsonConstructor(Box<super::JsonConstructor>),
    JsonExists(Box<super::JsonExists>),
    JsonValue(Box<super::JsonValue>),
    JsonQuery(Box<super::JsonQuery>),
    JsonSerialize(Box<super::JsonSerialize>),
    JsonObject(Box<super::JsonObject>),
    JsonArray(Box<super::JsonArray>),

    Identifier(Ident),
    Literal(Literal),
    FunctionCall(FunctionCall),
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Logical(LogicalExpr),
    Nested(NestedExpr),
    Predicated(AstBox<super::PredicatedExpr>),
    InList(AstBox<super::InListExpr>),
    Exists(AstBox<super::ExistsExpr>),
    SearchedCase(AstBox<super::SearchedCaseExpr>),
    SimpleCase(AstBox<super::SimpleCaseExpr>),
    Unique(AstBox<super::UniqueExpr>),
    Cast(AstBox<super::CastExpr>),
    Interval(IntervalExpr),
    Subquery(SubqueryExpr),
    Row(RowExpr),
    Array(ArrayExpr),
    Map(MapExpr),
    Lambda(AstBox<super::LambdaExpr>),
    Access(AccessExpr),
    Subscript(AstBox<SubscriptExpr>),
    ArrayWildcardSubscript(AstBox<ArrayWildcardSubscriptExpr>),
    Dereference(AstBox<DereferenceExpr>),
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Self::JsonConstructor(node) => node.span,
            Self::JsonExists(node) => node.span,
            Self::JsonValue(node) => node.span,
            Self::JsonQuery(node) => node.span,
            Self::JsonSerialize(node) => node.span,
            Self::JsonObject(node) => node.span,
            Self::JsonArray(node) => node.span,

            Self::Identifier(ident) => ident.span,
            Self::Literal(literal) => literal.span,
            Self::FunctionCall(call) => call.span,
            Self::Unary(expression) => expression.span,
            Self::Binary(expression) => expression.span,
            Self::Logical(expression) => expression.span,
            Self::Nested(expression) => expression.span,
            Self::InList(expression) => expression.span,
            Self::Predicated(expression) => expression.span,
            Self::Exists(expression) => expression.span,
            Self::SearchedCase(node) => node.span,
            Self::SimpleCase(node) => node.span,
            Self::Unique(node) => node.span,
            Self::Cast(expression) => expression.span,
            Self::Interval(expression) => expression.span,
            Self::Subquery(expression) => expression.span,
            Self::Row(expression) => expression.span,
            Self::Array(expression) => expression.span,
            Self::Map(expression) => expression.span,
            Self::Lambda(expression) => expression.span,
            Self::Access(expression) => expression.span,
            Self::Subscript(expression) => expression.span,
            Self::ArrayWildcardSubscript(expression) => expression.span,
            Self::Dereference(expression) => expression.span,
        }
    }
}

/// A function name and its argument expressions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionCall {
    pub name: ObjectName,
    pub arguments: Vec<Expr>,
    pub quantifier: FunctionQuantifier,
    pub order_by: Option<OrderBy>,
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
pub struct RowExpr {
    pub fields: AstVec<RowField>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowField {
    pub name: Option<Ident>,
    pub expression: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrayExpr {
    pub values: AstVec<Expr>,
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
pub struct SubscriptExpr {
    pub base: Expr,
    pub index: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrayWildcardSubscriptExpr {
    pub base: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DereferenceExpr {
    pub base: Expr,
    pub field: Option<Ident>,
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

/// Ordered operands of one syntactic AND or OR expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalExpr {
    pub operator: LogicalOperator,
    pub terms: AstVec<Expr>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
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
}
