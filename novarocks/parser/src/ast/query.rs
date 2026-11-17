// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Typed query syntax. These nodes intentionally retain grammar facts only;
//! binding, type checking, and capability admission belong to later owners.

use crate::{
    Span,
    ast::{AstVec, Expr, Ident, relation::TableWithJoins, window::WindowDefinition},
};

/// One top-level query expression and its query-level modifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Query {
    /// Overrides belonging to the root query, before its CTEs and body.
    pub session_properties: Vec<SessionProperty>,
    pub functions: Vec<super::FunctionSpecification>,
    pub with: Option<With>,
    pub body: Box<SetExpr>,
    pub modifiers: QueryModifiers,
    pub span: Span,
}

/// Suffix clauses owned by either a query or its direct specification.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryModifiers {
    pub order_by: Option<OrderBy>,
    pub offset: Option<Offset>,
    pub limit: Option<QueryLimit>,
}

impl QueryModifiers {
    pub fn is_empty(&self) -> bool {
        self.order_by.is_none() && self.offset.is_none() && self.limit.is_none()
    }
}

/// A query-local session assignment. Evaluation belongs to statement admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionProperty {
    pub name: super::ObjectName,
    pub value: Expr,
    pub span: Span,
}

/// A query EXPLAIN wrapper. It never grants a production execution route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainQuery {
    pub format: ExplainFormat,
    /// Whether the EXPLAIN targets the logical planning view. This remains
    /// separate from output detail so `EXPLAIN LOGICAL VERBOSE` is typed.
    pub logical: bool,
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainFormat {
    Default,
    Analyze,
    Verbose,
    Costs,
    Logical,
    /// The plan contract itself rather than the operators it describes.
    ///
    /// EXPLAIN answers what a statement will do, which is the operator tree.
    /// What the plan states to the backend -- every value and expression it
    /// defines, the partition spaces, the filter attachments, the cut
    /// evidence -- is a different question, asked separately.
    Contract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct With {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cte {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SetExpr {
    Select(Box<Select>),
    Values(Values),
    Query(Box<Query>),
    SetOperation(SetOperation),
}

impl SetExpr {
    pub const fn span(&self) -> Span {
        match self {
            Self::Select(select) => select.span,
            Self::Values(values) => values.span,
            Self::Query(query) => query.span,
            Self::SetOperation(operation) => operation.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetOperation {
    pub inputs: AstVec<SetExpr>,
    pub operator: SetOperator,
    pub distinct: bool,
    pub corresponding: Option<Corresponding>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Corresponding {
    pub columns: Vec<Ident>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Values {
    pub rows: Vec<Vec<Expr>>,
    pub explicit_row: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Select {
    pub quantifier: SelectQuantifier,
    pub projection: Vec<SelectItem>,
    pub from: Vec<TableWithJoins>,
    pub selection: Option<Expr>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Expr>,
    pub qualify: Option<Expr>,
    pub windows: AstVec<WindowDefinition>,
    pub modifiers: QueryModifiers,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectQuantifier {
    None,
    All(Span),
    Distinct { on: Vec<Expr>, span: Span },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectItem {
    UnnamedExpr(Expr),
    ExprWithAlias {
        expr: Expr,
        alias: Ident,
        explicit_as: bool,
        span: Span,
    },
    Wildcard {
        options: WildcardOptions,
        span: Span,
    },
    QualifiedWildcard {
        prefix: Vec<Ident>,
        options: WildcardOptions,
        span: Span,
    },
}

impl SelectItem {
    pub fn span(&self) -> Span {
        match self {
            Self::UnnamedExpr(expr) => expr.span(),
            Self::ExprWithAlias { span, .. }
            | Self::Wildcard { span, .. }
            | Self::QualifiedWildcard { span, .. } => *span,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WildcardOptions {
    pub exclude: Vec<Ident>,
    pub replace: Vec<ReplaceSelectItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplaceSelectItem {
    pub expr: Expr,
    pub alias: Ident,
    pub span: Span,
}

/// One GROUP BY clause; empty element lists remain structurally legal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupBy {
    pub distinct: bool,
    pub elements: AstVec<GroupingElement>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupingElement {
    Simple(SimpleGroupBy),
    Sets(GroupingSets),
    Auto(AutoGroupBy),
}

impl GroupingElement {
    pub fn span(&self) -> Span {
        match self {
            Self::Simple(node) => node.span,
            Self::Sets(node) => node.span,
            Self::Auto(node) => node.span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimpleGroupBy {
    pub columns: AstVec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupingSets {
    pub kind: GroupingSetsKind,
    pub sets: AstVec<AstVec<Expr>>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupingSetsKind {
    Explicit,
    Rollup,
    Cube,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoGroupBy {
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBy {
    pub items: AstVec<SortItem>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortItem {
    pub sort_key: Expr,
    pub ordering: SortOrdering,
    pub null_ordering: NullOrdering,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortOrdering {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullOrdering {
    First,
    Last,
    Undefined,
}

/// The mutually exclusive Trino row-count clauses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryLimit {
    Limit(Limit),
    FetchFirst(FetchFirst),
}
impl QueryLimit {
    pub const fn span(&self) -> Span {
        match self {
            Self::Limit(node) => node.span,
            Self::FetchFirst(node) => node.span,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Limit {
    pub row_count: Expr,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Offset {
    pub row_count: Expr,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchFirst {
    pub row_count: Option<Expr>,
    pub with_ties: bool,
    pub span: Span,
}
impl Limit {
    pub const fn span(&self) -> Span {
        self.span
    }
}
impl Offset {
    pub const fn span(&self) -> Span {
        self.span
    }
}
impl FetchFirst {
    pub const fn span(&self) -> Span {
        self.span
    }
}
