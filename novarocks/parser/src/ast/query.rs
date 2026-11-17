// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Typed query syntax. These nodes intentionally retain grammar facts only;
//! binding, type checking, and capability admission belong to later owners.

use crate::{
    Span,
    ast::{
        AstBox, AstVec, Expr, Ident,
        relation::{Relation, Table, TableSubquery},
        window::WindowDefinition,
    },
};

/// One top-level query expression and its query-level modifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Query {
    /// Overrides belonging to the root query, before its CTEs and body.
    pub session_properties: AstVec<SessionProperty>,
    pub functions: AstVec<super::FunctionSpecification>,
    pub with: Option<With>,
    pub body: AstBox<SetExpr>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct With {
    pub recursive: bool,
    pub ctes: AstVec<Cte>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cte {
    pub name: Ident,
    pub columns: Option<Vec<Ident>>,
    pub query: AstBox<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SetExpr {
    Select(AstBox<Select>),
    Values(Values),
    Table(AstBox<Table>),
    Query(AstBox<TableSubquery>),
    SetOperation(SetOperation),
}

impl SetExpr {
    pub fn span(&self) -> Span {
        match self {
            Self::Select(select) => select.span,
            Self::Values(values) => values.span,
            Self::Table(table) => table.span,
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
    pub rows: AstVec<Expr>,
    pub span: Span,
}

/// A neutral borrowed view of the actual row syntax. It grants no analysis
/// capability and never expands an inner struct-valued cell or clones a child.
#[derive(Clone, Copy)]
pub enum ValuesRowCells<'a> {
    Row(&'a super::RowExpr),
    Single(&'a Expr),
}
impl<'a> ValuesRowCells<'a> {
    pub fn new(row: &'a Expr) -> Self {
        match row {
            Expr::Row(row) => Self::Row(row),
            other => Self::Single(other),
        }
    }
    pub fn len(&self) -> usize {
        match *self {
            Self::Row(row) => row.fields.len(),
            Self::Single(_) => 1,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn get(&self, index: usize) -> Option<&'a Expr> {
        match *self {
            Self::Row(row) => row.fields.get(index).map(|field| &field.expression),
            Self::Single(expr) => (index == 0).then_some(expr),
        }
    }
    pub fn iter(self) -> impl ExactSizeIterator<Item = &'a Expr> + 'a {
        (0..self.len()).map(move |index| self.get(index).expect("bounded VALUES cell index"))
    }
    pub fn has_named_fields(&self) -> bool {
        matches!(*self,Self::Row(row)if row.fields.iter().any(|field|field.name.is_some()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub projection: AstVec<SelectItem>,
    pub from: Option<Relation>,
    pub selection: Option<Expr>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Expr>,
    pub qualify: Option<Expr>,
    pub windows: AstVec<WindowDefinition>,
    pub modifiers: QueryModifiers,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectItem {
    SingleColumn(AstBox<SingleColumn>),
    AllColumns(AstBox<AllColumns>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SingleColumn {
    pub expression: Expr,
    pub alias: Option<Ident>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllColumns {
    pub target: Option<Expr>,
    pub aliases: Vec<Ident>,
    pub span: Span,
}
impl SelectItem {
    pub fn span(&self) -> Span {
        match self {
            Self::SingleColumn(node) => node.span,
            Self::AllColumns(node) => node.span,
        }
    }
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
    pub fn span(&self) -> Span {
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
    pub fn span(&self) -> Span {
        self.span
    }
}
impl Offset {
    pub fn span(&self) -> Span {
        self.span
    }
}
impl FetchFirst {
    pub fn span(&self) -> Span {
        self.span
    }
}
