// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! FROM, relation and join syntax nodes.

use crate::{
    Span,
    ast::{Expr, Ident, ObjectName, Query},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableFactor {
    Table {
        name: ObjectName,
        /// A parser-owned `$metadata` suffix on the final unquoted table-name
        /// part. The parser preserves the spelling but does not decide whether
        /// the suffix is supported by the resolved catalog or provider.
        metadata: Option<Ident>,
        alias: Option<TableAlias>,
        version: Option<TableVersion>,
        span: Span,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
        span: Span,
    },
    TableFunction {
        lateral: bool,
        syntax: TableFunctionSyntax,
        expr: Expr,
        alias: Option<TableAlias>,
        span: Span,
    },
    Unnest {
        keyword: Ident,
        lateral: bool,
        array_exprs: Vec<Expr>,
        with_offset: bool,
        alias: Option<TableAlias>,
        span: Span,
    },
    NestedJoin {
        table_with_joins: Box<TableWithJoins>,
        alias: Option<TableAlias>,
        span: Span,
    },
}

/// The source form used for a table-valued function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableFunctionSyntax {
    TableWrapper,
    BareCall,
}

impl TableFactor {
    pub const fn span(&self) -> Span {
        match self {
            Self::Table { span, .. }
            | Self::Derived { span, .. }
            | Self::TableFunction { span, .. }
            | Self::Unnest { span, .. }
            | Self::NestedJoin { span, .. } => *span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub explicit_as: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableVersion {
    pub kind: TableVersionKind,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableVersionKind {
    ForSystemTimeAsOf,
    ForVersionAsOf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Join {
    pub relation: TableFactor,
    pub operator: JoinOperator,
    pub constraint: JoinConstraint,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinOperator {
    Inner,
    InnerExplicit,
    LeftOuter,
    LeftOuterExplicit,
    RightOuter,
    RightOuterExplicit,
    FullOuter,
    FullOuterExplicit,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JoinConstraint {
    None,
    On(Expr),
    Using { columns: Vec<Ident>, span: Span },
    Natural(Span),
}
