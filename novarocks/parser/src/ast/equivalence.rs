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

//! Span-insensitive structural equality for the SQLP-4 capability AST.
//!
//! This module compares syntax facts directly. In particular, it does not
//! canonicalize through the printer, and it never compares source offsets.

use super::*;

/// Structural syntax equality that deliberately excludes every source span.
pub trait SyntaxEq {
    /// Returns whether two nodes encode the same syntax facts.
    fn syntax_eq(&self, other: &Self) -> bool;
}

/// Compares two typed query trees without comparing their source spans.
pub fn syntax_eq_query(left: &Query, right: &Query) -> bool {
    left.syntax_eq(right)
}

/// Compares two typed expression trees without comparing their source spans.
pub fn syntax_eq_expr(left: &Expr, right: &Expr) -> bool {
    left.syntax_eq(right)
}

/// Compares two typed query EXPLAIN wrappers without comparing source spans.
pub fn syntax_eq_explain_query(left: &ExplainQuery, right: &ExplainQuery) -> bool {
    left.syntax_eq(right)
}

impl SyntaxEq for Ident {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value == other.value
            && self.quoted == other.quoted
            && self.quote_style == other.quote_style
    }
}

impl SyntaxEq for ObjectName {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.parts, &other.parts)
    }
}

impl SyntaxEq for Literal {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl SyntaxEq for TypeName {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && syntax_eq_slice(&self.arguments, &other.arguments)
            && self.argument_separator_spaces == other.argument_separator_spaces
    }
}

impl SyntaxEq for TypeNameArgument {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Type(left), Self::Type(right)) => left.syntax_eq(right),
            (Self::Literal(left), Self::Literal(right)) => left.syntax_eq(right),
            (Self::Field(left), Self::Field(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for StructField {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.data_type.syntax_eq(&other.data_type)
    }
}

impl SyntaxEq for ExplainQuery {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.format == other.format
            && self.logical == other.logical
            && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for Query {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.with, &other.with)
            && self.body.syntax_eq(&other.body)
            && syntax_eq_slice(&self.order_by, &other.order_by)
            && syntax_eq_option(&self.limit, &other.limit)
            && syntax_eq_option(&self.offset, &other.offset)
            && self.limit_comma_offset == other.limit_comma_offset
            && syntax_eq_option(&self.fetch, &other.fetch)
    }
}

impl SyntaxEq for With {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.recursive == other.recursive && syntax_eq_slice(&self.ctes, &other.ctes)
    }
}

impl SyntaxEq for Cte {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && syntax_eq_slice(&self.columns, &other.columns)
            && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for SetExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Select(left), Self::Select(right)) => left.syntax_eq(right),
            (Self::Values(left), Self::Values(right)) => left.syntax_eq(right),
            (Self::Query(left), Self::Query(right)) => left.syntax_eq(right),
            (Self::SetOperation(left), Self::SetOperation(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for SetOperation {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.left.syntax_eq(&other.left)
            && self.operator == other.operator
            && self.quantifier == other.quantifier
            && self.right.syntax_eq(&other.right)
    }
}

impl SyntaxEq for Values {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.explicit_row == other.explicit_row && syntax_eq_nested_slice(&self.rows, &other.rows)
    }
}

impl SyntaxEq for Select {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.hints, &other.hints)
            && self.quantifier.syntax_eq(&other.quantifier)
            && syntax_eq_slice(&self.projection, &other.projection)
            && syntax_eq_slice(&self.from, &other.from)
            && syntax_eq_option(&self.selection, &other.selection)
            && self.group_by.syntax_eq(&other.group_by)
            && syntax_eq_option(&self.having, &other.having)
            && syntax_eq_option(&self.qualify, &other.qualify)
            && syntax_eq_slice(&self.windows, &other.windows)
    }
}

impl SyntaxEq for SelectHint {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for SelectHintValue {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bare, Self::Bare) => true,
            (Self::Call { arguments: left }, Self::Call { arguments: right }) => {
                syntax_eq_slice(left, right)
            }
            (Self::Assignment { value: left }, Self::Assignment { value: right }) => {
                left.syntax_eq(right)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for SelectQuantifier {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None) | (Self::All(_), Self::All(_)) => true,
            (Self::Distinct { on: left, .. }, Self::Distinct { on: right, .. }) => {
                syntax_eq_slice(left, right)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for SelectItem {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnnamedExpr(left), Self::UnnamedExpr(right)) => left.syntax_eq(right),
            (
                Self::ExprWithAlias {
                    expr: left_expr,
                    alias: left_alias,
                    explicit_as: left_explicit_as,
                    ..
                },
                Self::ExprWithAlias {
                    expr: right_expr,
                    alias: right_alias,
                    explicit_as: right_explicit_as,
                    ..
                },
            ) => {
                left_expr.syntax_eq(right_expr)
                    && left_alias.syntax_eq(right_alias)
                    && left_explicit_as == right_explicit_as
            }
            (Self::Wildcard { options: left, .. }, Self::Wildcard { options: right, .. }) => {
                left.syntax_eq(right)
            }
            (
                Self::QualifiedWildcard {
                    prefix: left_prefix,
                    options: left_options,
                    ..
                },
                Self::QualifiedWildcard {
                    prefix: right_prefix,
                    options: right_options,
                    ..
                },
            ) => {
                syntax_eq_slice(left_prefix, right_prefix) && left_options.syntax_eq(right_options)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for WildcardOptions {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.exclude, &other.exclude)
            && syntax_eq_slice(&self.replace, &other.replace)
    }
}

impl SyntaxEq for ReplaceSelectItem {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr) && self.alias.syntax_eq(&other.alias)
    }
}

impl SyntaxEq for GroupBy {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None) => true,
            (
                Self::Expressions {
                    expressions: left, ..
                },
                Self::Expressions {
                    expressions: right, ..
                },
            )
            | (
                Self::Rollup {
                    expressions: left, ..
                },
                Self::Rollup {
                    expressions: right, ..
                },
            )
            | (
                Self::Cube {
                    expressions: left, ..
                },
                Self::Cube {
                    expressions: right, ..
                },
            ) => syntax_eq_slice(left, right),
            (Self::GroupingSets { sets: left, .. }, Self::GroupingSets { sets: right, .. }) => {
                syntax_eq_nested_slice(left, right)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for OrderByExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.asc == other.asc
            && self.nulls_first == other.nulls_first
    }
}

impl SyntaxEq for Offset {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value) && self.rows == other.rows
    }
}

impl SyntaxEq for Fetch {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.quantity, &other.quantity)
            && self.percent == other.percent
            && self.with_ties == other.with_ties
    }
}

impl SyntaxEq for TableWithJoins {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.relation.syntax_eq(&other.relation) && syntax_eq_slice(&self.joins, &other.joins)
    }
}

impl SyntaxEq for TableFactor {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Table {
                    name: left_name,
                    metadata: left_metadata,
                    alias: left_alias,
                    version: left_version,
                    ..
                },
                Self::Table {
                    name: right_name,
                    metadata: right_metadata,
                    alias: right_alias,
                    version: right_version,
                    ..
                },
            ) => {
                left_name.syntax_eq(right_name)
                    && syntax_eq_option(left_metadata, right_metadata)
                    && syntax_eq_option(left_alias, right_alias)
                    && syntax_eq_option(left_version, right_version)
            }
            (
                Self::Derived {
                    lateral: left_lateral,
                    subquery: left_query,
                    alias: left_alias,
                    ..
                },
                Self::Derived {
                    lateral: right_lateral,
                    subquery: right_query,
                    alias: right_alias,
                    ..
                },
            ) => {
                left_lateral == right_lateral
                    && left_query.syntax_eq(right_query)
                    && syntax_eq_option(left_alias, right_alias)
            }
            (
                Self::TableFunction {
                    lateral: left_lateral,
                    syntax: left_syntax,
                    expr: left_expr,
                    alias: left_alias,
                    ..
                },
                Self::TableFunction {
                    lateral: right_lateral,
                    syntax: right_syntax,
                    expr: right_expr,
                    alias: right_alias,
                    ..
                },
            ) => {
                left_lateral == right_lateral
                    && left_syntax == right_syntax
                    && left_expr.syntax_eq(right_expr)
                    && syntax_eq_option(left_alias, right_alias)
            }
            (
                Self::Unnest {
                    keyword: left_keyword,
                    lateral: left_lateral,
                    array_exprs: left_exprs,
                    with_offset: left_offset,
                    alias: left_alias,
                    ..
                },
                Self::Unnest {
                    keyword: right_keyword,
                    lateral: right_lateral,
                    array_exprs: right_exprs,
                    with_offset: right_offset,
                    alias: right_alias,
                    ..
                },
            ) => {
                left_keyword.syntax_eq(right_keyword)
                    && left_lateral == right_lateral
                    && syntax_eq_slice(left_exprs, right_exprs)
                    && left_offset == right_offset
                    && syntax_eq_option(left_alias, right_alias)
            }
            (
                Self::NestedJoin {
                    table_with_joins: left_relation,
                    alias: left_alias,
                    ..
                },
                Self::NestedJoin {
                    table_with_joins: right_relation,
                    alias: right_alias,
                    ..
                },
            ) => {
                left_relation.syntax_eq(right_relation) && syntax_eq_option(left_alias, right_alias)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for TableAlias {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && syntax_eq_slice(&self.columns, &other.columns)
            && self.explicit_as == other.explicit_as
    }
}

impl SyntaxEq for TableVersion {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for Join {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.relation.syntax_eq(&other.relation)
            && self.operator == other.operator
            && self.constraint.syntax_eq(&other.constraint)
    }
}

impl SyntaxEq for JoinConstraint {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None) | (Self::Natural(_), Self::Natural(_)) => true,
            (Self::On(left), Self::On(right)) => left.syntax_eq(right),
            (Self::Using { columns: left, .. }, Self::Using { columns: right, .. }) => {
                syntax_eq_slice(left, right)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for NamedWindow {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.specification.syntax_eq(&other.specification)
    }
}

impl SyntaxEq for WindowSpec {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.existing_window_name, &other.existing_window_name)
            && syntax_eq_slice(&self.partition_by, &other.partition_by)
            && syntax_eq_slice(&self.order_by, &other.order_by)
            && syntax_eq_option(&self.window_frame, &other.window_frame)
    }
}

impl SyntaxEq for WindowFrame {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.units == other.units
            && self.start_bound.syntax_eq(&other.start_bound)
            && syntax_eq_option(&self.end_bound, &other.end_bound)
            && self.exclusion == other.exclusion
    }
}

impl SyntaxEq for WindowFrameBound {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CurrentRow(_), Self::CurrentRow(_)) => true,
            (Self::Preceding(left, _), Self::Preceding(right, _))
            | (Self::Following(left, _), Self::Following(right, _)) => {
                syntax_eq_option(left, right)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for Expr {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Identifier(left), Self::Identifier(right)) => left.syntax_eq(right),
            (Self::CompoundIdentifier(left), Self::CompoundIdentifier(right)) => {
                syntax_eq_slice(&left.parts, &right.parts)
            }
            (Self::Literal(left), Self::Literal(right)) => left.syntax_eq(right),
            (Self::FunctionCall(left), Self::FunctionCall(right)) => left.syntax_eq(right),
            (Self::Unary(left), Self::Unary(right)) => left.syntax_eq(right),
            (Self::Binary(left), Self::Binary(right)) => left.syntax_eq(right),
            (Self::Nested(left), Self::Nested(right)) => left.syntax_eq(right),
            (Self::Between(left), Self::Between(right)) => left.syntax_eq(right),
            (Self::InList(left), Self::InList(right)) => left.syntax_eq(right),
            (Self::InSubquery(left), Self::InSubquery(right)) => left.syntax_eq(right),
            (Self::Exists(left), Self::Exists(right)) => left.syntax_eq(right),
            (Self::Like(left), Self::Like(right)) => left.syntax_eq(right),
            (Self::IsPredicate(left), Self::IsPredicate(right)) => left.syntax_eq(right),
            (Self::Case(left), Self::Case(right)) => left.syntax_eq(right),
            (Self::Cast(left), Self::Cast(right)) => left.syntax_eq(right),
            (Self::Interval(left), Self::Interval(right)) => left.syntax_eq(right),
            (Self::Subquery(left), Self::Subquery(right)) => left.syntax_eq(right),
            (Self::Tuple(left), Self::Tuple(right)) => left.syntax_eq(right),
            (Self::Array(left), Self::Array(right)) => left.syntax_eq(right),
            (Self::Map(left), Self::Map(right)) => left.syntax_eq(right),
            (Self::Struct(left), Self::Struct(right)) => left.syntax_eq(right),
            (Self::Lambda(left), Self::Lambda(right)) => left.syntax_eq(right),
            (Self::Access(left), Self::Access(right)) => left.syntax_eq(right),
            (Self::TypedString(left), Self::TypedString(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for FunctionCall {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && syntax_eq_slice(&self.arguments, &other.arguments)
            && self.quantifier == other.quantifier
            && syntax_eq_slice(&self.order_by, &other.order_by)
            && syntax_eq_option(&self.separator, &other.separator)
            && syntax_eq_option(&self.filter, &other.filter)
            && self.null_treatment == other.null_treatment
            && syntax_eq_option(&self.over, &other.over)
            && self.substring_from_syntax == other.substring_from_syntax
    }
}

impl SyntaxEq for FunctionOrderBy {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.asc == other.asc
            && self.nulls_first == other.nulls_first
    }
}

impl SyntaxEq for UnaryExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.expression.syntax_eq(&other.expression)
    }
}

impl SyntaxEq for BinaryExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.left.syntax_eq(&other.left)
            && self.operator == other.operator
            && self.right.syntax_eq(&other.right)
    }
}

impl SyntaxEq for NestedExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
    }
}

impl SyntaxEq for BetweenExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.negated == other.negated
            && self.low.syntax_eq(&other.low)
            && self.high.syntax_eq(&other.high)
    }
}

impl SyntaxEq for InListExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.negated == other.negated
            && syntax_eq_slice(&self.list, &other.list)
    }
}

impl SyntaxEq for InSubqueryExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.negated == other.negated
            && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for ExistsExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for LikeExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.negated == other.negated
            && self.operator == other.operator
            && self.pattern.syntax_eq(&other.pattern)
            && syntax_eq_option(&self.escape, &other.escape)
    }
}

impl SyntaxEq for IsPredicateExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr) && self.predicate == other.predicate
    }
}

impl SyntaxEq for CaseExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.operand, &other.operand)
            && syntax_eq_slice(&self.conditions, &other.conditions)
            && syntax_eq_slice(&self.results, &other.results)
            && syntax_eq_option(&self.else_result, &other.else_result)
    }
}

impl SyntaxEq for CastExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr)
            && self.data_type.syntax_eq(&other.data_type)
            && self.kind == other.kind
            && syntax_eq_option(&self.format, &other.format)
    }
}

impl SyntaxEq for IntervalExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value)
            && self.leading_field == other.leading_field
            && syntax_eq_option(&self.leading_precision, &other.leading_precision)
            && self.last_field == other.last_field
            && syntax_eq_option(
                &self.fractional_seconds_precision,
                &other.fractional_seconds_precision,
            )
    }
}

impl SyntaxEq for SubqueryExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for TupleExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.expressions, &other.expressions)
    }
}

impl SyntaxEq for ArrayExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (&self.element_type, &other.element_type) {
            (Some(left), Some(right)) if !left.syntax_eq(right) => false,
            (None, None) | (Some(_), Some(_)) => syntax_eq_slice(&self.elements, &other.elements),
            _ => false,
        }
    }
}

impl SyntaxEq for MapExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.entries, &other.entries)
    }
}

impl SyntaxEq for MapEntry {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.key.syntax_eq(&other.key) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for StructExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.fields, &other.fields)
    }
}

impl SyntaxEq for StructExprField {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.name, &other.name) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for LambdaExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.parameters, &other.parameters)
            && self.parenthesized_single_parameter == other.parenthesized_single_parameter
            && self.body.syntax_eq(&other.body)
    }
}

impl SyntaxEq for AccessExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expr.syntax_eq(&other.expr) && self.kind.syntax_eq(&other.kind)
    }
}

impl SyntaxEq for AccessKind {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Field(left), Self::Field(right)) => left.syntax_eq(right),
            (Self::Subscript(left), Self::Subscript(right)) => left.syntax_eq(right),
            (
                Self::Json {
                    operator: left_operator,
                    path: left_path,
                },
                Self::Json {
                    operator: right_operator,
                    path: right_path,
                },
            ) => left_operator == right_operator && left_path.syntax_eq(right_path),
            _ => false,
        }
    }
}

impl SyntaxEq for TypedStringExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.data_type.syntax_eq(&other.data_type) && self.value.syntax_eq(&other.value)
    }
}

impl<T: SyntaxEq> SyntaxEq for Box<T> {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.as_ref().syntax_eq(other.as_ref())
    }
}

fn syntax_eq_option<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}

fn syntax_eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}

fn syntax_eq_nested_slice<T: SyntaxEq>(left: &[Vec<T>], right: &[Vec<T>]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| syntax_eq_slice(left, right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;

    fn span(start: usize) -> Span {
        Span::new(start, start + 1)
    }

    fn ident(value: &str, start: usize) -> Ident {
        Ident {
            value: value.to_owned(),
            quoted: false,
            quote_style: None,
            span: span(start),
        }
    }

    fn number(value: &str, start: usize) -> Expr {
        Expr::Literal(Literal {
            kind: LiteralKind::Number(value.to_owned()),
            span: span(start),
        })
    }

    fn sample_expr(start: usize) -> Expr {
        Expr::FunctionCall(FunctionCall {
            name: ObjectName {
                parts: vec![ident("catalog", start), ident("f", start + 1)],
                span: span(start + 2),
            },
            arguments: vec![Expr::Access(AccessExpr {
                expr: Box::new(Expr::CompoundIdentifier(CompoundIdentifier {
                    parts: vec![ident("t", start + 3), ident("payload", start + 4)],
                    span: span(start + 5),
                })),
                kind: AccessKind::Json {
                    operator: JsonOperator::ArrowText,
                    path: Box::new(Expr::Literal(Literal {
                        kind: LiteralKind::String("name".to_owned()),
                        span: span(start + 6),
                    })),
                },
                span: span(start + 7),
            })],
            quantifier: FunctionQuantifier::Distinct,
            order_by: vec![FunctionOrderBy {
                expr: number("1", start + 8),
                asc: Some(false),
                nulls_first: Some(false),
                span: span(start + 9),
            }],
            separator: Some(Box::new(Expr::Literal(Literal {
                kind: LiteralKind::String(",".to_owned()),
                span: span(start + 10),
            }))),
            filter: Some(Box::new(Expr::IsPredicate(IsPredicateExpr {
                expr: Box::new(Expr::Identifier(ident("present", start + 11))),
                predicate: IsPredicate::NotNull,
                span: span(start + 12),
            }))),
            null_treatment: Some(NullTreatment::IgnoreNulls),
            over: Some(Box::new(WindowSpec {
                existing_window_name: Some(ident("base", start + 13)),
                partition_by: vec![Expr::Identifier(ident("k", start + 14))],
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier(ident("ts", start + 15)),
                    asc: Some(true),
                    nulls_first: Some(false),
                    span: span(start + 16),
                }],
                window_frame: Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(
                        Some(number("2", start + 17)),
                        span(start + 18),
                    ),
                    end_bound: Some(WindowFrameBound::CurrentRow(span(start + 19))),
                    exclusion: WindowFrameExclusion::Ties,
                    span: span(start + 20),
                }),
                span: span(start + 21),
            })),
            substring_from_syntax: false,
            span: span(start + 22),
        })
    }

    fn sample_query(start: usize) -> Query {
        Query {
            with: Some(With {
                recursive: true,
                ctes: vec![Cte {
                    name: ident("seed", start),
                    columns: vec![ident("value", start + 1)],
                    query: Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Values(Values {
                            rows: vec![vec![number("1", start + 2)]],
                            explicit_row: true,
                            span: span(start + 3),
                        })),
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                        limit_comma_offset: false,
                        fetch: None,
                        span: span(start + 4),
                    }),
                    span: span(start + 5),
                }],
                span: span(start + 6),
            }),
            body: Box::new(SetExpr::Select(Box::new(Select {
                hints: Vec::new(),
                quantifier: SelectQuantifier::Distinct {
                    on: vec![sample_expr(start + 7)],
                    span: span(start + 8),
                },
                projection: vec![SelectItem::ExprWithAlias {
                    expr: sample_expr(start + 9),
                    alias: ident("result", start + 10),
                    explicit_as: true,
                    span: span(start + 11),
                }],
                from: Vec::new(),
                selection: None,
                group_by: GroupBy::Expressions {
                    expressions: vec![Expr::Identifier(ident("k", start + 12))],
                    span: span(start + 13),
                },
                having: None,
                qualify: None,
                windows: Vec::new(),
                span: span(start + 14),
            }))),
            order_by: vec![OrderByExpr {
                expr: Expr::Identifier(ident("result", start + 15)),
                asc: Some(false),
                nulls_first: Some(true),
                span: span(start + 16),
            }],
            limit: Some(number("10", start + 17)),
            offset: Some(Offset {
                value: number("3", start + 18),
                rows: OffsetRows::Rows,
                span: span(start + 19),
            }),
            limit_comma_offset: false,
            fetch: Some(Fetch {
                quantity: Some(number("4", start + 20)),
                percent: true,
                with_ties: true,
                span: span(start + 21),
            }),
            span: span(start + 22),
        }
    }

    #[test]
    fn recursive_comparison_ignores_every_span() {
        let left = sample_query(0);
        let right = sample_query(1_000);

        assert_ne!(left, right);
        assert!(syntax_eq_query(&left, &right));
        assert!(left.syntax_eq(&right));
    }

    #[test]
    fn recursive_comparison_preserves_nested_syntax_facts() {
        let left = sample_query(0);
        let mut right = sample_query(1_000);
        right.fetch.as_mut().expect("fixture has fetch").with_ties = false;

        assert!(!syntax_eq_query(&left, &right));
    }

    #[test]
    fn expression_comparison_distinguishes_non_span_changes() {
        let left = sample_expr(0);
        let mut right = sample_expr(1_000);
        let Expr::FunctionCall(call) = &mut right else {
            panic!("fixture must remain a function call");
        };
        call.null_treatment = Some(NullTreatment::RespectNulls);

        assert!(!syntax_eq_expr(&left, &right));
    }
}
