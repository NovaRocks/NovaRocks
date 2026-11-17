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
        self.value == other.value && self.quoted == other.quoted
    }
}

impl SyntaxEq for ObjectName {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.parts, &other.parts)
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
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.session_properties, &other.session_properties)
            && syntax_eq_slice(&self.functions, &other.functions)
            && syntax_eq_option(&self.with, &other.with)
            && self.body.syntax_eq(&other.body)
            && self.modifiers.syntax_eq(&other.modifiers)
    }
}

impl SyntaxEq for QueryModifiers {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.order_by, &other.order_by)
            && syntax_eq_option(&self.limit, &other.limit)
            && syntax_eq_option(&self.offset, &other.offset)
    }
}

impl SyntaxEq for SessionProperty {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for With {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.recursive == other.recursive && syntax_eq_slice(&self.ctes, &other.ctes)
    }
}

impl SyntaxEq for Cte {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && match (&self.columns, &other.columns) {
                (Some(a), Some(b)) => syntax_eq_slice(a, b),
                (None, None) => true,
                _ => false,
            }
            && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for SetExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Select(left), Self::Select(right)) => left.syntax_eq(right),
            (Self::Values(left), Self::Values(right)) => left.syntax_eq(right),
            (Self::Table(left), Self::Table(right)) => left.syntax_eq(right),
            (Self::Query(left), Self::Query(right)) => left.syntax_eq(right),
            (Self::SetOperation(left), Self::SetOperation(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for SetOperation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.inputs, &other.inputs)
            && self.operator == other.operator
            && self.distinct == other.distinct
            && syntax_eq_option(&self.corresponding, &other.corresponding)
    }
}

impl SyntaxEq for Corresponding {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.columns, &other.columns)
    }
}

impl SyntaxEq for Values {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.rows, &other.rows)
    }
}

impl SyntaxEq for Select {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.distinct == other.distinct
            && syntax_eq_slice(&self.projection, &other.projection)
            && syntax_eq_option(&self.from, &other.from)
            && syntax_eq_option(&self.selection, &other.selection)
            && syntax_eq_option(&self.group_by, &other.group_by)
            && syntax_eq_option(&self.having, &other.having)
            && syntax_eq_option(&self.qualify, &other.qualify)
            && syntax_eq_slice(&self.windows, &other.windows)
            && self.modifiers.syntax_eq(&other.modifiers)
    }
}

impl SyntaxEq for SelectItem {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SingleColumn(a), Self::SingleColumn(b)) => a.syntax_eq(b),
            (Self::AllColumns(a), Self::AllColumns(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}
impl SyntaxEq for SingleColumn {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression) && syntax_eq_option(&self.alias, &other.alias)
    }
}
impl SyntaxEq for AllColumns {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.target, &other.target)
            && syntax_eq_slice(&self.aliases, &other.aliases)
    }
}

impl SyntaxEq for GroupBy {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.distinct == other.distinct && syntax_eq_slice(&self.elements, &other.elements)
    }
}
impl SyntaxEq for GroupingElement {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple(left), Self::Simple(right)) => left.syntax_eq(right),
            (Self::Sets(left), Self::Sets(right)) => left.syntax_eq(right),
            (Self::Auto(left), Self::Auto(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}
impl SyntaxEq for SimpleGroupBy {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.columns, &other.columns)
    }
}
impl SyntaxEq for GroupingSets {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.sets.len() == other.sets.len()
            && self
                .sets
                .iter()
                .zip(&other.sets)
                .all(|(left, right)| syntax_eq_slice(left, right))
    }
}
impl SyntaxEq for AutoGroupBy {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}

impl SyntaxEq for OrderBy {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.items, &other.items)
    }
}

impl SyntaxEq for SortItem {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.sort_key.syntax_eq(&other.sort_key)
            && self.ordering == other.ordering
            && self.null_ordering == other.null_ordering
    }
}

impl SyntaxEq for QueryLimit {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Limit(left), Self::Limit(right)) => left.syntax_eq(right),
            (Self::FetchFirst(left), Self::FetchFirst(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}
impl SyntaxEq for Limit {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.row_count.syntax_eq(&other.row_count)
    }
}
impl SyntaxEq for Offset {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.row_count.syntax_eq(&other.row_count)
    }
}
impl SyntaxEq for FetchFirst {
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.row_count, &other.row_count) && self.with_ties == other.with_ties
    }
}
impl SyntaxEq for AllRows {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}
impl SyntaxEq for Parameter {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}

impl SyntaxEq for WindowDefinition {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.specification.syntax_eq(&other.specification)
    }
}

impl SyntaxEq for WindowSpecification {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.existing_window_name, &other.existing_window_name)
            && syntax_eq_slice(&self.partition_by, &other.partition_by)
            && syntax_eq_option(&self.order_by, &other.order_by)
            && syntax_eq_option(&self.window_frame, &other.window_frame)
    }
}

impl SyntaxEq for WindowFrame {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.units == other.units
            && self.start_bound.syntax_eq(&other.start_bound)
            && syntax_eq_option(&self.end_bound, &other.end_bound)
            && self.exclusion == other.exclusion
            && syntax_eq_slice(&self.measures, &other.measures)
            && syntax_eq_option(&self.after_match_skip_to, &other.after_match_skip_to)
            && syntax_eq_option(&self.pattern_search_mode, &other.pattern_search_mode)
            && syntax_eq_option(&self.pattern, &other.pattern)
            && syntax_eq_slice(&self.subsets, &other.subsets)
            && syntax_eq_slice(&self.variable_definitions, &other.variable_definitions)
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
            (Self::AtLocal(a), Self::AtLocal(b)) => a.syntax_eq(b),
            (Self::AtTimeZone(a), Self::AtTimeZone(b)) => a.syntax_eq(b),
            (Self::GroupingOperation(a), Self::GroupingOperation(b)) => a.syntax_eq(b),
            (Self::Trim(a), Self::Trim(b)) => a.syntax_eq(b),
            (Self::Overlay(a), Self::Overlay(b)) => a.syntax_eq(b),
            (Self::IntervalValueExpression(a), Self::IntervalValueExpression(b)) => a.syntax_eq(b),
            (Self::MethodCall(a), Self::MethodCall(b)) => a.syntax_eq(b),
            (Self::StaticMethodCall(a), Self::StaticMethodCall(b)) => a.syntax_eq(b),
            (Self::CurrentCatalog(a), Self::CurrentCatalog(b)) => a.syntax_eq(b),
            (Self::CurrentDate(a), Self::CurrentDate(b)) => a.syntax_eq(b),
            (Self::CurrentPath(a), Self::CurrentPath(b)) => a.syntax_eq(b),
            (Self::CurrentSchema(a), Self::CurrentSchema(b)) => a.syntax_eq(b),
            (Self::CurrentUser(a), Self::CurrentUser(b)) => a.syntax_eq(b),
            (Self::CurrentTime(a), Self::CurrentTime(b)) => a.syntax_eq(b),
            (Self::CurrentTimestamp(a), Self::CurrentTimestamp(b)) => a.syntax_eq(b),
            (Self::LocalTime(a), Self::LocalTime(b)) => a.syntax_eq(b),
            (Self::LocalTimestamp(a), Self::LocalTimestamp(b)) => a.syntax_eq(b),

            (Self::JsonConstructor(left), Self::JsonConstructor(right)) => left.syntax_eq(right),
            (Self::JsonExists(left), Self::JsonExists(right)) => left.syntax_eq(right),
            (Self::JsonValue(left), Self::JsonValue(right)) => left.syntax_eq(right),
            (Self::JsonQuery(left), Self::JsonQuery(right)) => left.syntax_eq(right),
            (Self::JsonSerialize(left), Self::JsonSerialize(right)) => left.syntax_eq(right),
            (Self::JsonObject(left), Self::JsonObject(right)) => left.syntax_eq(right),
            (Self::JsonArray(left), Self::JsonArray(right)) => left.syntax_eq(right),

            (Self::Identifier(left), Self::Identifier(right)) => left.syntax_eq(right),
            (Self::AllRows(left), Self::AllRows(right)) => left.syntax_eq(right),
            (Self::Parameter(left), Self::Parameter(right)) => left.syntax_eq(right),
            (Self::Literal(left), Self::Literal(right)) => left.syntax_eq(right),
            (Self::FunctionCall(left), Self::FunctionCall(right)) => left.syntax_eq(right),
            (Self::Extract(left), Self::Extract(right)) => left.syntax_eq(right),
            (Self::If(left), Self::If(right)) => left.syntax_eq(right),
            (Self::NullIf(left), Self::NullIf(right)) => left.syntax_eq(right),
            (Self::Coalesce(left), Self::Coalesce(right)) => left.syntax_eq(right),
            (Self::Try(left), Self::Try(right)) => left.syntax_eq(right),
            (Self::Format(left), Self::Format(right)) => left.syntax_eq(right),

            (Self::WindowOperation(left), Self::WindowOperation(right)) => left.syntax_eq(right),
            (Self::Unary(left), Self::Unary(right)) => left.syntax_eq(right),
            (Self::Binary(left), Self::Binary(right)) => left.syntax_eq(right),
            (Self::Logical(left), Self::Logical(right)) => left.syntax_eq(right),
            (Self::Exists(left), Self::Exists(right)) => left.syntax_eq(right),
            (Self::Unique(left), Self::Unique(right)) => left.syntax_eq(right),
            (Self::Cast(left), Self::Cast(right)) => left.syntax_eq(right),
            (Self::Lambda(left), Self::Lambda(right)) => left.syntax_eq(right),
            (Self::SearchedCase(left), Self::SearchedCase(right)) => left.syntax_eq(right),
            (Self::SimpleCase(left), Self::SimpleCase(right)) => left.syntax_eq(right),
            (Self::Predicated(left), Self::Predicated(right)) => left.syntax_eq(right),
            (Self::InList(left), Self::InList(right)) => left.syntax_eq(right),
            (Self::Nested(left), Self::Nested(right)) => left.syntax_eq(right),
            (Self::Interval(left), Self::Interval(right)) => left.syntax_eq(right),
            (Self::Subquery(left), Self::Subquery(right)) => left.syntax_eq(right),
            (Self::Row(left), Self::Row(right)) => left.syntax_eq(right),
            (Self::Array(left), Self::Array(right)) => left.syntax_eq(right),
            (Self::Subscript(left), Self::Subscript(right)) => left.syntax_eq(right),
            (Self::ArrayWildcardSubscript(left), Self::ArrayWildcardSubscript(right)) => {
                left.syntax_eq(right)
            }
            (Self::Dereference(left), Self::Dereference(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for FunctionCall {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && syntax_eq_slice(&self.arguments, &other.arguments)
            && syntax_eq_option(&self.processing_mode, &other.processing_mode)
            && self.distinct == other.distinct
            && syntax_eq_option(&self.order_by, &other.order_by)
            && syntax_eq_option(&self.filter, &other.filter)
            && self.null_treatment == other.null_treatment
            && syntax_eq_option(&self.window, &other.window)
    }
}

impl SyntaxEq for UnaryExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.expression.syntax_eq(&other.expression)
    }
}

impl SyntaxEq for LogicalExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.operator == other.operator && syntax_eq_slice(&self.terms, &other.terms)
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

impl SyntaxEq for RowExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.fields, &other.fields)
    }
}

impl SyntaxEq for RowField {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.name, &other.name) && self.expression.syntax_eq(&other.expression)
    }
}

impl SyntaxEq for ArrayExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_slice(&self.values, &other.values)
    }
}

impl SyntaxEq for SubscriptExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.base.syntax_eq(&other.base) && self.index.syntax_eq(&other.index)
    }
}
impl SyntaxEq for ArrayWildcardSubscriptExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.base.syntax_eq(&other.base)
    }
}
impl SyntaxEq for DereferenceExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.base.syntax_eq(&other.base) && syntax_eq_option(&self.field, &other.field)
    }
}

impl<T: SyntaxEq> SyntaxEq for AstBox<T> {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.as_ref().syntax_eq(other.as_ref())
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
            span: span(start),
        }
    }

    fn number(value: &str, start: usize) -> Expr {
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                spelling: value.to_owned(),
                parsed_value: (value.to_owned()).parse::<i64>().unwrap(),
            },
            span: span(start),
        })
    }

    fn sample_expr(start: usize) -> Expr {
        Expr::FunctionCall(AstBox::new(FunctionCall {
            name: ObjectName {
                parts: vec![ident("catalog", start), ident("f", start + 1)],
                span: span(start + 2),
            },
            arguments: vec![
                {
                    let mut bridge = crate::build::function(
                        ObjectName {
                            parts: vec![crate::build::ident("json_query", false)],
                            span: crate::SYNTHETIC,
                        },
                        vec![
                            Expr::Dereference(AstBox::new(DereferenceExpr {
                                base: Expr::Identifier(ident("t", start + 3)),
                                field: Some(ident("payload", start + 4)),
                                span: span(start + 5),
                            })),
                            Expr::Literal(Literal {
                                kind: LiteralKind::String("name".to_owned()),
                                span: span(start + 6),
                            }),
                        ],
                    );
                    let Expr::FunctionCall(call) = &mut bridge else {
                        unreachable!()
                    };
                    call.span = span(start + 7);
                    bridge
                },
                Expr::Literal(Literal {
                    kind: LiteralKind::String(",".to_owned()),
                    span: span(start + 10),
                }),
            ]
            .into_iter()
            .map(|value| crate::build::call_argument(value, None))
            .collect(),
            processing_mode: None,
            distinct: true,
            order_by: Some(OrderBy {
                items: vec![SortItem {
                    sort_key: number("1", start + 8),
                    ordering: SortOrdering::Descending,
                    null_ordering: NullOrdering::Last,
                    span: span(start + 9),
                }]
                .into(),
                span: span(start + 9),
            }),
            filter: Some(Expr::Predicated(AstBox::new(PredicatedExpr {
                value: Expr::Identifier(ident("present", start + 11)),
                predicate: Predicate::IsNull(IsNullPredicate {
                    negated: true,
                    span: span(start + 12),
                }),
                span: span(start + 12),
            }))),
            null_treatment: Some(NullTreatment::IgnoreNulls),
            window: Some(Window::Specification(AstBox::new(WindowSpecification {
                existing_window_name: Some(ident("base", start + 13)),
                partition_by: vec![Expr::Identifier(ident("k", start + 14))].into(),
                order_by: Some(OrderBy {
                    items: vec![SortItem {
                        sort_key: Expr::Identifier(ident("ts", start + 15)),
                        ordering: SortOrdering::Ascending,
                        null_ordering: NullOrdering::Last,
                        span: span(start + 16),
                    }]
                    .into(),
                    span: span(start + 16),
                }),
                window_frame: Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(
                        Some(number("2", start + 17)),
                        span(start + 18),
                    ),
                    end_bound: Some(WindowFrameBound::CurrentRow(span(start + 19))),
                    exclusion: WindowFrameExclusion::Ties,
                    measures: AstVec::new(),
                    after_match_skip_to: None,
                    pattern_search_mode: None,
                    pattern: None,
                    subsets: AstVec::new(),
                    variable_definitions: AstVec::new(),
                    span: span(start + 20),
                }),
                span: span(start + 21),
            }))),
            span: span(start + 22),
        }))
    }

    fn sample_query(start: usize) -> Query {
        Query {
            session_properties: crate::ast::AstVec::new(),
            functions: crate::ast::AstVec::new(),
            with: Some(With {
                recursive: true,
                ctes: (vec![Cte {
                    name: ident("seed", start),
                    columns: Some(vec![ident("value", start + 1)]),
                    query: crate::ast::AstBox::new(Query {
                        session_properties: crate::ast::AstVec::new(),
                        functions: crate::ast::AstVec::new(),
                        with: None,
                        body: crate::ast::AstBox::new(SetExpr::Values(Values {
                            rows: (vec![vec![number("1", start + 2)]])
                                .into_iter()
                                .map(|cells| {
                                    crate::ast::Expr::Row(crate::ast::RowExpr {
                                        fields: cells
                                            .into_iter()
                                            .map(|expression| crate::ast::RowField {
                                                name: None,
                                                span: expression.span(),
                                                expression,
                                            })
                                            .collect(),
                                        span: span(start + 3),
                                    })
                                })
                                .collect(),
                            span: span(start + 3),
                        })),
                        modifiers: Default::default(),
                        span: span(start + 4),
                    }),
                    span: span(start + 5),
                }])
                .into(),
                span: span(start + 6),
            }),
            body: crate::ast::AstBox::new(SetExpr::Select(crate::ast::AstBox::new(Select {
                projection: (vec![crate::ast::SelectItem::SingleColumn(
                    crate::ast::AstBox::new(crate::ast::SingleColumn {
                        expression: sample_expr(start + 9),
                        alias: Some(ident("result", start + 10)),
                        span: span(start + 11),
                    }),
                )])
                .into(),
                from: None,
                selection: None,
                group_by: Some(GroupBy {
                    distinct: false,
                    elements: vec![GroupingElement::Simple(SimpleGroupBy {
                        columns: vec![Expr::Identifier(ident("k", start + 12))].into(),
                        span: span(start + 13),
                    })]
                    .into(),
                    span: span(start + 13),
                }),
                having: None,
                qualify: None,
                windows: AstVec::new(),
                modifiers: QueryModifiers {
                    order_by: Some(OrderBy {
                        items: vec![SortItem {
                            sort_key: Expr::Identifier(ident("result", start + 15)),
                            ordering: SortOrdering::Descending,
                            null_ordering: NullOrdering::First,
                            span: span(start + 16),
                        }]
                        .into(),
                        span: span(start + 16),
                    }),
                    limit: Some(QueryLimit::FetchFirst(FetchFirst {
                        row_count: Some(number("4", start + 20)),
                        with_ties: true,
                        span: span(start + 21),
                    })),
                    offset: Some(Offset {
                        row_count: number("3", start + 18),
                        span: span(start + 19),
                    }),
                },
                span: span(start + 14),
                distinct: true,
            }))),
            modifiers: Default::default(),
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
        let SetExpr::Select(select) = right.body.as_mut() else {
            panic!("SELECT")
        };
        let Some(QueryLimit::FetchFirst(fetch)) = select.modifiers.limit.as_mut() else {
            panic!("fixture has FETCH FIRST");
        };
        fetch.with_ties = false;

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

impl SyntaxEq for Window {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Reference(a), Self::Reference(b)) => a.syntax_eq(b),
            (Self::Specification(a), Self::Specification(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}
impl SyntaxEq for WindowReference {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}
impl SyntaxEq for WindowOperation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.window.syntax_eq(&other.window)
    }
}

impl SyntaxEq for CallArgument {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        syntax_eq_option(&self.name, &other.name) && self.value.syntax_eq(&other.value)
    }
}
impl SyntaxEq for ProcessingMode {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.mode == other.mode
    }
}
