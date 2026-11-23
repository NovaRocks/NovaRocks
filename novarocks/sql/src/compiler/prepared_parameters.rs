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

//! Source occurrences and argument expressions for one prepared statement.
// Design: ADR-0258 (docs/adr/ADR-0258-prepared-sql-request-snapshots-and-semantic-authors.md)

use std::{collections::HashMap, sync::Arc};

use novarocks_functions::ConstantValue;
use novarocks_parser::{
    Span,
    ast::{self, Fold, Visit},
};
use novarocks_type_contract::{CompilePhase, PureCompileControl, TemporalValue, ValueKind};

use super::{RequiredConstantInput, RequiredConstantTarget, SqlCompileControl, SqlCompileError};
use crate::analyze_error::AnalyzeError;

#[derive(Clone, Debug)]
pub struct SqlPreparedParameters {
    positions: Box<[Span]>,
    arguments: Option<Box<[ast::Expr]>>,
    argument_sql: Option<Arc<str>>,
    contextual_values: HashMap<Span, ast::LiteralKind>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedParameterError {
    Arity { expected: usize, actual: usize },
    NonConstant(Span),
    UnboundArgument(Span),
}

impl SqlPreparedParameters {
    /// Enumerates surviving parameter nodes by their exact positions in this source.
    pub fn positions_in(statement: &ast::TrinoStatement) -> Vec<Span> {
        let mut positions = ParameterPositions(Vec::new());
        positions.visit_trino_statement(statement);
        positions.into_lexical_order()
    }

    pub fn bind(
        statement: &ast::TrinoStatement,
        arguments: Vec<ast::Expr>,
        argument_sql: Arc<str>,
    ) -> Result<Self, PreparedParameterError> {
        let positions = Self::positions_in(statement);
        if arguments.len() != positions.len() {
            return Err(PreparedParameterError::Arity {
                expected: positions.len(),
                actual: arguments.len(),
            });
        }
        for argument in &arguments {
            let mut check = ArgumentCheck::default();
            check.visit_expr(argument);
            if let Some(span) = check.unbound {
                return Err(PreparedParameterError::UnboundArgument(span));
            }
            if check.column_reference {
                return Err(PreparedParameterError::NonConstant(argument.span()));
            }
        }
        Ok(Self {
            positions: positions.into_boxed_slice(),
            arguments: Some(arguments.into_boxed_slice()),
            argument_sql: Some(argument_sql),
            contextual_values: HashMap::new(),
        })
    }

    pub fn describe(statement: &ast::TrinoStatement) -> Self {
        Self {
            positions: Self::positions_in(statement).into_boxed_slice(),
            arguments: None,
            argument_sql: None,
            contextual_values: HashMap::new(),
        }
    }

    pub fn describe_query(query: &ast::Query) -> Self {
        let mut positions = ParameterPositions(Vec::new());
        positions.visit_query(query);
        Self {
            positions: positions.into_lexical_order().into_boxed_slice(),
            arguments: None,
            argument_sql: None,
            contextual_values: HashMap::new(),
        }
    }

    pub const fn is_description(&self) -> bool {
        self.arguments.is_none()
    }

    pub fn positions(&self) -> &[Span] {
        &self.positions
    }

    pub fn ordinal(&self, span: Span) -> Option<usize> {
        self.positions
            .binary_search_by_key(&(span.start(), span.end()), |span| {
                (span.start(), span.end())
            })
            .ok()
    }

    pub fn first_in_expression(&self, expression: &ast::Expr) -> Option<Span> {
        let mut positions = ParameterPositions(Vec::new());
        positions.visit_expr(expression);
        positions
            .into_lexical_order()
            .into_iter()
            .find(|&span| self.ordinal(span).is_some())
    }

    pub fn argument_at(&self, span: Span) -> Option<&ast::Expr> {
        self.arguments.as_deref()?.get(self.ordinal(span)?)
    }

    pub fn argument_sql(&self) -> Option<&Arc<str>> {
        self.argument_sql.as_ref()
    }

    /// Observe the contextual expressions requiring scalar work before catalog
    /// selection. Ordinary expressions stay deferred to their exact use sites.
    pub fn contextual_needs(&self, query: &ast::Query) -> Vec<PreparedContextNeed<'_>> {
        let mut collector = ContextPlaces {
            parameters: self,
            needs: Vec::new(),
        };
        collector.visit_query(query);
        collector
            .needs
            .sort_unstable_by_key(|need| need.span().start());
        collector.needs.dedup_by_key(|need| need.span());
        collector.needs
    }

    /// Accept one exact round of original evaluated values. Check every
    /// result against the SQL use before publishing the completed bindings.
    pub fn with_contextual_values(
        &self,
        query: &ast::Query,
        values: Vec<ConstantValue>,
        control: &SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let needs = self.contextual_needs(query);
        if self.is_description() || needs.len() != values.len() {
            return Err(SqlCompileError::InvalidRequest(
                "Prepared contextual facts do not match the statement needs".into(),
            ));
        }
        let mut completed = self.clone();
        for (need, value) in needs.into_iter().zip(values) {
            control.checkpoint(CompilePhase::Validate, 1)?;
            completed
                .contextual_values
                .insert(need.span(), need.finish(&value, control)?);
        }
        Ok(completed)
    }

    /// Resolve only the context-specific inputs checked by the original SQL
    /// authors. The rest of the tree retains its parameter nodes.
    pub fn complete_query(&self, query: ast::Query) -> Result<ast::Query, SqlCompileError> {
        for need in self.contextual_needs(&query) {
            if self.is_description() && need.is_row_count() {
                continue;
            }
            if self.is_description() {
                return Err(AnalyzeError::unsupported_query_shape(
                    "DESCRIBE requires a bound table version",
                    need.span(),
                )
                .into());
            }
            return Err(SqlCompileError::InvalidRequest(
                "A prepared contextual requirement has no evaluated fact".into(),
            ));
        }
        if self.contextual_values.is_empty() {
            return Ok(query);
        }
        Ok(ContextValues(&self.contextual_values).fold_query(query))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContextKind {
    Limit,
    Offset,
    Version,
    Timestamp,
}

pub struct PreparedContextNeed<'a> {
    expression: ast::Expr,
    kind: ContextKind,
    parameters: &'a SqlPreparedParameters,
}

impl PreparedContextNeed<'_> {
    pub fn span(&self) -> Span {
        self.expression.span()
    }

    pub fn is_row_count(&self) -> bool {
        matches!(self.kind, ContextKind::Limit | ContextKind::Offset)
    }

    pub fn input(&self) -> RequiredConstantInput<'_> {
        RequiredConstantInput::PreparedExpression {
            expression: &self.expression,
            parameters: self.parameters,
        }
    }

    pub fn target(&self) -> RequiredConstantTarget {
        if self.is_row_count() {
            RequiredConstantTarget::PreparedRowCount
        } else {
            RequiredConstantTarget::PreparedTableSelection
        }
    }

    fn finish(
        &self,
        value: &ConstantValue,
        control: &SqlCompileControl,
    ) -> Result<ast::LiteralKind, SqlCompileError> {
        let phase = CompilePhase::Validate;
        let long = |value: i64| ast::LiteralKind::Long {
            spelling: value.to_string(),
            parsed_value: value,
        };
        if self.is_row_count() {
            let keyword = if self.kind == ContextKind::Limit {
                "LIMIT"
            } else {
                "OFFSET"
            };
            let count = value.int64_observed(phase, control)?.ok_or_else(|| {
                AnalyzeError::invalid_argument(
                    format!("{keyword} count must not be NULL"),
                    self.span(),
                )
            })?;
            if count < 0 {
                return Err(AnalyzeError::numeric_out_of_range(
                    format!("{keyword} count must not be negative"),
                    self.span(),
                )
                .into());
            }
            return Ok(long(count));
        }
        let value_type = value.value_type().value_type();
        if self.kind == ContextKind::Timestamp && value_type.temporal().is_some() {
            match value.temporal_observed(phase, control)? {
                Some(TemporalValue::Timestamp(value)) => return Ok(long(value.floor_millis())),
                Some(TemporalValue::TimestampWithTimeZone { instant, .. }) => {
                    return Ok(long(instant.floor_millis()));
                }
                _ => {}
            }
        }
        if matches!(
            value_type.kind(),
            ValueKind::String | ValueKind::BoundedCharacter(_)
        ) && let Some(text) = value.utf8_observed(phase, control)?
        {
            return Ok(ast::LiteralKind::String(text.to_owned()));
        }
        if value_type.temporal().is_none()
            && value.value_type().logical_type()
                == novarocks_type_contract::ValueLogicalType::Physical
            && let Some(value) = value.signed_integer_observed(phase, control)?
        {
            return Ok(long(value));
        }
        Err(AnalyzeError::type_mismatch(
            "Prepared table selection must have a non-null snapshot ID, reference name, or timestamp",
            self.span(),
        ).into())
    }
}

struct ContextPlaces<'a> {
    parameters: &'a SqlPreparedParameters,
    needs: Vec<PreparedContextNeed<'a>>,
}

impl ContextPlaces<'_> {
    fn visit_contextual_expression(&mut self, expression: &ast::Expr, kind: ContextKind) {
        if !self
            .parameters
            .contextual_values
            .contains_key(&expression.span())
        {
            let mut markers = ParameterPositions(Vec::new());
            markers.visit_expr(expression);
            if markers
                .0
                .iter()
                .any(|&span| self.parameters.ordinal(span).is_some())
            {
                self.needs.push(PreparedContextNeed {
                    expression: expression.clone(),
                    kind,
                    parameters: self.parameters,
                });
            }
        }
        self.visit_expr(expression);
    }
}

impl Visit for ContextPlaces<'_> {
    fn visit_limit(&mut self, node: &ast::Limit) {
        self.visit_contextual_expression(&node.row_count, ContextKind::Limit);
    }

    fn visit_offset(&mut self, node: &ast::Offset) {
        self.visit_contextual_expression(&node.row_count, ContextKind::Offset);
    }

    fn visit_query_period(&mut self, node: &ast::QueryPeriod) {
        if let Ok(expression) = crate::planning::catalog::query_period_end(node) {
            let kind = match node.range_type {
                ast::QueryPeriodRangeType::Version => ContextKind::Version,
                ast::QueryPeriodRangeType::Timestamp => ContextKind::Timestamp,
            };
            self.visit_contextual_expression(expression, kind);
        }
    }
}

struct ContextValues<'a>(&'a HashMap<Span, ast::LiteralKind>);

impl Fold for ContextValues<'_> {
    fn fold_expr(&mut self, expression: ast::Expr) -> ast::Expr {
        let span = expression.span();
        match self.0.get(&span) {
            Some(kind) => ast::Expr::Literal(ast::Literal {
                kind: kind.clone(),
                span,
            }),
            None => ast::fold_expr(self, expression),
        }
    }
}

struct ParameterPositions(Vec<Span>);

impl ParameterPositions {
    fn into_lexical_order(mut self) -> Vec<Span> {
        self.0
            .sort_unstable_by_key(|span| (span.start(), span.end()));
        self.0.dedup();
        self.0
    }
}

impl Visit for ParameterPositions {
    fn visit_parameter(&mut self, node: &ast::Parameter) {
        self.0.push(node.span);
    }
}

#[derive(Default)]
struct ArgumentCheck {
    column_reference: bool,
    unbound: Option<Span>,
}

impl Visit for ArgumentCheck {
    fn visit_expr(&mut self, expression: &ast::Expr) {
        if matches!(expression, ast::Expr::Identifier(_)) {
            self.column_reference = true;
        } else {
            ast::walk_expr(self, expression);
        }
    }

    fn visit_parameter(&mut self, node: &ast::Parameter) {
        self.unbound.get_or_insert(node.span);
    }
}
