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

//! Catalog-free structural checks for typed SQL syntax.
//!
//! Validation deliberately runs after parsing. It protects AST construction
//! invariants and rejects only facts knowable without name resolution, type
//! checking, catalog access, or function capability admission.

use std::collections::HashSet;

use crate::{StructuralViolation, ValidateError};

use super::{
    Cte, Expr, GroupBy, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableStatement,
    TableWithJoins, Values, WindowFrame, WindowFrameBound, WindowSpec,
};

/// Validates every statement in a public parser result.
pub fn validate_statements(statements: &[Statement]) -> Result<(), ValidateError> {
    for statement in statements {
        validate_statement(statement)?;
    }
    Ok(())
}

/// Validates one complete syntax tree without invoking semantic analysis.
pub fn validate_statement(statement: &Statement) -> Result<(), ValidateError> {
    match statement {
        Statement::Query(query) => validate_query(query),
        Statement::ExplainQuery(explain) => validate_query(&explain.query),
        Statement::Table(TableStatement::Create(table)) => validate_create_table(table),
        Statement::ShowBackends(_)
        | Statement::ShowProcessList(_)
        | Statement::Statistics(_)
        | Statement::Catalog(_)
        | Statement::Iceberg(_)
        | Statement::Maintenance(_)
        | Statement::MaterializedView(_)
        | Statement::View(_)
        | Statement::Dml(_) => Ok(()),
        Statement::Session(_) => Ok(()),
    }
}

fn validate_create_table(table: &super::CreateTable) -> Result<(), ValidateError> {
    let mut names = HashSet::with_capacity(table.columns.len());
    for column in &table.columns {
        if !names.insert(identifier_key(&column.name.value, column.name.quoted)) {
            return Err(ValidateError::invalid_structure(
                StructuralViolation::DuplicateTableColumnName,
                column.name.span,
            ));
        }
    }
    Ok(())
}

fn validate_query(query: &Query) -> Result<(), ValidateError> {
    if let Some(with) = &query.with {
        if with.ctes.is_empty() {
            return Err(ValidateError::invalid_structure(
                StructuralViolation::EmptyWithCteList,
                with.span,
            ));
        }
        validate_unique_ctes(&with.ctes)?;
        for cte in &with.ctes {
            validate_query(&cte.query)?;
        }
    }
    validate_set_expr(&query.body)?;
    for order_by in &query.order_by {
        validate_expr(&order_by.expr)?;
    }
    if let Some(limit) = &query.limit {
        validate_expr(limit)?;
    }
    if let Some(offset) = &query.offset {
        validate_expr(&offset.value)?;
    }
    if let Some(fetch) = &query.fetch
        && let Some(quantity) = &fetch.quantity
    {
        validate_expr(quantity)?;
    }
    Ok(())
}

fn validate_unique_ctes(ctes: &[Cte]) -> Result<(), ValidateError> {
    let mut names = HashSet::with_capacity(ctes.len());
    for cte in ctes {
        if !names.insert(identifier_key(&cte.name.value, cte.name.quoted)) {
            return Err(ValidateError::duplicate_cte_name(
                cte.name.value.clone(),
                cte.name.span,
            ));
        }
    }
    Ok(())
}

fn validate_set_expr(set_expr: &SetExpr) -> Result<(), ValidateError> {
    match set_expr {
        SetExpr::Select(select) => validate_select(select),
        SetExpr::Values(values) => validate_values(values),
        SetExpr::Query(query) => validate_query(query),
        SetExpr::SetOperation(operation) => {
            validate_set_expr(&operation.left)?;
            validate_set_expr(&operation.right)
        }
    }
}

fn validate_values(values: &Values) -> Result<(), ValidateError> {
    if values.rows.is_empty() {
        return Err(ValidateError::invalid_structure(
            StructuralViolation::EmptyValuesRowList,
            values.span,
        ));
    }
    for row in &values.rows {
        if row.is_empty() {
            return Err(ValidateError::invalid_structure(
                StructuralViolation::EmptyValuesRow,
                values.span,
            ));
        }
        for expression in row {
            validate_expr(expression)?;
        }
    }
    Ok(())
}

fn validate_select(select: &Select) -> Result<(), ValidateError> {
    for hint in &select.hints {
        match &hint.value {
            super::SelectHintValue::Bare => {}
            super::SelectHintValue::Call { arguments } => {
                for argument in arguments {
                    validate_expr(argument)?;
                }
            }
            super::SelectHintValue::Assignment { value } => validate_expr(value)?,
        }
    }
    if select.projection.is_empty() {
        return Err(ValidateError::invalid_structure(
            StructuralViolation::EmptySelectProjection,
            select.span,
        ));
    }
    match &select.quantifier {
        super::SelectQuantifier::Distinct { on, .. } => {
            for expression in on {
                validate_expr(expression)?;
            }
        }
        super::SelectQuantifier::None | super::SelectQuantifier::All(_) => {}
    }
    for item in &select.projection {
        validate_select_item(item)?;
    }
    let mut names = HashSet::with_capacity(select.windows.len());
    for window in &select.windows {
        if !names.insert(identifier_key(&window.name.value, window.name.quoted)) {
            return Err(ValidateError::duplicate_window_name(
                window.name.value.clone(),
                window.name.span,
            ));
        }
        validate_window_spec(&window.specification)?;
    }
    for relation in &select.from {
        validate_table_with_joins(relation)?;
    }
    if let Some(selection) = &select.selection {
        validate_expr(selection)?;
    }
    if let Some(having) = &select.having {
        validate_expr(having)?;
    }
    if let Some(qualify) = &select.qualify {
        validate_expr(qualify)?;
    }
    validate_group_by(&select.group_by)?;
    Ok(())
}

fn validate_select_item(item: &SelectItem) -> Result<(), ValidateError> {
    match item {
        SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } => validate_expr(expression),
        SelectItem::Wildcard { options, .. } | SelectItem::QualifiedWildcard { options, .. } => {
            for replacement in &options.replace {
                validate_expr(&replacement.expr)?;
            }
            Ok(())
        }
    }
}

fn validate_group_by(group_by: &GroupBy) -> Result<(), ValidateError> {
    match group_by {
        GroupBy::None => Ok(()),
        GroupBy::Expressions { expressions, .. }
        | GroupBy::Rollup { expressions, .. }
        | GroupBy::Cube { expressions, .. } => {
            for expression in expressions {
                validate_expr(expression)?;
            }
            Ok(())
        }
        GroupBy::GroupingSets { sets, .. } => {
            for set in sets {
                for expression in set {
                    validate_expr(expression)?;
                }
            }
            Ok(())
        }
    }
}

fn validate_table_with_joins(table_with_joins: &TableWithJoins) -> Result<(), ValidateError> {
    validate_table_factor(&table_with_joins.relation)?;
    for join in &table_with_joins.joins {
        validate_table_factor(&join.relation)?;
        if let super::JoinConstraint::On(expression) = &join.constraint {
            validate_expr(expression)?;
        }
    }
    Ok(())
}

fn validate_table_factor(table_factor: &TableFactor) -> Result<(), ValidateError> {
    match table_factor {
        TableFactor::Table { version, .. } => {
            if let Some(version) = version {
                validate_expr(&version.value)?;
            }
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => validate_query(subquery),
        TableFactor::TableFunction { expr, .. } => validate_expr(expr),
        TableFactor::Unnest { array_exprs, .. } => {
            if array_exprs.is_empty() {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::EmptyUnnestExpressionList,
                    table_factor.span(),
                ));
            }
            for expression in array_exprs {
                validate_expr(expression)?;
            }
            Ok(())
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => validate_table_with_joins(table_with_joins),
    }
}

fn validate_window_spec(specification: &WindowSpec) -> Result<(), ValidateError> {
    for expression in &specification.partition_by {
        validate_expr(expression)?;
    }
    for order_by in &specification.order_by {
        validate_expr(&order_by.expr)?;
    }
    if let Some(frame) = &specification.window_frame {
        validate_window_frame(frame)?;
    }
    Ok(())
}

fn validate_window_frame(frame: &WindowFrame) -> Result<(), ValidateError> {
    validate_window_frame_bound(&frame.start_bound)?;
    if let Some(end_bound) = &frame.end_bound {
        validate_window_frame_bound(end_bound)?;
        let start = bound_position(&frame.start_bound);
        let end = bound_position(end_bound);
        if !start.can_precede(end) {
            return Err(ValidateError::invalid_window_frame_bounds(frame.span));
        }
    }
    if matches!(frame.start_bound, WindowFrameBound::Following(None, _)) {
        return Err(ValidateError::invalid_window_frame_bounds(
            frame.start_bound.span(),
        ));
    }
    if let Some(end_bound @ WindowFrameBound::Preceding(None, _)) = &frame.end_bound {
        return Err(ValidateError::invalid_window_frame_bounds(end_bound.span()));
    }
    Ok(())
}

fn validate_window_frame_bound(bound: &WindowFrameBound) -> Result<(), ValidateError> {
    match bound {
        WindowFrameBound::Preceding(Some(expression), _)
        | WindowFrameBound::Following(Some(expression), _) => validate_expr(expression),
        WindowFrameBound::CurrentRow(_)
        | WindowFrameBound::Preceding(None, _)
        | WindowFrameBound::Following(None, _) => Ok(()),
    }
}

/// Partial ordering known from frame-bound syntax alone.
///
/// Two bounded PRECEDING or bounded FOLLOWING expressions cannot be ordered
/// without evaluating expressions, so they deliberately remain comparable to
/// each other here. Analyze owns any stronger numeric rule.
#[derive(Clone, Copy)]
enum FramePosition {
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
}

impl FramePosition {
    const fn can_precede(self, other: Self) -> bool {
        use FramePosition::*;
        matches!(
            (self, other),
            (UnboundedPreceding, _)
                | (
                    Preceding,
                    Preceding | CurrentRow | Following | UnboundedFollowing
                )
                | (CurrentRow, CurrentRow | Following | UnboundedFollowing)
                | (Following, Following | UnboundedFollowing)
                | (UnboundedFollowing, UnboundedFollowing)
        )
    }
}

fn bound_position(bound: &WindowFrameBound) -> FramePosition {
    match bound {
        WindowFrameBound::Preceding(None, _) => FramePosition::UnboundedPreceding,
        WindowFrameBound::Preceding(Some(_), _) => FramePosition::Preceding,
        WindowFrameBound::CurrentRow(_) => FramePosition::CurrentRow,
        WindowFrameBound::Following(Some(_), _) => FramePosition::Following,
        WindowFrameBound::Following(None, _) => FramePosition::UnboundedFollowing,
    }
}

fn validate_expr(expression: &Expr) -> Result<(), ValidateError> {
    // The query validator only needs to descend into syntax nodes that can
    // contain a Query or WindowSpec. Other expression-local invariants are
    // enforced by their parser production and remain valid after construction.
    match expression {
        Expr::FunctionCall(function) => {
            for argument in &function.arguments {
                validate_expr(argument)?;
            }
            for order_by in &function.order_by {
                validate_expr(&order_by.expr)?;
            }
            if let Some(separator) = &function.separator {
                validate_expr(separator)?;
            }
            if let Some(filter) = &function.filter {
                validate_expr(filter)?;
            }
            if let Some(window) = &function.over {
                validate_window_spec(window)?;
            }
        }
        Expr::Subquery(subquery) => validate_query(&subquery.query)?,
        Expr::Exists(exists) => validate_query(&exists.query)?,
        Expr::InSubquery(in_subquery) => {
            validate_expr(&in_subquery.expr)?;
            validate_query(&in_subquery.query)?;
        }
        Expr::Unary(unary) => validate_expr(&unary.expression)?,
        Expr::Binary(binary) => {
            validate_expr(&binary.left)?;
            validate_expr(&binary.right)?;
        }
        Expr::Nested(nested) => validate_expr(&nested.expression)?,
        Expr::Between(between) => {
            validate_expr(&between.expr)?;
            validate_expr(&between.low)?;
            validate_expr(&between.high)?;
        }
        Expr::InList(in_list) => {
            validate_expr(&in_list.expr)?;
            for item in &in_list.list {
                validate_expr(item)?;
            }
        }
        Expr::Like(like) => {
            validate_expr(&like.expr)?;
            validate_expr(&like.pattern)?;
            if let Some(escape) = &like.escape {
                validate_expr(escape)?;
            }
        }
        Expr::IsPredicate(predicate) => validate_expr(&predicate.expr)?,
        Expr::Case(case) => {
            if case.conditions.len() != case.results.len() {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::MismatchedCaseArms,
                    case.span,
                ));
            }
            if let Some(operand) = &case.operand {
                validate_expr(operand)?;
            }
            for condition in &case.conditions {
                validate_expr(condition)?;
            }
            for result in &case.results {
                validate_expr(result)?;
            }
            if let Some(else_result) = &case.else_result {
                validate_expr(else_result)?;
            }
        }
        Expr::Cast(cast) => {
            validate_expr(&cast.expr)?;
            if let Some(format) = &cast.format {
                validate_expr(format)?;
            }
        }
        Expr::Interval(interval) => {
            validate_expr(&interval.value)?;
            if let Some(precision) = &interval.leading_precision {
                validate_expr(precision)?;
            }
            if let Some(precision) = &interval.fractional_seconds_precision {
                validate_expr(precision)?;
            }
        }
        Expr::Tuple(tuple) => {
            for expression in &tuple.expressions {
                validate_expr(expression)?;
            }
        }
        Expr::Array(array) => {
            for expression in &array.elements {
                validate_expr(expression)?;
            }
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                validate_expr(&entry.key)?;
                validate_expr(&entry.value)?;
            }
        }
        Expr::Struct(struct_expr) => {
            for field in &struct_expr.fields {
                validate_expr(&field.value)?;
            }
        }
        Expr::Lambda(lambda) => validate_expr(&lambda.body)?,
        Expr::Access(access) => {
            validate_expr(&access.expr)?;
            match &access.kind {
                super::AccessKind::Field(_) => {}
                super::AccessKind::Subscript(index) => validate_expr(index)?,
                super::AccessKind::Json { path, .. } => validate_expr(path)?,
            }
        }
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Literal(_)
        | Expr::TypedString(_) => {}
    }
    Ok(())
}

fn identifier_key(value: &str, quoted: bool) -> String {
    if quoted {
        format!("quoted:{value}")
    } else {
        format!("unquoted:{}", value.to_ascii_lowercase())
    }
}
