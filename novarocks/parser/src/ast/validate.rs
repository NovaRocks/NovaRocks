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
    TableWithJoins, Values, WindowFrame, WindowFrameBound, WindowSpecification,
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
        Statement::ShowProcessList(_)
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
    if matches!(query.body.as_ref(), SetExpr::Select(_)) && !query.modifiers.is_empty() {
        return Err(ValidateError::invalid_structure(
            StructuralViolation::InvalidQueryModifierOwner,
            query.span,
        ));
    }
    for function in &query.functions {
        struct FunctionExpressions(Result<(), ValidateError>);
        impl super::Visit for FunctionExpressions {
            fn visit_expr(&mut self, expression: &Expr) {
                if self.0.is_ok() {
                    self.0 = validate_expr(expression);
                }
            }
        }
        let mut expressions = FunctionExpressions(Ok(()));
        super::Visit::visit_function_specification(&mut expressions, function);
        expressions.0?;
    }
    for property in &query.session_properties {
        validate_expr(&property.value)?;
    }
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
    validate_query_modifiers(&query.modifiers)?;
    Ok(())
}

fn validate_query_modifiers(modifiers: &super::QueryModifiers) -> Result<(), ValidateError> {
    if let Some(order_by) = &modifiers.order_by {
        validate_order_by(order_by)?;
    }
    if let Some(limit) = &modifiers.limit {
        validate_expr(limit)?;
    }
    if let Some(offset) = &modifiers.offset {
        validate_expr(&offset.value)?;
    }
    if let Some(fetch) = &modifiers.fetch
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
            if operation.inputs.len() < 2
                || (operation.operator == super::SetOperator::Except && operation.inputs.len() != 2)
            {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::InvalidSetOperationOperandCount,
                    operation.span,
                ));
            }
            for input in &operation.inputs {
                validate_set_expr(input)?;
            }
            Ok(())
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
        validate_window_specification(&window.specification)?;
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
    validate_query_modifiers(&select.modifiers)?;
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
        TableFactor::JsonTable { table, .. } => {
            validate_json_root(|visitor| super::Visit::visit_json_table(visitor, table))
        }

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

#[recursive::recursive]
fn validate_window_specification(specification: &WindowSpecification) -> Result<(), ValidateError> {
    for expression in &specification.partition_by {
        validate_expr(expression)?;
    }
    if let Some(order_by) = &specification.order_by {
        validate_order_by(order_by)?;
    }
    if let Some(frame) = &specification.window_frame {
        validate_window_frame(frame)?;
    }
    Ok(())
}

#[recursive::recursive]
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

    // Optional pattern clauses are syntax facts even when the analyzer cannot execute them.
    for measure in &frame.measures {
        validate_expr(&measure.expression)?;
    }
    if let Some(pattern) = &frame.pattern {
        validate_row_pattern(pattern)?;
    }
    for subset in &frame.subsets {
        if subset.identifiers.is_empty() {
            return Err(ValidateError::invalid_structure(
                StructuralViolation::EmptySubsetIdentifierList,
                subset.span,
            ));
        }
    }
    for variable in &frame.variable_definitions {
        validate_expr(&variable.expression)?;
    }
    Ok(())
}

#[recursive::recursive]
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

#[recursive::recursive]
fn validate_expr(expression: &Expr) -> Result<(), ValidateError> {
    // The query validator only needs to descend into syntax nodes that can
    // contain a Query or WindowSpecification. Other expression-local invariants are
    // enforced by their parser production and remain valid after construction.
    match expression {
        Expr::JsonConstructor(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_constructor(visitor, node))?
        }
        Expr::JsonExists(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_exists(visitor, node))?
        }
        Expr::JsonValue(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_value(visitor, node))?
        }
        Expr::JsonQuery(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_query(visitor, node))?
        }
        Expr::JsonSerialize(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_serialize(visitor, node))?
        }
        Expr::JsonObject(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_object(visitor, node))?
        }
        Expr::JsonArray(node) => {
            validate_json_root(|visitor| super::Visit::visit_json_array(visitor, node))?
        }

        Expr::If(node) => {
            validate_expr(&node.condition)?;
            validate_expr(&node.true_value)?;
            if let Some(value) = &node.false_value {
                validate_expr(value)?;
            }
        }
        Expr::NullIf(node) => {
            validate_expr(&node.first)?;
            validate_expr(&node.second)?;
        }
        Expr::Coalesce(node) => {
            if node.operands.len() < 2 {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::TooFewCoalesceOperands,
                    node.span,
                ));
            }
            for value in &node.operands {
                validate_expr(value)?;
            }
        }
        Expr::Try(node) => validate_expr(&node.inner)?,
        Expr::Format(node) => {
            if node.arguments.len() < 2 {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::TooFewFormatArguments,
                    node.span,
                ));
            }
            for value in &node.arguments {
                validate_expr(value)?;
            }
        }
        Expr::Extract(node) => validate_expr(&node.expression)?,
        Expr::WindowOperation(node) => validate_window(&node.window)?,
        Expr::FunctionCall(function) => {
            for argument in &function.arguments {
                validate_call_argument(argument)?;
            }
            if let Some(order_by) = &function.order_by {
                validate_order_by(order_by)?;
            }
            if let Some(separator) = &function.separator {
                validate_expr(separator)?;
            }
            if let Some(filter) = &function.filter {
                validate_expr(filter)?;
            }
            if let Some(window) = &function.window {
                validate_window(window)?;
            }
        }
        Expr::Subquery(subquery) => validate_query(&subquery.query)?,
        Expr::Exists(exists) => validate_expr(&exists.subquery)?,
        Expr::Unique(unique) => validate_expr(&unique.subquery)?,

        Expr::Unary(unary) => validate_expr(&unary.expression)?,
        Expr::Predicated(node) => {
            validate_expr(&node.value)?;
            struct Children(Result<(), ValidateError>);
            impl super::Visit for Children {
                fn visit_expr(&mut self, expression: &Expr) {
                    if self.0.is_ok() {
                        self.0 = validate_expr(expression);
                    }
                }
            }
            let mut children = Children(Ok(()));
            super::Visit::visit_predicate(&mut children, &node.predicate);
            children.0?;
        }
        Expr::InList(list) => {
            if list.values.is_empty() {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::EmptyInList,
                    list.span,
                ));
            }
            for value in &list.values {
                validate_expr(value)?;
            }
        }
        Expr::Logical(logical) => {
            if logical.terms.len() < 2 {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::InvalidLogicalTermCount,
                    logical.span,
                ));
            }
            for term in &logical.terms {
                validate_expr(term)?;
            }
        }
        Expr::Binary(binary) => {
            validate_expr(&binary.left)?;
            validate_expr(&binary.right)?;
        }
        Expr::Nested(nested) => validate_expr(&nested.expression)?,

        Expr::SearchedCase(case) => validate_case(
            None,
            &case.when_clauses,
            case.default_value.as_ref(),
            case.span,
        )?,
        Expr::SimpleCase(case) => validate_case(
            Some(&case.operand),
            &case.when_clauses,
            case.default_value.as_ref(),
            case.span,
        )?,
        Expr::Cast(cast) => validate_expr(&cast.expression)?,
        Expr::Interval(interval) => {
            validate_expr(&interval.value)?;
            if let Some(precision) = &interval.leading_precision {
                validate_expr(precision)?;
            }
            if let Some(precision) = &interval.fractional_seconds_precision {
                validate_expr(precision)?;
            }
        }
        Expr::Row(row) => {
            for field in &row.fields {
                validate_expr(&field.expression)?;
            }
        }
        Expr::Array(array) => {
            for expression in &array.values {
                validate_expr(expression)?;
            }
        }
        Expr::Lambda(lambda) => validate_expr(&lambda.body)?,
        Expr::Access(access) => {
            validate_expr(&access.expr)?;
            match &access.kind {
                super::AccessKind::Json { path, .. } => validate_expr(path)?,
            }
        }
        Expr::Subscript(node) => {
            validate_expr(&node.base)?;
            validate_expr(&node.index)?;
        }
        Expr::ArrayWildcardSubscript(node) => validate_expr(&node.base)?,
        Expr::Dereference(node) => validate_expr(&node.base)?,
        Expr::Identifier(_) | Expr::Literal(_) => {}
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

struct JsonValidation(Result<(), ValidateError>);
impl super::Visit for JsonValidation {
    fn visit_expr(&mut self, expression: &Expr) {
        if self.0.is_ok() {
            self.0 = validate_expr(expression);
        }
    }
    fn visit_json_value(&mut self, node: &super::JsonValue) {
        if self.0.is_err() {
            return;
        }
        self.0 = node.validate_constructor_invariants().map_err(|error| {
            ValidateError::invalid_structure(
                StructuralViolation::JsonConstructorInvariant(error),
                node.span,
            )
        });
        if self.0.is_ok() {
            super::walk_json_value(self, node);
        }
    }
    fn visit_json_table_value_column(&mut self, node: &super::JsonTableValueColumn) {
        if self.0.is_err() {
            return;
        }
        self.0 = node.validate_constructor_invariants().map_err(|error| {
            ValidateError::invalid_structure(
                StructuralViolation::JsonConstructorInvariant(error),
                node.span,
            )
        });
        if self.0.is_ok() {
            super::walk_json_table_value_column(self, node);
        }
    }
    fn visit_json_table(&mut self, node: &super::JsonTable) {
        if self.0.is_err() {
            return;
        }
        self.0 = node.validate_constructor_invariants().map_err(|error| {
            ValidateError::invalid_structure(
                StructuralViolation::JsonConstructorInvariant(error),
                node.span,
            )
        });
        if self.0.is_ok() {
            super::walk_json_table(self, node);
        }
    }
    fn visit_json_table_nested_columns(&mut self, node: &super::JsonTableNestedColumns) {
        if self.0.is_err() {
            return;
        }
        self.0 = node.validate_constructor_invariants().map_err(|error| {
            ValidateError::invalid_structure(
                StructuralViolation::JsonConstructorInvariant(error),
                node.span,
            )
        });
        if self.0.is_ok() {
            super::walk_json_table_nested_columns(self, node);
        }
    }
    fn visit_json_table_plan_siblings(&mut self, node: &super::JsonTablePlanSiblings) {
        if self.0.is_err() {
            return;
        }
        self.0 = node.validate_constructor_invariants().map_err(|error| {
            ValidateError::invalid_structure(
                StructuralViolation::JsonConstructorInvariant(error),
                node.span,
            )
        });
        if self.0.is_ok() {
            super::walk_json_table_plan_siblings(self, node);
        }
    }
}
fn validate_json_root(walk: impl FnOnce(&mut JsonValidation)) -> Result<(), ValidateError> {
    let mut visitor = JsonValidation(Ok(()));
    walk(&mut visitor);
    visitor.0
}

fn validate_case(
    operand: Option<&Expr>,
    clauses: &[super::WhenClause],
    default: Option<&Expr>,
    span: crate::Span,
) -> Result<(), ValidateError> {
    if clauses.is_empty() {
        return Err(ValidateError::invalid_structure(
            StructuralViolation::EmptyCaseBranches,
            span,
        ));
    }
    if let Some(operand) = operand {
        validate_expr(operand)?;
    }
    struct Children(Result<(), ValidateError>);
    impl super::Visit for Children {
        fn visit_expr(&mut self, expression: &Expr) {
            if self.0.is_ok() {
                self.0 = validate_expr(expression);
            }
        }
    }
    for clause in clauses {
        let mut children = Children(Ok(()));
        super::Visit::visit_when_clause(&mut children, clause);
        children.0?;
    }
    if let Some(default) = default {
        validate_expr(default)?;
    }
    Ok(())
}

#[recursive::recursive]
fn validate_order_by(order: &super::OrderBy) -> Result<(), ValidateError> {
    if order.items.is_empty() {
        return Err(ValidateError::invalid_structure(
            StructuralViolation::EmptyOrderBy,
            order.span,
        ));
    }
    for item in &order.items {
        validate_expr(&item.sort_key)?;
    }
    Ok(())
}

#[recursive::recursive]
fn validate_window(window: &super::Window) -> Result<(), ValidateError> {
    match window {
        super::Window::Reference(_) => Ok(()),
        super::Window::Specification(node) => validate_window_specification(node),
    }
}
#[recursive::recursive]
fn validate_row_pattern(pattern: &super::RowPattern) -> Result<(), ValidateError> {
    use super::RowPattern;
    match pattern {
        RowPattern::Alternation { patterns, span }
        | RowPattern::Concatenation { patterns, span }
        | RowPattern::Permutation { patterns, span } => {
            if patterns.is_empty() {
                return Err(ValidateError::invalid_structure(
                    StructuralViolation::EmptyPatternList,
                    *span,
                ));
            }
            for child in patterns {
                validate_row_pattern(child)?;
            }
        }
        RowPattern::Excluded { pattern, .. } | RowPattern::Quantified { pattern, .. } => {
            validate_row_pattern(pattern)?
        }
        RowPattern::Variable { .. } | RowPattern::Empty { .. } | RowPattern::Anchor { .. } => {}
    }
    Ok(())
}

#[recursive::recursive]
fn validate_call_argument(argument: &super::CallArgument) -> Result<(), ValidateError> {
    validate_expr(&argument.value)
}
