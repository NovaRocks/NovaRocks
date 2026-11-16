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

//! Typed write source context and ordinary assignment conversion.
//! The source has ALREADY received only its exact literal-hole context. This
//! method selects ordinary conversion and returns a real V expression; it does
//! not mutate output declarations or evaluate writer normalization.

use crate::analysis::TypedExpr;
use crate::analyze_error::AnalyzeError;
use crate::compiler::{SqlCompileError, SqlWriteAssignmentSite};
use novarocks_parser::Span;
use novarocks_type_contract::{CheckedWriteInputProjection, CompilePhase, PureCompileControl};

impl super::AnalyzerContext<'_> {
    pub(super) fn bind_write_assignment_value(
        &self,
        source: TypedExpr,
        site: &SqlWriteAssignmentSite,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let related = |source: &TypedExpr| {
            CheckedWriteInputProjection::try_new_observed(
                source.value_type.typed_binding().borrowed(),
                site.input().typed_binding().borrowed(),
                site.storage().map(|ty| ty.typed_binding().borrowed()),
                || {
                    self.control
                        .checkpoint(CompilePhase::Validate, 1)
                        .map_err(AnalyzeError::control)
                },
            )
            .map(|proof| proof.is_some())
        };
        // Preserve Fixed(m), actual NULL declarations and original constant
        // ownership whenever only the licensed write projection remains.
        if related(&source)? {
            return Ok(source);
        }
        // Exact writer declarations cannot smuggle a byte-width conversion
        // through ordinary safe CAST, which would turn bad widths into NULL.
        let mut fixed_target = false;
        novarocks_type_contract::visit_value_type_observed(site.input().value_type(), |event| {
            self.control
                .checkpoint(CompilePhase::Validate, 1)
                .map_err(AnalyzeError::control)?;
            if let novarocks_type_contract::ValueSemanticVisit::Node(value) = event {
                fixed_target |= matches!(
                    value.kind(),
                    novarocks_type_contract::ValueKind::Scalar(
                        arrow::datatypes::DataType::FixedSizeBinary(_)
                    )
                );
            }
            Ok::<_, AnalyzeError>(())
        })?;
        if fixed_target {
            return Err(AnalyzeError::type_mismatch(
                "fixed write assignment requires its explicit signed byte projection",
                span,
            ));
        }
        if let Some(storage) = site.storage() {
            let projection = CheckedWriteInputProjection::try_new_observed(
                storage.typed_binding().borrowed(),
                site.input().typed_binding().borrowed(),
                Some(storage.typed_binding().borrowed()),
                || {
                    self.control
                        .checkpoint(CompilePhase::Validate, 1)
                        .map_err(AnalyzeError::control)
                },
            )?
            .ok_or_else(|| {
                AnalyzeError::internal("validated write declaration lost its projection")
            })?;
            if pending_fixed_projection(
                source.value_type.typed_binding().borrowed(),
                projection,
                self.control,
            )? {
                return Err(AnalyzeError::type_mismatch(
                    "ordinary nested conversion cannot precede its licensed fixed byte projection",
                    span,
                ));
            }
        }
        // Reuse the neutral declaration-widening author for a truthful SOURCE
        // conversion target. W itself retains its signed requiredness.
        // Normal literal constructors may choose tighter non-NULL promises.
        let target = super::declaration_widening::widen_declaration(site.input(), self.control)
            .map_err(original_analysis_error)?;
        let converted = self.cast_to_value_type(source, target, span)?;
        if !related(&converted)? {
            return Err(AnalyzeError::type_mismatch(
                "ordinary assignment conversion cannot satisfy the signed write projection",
                span,
            ));
        }
        Ok(converted)
    }
}

/// Preserve the original literal constructor's child target context. The
/// complete child declarations come from the same signed W/T author; this walk
/// grants no constructor kernel or cast capability and never enters an explicit
/// CAST or explicitly typed ARRAY.
fn register_literal_assignments<'a>(
    expression: &'a novarocks_parser::ast::Expr,
    assignment: std::borrow::Cow<'a, SqlWriteAssignmentSite>,
    source: &mut crate::compiler::materialized_values::MaterializedSourceBorrow<'a>,
    control: &crate::compiler::SqlCompileControl,
) -> Result<(), AnalyzeError> {
    use arrow::datatypes::DataType;
    use novarocks_parser::{ast, printer};
    use novarocks_type_contract::ValueLogicalType;
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(AnalyzeError::control)?;
    if let ast::Expr::Nested(nested) = expression {
        return register_literal_assignments(&nested.expression, assignment, source, control);
    }
    // A carrier cannot authorize child semantics for a nominal domain.
    if assignment.input().logical_type() == ValueLogicalType::Physical {
        let child = |ordinal| -> Result<SqlWriteAssignmentSite, AnalyzeError> {
            control
                .checkpoint(CompilePhase::Validate, 1)
                .map_err(AnalyzeError::control)?;
            let input = assignment.input().child_at(ordinal).ok_or_else(|| {
                AnalyzeError::internal("literal assignment lost its signed input child")
            })?;
            let storage = assignment
                .storage()
                .map(|storage| {
                    control
                        .checkpoint(CompilePhase::Validate, 1)
                        .map_err(AnalyzeError::control)?;
                    storage.child_at(ordinal).ok_or_else(|| {
                        AnalyzeError::internal("literal assignment lost its signed storage child")
                    })
                })
                .transpose()?;
            SqlWriteAssignmentSite::try_new(assignment.output_ordinal(), input, storage, control)
                .map_err(original_analysis_error)
        };
        let mut visit = |value, ordinal| {
            register_literal_assignments(
                value,
                std::borrow::Cow::Owned(child(ordinal)?),
                source,
                control,
            )
        };
        match expression {
            ast::Expr::Array(array)
                if matches!(
                    assignment.input().data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                ) =>
            {
                for value in &array.values {
                    visit(value, 0)?;
                }
            }
            ast::Expr::Map(map) if matches!(assignment.input().data_type(), DataType::Map(..)) => {
                for entry in &map.entries {
                    visit(&entry.key, 0)?;
                    visit(&entry.value, 1)?;
                }
            }
            ast::Expr::Row(structure) if matches!(assignment.input().data_type(), DataType::Struct(fields) if fields.len() == structure.fields.len()) => {
                for (ordinal, field) in structure.fields.iter().enumerate() {
                    visit(&field.expression, ordinal)?;
                }
            }
            ast::Expr::FunctionCall(function)
                if matches!(function.quantifier, ast::FunctionQuantifier::None)
                    && function.order_by.is_none()
                    && function.separator.is_none()
                    && function.filter.is_none()
                    && function.null_treatment.is_none()
                    && function.over.is_none() =>
            {
                // These are only the original INSERT literal syntax forms.
                // Normal analysis still resolves or rejects the actual call.
                match (
                    printer::print_object_name(&function.name)
                        .to_ascii_lowercase()
                        .as_str(),
                    assignment.input().data_type(),
                ) {
                    ("array", DataType::List(_) | DataType::LargeList(_)) => {
                        for value in &function.arguments {
                            visit(value, 0)?;
                        }
                    }
                    ("row", DataType::Struct(fields))
                        if fields.len() == function.arguments.len() =>
                    {
                        for (ordinal, value) in function.arguments.iter().enumerate() {
                            visit(value, ordinal)?;
                        }
                    }
                    ("named_struct", DataType::Struct(fields))
                        if fields.len() * 2 == function.arguments.len() =>
                    {
                        for (ordinal, pair) in
                            function.arguments.as_chunks::<2>().0.iter().enumerate()
                        {
                            visit(&pair[1], ordinal)?;
                        }
                    }
                    ("map", DataType::Map(..)) if function.arguments.len() % 2 == 0 => {
                        for pair in function.arguments.as_chunks::<2>().0 {
                            visit(&pair[0], 0)?;
                            visit(&pair[1], 1)?;
                        }
                    }
                    _ => {}
                }
            }
            // An explicit CAST or typed ARRAY owns its whole source inference.
            // The containing cell still receives its ordinary outer assignment.
            _ => {}
        }
    }
    source
        .insert_write_assignment(expression, assignment, control)
        .map_err(original_analysis_error)
}

fn original_analysis_error(error: SqlCompileError) -> AnalyzeError {
    use novarocks_type_contract::CompileControlError;
    match error {
        SqlCompileError::Cancelled => AnalyzeError::control(CompileControlError::Cancelled),
        SqlCompileError::DeadlineExceeded => {
            AnalyzeError::control(CompileControlError::DeadlineExceeded)
        }
        SqlCompileError::ResourceExhausted => {
            AnalyzeError::control(CompileControlError::ResourceExhausted)
        }
        SqlCompileError::Analyze(error) => error,
        SqlCompileError::CompilationSource(source) => AnalyzeError::shared_domain_source(source),
        SqlCompileError::InvalidRequest(detail) | SqlCompileError::Compilation(detail) => {
            AnalyzeError::internal(detail)
        }
    }
}

pub(super) fn materialize_write_literal_contexts<'a>(
    query: &'a novarocks_parser::ast::Query,
    assignments: &'a crate::compiler::SqlWriteAssignments,
    source: &mut crate::compiler::materialized_values::MaterializedSourceBorrow<'a>,
    policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<(), AnalyzeError> {
    context_body(&query.body, assignments, source, policy, control)?;
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(control, CompilePhase::Validate)
            .map_err(AnalyzeError::control)?;
    for hint in assignments.literal_contexts() {
        let expression = crate::compiler::materialized_values::resolve_expression_site(
            query,
            &hint.body_path,
            hint.site,
            &[],
            true,
            &mut work,
        )
        .map_err(original_analysis_error)?;
        let target = assignments
            .at_observed(hint.output_ordinal, || work.step())
            .map_err(AnalyzeError::control)?
            .ok_or_else(|| {
                AnalyzeError::internal("literal context refers to an absent write assignment")
            })?;
        if hint.assignment_before_common_type {
            register_literal_assignments(
                expression,
                std::borrow::Cow::Borrowed(target),
                source,
                control,
            )?;
        }
        context_literal(expression, target.input(), source, policy, control)?;
    }
    work.finish().map_err(AnalyzeError::control)
}

fn context_body<'a>(
    body: &'a novarocks_parser::ast::SetExpr,
    assignments: &crate::compiler::SqlWriteAssignments,
    source: &mut crate::compiler::materialized_values::MaterializedSourceBorrow<'a>,
    policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<(), AnalyzeError> {
    use novarocks_parser::ast;
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(AnalyzeError::control)?;
    match body {
        ast::SetExpr::Values(values) => {
            for site in assignments.sites() {
                let mut only_holes = true;
                for row in &values.rows {
                    if let Some(expression) = row.get(site.output_ordinal()) {
                        only_holes &= context_hole(expression, source, control)?;
                    }
                }
                for row in &values.rows {
                    if let Some(expression) = row.get(site.output_ordinal())
                        && (only_holes || !context_hole(expression, source, control)?)
                    {
                        context_literal(expression, site.input(), source, policy, control)?;
                    }
                }
            }
        }
        ast::SetExpr::Select(select) => {
            // With a wildcard, AST item ordinal is not output ordinal. Exact
            // producer-authored paths may still supply a literal context.
            let scalar_projection = select.projection.iter().all(|item| {
                matches!(
                    item,
                    ast::SelectItem::UnnamedExpr(_) | ast::SelectItem::ExprWithAlias { .. }
                )
            });
            if scalar_projection {
                for site in assignments.sites() {
                    let expression =
                        select
                            .projection
                            .get(site.output_ordinal())
                            .and_then(|item| match item {
                                ast::SelectItem::UnnamedExpr(expr)
                                | ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                                _ => None,
                            });
                    if let Some(expression) = expression {
                        context_literal(expression, site.input(), source, policy, control)?;
                    }
                }
            }
        }
        ast::SetExpr::Query(query) => {
            context_body(&query.body, assignments, source, policy, control)?
        }
        ast::SetExpr::SetOperation(operation) => {
            // Only literal holes are contextualized here; actual ordinary
            // value conversions remain ABOVE the complete set operation.
            for input in &operation.inputs {
                context_body(input, assignments, source, policy, control)?;
            }
        }
    }
    Ok(())
}

/// Borrow result/default occurrences only. Predicate/operand syntax is never
/// assigned the sink's literal-hole context.
fn case_results(
    expression: &novarocks_parser::ast::Expr,
) -> impl Iterator<Item = &novarocks_parser::ast::Expr> {
    use novarocks_parser::ast::Expr;
    let (clauses, default) = match expression {
        Expr::SearchedCase(case) => (&case.when_clauses, &case.default_value),
        Expr::SimpleCase(case) => (&case.when_clauses, &case.default_value),
        _ => unreachable!("only CASE variants have branch results"),
    };
    clauses
        .iter()
        .map(|clause| &clause.result)
        .chain(default.iter())
}

fn context_hole(
    expression: &novarocks_parser::ast::Expr,
    source: &crate::compiler::materialized_values::MaterializedSourceBorrow<'_>,
    control: &crate::compiler::SqlCompileControl,
) -> Result<bool, AnalyzeError> {
    use novarocks_parser::ast;
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(AnalyzeError::control)?;
    if source.has_expression(expression) {
        return Ok(false);
    }
    match expression {
        ast::Expr::Nested(nested) => context_hole(&nested.expression, source, control),
        ast::Expr::Literal(literal) => Ok(matches!(literal.kind, ast::LiteralKind::Null)),
        ast::Expr::Array(array) => Ok(array.values.is_empty()),
        ast::Expr::Map(map) => Ok(map.entries.is_empty()),
        ast::Expr::SearchedCase(_) | ast::Expr::SimpleCase(_) => {
            for value in case_results(expression) {
                if !context_hole(value, source, control)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn context_literal<'a>(
    expression: &'a novarocks_parser::ast::Expr,
    expected: &novarocks_type_contract::FunctionValueType,
    source: &mut crate::compiler::materialized_values::MaterializedSourceBorrow<'a>,
    policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<(), AnalyzeError> {
    use novarocks_parser::ast;
    use novarocks_type_contract::FunctionValueType;
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(AnalyzeError::control)?;
    if source.has_expression(expression) {
        return Ok(());
    }
    let constant = match expression {
        ast::Expr::Nested(nested) => {
            return context_literal(&nested.expression, expected, source, policy, control);
        }
        ast::Expr::Literal(literal) if matches!(literal.kind, ast::LiteralKind::Null) => {
            let ty = expected
                .with_nullable(true)
                .map_err(AnalyzeError::domain_source)?;
            let field = std::sync::Arc::new(arrow::datatypes::Field::new(
                "",
                ty.data_type().clone(),
                true,
            ));
            Some(
                novarocks_functions::ConstantValue::null(
                    field,
                    ty,
                    policy,
                    CompilePhase::Validate,
                    control,
                )
                .map_err(|error| original_analysis_error(error.into()))?,
            )
        }
        ast::Expr::SearchedCase(_) | ast::Expr::SimpleCase(_) => {
            // A real branch, such as a Fixed read column, authors common-type
            // inference. Giving its NULL peer W would normalize that branch
            // before selection. Context only an otherwise untyped whole CASE.
            let mut only_holes = true;
            for result in case_results(expression) {
                only_holes &= context_hole(result, source, control)?;
            }
            for result in case_results(expression) {
                if only_holes || !context_hole(result, source, control)? {
                    context_literal(result, expected, source, policy, control)?;
                }
            }
            None
        }
        ast::Expr::Array(array) => {
            if !matches!(
                expected.data_type(),
                arrow::datatypes::DataType::List(_) | arrow::datatypes::DataType::LargeList(_)
            ) {
                return Ok(());
            }
            if array.values.is_empty() {
                Some(empty_literal(expected, policy, control)?)
            } else {
                let child: FunctionValueType = expected
                    .child_at(0)
                    .ok_or_else(|| AnalyzeError::internal("array literal context lost its item"))?;
                for element in &array.values {
                    context_literal(element, &child, source, policy, control)?;
                }
                None
            }
        }
        ast::Expr::Map(map)
            if matches!(expected.data_type(), arrow::datatypes::DataType::Map(..)) =>
        {
            if map.entries.is_empty() {
                Some(empty_literal(expected, policy, control)?)
            } else {
                let key = expected
                    .child_at(0)
                    .ok_or_else(|| AnalyzeError::internal("map literal context lost its key"))?;
                let value = expected
                    .child_at(1)
                    .ok_or_else(|| AnalyzeError::internal("map literal context lost its value"))?;
                for entry in &map.entries {
                    context_literal(&entry.key, &key, source, policy, control)?;
                    context_literal(&entry.value, &value, source, policy, control)?;
                }
                None
            }
        }
        ast::Expr::Row(structure) if matches!(expected.data_type(), arrow::datatypes::DataType::Struct(fields) if fields.len() == structure.fields.len()) =>
        {
            for (index, value) in structure.fields.iter().enumerate() {
                let child = expected.child_at(index).ok_or_else(|| {
                    AnalyzeError::internal("struct literal context lost its child")
                })?;
                context_literal(&value.expression, &child, source, policy, control)?;
            }
            None
        }
        // Explicit CAST, explicitly typed ARRAY, functions and nonliteral
        // expressions retain their original independent inference boundary.
        _ => None,
    };
    if let Some(value) = constant {
        source
            .insert_context_constant(expression, value, control)
            .map_err(original_analysis_error)?;
    }
    Ok(())
}

fn empty_literal(
    expected: &novarocks_type_contract::FunctionValueType,
    policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<novarocks_functions::ConstantValue, AnalyzeError> {
    let ty = expected
        .with_nullable(false)
        .map_err(AnalyzeError::domain_source)?;
    let field = std::sync::Arc::new(arrow::datatypes::Field::new(
        "",
        ty.data_type().clone(),
        false,
    ));
    novarocks_functions::ConstantValue::from_empty_collection(
        field,
        ty,
        policy,
        CompilePhase::Validate,
        control,
    )
    .map_err(|error| original_analysis_error(error.into()))
}

impl super::AnalyzerContext<'_> {
    pub(super) fn bind_write_source_query(
        &self,
        query: crate::analysis::ResolvedQuery,
        assignments: &crate::compiler::SqlWriteAssignments,
        span: Span,
    ) -> Result<crate::analysis::ResolvedQuery, AnalyzeError> {
        use crate::analysis::{
            AnalyzedOutputField, AnalyzedProjectItem, ExprKind, QueryBody, Relation, ResolvedQuery,
            ResolvedSelect,
        };
        assignments
            .validate_output_width(query.output_columns.len(), self.control)
            .map_err(original_analysis_error)?;
        let mut original = Vec::new();
        original
            .try_reserve_exact(query.output_columns.len())
            .map_err(assignment_allocation)?;
        for field in &query.output_columns {
            self.check_control()?;
            original.push(field.clone());
        }
        let mut projection = Vec::new();
        let mut output_columns = Vec::new();
        projection
            .try_reserve_exact(original.len())
            .map_err(assignment_allocation)?;
        output_columns
            .try_reserve_exact(original.len())
            .map_err(assignment_allocation)?;
        let alias = "__nr_write_source";
        for (ordinal, field) in original.iter().enumerate() {
            self.check_control()?;
            let source = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: field.column_id,
                    qualifier: Some(alias.to_owned()),
                    column: field.name.clone(),
                },
                value_type: field.value_type.clone(),
            };
            let site = assignments
                .at_observed(ordinal, || {
                    self.control.checkpoint(CompilePhase::Validate, 1)
                })
                .map_err(AnalyzeError::control)?;
            let value = match site {
                Some(site) => self.bind_write_assignment_value(
                    source,
                    site,
                    site.source_span().unwrap_or(span),
                )?,
                None => source,
            };
            let column_id = if let ExprKind::ColumnRef { column_id, .. } = &value.kind {
                *column_id
            } else {
                let id = self.alloc_column_id(None, field.name.clone(), value.value_type.clone());
                let logical = self.logical_output_type(None, &value, &self.new_scope())?;
                self.factory.borrow_mut().set_logical_type(id, logical);
                id
            };
            output_columns.push(AnalyzedOutputField {
                column_id,
                name: field.name.clone(),
                field_name: field.field_name.clone(),
                value_type: value.value_type.clone(),
                is_internal: field.is_internal,
            });
            projection.push(AnalyzedProjectItem {
                expr: value,
                output_name: field.name.clone(),
                field_name: field.field_name.clone(),
                output_column_id: column_id,
            });
        }
        Ok(ResolvedQuery {
            body: QueryBody::Select(ResolvedSelect {
                from: Some(Relation::Subquery {
                    query: Box::new(query),
                    alias: alias.into(),
                    output_columns: original,
                }),
                filter: None,
                group_by: Vec::new(),
                having: None,
                projection,
                has_aggregation: false,
                distinct: false,
                repeat: None,
                apply_specs: Vec::new(),
                predicate_apply_specs: Vec::new(),
            }),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_columns,
            local_cte_ids: Vec::new(),
        })
    }
}

// Walk only children already authorized by the checked T/W proof. This is not
// another logical/layout matcher or a new nested cast implementation.
fn pending_fixed_projection(
    source: novarocks_type_contract::BorrowedTypedValueBinding<'_>,
    projection: CheckedWriteInputProjection<'_>,
    control: &crate::compiler::SqlCompileControl,
) -> Result<bool, AnalyzeError> {
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(AnalyzeError::control)?;
    if matches!(
        projection.leaf_projection(),
        novarocks_type_contract::WriteInputLeafProjection::FixedBytesToBinary { .. }
    ) && matches!(
        source.data_type(),
        arrow::datatypes::DataType::FixedSizeBinary(_)
    ) {
        return Ok(true);
    }
    let mut ordinal = 0;
    while let Some(child) = projection.child_at(ordinal) {
        if let Some(source) = source.child_at(ordinal)
            && pending_fixed_projection(source, child, control)?
        {
            return Ok(true);
        }
        ordinal += 1;
    }
    Ok(false)
}

fn assignment_allocation(cause: std::collections::TryReserveError) -> AnalyzeError {
    AnalyzeError::domain_source(
        novarocks_functions::allocation_failure::AllocationRefusal::new(
            "typed write assignment projection",
            cause,
        ),
    )
}

#[cfg(test)]
mod case_context_tests {
    use super::*;
    use crate::compiler::materialized_values::MaterializedSourceBorrow;
    use novarocks_parser::ast;
    use novarocks_type_contract::FunctionValueType;

    #[test]
    fn case_holes_borrow_only_results_and_preserve_mixed_real_source_branches() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let policy = crate::constant::test_constant_policy();
        let expected = FunctionValueType::new(arrow::datatypes::DataType::Int64, false);
        for (sql, only_holes) in [
            ("SELECT CASE WHEN TRUE THEN NULL ELSE NULL END", true),
            ("SELECT CASE 1 WHEN 2 THEN NULL ELSE NULL END", true),
            (
                "SELECT CASE WHEN TRUE THEN original_read ELSE NULL END",
                false,
            ),
            (
                "SELECT CASE 1 WHEN 2 THEN original_read ELSE NULL END",
                false,
            ),
        ] {
            let statements = novarocks_parser::parse(sql).unwrap();
            let [ast::Statement::Query(query)] = statements.as_slice() else {
                panic!("query")
            };
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("select")
            };
            let ast::SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
                panic!("CASE projection")
            };
            let mut source = MaterializedSourceBorrow::empty(query);
            assert_eq!(
                context_hole(expression, &source, &control).unwrap(),
                only_holes
            );
            context_literal(expression, &expected, &mut source, policy, &control).unwrap();
            for result in case_results(expression) {
                if only_holes {
                    let value = source
                        .view()
                        .constant_at(result)
                        .expect("an actual NULL result receives context");
                    assert_eq!(
                        value.value_type(),
                        &FunctionValueType::new(arrow::datatypes::DataType::Int64, true)
                    );
                    assert!(source.view().constant_at(&result.clone()).is_none());
                } else {
                    assert!(
                        source.view().constant_at(result).is_none(),
                        "a real read branch keeps common-type authority over its NULL peer"
                    );
                }
            }
            let (operand, clauses) = match expression {
                ast::Expr::SimpleCase(case) => (Some(&case.operand), &case.when_clauses),
                ast::Expr::SearchedCase(case) => (None, &case.when_clauses),
                _ => panic!("CASE"),
            };
            if let Some(operand) = operand {
                assert!(source.view().constant_at(operand).is_none());
            }
            for clause in clauses {
                let ast::WhenMatch::Operand(condition) = &clause.match_value else {
                    panic!("WHEN expression")
                };
                assert!(source.view().constant_at(condition).is_none());
            }
        }
    }
}
