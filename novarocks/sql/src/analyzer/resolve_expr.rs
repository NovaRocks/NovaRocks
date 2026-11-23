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

#[cfg(test)]
use std::sync::Arc;

use crate::parser::{expression_for_diagnostic, type_for_diagnostic};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_expression::typing::{NodeShape, ScalarTypingRoute, scalar_typing_route};
use novarocks_parser::Span;
use novarocks_parser::ast;
use novarocks_parser::printer::print_object_name;

use crate::analysis::*;
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use novarocks_type_contract::{
    ArithmeticOperator, TemporalType, ValueTypeRef, arithmetic_result_value_type_with_op,
};

use super::functions::*;
use super::helpers::{eval_const_i64, expr_display_name};
use super::scope::AnalyzerScope;

type WindowSpecAnalysis = (Vec<TypedExpr>, Vec<SortItem>, Option<WindowFrame>);

fn scalar_signature_is_unknown(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    arg_types: &[DataType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, AnalyzeError> {
    match function_catalog.resolve_scalar_signature(name, arg_types, control) {
        Err(error @ novarocks_functions::FunctionResolutionError::Control(_)) => {
            Err(AnalyzeError::function_resolution(error))
        }
        Err(crate::functions::ResolveError::UnknownFunction) => Ok(true),
        _ => Ok(false),
    }
}
fn scalar_function_is_unknown(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    arg_types: &[DataType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, AnalyzeError> {
    Ok(
        scalar_signature_is_unknown(function_catalog, name, arg_types, control)?
            && legacy_scalar_return_type_with_catalog(function_catalog, name, arg_types, control)?
                .is_none(),
    )
}

pub(crate) fn unsupported_predicate_reason(node: &ast::PredicatedExpr) -> Option<&'static str> {
    match &node.predicate {
        ast::Predicate::Between(p) if p.symmetry.is_some() => {
            Some("explicit BETWEEN symmetry is not supported")
        }
        ast::Predicate::In(p) => match &p.value_list {
            ast::Expr::InList(list) if !list.values.is_empty() => None,
            ast::Expr::Subquery(_) => None,
            _ => Some("IN predicates require a nonempty value list or a subquery"),
        },
        ast::Predicate::Overlaps(_) => Some("OVERLAPS predicates are not supported"),
        ast::Predicate::QuantifiedComparison(_) => {
            Some("quantified comparison predicates are not supported")
        }
        ast::Predicate::Match(_) => Some("MATCH predicates are not supported"),
        _ => None,
    }
}

pub(crate) fn unsupported_call_facts(call: &ast::FunctionCall) -> Option<&'static str> {
    if call.processing_mode.is_some() {
        return Some("function processing modes are not supported");
    }
    if call
        .arguments
        .iter()
        .any(|argument| argument.name.is_some())
    {
        return Some("named function arguments are not supported");
    }
    None
}

pub(crate) fn unsupported_window_frame_reason(frame: &ast::WindowFrame) -> Option<&'static str> {
    if frame.exclusion != ast::WindowFrameExclusion::NoOthers {
        return Some("window frame EXCLUDE is not supported");
    }
    if !frame.measures.is_empty()
        || frame.after_match_skip_to.is_some()
        || frame.pattern_search_mode.is_some()
        || frame.pattern.is_some()
        || !frame.subsets.is_empty()
        || !frame.variable_definitions.is_empty()
    {
        return Some("pattern recognition in window frames is not supported");
    }
    None
}

pub(crate) fn unsupported_window_reason(window: &ast::Window) -> Option<&'static str> {
    let specification = match window {
        ast::Window::Reference(_) => return Some("named window references are not supported"),
        ast::Window::Specification(specification) => specification,
    };
    if specification.existing_window_name.is_some() {
        return Some("named window references are not supported");
    }
    if let Some(frame) = &specification.window_frame
        && let Some(reason) = unsupported_window_frame_reason(frame)
    {
        return Some(reason);
    }
    None
}

pub(crate) fn unsupported_control_expression_reason(
    expression: &ast::Expr,
) -> Option<&'static str> {
    let clauses = match expression {
        ast::Expr::SearchedCase(case) => Some(case.when_clauses.as_slice()),
        ast::Expr::SimpleCase(case) => Some(case.when_clauses.as_slice()),
        ast::Expr::Unique(_) => return Some("UNIQUE subquery predicates are not supported"),
        ast::Expr::WindowOperation(_) => return Some("window operations are not supported"),
        ast::Expr::FunctionCall(call) => {
            if let Some(reason) = unsupported_call_facts(call) {
                return Some(reason);
            }
            if let Some(reason) = call.window.as_ref().and_then(|window| match window {
                ast::Window::Specification(specification) => specification
                    .window_frame
                    .as_ref()
                    .and_then(unsupported_window_frame_reason),
                ast::Window::Reference(_) => None,
            }) {
                return Some(reason);
            }
            None
        }
        ast::Expr::Dereference(node) if node.field.is_none() => {
            return Some("label-only dereference expressions are not supported");
        }
        ast::Expr::ArrayWildcardSubscript(_) => {
            return Some("array wildcard subscripts are not supported");
        }
        ast::Expr::Exists(exists) if !matches!(exists.subquery, ast::Expr::Subquery(_)) => {
            return Some("EXISTS requires a subquery expression");
        }
        _ => None,
    };
    if clauses.is_some_and(|clauses| {
        clauses
            .iter()
            .any(|clause| matches!(clause.match_value, ast::WhenMatch::Partial(_)))
    }) {
        Some("partial CASE predicates are not supported")
    } else {
        None
    }
}

#[cfg(test)]
fn string_literal_expr(value: String, span: Span) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::String(value),
        span,
    })
}

/// Neutral facts for the original function author. Dedicated syntax supplies
/// an explicit builtin; ordinary calls retain their actual metadata owner.

#[derive(Clone, Copy)]
struct FunctionMetadata<'a> {
    original: Option<&'a ast::FunctionCall>,
    span: Span,
    distinct: bool,
    order_by: Option<&'a ast::OrderBy>,
    window: Option<&'a ast::Window>,
    null_treatment: Option<ast::NullTreatment>,
}
fn unsupported_remaining_root(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Overlay(_)
            | ast::Expr::MethodCall(_)
            | ast::Expr::StaticMethodCall(_)
            | ast::Expr::CurrentCatalog(_)
            | ast::Expr::CurrentPath(_)
            | ast::Expr::CurrentSchema(_)
            | ast::Expr::CurrentUser(_)
    ) || matches!(expr, ast::Expr::Trim(node) if node.trim_character.is_some())
}

impl<'a> super::AnalyzerContext<'a> {
    /// Analyze a single expression and produce a TypedExpr.
    #[recursive::recursive]
    pub(super) fn analyze_expr(
        &self,
        expr: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        self.check_control()?;
        // Direct expression admission precedes its own CV lookup. This is not
        // a whole-tree prepass over opaque declaration shortcuts.
        if unsupported_remaining_root(expr) {
            return Err(AnalyzeError::unsupported_expression(
                "expression profile is not supported",
                expr.span(),
            ));
        }
        if let Some(reason) = unsupported_control_expression_reason(expr) {
            return Err(AnalyzeError::unsupported_expression(reason, expr.span()));
        }
        let resolved = match self
            .materialized_values
            .and_then(|source| source.constant_at(expr))
        {
            Some(value) => TypedExpr {
                kind: ExprKind::Constant(value.clone()),
                value_type: value.value_type().clone(),
            },
            None => self.analyze_expr_impl(expr, scope)?,
        };
        self.check_control()?;
        // Borrow only this root's original authored binding. Child analysis has
        // already used this same recursion; do not rescan or rebind its graph.
        let binding = match &resolved.kind {
            ExprKind::FunctionCall { binding, .. } | ExprKind::WindowCall { binding, .. } => {
                Some(binding)
            }
            ExprKind::AggregateCall { resolved, .. } => Some(resolved),
            _ => None,
        };
        if let Some(binding) = binding {
            self.function_catalog
                .admit_authored_environment_observed(binding, self.control)
                .map_err(|error| {
                    AnalyzeError::function_binding(error).at_type_mismatch(expr.span())
                })?;
        }
        Ok(resolved)
    }

    #[recursive::recursive]
    fn analyze_expr_impl(
        &self,
        expr: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if let ast::Expr::Row(row) = expr {
            for _ in &row.fields {
                self.check_control()?;
            }
        }
        if let ast::Expr::Extract(extract) = expr {
            let operand = self.analyze_expr(&extract.expression, scope)?;
            return self.type_extract(extract.field, operand, extract.span);
        }
        if let Some(call) = novarocks_parser::canonical_call(expr)
            .map_err(|error| AnalyzeError::unsupported_expression(error.message, error.span))?
        {
            return self.analyze_syntax_function(call.name, &call.arguments, call.span, scope);
        }

        match expr {
            ast::Expr::Trim(_)
            | ast::Expr::Row(_)
            | ast::Expr::If(_)
            | ast::Expr::NullIf(_)
            | ast::Expr::Coalesce(_)
            | ast::Expr::Try(_)
            | ast::Expr::Extract(_) => unreachable!("canonical syntax call was already mapped"),
            ast::Expr::JsonQuery(node) => self.analyze_json_query_syntax(node, scope),
            ast::Expr::JsonObject(node) => self.analyze_json_object_syntax(node, scope),
            ast::Expr::JsonArray(node) => self.analyze_json_array_syntax(node, scope),
            ast::Expr::JsonConstructor(_)
            | ast::Expr::JsonExists(_)
            | ast::Expr::JsonValue(_)
            | ast::Expr::JsonSerialize(_) => Err(AnalyzeError::unsupported_expression(
                "SQL/JSON constructor is not supported",
                expr.span(),
            )),

            // Simple column reference
            ast::Expr::Identifier(ident) => {
                // If the scope has a synthetic expression for this name
                // (FULL OUTER USING column → COALESCE), return that
                // expression directly so the merged value is computed.
                if let Some(expr) = scope.computed_column_for(&ident.value) {
                    return Ok(expr.clone());
                }
                let (column_id, value_type) = scope.resolve_at(None, &ident.value, ident.span)?;
                Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id,
                        qualifier: None,
                        column: ident.value.to_lowercase(),
                    },
                    value_type,
                })
            }

            ast::Expr::GroupingOperation(node) => {
                self.analyze_grouping_input(super::GroupingSyntaxRef::Operation(node), scope)
            }
            ast::Expr::IntervalValueExpression(node) => {
                let left = self.analyze_expr(&node.left, scope)?;
                let right = self.analyze_expr(&node.right, scope)?;
                let [left_type, right_type, target] =
                    novarocks_expression::typing::qualified_datetime_difference(
                        &left.value_type,
                        &right.value_type,
                        &node.data_type,
                    )
                    .map_err(|error| {
                        super::expression_typing::analyze_typing_error(error, node.span)
                    })?;
                let left = self.cast_to_value_type(left, left_type, node.left.span())?;
                let right = self.cast_to_value_type(right, right_type, node.right.span())?;
                let difference = self.analyze_temporal_operator(
                    novarocks_type_contract::OperatorFunction::TemporalSubtract,
                    vec![left, right],
                    node.span,
                )?;
                self.cast_to_value_type(difference, target, node.span)
            }
            ast::Expr::AtLocal(node) => {
                let source = self.analyze_expr(&node.value, scope)?;
                self.type_at_zone(source, None, node.span)
            }
            ast::Expr::AtTimeZone(node) => {
                let source = self.analyze_expr(&node.value, scope)?;
                let zone = self.analyze_expr(&node.time_zone, scope)?;
                self.type_at_zone(source, Some(zone), node.span)
            }
            ast::Expr::CurrentDate(_)
            | ast::Expr::CurrentTime(_)
            | ast::Expr::CurrentTimestamp(_)
            | ast::Expr::LocalTime(_)
            | ast::Expr::LocalTimestamp(_) => self.type_clock(expr),
            ast::Expr::Overlay(_)
            | ast::Expr::MethodCall(_)
            | ast::Expr::StaticMethodCall(_)
            | ast::Expr::CurrentCatalog(_)
            | ast::Expr::CurrentPath(_)
            | ast::Expr::CurrentSchema(_)
            | ast::Expr::CurrentUser(_) => Err(AnalyzeError::unsupported_expression(
                "expression profile is not supported",
                expr.span(),
            )),
            ast::Expr::Literal(value) => self.type_literal(value),

            ast::Expr::Array(array) => self.analyze_array_literal(array, scope),
            ast::Expr::Binary(binary) => self.analyze_binary_op(binary, scope),

            ast::Expr::Logical(logical) => self.analyze_boolean_chain(logical, scope),

            // A lambda is valid only as a higher-order function argument.
            // Those callers bind its parameters before analyzing the body.
            ast::Expr::Lambda(lambda) => Err(AnalyzeError::unsupported_expression(
                "lambda expressions are only allowed inside higher-order function calls",
                lambda.span,
            )),

            // Unary NOT. `NOT NULL` is NULL, so the result admits null
            // exactly when its operand does.
            ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Not) => {
                if let ast::Expr::Exists(exists) = unary.expression.as_ref() {
                    return self.analyze_exists(exists, true);
                }
                let inner_typed = self.analyze_expr(&unary.expression, scope)?;
                let nullable = inner_typed.value_type.nullable();
                Ok(TypedExpr {
                    kind: ExprKind::UnaryOp {
                        op: UnOp::Not,
                        expr: novarocks_parser::ast::AstBox::new(inner_typed),
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        nullable,
                    ),
                })
            }

            // Unary plus preserves the original producer and exact value type.
            ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Plus) => {
                let inner_typed = self.analyze_expr(&unary.expression, scope)?;
                let (_, mut operands) =
                    self.type_value_node(NodeShape::UnaryPlus, vec![inner_typed], unary.span)?;
                Ok(operands.pop().expect("one unary plus operand"))
            }

            // Unary minus
            ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
                let operand = self.analyze_expr(&unary.expression, scope)?;
                let operator = novarocks_expression::typing::negation_operator(&operand.value_type);
                let mut bound = self.type_scalar_call(
                    NodeShape::Operator(operator),
                    vec![operand],
                    unary.span,
                )?;
                Ok(crate::common::negation::typed_call(
                    bound.binding,
                    bound.args.pop().expect("one negation operand"),
                ))
            }

            ast::Expr::Predicated(node) => self.analyze_predicated(node, scope),
            ast::Expr::InList(list) => Err(AnalyzeError::unsupported_expression(
                "IN lists are only supported as an IN predicate value list",
                list.span,
            )),

            ast::Expr::Cast(cast) => {
                use novarocks_type_contract::TrinoCastMode;
                let source = self.analyze_expr(&cast.expression, scope)?;
                let target =
                    super::helpers::sql_type_to_value_type(&cast.data_type, true, self.control)?;
                let mode = if cast.safe {
                    TrinoCastMode::Try
                } else {
                    TrinoCastMode::Explicit
                };
                self.cast_with_trino_mode(source, target, mode, cast.span)
            }

            // CASE WHEN
            ast::Expr::SearchedCase(case) => self.analyze_searched_case(
                &case.when_clauses,
                case.default_value.as_ref(),
                scope,
                case.span,
            ),
            ast::Expr::SimpleCase(case) => self.analyze_case(
                Some(&case.operand),
                &case.when_clauses,
                case.default_value.as_ref(),
                scope,
                case.span,
            ),

            ast::Expr::Format(node) => Err(AnalyzeError::unsupported_expression(
                "FORMAT expressions are not supported",
                node.span,
            )),

            // Function call
            ast::Expr::FunctionCall(func) => self.analyze_function(func, scope),

            ast::Expr::Dereference(node) => {
                let mut interior_source = false;
                let chain = ast::identifier_chain_observed(expr, |occurrence| {
                    self.check_control()?;
                    if !std::ptr::eq(occurrence, expr)
                        && self
                            .materialized_values
                            .is_some_and(|source| source.constant_at(occurrence).is_some())
                    {
                        interior_source = true;
                    }
                    Ok::<(), AnalyzeError>(())
                })?;
                if let Some(chain) = chain.filter(|_| !interior_source) {
                    let mut parts = Vec::new();
                    parts.try_reserve_exact(chain.len()).map_err(|_| {
                        AnalyzeError::control(
                            novarocks_type_contract::CompileControlError::ResourceExhausted,
                        )
                    })?;
                    for ident in chain.iter_rev() {
                        self.check_control()?;
                        parts.push(ident);
                    }
                    for index in 0..parts.len() / 2 {
                        self.check_control()?;
                        let other = parts.len() - 1 - index;
                        parts.swap(index, other);
                    }
                    return self.analyze_compound_identifier(&parts, scope);
                }
                let Some(field) = &node.field else {
                    return Err(AnalyzeError::unsupported_expression(
                        "label-only dereference expressions are not supported",
                        node.span,
                    ));
                };
                let base = self.analyze_expr(&node.base, scope)?;
                self.analyze_struct_field_access(base, field.value.clone(), field.span)
            }
            ast::Expr::Subscript(node) => {
                let base = self.analyze_expr(&node.base, scope)?;
                self.analyze_subscript(base, &node.index, scope, node.span)
            }
            ast::Expr::Exists(exists) => self.analyze_exists(exists, false),
            ast::Expr::Unique(node) => Err(AnalyzeError::unsupported_expression(
                "UNIQUE subquery predicates are not supported",
                node.span,
            )),

            // Scalar subquery: (SELECT ...)
            ast::Expr::Subquery(subquery) => {
                let id = self.alloc_subquery_id();
                let value_type = self.infer_scalar_subquery_value_type(&subquery.query, scope)?;
                let data_type = value_type.data_type().clone();
                let kind = SubqueryKind::Scalar;
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: subquery.query.clone(),
                    data_type: data_type.clone(),
                    in_expr: None,
                });
                Ok(TypedExpr {
                    kind: ExprKind::SubqueryPlaceholder {
                        id,
                        kind,
                        data_type: data_type.clone(),
                    },
                    value_type: super::helpers::with_nullability(value_type, true),
                })
            }

            other => Err(AnalyzeError::unsupported_expression(
                format!(
                    "unsupported expression: {}",
                    expression_for_diagnostic(other)
                ),
                other.span(),
            )),
        }
    }

    fn analyze_subscript(
        &self,
        base: TypedExpr,
        index: &ast::Expr,
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let index_typed = self.analyze_expr(index, scope)?;
        let output_type = match &base.value_type.data_type() {
            DataType::List(_) => {
                let operator = novarocks_type_contract::OperatorFunction::ArraySubscript;
                let bound = self.type_scalar_call(
                    novarocks_expression::typing::NodeShape::Operator(operator),
                    vec![base, index_typed],
                    span,
                )?;
                let value_type = bound.value_type().clone();
                return Ok(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: operator.catalog_name().into(),
                        volatility: bound.binding.semantics.volatility,
                        args: bound.args.into(),
                        distinct: false,
                        binding: bound.binding,
                    },
                    value_type,
                });
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(AnalyzeError::invalid_argument(
                        "map subscript expects STRUCT map entries",
                        span,
                    ));
                };
                if fields.len() != 2 {
                    return Err(AnalyzeError::invalid_argument(
                        "map subscript expects key/value entries",
                        span,
                    ));
                }
                fields[1].data_type().clone()
            }
            DataType::Struct(fields) => {
                return match &index_typed.kind {
                    ExprKind::Literal(LiteralValue::String(field_name)) => {
                        self.analyze_struct_field_access(base, field_name.clone(), span)
                    }
                    // 1-based positional access: `struct_val[1]` →
                    // first field of the STRUCT. Matches StarRocks /
                    // Spark / Trino convention.
                    ExprKind::Literal(LiteralValue::Int(pos)) => {
                        if *pos < 1 || (*pos as usize) > fields.len() {
                            return Err(AnalyzeError::invalid_argument(
                                format!(
                                    "struct subscript {} is out of range (1..{})",
                                    pos,
                                    fields.len()
                                ),
                                span,
                            ));
                        }
                        let field_name = fields[(*pos as usize) - 1].name().clone();
                        self.analyze_struct_field_access(base, field_name, span)
                    }
                    _ => Err(AnalyzeError::invalid_argument(
                        format!(
                            "struct subscript requires a string literal field name or 1-based integer index, got {:?}",
                            index_typed.kind
                        ),
                        span,
                    )),
                };
            }
            other => {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "subscript access expects ARRAY, MAP, or STRUCT input, got {:?}",
                        other
                    ),
                    span,
                ));
            }
        };
        let name = "__map_subscript";
        let bound =
            self.type_scalar_call(NodeShape::MapSubscript, vec![base, index_typed], span)?;
        let value_type = bound.value_type().clone();
        debug_assert_eq!(value_type.data_type(), &output_type);
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.into(),
                volatility: bound.binding.semantics.volatility,
                args: bound.args.into(),
                distinct: false,
                binding: bound.binding,
            },
            value_type,
        })
    }

    fn infer_scalar_subquery_value_type(
        &self,
        subquery: &ast::Query,
        outer_scope: &AnalyzerScope,
    ) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
        // `analyze_query` re-enters `analyze_select` which calls
        // `rewrite_subqueries` whenever `collected_subqueries` is non-empty.
        // That rewrite *drains* the shared `collected_subqueries` vec, so any
        // outer-query subqueries collected before us would be lost. Isolate
        // the inference pass from that outer collection: a derived FROM
        // SELECT must not rewrite an earlier sibling's outer subquery using
        // its own local scope. Restore the complete collection on every exit.
        let saved_collected: Vec<SubqueryInfo> =
            std::mem::take(&mut *self.collected_subqueries.borrow_mut());
        let saved_next_subquery_id = self.next_subquery_id.get();
        let saved_cte_registry = self.cte_registry.borrow().clone();
        let inferred = self
            .analyze_query_with_outer_scope_inner(subquery, outer_scope)
            .and_then(|(query, _inner_scope)| {
                query
                    .output_columns
                    .first()
                    .map(|col| col.value_type.clone())
                    .ok_or_else(|| {
                        AnalyzeError::invalid_query_shape(
                            "scalar subquery has no output column",
                            subquery.span,
                        )
                    })
            });
        *self.collected_subqueries.borrow_mut() = saved_collected;
        self.next_subquery_id.set(saved_next_subquery_id);
        self.cte_registry
            .borrow_mut()
            .clone_from(&saved_cte_registry);
        inferred
    }

    fn analyze_compound_identifier(
        &self,
        parts: &[&ast::Ident],
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        // A compound identifier `a.b.c.d...` can mean several different things
        // depending on schema and aliases. Try them in order of specificity:
        //
        //   1. `qual.col` (+ optional `.field.field…` struct chain) — the most
        //      common form. The qualifier may be a table name, a FROM-clause
        //      alias, or a CTE alias.
        //   2. `db.tbl.col` (+ optional struct chain) — fully qualified
        //      reference where the database prefix is ignored for resolution
        //      because the analyzer has already attached the column to a
        //      specific table.
        //   3. `col.field.field…` — implicit qualifier, the leading identifier
        //      *is* the column and the rest walk into a STRUCT.
        //
        let base_name = &parts[0].value;
        let build_column_ref =
            |column_id, value_type, qualifier: Option<String>, column: &str| TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier,
                    column: column.to_lowercase(),
                },
                value_type,
            };

        // Form 1: parts[0] is a qualifier, parts[1] is a column, parts[2..] are
        // struct subfields. Supports `t.col`, `t.col.field`, `t.col.f1.f2`, …
        if parts.len() >= 2 {
            let qualifier = &parts[0].value;
            let col_name = &parts[1].value;
            if let Ok((column_id, value_type)) = scope.resolve_value_type(Some(qualifier), col_name)
            {
                scope.check_read_column(column_id, &parts[2..], parts[1].span)?;
                let mut current = build_column_ref(
                    column_id,
                    value_type,
                    Some(qualifier.to_lowercase()),
                    col_name,
                );
                for field in &parts[2..] {
                    current =
                        self.analyze_struct_field_access(current, field.value.clone(), field.span)?;
                }
                return Ok(current);
            }
        }

        // Form 2: `db.tbl.col[.field…]`. Drop the database prefix and resolve
        // the remaining `tbl.col[.field…]` as form 1.
        if parts.len() >= 3 {
            let qualifier = &parts[1].value;
            let col_name = &parts[2].value;
            if let Ok((column_id, value_type)) = scope.resolve_value_type(Some(qualifier), col_name)
            {
                scope.check_read_column(column_id, &parts[3..], parts[2].span)?;
                let mut current = build_column_ref(
                    column_id,
                    value_type,
                    Some(qualifier.to_lowercase()),
                    col_name,
                );
                for field in &parts[3..] {
                    current =
                        self.analyze_struct_field_access(current, field.value.clone(), field.span)?;
                }
                return Ok(current);
            }
        }

        // Form 3: leading identifier is the column itself, the rest walk a
        // STRUCT. Falls back to producing a `Column 'X' cannot be resolved`
        // error from `scope.resolve` if even this fails.
        let (column_id, value_type) = scope
            .resolve_value_type(None, base_name)
            .map_err(|message| AnalyzeError::unknown_column(message, parts[0].span))?;
        scope.check_read_column(column_id, &parts[1..], parts[0].span)?;
        let mut current = build_column_ref(column_id, value_type, None, base_name);
        for field in &parts[1..] {
            current = self.analyze_struct_field_access(current, field.value.clone(), field.span)?;
        }
        Ok(current)
    }

    fn analyze_struct_field_access(
        &self,
        base: TypedExpr,
        field_name: String,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let DataType::Struct(fields) = &base.value_type.data_type() else {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "field access expects STRUCT input, got {:?}",
                    base.value_type.data_type()
                ),
                span,
            ));
        };
        // Canonical spelling is presentation only. The same Functions author
        // rejects case collisions before either SQL or binding can pick a field.
        let mut field_work = novarocks_type_contract::CompileCheckpoints::try_new(
            self.control,
            novarocks_type_contract::CompilePhase::Validate,
        )
        .map_err(AnalyzeError::control)?;
        let ordinal = novarocks_functions::builtin::struct_field_resolution::resolve_struct_field_ordinal_observed(
            fields, &field_name, || field_work.step().map_err(novarocks_functions::FunctionBindingError::Control),
        ).map_err(|source| match source {
            novarocks_functions::FunctionBindingError::Control(source) => AnalyzeError::control(source),
            source => AnalyzeError::invalid_argument(source.to_string(), span),
        })?.ok_or_else(|| AnalyzeError::unknown_column(format!("struct field '{}' does not exist", field_name), span))?;
        field_work.finish().map_err(AnalyzeError::control)?;
        let field = &fields[ordinal];
        let field_type = field.data_type().clone();
        let canonical_field_name = field.name().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(canonical_field_name)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        };
        let result = resolved_scalar_call_at(
            self.function_catalog,
            "__struct_subfield",
            vec![base, field_name_expr],
            span,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.constant_policy,
            self.control,
        )?;
        debug_assert_eq!(result.value_type.data_type(), &field_type);
        Ok(result)
    }

    fn analyze_array_literal(
        &self,
        array: &ast::ArrayExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut args = Vec::with_capacity(array.values.len());
        let mut common = novarocks_type_contract::FunctionValueType::new(DataType::Null, true);
        for item in &array.values {
            let value = self.analyze_expr(item, scope)?;
            common = self.require_common_trino_type(&common, &value.value_type, item.span())?;
            args.push(value);
        }
        let args = args
            .into_iter()
            .map(|source| {
                // The array owner materializes unknown elements in their exact
                // selected layout, including a NULL nested list beside [NULL].
                if source.value_type.data_type() == &DataType::Null {
                    return Ok(source);
                }
                self.coerce_common_value(source, &common, array.span)
            })
            .collect::<Result<Vec<_>, _>>()?;
        resolved_scalar_call_at(
            self.function_catalog,
            "__array_literal",
            args,
            array.span,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.constant_policy,
            self.control,
        )
    }

    fn analyze_predicated(
        &self,
        node: &ast::PredicatedExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if let Some(reason) = unsupported_predicate_reason(node) {
            return Err(AnalyzeError::unsupported_expression(reason, node.span));
        }
        let value = &node.value;
        let span = node.span;
        match &node.predicate {
            ast::Predicate::Comparison(predicate) => self.analyze_comparison_predicate(
                value,
                novarocks_expression::typing::syntax_comparison(predicate.operator),
                &predicate.right,
                scope,
            ),
            ast::Predicate::DistinctFrom(predicate) => self.analyze_comparison_predicate(
                value,
                novarocks_expression::typing::syntax_distinct_from(predicate.negated),
                &predicate.right,
                scope,
            ),
            ast::Predicate::IsNull(predicate) => {
                let inner = self.analyze_expr(value, scope)?;
                let (value_type, mut children) =
                    self.type_value_node(NodeShape::IsNull, vec![inner], span)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsNull {
                        expr: novarocks_parser::ast::AstBox::new(
                            children.pop().expect("IS NULL has one child"),
                        ),
                        negated: predicate.negated,
                    },
                    value_type,
                })
            }
            ast::Predicate::BooleanTest(predicate) => {
                let inner = self.require_boolean(
                    self.analyze_expr(value, scope)?,
                    value.span(),
                    "Boolean test operand must be BOOLEAN",
                )?;
                let value_type =
                    novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false);
                match predicate.truth_value {
                    ast::TruthValue::Unknown => Ok(TypedExpr {
                        value_type,
                        kind: ExprKind::IsNull {
                            expr: novarocks_parser::ast::AstBox::new(inner),
                            negated: predicate.negated,
                        },
                    }),
                    ast::TruthValue::True | ast::TruthValue::False => {
                        let literal = TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Bool(
                                predicate.truth_value == ast::TruthValue::True,
                            )),
                            value_type: value_type.clone(),
                        };
                        let equality = self.analyze_binary_operator(
                            novarocks_type_contract::OperatorFunction::Identical,
                            inner,
                            literal,
                            span,
                        )?;
                        if !predicate.negated {
                            return Ok(equality);
                        }
                        Ok(TypedExpr {
                            value_type,
                            kind: ExprKind::UnaryOp {
                                op: UnOp::Not,
                                expr: novarocks_parser::ast::AstBox::new(equality),
                            },
                        })
                    }
                }
            }
            ast::Predicate::Between(predicate) => {
                self.analyze_between_predicate(value, predicate, span, scope)
            }
            ast::Predicate::Like(predicate) => self.analyze_like_predicate(value, predicate, scope),
            ast::Predicate::In(predicate) => match &predicate.value_list {
                ast::Expr::InList(list) => self.analyze_in_list_predicate(
                    value,
                    &list.values,
                    predicate.negated,
                    span,
                    scope,
                ),
                ast::Expr::Subquery(subquery) => self.analyze_in_subquery_predicate(
                    value,
                    &subquery.query,
                    predicate.negated,
                    span,
                    scope,
                ),
                _ => unreachable!("the IN profile is checked before child analysis"),
            },
            ast::Predicate::Overlaps(_)
            | ast::Predicate::QuantifiedComparison(_)
            | ast::Predicate::Match(_) => {
                unreachable!("unsupported predicate profiles are rejected before child analysis")
            }
        }
    }
    fn analyze_in_list_predicate(
        &self,
        value: &ast::Expr,
        values: &[ast::Expr],
        negated: bool,
        span: Span,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut expr_typed = self.analyze_expr(value, scope)?;
        let mut list_typed = Vec::with_capacity(values.len());
        for item in values {
            let item_typed = self.analyze_expr(item, scope)?;
            check_json_comparison_domains(
                &expr_typed.value_type,
                &item_typed.value_type,
                "IN",
                span,
            )?;
            list_typed.push(item_typed);
        }
        // BITMAP / HLL operands cannot participate in IN / NOT IN
        // because they have no scalar identity. Reject upfront so the
        // user sees a clear error before lowering / codegen.
        let kw = if negated { "NOT IN" } else { "IN" };
        if let Some(logical) = scope
            .logical_type_of_expr(&expr_typed)
            .filter(is_bitmap_or_hll_type)
        {
            let col = column_name_of_expr(&expr_typed);
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "BITMAP/HLL columns cannot appear in {kw} expressions (operand `{col}` has type {logical:?})"
                ),
                span,
            ));
        }
        for item in &list_typed {
            if let Some(logical) = scope
                .logical_type_of_expr(item)
                .filter(is_bitmap_or_hll_type)
            {
                let col = column_name_of_expr(item);
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "BITMAP/HLL columns cannot appear in {kw} expressions (operand `{col}` has type {logical:?})"
                    ),
                    span,
                ));
            }
        }
        for item in &list_typed {
            if incompatible_complex_compare(
                expr_typed.value_type.data_type(),
                item.value_type.data_type(),
            )
            .is_some()
            {
                return Err(AnalyzeError::type_mismatch(
                    in_predicate_type_error(
                        expr_typed.value_type.data_type(),
                        item.value_type.data_type(),
                    ),
                    span,
                ));
            }
        }
        let mut common_type = expr_typed.value_type.clone();
        for item in &list_typed {
            self.check_control()?;
            common_type = self.require_common_trino_type(&common_type, &item.value_type, span)?;
        }
        expr_typed = self.coerce_common_value(expr_typed, &common_type, span)?;
        let mut coerced_list = Vec::with_capacity(list_typed.len());
        for item in list_typed {
            self.check_control()?;
            coerced_list.push(self.coerce_common_value(item, &common_type, span)?);
        }
        let list_typed = coerced_list;
        // Three-valued: `NULL IN (1, 2)` and `1 IN (NULL, 2)` are both
        // NULL, so the result is nullable whenever any operand is.
        let nullable = expr_typed.value_type.nullable()
            || list_typed
                .iter()
                .any(|candidate| candidate.value_type.nullable());
        Ok(TypedExpr {
            kind: ExprKind::InList {
                expr: novarocks_parser::ast::AstBox::new(expr_typed),
                list: (list_typed).into(),
                negated,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                nullable,
            ),
        })
    }
    fn analyze_between_predicate(
        &self,
        value: &ast::Expr,
        predicate: &ast::BetweenPredicate,
        span: Span,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let expr_typed = self.analyze_expr(value, scope)?;
        let low_typed = self.analyze_expr(&predicate.min, scope)?;
        let high_typed = self.analyze_expr(&predicate.max, scope)?;
        for (left, right) in [
            (&expr_typed, &low_typed),
            (&expr_typed, &high_typed),
            (&low_typed, &high_typed),
        ] {
            check_json_comparison_domains(&left.value_type, &right.value_type, "BETWEEN", span)?;
        }
        // BITMAP / HLL operands cannot participate in BETWEEN because
        // they have no ordering. Reject upfront so the user sees a
        // clear error before lowering / codegen.
        for operand in [&expr_typed, &low_typed, &high_typed] {
            if let Some(logical) = scope
                .logical_type_of_expr(operand)
                .filter(is_bitmap_or_hll_type)
            {
                let col = column_name_of_expr(operand);
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "BITMAP/HLL columns cannot appear in BETWEEN expressions (operand `{col}` has type {logical:?})"
                    ),
                    span,
                ));
            }
        }
        let common =
            self.require_common_trino_type(&expr_typed.value_type, &low_typed.value_type, span)?;
        let common = self.require_common_trino_type(&common, &high_typed.value_type, span)?;
        let cast = |operand: TypedExpr| self.coerce_common_value(operand, &common, span);
        let expr_typed = cast(expr_typed)?;
        let low_typed = cast(low_typed)?;
        let high_typed = cast(high_typed)?;
        // A comparison against NULL is NULL, so the result admits
        // null whenever any operand does. A filter treating null as
        // not-matching is the filter's own semantics, not this type's.
        let nullable = expr_typed.value_type.nullable()
            || low_typed.value_type.nullable()
            || high_typed.value_type.nullable();
        Ok(TypedExpr {
            kind: ExprKind::Between {
                expr: novarocks_parser::ast::AstBox::new(expr_typed),
                low: novarocks_parser::ast::AstBox::new(low_typed),
                high: novarocks_parser::ast::AstBox::new(high_typed),
                negated: predicate.negated,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                nullable,
            ),
        })
    }
    fn analyze_like_predicate(
        &self,
        value: &ast::Expr,
        predicate: &ast::LikePredicate,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        use novarocks_type_contract::{
            CharacterKind, FunctionValueType, ValueKind, ValueLogicalType,
        };

        let operand = |syntax: &ast::Expr| {
            let expr = self.analyze_expr(syntax, scope)?;
            if is_strict_null_value_type(&expr.value_type) {
                self.cast_to_value_type(
                    expr,
                    FunctionValueType::new(DataType::Utf8, true),
                    syntax.span(),
                )
            } else if expr.value_type.logical_type() == ValueLogicalType::Physical
                && expr.value_type.data_type() == &DataType::Utf8
            {
                Ok(expr)
            } else {
                Err(AnalyzeError::type_mismatch(
                    "LIKE operands must be character strings",
                    syntax.span(),
                ))
            }
        };
        let mut expr_typed = operand(value)?;
        let varchar = FunctionValueType::new(DataType::Utf8, false);
        if let ValueKind::BoundedCharacter(character) = expr_typed.value_type.value_type().kind()
            && character.kind() == CharacterKind::Char
        {
            // LIKE matches the CHAR value with spaces up to its declared width.
            let width = TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(i64::from(character.length()))),
                value_type: FunctionValueType::new(DataType::Int64, false),
            };
            let space = TypedExpr {
                kind: ExprKind::Literal(LiteralValue::String(" ".to_owned())),
                value_type: varchar.clone(),
            };
            let text = self.coerce_common_value(expr_typed, &varchar, value.span())?;
            expr_typed = resolved_scalar_call_at(
                self.function_catalog,
                "rpad",
                vec![text, width, space],
                value.span(),
                self.sql_semantics.sql_mode().decimal_overflow_policy(),
                self.constant_policy,
                self.control,
            )?;
        }
        let pattern_typed = operand(&predicate.pattern)?;
        if let Some(escape) = &predicate.escape {
            let args = vec![
                self.coerce_common_value(expr_typed, &varchar, value.span())?,
                self.coerce_common_value(pattern_typed, &varchar, predicate.pattern.span())?,
                self.coerce_common_value(operand(escape)?, &varchar, escape.span())?,
            ];
            let arguments = args
                .iter()
                .map(|arg| {
                    crate::analysis::function_argument(arg, self.constant_policy, self.control)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    AnalyzeError::function_binding(error).at_type_mismatch(predicate.span)
                })?;
            let binding = self
                .function_catalog
                .resolve_like_with_escape_binding(&arguments, self.control)
                .map_err(|error| {
                    AnalyzeError::function_binding(error).at_type_mismatch(predicate.span)
                })?;
            let novarocks_functions::FunctionResultType::Scalar(value_type) =
                &binding.selected.result_type
            else {
                return Err(AnalyzeError::internal("LIKE selected a non-scalar result"));
            };
            let value_type = value_type.clone();
            let mut result = TypedExpr {
                kind: ExprKind::FunctionCall {
                    volatility: binding.semantics.volatility,
                    name: novarocks_functions::native_like::ESCAPE_FUNCTION_NAME.to_owned(),
                    binding: crate::binding::SqlFunctionBinding::new(
                        binding,
                        self.sql_semantics.sql_mode().decimal_overflow_policy(),
                    ),
                    args: args.into(),
                    distinct: false,
                },
                value_type: value_type.clone(),
            };
            if predicate.negated {
                result = TypedExpr {
                    kind: ExprKind::UnaryOp {
                        op: UnOp::Not,
                        expr: novarocks_parser::ast::AstBox::new(result),
                    },
                    value_type,
                };
            }
            return Ok(result);
        }
        let nullable = expr_typed.value_type.nullable() || pattern_typed.value_type.nullable();
        Ok(TypedExpr {
            kind: ExprKind::Like {
                expr: novarocks_parser::ast::AstBox::new(expr_typed),
                pattern: novarocks_parser::ast::AstBox::new(pattern_typed),
                negated: predicate.negated,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                nullable,
            ),
        })
    }
    /// Borrow only the old positional tuple profile; field names never grant
    /// multi-column IN or higher-order map coercion.
    pub(super) fn anonymous_row<'e>(
        &self,
        expression: &'e ast::Expr,
    ) -> Result<Option<&'e ast::RowExpr>, AnalyzeError> {
        let ast::Expr::Row(row) = expression else {
            return Ok(None);
        };
        for field in &row.fields {
            self.check_control()?;
            if field.name.is_some() {
                return Ok(None);
            }
        }
        Ok(Some(row))
    }

    fn analyze_in_subquery_predicate(
        &self,
        value: &ast::Expr,
        query: &ast::Query,
        negated: bool,
        span: Span,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        // Multi-column LHS `(a, b) IN (SELECT c, d FROM ...)` arrives
        // here wrapped as `Expr::Row(...)` (or `Expr::Nested(Row(...))`).
        // The whole tuple is not a single scalar expression — the
        // analyzer's catch-all rejects it as "unsupported expression".
        // `subquery_rewrite::rewrite_in_subquery` handles the
        // decomposition (per-column equi-join) downstream, so just
        // skip the BITMAP / JSON / HLL precheck here: those checks
        // are per-column and only meaningful for a single-column LHS.
        let lhs_is_tuple = self.anonymous_row(value)?.is_some();
        let mut json_probe = false;
        if !lhs_is_tuple {
            // BITMAP / HLL operands cannot participate in IN subquery
            // because they have no scalar identity. Reject upfront by
            // resolving the LHS to detect a logical-type tag; the LHS
            // is dropped before the subquery is planned so we cannot
            // wait until later.
            let in_expr_typed = self.analyze_expr(value, scope)?;
            json_probe = is_json_in_subquery_operand(&in_expr_typed, scope);
            if let Some(logical) = scope
                .logical_type_of_expr(&in_expr_typed)
                .filter(is_bitmap_or_hll_type)
            {
                let col = column_name_of_expr(&in_expr_typed);
                let kw = if negated { "NOT IN" } else { "IN" };
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "BITMAP/HLL columns cannot appear in {kw} subquery expressions (operand `{col}` has type {logical:?})"
                    ),
                    span,
                ));
            }
        }
        let id = self.alloc_subquery_id();
        let kind = SubqueryKind::InSubquery { negated };
        self.collected_subqueries.borrow_mut().push(SubqueryInfo {
            id,
            kind: kind.clone(),
            subquery: ast::AstBox::new(query.clone()),
            data_type: DataType::Boolean,
            in_expr: Some(Box::new(value.clone())),
        });
        Ok(TypedExpr {
            kind: ExprKind::SubqueryPlaceholder {
                id,
                kind,
                data_type: DataType::Boolean,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                json_probe,
            ),
        })
    }
    fn analyze_comparison_predicate(
        &self,
        left: &ast::Expr,
        operator: novarocks_type_contract::OperatorFunction,
        right: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        use novarocks_type_contract::OperatorFunction;
        let op = crate::common::binary_operator::comparison_op(operator);
        let left_typed = self.analyze_expr(left, scope)?;
        let right_typed = self.analyze_expr(right, scope)?;
        let legacy_wide = crate::common::binary_operator::requires_legacy_wide(
            left_typed.value_type.value_type(),
        ) || crate::common::binary_operator::requires_legacy_wide(
            right_typed.value_type.value_type(),
        );
        let (left_typed, right_typed) = {
            // BITMAP / HLL columns have no scalar identity and cannot be
            // compared with `=`, `<`, etc. Reject before the operand type
            // is dropped to `Boolean` so the user sees a clear error.
            let op_sym = match op {
                BinOp::Eq => "=",
                BinOp::Ne => "!=",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::EqForNull => "<=>",
                _ => unreachable!(),
            };
            check_json_comparison_domains(
                &left_typed.value_type,
                &right_typed.value_type,
                op_sym,
                left.span(),
            )?;
            if let Some(logical) = scope
                .logical_type_of_expr(&left_typed)
                .filter(is_bitmap_or_hll_type)
            {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "comparison operator `{op_sym}` is not supported for BITMAP/HLL (left operand has type {logical:?})"
                    ),
                    left.span(),
                ));
            }
            if let Some(logical) = scope
                .logical_type_of_expr(&right_typed)
                .filter(is_bitmap_or_hll_type)
            {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "comparison operator `{op_sym}` is not supported for BITMAP/HLL (right operand has type {logical:?})"
                    ),
                    right.span(),
                ));
            }
            // Ordering operators (`<`, `<=`, `>`, `>=`) are undefined on
            // ARRAY / MAP / STRUCT values in standalone SQL. Keep equality
            // and null-safe equality on ARRAY available, but reject
            // ordering predicates at analysis time so join predicates fail
            // before reaching execution.
            let is_ordering_op = matches!(op, BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge);
            let has_temporal = left_typed.value_type.value_type().temporal().is_some()
                || right_typed.value_type.value_type().temporal().is_some();
            if is_ordering_op && !has_temporal {
                let unsupported_complex_kind = |dt: &DataType| match dt {
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                        Some("ARRAY")
                    }
                    DataType::Map(_, _) => Some("MAP"),
                    DataType::Struct(_) => Some("STRUCT"),
                    _ => None,
                };
                if let Some(kind) = unsupported_complex_kind(left_typed.value_type.data_type())
                    .or_else(|| unsupported_complex_kind(right_typed.value_type.data_type()))
                {
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "comparison operator `{op_sym}` does not support binary predicate operation on {kind} values"
                        ),
                        left.span(),
                    ));
                }
            }
            // Reject comparisons between complex types whose element /
            // entry / field layouts are fundamentally incompatible.
            // Cases like `array<int> = [map{...}, null]` would otherwise
            // fall through to a runtime CAST that produces a confusing
            // error; surface a clear analyzer-level message instead.
            if !has_temporal
                && let Some(reason) = incompatible_complex_compare(
                    left_typed.value_type.data_type(),
                    right_typed.value_type.data_type(),
                )
            {
                return Err(AnalyzeError::type_mismatch(
                    format!(
                        "comparison operator `{op_sym}` does not support binary predicate operation between {reason}"
                    ),
                    left.span(),
                ));
            }
            let integer_array_equality =
                matches!(
                    operator,
                    OperatorFunction::Equal | OperatorFunction::NotEqual
                ) && [&left_typed, &right_typed].into_iter().all(|operand| {
                    novarocks_functions::PreparedComparisonRecipe::supports_array_equality_operand(
                        &operand.value_type,
                    )
                });
            if !legacy_wide
                && (integer_array_equality
                    || !crate::common::binary_operator::uses_structural_comparison(
                        left_typed.value_type.value_type(),
                    ) && !crate::common::binary_operator::uses_structural_comparison(
                        right_typed.value_type.value_type(),
                    ))
            {
                return self.analyze_binary_operator(
                    operator,
                    left_typed,
                    right_typed,
                    left.span(),
                );
            }
            let common = self.require_common_trino_type(
                &left_typed.value_type,
                &right_typed.value_type,
                left.span(),
            )?;
            (
                self.coerce_common_value(left_typed, &common, left.span())?,
                self.coerce_common_value(right_typed, &common, right.span())?,
            )
        };
        let nullable = op != BinOp::EqForNull
            && (left_typed.value_type.nullable()
                || right_typed.value_type.nullable()
                || compares_element_wise_with_nulls(left_typed.value_type.data_type())
                || compares_element_wise_with_nulls(right_typed.value_type.data_type()));
        let equality = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(left_typed),
                op,
                right: novarocks_parser::ast::AstBox::new(right_typed),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                nullable,
            ),
        };
        if operator == OperatorFunction::Distinct {
            let value_type = equality.value_type.clone();
            Ok(TypedExpr {
                kind: ExprKind::UnaryOp {
                    op: UnOp::Not,
                    expr: novarocks_parser::ast::AstBox::new(equality),
                },
                value_type,
            })
        } else {
            Ok(equality)
        }
    }

    pub(super) fn analyze_binary_operator(
        &self,
        operator: novarocks_type_contract::OperatorFunction,
        left: TypedExpr,
        right: TypedExpr,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let call = self.type_scalar_call(NodeShape::Operator(operator), vec![left, right], span)?;
        Ok(crate::common::binary_operator::typed_call(
            operator,
            call.binding,
            call.args,
        ))
    }

    /// Analyze original logical terms in source order, then balance only the
    /// typed IR. Each term keeps its actual materialized-source identity.
    fn analyze_boolean_chain(
        &self,
        logical: &ast::LogicalExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if logical.terms.len() < 2 {
            return Err(AnalyzeError::unsupported_expression(
                "a logical expression requires at least two terms",
                logical.span,
            ));
        }
        let mut analyzed = Vec::with_capacity(logical.terms.len());
        for term in &logical.terms {
            analyzed.push(self.analyze_expr(term, scope)?);
        }
        let (bin_op, shape) = match logical.operator {
            ast::LogicalOperator::And => (BinOp::And, NodeShape::And),
            ast::LogicalOperator::Or => (BinOp::Or, NodeShape::Or),
        };
        let (result_type, mut analyzed) = self.type_value_node(shape, analyzed, logical.span)?;
        while analyzed.len() > 1 {
            let mut next_level = Vec::with_capacity(analyzed.len().div_ceil(2));
            let mut operands = analyzed.into_iter();
            while let Some(left) = operands.next() {
                if let Some(right) = operands.next() {
                    let nullable = left.value_type.nullable() || right.value_type.nullable();
                    next_level.push(TypedExpr {
                        kind: ExprKind::BinaryOp {
                            left: novarocks_parser::ast::AstBox::new(left),
                            op: bin_op,
                            right: novarocks_parser::ast::AstBox::new(right),
                            decimal_overflow_policy:
                                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                        },
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Boolean,
                            nullable,
                        ),
                    });
                } else {
                    next_level.push(left);
                }
            }
            analyzed = next_level;
        }
        let mut result = analyzed
            .pop()
            .expect("a validated logical expression has terms");
        result.value_type = result_type;
        Ok(result)
    }

    pub(super) fn require_common_trino_type(
        &self,
        left: &novarocks_type_contract::FunctionValueType,
        right: &novarocks_type_contract::FunctionValueType,
        span: Span,
    ) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
        super::helpers::assignment_common_value_type(left, right, self.control)
            .map_err(|error| error.at_type_mismatch(span))
    }

    pub(super) fn coerce_common_value(
        &self,
        source: TypedExpr,
        common: &novarocks_type_contract::FunctionValueType,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let target = super::helpers::with_nullability(common.clone(), source.value_type.nullable());
        if source.value_type == target {
            return Ok(source);
        }
        self.cast_with_trino_mode(
            source,
            target,
            novarocks_type_contract::TrinoCastMode::Implicit,
            span,
        )
    }

    fn analyze_binary_op(
        &self,
        binary: &ast::BinaryExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let left = binary.left.as_ref();
        let op = &binary.operator;
        let right = binary.right.as_ref();
        let left_typed = self.analyze_expr(left, scope)?;
        let right_typed = self.analyze_expr(right, scope)?;

        if let Some(operator) = novarocks_expression::typing::temporal_binary_operator(
            arithmetic_operator_of(op),
            &left_typed.value_type,
            &right_typed.value_type,
        )
        .map_err(|error| super::expression_typing::analyze_typing_error(error, binary.span))?
        {
            return self.analyze_temporal_operator(
                operator,
                vec![left_typed, right_typed],
                binary.span,
            );
        }
        let legacy = |value: &TypedExpr| {
            crate::common::binary_operator::requires_legacy_wide(value.value_type.value_type())
                || matches!(
                    value.value_type.data_type(),
                    DataType::Date32
                        | DataType::Date64
                        | DataType::Timestamp(..)
                        | DataType::Time32(_)
                        | DataType::Time64(_)
                        | DataType::Duration(_)
                        | DataType::Interval(_)
                )
        };
        if !legacy(&left_typed) && !legacy(&right_typed) {
            return self.analyze_binary_operator(
                novarocks_expression::typing::syntax_arithmetic(*op),
                left_typed,
                right_typed,
                binary.left_operand_span,
            );
        }

        // A boolean is a one-bit number once it reaches arithmetic: TRUE is 1
        // and FALSE is 0, and `x + any_match(...)` is how a lambda counts the
        // elements that matched. Spelling that as a cast to the width a
        // boolean is stored at keeps the frozen numeric rules below the only
        // place that decides a result type, instead of teaching each of them
        // a second operand kind.
        let (left_typed, right_typed) = self.cast_operands_to_numbers(
            cast_boolean_operand_to_number(left_typed),
            cast_boolean_operand_to_number(right_typed),
            arithmetic_operator_of(op),
            binary.left_operand_span,
        )?;

        let arithmetic_type = |operator| {
            arithmetic_result_value_type_with_op(
                &left_typed.value_type,
                &right_typed.value_type,
                operator,
            )
            .ok_or_else(|| {
                AnalyzeError::type_mismatch(
                    format!(
                        "arithmetic operator `{operator:?}` has no frozen result rule for {:?} and {:?}",
                        left_typed.value_type.data_type(), right_typed.value_type.data_type()
                    ),
                    binary.left_operand_span,
                )
            })
        };
        let (bin_op, result_type) = match op {
            // Arithmetic operators -> inferred type
            ast::BinaryOperator::Add => {
                let dt = arithmetic_type(ArithmeticOperator::Add)?;
                (BinOp::Add, dt)
            }
            ast::BinaryOperator::Subtract => {
                let dt = arithmetic_type(ArithmeticOperator::Subtract)?;
                (BinOp::Sub, dt)
            }
            ast::BinaryOperator::Multiply => {
                let dt = arithmetic_type(ArithmeticOperator::Multiply)?;
                (BinOp::Mul, dt)
            }
            ast::BinaryOperator::Divide => {
                let dt = arithmetic_type(ArithmeticOperator::Divide)?;
                (BinOp::Div, dt)
            }
            ast::BinaryOperator::Modulo => {
                let dt = arithmetic_type(ArithmeticOperator::Modulo)?;
                (BinOp::Mod, dt)
            }
        };

        if let DataType::Decimal128(precision, scale) = &result_type.data_type()
            && *scale > *precision as i8
        {
            return Err(AnalyzeError::invalid_literal(
                format!("scale {scale} is greater than max {precision}"),
                binary.left_operand_span,
            ));
        }

        // Arithmetic answers with NULL where it cannot answer with a number:
        // a product or sum that overflows its declared decimal, a division or
        // modulo by zero. Comparison and the boolean connectives have no such
        // gap -- they are defined for every pair of values they accept -- so
        // only they carry their operands' nullability through.
        //
        // Null-safe equality is the exception among comparisons: answering
        // about null is what it is for. `NULL <=> NULL` is true and
        // `1 <=> NULL` is false, so it is total whatever its operands admit.
        let nullable = match bin_op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => true,
            BinOp::EqForNull => false,
            // An ordering comparison of two complex values compares their
            // elements, and a NULL element answers NULL. `<=>` is exempt: it
            // is defined to answer a boolean for every pair of values.
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                left_typed.value_type.nullable()
                    || right_typed.value_type.nullable()
                    || compares_element_wise_with_nulls(left_typed.value_type.data_type())
                    || compares_element_wise_with_nulls(right_typed.value_type.data_type())
            }
            _ => left_typed.value_type.nullable() || right_typed.value_type.nullable(),
        };
        Ok(TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(left_typed),
                op: bin_op,
                right: novarocks_parser::ast::AstBox::new(right_typed),
                decimal_overflow_policy: if matches!(
                    bin_op,
                    BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod
                ) {
                    self.sql_semantics.sql_mode().decimal_overflow_policy()
                } else {
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull
                },
            },
            value_type: super::helpers::with_nullability(result_type, nullable),
        })
    }

    fn analyze_temporal_operator(
        &self,
        operator: novarocks_type_contract::OperatorFunction,
        args: Vec<TypedExpr>,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let bound = self.type_scalar_call(NodeShape::Operator(operator), args, span)?;
        let novarocks_functions::FunctionResultType::Scalar(result) =
            &bound.binding.selected.result_type
        else {
            unreachable!("temporal operators have scalar results");
        };
        Ok(TypedExpr {
            value_type: result.clone(),
            kind: ExprKind::FunctionCall {
                name: operator.catalog_name().to_owned(),
                args: bound.args.into(),
                distinct: false,
                volatility: bound.binding.semantics.volatility,
                binding: bound.binding,
            },
        })
    }

    fn analyze_exists(
        &self,
        exists: &ast::ExistsExpr,
        negated: bool,
    ) -> Result<TypedExpr, AnalyzeError> {
        self.check_control()?;
        let ast::Expr::Subquery(subquery) = &exists.subquery else {
            return Err(AnalyzeError::unsupported_expression(
                "EXISTS requires a subquery expression",
                exists.span,
            ));
        };
        let id = self.alloc_subquery_id();
        let kind = SubqueryKind::Exists { negated };
        self.collected_subqueries.borrow_mut().push(SubqueryInfo {
            id,
            kind: kind.clone(),
            subquery: subquery.query.clone(),
            data_type: DataType::Boolean,
            in_expr: None,
        });
        Ok(TypedExpr {
            kind: ExprKind::SubqueryPlaceholder {
                id,
                kind,
                data_type: DataType::Boolean,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        })
    }

    /// Analyze a CASE expression.
    fn analyze_searched_case(
        &self,
        clauses: &[ast::WhenClause],
        else_result: Option<&ast::Expr>,
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut children = Vec::new();
        for clause in clauses {
            let ast::WhenMatch::Operand(condition) = &clause.match_value else {
                return Err(AnalyzeError::unsupported_expression(
                    "partial CASE predicates are not supported",
                    clause.span,
                ));
            };
            children.push(self.analyze_expr(condition, scope)?);
            children.push(self.analyze_expr(&clause.result, scope)?);
        }
        if let Some(otherwise) = else_result {
            children.push(self.analyze_expr(otherwise, scope)?);
        }
        let (value_type, mut children) = self.type_value_node(
            NodeShape::SearchedCase {
                when_count: clauses.len(),
                has_else: else_result.is_some(),
            },
            children,
            span,
        )?;
        let else_expr = else_result.map(|_| {
            novarocks_parser::ast::AstBox::new(children.pop().expect("searched CASE has an ELSE"))
        });
        let mut operands = children.into_iter();
        let when_then = std::iter::from_fn(|| {
            let condition = operands.next()?;
            let value = operands
                .next()
                .expect("searched CASE has a value for every WHEN");
            Some((condition, value))
        })
        .collect::<Vec<_>>();
        Ok(TypedExpr {
            kind: ExprKind::Case {
                ieee_float_equality: false,
                operand: None,
                when_then: when_then.into(),
                else_expr,
            },
            value_type,
        })
    }

    fn analyze_case(
        &self,
        operand: Option<&ast::Expr>,
        clauses: &[ast::WhenClause],
        else_result: Option<&ast::Expr>,
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut operand_typed = match operand {
            Some(e) => Some(Box::new(self.analyze_expr(e, scope)?)),
            None => None,
        };

        let mut when_then = Vec::with_capacity(clauses.len());
        let mut result_type = novarocks_type_contract::FunctionValueType::new(DataType::Null, true);
        for clause in clauses {
            let ast::WhenMatch::Operand(condition) = &clause.match_value else {
                return Err(AnalyzeError::unsupported_expression(
                    "partial CASE predicates are not supported",
                    clause.span,
                ));
            };
            let when_typed = self.analyze_expr(condition, scope)?;
            let then_typed = self.analyze_expr(&clause.result, scope)?;
            result_type = super::helpers::assignment_common_value_type(
                &result_type,
                &then_typed.value_type,
                self.control,
            )?;
            when_then.push((when_typed, then_typed));
        }

        let else_typed = match else_result {
            Some(e) => {
                let typed = self.analyze_expr(e, scope)?;
                result_type = super::helpers::assignment_common_value_type(
                    &result_type,
                    &typed.value_type,
                    self.control,
                )?;
                Some(Box::new(typed))
            }
            None => None,
        };

        let mut ieee_float_equality = false;
        if let Some(value) = operand_typed.take() {
            use crate::common::binary_operator::requires_legacy_wide;

            let mut legacy_wide = requires_legacy_wide(value.value_type.value_type());
            let mut common = value.value_type.clone();
            for (label, _) in &when_then {
                self.check_control()?;
                legacy_wide |= requires_legacy_wide(label.value_type.value_type());
                check_json_comparison_domains(&common, &label.value_type, "=", span)?;
                common = self.require_common_trino_type(&common, &label.value_type, span)?;
            }
            ieee_float_equality = !legacy_wide
                && common.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
                && matches!(common.data_type(), DataType::Float32 | DataType::Float64);

            let cast = |input: TypedExpr| {
                if ieee_float_equality
                    && matches!(input.value_type.data_type(), DataType::Decimal128(..))
                {
                    self.coerce_simple_case_float_operand(input, &common, span)
                } else {
                    self.coerce_common_value(input, &common, span)
                }
            };
            operand_typed = Some(Box::new(cast(*value)?));
            let mut coerced_when_then = Vec::with_capacity(when_then.len());
            for (label, result) in when_then {
                self.check_control()?;
                coerced_when_then.push((cast(label)?, result));
            }
            when_then = coerced_when_then;
        }

        let when_then: Vec<(TypedExpr, TypedExpr)> = when_then
            .into_iter()
            .map(|(w, t)| Ok((w, self.coerce_common_value(t, &result_type, span)?)))
            .collect::<Result<_, AnalyzeError>>()?;
        let else_typed = else_typed
            .map(|e| {
                self.coerce_common_value(*e, &result_type, span)
                    .map(Box::new)
            })
            .transpose()?;

        Ok(TypedExpr {
            kind: ExprKind::Case {
                ieee_float_equality,
                operand: operand_typed.map(novarocks_parser::ast::AstBox::from_box),
                when_then: (when_then).into(),
                else_expr: else_typed.map(novarocks_parser::ast::AstBox::from_box),
            },
            value_type: super::helpers::with_nullability(result_type, true),
        })
    }

    /// Analyze a function call expression.
    fn analyze_function(
        &self,
        func: &ast::FunctionCall,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let original_arguments = func
            .arguments
            .iter()
            .map(|argument| &argument.value)
            .collect::<Vec<_>>();
        self.analyze_function_with_original_arguments(func, &original_arguments, scope)
    }

    /// Borrow dedicated syntax children into the original function author.
    fn analyze_syntax_function(
        &self,
        name: &str,
        arguments: &[&ast::Expr],
        span: Span,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if name == "row" {
            let args = arguments
                .iter()
                .map(|argument| self.analyze_expr(argument, scope))
                .collect::<Result<Vec<_>, _>>()?;
            let call = self.type_scalar_call(NodeShape::Row, args, span)?;
            let value_type = call.value_type().clone();
            let volatility = call.binding.semantics.volatility;
            return Ok(TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: "row".to_owned(),
                    args: call.args.into(),
                    distinct: false,
                    volatility,
                    binding: call.binding,
                },
                value_type,
            });
        }
        self.analyze_function_from_metadata(
            FunctionMetadata {
                original: None,
                span,
                distinct: false,
                order_by: None,
                window: None,
                null_treatment: None,
            },
            name.to_owned(),
            arguments,
            scope,
        )
    }

    /// Share the original function admission/binding path while preserving
    /// actual child-expression addresses for materialized constants and writes.
    /// Reserved JSON constructors supply plain call metadata and their original
    /// ordered child references; ordinary calls supply their own argument list.
    pub(super) fn analyze_function_with_original_arguments(
        &self,
        func: &ast::FunctionCall,
        original_arguments: &[&ast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if let Some(reason) = unsupported_call_facts(func) {
            return Err(AnalyzeError::unsupported_expression(reason, func.span));
        }
        // These exact canonical Trino calls have no selected implementation.
        // Keep this direct capability refusal after the original metadata guard;
        // qualified and differently quoted ordinary names retain their lookup.
        if let [name] = func.name.parts.as_slice()
            && ((!name.quoted && name.value.eq_ignore_ascii_case("strpos"))
                || (name.quoted && name.value == "normalize"))
        {
            return Err(AnalyzeError::unsupported_expression(
                format!(
                    "builtin function `{}` has no admitted selected scalar implementation",
                    name.value.to_ascii_lowercase()
                ),
                func.span,
            ));
        }
        let original_name = print_object_name(&func.name).to_ascii_lowercase();
        let original_input = if func.filter.is_some()
            && func.window.is_none()
            && (is_aggregate_function(self.function_catalog, &original_name)
                || is_analyzer_aggregate_syntax(&original_name))
        {
            self.grouping_scope
                .borrow()
                .as_ref()
                .and_then(|state| state.from_scope.borrow().clone())
        } else {
            None
        };
        let mut call = if original_name == "grouping" {
            self.analyze_grouping_function(func, scope)?
        } else {
            self.analyze_function_from_metadata(
                FunctionMetadata {
                    original: Some(func),
                    span: func.span,
                    distinct: func.distinct,
                    order_by: func.order_by.as_ref(),
                    window: func.window.as_ref(),
                    null_treatment: func.null_treatment,
                },
                original_name,
                original_arguments,
                original_input.as_ref().unwrap_or(scope),
            )?
        };
        if let Some(predicate) = &func.filter {
            match &mut call.kind {
                ExprKind::AggregateCall { filter, .. } => {
                    *filter = Some(ast::AstBox::new(
                        self.analyze_aggregate_filter(predicate, scope)?,
                    ));
                }
                ExprKind::WindowCall { .. } => {
                    return Err(AnalyzeError::unsupported_expression(
                        "FILTER is not supported for window functions",
                        predicate.span(),
                    ));
                }
                _ => {
                    return Err(AnalyzeError::at(
                        AnalyzeErrorKind::FunctionNotAggregate,
                        "FILTER requires an aggregate function",
                        predicate.span(),
                    ));
                }
            }
        }
        Ok(call)
    }

    fn analyze_aggregate_filter(
        &self,
        predicate: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        struct ScalarCheck<'a, 'catalog> {
            analyzer: &'a super::AnalyzerContext<'catalog>,
            error: Option<AnalyzeError>,
        }
        impl ast::Visit for ScalarCheck<'_, '_> {
            fn visit_query(&mut self, _: &ast::Query) {}

            #[recursive::recursive]
            fn visit_expr(&mut self, expression: &ast::Expr) {
                if self.error.is_some() {
                    return;
                }
                if let Err(error) = self.analyzer.check_control() {
                    self.error = Some(error);
                    return;
                }
                let relation_predicate = match expression {
                    ast::Expr::Exists(_) => true,
                    ast::Expr::Predicated(node) => matches!(
                        &node.predicate,
                        ast::Predicate::In(input) if matches!(input.value_list, ast::Expr::Subquery(_))
                    ),
                    _ => false,
                };
                if relation_predicate {
                    self.error = Some(AnalyzeError::unsupported_expression(
                        "Relational EXISTS and IN subqueries are not supported in aggregate FILTER",
                        expression.span(),
                    ));
                    return;
                }
                let non_scalar = match expression {
                    ast::Expr::GroupingOperation(_) | ast::Expr::WindowOperation(_) => true,
                    ast::Expr::FunctionCall(call) => {
                        let name = print_object_name(&call.name).to_ascii_lowercase();
                        call.window.is_some()
                            || name == "grouping"
                            || is_aggregate_function(self.analyzer.function_catalog, &name)
                            || is_analyzer_aggregate_syntax(&name)
                    }
                    _ => false,
                };
                if non_scalar {
                    self.error = Some(AnalyzeError::at(
                        AnalyzeErrorKind::ExpressionNotScalar,
                        "Aggregate FILTER must contain only scalar expressions",
                        expression.span(),
                    ));
                } else {
                    ast::walk_expr(self, expression);
                }
            }
        }
        let mut check = ScalarCheck {
            analyzer: self,
            error: None,
        };
        ast::Visit::visit_expr(&mut check, predicate);
        if let Some(error) = check.error {
            return Err(error);
        }
        let from_scope = self
            .grouping_scope
            .borrow()
            .as_ref()
            .and_then(|state| state.from_scope.borrow().clone());
        let typed = self.analyze_expr(predicate, from_scope.as_ref().unwrap_or(scope))?;
        self.require_boolean(
            typed,
            predicate.span(),
            "Aggregate FILTER predicate must be BOOLEAN",
        )
    }

    fn require_boolean(
        &self,
        value: TypedExpr,
        span: Span,
        message: &str,
    ) -> Result<TypedExpr, AnalyzeError> {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        if is_strict_null_value_type(&value.value_type) {
            self.cast_to_value_type(value, FunctionValueType::new(DataType::Boolean, true), span)
        } else if value.value_type.logical_type() == ValueLogicalType::Physical
            && value.value_type.data_type() == &DataType::Boolean
        {
            Ok(value)
        } else {
            Err(AnalyzeError::type_mismatch(message, span))
        }
    }

    #[recursive::recursive]
    fn analyze_function_from_metadata(
        &self,
        func: FunctionMetadata<'_>,
        original_name: String,
        original_arguments: &[&ast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if matches!(
            original_name.as_str(),
            "row" | "\"row\"" | "named_struct" | "\"named_struct\""
        ) {
            return Err(AnalyzeError::unknown_function(
                format!("Unknown function: {original_name}"),
                func.span,
            ));
        }
        let name = match original_name.as_str() {
            "every" => "bool_and".to_string(),
            _ => original_name,
        };
        let mut name = name;

        // Preserve the original zero-input ORDER BY refusal before aggregate
        // ordering validation. Legacy SEPARATOR is normalized by its parser owner.
        if original_arguments.is_empty() && func.order_by.is_some() {
            return Err(AnalyzeError::invalid_query_shape(
                if func.distinct {
                    "Unexpected input 'order', the most similar input is {a legal identifier}."
                        .to_string()
                } else {
                    "Unexpected input '(', the most similar input is {<EOF>, ';'}.".to_string()
                },
                func.span,
            ));
        }
        // Check for DISTINCT
        let is_distinct = func.distinct;

        // `*` is parser-native syntax inside a function call, not a column.
        // Keep it out of ordinary argument resolution: the empty aggregate
        // argument list is the analyzer representation of COUNT(*).
        let is_count_star = name == "count"
            && matches!(original_arguments, [ast::Expr::Identifier(ident)] if ident.value == "*");

        // Canonical calls borrow the actual original argument occurrences.
        let arg_exprs = original_arguments.to_vec();

        if name == "typeof"
            && let [argument] = arg_exprs.as_slice()
        {
            // A CAST declaration supplies the type without inspecting its
            // child. Other expressions contribute their analyzed value type.
            let declared_name = match *argument {
                ast::Expr::Cast(cast) => typeof_cast_name(&cast.data_type, self.control)?,
                argument => sql_expr_logical_type_name(argument),
            };
            let type_name = match declared_name {
                Some(name) => name,
                None => {
                    let argument = self.analyze_expr(argument, scope)?;
                    typeof_name(
                        argument.value_type.data_type(),
                        Some(argument.value_type.value_type()),
                    )
                }
            };
            return Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::String(type_name)),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
            });
        }
        if matches!(name.as_str(), "group_concat" | "string_agg") && arg_exprs.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                "group_concat should have at least one input.",
                func.span,
            ));
        }
        if matches!(name.as_str(), "array_agg" | "array_agg_distinct") {
            if arg_exprs.is_empty() {
                return Err(AnalyzeError::invalid_argument(
                    "array_agg should have at least one input.",
                    func.span,
                ));
            }
            if arg_exprs.len() != 1 {
                return Err(AnalyzeError::invalid_query_shape(
                    "Unexpected input 'order', the most similar input is {',', ')'}.",
                    func.span,
                ));
            }
        }
        if name == "any_value" && is_distinct {
            return Err(AnalyzeError::invalid_query_shape(
                "Getting syntax error",
                func.span,
            ));
        }
        let effective_arg_exprs = if is_count_star {
            &[]
        } else {
            arg_exprs.as_slice()
        };
        if let Some(signature) = novarocks_functions::builtin::collection_lambda::signature(&name)
            && effective_arg_exprs
                .iter()
                .any(|argument| matches!(argument, ast::Expr::Lambda(_)))
        {
            let [collection, ast::Expr::Lambda(lambda)] = effective_arg_exprs else {
                return Err(AnalyzeError::unknown_function(
                    format!("No matching signature for function {name}"),
                    func.span,
                ));
            };
            let collection = self.analyze_expr(collection, scope)?;
            if lambda.arguments.len() != signature.arity()
                || !signature.accepts_collection(&collection.value_type)
            {
                return Err(AnalyzeError::unknown_function(
                    format!("No matching signature for function {name}"),
                    func.span,
                ));
            }
            return Err(AnalyzeError::function_binding(
                novarocks_functions::FunctionBindingError::UnavailableImplementation(
                    signature.overload(),
                ),
            )
            .at_type_mismatch(func.span));
        }
        if effective_arg_exprs
            .iter()
            .any(|arg| matches!(arg, ast::Expr::Lambda(_)))
            && !is_window_only_function(&name)
            && !is_aggregate_function(self.function_catalog, &name)
            && crate::functions::builtin_disposition(&name).is_none()
            && !self
                .function_catalog
                .recognizes_scalar_name(&name, &[], self.control)
                .map_err(AnalyzeError::function_resolution)?
        {
            return Err(AnalyzeError::unknown_function(
                format!("Unknown function: {name}"),
                func.span,
            ));
        }

        let mut args_typed = Vec::with_capacity(effective_arg_exprs.len());
        let mut arg_types = Vec::with_capacity(effective_arg_exprs.len());
        for arg in effective_arg_exprs {
            let typed = self.analyze_expr(arg, scope)?;
            arg_types.push(typed.value_type.data_type().clone());
            args_typed.push(typed);
        }

        let needs_statistical_float_args = matches!(
            name.as_str(),
            "corr"
                | "covar_pop"
                | "covar_samp"
                | "var_pop"
                | "var_samp"
                | "variance"
                | "variance_pop"
                | "variance_samp"
                | "stddev"
                | "stddev_pop"
                | "stddev_samp"
        );
        if needs_statistical_float_args {
            for arg in &mut args_typed {
                if matches!(
                    arg.value_type.data_type(),
                    DataType::Null | DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                ) {
                    let inner = std::mem::replace(
                        arg,
                        TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Null),
                            value_type: novarocks_type_contract::FunctionValueType::new(
                                DataType::Null,
                                true,
                            ),
                        },
                    );
                    *arg = TypedExpr {
                        kind: ExprKind::Cast {
                            cast_mode: None,
                            expr: novarocks_parser::ast::AstBox::new(inner),
                            target: DataType::Float64,
                            decimal_overflow_policy: self
                                .sql_semantics
                                .sql_mode()
                                .decimal_overflow_policy(),
                        },
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Float64,
                            true,
                        ),
                    };
                }
            }
            arg_types = args_typed
                .iter()
                .map(|a| a.value_type.data_type().clone())
                .collect();
        }

        if name == "flatten"
            && let Some(DataType::List(item)) = arg_types.first()
            && !matches!(item.data_type(), DataType::List(_))
        {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "The only one input of array_flatten should be an array of arrays, rather than {}",
                    starrocks_error_type_name(&arg_types[0])
                ),
                func.span,
            ));
        }

        if name == "array_agg" && is_distinct {
            if args_typed
                .first()
                .is_some_and(is_non_groupable_map_constructor)
            {
                return Err(AnalyzeError::invalid_argument("Unknown error", func.span));
            }
            if let Some(semantic_type) = args_typed.first().and_then(|arg| {
                // Metadata output adapters must preserve the existing JSON
                // DISTINCT rejection after the literal's physical call is wrapped.
                if self.json_list_provenance(effective_arg_exprs.first().copied(), arg, scope) {
                    Some("array<json>".to_string())
                } else {
                    json_semantic_group_by_type_name(arg)
                }
            }) {
                let arg_display = expr_display_name(arg_exprs[0]);
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "array_agg(DISTINCT {arg_display}) can't rewrite distinct to group by on ({semantic_type})."
                    ),
                    func.span,
                ));
            }
        }

        let needs_boolean_args = matches!(
            name.as_str(),
            "bool_or" | "bool_and" | "boolor_agg" | "booland_agg" | "every" | "count_if"
        );
        if needs_boolean_args {
            let boolean = novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true);
            args_typed = args_typed
                .into_iter()
                .map(|arg| {
                    if arg.value_type.data_type() == &DataType::Null {
                        self.coerce_common_value(arg, &boolean, func.span)
                    } else {
                        Ok(arg)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            arg_types = args_typed
                .iter()
                .map(|arg| arg.value_type.data_type().clone())
                .collect();
        }

        if name == "count_if" && is_distinct {
            return Err(AnalyzeError::invalid_query_shape(
                "Unexpected input '(', the most similar input is {<EOF>, ';'}.",
                func.span,
            ));
        }

        validate_group_concat_separator_argument(&name, &arg_exprs, &args_typed)
            .map_err(|message| AnalyzeError::invalid_argument(message, func.span))?;
        validate_group_concat_value_arguments(&name, &args_typed)
            .map_err(|message| AnalyzeError::invalid_argument(message, func.span))?;

        // Extract ORDER BY within function args (for aggregates like array_agg)
        let func_order_by = match func.original {
            Some(original) => self.extract_function_order_by(original, scope, &args_typed)?,
            None => Vec::new(),
        };

        // Check for window function: func(...) OVER (...)
        if let Some(window_type) = func.window {
            let window_only = is_window_only_function(&name);
            let aggregate_binding = if window_only {
                None
            } else {
                if !self.function_catalog.contains_aggregate(&name) {
                    // Legacy declarations are type-inspection facts, not installed selected bindings.
                    // A custom contribution of the same name is resolved by its own catalog identity.
                    if scalar_signature_is_unknown(
                        self.function_catalog,
                        &name,
                        &arg_types,
                        self.control,
                    )? && matches!(
                        crate::functions::builtin_disposition(&name),
                        Some(
                            crate::functions::BuiltinDisposition::Unavailable
                                | crate::functions::BuiltinDisposition::LoweredOnly
                        )
                    ) {
                        return Err(AnalyzeError::unsupported_expression(
                            format!(
                                "builtin function `{name}` has no admitted selected scalar implementation"
                            ),
                            func.span,
                        ));
                    }
                    if scalar_function_is_unknown(
                        self.function_catalog,
                        &name,
                        &arg_types,
                        self.control,
                    )? {
                        return Err(AnalyzeError::unknown_function(
                            format!("Unknown function: {name}"),
                            func.span,
                        ));
                    }
                    return Err(AnalyzeError::unsupported_expression(
                        format!("function {name} is not supported with OVER clause"),
                        func.span,
                    ));
                }
                Some(resolve_aggregate_function_call_with_order(
                    self.function_catalog,
                    &name,
                    is_distinct,
                    &args_typed,
                    &func_order_by,
                    func.span,
                    self.sql_semantics.sql_mode().decimal_overflow_policy(),
                    self.constant_policy,
                    self.control,
                    self.purpose,
                )?)
            };
            if is_distinct && (window_only || !aggregate_window_supports_distinct(&name)) {
                return Err(AnalyzeError::unsupported_expression(
                    format!("{name} DISTINCT is not supported with OVER clause"),
                    func.span,
                ));
            }
            if !func_order_by.is_empty() && !aggregate_window_supports_function_order_by(&name) {
                return Err(AnalyzeError::unsupported_expression(
                    format!("{name} ORDER BY is not supported with OVER clause"),
                    func.span,
                ));
            }
            if aggregate_binding.is_some() && !is_supported_aggregate_window_function(&name) {
                return Err(AnalyzeError::unsupported_expression(
                    format!("aggregate function {name} is not supported with OVER clause"),
                    func.span,
                ));
            }
            // StarRocks rejects LEAD/LAG when the third (default) argument
            // doesn't match a per-shape type rule. The error message echoes
            // the value column's type (INT/FLOAT/DECIMAL...) and is asserted
            // by SQL regression tests.
            if matches!(name.as_str(), "lead" | "lag") && args_typed.len() >= 3 {
                let value_type = args_typed[0].value_type.data_type().clone();
                let default_arg = &args_typed[2];
                if !is_lead_lag_default_arg_acceptable(default_arg, &value_type) {
                    return Err(AnalyzeError::type_mismatch(
                        format!(
                            "The type of the third parameter of LEAD/LAG not match the type {}.",
                            lead_lag_type_display(&value_type)
                        ),
                        func.span,
                    ));
                }
            }
            let (partition_by, order_by, window_frame) =
                self.analyze_window_spec(window_type, scope)?;
            let (execution_name, order_by, window_frame) =
                crate::analysis::normalize_window_for_execution(&name, order_by, window_frame);
            name = execution_name;
            let binding = if let Some(aggregate) = &aggregate_binding {
                aggregate.clone()
            } else {
                let (coerced, binding) = bind_window_function_call_with_catalog(
                    self.function_catalog,
                    &name,
                    args_typed,
                    func.span,
                    self.sql_semantics.sql_mode().decimal_overflow_policy(),
                    self.constant_policy,
                    self.control,
                )?;
                args_typed = coerced;
                binding
            };
            // OVER is authored here; aggregate presence alone is not an OVER ABI.
            let lifecycle = if aggregate_binding.is_some() {
                novarocks_functions::PureCallLifecycle::AggregateWindow
            } else {
                novarocks_functions::PureCallLifecycle::Window
            };
            self.function_catalog
                .admit_bound_lifecycle_observed(binding.resolved(), lifecycle, self.control)
                .map_err(|error| {
                    AnalyzeError::function_binding(error).at_type_mismatch(func.span)
                })?;
            let result = match &binding.selected.result_type {
                novarocks_functions::FunctionResultType::Scalar(result) => result.clone(),
                novarocks_functions::FunctionResultType::Relation(_) => {
                    unreachable!("window function cannot produce a relation")
                }
            };
            let ignore_nulls = matches!(func.null_treatment, Some(ast::NullTreatment::IgnoreNulls));
            return Ok(TypedExpr {
                kind: ExprKind::WindowCall {
                    name,
                    args: (args_typed).into(),
                    distinct: is_distinct,
                    binding,
                    function_order_by: (func_order_by).into(),
                    aggregate_binding,
                    partition_by: (partition_by).into(),
                    order_by: (order_by).into(),
                    window_frame,
                    ignore_nulls,
                },
                value_type: result,
            });
        }

        if matches!(name.as_str(), "group_concat" | "string_agg") {
            args_typed = args_typed
                .into_iter()
                .map(|arg| {
                    if matches!(
                        arg.value_type.data_type(),
                        DataType::Utf8 | DataType::LargeUtf8
                    ) {
                        Ok(arg)
                    } else {
                        let result = novarocks_type_contract::FunctionValueType::new(
                            DataType::Utf8,
                            arg.value_type.nullable(),
                        );
                        self.cast_with_trino_mode(
                            arg,
                            result,
                            novarocks_type_contract::TrinoCastMode::Explicit,
                            func.span,
                        )
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            arg_types = args_typed
                .iter()
                .map(|arg| arg.value_type.data_type().clone())
                .collect();
        }

        if name == "if" && !args_typed.is_empty() {
            let condition = self.require_boolean_predicate(
                args_typed.remove(0),
                "IF condition",
                original_arguments[0].span(),
            )?;
            args_typed.insert(0, condition);
            arg_types[0] = DataType::Boolean;
        }

        let mut bound_scalar = None;
        self.validate_percentile_arguments(&name, &args_typed, func.span)?;
        let mut bound_aggregate = None;
        if is_aggregate_function(self.function_catalog, &name) {
            bound_aggregate = Some(resolve_aggregate_function_call_with_order(
                self.function_catalog,
                &name,
                is_distinct,
                &args_typed,
                &func_order_by,
                func.span,
                self.sql_semantics.sql_mode().decimal_overflow_policy(),
                self.constant_policy,
                self.control,
                self.purpose,
            )?);
        } else {
            if matches!(name.as_str(), "variant_get" | "try_variant_get") {
                if !(2..=3).contains(&args_typed.len()) {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} expects 2 or 3 arguments, got {}", args_typed.len()),
                        func.span,
                    ));
                }
                if !matches!(
                    args_typed[1].kind,
                    ExprKind::Literal(LiteralValue::String(_))
                ) {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} path argument must be a string literal"),
                        func.span,
                    ));
                }
                if args_typed.len() == 3
                    && !matches!(
                        args_typed[2].kind,
                        ExprKind::Literal(LiteralValue::String(_))
                    )
                {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} type argument must be a string literal"),
                        func.span,
                    ));
                }
            }
            validate_scalar_function_call_typed(&name, &args_typed)
                .map_err(|message| AnalyzeError::type_mismatch(message, func.span))?;
            let route = scalar_typing_route(&name);
            let unknown = match route {
                ScalarTypingRoute::Shared => !self
                    .function_catalog
                    .recognizes_scalar_name(&name, &arg_types, self.control)
                    .map_err(AnalyzeError::function_resolution)?,
                ScalarTypingRoute::Legacy => scalar_signature_is_unknown(
                    self.function_catalog,
                    &name,
                    &arg_types,
                    self.control,
                )?,
            };
            // Legacy declarations are type-inspection facts, not installed selected bindings.
            // A custom contribution of the same name is resolved by its own catalog identity.
            if unknown {
                if matches!(
                    crate::functions::builtin_disposition(&name),
                    Some(
                        crate::functions::BuiltinDisposition::Unavailable
                            | crate::functions::BuiltinDisposition::LoweredOnly
                    )
                ) {
                    return Err(AnalyzeError::unsupported_expression(
                        format!(
                            "builtin function `{name}` has no admitted selected scalar implementation"
                        ),
                        func.span,
                    ));
                }
                if legacy_scalar_return_type_with_catalog(
                    self.function_catalog,
                    &name,
                    &arg_types,
                    self.control,
                )?
                .is_none()
                {
                    return Err(AnalyzeError::unknown_function(
                        format!("Unknown function: {name}"),
                        func.span,
                    ));
                }
            }
            let bound = match route {
                ScalarTypingRoute::Shared => {
                    let shape = if func.original.is_none() && name == "coalesce" {
                        NodeShape::Coalesce
                    } else {
                        NodeShape::CatalogCall {
                            name: &name,
                            expected_result: None,
                        }
                    };
                    self.type_scalar_call(shape, args_typed, func.span)?
                }
                ScalarTypingRoute::Legacy => bind_scalar_function_call_with_catalog(
                    self.function_catalog,
                    &name,
                    args_typed,
                    self.sql_semantics.sql_mode().decimal_overflow_policy(),
                    self.constant_policy,
                    self.control,
                )
                .map_err(|error| error.at_type_mismatch(func.span))?,
            };
            // This overload is also exposed by Trino's named array concat call.
            if bound.binding.function_id.as_str() == "builtin.scalar/concat/v1"
                && bound.binding.logical_argument_count > 127
            {
                return Err(AnalyzeError::too_many_arguments(
                    "Too many arguments for function call concat()",
                    func.span,
                ));
            }
            arg_types = bound
                .args
                .iter()
                .map(|arg| arg.value_type.data_type().clone())
                .collect();
            args_typed = bound.args;
            bound_scalar = Some(bound.binding);
        }

        if is_aggregate_function(self.function_catalog, &name) {
            // Aggregate function
            let mut signature =
                bound_aggregate.expect("catalog-classified aggregate must be resolved");
            {
                use novarocks_type_contract::{
                    AggregateStateInterpretation, AggregateStateOrderKey, CompileCheckpoints,
                    CompilePhase,
                };
                let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Validate)
                    .map_err(AnalyzeError::control)?;
                let mut keys = Vec::new();
                keys.try_reserve_exact(func_order_by.len()).map_err(|_| {
                    AnalyzeError::control(
                        novarocks_type_contract::CompileControlError::ResourceExhausted,
                    )
                })?;
                for key in &func_order_by {
                    keys.push(AggregateStateOrderKey {
                        ascending: key.asc,
                        nulls_first: key.nulls_first,
                    });
                    work.step().map_err(AnalyzeError::control)?;
                }
                work.flush().map_err(AnalyzeError::control)?;
                signature = signature.with_aggregate_state_source(AggregateStateInterpretation {
                    distinct: is_distinct,
                    order_keys: novarocks_type_contract::AggregateStateOrderKeys::from_pure_boxed(
                        keys.into_boxed_slice(),
                    ),
                });
                if matches!(name.as_str(), "group_concat" | "string_agg") {
                    signature = signature.with_group_concat_source(
                        crate::binding::GroupConcatSourceFacts {
                            legacy: self.sql_semantics.sql_mode().group_concat_legacy(),
                            max_len: self.sql_semantics.group_concat_max_len(),
                        },
                    );
                }
                work.finish().map_err(AnalyzeError::control)?;
            }
            let value_type = crate::functions::aggregate_result_type(&signature).clone();
            Ok(TypedExpr {
                kind: ExprKind::AggregateCall {
                    occurrence: crate::analysis::AggregateOccurrence::new(),
                    filter: None,
                    name,
                    args: (args_typed).into(),
                    distinct: is_distinct,
                    order_by: (func_order_by).into(),
                    resolved: signature,
                },
                value_type,
            })
        } else {
            // Scalar function
            let mut return_type = match bound_scalar.as_ref() {
                Some(binding) => match &binding.selected.result_type {
                    novarocks_functions::FunctionResultType::Scalar(result) => {
                        result.data_type().clone()
                    }
                    novarocks_functions::FunctionResultType::Relation(_) => {
                        unreachable!("scalar function cannot have a relation result")
                    }
                },
                None => infer_scalar_return_type_with_catalog(
                    self.function_catalog,
                    &name,
                    &arg_types,
                    self.control,
                )?,
            };
            // variant_get / try_variant_get: the path argument and optional
            // 3rd result-type argument are string literals (Spark-aligned).
            // Surface the type argument as the expression's static type;
            // reject non-literal arguments up front.
            if matches!(name.as_str(), "variant_get" | "try_variant_get") {
                if !(2..=3).contains(&args_typed.len()) {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} expects 2 or 3 arguments, got {}", args_typed.len()),
                        func.span,
                    ));
                }
                match &args_typed[1].kind {
                    ExprKind::Literal(LiteralValue::String(_)) => {}
                    _ => {
                        return Err(AnalyzeError::invalid_argument(
                            format!("{name} path argument must be a string literal"),
                            func.span,
                        ));
                    }
                }
                if args_typed.len() == 3 {
                    match &args_typed[2].kind {
                        ExprKind::Literal(LiteralValue::String(t)) => {
                            return_type =
                                novarocks_types::value::variant::variant_get_target_type(t)
                                    .map_err(|message| {
                                        AnalyzeError::invalid_literal_source(message, func.span)
                                    })?;
                        }
                        _ => {
                            return Err(AnalyzeError::invalid_argument(
                                format!("{name} type argument must be a string literal"),
                                func.span,
                            ));
                        }
                    }
                }
            }
            let binding = bound_scalar.expect("non-macro scalar function must be bound");
            let bound_result = match &binding.selected.result_type {
                novarocks_functions::FunctionResultType::Scalar(result) => result,
                novarocks_functions::FunctionResultType::Relation(_) => {
                    unreachable!("scalar function cannot have a relation result")
                }
            };
            assert_eq!(
                bound_result.data_type(),
                &return_type,
                "analyzer result type must match the exact scalar binding"
            );
            let value_type = bound_result.clone();
            Ok(TypedExpr {
                kind: ExprKind::FunctionCall {
                    volatility: self.function_catalog.volatility(&name),
                    name,
                    args: (args_typed).into(),
                    distinct: is_distinct,
                    binding,
                },
                value_type,
            })
        }
    }

    fn validate_percentile_arguments(
        &self,
        name: &str,
        args: &[TypedExpr],
        span: Span,
    ) -> Result<(), AnalyzeError> {
        match name {
            "percentile_approx" => {
                if let Some(expr) = args.first() {
                    validate_percentile_numeric_arg(name, 0, "value", expr)
                        .map_err(|message| AnalyzeError::invalid_argument(message, span))?;
                }
            }
            "percentile_approx_weighted" => {
                if let Some(expr) = args.first() {
                    validate_percentile_numeric_arg(name, 0, "value", expr)
                        .map_err(|message| AnalyzeError::invalid_argument(message, span))?;
                }
                if let Some(expr) = args.get(1) {
                    validate_percentile_numeric_arg(name, 1, "weight", expr)
                        .map_err(|message| AnalyzeError::invalid_argument(message, span))?;
                }
            }
            _ => {}
        }

        let (quantile_idx, compression_idx) = match name {
            "percentile_approx" => (1usize, 2usize),
            "percentile_approx_weighted" => (2usize, 3usize),
            _ => return Ok(()),
        };
        if let Some(expr) = args.get(quantile_idx) {
            self.validate_percentile_quantile_arg(name, quantile_idx, expr, span)?;
        }
        if let Some(expr) = args.get(compression_idx) {
            self.validate_percentile_compression_arg(name, expr, span)?;
        }
        Ok(())
    }

    fn validate_percentile_quantile_arg(
        &self,
        name: &str,
        quantile_idx: usize,
        expr: &TypedExpr,
        span: Span,
    ) -> Result<(), AnalyzeError> {
        match &expr.value_type.data_type() {
            DataType::List(item) => {
                if matches!(item.data_type(), DataType::Null) {
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "{name} requires the {} parameter (percentile) to be ARRAY<NUMERIC>, but got: ARRAY<NULL_TYPE>.",
                            ordinal_name(quantile_idx)
                        ),
                        span,
                    ));
                }
                if !is_numeric_type(item.data_type()) {
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "{name} requires the {} parameter (percentile) to be ARRAY<NUMERIC>, but got: ARRAY<{:?}>.",
                            ordinal_name(quantile_idx),
                            item.data_type()
                        ),
                        span,
                    ));
                }
                if let Some(items) = array_literal_items(expr) {
                    for (idx, item) in items.iter().enumerate() {
                        if let Some(value) = const_numeric_value(item) {
                            validate_percentile_value(name, value, Some(idx))
                                .map_err(|message| AnalyzeError::invalid_argument(message, span))?;
                        }
                    }
                }
            }
            data_type if is_numeric_type(data_type) => {
                if let Some(value) = const_numeric_value(expr) {
                    validate_percentile_value(name, value, None)
                        .map_err(|message| AnalyzeError::invalid_argument(message, span))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_percentile_compression_arg(
        &self,
        name: &str,
        expr: &TypedExpr,
        span: Span,
    ) -> Result<(), AnalyzeError> {
        if let Some(value) = const_numeric_value(expr)
            && value <= 0.0
        {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "Type check failed. compression parameter must be positive in {name}, but got: {}",
                    format_percentile_error_value(value)
                ),
                span,
            ));
        }
        Ok(())
    }

    /// Extract ORDER BY clauses from within function arguments (e.g. array_agg(x ORDER BY y)).
    fn extract_function_order_by(
        &self,
        func: &ast::FunctionCall,
        scope: &AnalyzerScope,
        args: &[TypedExpr],
    ) -> Result<Vec<SortItem>, AnalyzeError> {
        let func_name = print_object_name(&func.name).to_ascii_lowercase();
        let visible_args =
            if matches!(func_name.as_str(), "group_concat" | "string_agg") && !args.is_empty() {
                &args[..args.len() - 1]
            } else {
                args
            };
        if let Some(order_by) = &func.order_by {
            if order_by.items.is_empty() {
                return Err(AnalyzeError::invalid_argument(
                    "ORDER BY requires a sort key",
                    order_by.span,
                ));
            }
            let mut items = Vec::with_capacity(order_by.items.len());
            for ob in &order_by.items {
                let typed = if let Some(pos) = function_order_by_position(&ob.sort_key) {
                    let pos_index = usize::try_from(pos).ok();
                    if let Some(pos_index) = pos_index
                        && (1..=visible_args.len()).contains(&pos_index)
                    {
                        visible_args[pos_index - 1].clone()
                    } else if matches!(
                        func_name.as_str(),
                        "array_agg" | "array_agg_distinct" | "group_concat" | "string_agg"
                    ) {
                        let display_name = if func_name == "string_agg" {
                            "group_concat"
                        } else {
                            func_name.as_str()
                        };
                        return Err(AnalyzeError::invalid_argument(
                            format!(
                                "ORDER BY position {pos} is not in {display_name} output list."
                            ),
                            ob.span,
                        ));
                    } else {
                        self.analyze_expr(&ob.sort_key, scope)?
                    }
                } else {
                    self.analyze_expr(&ob.sort_key, scope)?
                };
                let (asc, nulls_first) = super::helpers::sort_item_flags(ob);
                items.push(SortItem {
                    expr: typed,
                    asc,
                    nulls_first,
                });
            }
            return Ok(items);
        }
        Ok(vec![])
    }

    /// Analyze a window specification (OVER clause).
    fn analyze_window_spec(
        &self,
        window: &ast::Window,
        scope: &AnalyzerScope,
    ) -> Result<WindowSpecAnalysis, AnalyzeError> {
        if let Some(reason) = unsupported_window_reason(window) {
            return Err(AnalyzeError::unsupported_expression(reason, window.span()));
        }
        let ast::Window::Specification(spec) = window else {
            return Err(AnalyzeError::unsupported_expression(
                "named window references are not supported",
                window.span(),
            ));
        };

        // PARTITION BY
        let mut partition_by = Vec::new();
        for expr in &spec.partition_by {
            partition_by.push(self.analyze_expr(expr, scope)?);
        }

        // ORDER BY
        let mut order_by = Vec::new();
        if let Some(order) = &spec.order_by
            && order.items.is_empty()
        {
            return Err(AnalyzeError::invalid_argument(
                "ORDER BY requires a sort key",
                order.span,
            ));
        }
        for ob in spec.order_by.iter().flat_map(|order| &order.items) {
            self.check_control()?;
            let typed = self.analyze_expr(&ob.sort_key, scope)?;
            let (asc, nulls_first) = super::helpers::sort_item_flags(ob);
            order_by.push(SortItem {
                expr: typed,
                asc,
                nulls_first,
            });
        }

        // Window frame
        let window_frame = if let Some(ref frame) = spec.window_frame {
            let frame_type = match frame.units {
                ast::WindowFrameUnits::Rows => WindowFrameType::Rows,
                ast::WindowFrameUnits::Range => WindowFrameType::Range,
                ast::WindowFrameUnits::Groups => {
                    return Err(AnalyzeError::unsupported_expression(
                        "GROUPS window frame is not supported",
                        frame.span,
                    ));
                }
            };
            let start = self.analyze_window_bound(&frame.start_bound)?;
            let end = match &frame.end_bound {
                Some(bound) => self.analyze_window_bound(bound)?,
                None => WindowBound::CurrentRow,
            };
            Some(WindowFrame {
                frame_type,
                start,
                end,
            })
        } else if !order_by.is_empty() {
            // SQL standard: when ORDER BY is present but no explicit window
            // frame is given, the implicit frame is
            //   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            // (i.e. running aggregate over peers). Without this default,
            // aggregate window functions like SUM/AVG/BITMAP_UNION over
            // ORDER BY would return the whole-partition value on every row.
            // Ranking/offset functions (row_number, rank, lead, lag, ...)
            // ignore the frame at execution time, so synthesizing a default
            // here has no effect on them.
            Some(WindowFrame {
                frame_type: WindowFrameType::Range,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            })
        } else {
            None
        };

        Ok((partition_by, order_by, window_frame))
    }

    fn analyze_window_bound(
        &self,
        bound: &ast::WindowFrameBound,
    ) -> Result<WindowBound, AnalyzeError> {
        match bound {
            ast::WindowFrameBound::CurrentRow(_) => Ok(WindowBound::CurrentRow),
            ast::WindowFrameBound::Preceding(None, _) => Ok(WindowBound::UnboundedPreceding),
            ast::WindowFrameBound::Preceding(Some(expr), _) => {
                let n = eval_const_i64(expr).map_err(|_| {
                    AnalyzeError::invalid_argument(
                        "window frame offset must be a constant integer",
                        bound.span(),
                    )
                })?;
                Ok(WindowBound::Preceding(n))
            }
            ast::WindowFrameBound::Following(None, _) => Ok(WindowBound::UnboundedFollowing),
            ast::WindowFrameBound::Following(Some(expr), _) => {
                let n = eval_const_i64(expr).map_err(|_| {
                    AnalyzeError::invalid_argument(
                        "window frame offset must be a constant integer",
                        bound.span(),
                    )
                })?;
                Ok(WindowBound::Following(n))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Aggregate detection
    // -----------------------------------------------------------------------

    /// Check if any projection item contains an aggregate function call.
    pub(super) fn select_has_aggregate_functions(&self, projection: &[ast::SelectItem]) -> bool {
        for item in projection {
            let expr = match item {
                ast::SelectItem::SingleColumn(column) => &column.expression,
                _ => continue,
            };
            if self.expr_contains_aggregate(expr) {
                return true;
            }
        }
        false
    }

    fn case_contains_aggregate(
        &self,
        clauses: &[ast::WhenClause],
        default: Option<&ast::Expr>,
    ) -> bool {
        clauses.iter().any(|clause| {
            matches!(&clause.match_value, ast::WhenMatch::Operand(value) if self.expr_contains_aggregate(value)) || self.expr_contains_aggregate(&clause.result)
        }) || default.is_some_and(|value| self.expr_contains_aggregate(value))
    }

    /// Recursively check if an expression contains an aggregate function call.
    /// Window functions (with OVER) are NOT counted as aggregates.
    #[recursive::recursive]
    pub(super) fn expr_contains_aggregate(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::JsonQuery(node) => {
                self.expr_contains_aggregate(&node.path_invocation.input_expression)
            }
            ast::Expr::GroupingOperation(node) => node
                .grouping_columns
                .iter()
                .any(|value| self.expr_contains_aggregate(value)),
            ast::Expr::Trim(node) => {
                self.expr_contains_aggregate(&node.trim_source)
                    || node
                        .trim_character
                        .as_ref()
                        .is_some_and(|value| self.expr_contains_aggregate(value))
            }
            ast::Expr::Extract(node) => self.expr_contains_aggregate(&node.expression),
            ast::Expr::If(node) => {
                self.expr_contains_aggregate(&node.condition)
                    || self.expr_contains_aggregate(&node.true_value)
                    || node
                        .false_value
                        .as_ref()
                        .is_some_and(|value| self.expr_contains_aggregate(value))
            }
            ast::Expr::NullIf(node) => {
                self.expr_contains_aggregate(&node.first)
                    || self.expr_contains_aggregate(&node.second)
            }
            ast::Expr::Coalesce(node) => node
                .operands
                .iter()
                .any(|value| self.expr_contains_aggregate(value)),
            ast::Expr::Try(node) => self.expr_contains_aggregate(&node.inner),
            ast::Expr::Format(node) => node
                .arguments
                .iter()
                .any(|value| self.expr_contains_aggregate(value)),
            ast::Expr::FunctionCall(f) => {
                // A function with OVER is a window function, not an aggregate
                if f.window.is_some() {
                    return false;
                }
                let name = print_object_name(&f.name).to_ascii_lowercase();
                if is_aggregate_function(self.function_catalog, &name)
                    || is_analyzer_aggregate_syntax(&name)
                {
                    return true;
                }
                f.arguments
                    .iter()
                    .any(|argument| self.expr_contains_aggregate(&argument.value))
                    || f.order_by
                        .iter()
                        .flat_map(|order| &order.items)
                        .any(|item| self.expr_contains_aggregate(&item.sort_key))
            }
            ast::Expr::Binary(binary) => {
                self.expr_contains_aggregate(&binary.left)
                    || self.expr_contains_aggregate(&binary.right)
            }
            ast::Expr::Logical(logical) => logical
                .terms
                .iter()
                .any(|term| self.expr_contains_aggregate(term)),
            ast::Expr::Unary(unary) => self.expr_contains_aggregate(&unary.expression),
            ast::Expr::Predicated(node) => {
                self.expr_contains_aggregate(&node.value)
                    || match &node.predicate {
                        ast::Predicate::Comparison(p) => self.expr_contains_aggregate(&p.right),
                        ast::Predicate::DistinctFrom(p) => self.expr_contains_aggregate(&p.right),
                        ast::Predicate::Between(p) => {
                            self.expr_contains_aggregate(&p.min)
                                || self.expr_contains_aggregate(&p.max)
                        }
                        ast::Predicate::Like(p) => {
                            self.expr_contains_aggregate(&p.pattern)
                                || p.escape
                                    .as_ref()
                                    .is_some_and(|e| self.expr_contains_aggregate(e))
                        }
                        ast::Predicate::In(p) => match &p.value_list {
                            ast::Expr::InList(list) => {
                                list.values.iter().any(|e| self.expr_contains_aggregate(e))
                            }
                            _ => false,
                        },
                        _ => false,
                    }
            }
            ast::Expr::InList(list) => list.values.iter().any(|e| self.expr_contains_aggregate(e)),
            ast::Expr::Cast(cast) => self.expr_contains_aggregate(&cast.expression),
            ast::Expr::Array(array) => array
                .values
                .iter()
                .any(|item| self.expr_contains_aggregate(item)),
            ast::Expr::Row(structure) => structure
                .fields
                .iter()
                .any(|field| self.expr_contains_aggregate(&field.expression)),
            ast::Expr::Subscript(node) => {
                self.expr_contains_aggregate(&node.base)
                    || self.expr_contains_aggregate(&node.index)
            }
            ast::Expr::Dereference(node) => self.expr_contains_aggregate(&node.base),
            ast::Expr::ArrayWildcardSubscript(node) => self.expr_contains_aggregate(&node.base),
            ast::Expr::SearchedCase(case) => {
                self.case_contains_aggregate(&case.when_clauses, case.default_value.as_ref())
            }
            ast::Expr::SimpleCase(case) => {
                self.expr_contains_aggregate(&case.operand)
                    || self.case_contains_aggregate(&case.when_clauses, case.default_value.as_ref())
            }
            _ => false,
        }
    }
}

fn function_order_by_position(expr: &ast::Expr) -> Option<i64> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Long { parsed_value, .. },
            ..
        }) => Some(*parsed_value),
        ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Minus => {
            function_order_by_position(&unary.expression)?.checked_neg()
        }
        _ => None,
    }
}

fn json_semantic_group_by_type_name(expr: &TypedExpr) -> Option<String> {
    match &expr.kind {
        ExprKind::FunctionCall { name, .. }
            if matches!(
                name.as_str(),
                "json_query"
                    | "json_extract"
                    | "get_json_object"
                    | "json_object"
                    | "json_array"
                    | "to_json"
                    | "parse_json"
            ) =>
        {
            Some("json".to_string())
        }
        ExprKind::FunctionCall { name, args, .. } if name == "__array_literal" => args
            .first()
            .and_then(json_semantic_group_by_type_name)
            .map(|inner| format!("array<{inner}>")),
        ExprKind::AggregateCall { name, args, .. } if name == "array_agg" => args
            .first()
            .and_then(json_semantic_group_by_type_name)
            .map(|inner| format!("array<{inner}>")),
        ExprKind::Nested(inner) => json_semantic_group_by_type_name(inner),
        _ => None,
    }
}

/// A carrier-compatible value is not authority to cross the JSON domain.
fn check_json_comparison_domains(
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
    operator: &str,
    span: novarocks_parser::Span,
) -> Result<(), AnalyzeError> {
    use novarocks_type_contract::ValueLogicalType;
    let left_json = left.logical_type() == ValueLogicalType::Json;
    let right_json = right.logical_type() == ValueLogicalType::Json;
    if left_json != right_json
        && left.data_type() != &DataType::Null
        && right.data_type() != &DataType::Null
    {
        let label = |value: &novarocks_type_contract::FunctionValueType| match value.logical_type()
        {
            ValueLogicalType::Json => "json".to_string(),
            ValueLogicalType::Variant => "variant".to_string(),
            _ => format!("{:?}", value.data_type()),
        };
        return Err(AnalyzeError::type_mismatch(
            format!(
                "Cannot apply operator: {} {operator} {}",
                label(left),
                label(right)
            ),
            span,
        ));
    }
    Ok(())
}

pub(super) fn is_json_in_subquery_operand(expr: &TypedExpr, scope: &AnalyzerScope) -> bool {
    matches!(
        scope.logical_type_of_expr(expr),
        Some(novarocks_types::schema::SqlType::Json)
    ) || matches!(
        json_semantic_group_by_type_name(expr).as_deref(),
        Some("json")
    )
}

fn is_bitmap_or_hll_type(sql_type: &novarocks_types::schema::SqlType) -> bool {
    matches!(
        sql_type,
        novarocks_types::schema::SqlType::Bitmap | novarocks_types::schema::SqlType::Hll
    )
}

fn in_predicate_type_error(left: &DataType, right: &DataType) -> String {
    if data_type_is_complex(left) && data_type_is_complex(right) {
        "of in predict are not compatible".to_string()
    } else {
        "in predicate type does not support comparison".to_string()
    }
}

fn data_type_is_complex(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_)
    )
}

fn is_non_groupable_map_constructor(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, .. } => name == "map",
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => {
            is_non_groupable_map_constructor(expr)
        }
        _ => false,
    }
}

/// The frozen numeric rule an operator answers by, or `None` when the
/// operator's result does not come from those rules at all.
const fn arithmetic_operator_of(op: &ast::BinaryOperator) -> ArithmeticOperator {
    match op {
        ast::BinaryOperator::Add => ArithmeticOperator::Add,
        ast::BinaryOperator::Subtract => ArithmeticOperator::Subtract,
        ast::BinaryOperator::Multiply => ArithmeticOperator::Multiply,
        ast::BinaryOperator::Divide => ArithmeticOperator::Divide,
        ast::BinaryOperator::Modulo => ArithmeticOperator::Modulo,
    }
}

/// Strict unary operators retain NULL's own domain and still evaluate its child.
fn is_strict_null_value_type(value: &novarocks_type_contract::FunctionValueType) -> bool {
    value.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && *value.data_type() == DataType::Null
        && value.nullable()
}

/// A boolean operand of an arithmetic operator becomes the integer it stands
/// for, at the width a boolean is stored at.
fn cast_boolean_operand_to_number(expr: TypedExpr) -> TypedExpr {
    if *expr.value_type.data_type() == DataType::Boolean {
        return cast_null_preserving_target_type(
            expr,
            &DataType::Int8,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        );
    }
    expr
}

impl super::AnalyzerContext<'_> {
    /// An untyped NULL borrows the other operand's admitted numeric domain.
    /// Nonliteral NULL expressions retain their evaluation through the exact
    /// conversion owner; only literal NULL can become a direct declaration.
    fn cast_operands_to_numbers(
        &self,
        left: TypedExpr,
        right: TypedExpr,
        operator: ArithmeticOperator,
        span: Span,
    ) -> Result<(TypedExpr, TypedExpr), AnalyzeError> {
        let decided = match (&left.value_type.data_type(), &right.value_type.data_type()) {
            (DataType::Null, DataType::Null) => {
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true)
            }
            (DataType::Null, _) => right.value_type.clone(),
            (_, DataType::Null) => left.value_type.clone(),
            _ => return Ok((left, right)),
        };
        if arithmetic_result_value_type_with_op(&decided, &decided, operator).is_none() {
            return Ok((left, right));
        }
        let lift = |expr: TypedExpr| {
            if *expr.value_type.data_type() == DataType::Null {
                self.cast_to_value_type(
                    expr,
                    super::helpers::with_nullability(decided.clone(), true),
                    span,
                )
            } else {
                Ok(expr)
            }
        };
        Ok((lift(left)?, lift(right)?))
    }
}

/// True when a value of this type can hold a NULL inside it.
///
/// Comparing two complex values compares their elements, so a NULL element
/// makes the whole comparison NULL even though neither operand is NULL --
/// `[1, NULL] = [1, 2]` answers NULL. The type is what says a NULL element is
/// possible, so it is what the comparison's nullability has to read.
fn compares_element_wise_with_nulls(data_type: &DataType) -> bool {
    let field_admits_null = |field: &arrow::datatypes::FieldRef| {
        field.is_nullable() || compares_element_wise_with_nulls(field.data_type())
    };
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => field_admits_null(field),
        DataType::Struct(fields) => fields.iter().any(field_admits_null),
        _ => false,
    }
}

// Literal classification chooses a type; actual parsing remains an ordinary CAST.
pub(super) fn cast_null_preserving_target_type(
    expr: TypedExpr,
    target: &DataType,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> TypedExpr {
    if *expr.value_type.data_type() == *target {
        return expr;
    }
    let nullable = expr.value_type.nullable()
        || novarocks_functions::signed_integer_cast_can_produce_null(
            expr.value_type.data_type(),
            target,
        );
    TypedExpr {
        kind: ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(expr),
            target: target.clone(),
            decimal_overflow_policy,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(target.clone(), nullable),
    }
}

pub(super) struct BoundScalarCall {
    pub(super) args: Vec<TypedExpr>,
    pub(super) binding: crate::binding::SqlFunctionBinding,
}

impl BoundScalarCall {
    pub(super) fn value_type(&self) -> &novarocks_type_contract::FunctionValueType {
        match &self.binding.selected.result_type {
            novarocks_functions::FunctionResultType::Scalar(result) => result,
            novarocks_functions::FunctionResultType::Relation(_) => {
                unreachable!("scalar function binding cannot have a relation result")
            }
        }
    }
}

fn resolve_scalar_binding(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: &[TypedExpr],
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::binding::SqlFunctionBinding, AnalyzeError> {
    crate::analysis::resolve_function_binding(
        function_catalog,
        name,
        args,
        decimal_overflow_policy,
        constant_policy,
        control,
    )
    .map_err(AnalyzeError::function_binding)
}

pub(super) fn resolve_scalar_binding_at(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: &[TypedExpr],
    span: Span,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::binding::SqlFunctionBinding, AnalyzeError> {
    resolve_scalar_binding(
        function_catalog,
        name,
        args,
        decimal_overflow_policy,
        constant_policy,
        control,
    )
    .map_err(|error| error.at_type_mismatch(span))
}

fn finish_selected_scalar_call(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    selected: &crate::binding::SqlFunctionBinding,
    args: &[TypedExpr],
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_functions::ResolvedFunctionBinding, AnalyzeError> {
    let arguments = args
        .iter()
        .map(|argument| crate::analysis::function_argument(argument, constant_policy, control))
        .collect::<Result<Vec<_>, _>>()
        .map_err(AnalyzeError::function_binding)?;
    catalog
        .finish_scalar_binding(
            &selected.function_id,
            &selected.selected.overload,
            novarocks_functions::FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: args.len(),
                expected_result_type: None,
            },
            control,
        )
        .map_err(AnalyzeError::function_binding)
}

pub(super) fn resolved_scalar_call_at(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: Vec<TypedExpr>,
    span: Span,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    let binding = resolve_scalar_binding_at(
        function_catalog,
        name,
        &args,
        span,
        decimal_overflow_policy,
        constant_policy,
        control,
    )?;
    if !selected_call_shape_matches_observed(
        binding.resolved(),
        args.len(),
        novarocks_functions::FunctionKind::Scalar,
        control,
    )? {
        return Err(AnalyzeError::type_mismatch(
            format!("function `{name}` selected an invalid call shape"),
            span,
        ));
    }
    let array_common = binding.function_id.as_str() == "builtin.scalar/__array_literal/v1";
    let args = args
        .into_iter()
        .zip(binding.selected.argument_types.iter())
        .map(|(argument, target)| match target {
            novarocks_functions::FunctionArgumentType::Value(_)
                if matches!(argument.kind, ExprKind::LambdaFunction { .. }) =>
            {
                Err(AnalyzeError::type_mismatch(
                    format!("function `{name}` selected a value target for a lambda argument"),
                    span,
                ))
            }
            novarocks_functions::FunctionArgumentType::Value(target) if array_common => {
                super::value_conversion::coerce_array_common_argument(
                    function_catalog,
                    argument,
                    target,
                    decimal_overflow_policy,
                    constant_policy,
                    control,
                )
            }
            novarocks_functions::FunctionArgumentType::Value(target) => {
                coerce_selected_function_argument(
                    function_catalog,
                    argument,
                    target,
                    decimal_overflow_policy,
                    constant_policy,
                    control,
                )
            }
            novarocks_functions::FunctionArgumentType::Lambda { .. }
                if matches!(argument.kind, ExprKind::LambdaFunction { .. }) =>
            {
                Ok(argument)
            }
            novarocks_functions::FunctionArgumentType::Lambda { .. } => {
                Err(AnalyzeError::type_mismatch(
                    format!("function `{name}` selected a lambda target for a value argument"),
                    span,
                ))
            }
        })
        .collect::<Result<Vec<_>, AnalyzeError>>()
        .map_err(|error| error.at_type_mismatch(span))?;
    let exact =
        finish_selected_scalar_call(function_catalog, &binding, &args, constant_policy, control)
            .map_err(|error| error.at_type_mismatch(span))?;
    if !rebound_call_matches_observed(binding.resolved(), &exact, &args, control)? {
        return Err(AnalyzeError::type_mismatch(
            format!("function `{name}` lost the exact selected argument declaration"),
            span,
        ));
    }
    let binding = crate::binding::SqlFunctionBinding::new(exact, decimal_overflow_policy);
    let result = match &binding.selected.result_type {
        novarocks_functions::FunctionResultType::Scalar(result) => result.clone(),
        novarocks_functions::FunctionResultType::Relation(_) => {
            unreachable!("scalar function cannot produce a relation")
        }
    };
    Ok(TypedExpr {
        kind: ExprKind::FunctionCall {
            volatility: binding.semantics.volatility,
            name: name.to_string(),
            binding,
            args: (args).into(),
            distinct: false,
        },
        value_type: result,
    })
}

pub(super) fn signed_int_literal_value(expr: &TypedExpr) -> Option<i64> {
    if let ExprKind::Literal(LiteralValue::Int(value)) = &expr.kind {
        return Some(*value);
    }
    let operand = crate::common::negation::typed_operand(expr)?;
    // Each original operation must fit its declared signed width before a
    // consuming parameter can widen the value.
    let value = signed_int_literal_value(operand)?.checked_neg()?;
    if expr.value_type.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || !matches!(
            expr.value_type.data_type(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    {
        return None;
    }
    checked_int_literal_for_target(value, expr.value_type.data_type()).ok()?;
    Some(value)
}

fn checked_int_literal_for_target(value: i64, target: &DataType) -> Result<(), String> {
    let fits = match target {
        DataType::Int8 => i8::try_from(value).is_ok(),
        DataType::Int16 => i16::try_from(value).is_ok(),
        DataType::Int32 => i32::try_from(value).is_ok(),
        DataType::Int64 => true,
        _ => return Ok(()),
    };
    if fits {
        Ok(())
    } else {
        let target_name = match target {
            DataType::Int8 => "tinyint",
            DataType::Int16 => "smallint",
            DataType::Int32 => "int",
            DataType::Int64 => "bigint",
            _ => unreachable!(),
        };
        Err(format!(
            "Cast argument {value} to {target_name} type failed"
        ))
    }
}

fn coerce_function_argument(
    expr: TypedExpr,
    target: &DataType,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<TypedExpr, String> {
    if *expr.value_type.data_type() == *target {
        return Ok(expr);
    }
    if let Some(value) = signed_int_literal_value(&expr)
        && matches!(
            target,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    {
        checked_int_literal_for_target(value, target)?;
        return Ok(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            value_type: novarocks_type_contract::FunctionValueType::new(
                target.clone(),
                expr.value_type.nullable(),
            ),
        });
    }
    if matches!(expr.value_type.data_type(), DataType::Null) {
        return Ok(TypedExpr {
            kind: ExprKind::Cast {
                cast_mode: None,
                expr: novarocks_parser::ast::AstBox::new(expr),
                target: target.clone(),
                decimal_overflow_policy,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(target.clone(), true),
        });
    }
    let nullable = expr.value_type.nullable()
        || (expr.value_type.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
            && novarocks_functions::carrier_cast_can_produce_null_with_policy(
                expr.value_type.data_type(),
                target,
                decimal_overflow_policy,
                false,
            ))
        || timestamp_unit_cast_can_return_null(expr.value_type.data_type(), target);
    Ok(TypedExpr {
        kind: ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(expr),
            target: target.clone(),
            decimal_overflow_policy,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(target.clone(), nullable),
    })
}

/// The selected owner, rather than the carrier, declares an argument domain.
fn coerce_selected_function_argument(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    expr: TypedExpr,
    target: &novarocks_type_contract::FunctionValueType,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    if expr.value_type.same_value_domain(target) {
        let mut coerced =
            coerce_function_argument(expr, target.data_type(), decimal_overflow_policy)
                .map_err(AnalyzeError::internal)?;
        coerced.value_type =
            super::helpers::with_nullability(target.clone(), coerced.value_type.nullable());
        return Ok(coerced);
    }
    let nullable = expr.value_type.nullable()
        || (expr.value_type.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
            && target.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
            && novarocks_functions::carrier_cast_can_produce_null_with_policy(
                expr.value_type.data_type(),
                target.data_type(),
                decimal_overflow_policy,
                false,
            ))
        || timestamp_unit_cast_can_return_null(expr.value_type.data_type(), target.data_type())
        || (expr.value_type.logical_type() == novarocks_type_contract::ValueLogicalType::LargeInt
            && target.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
            && matches!(
                target.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ));
    let value_type = super::helpers::with_nullability(target.clone(), nullable);
    if super::value_conversion::constant_is_null(&expr, constant_policy, control)?
        || !novarocks_type_contract::preserves_value_domains_observed(
            expr.value_type.value_type(),
            target.value_type(),
            |units| {
                control
                    .checkpoint(
                        novarocks_type_contract::CompilePhase::Validate,
                        u32::try_from(units).expect("one checked semantic node"),
                    )
                    .map_err(AnalyzeError::control)
            },
        )?
    {
        return super::value_conversion::convert_value_domain_with_catalog(
            function_catalog,
            expr,
            value_type,
            decimal_overflow_policy,
            constant_policy,
            control,
        );
    }
    // Ordinary carrier coercion retains its checked integer-literal
    // normalization and runtime narrowing policy. The selected owner still
    // supplies the complete target domain, including nested field facts.
    let mut coerced = coerce_function_argument(expr, target.data_type(), decimal_overflow_policy)
        .map_err(AnalyzeError::internal)?;
    coerced.value_type =
        super::helpers::with_nullability(target.clone(), coerced.value_type.nullable());
    Ok(coerced)
}

// These unit expansions use the existing safe Arrow cast, whose checked
// multiplication can produce NULL. Microsecond -> nanosecond is excluded:
// the installed cast owner reports overflow as an error for that pair.
fn timestamp_unit_cast_can_return_null(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Timestamp(TimeUnit::Second, _),
            DataType::Timestamp(
                TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
                _
            )
        ) | (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
        )
    )
}

#[cfg(test)]
fn bind_scalar_function_call(
    name: &str,
    args: Vec<TypedExpr>,
) -> Result<BoundScalarCall, AnalyzeError> {
    bind_scalar_function_call_with_catalog(
        crate::functions::builtin_sql_function_catalog(),
        name,
        args,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
}

// FIELD chooses one comparison family before ordinary argument binding. The
// frozen signature must contain this common type for every value and candidate.

fn selected_call_shape_matches_observed(
    binding: &novarocks_functions::ResolvedFunctionBinding,
    arity: usize,
    kind: novarocks_functions::FunctionKind,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, AnalyzeError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
    )
    .map_err(AnalyzeError::control)?;
    let matches = binding.kind == kind
        && binding.selected.argument_types.len() == arity
        && matches!(
            binding.selected.result_type,
            novarocks_functions::FunctionResultType::Scalar(_)
        );
    work.step().map_err(AnalyzeError::control)?;
    work.finish().map_err(AnalyzeError::control)?;
    Ok(matches)
}

/// Rebinding cannot change the call identity or the already-coerced argument
/// facts. The exact binding still owns its result and constant specialization.
fn rebound_call_matches_observed(
    first: &novarocks_functions::ResolvedFunctionBinding,
    exact: &novarocks_functions::ResolvedFunctionBinding,
    args: &[TypedExpr],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, AnalyzeError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
    )
    .map_err(AnalyzeError::control)?;
    let matches = (|| -> Result<bool, novarocks_functions::FunctionBindingError> {
        work.step()?;
        if exact.function_id != first.function_id
            || exact.selected.overload != first.selected.overload
            || exact.kind != first.kind
            || exact.selected.argument_types.len() != args.len()
            || !matches!(
                exact.selected.result_type,
                novarocks_functions::FunctionResultType::Scalar(_)
            )
        {
            return Ok(false);
        }
        for (target, arg) in exact.selected.argument_types.iter().zip(args.iter()) {
            work.step()?;
            match (target, &arg.kind) {
                (
                    novarocks_functions::FunctionArgumentType::Value(_),
                    ExprKind::LambdaFunction { .. },
                ) => return Ok(false),
                (novarocks_functions::FunctionArgumentType::Value(value), _) => {
                    if !value.exactly_equals_observed::<novarocks_functions::FunctionBindingError>(
                        &arg.value_type,
                        || work.step().map_err(Into::into),
                    )? {
                        return Ok(false);
                    }
                }
                (
                    novarocks_functions::FunctionArgumentType::Lambda {
                        parameter_types,
                        result_type,
                    },
                    ExprKind::LambdaFunction { params, body },
                ) => {
                    if parameter_types.len() != params.len() {
                        return Ok(false);
                    }
                    for (target, parameter) in parameter_types.iter().zip(params.iter()) {
                        work.step()?;
                        if !target
                            .exactly_equals_observed::<novarocks_functions::FunctionBindingError>(
                                &parameter.value_type,
                                || work.step().map_err(Into::into),
                            )?
                        {
                            return Ok(false);
                        }
                    }
                    if !result_type
                        .exactly_equals_observed::<novarocks_functions::FunctionBindingError>(
                            &body.value_type,
                            || work.step().map_err(Into::into),
                        )?
                    {
                        return Ok(false);
                    }
                }
                (novarocks_functions::FunctionArgumentType::Lambda { .. }, _) => return Ok(false),
            }
        }
        Ok(true)
    })();
    if let Err(error) = &matches
        && let Some(cause) = error.control_error()
    {
        return Err(AnalyzeError::control(cause));
    }
    work.finish().map_err(AnalyzeError::control)?;
    Ok(matches!(matches, Ok(true)))
}

/// Window-only selected arguments use the same carrier/domain conversion
/// author as ordinary calls, then retain the exact window selection.
pub(super) fn bind_window_function_call_with_catalog(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: Vec<TypedExpr>,
    span: Span,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(Vec<TypedExpr>, crate::binding::SqlFunctionBinding), AnalyzeError> {
    let resolve = |args: &[TypedExpr]| {
        let arguments = args
            .iter()
            .map(|argument| crate::analysis::function_argument(argument, constant_policy, control))
            .collect::<Result<Vec<_>, _>>()
            .map_err(AnalyzeError::function_binding)?;
        function_catalog
            .resolve_window_binding(name, &arguments, control)
            .map_err(|error| {
                if let novarocks_functions::FunctionBindingError::Control(error) = error {
                    return AnalyzeError::control(error);
                }
                if matches!(
                    error,
                    novarocks_functions::FunctionBindingError::UnavailableImplementation(_)
                ) {
                    return AnalyzeError::function_binding(error).at_type_mismatch(span);
                }
                AnalyzeError::type_mismatch(
                    format!(
                        "cannot bind window function `{name}` for argument types {:?}: {error}",
                        args.iter()
                            .map(|argument| argument.value_type.data_type().clone())
                            .collect::<Vec<_>>()
                    ),
                    span,
                )
            })
    };
    let binding = resolve(&args)?;
    if !selected_call_shape_matches_observed(
        &binding,
        args.len(),
        novarocks_functions::FunctionKind::Window,
        control,
    )? {
        return Err(AnalyzeError::type_mismatch(
            format!("window function `{name}` selected an invalid call shape"),
            span,
        ));
    }
    let args = args
        .into_iter()
        .zip(binding.selected.argument_types.iter())
        .map(|(arg, target)| match target {
            novarocks_functions::FunctionArgumentType::Value(value) => {
                coerce_selected_function_argument(
                    function_catalog,
                    arg,
                    value,
                    decimal_overflow_policy,
                    constant_policy,
                    control,
                )
            }
            novarocks_functions::FunctionArgumentType::Lambda { .. } => {
                Err(AnalyzeError::type_mismatch(
                    format!(
                        "window function `{name}` selected a lambda target for a value argument"
                    ),
                    span,
                ))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let exact = resolve(&args)?;
    if !rebound_call_matches_observed(&binding, &exact, &args, control)? {
        return Err(AnalyzeError::type_mismatch(
            format!("window function `{name}` changed selected identity after argument coercion"),
            span,
        ));
    }
    Ok((
        args,
        crate::binding::SqlFunctionBinding::new(exact, decimal_overflow_policy),
    ))
}

pub(super) fn bind_scalar_function_call_with_catalog(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: Vec<TypedExpr>,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<BoundScalarCall, AnalyzeError> {
    let arg_types = args
        .iter()
        .map(|arg| arg.value_type.data_type().clone())
        .collect::<Vec<_>>();

    match resolve_scalar_binding(
        function_catalog,
        name,
        &args,
        decimal_overflow_policy,
        constant_policy,
        control,
    ) {
        Ok(binding) => {
            if !selected_call_shape_matches_observed(
                binding.resolved(),
                args.len(),
                novarocks_functions::FunctionKind::Scalar,
                control,
            )? {
                return Err(AnalyzeError::internal(format!(
                    "function `{name}` selected an invalid call shape"
                )));
            }

            let args = args
                .into_iter()
                .zip(binding.selected.argument_types.iter())
                .map(|(arg, target)| match target {
                    novarocks_functions::FunctionArgumentType::Value(_)
                        if matches!(arg.kind, ExprKind::LambdaFunction { .. }) =>
                    {
                        Err(AnalyzeError::internal(format!(
                            "function `{name}` selected a value target for a lambda argument"
                        )))
                    }
                    novarocks_functions::FunctionArgumentType::Value(value) => {
                        coerce_selected_function_argument(
                            function_catalog,
                            arg,
                            value,
                            decimal_overflow_policy,
                            constant_policy,
                            control,
                        )
                    }
                    novarocks_functions::FunctionArgumentType::Lambda { .. }
                        if matches!(arg.kind, ExprKind::LambdaFunction { .. }) =>
                    {
                        Ok(arg)
                    }
                    novarocks_functions::FunctionArgumentType::Lambda { .. } => {
                        Err(AnalyzeError::internal(format!(
                            "function `{name}` selected a lambda target for a value argument"
                        )))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            validate_scalar_function_call_typed(name, &args).map_err(AnalyzeError::internal)?;
            let exact = finish_selected_scalar_call(
                function_catalog,
                &binding,
                &args,
                constant_policy,
                control,
            )?;
            if !rebound_call_matches_observed(binding.resolved(), &exact, &args, control)? {
                return Err(AnalyzeError::internal(format!(
                    "function `{name}` lost the exact selected argument declaration"
                )));
            }
            Ok(BoundScalarCall {
                args,
                binding: crate::binding::SqlFunctionBinding::new(exact, decimal_overflow_policy),
            })
        }
        Err(error)
            if error.control_error().is_some()
                || error.source_failure().is_some()
                || matches!(
                    error.kind(),
                    crate::analyze_error::AnalyzeErrorKind::UnavailableImplementation
                        | crate::analyze_error::AnalyzeErrorKind::UnknownFunction
                        | crate::analyze_error::AnalyzeErrorKind::AmbiguousFunctionCall
                ) =>
        {
            Err(error)
        }
        Err(error) => Err(AnalyzeError::internal(format!(
            "cannot bind scalar function `{name}` for argument types {arg_types:?}: {error}"
        ))),
    }
}

pub(super) fn resolve_aggregate_function_call(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: &[TypedExpr],
    span: Span,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::binding::SqlFunctionBinding, AnalyzeError> {
    resolve_aggregate_function_call_with_order(
        function_catalog,
        name,
        false,
        args,
        &[],
        span,
        decimal_overflow_policy,
        constant_policy,
        control,
        super::QueryAnalysisPurpose::Ordinary,
    )
}

fn resolve_aggregate_function_call_with_order(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    distinct: bool,
    args: &[TypedExpr],
    function_order_by: &[SortItem],
    span: Span,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
    purpose: super::QueryAnalysisPurpose,
) -> Result<crate::binding::SqlFunctionBinding, AnalyzeError> {
    let arg_types = args
        .iter()
        .map(|argument| argument.value_type.data_type().clone())
        .collect::<Vec<_>>();
    crate::functions::resolve_sql_aggregate_call(
        function_catalog,
        name,
        distinct,
        args,
        function_order_by,
        constant_policy,
        control,
    )
    .map(|binding| crate::binding::SqlFunctionBinding::new(binding, decimal_overflow_policy))
    .map_err(|error| {
        if let novarocks_functions::FunctionBindingError::Control(error) = error {
            return AnalyzeError::control(error);
        }
        if matches!(
            error,
            novarocks_functions::FunctionBindingError::UnavailableImplementation(_)
        ) {
            return AnalyzeError::function_binding(error).at_type_mismatch(span);
        }
        if purpose == super::QueryAnalysisPurpose::MaterializedViewDefinition
            && args.is_empty()
            && matches!(name, "avg" | "min")
            && matches!(
                error,
                novarocks_functions::FunctionBindingError::NoMatchingOverload
            )
        {
            return AnalyzeError::unknown_function(
                format!("Unexpected parameters () for function {name}"),
                span,
            );
        }
        let message = format!("cannot bind aggregate `{name}` for {arg_types:?}: {error}");
        if error == novarocks_functions::FunctionBindingError::NoMatchingOverload {
            AnalyzeError::unknown_function(message, span)
        } else {
            AnalyzeError::type_mismatch(message, span)
        }
    })
}

fn is_analyzer_aggregate_syntax(name: &str) -> bool {
    name == "every"
}

fn validate_group_concat_separator_argument(
    name: &str,
    arg_exprs: &[&ast::Expr],
    args: &[TypedExpr],
) -> Result<(), String> {
    if !matches!(name, "group_concat" | "string_agg") {
        return Ok(());
    }
    let Some(separator) = args.last() else {
        return Ok(());
    };
    if matches!(
        separator.value_type.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Null
    ) {
        return Ok(());
    }
    Err(format!(
        "group_concat requires separator to be of getType() STRING: {}.",
        group_concat_separator_signature(arg_exprs)
    ))
}

fn group_concat_separator_signature(arg_exprs: &[&ast::Expr]) -> String {
    let args = arg_exprs
        .iter()
        .map(|arg| expr_display_name(arg))
        .collect::<Vec<_>>()
        .join(", ");
    format!("group_concat({args})")
}

fn validate_group_concat_value_arguments(name: &str, args: &[TypedExpr]) -> Result<(), String> {
    if !matches!(name, "group_concat" | "string_agg") || args.is_empty() {
        return Ok(());
    }
    let value_args = &args[..args.len().saturating_sub(1)];
    if value_args.iter().all(is_supported_group_concat_value_type) {
        return Ok(());
    }
    Err(format!(
        "No matching function with signature: group_concat({}).",
        args.iter()
            .enumerate()
            .map(|(idx, arg)| group_concat_signature_type(arg, idx == args.len() - 1))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn is_supported_group_concat_value_type(expr: &TypedExpr) -> bool {
    !matches!(
        expr.value_type.data_type(),
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
    )
}

fn group_concat_signature_type(expr: &TypedExpr, separator: bool) -> String {
    if separator
        && matches!(
            expr.value_type.data_type(),
            DataType::Utf8 | DataType::LargeUtf8
        )
    {
        return "varchar".to_string();
    }
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } if name == "__array_literal" => {
            let item = infer_literal_signature_type(args).unwrap_or_else(|| {
                match &expr.value_type.data_type() {
                    DataType::List(item) => {
                        group_concat_data_type_signature(item.data_type(), false)
                    }
                    _ => group_concat_data_type_signature(expr.value_type.data_type(), false),
                }
            });
            format!("array<{item}>")
        }
        ExprKind::FunctionCall { name, args, .. } if name == "map" => {
            let (keys, values): (Vec<_>, Vec<_>) = args
                .chunks(2)
                .filter_map(|chunk| match chunk {
                    [key, value] => Some((key.clone(), value.clone())),
                    _ => None,
                })
                .unzip();
            let key_type = infer_literal_signature_type(&keys).unwrap_or_else(|| {
                map_entry_data_type(expr.value_type.data_type(), 0)
                    .map(|data_type| group_concat_data_type_signature(data_type, false))
                    .unwrap_or_else(|| "unknown".to_string())
            });
            let value_type = infer_literal_signature_type(&values).unwrap_or_else(|| {
                map_entry_data_type(expr.value_type.data_type(), 1)
                    .map(|data_type| group_concat_data_type_signature(data_type, true))
                    .unwrap_or_else(|| "unknown".to_string())
            });
            format!("map<{key_type},{value_type}>")
        }
        _ => group_concat_data_type_signature(expr.value_type.data_type(), false),
    }
}

fn infer_literal_signature_type(args: &[TypedExpr]) -> Option<String> {
    let mut rank = None;
    for arg in args {
        let ExprKind::Literal(LiteralValue::Int(value)) = arg.kind else {
            return None;
        };
        let current = integer_literal_signature_rank(value);
        rank = Some(rank.map_or(current, |existing: usize| existing.max(current)));
    }
    rank.map(integer_literal_signature_type)
}

fn integer_literal_signature_rank(value: i64) -> usize {
    if i32::try_from(value).is_ok() { 2 } else { 3 }
}

fn integer_literal_signature_type(rank: usize) -> String {
    match rank {
        0 => "tinyint(4)",
        1 => "smallint(6)",
        2 => "int(11)",
        _ => "bigint(20)",
    }
    .to_string()
}

fn map_entry_data_type(data_type: &DataType, index: usize) -> Option<&DataType> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    fields.get(index).map(|field| field.data_type())
}

fn group_concat_data_type_signature(data_type: &DataType, map_value_context: bool) -> String {
    match data_type {
        DataType::Null => "null_type".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint(4)".to_string(),
        DataType::Int16 => "smallint(6)".to_string(),
        DataType::Int32 => "int(11)".to_string(),
        DataType::Int64 => "bigint(20)".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => {
            if map_value_context {
                "varchar(20)".to_string()
            } else {
                "varchar".to_string()
            }
        }
        DataType::Binary | DataType::LargeBinary => "varbinary".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        DataType::List(item) => {
            format!(
                "array<{}>",
                group_concat_data_type_signature(item.data_type(), false)
            )
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return "map<unknown,unknown>".to_string();
            };
            if fields.len() != 2 {
                return "map<unknown,unknown>".to_string();
            }
            format!(
                "map<{},{}>",
                group_concat_data_type_signature(fields[0].data_type(), false),
                group_concat_data_type_signature(fields[1].data_type(), true)
            )
        }
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|field| group_concat_data_type_signature(field.data_type(), false))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => format!("{other:?}").to_lowercase(),
    }
}

fn ordinal_name(index: usize) -> &'static str {
    match index {
        0 => "first",
        1 => "second",
        2 => "third",
        3 => "fourth",
        _ => "unknown",
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

fn validate_percentile_numeric_arg(
    name: &str,
    index: usize,
    role: &str,
    expr: &TypedExpr,
) -> Result<(), String> {
    if is_numeric_type(expr.value_type.data_type()) {
        return Ok(());
    }
    Err(format!(
        "{name} requires the {} parameter ({role}) to be numeric type, but got: {}.",
        ordinal_name(index),
        percentile_argument_type_name(expr.value_type.data_type())
    ))
}

fn percentile_argument_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL_TYPE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar(65533)".to_string(),
        DataType::Date32 => "date".to_string(),
        DataType::Timestamp(_, _) => "datetime".to_string(),
        dt if novarocks_types::largeint::is_largeint_data_type(dt) => "largeint".to_string(),
        other => format!("{other:?}").to_lowercase(),
    }
}

fn strip_casts(expr: &TypedExpr) -> &TypedExpr {
    match &expr.kind {
        ExprKind::Cast { expr, .. } => strip_casts(expr),
        ExprKind::Nested(inner) => strip_casts(inner),
        _ => expr,
    }
}

/// StarRocks's LEAD/LAG default-argument check uses different rules per
/// expression shape:
///   - Plain literal (or NULL): always accepted; runtime coerces to the value
///     type.
///   - Bare column reference: type must be in the same broad family as the
///     value column (numeric/string/temporal).
///   - Constant expression (literals + arithmetic on literals): type must be
///     in the same *narrow* numeric family as the value column (INT-INT,
///     FLOAT-FLOAT, DECIMAL-DECIMAL...). This is stricter than the bare
///     ColumnRef rule because StarRocks doesn't constant-fold the expression
///     for type purposes.
///   - Anything else (function calls, column-bearing arithmetic): rejected
///     for INT/FLOAT/DECIMAL value columns; VARCHAR allows them through
///     because StarRocks stringifies arbitrary scalars.
fn is_lead_lag_default_arg_acceptable(default_arg: &TypedExpr, value_type: &DataType) -> bool {
    // VARCHAR is the lenient case: StarRocks stringifies arbitrary scalars,
    // so any expression is accepted as the default.
    if matches!(value_type, DataType::Utf8 | DataType::LargeUtf8) {
        return true;
    }
    let stripped = strip_casts(default_arg);
    // Plain (signed) literal — `1`, `-1`, `(1)`, etc. — is always accepted.
    // The native parser represents `-1` as a unary minus around `Literal(1)`,
    // so peel the unary minus before deciding.
    if is_signed_literal(stripped) {
        return true;
    }
    if matches!(stripped.kind, ExprKind::ColumnRef { .. }) {
        return lead_lag_family_compatible(value_type, default_arg.value_type.data_type());
    }
    if is_constant_default_expression(stripped) {
        return lead_lag_narrow_numeric_compatible(value_type, stripped.value_type.data_type());
    }
    false
}

fn is_signed_literal(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Literal(_) | ExprKind::Constant(_) => true,
        ExprKind::Nested(inner) => is_signed_literal(inner),
        ExprKind::UnaryOp { expr: inner, .. } => is_signed_literal(inner),
        ExprKind::Cast { expr: inner, .. } => is_signed_literal(inner),
        _ => false,
    }
}

fn is_constant_default_expression(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Literal(_) | ExprKind::Constant(_) => true,
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => {
            is_constant_default_expression(expr)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            is_constant_default_expression(left) && is_constant_default_expression(right)
        }
        ExprKind::FunctionCall { .. } => crate::common::binary_operator::typed_operands(expr)
            .is_some_and(|(_, [left, right])| {
                is_constant_default_expression(left) && is_constant_default_expression(right)
            }),
        ExprKind::UnaryOp { expr, .. } => is_constant_default_expression(expr),
        _ => false,
    }
}

fn lead_lag_family_compatible(value: &DataType, default: &DataType) -> bool {
    if value == default || matches!(default, DataType::Null) {
        return true;
    }
    if value.is_numeric() && default.is_numeric() {
        return true;
    }
    let is_str = |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8);
    if is_str(value) && is_str(default) {
        return true;
    }
    let is_temporal = |t: &DataType| {
        matches!(
            t,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        )
    };
    if is_temporal(value) && is_temporal(default) {
        return true;
    }
    false
}

fn lead_lag_narrow_numeric_compatible(value: &DataType, default: &DataType) -> bool {
    use DataType::*;
    if matches!(default, Null) {
        return true;
    }
    let is_int = |t: &DataType| matches!(t, Int8 | Int16 | Int32 | Int64);
    let is_float = |t: &DataType| matches!(t, Float32 | Float64);
    let is_decimal = |t: &DataType| matches!(t, Decimal128(_, _) | Decimal256(_, _));
    let is_str = |t: &DataType| matches!(t, Utf8 | LargeUtf8);
    if is_int(value) && is_int(default) {
        return true;
    }
    if is_float(value) && is_float(default) {
        return true;
    }
    if is_decimal(value) && is_decimal(default) {
        return true;
    }
    if is_str(value) && is_str(default) {
        return true;
    }
    value == default
}

fn lead_lag_type_display(t: &DataType) -> &'static str {
    use DataType::*;
    match t {
        Int8 => "TINYINT",
        Int16 => "SMALLINT",
        Int32 => "INT",
        Int64 => "BIGINT",
        Float32 => "FLOAT",
        Float64 => "DOUBLE",
        Decimal128(_, _) | Decimal256(_, _) => "DECIMAL",
        Utf8 | LargeUtf8 => "VARCHAR",
        Date32 | Date64 => "DATE",
        Timestamp(_, _) => "DATETIME",
        Boolean => "BOOLEAN",
        Binary | LargeBinary => "VARBINARY",
        _ => "UNKNOWN",
    }
}

fn array_literal_items(expr: &TypedExpr) -> Option<&[TypedExpr]> {
    match &strip_casts(expr).kind {
        ExprKind::FunctionCall { name, args, .. } if name == "__array_literal" => Some(args),
        _ => None,
    }
}

fn const_numeric_value(expr: &TypedExpr) -> Option<f64> {
    match &strip_casts(expr).kind {
        ExprKind::Literal(LiteralValue::Int(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::LargeInt(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::Float(v)) => Some(*v),
        ExprKind::Literal(LiteralValue::Decimal(v)) => v.parse::<f64>().ok(),
        _ => crate::common::negation::typed_operand(strip_casts(expr))
            .and_then(const_numeric_value)
            .map(|value| -value),
    }
}

fn validate_percentile_value(
    name: &str,
    value: f64,
    array_index: Option<usize>,
) -> Result<(), String> {
    if (0.0..=1.0).contains(&value) {
        return Ok(());
    }
    match array_index {
        Some(idx) => Err(format!(
            "Type check failed. percentile array element[{idx}] must be between 0 and 1 in {name}, but got: {}",
            format_percentile_error_value(value)
        )),
        None => Err(format!(
            "Type check failed. percentile parameter must be between 0 and 1 in {name}, but got: {}",
            format_percentile_error_value(value)
        )),
    }
}

fn format_percentile_error_value(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn arrow_type_to_starrocks_name(dt: &DataType) -> String {
    typeof_name(dt, None)
}

fn typeof_name(dt: &DataType, declared: Option<&ValueTypeRef>) -> String {
    fn contains_temporal(ty: &ValueTypeRef) -> bool {
        ty.temporal().is_some()
            || (0..ty.child_count())
                .any(|ordinal| ty.child_type_at(ordinal).is_some_and(contains_temporal))
    }
    render_typeof_name(dt, declared, declared.is_some_and(contains_temporal))
}

fn typeof_cast_name(
    syntax: &ast::TypeName,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<String>, AnalyzeError> {
    struct ContainsTemporal(bool);

    impl ast::Visit for ContainsTemporal {
        fn visit_type_name(&mut self, name: &ast::TypeName) {
            if matches!(
                name.kind,
                ast::TypeKind::DateTime(_) | ast::TypeKind::Interval(_)
            ) {
                self.0 = true;
            } else if !self.0 {
                ast::walk_type_name(self, name);
            }
        }
    }

    let mut temporal = ContainsTemporal(false);
    ast::Visit::visit_type_name(&mut temporal, syntax);
    if !temporal.0 {
        return Ok(sql_type_starrocks_name(syntax));
    }
    let declared = super::helpers::sql_type_to_value_type(syntax, false, control)?;
    Ok(Some(render_typeof_name(
        declared.data_type(),
        Some(declared.value_type()),
        true,
    )))
}

fn render_typeof_name(dt: &DataType, declared: Option<&ValueTypeRef>, exact: bool) -> String {
    if let Some(temporal) = declared.and_then(ValueTypeRef::temporal) {
        return temporal_typeof_name(temporal);
    }
    let child_name = |ordinal, field: &DataType| {
        render_typeof_name(
            field,
            declared.and_then(|ty| ty.child_type_at(ordinal)),
            exact,
        )
    };
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 if exact => "integer".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::UInt8 => "tinyint unsigned".to_string(),
        DataType::UInt16 => "smallint unsigned".to_string(),
        DataType::UInt32 => "int unsigned".to_string(),
        DataType::UInt64 => "bigint unsigned".to_string(),
        DataType::Float32 if exact => "real".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) if exact => {
            format!("decimal({p}, {s})")
        }
        DataType::Decimal128(p, s) => format!("decimal128({}, {})", p, s),
        DataType::FixedSizeBinary(w) if *w == novarocks_types::largeint::LARGEINT_BYTE_WIDTH => {
            "largeint".to_string()
        }
        DataType::Utf8 | DataType::LargeUtf8 => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            "varbinary".to_string()
        }
        DataType::Date32 => "date".to_string(),
        DataType::Timestamp(_, _) => "datetime".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        DataType::List(field) => {
            let child = child_name(0, field.data_type());
            if exact {
                format!("array({child})")
            } else {
                format!("array<{child}>")
            }
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                let key = child_name(0, fields[0].data_type());
                let value = child_name(1, fields[1].data_type());
                if exact {
                    format!("map({key}, {value})")
                } else {
                    format!("map<{key},{value}>")
                }
            }
            _ => "map".to_string(),
        },
        DataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .enumerate()
                .map(|(ordinal, field)| {
                    let ty = child_name(ordinal, field.data_type());
                    if exact {
                        let name = field.name().replace('"', "\"\"");
                        format!("\"{name}\" {ty}")
                    } else {
                        format!("{} {ty}", field.name())
                    }
                })
                .collect();
            let fields = parts.join(", ");
            if exact {
                format!("row({fields})")
            } else {
                format!("struct<{fields}>")
            }
        }
        DataType::Null => "null".to_string(),
        other => format!("{:?}", other).to_lowercase(),
    }
}

fn temporal_typeof_name(temporal: TemporalType) -> String {
    let precision = temporal.precision().digits();
    match temporal {
        TemporalType::Time(_) => format!("time({precision})"),
        TemporalType::TimeWithTimeZone(_) => format!("time({precision}) with time zone"),
        TemporalType::Timestamp(_) => format!("timestamp({precision})"),
        TemporalType::TimestampWithTimeZone(_) => {
            format!("timestamp({precision}) with time zone")
        }
        TemporalType::Interval(qualifier) => {
            use novarocks_type_contract::IntervalField;
            let field = |kind| match kind {
                IntervalField::Year => "year",
                IntervalField::Month => "month",
                IntervalField::Day => "day",
                IntervalField::Hour => "hour",
                IntervalField::Minute => "minute",
                IntervalField::Second => "second",
            };
            let start = qualifier.start();
            let end = qualifier.end();
            let leading = qualifier.leading_precision();
            let fractional = qualifier.fractional_precision().digits();
            let start_name = field(start);
            let end_name = field(end);
            match (start, end) {
                (IntervalField::Second, IntervalField::Second) => {
                    format!("interval second({leading}, {fractional})")
                }
                (start, end) if start == end => format!("interval {start_name}({leading})"),
                (_, IntervalField::Second) => {
                    format!("interval {start_name}({leading}) to second({fractional})")
                }
                _ => format!("interval {start_name}({leading}) to {end_name}"),
            }
        }
    }
}

fn starrocks_error_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Int8 => "tinyint(4)".to_string(),
        DataType::Int16 => "smallint(6)".to_string(),
        DataType::Int32 => "int(11)".to_string(),
        DataType::Int64 => "bigint(20)".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar(65533)".to_string(),
        DataType::List(field) => {
            format!("array<{}>", starrocks_error_type_name(field.data_type()))
        }
        other => arrow_type_to_starrocks_name(other),
    }
}

/// Some StarRocks logical types (BITMAP / HLL / JSON) are represented as
/// BINARY or VARCHAR at the Arrow level but should be reported as their
/// own type name by `typeof()`. Detect the logical type by inspecting the
/// producing function in the AST. Also handles the bare `NULL` literal.
fn sql_expr_logical_type_name(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::JsonObject(node) if super::is_supported_json_object_profile(node) => {
            Some("json".to_string())
        }
        ast::Expr::JsonArray(node) if super::is_supported_json_array_profile(node) => {
            Some("json".to_string())
        }

        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        }) => Some("null_type".to_string()),
        ast::Expr::Literal(_) => None,
        ast::Expr::FunctionCall(function) => {
            let name = print_object_name(&function.name).to_ascii_lowercase();
            let name = name.split('.').next_back().unwrap_or(name.as_str());
            match name {
                "parse_json" | "json_object" | "json_array" | "to_json" => Some("json".to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Render an SQL data type using StarRocks' canonical spelling, used by
/// `typeof(CAST(x AS T))` so the result preserves the user-supplied SQL
/// width (CHAR vs VARCHAR, DECIMAL128(p, s), etc.) that is otherwise lost
/// once the cast target lowers to Arrow.
fn sql_type_starrocks_name(sql_type: &ast::TypeName) -> Option<String> {
    let name = match &sql_type.kind {
        ast::TypeKind::Generic(value) => value.name.value.to_ascii_lowercase(),
        _ => return None,
    };
    let canonical = match name.as_str() {
        "tinyint" | "smallint" | "int" | "integer" | "bigint" | "float" | "double" | "boolean"
        | "char" | "date" | "time" | "largeint" | "bitmap" | "hll" => name,
        "varchar" | "character" | "string" | "text" => "varchar".to_string(),
        "json" | "jsonb" => "json".to_string(),
        "varbinary" | "binary" => "varbinary".to_string(),
        "datetime" | "timestamp" => "datetime".to_string(),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let rendered = type_for_diagnostic(sql_type).to_ascii_lowercase();
            let suffix = rendered
                .find('(')
                .map(|index| &rendered[index..])
                .unwrap_or("");
            return Some(format!("decimal128{suffix}"));
        }
        _ => return None,
    };
    Some(canonical)
}

/// Render a short, user-facing display name for an expression operand. Used by
/// the BITMAP/HLL fail-fast checks (IN list, BETWEEN, IN subquery, comparison)
/// so the rejection message can say *which* operand had the offending type. We
/// only care about column refs here — the same convention `logical_type_of_expr`
/// uses — but fall back to a placeholder so the format string never panics.
fn column_name_of_expr(expr: &TypedExpr) -> String {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        },
        _ => "<expr>".to_string(),
    }
}

#[recursive::recursive]
#[cfg(test)]
fn typed_expr_contains_nondeterministic_call(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    expr: &TypedExpr,
) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            function_catalog.volatility(name).is_volatile()
                || args
                    .iter()
                    .any(|arg| typed_expr_contains_nondeterministic_call(function_catalog, arg))
        }
        ExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_nondeterministic_call(function_catalog, left)
                || typed_expr_contains_nondeterministic_call(function_catalog, right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => {
            typed_expr_contains_nondeterministic_call(function_catalog, expr)
        }
        ExprKind::AggregateCall { args, .. } | ExprKind::WindowCall { args, .. } => args
            .iter()
            .any(|arg| typed_expr_contains_nondeterministic_call(function_catalog, arg)),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand.as_ref().is_some_and(|expr| {
                typed_expr_contains_nondeterministic_call(function_catalog, expr)
            }) || when_then.iter().any(|(when, then)| {
                typed_expr_contains_nondeterministic_call(function_catalog, when)
                    || typed_expr_contains_nondeterministic_call(function_catalog, then)
            }) || else_expr.as_ref().is_some_and(|expr| {
                typed_expr_contains_nondeterministic_call(function_catalog, expr)
            })
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            typed_expr_contains_nondeterministic_call(function_catalog, expr)
                || typed_expr_contains_nondeterministic_call(function_catalog, low)
                || typed_expr_contains_nondeterministic_call(function_catalog, high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            typed_expr_contains_nondeterministic_call(function_catalog, expr)
                || typed_expr_contains_nondeterministic_call(function_catalog, pattern)
        }
        ExprKind::InList { expr, list, .. } => {
            typed_expr_contains_nondeterministic_call(function_catalog, expr)
                || list
                    .iter()
                    .any(|item| typed_expr_contains_nondeterministic_call(function_catalog, item))
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            typed_expr_contains_nondeterministic_call(function_catalog, body)
        }
        _ => false,
    }
}

pub(super) fn incompatible_complex_compare(left: &DataType, right: &DataType) -> Option<String> {
    fn is_complex(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_)
        )
    }
    // NULL on either side is always comparable to anything — `x = NULL`
    // and `x = some_complex_literal` containing NULLs are both valid SQL.
    if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
        return None;
    }
    // Defer to the existing coercion path when *both* sides are scalar.
    // When even one side is complex we want to validate the shape — the
    // mismatch surfaces below via `outer_kind` and the recursive check.
    if !is_complex(left) && !is_complex(right) {
        return None;
    }
    // Outer-container kind mismatch (e.g. ARRAY = MAP) is always an error.
    let outer_kind = |dt: &DataType| match dt {
        DataType::List(_) | DataType::LargeList(_) => "ARRAY",
        DataType::Map(_, _) => "MAP",
        DataType::Struct(_) => "STRUCT",
        _ => "",
    };
    if outer_kind(left) != outer_kind(right) {
        return Some(format!("{:?} and {:?}", left, right));
    }
    // Same outer kind — recurse into the element / entry / field types and
    // report incompatibility when the inner shapes diverge in a way that
    // can't be widened. We use `is_complex && outer_kind_mismatch` as the
    // disqualifying signal; pairs of incompatible scalars (e.g. Int32 vs
    // Utf8) are left to the existing CAST kernel.
    match (left, right) {
        (DataType::List(l), DataType::List(r))
        | (DataType::LargeList(l), DataType::LargeList(r)) => {
            incompatible_complex_compare(l.data_type(), r.data_type())
                .map(|inner| format!("ARRAY of incompatible elements ({inner})"))
        }
        (DataType::Map(l, _), DataType::Map(r, _)) => {
            let (DataType::Struct(lf), DataType::Struct(rf)) = (l.data_type(), r.data_type())
            else {
                return None;
            };
            if lf.len() != 2 || rf.len() != 2 {
                return None;
            }
            incompatible_complex_compare(lf[0].data_type(), rf[0].data_type())
                .or_else(|| incompatible_complex_compare(lf[1].data_type(), rf[1].data_type()))
                .map(|inner| format!("MAP entries with incompatible shape ({inner})"))
        }
        (DataType::Struct(lf), DataType::Struct(rf)) => {
            if lf.len() != rf.len() {
                return Some(format!(
                    "STRUCT with different field counts ({} vs {})",
                    lf.len(),
                    rf.len()
                ));
            }
            for (a, b) in lf.iter().zip(rf.iter()) {
                if let Some(inner) = incompatible_complex_compare(a.data_type(), b.data_type()) {
                    return Some(format!("STRUCT field `{}` ({inner})", a.name()));
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::analyze;
    use super::{BinOp, TypedExpr, string_literal_expr};
    use crate::analysis::{ExprKind, LiteralValue, QueryBody};
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::catalog::PlannerTableProvider;
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
    };
    use arrow::datatypes::DataType;
    use novarocks_parser::ast;
    use std::num::{NonZeroU32, NonZeroU64};

    struct EmptyCatalog;

    impl PlannerTableProvider for EmptyCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            _database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            let _ = catalog;
            Err(format!("table not found: {table}"))
        }
    }

    struct BigintOffsetCatalog;

    impl PlannerTableProvider for BigintOffsetCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            if table != "offsets" {
                return Err(format!("table not found: {table}"));
            }
            let planner = crate::planner::table::TableDef {
                name: table.to_string(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "offset".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::Sql(SqlScanSource::new(
                    SqlTableBindingId::new(
                        SqlTableBindingScopeId::new(NonZeroU64::new(43).expect("non-zero scope")),
                        NonZeroU32::new(1).expect("non-zero binding ordinal"),
                    ),
                    SqlTableIdentity {
                        catalog: catalog.unwrap_or("default_catalog").to_string(),
                        namespace: database.to_string(),
                        table: table.to_string(),
                    },
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            };
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    fn analyze_projection_expr(sql: &str) -> Result<crate::analysis::TypedExpr, String> {
        analyze_projection_expr_with_function_catalog(
            sql,
            crate::functions::builtin_sql_function_catalog(),
        )
    }

    fn logical_context(
        control: &crate::compiler::SqlCompileControl,
    ) -> super::super::AnalyzerContext<'_> {
        super::super::AnalyzerContext {
            purpose: super::super::QueryAnalysisPurpose::Ordinary,
            grouping_scope: Default::default(),
            view_stack: Default::default(),
            control,
            constant_policy: crate::constant::test_constant_policy(),
            materialized_values: None,
            table_sequence: None,
            catalog: &EmptyCatalog,
            current_database: "default",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            sql_semantics: Default::default(),
            factory: std::rc::Rc::new(std::cell::RefCell::new(
                crate::column_id::ColumnRefFactory::new(),
            )),
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: Default::default(),
            collected_subqueries: Default::default(),
            cte_registry: std::cell::RefCell::new(crate::analysis::cte::CTERegistry::new()),
        }
    }

    #[test]
    fn dedicated_trim_profiles_keep_original_binding_and_refuse_explicit_characters() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let ctx = logical_context(&control);
        let scope = ctx.new_scope();
        for (specification, name) in [
            (ast::TrimSpecification::Both, "trim"),
            (ast::TrimSpecification::Leading, "ltrim"),
            (ast::TrimSpecification::Trailing, "rtrim"),
        ] {
            let node = ast::Trim {
                specification,
                trim_source: string_literal_expr("  x  ".into(), novarocks_parser::SYNTHETIC),
                trim_character: None,
                span: novarocks_parser::SYNTHETIC,
            };
            let actual = ctx
                .analyze_expr(&ast::Expr::Trim(ast::AstBox::new(node.clone())), &scope)
                .unwrap();
            let expected = analyze_projection_expr(&format!("SELECT {name}('  x  ')")).unwrap();
            assert_eq!(actual.value_type, expected.value_type);
            let (
                ExprKind::FunctionCall {
                    binding: actual, ..
                },
                ExprKind::FunctionCall {
                    binding: expected, ..
                },
            ) = (&actual.kind, &expected.kind)
            else {
                panic!("real selected scalar binding")
            };
            assert_eq!(actual.selected.overload, expected.selected.overload);
            let mut node = node;
            node.trim_source =
                ast::Expr::Identifier(novarocks_parser::build::ident("must_not_resolve", false));
            node.trim_character = Some(string_literal_expr("x".into(), node.span));
            assert_eq!(
                ctx.analyze_expr(&ast::Expr::Trim(ast::AstBox::new(node)), &scope)
                    .unwrap_err()
                    .kind(),
                crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
            );
        }
        let leaf = ast::Expr::CurrentTimestamp(ast::CurrentTimestamp {
            precision: Some(-17),
            span: novarocks_parser::SYNTHETIC,
        });
        assert_eq!(
            ctx.analyze_expr(&leaf, &scope).unwrap_err().kind(),
            crate::analyze_error::AnalyzeErrorKind::InvalidLiteral
        );
        // The original ordinary CAST declaration shortcut remains opaque.
        assert!(
            analyze_projection_expr("SELECT typeof(CAST(CURRENT_TIMESTAMP AS INTEGER))").is_ok()
        );
        let query =
            &mut novarocks_parser::parser::parse_query("SELECT typeof(CAST(NULL AS INTEGER))")
                .unwrap();
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(column) = &mut select.projection[0] else {
            panic!("projection")
        };
        let ast::Expr::FunctionCall(call) = &mut column.expression else {
            panic!("typeof")
        };
        let ast::Expr::Cast(cast) = &mut call.arguments[0].value else {
            panic!("declaration CAST")
        };
        cast.expression = leaf;
        let declared = ctx.analyze_expr(&column.expression, &scope).unwrap();
        assert_eq!(
            declared.value_type,
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false)
        );
        assert!(
            matches!(declared.kind, ExprKind::Literal(LiteralValue::String(name)) if name == "integer")
        );
    }

    #[test]
    fn logical_terms_balance_only_typed_ir_and_keep_source_order_and_nullable_bounds() {
        fn leaves<'a>(expression: &'a TypedExpr, op: BinOp, out: &mut Vec<&'a TypedExpr>) -> usize {
            if let ExprKind::BinaryOp {
                left,
                right,
                op: actual,
                ..
            } = &expression.kind
                && *actual == op
            {
                return 1 + leaves(left, op, out).max(leaves(right, op, out));
            }
            out.push(expression);
            0
        }
        for (keyword, op) in [("AND", BinOp::And), ("OR", BinOp::Or)] {
            let sql = format!(
                "SELECT 1 = 10 {keyword} 2 = 20 {keyword} 3 = 30 {keyword} 4 = 40 {keyword} 5 = 50"
            );
            let actual = analyze_projection_expr(&sql).unwrap();
            assert_eq!(actual.value_type.data_type(), &DataType::Boolean);
            assert!(!actual.value_type.nullable());
            let mut operands = Vec::new();
            assert_eq!(leaves(&actual, op, &mut operands), 3);
            let original_order: Vec<_> = operands
                .iter()
                .map(|operand| {
                    let Some((BinOp::Eq, [left, right])) =
                        crate::common::binary_operator::typed_comparison(operand)
                    else {
                        panic!("comparison term")
                    };
                    let (
                        ExprKind::Literal(LiteralValue::Int(left)),
                        ExprKind::Literal(LiteralValue::Int(right)),
                    ) = (&left.kind, &right.kind)
                    else {
                        panic!("original integer arguments")
                    };
                    (*left, *right)
                })
                .collect();
            assert_eq!(
                original_order,
                vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
            );
            let nullable =
                analyze_projection_expr(&format!("SELECT TRUE {keyword} NULL {keyword} FALSE"))
                    .unwrap();
            assert!(nullable.value_type.nullable());
            let mut operands = Vec::new();
            leaves(&nullable, op, &mut operands);
            assert!(matches!(
                operands[0].kind,
                ExprKind::Literal(LiteralValue::Bool(true))
            ));
            assert!(matches!(
                operands[1].kind,
                ExprKind::Literal(LiteralValue::Null)
            ));
            assert!(matches!(
                operands[2].kind,
                ExprKind::Literal(LiteralValue::Bool(false))
            ));
        }
        let query = &novarocks_parser::parser::parse_query(
            "SELECT TRUE AND (FALSE OR NULL) AND COALESCE(FALSE, TRUE)",
        )
        .unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("logical projection")
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let expression @ ast::Expr::Logical(logical) = &projection_column.expression else {
            panic!("logical projection")
        };
        let control = crate::compiler::SqlCompileControl::unbounded();
        let ctx = logical_context(&control);
        let scope = ctx.new_scope();
        let expected: Vec<_> = logical
            .terms
            .iter()
            .map(|term| ctx.analyze_expr(term, &scope).unwrap())
            .collect();
        let mixed = ctx.analyze_expr(expression, &scope).unwrap();
        let mut terms = Vec::new();
        leaves(&mixed, BinOp::And, &mut terms);
        assert_eq!(terms.len(), expected.len());
        for (actual, expected) in terms.iter().zip(&expected) {
            assert_eq!(actual.value_type, expected.value_type);
            assert!(
                crate::analysis::expr_identity::typed_expr_semantically_eq(
                    actual, expected, &control
                )
                .unwrap(),
                "the original term keeps its complete typed subtree and canonical bindings"
            );
        }
        let inner = terms[1];
        assert!(matches!(logical.terms[1], ast::Expr::Logical(_)));
        assert_eq!(
            terms[1].value_type,
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true)
        );
        assert_eq!(inner.value_type, terms[1].value_type);
        let ExprKind::BinaryOp {
            left,
            right,
            op: BinOp::Or,
            decimal_overflow_policy,
        } = &inner.kind
        else {
            panic!("the nested disjunction remains one source term")
        };
        assert_eq!(
            *decimal_overflow_policy,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull
        );
        assert!(matches!(
            left.kind,
            ExprKind::Literal(LiteralValue::Bool(false))
        ));
        assert!(matches!(right.kind, ExprKind::Literal(LiteralValue::Null)));
        let ExprKind::FunctionCall {
            binding: actual_binding,
            ..
        } = &terms[2].kind
        else {
            panic!("original scalar call")
        };
        let ExprKind::FunctionCall {
            binding: expected_binding,
            ..
        } = &expected[2].kind
        else {
            panic!("independent scalar call")
        };
        assert_eq!(actual_binding, expected_binding);
    }

    #[test]
    fn final_literal_nodes_keep_minimal_numeric_types_and_original_typed_string_results() {
        for (sql, expected) in [
            ("SELECT 1", DataType::Int32),
            ("SELECT 2147483648", DataType::Int64),
            ("SELECT 1.20", DataType::Decimal128(3, 2)),
            ("SELECT DECIMAL '001.200'", DataType::Decimal128(4, 3)),
            ("SELECT 1e0", DataType::Float64),
            ("SELECT X'00ff'", DataType::Binary),
            ("SELECT DATE '2024-01-02'", DataType::Date32),
            (
                "SELECT TIMESTAMP '2024-01-02 03:04:05.123456'",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ),
        ] {
            let result =
                analyze_projection_expr(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(result.value_type.data_type(), &expected, "{sql}");
            assert!(!result.value_type.nullable(), "{sql}");
        }
        let decimal = analyze_projection_expr("SELECT DECIMAL '001.200'").unwrap();
        assert!(
            matches!(decimal.kind, ExprKind::Literal(LiteralValue::Decimal(value)) if value == "001.200")
        );
        let binary = analyze_projection_expr("SELECT X'00ff'").unwrap();
        assert!(
            matches!(binary.kind, ExprKind::Literal(LiteralValue::Binary(value)) if value == vec![0, 255])
        );
        let json = analyze_projection_expr("SELECT JSON '{\"a\": 1}'").unwrap();
        assert_eq!(
            json.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        assert!(
            matches!(json.kind, ExprKind::Literal(LiteralValue::String(value)) if value == "{\"a\": 1}")
        );
    }

    #[test]
    fn mixed_decimal_array_literal_freezes_precision_for_every_integer() {
        for sql in [
            "SELECT [123, NULL, 1.0]",
            "SELECT [1.0, NULL, 123]",
            "SELECT [NULL, 123, 1.0]",
        ] {
            let expression = analyze_projection_expr(sql).expect("analyze mixed array literal");
            let DataType::List(element) = &expression.value_type.data_type() else {
                panic!("mixed array literal must produce a list");
            };
            assert_eq!(element.data_type(), &DataType::Decimal128(11, 1), "{sql}");
            assert!(element.is_nullable(), "{sql}");
            let ExprKind::FunctionCall { args, binding, .. } = &expression.kind else {
                panic!("array literal must retain its exact function binding");
            };
            for (arg, selected) in args.iter().zip(&binding.selected.argument_types) {
                let novarocks_functions::FunctionArgumentType::Value(selected) = selected else {
                    panic!("array literal arguments are values");
                };
                assert_eq!(selected.data_type(), arg.value_type.data_type(), "{sql}");
            }
            let novarocks_functions::FunctionResultType::Scalar(selected) =
                &binding.selected.result_type
            else {
                panic!("array literal produces one scalar container value");
            };
            assert_eq!(
                selected.data_type(),
                expression.value_type.data_type(),
                "{sql}"
            );
        }
    }

    #[test]
    fn typeof_uses_exact_temporal_declarations_and_preserves_legacy_aliases() {
        for (expression, expected) in [
            ("CAST(missing_value AS TIMESTAMP(6))", "timestamp(6)"),
            ("CAST(NULL AS TIMESTAMP(7))", "timestamp(7)"),
            (
                "TIMESTAMP '2024-01-01 12:00:00.123 UTC'",
                "timestamp(3) with time zone",
            ),
            ("TIME '12:00:00.1234567 +05:30'", "time(7) with time zone"),
            ("CAST(NULL AS TIME(12))", "time(12)"),
            ("INTERVAL '1' DAY", "interval day(1)"),
            ("INTERVAL '1' MONTH", "interval month(1)"),
            ("INTERVAL '1.123' SECOND", "interval second(1, 3)"),
            (
                "INTERVAL '1 02:03:04.123' DAY TO SECOND",
                "interval day(1) to second(6)",
            ),
            (
                "CAST(NULL AS INTERVAL SECOND(2, 4))",
                "interval second(2, 4)",
            ),
            (
                "CAST(NULL AS MAP(TIME(7) WITH TIME ZONE, INTERVAL SECOND(1, 3)))",
                "map(time(7) with time zone, interval second(1, 3))",
            ),
            (
                "CAST(NULL AS ROW(ts TIMESTAMP(3) WITH TIME ZONE))",
                "row(\"ts\" timestamp(3) with time zone)",
            ),
            (
                "[TIMESTAMP '2024-01-01 00:00:00.1234567']",
                "array(timestamp(7))",
            ),
            ("CAST(missing_value AS DATETIME)", "datetime"),
            ("CAST(missing_value AS CHAR)", "char"),
        ] {
            let source = format!("SELECT typeof({expression})");
            let typed = analyze_projection_expr(&source)
                .unwrap_or_else(|error| panic!("{source}: {error}"));
            let ExprKind::Literal(LiteralValue::String(actual)) = typed.kind else {
                panic!("expected a type name: {source}");
            };
            assert_eq!(actual, expected, "{source}");
        }
    }

    #[test]
    fn typeof_parentheses_do_not_change_metadata_argument_analysis() {
        for (sql, expected) in [
            (
                "SELECT typeof(CAST(only_missing_child AS integer))",
                "integer",
            ),
            (
                "SELECT typeof((CAST(only_missing_child AS integer)))",
                "integer",
            ),
            (
                "SELECT typeof(((CAST(only_missing_child AS integer))))",
                "integer",
            ),
            ("SELECT typeof(parse_json(only_missing_child))", "json"),
            ("SELECT typeof((parse_json(only_missing_child)))", "json"),
        ] {
            let actual = analyze_projection_expr(sql).expect("metadata argument is opaque");
            assert_eq!(
                actual.value_type,
                novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                "{sql}"
            );
            assert!(
                matches!(actual.kind, ExprKind::Literal(LiteralValue::String(ref value)) if value == expected),
                "{sql}"
            );
        }
    }

    #[test]
    fn explicit_boolean_casts_keep_types_and_original_bound_children() {
        use novarocks_type_contract::{FunctionValueType, TrinoCastMode};

        // The nullable and required STRING declarations represent c8 and c20
        // in aggregate/sql/compressed_key.sql, not a guessed carrier domain.
        for (profile, nullable, prefix, left_source, right_source, from) in [
            ("literal", false, "", "'DHL'", "','", ""),
            (
                "required_string",
                false,
                "",
                "k",
                "','",
                " FROM unary_input",
            ),
            ("nullable_string", true, "", "k", "','", " FROM unary_input"),
            (
                "coalesce",
                true,
                "",
                "coalesce(k,'')",
                "coalesce(k,',')",
                " FROM unary_input",
            ),
            (
                "inner_cast",
                false,
                "",
                "CAST(v AS VARCHAR)",
                "CAST(7 AS VARCHAR)",
                " FROM unary_input",
            ),
            (
                "cte_grouping",
                true,
                "WITH g AS (SELECT k AS s FROM unary_input GROUP BY k) ",
                "s",
                "','",
                " FROM g",
            ),
        ] {
            let catalog = UnaryInputCatalog {
                key_type: DataType::Utf8,
                nullable,
                logical_type: None,
            };
            let control = crate::compiler::SqlCompileControl::unbounded();
            let boolean =
                format!("CAST({left_source} AS BOOLEAN) OR CAST({right_source} AS BOOLEAN)");
            let sql = format!(
                "{prefix}SELECT {boolean} AS boolean_result, {left_source} AS source_left, {right_source} AS source_right{from}"
            );
            let query = &novarocks_parser::parser::parse_query(&sql)
                .unwrap_or_else(|error| panic!("{profile}: {sql}: {error:?}"));
            let (resolved, registry, _) = super::super::analyze(query, &catalog, "default")
                .unwrap_or_else(|error| panic!("{profile}: {sql}: {error:?}"));
            let QueryBody::Select(select) = &resolved.body else {
                panic!("actual SELECT");
            };
            assert_eq!(select.projection.len(), 3, "{profile}");
            assert_eq!(resolved.output_columns.len(), 3, "{profile}");
            for (actual, output) in select.projection.iter().zip(&resolved.output_columns) {
                assert_eq!(actual.expr.value_type, output.value_type, "{profile}");
                assert_eq!(
                    actual.output_column_id, output.column_id,
                    "same query output binding"
                );
            }
            assert_eq!(
                select.projection[0].field_name.as_deref(),
                Some("boolean_result")
            );
            let expected = FunctionValueType::new(
                DataType::Boolean,
                select.projection[1..]
                    .iter()
                    .any(|item| item.expr.value_type.nullable()),
            );
            assert_eq!(
                select.projection[0].expr.value_type, expected,
                "{profile}: {sql}"
            );
            let boolean = &select.projection[0].expr;
            let ExprKind::BinaryOp {
                left,
                right,
                op: BinOp::Or,
                ..
            } = &boolean.kind
            else {
                panic!("actual Boolean OR binding");
            };
            for (operand, original) in [left.as_ref(), right.as_ref()]
                .into_iter()
                .zip(&select.projection[1..])
            {
                assert_eq!(
                    operand.value_type,
                    FunctionValueType::new(DataType::Boolean, original.expr.value_type.nullable()),
                    "{profile}"
                );
                let ExprKind::Cast {
                    expr,
                    target,
                    cast_mode,
                    ..
                } = &operand.kind
                else {
                    panic!("one actual String-to-Boolean conversion per source");
                };
                assert_eq!(*target, DataType::Boolean);
                assert_eq!(*cast_mode, Some(TrinoCastMode::Explicit));
                assert_eq!(
                    expr.value_type, original.expr.value_type,
                    "complete original child declaration"
                );
                assert!(
                    crate::analysis::expr_identity::typed_expr_semantically_eq(
                        expr,
                        &original.expr,
                        &control,
                    )
                    .unwrap(),
                    "{profile}: source ColumnId, full FVT, call binding and ordered arguments"
                );
                if let ExprKind::FunctionCall { args, binding, .. } = &expr.kind {
                    let ExprKind::FunctionCall {
                        args: expected_args,
                        binding: expected_binding,
                        ..
                    } = &original.expr.kind
                    else {
                        panic!("same original bound source call");
                    };
                    assert_eq!(binding, expected_binding);
                    assert_eq!(args.len(), expected_args.len());
                    assert_eq!(
                        args.len(),
                        2,
                        "the coalesce source keeps both original operands"
                    );
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &binding.selected.result_type
                    else {
                        panic!("source scalar result");
                    };
                    assert_eq!(result, &expr.value_type);
                }
            }
            if profile == "required_string" || profile == "nullable_string" {
                assert_eq!(
                    select.projection[1].expr.value_type,
                    FunctionValueType::new(DataType::Utf8, nullable)
                );
                let ExprKind::ColumnRef {
                    column_id, column, ..
                } = &select.projection[1].expr.kind
                else {
                    panic!("actual catalog input");
                };
                assert_eq!(column, "k");
                let Some(crate::analysis::Relation::Scan(scan)) = &select.from else {
                    panic!("actual source scan");
                };
                assert_eq!(*column_id, scan.column_ids[1]);
                assert_eq!(scan.table.columns[1].name, "k");
                assert_eq!(scan.table.columns[1].nullable, nullable);
                assert_eq!(scan.table.columns[1].logical_type, None);
            }
            if profile == "cte_grouping" {
                assert_eq!(registry.entries.len(), 1);
                let entry = &registry.entries[0];
                let QueryBody::Select(producer) = &entry.resolved_query.body else {
                    panic!("actual grouped CTE producer");
                };
                assert_eq!(producer.group_by.len(), 1);
                assert!(producer.has_aggregation);
                assert!(
                    crate::analysis::expr_identity::typed_expr_semantically_eq(
                        &producer.group_by[0],
                        &producer.projection[0].expr,
                        &control,
                    )
                    .unwrap()
                );
                let Some(crate::analysis::Relation::CTEConsume {
                    cte_id,
                    output_columns,
                    producer_column_ids,
                    ..
                }) = &select.from
                else {
                    panic!("actual grouped CTE consume");
                };
                assert_eq!(*cte_id, entry.id);
                assert_eq!(
                    producer_column_ids.as_slice(),
                    &[entry.output_columns[0].column_id]
                );
                assert_eq!(
                    output_columns[0].value_type,
                    entry.output_columns[0].value_type
                );
                let ExprKind::ColumnRef { column_id, .. } = &select.projection[1].expr.kind else {
                    panic!("actual consume source");
                };
                assert_eq!(*column_id, output_columns[0].column_id);
            }
        }
    }

    #[test]
    fn explicit_boolean_cast_context_constants_reloan_only_actual_occurrences() {
        use crate::compiler::materialized_values::MaterializedSourceBorrow;
        use arrow::array::{Array, StringArray};
        use novarocks_functions::ConstantPool;
        use novarocks_type_contract::{CompilePhase, FunctionValueType};
        use std::sync::Arc;

        let control = crate::compiler::SqlCompileControl::unbounded();
        let source_type = FunctionValueType::new(DataType::Utf8, false);
        let field = Arc::new(
            arrow::datatypes::Field::new("original_string", DataType::Utf8, false).with_metadata(
                std::collections::HashMap::from([(
                    "owner".to_string(),
                    "boolean_cast_original_source".to_string(),
                )]),
            ),
        );
        let pool = ConstantPool::try_new(
            field.clone(),
            source_type.clone(),
            StringArray::from(vec!["unused", "DHL"]).to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let value = pool.value(1).unwrap();
        let sql = "SELECT CAST('DHL' AS BOOLEAN) OR CAST('DHL' AS BOOLEAN)";
        let query = &novarocks_parser::parser::parse_query(sql).unwrap();
        let other_query = &novarocks_parser::parser::parse_query(sql).unwrap();
        fn projection(query: &ast::Query) -> &ast::Expr {
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("select");
            };
            let ast::SelectItem::SingleColumn(column) = &select.projection[0] else {
                panic!("projection");
            };
            &column.expression
        }
        fn original_leaf(expression: &ast::Expr) -> &ast::Expr {
            let ast::Expr::Logical(logical) = expression else {
                panic!("actual OR");
            };
            assert_eq!(logical.terms.len(), 2);
            let ast::Expr::Cast(cast) = &logical.terms[0] else {
                panic!("actual CAST");
            };
            &cast.expression
        }
        let expression = projection(query);
        let source = original_leaf(expression);
        assert!(matches!(source, ast::Expr::Literal(_)));
        let cloned_source = source.clone();
        // This original context-borrow port trusts the caller's actual
        // query-member reference; it does not validate membership itself.
        let mut owned = MaterializedSourceBorrow::empty(query);
        owned
            .insert_context_constant(source, value.clone(), &control)
            .unwrap();
        assert!(owned.view().constant_at(source).is_some());
        assert!(owned.view().constant_at(&cloned_source).is_none());
        assert!(
            owned
                .view()
                .constant_at(original_leaf(projection(other_query)))
                .is_none()
        );
        assert!(owned.view().constant_at(expression).is_none());
        let mut context = logical_context(&control);
        context.materialized_values = Some(owned.view());
        let scope = context.new_scope();
        let actual = context.analyze_expr(expression, &scope).unwrap();
        assert_eq!(
            actual.value_type,
            FunctionValueType::new(DataType::Boolean, false)
        );
        let boolean = &actual;
        let ExprKind::BinaryOp {
            left,
            right,
            op: BinOp::Or,
            ..
        } = &boolean.kind
        else {
            panic!("OR");
        };
        let ExprKind::Cast { expr: child, .. } = &left.kind else {
            panic!("original source cast");
        };
        let ExprKind::Constant(retained) = &child.kind else {
            panic!("original source CV");
        };
        assert_eq!(child.value_type, source_type);
        assert_eq!(retained.value_type(), &source_type);
        assert_eq!(retained.ordinal(), 1);
        assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
        assert_eq!(retained.field().metadata(), field.metadata());
        assert!(!retained.field().metadata().is_empty());
        let ExprKind::Cast { expr: sibling, .. } = &right.kind else {
            panic!("unloaned sibling conversion");
        };
        assert!(
            matches!(&sibling.kind, ExprKind::Literal(LiteralValue::String(text)) if text == "DHL")
        );
    }

    #[test]
    fn computed_arithmetic_operands_keep_complete_types_source_bindings_and_effects() {
        use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, OperatorFunction};
        for nullable in [false, true] {
            let catalog = UnaryInputCatalog {
                key_type: DataType::Int32,
                nullable,
                logical_type: None,
            };
            for (mode, policy) in [
                ("32", DecimalOverflowPolicy::OutputNull),
                ("ERROR_IF_OVERFLOW", DecimalOverflowPolicy::ReportError),
            ] {
                for (sql, decimal, expected_type, expected_op, right_op) in [
                    (
                        "SELECT (k+1)*(v-2) AS result, k, v FROM unary_input",
                        false,
                        DataType::Int64,
                        OperatorFunction::Multiply,
                        OperatorFunction::Subtract,
                    ),
                    (
                        "SELECT (CAST(k AS DECIMAL(4,1))+CAST(1 AS DECIMAL(4,1)))+(CAST(v AS DECIMAL(4,1))+CAST(2 AS DECIMAL(4,1))) AS result, k, v FROM unary_input",
                        true,
                        DataType::Decimal128(6, 1),
                        OperatorFunction::Add,
                        OperatorFunction::Add,
                    ),
                ] {
                    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
                    let ast::SetExpr::Select(original) = query.body.as_ref() else {
                        panic!("SELECT");
                    };
                    let ast::SelectItem::SingleColumn(column) = &original.projection[0] else {
                        panic!("projection");
                    };
                    let ast::Expr::Binary(binary) = &column.expression else {
                        panic!("actual outer arithmetic");
                    };
                    assert!(matches!(binary.left.as_ref(), ast::Expr::Binary(_)));
                    assert!(matches!(binary.right.as_ref(), ast::Expr::Binary(_)));
                    let control = crate::compiler::SqlCompileControl::unbounded();
                    let settings = crate::sql_mode::SqlSemanticSettings::default()
                        .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(mode));
                    let (resolved, _, _) =
                        super::super::analyze_with_function_catalog_and_sql_semantics(
                            query,
                            &catalog,
                            "default",
                            crate::functions::builtin_sql_function_catalog(),
                            &settings,
                            crate::constant::test_constant_policy(),
                            &control,
                        )
                        .unwrap();
                    let QueryBody::Select(select) = &resolved.body else {
                        panic!("resolved SELECT");
                    };
                    let actual = &select.projection[0].expr;
                    let expected = FunctionValueType::new(expected_type, decimal || nullable);
                    assert_eq!(actual.value_type, expected, "{mode}: {sql}");
                    assert_eq!(resolved.output_columns[0].value_type, expected);
                    assert_eq!(select.projection[0].field_name.as_deref(), Some("result"));
                    let (operator, [left, right]) =
                        crate::common::binary_operator::typed_operands(actual)
                            .expect("original outer arithmetic");
                    assert_eq!(operator, expected_op);
                    for (computed, source, expected_op) in [
                        (left, &select.projection[1].expr, OperatorFunction::Add),
                        (right, &select.projection[2].expr, right_op),
                    ] {
                        let computed = match &computed.kind {
                            ExprKind::Cast {
                                expr,
                                target: DataType::Int64,
                                ..
                            } => expr.as_ref(),
                            _ => computed,
                        };
                        let (operator, [left, _]) =
                            crate::common::binary_operator::typed_operands(computed)
                                .expect("original computed child");
                        assert_eq!(operator, expected_op);
                        let source_child = if decimal {
                            assert_eq!(
                                computed.value_type,
                                FunctionValueType::new(DataType::Decimal128(5, 1), true)
                            );
                            let ExprKind::Cast {
                                expr,
                                target,
                                cast_mode,
                                decimal_overflow_policy,
                            } = &left.kind
                            else {
                                panic!("original explicit Decimal cast");
                            };
                            assert_eq!(*target, DataType::Decimal128(4, 1));
                            assert_eq!(*cast_mode, None);
                            assert_eq!(*decimal_overflow_policy, policy);
                            expr.as_ref()
                        } else {
                            left
                        };
                        assert_eq!(source_child.value_type, source.value_type);
                        assert!(
                            crate::analysis::expr_identity::typed_expr_semantically_eq(
                                source_child,
                                source,
                                &control,
                            )
                            .unwrap(),
                            "same-query original column binding and complete FVT"
                        );
                    }
                    assert_eq!(
                        select.projection[1].expr.value_type,
                        FunctionValueType::new(DataType::Int32, nullable)
                    );
                    assert_eq!(
                        select.projection[2].expr.value_type,
                        FunctionValueType::new(DataType::Int64, false)
                    );
                    let mut arena = crate::optimizer::scalar::ScalarArena::new();
                    let scalar = crate::planner::optimizer_bridge::scalar::intern_typed(
                        &mut arena, actual, &control,
                    )
                    .unwrap();
                    assert_eq!(arena.value_type(scalar), &expected);
                    assert!(crate::optimizer::scalar_expr::can_fail(&arena, scalar));
                    let rebuilt = crate::planner::optimizer_bridge::scalar::materialize_exprs(
                        &arena,
                        &[scalar],
                    );
                    assert!(
                        crate::analysis::expr_identity::typed_expr_semantically_eq(
                            actual,
                            &rebuilt[0],
                            &control
                        )
                        .unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn computed_operand_diagnostics_keep_the_original_left_range_and_child_error_order() {
        use crate::analyze_error::AnalyzeErrorKind;
        use novarocks_parser::Span;
        for (sql, kind, span, name) in [
            (
                "SELECT (1+2) + [1]",
                AnalyzeErrorKind::TypeMismatch,
                Span::new(7, 12),
                None,
            ),
            (
                "SELECT ((1+2)) + [1]",
                AnalyzeErrorKind::TypeMismatch,
                Span::new(7, 14),
                None,
            ),
            (
                "SELECT (TRUE OR FALSE) + [1]",
                AnalyzeErrorKind::TypeMismatch,
                Span::new(7, 22),
                None,
            ),
            (
                "SELECT (only_missing_left+1) + only_missing_right",
                AnalyzeErrorKind::UnknownColumn,
                Span::new(8, 25),
                Some("only_missing_left"),
            ),
            (
                "SELECT (1+2) + only_missing_right",
                AnalyzeErrorKind::UnknownColumn,
                Span::new(15, 33),
                Some("only_missing_right"),
            ),
            (
                "SELECT ([1]+1) + only_missing_right",
                AnalyzeErrorKind::TypeMismatch,
                Span::new(8, 11),
                None,
            ),
        ] {
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            let error = super::super::analyze(query, &EmptyCatalog, "default").unwrap_err();
            assert_eq!(error.kind(), kind, "{sql}: {error}");
            assert_eq!(error.span(), Some(span), "{sql}: {error}");
            if let Some(name) = name {
                assert!(error.message().contains(name), "{error}");
            }
        }
    }

    #[test]
    fn computed_arithmetic_keeps_the_actual_constant_field_ordinal_and_compiler_source() {
        use crate::compiler::materialized_values::MaterializedSourceBorrow;
        use arrow::array::{Array, Int32Array};
        use novarocks_functions::ConstantPool;
        use novarocks_type_contract::{CompilePhase, FunctionValueType};
        use std::sync::Arc;
        let control = crate::compiler::SqlCompileControl::unbounded();
        let source_type = FunctionValueType::new(DataType::Int32, false);
        let field = Arc::new(
            arrow::datatypes::Field::new("computed_source", DataType::Int32, false).with_metadata(
                std::collections::HashMap::from([(
                    "owner".to_string(),
                    "actual_computed_input".to_string(),
                )]),
            ),
        );
        let pool = ConstantPool::try_new(
            field.clone(),
            source_type.clone(),
            Int32Array::from(vec![17, 21]).to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let query =
            &novarocks_parser::parser::parse_query("SELECT (source_value+1)*(3+4)").unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let ast::SelectItem::SingleColumn(column) = &select.projection[0] else {
            panic!("projection");
        };
        let ast::Expr::Binary(outer) = &column.expression else {
            panic!("actual outer product");
        };
        let ast::Expr::Binary(left) = outer.left.as_ref() else {
            panic!("actual computed left operand");
        };
        let source = left.left.as_ref();
        assert!(matches!(source, ast::Expr::Identifier(name) if name.value == "source_value"));
        let clone = source.clone();
        // This existing context port trusts the caller's actual query member.
        let mut owned = MaterializedSourceBorrow::empty(query);
        owned
            .insert_context_constant(source, pool.value(1).unwrap(), &control)
            .unwrap();
        assert!(owned.view().constant_at(&clone).is_none());
        let mut context = logical_context(&control);
        context.materialized_values = Some(owned.view());
        let scope = context.new_scope();
        let actual = context.analyze_expr(&column.expression, &scope).unwrap();
        assert_eq!(
            actual.value_type,
            FunctionValueType::new(DataType::Int32, false)
        );
        let mut arena = crate::optimizer::scalar::ScalarArena::new();
        let scalar =
            crate::planner::optimizer_bridge::scalar::intern_typed(&mut arena, &actual, &control)
                .unwrap();
        assert!(crate::optimizer::scalar_expr::can_fail(&arena, scalar));
        let rebuilt =
            crate::planner::optimizer_bridge::scalar::materialize_exprs(&arena, &[scalar]);
        for expression in [&actual, &rebuilt[0]] {
            let operands = crate::common::binary_operator::typed_operands;
            let (operator, [left, _]) = operands(expression).expect("original product");
            assert_eq!(
                operator,
                novarocks_type_contract::OperatorFunction::Multiply
            );
            let (operator, [left, _]) = operands(left).expect("original computed sum");
            assert_eq!(operator, novarocks_type_contract::OperatorFunction::Add);
            let ExprKind::Constant(retained) = &left.kind else {
                panic!("original source");
            };
            assert_eq!(left.value_type, source_type);
            assert_eq!(retained.value_type(), &source_type);
            assert_eq!(retained.ordinal(), 1);
            assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
            assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
            assert_eq!(retained.field().metadata(), field.metadata());
            assert!(!retained.field().metadata().is_empty());
        }
    }

    #[test]
    fn try_cast_boolean_or_preserves_runtime_nullability() {
        for sql in [
            "select TRY_CAST('DHL' AS BOOLEAN) OR TRY_CAST(',' AS BOOLEAN) OR TRY_CAST('BARIAN' AS BOOLEAN)",
            "select TRY_CAST(coalesce(NULL, '') AS BOOLEAN) OR TRY_CAST(', ' AS BOOLEAN) OR TRY_CAST(coalesce(NULL, '') AS BOOLEAN)",
        ] {
            let expression = analyze_projection_expr(sql).expect("analyze string boolean coercion");
            assert_eq!(
                expression.value_type,
                novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
                "complete nullable Boolean declaration: {sql}",
            );
        }
        for sql in [
            "SELECT TRUE OR FALSE",
            "SELECT CAST(1 AS BOOLEAN) OR CAST(0 AS BOOLEAN)",
            "SELECT CAST('DHL' AS BOOLEAN) OR CAST(',' AS BOOLEAN)",
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            assert_eq!(
                expression.value_type,
                novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
                "{sql}"
            );
        }
    }

    #[test]
    fn canonical_missing_specials_refuse_before_children_but_keep_metadata_and_opaque_casts() {
        use crate::analyze_error::AnalyzeErrorKind;
        use novarocks_parser::build;
        let control = crate::compiler::SqlCompileControl::unbounded();
        let ctx = logical_context(&control);
        let scope = ctx.new_scope();
        let missing = build::identifier(build::ident("only_missing_child", false));
        let missing_kind = ctx.analyze_expr(&missing, &scope).unwrap_err().kind();
        for (name, quoted) in [("strpos", false), ("normalize", true)] {
            let mut expression = build::function(
                build::object_name(vec![build::ident(name, quoted)]).unwrap(),
                vec![missing.clone(), build::string("NFC")],
            );
            let error = ctx.analyze_expr(&expression, &scope).unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert!(error.message().contains(name), "{error}");
            assert_eq!(error.span(), Some(expression.span()));

            let ast::Expr::FunctionCall(call) = &mut expression else {
                panic!("call");
            };
            call.arguments[0].name = Some(build::ident("named", false));
            let error = ctx.analyze_expr(&expression, &scope).unwrap_err();
            assert_eq!(
                error.message(),
                "named function arguments are not supported"
            );
            let ast::Expr::FunctionCall(call) = &mut expression else {
                panic!("call");
            };
            call.arguments[0].name = None;

            let query =
                &novarocks_parser::parser::parse_query("SELECT typeof(CAST(NULL AS INTEGER))")
                    .unwrap();
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("select");
            };
            let ast::SelectItem::SingleColumn(projection_column) = select.projection[0].clone()
            else {
                panic!("projection");
            };
            assert!(projection_column.alias.is_none(), "anonymous projection");
            let mut opaque = projection_column.into_inner().expression;
            let ast::Expr::FunctionCall(call) = &mut opaque else {
                panic!("typeof");
            };
            let ast::Expr::Cast(cast) = &mut call.arguments[0].value else {
                panic!("cast");
            };
            cast.expression = expression;
            let actual = ctx.analyze_expr(&opaque, &scope).unwrap();
            assert_eq!(
                actual.value_type,
                novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false)
            );
            assert!(
                matches!(actual.kind, ExprKind::Literal(LiteralValue::String(value)) if value == "integer")
            );
        }
        let unknown = build::function(
            build::object_name(vec![build::ident("only_unknown_function", false)]).unwrap(),
            vec![build::string("original")],
        );
        assert_eq!(
            ctx.analyze_expr(&unknown, &scope).unwrap_err().kind(),
            AnalyzeErrorKind::UnknownFunction
        );
        for parts in [
            vec![build::ident("strpos", true)],
            vec![build::ident("normalize", false)],
            vec![build::ident("NORMALIZE", true)],
            vec![
                build::ident("catalog", false),
                build::ident("strpos", false),
            ],
            vec![
                build::ident("catalog", false),
                build::ident("normalize", true),
            ],
            vec![build::ident("only_unknown_function", false)],
        ] {
            let expression =
                build::function(build::object_name(parts).unwrap(), vec![missing.clone()]);
            assert_eq!(
                ctx.analyze_expr(&expression, &scope).unwrap_err().kind(),
                missing_kind,
                "noncanonical names must still analyze their original child before catalog lookup"
            );
        }
    }

    fn analyze_projection_expr_with_function_catalog(
        sql: &str,
        function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    ) -> Result<crate::analysis::TypedExpr, String> {
        analyze_projection_expr_with_catalog(sql, &EmptyCatalog, function_catalog)
    }

    fn analyze_projection_expr_with_catalog(
        sql: &str,
        catalog: &dyn PlannerTableProvider,
        function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    ) -> Result<crate::analysis::TypedExpr, String> {
        let query =
            &novarocks_parser::parser::parse_query(sql).map_err(|error| error.to_string())?;
        let (resolved, _registry, _factory) = super::super::analyze_with_function_catalog(
            query,
            catalog,
            "default",
            function_catalog,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .map_err(|error| error.to_string())?;
        let QueryBody::Select(select) = resolved.body else {
            return Err("expected select".to_string());
        };
        select
            .projection
            .into_iter()
            .next()
            .map(|item| item.expr)
            .ok_or_else(|| "expected projection".to_string())
    }

    #[test]
    fn map_constructor_preserves_complete_json_and_same_carrier_string_children() {
        use novarocks_type_contract::ValueLogicalType;
        let mut outputs = Vec::new();
        for (input, domain) in [
            ("parse_json('{}')", Some(ValueLogicalType::Json)),
            ("'{}'", None),
        ] {
            let sql = format!("SELECT map(ARRAY[1], ARRAY[{input}])");
            let expression =
                analyze_projection_expr(&sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let ExprKind::FunctionCall { binding, .. } = &expression.kind else {
                panic!("selected MAP call")
            };
            let novarocks_functions::FunctionResultType::Scalar(selected) =
                &binding.selected.result_type
            else {
                panic!("scalar MAP result")
            };
            assert_eq!(selected, &expression.value_type);
            let child = expression
                .value_type
                .typed_binding()
                .borrowed()
                .child_at(1)
                .unwrap();
            assert_eq!(child.value_type().domain(), domain, "{sql}");
            let DataType::Map(entries, _) = expression.value_type.data_type() else {
                panic!("MAP")
            };
            let DataType::Struct(fields) = entries.data_type() else {
                panic!("entries")
            };
            assert!(entries.metadata().as_arc().is_none());
            assert!(
                fields
                    .iter()
                    .all(|field| field.metadata().as_arc().is_none())
            );
            outputs.push(expression.value_type);
        }
        assert_eq!(outputs[0].data_type(), outputs[1].data_type());
        assert_ne!(
            outputs[0], outputs[1],
            "equal carriers preserve distinct nominal domains"
        );
    }

    #[test]
    fn map_selector_preserves_row_children_and_nested_nulls() {
        let row = "CAST(ROW(parse_json('{}'), CAST(NULL AS VARCHAR)) AS ROW(j JSON, s VARCHAR))";
        let map = format!("map(ARRAY[1], ARRAY[{row}])");
        let source = analyze_projection_expr(&format!("SELECT {map}")).unwrap();
        let output = analyze_projection_expr(&format!("SELECT element_at({map}, 1)")).unwrap();
        let expected = super::container_child_value_type_observed(
            &source.value_type,
            1,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let actual = &output.value_type;
        assert_eq!(actual, &expected.with_nullable(true).unwrap());
        let DataType::Struct(fields) = actual.data_type() else {
            panic!("named ROW")
        };
        assert_eq!(fields[0].name(), "j");
        assert_eq!(fields[1].name(), "s");
        assert!(fields[1].is_nullable());
        assert_eq!(
            actual.value_type().child_type_at(0).unwrap().domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
    }

    #[test]
    fn map_complete_child_projection_keeps_original_control_at_every_poll() {
        use crate::compiler::{SqlCancellationObservation, SqlCompileControl};
        use novarocks_type_contract::CompileControlError;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Observation {
            polls: AtomicUsize,
            cancel_at: Option<usize>,
        }
        impl SqlCancellationObservation for Observation {
            fn is_cancelled(&self) -> bool {
                let poll = self.polls.fetch_add(1, Ordering::SeqCst);
                self.cancel_at.is_some_and(|at| poll >= at)
            }
        }
        let source = analyze_projection_expr("select map(ARRAY[1], ARRAY[parse_json('{}')])")
            .unwrap()
            .value_type;
        let observation = Arc::new(Observation {
            polls: AtomicUsize::new(0),
            cancel_at: None,
        });
        let control = SqlCompileControl::new(None, observation.clone());
        super::container_child_value_type_observed(&source, 1, &control).unwrap();
        let baseline = observation.polls.load(Ordering::SeqCst);
        assert!(baseline >= 2);
        for at in 0..baseline {
            let observation = Arc::new(Observation {
                polls: AtomicUsize::new(0),
                cancel_at: Some(at),
            });
            let control = SqlCompileControl::new(None, observation.clone());
            let error =
                super::container_child_value_type_observed(&source, 1, &control).unwrap_err();
            assert_eq!(error.control_error(), Some(CompileControlError::Cancelled));
            assert_eq!(observation.polls.load(Ordering::SeqCst), at + 1);
        }
        let observation = Arc::new(Observation {
            polls: AtomicUsize::new(0),
            cancel_at: None,
        });
        let control = SqlCompileControl::new(
            Some(std::time::Instant::now() - std::time::Duration::from_millis(1)),
            observation.clone(),
        );
        let error = super::container_child_value_type_observed(&source, 1, &control).unwrap_err();
        assert_eq!(
            error.control_error(),
            Some(CompileControlError::DeadlineExceeded)
        );
        assert_eq!(observation.polls.load(Ordering::SeqCst), 1);
    }

    fn assert_exact_list_scalar_owner(expression: &crate::analysis::TypedExpr, expected_id: &str) {
        let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
            panic!("expected direct exact scalar owner");
        };
        assert_eq!(binding.function_id.as_str(), expected_id);
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("expected scalar");
        };
        assert_eq!(&expression.value_type, result);
        for (argument, selected) in args.iter().zip(&binding.selected.argument_types) {
            let novarocks_functions::FunctionArgumentType::Value(selected) = selected else {
                panic!("expected value argument");
            };
            assert!(
                argument
                    .value_type
                    .fits_physical_binding_observed::<novarocks_type_contract::ValueTypeError>(
                        selected,
                        || Ok(())
                    )
                    .unwrap()
            );
        }
    }
    fn assert_json_list_declaration(expression: &crate::analysis::TypedExpr) {
        let DataType::List(item) = expression.value_type.data_type() else {
            panic!("expected List");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert_eq!(
            expression
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert!(item.metadata().as_arc().is_none());
    }
    fn assert_json_list_scalar_owner(expression: &crate::analysis::TypedExpr, expected_id: &str) {
        assert_json_list_declaration(expression);
        assert_exact_list_scalar_owner(expression, expected_id);
    }

    #[test]
    fn json_array_literal_and_concat_keep_complete_exact_owner_bindings() {
        let expression =
            analyze_projection_expr("select [json_object('k':1), json_object('k':2), null]")
                .unwrap();
        assert_json_list_scalar_owner(&expression, "builtin.scalar/__array_literal/v1");
        for (sql, source_carrier, source_domain) in [
            (
                "select CAST([json_object('k':1), null] AS ARRAY<json>)",
                DataType::Utf8,
                novarocks_type_contract::ValueLogicalType::Json,
            ),
            (
                "select CAST([] AS ARRAY<json>)",
                DataType::Null,
                novarocks_type_contract::ValueLogicalType::Physical,
            ),
            (
                "select CAST([null] AS ARRAY<json>)",
                DataType::Null,
                novarocks_type_contract::ValueLogicalType::Physical,
            ),
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            assert_json_list_declaration(&expression);
            let ExprKind::Cast {
                expr, cast_mode, ..
            } = &expression.kind
            else {
                panic!("ordinary outer CAST")
            };
            assert_eq!(
                *cast_mode,
                Some(novarocks_type_contract::TrinoCastMode::Explicit)
            );
            assert_exact_list_scalar_owner(expr, "builtin.scalar/__array_literal/v1");
            let child = expr.value_type.child_at(0).unwrap();
            assert_eq!(child.data_type(), &source_carrier);
            assert_eq!(child.logical_type(), source_domain);
            assert!(!expr.value_type.nullable());
        }
        for sql in [
            "select concat([json_object('k':1), json_object('k':2)], CAST(ARRAY[] AS ARRAY(JSON)))",
            "select concat(cast(['not JSON', '123'] as array<json>), CAST(ARRAY[] AS ARRAY(JSON)))",
            "with q as (select cast(['not JSON', '123'] as array<json>) as j) select concat(j, CAST(ARRAY[] AS ARRAY(JSON))) from q",
            "select concat(j2, CAST(ARRAY[] AS ARRAY(JSON))) from (select cast(['not JSON','123'] as array<json>) as j, j as j2) q",
            "select concat(CAST([json_object('k':1), json_object('k':2)] AS ARRAY<json>), CAST(ARRAY[] AS ARRAY(JSON)))",
            "select concat(cast([json_object('k':1), json_object('k':2)] as array<json>), CAST(ARRAY[] AS ARRAY(JSON)))",
            "with q as (select [json_object('k':1), json_object('k':2)] as j) select concat(j, CAST(ARRAY[] AS ARRAY(JSON))) from q",
            "select concat(j2, CAST(ARRAY[] AS ARRAY(JSON))) from (select [json_object('k':1),json_object('k':2)] as j, j as j2) q",
            "select concat(j, CAST(ARRAY[] AS ARRAY(JSON))) from (values ([json_object('k':1),json_object('k':2)]), (null)) q(j)",
            "select concat(j, CAST(ARRAY[] AS ARRAY(JSON))) from (select [json_object('k':1),json_object('k':2)] as j union all select null) q",
            "select concat(case when true then [json_object('k':1),json_object('k':2)] else null end, CAST(ARRAY[] AS ARRAY(JSON)))",
        ] {
            let expression =
                analyze_projection_expr(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_json_list_scalar_owner(&expression, "builtin.scalar/concat/v1");
        }
        for (sql, cast_boundary, owner_id) in [
            (
                r#"select ['{"k":1}', '{"k":2}']"#,
                false,
                "builtin.scalar/__array_literal/v1",
            ),
            (
                "select CAST([json_object('k':1), json_object('k':2)] AS ARRAY<varchar>)",
                true,
                "builtin.scalar/__array_literal/v1",
            ),
            (
                r#"select concat(['{"k":1}', '{"k":2}'], CAST(ARRAY[] AS ARRAY(VARCHAR)))"#,
                false,
                "builtin.scalar/concat/v1",
            ),
            (
                "select concat(cast([json_object('k':1), json_object('k':2)] as array<varchar>), CAST(ARRAY[] AS ARRAY(VARCHAR)))",
                false,
                "builtin.scalar/concat/v1",
            ),
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            let DataType::List(item) = expression.value_type.data_type() else {
                panic!("expected List")
            };
            assert_eq!(novarocks_types::logical::logical_type_of_field(item), None);
            assert_eq!(
                expression.value_type.child_at(0).unwrap().logical_type(),
                novarocks_type_contract::ValueLogicalType::Physical
            );
            if cast_boundary {
                let ExprKind::Cast {
                    expr, cast_mode, ..
                } = &expression.kind
                else {
                    panic!("ordinary outer CAST")
                };
                assert_eq!(
                    *cast_mode,
                    Some(novarocks_type_contract::TrinoCastMode::Explicit)
                );
                assert_json_list_scalar_owner(expr, owner_id);
            } else {
                assert_exact_list_scalar_owner(&expression, owner_id);
            }
        }
    }

    #[test]
    fn json_list_output_adapters_preserve_existing_distinct_error_contract() {
        let error =
            analyze_projection_expr("select array_agg(distinct ARRAY[JSON_OBJECT('2': 3)])")
                .unwrap_err();
        assert!(
            error.contains("can't rewrite distinct to group by on (array<json>)."),
            "{error}"
        );
        let error =
            analyze_projection_expr("select array_agg(distinct JSON_OBJECT('2': 3))").unwrap_err();
        assert!(
            error.contains("can't rewrite distinct to group by on (json)."),
            "{error}"
        );
    }

    fn assert_array_agg_json_owner(expression: &crate::analysis::TypedExpr) {
        let DataType::List(item) = &expression.value_type.data_type() else {
            panic!("expected List");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert_eq!(
            expression
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert!(item.metadata().as_arc().is_none());
        let (binding, arguments) = match &expression.kind {
            ExprKind::AggregateCall { resolved, args, .. } => (resolved, args),
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } => (binding, args),
            _ => panic!("expected direct exact aggregate owner"),
        };
        let result = crate::functions::aggregate_result_type(binding);
        assert_eq!(&expression.value_type, result);
        assert_eq!(
            arguments[0].value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        let selected = crate::functions::aggregate_selection(binding);
        match &selected.intermediate_type.data_type() {
            DataType::List(item) => {
                assert!(item.metadata().as_arc().is_none());
                assert_eq!(
                    selected
                        .intermediate_type
                        .value_type()
                        .child_type_at(0)
                        .unwrap()
                        .domain(),
                    arguments[0].value_type.value_type().domain()
                );
            }
            DataType::Struct(fields) => {
                let DataType::List(item) = fields[0].data_type() else {
                    panic!("expected update channel List");
                };
                assert!(item.metadata().as_arc().is_none());
                assert_eq!(
                    selected
                        .intermediate_type
                        .value_type()
                        .child_type_at(0)
                        .unwrap()
                        .child_type_at(0)
                        .unwrap()
                        .domain(),
                    arguments[0].value_type.value_type().domain()
                );
            }
            _ => panic!("unexpected ARRAY_AGG intermediate type"),
        }
    }

    #[test]
    fn array_agg_freezes_json_output_separately_from_physical_binding() {
        for sql in [
            "select array_agg(JSON_OBJECT('2': 3))",
            "select array_agg(parse_json('[1]'))",
            "select array_agg(j2) from (select cast(json_object('k':1) as json) as j, j as j2) s",
            "select array_agg(j) from (values (cast(json_object('k':1) as json)), (null)) q(j)",
            "select array_agg(j) from (select cast(json_object('k':1) as json) as j union all select null) q",
            "select array_agg(case when true then json_object('k':1) else null end)",
            "select array_agg(case when true then json_object('k':1) else json_object('k':2) end)",
            "select array_agg(parse_json('{\"k\":1}'))",
            "select array_agg(cast(json_object('k':1) as json))",
            "select array_agg(json_object('k': 1) order by 1 desc)",
            "select array_agg(json_object('k': 1)) over ()",
            "select array_agg(j) from (select json_object('k': 1) as j) s",
            "select array_agg(j) from (select cast(json_object('k':1) as json) as j) s",
            "with s as (select cast(json_object('k':1) as json) as j) select array_agg(j) from s",
            "with s as (select json_object('k':1) as j) select array_agg(a.j) from s a, s b",
            "select array_agg(j) from (select json_object('k':1) as j union all select json_object('k':2)) s",
        ] {
            let expression =
                analyze_projection_expr(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_array_agg_json_owner(&expression);
        }
        // JSON_ARRAY has a legacy declaration but no installed scalar kernel.
        // Keep its admission control alongside the JSON aggregate boundary.
        for sql in [
            "select array_agg(JSON_ARRAY(1 NULL ON NULL))",
            "select array_agg(JSON_ARRAY(1 NULL ON NULL)) where false",
        ] {
            let error = analyze_projection_expr(sql).unwrap_err();
            assert!(
                error.contains(
                    "builtin function `json_array` has no admitted selected scalar implementation"
                ),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn array_agg_string_values_do_not_gain_json_semantics() {
        use novarocks_type_contract::{ValueKind, ValueLogicalType};
        for sql in [
            "select array_agg('{\"k\":1}')",
            "select array_agg(cast(cast('not JSON' as json) as varchar))",
            "with q as (select cast(cast('not JSON' as json) as varchar) as j) select array_agg(j) from q",
            "select array_agg(j2) from (select cast(cast('not JSON' as json) as varchar) as j, j as j2) q",
            "select array_agg(j) from (values (cast('{\"k\":1}' as varchar)), (null)) q(j)",
            "select array_agg(cast(json_object('k':1) as varchar))",
            "select array_agg(case when true then cast(json_object('k':1) as varchar) else cast(json_object('k':2) as varchar) end)",
            "select array_agg(j) from (select cast(json_object('k':1) as varchar) as j) s",
            "with s as (select cast(json_object('k':1) as varchar) as j) select array_agg(j) from s",
        ] {
            let expression =
                analyze_projection_expr(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let ValueKind::List(item) = expression.value_type.value_type().kind() else {
                panic!("expected List");
            };
            assert!(
                matches!(
                    item.kind(),
                    ValueKind::String | ValueKind::BoundedCharacter(_)
                ),
                "{sql}"
            );
            assert!(matches!(expression.kind, ExprKind::AggregateCall { .. }));
        }
        let typed_json =
            analyze_projection_expr("select array_agg(cast('not JSON' as json))").unwrap();
        assert_eq!(
            typed_json.value_type.child_at(0).unwrap().logical_type(),
            ValueLogicalType::Json
        );
        let mixed = analyze_projection_expr(
            "select array_agg(j) from (values (cast(json_object('k':1) as json)), ('{\"k\":2}')) q(j)",
        ).unwrap_err();
        assert!(mixed.contains("do not have a common Trino type"), "{mixed}");
    }

    #[test]
    fn array_agg_catalog_json_provenance_survives_alias_and_cte_rebinding() {
        struct JsonCatalog;
        impl PlannerTableProvider for JsonCatalog {
            fn resolve_table_for_analysis(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                // Use the existing resolved table constructor so the logical tag
                // comes from catalog metadata, not a name or physical-type guess.
                let planner = crate::planner::table::TableDef {
                    name: table.into(),
                    columns: vec![
                        novarocks_types::schema::ColumnDef {
                            name: "j".into(),
                            data_type: DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: Some(novarocks_types::schema::SqlType::Json),
                        },
                        novarocks_types::schema::ColumnDef {
                            name: "ja".into(),
                            data_type: DataType::List(std::sync::Arc::new(
                                arrow::datatypes::Field::new("item", DataType::Utf8, true),
                            )),
                            nullable: true,
                            write_default: None,
                            logical_type: Some(novarocks_types::schema::SqlType::Array(Box::new(
                                novarocks_types::schema::SqlType::Json,
                            ))),
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::Sql(SqlScanSource::new(
                        SqlTableBindingId::new(
                            SqlTableBindingScopeId::new(NonZeroU64::new(44).unwrap()),
                            NonZeroU32::new(1).unwrap(),
                        ),
                        SqlTableIdentity {
                            catalog: catalog.unwrap_or("default_catalog").into(),
                            namespace: database.into(),
                            table: table.into(),
                        },
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
                };
                Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                    catalog, database, planner,
                ))
            }
        }
        for sql in [
            "select array_agg(j) from json_input",
            "select array_agg(q.payload) from (select j as renamed from json_input) q(payload)",
            "with q as (select j as payload from json_input) select array_agg(payload) from q",
        ] {
            let expression = analyze_projection_expr_with_catalog(
                sql,
                &JsonCatalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            assert_array_agg_json_owner(&expression);
        }
        for sql in [
            "select concat(ja, CAST(ARRAY[] AS ARRAY(JSON))) from json_input",
            "select concat(q.payload, CAST(ARRAY[] AS ARRAY(JSON))) from (select ja as renamed from json_input) q(payload)",
            "with q as (select ja as payload from json_input) select concat(payload, CAST(ARRAY[] AS ARRAY(JSON))) from q",
        ] {
            let expression = analyze_projection_expr_with_catalog(
                sql,
                &JsonCatalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            assert_json_list_scalar_owner(&expression, "builtin.scalar/concat/v1");
        }
    }

    struct UnaryInputCatalog {
        key_type: DataType,
        nullable: bool,
        logical_type: Option<novarocks_types::schema::SqlType>,
    }

    impl PlannerTableProvider for UnaryInputCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            if table != "unary_input" {
                return Err(format!("table not found: {table}"));
            }
            let column = |name: &str, data_type, nullable| novarocks_types::schema::ColumnDef {
                name: name.to_string(),
                data_type,
                nullable,
                write_default: None,
                logical_type: None,
            };
            let planner = crate::planner::table::TableDef {
                name: table.to_string(),
                columns: vec![
                    column("v", DataType::Int64, false),
                    novarocks_types::schema::ColumnDef {
                        logical_type: self.logical_type.clone(),
                        ..column("k", self.key_type.clone(), self.nullable)
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::Sql(SqlScanSource::new(
                    SqlTableBindingId::new(
                        SqlTableBindingScopeId::new(NonZeroU64::new(44).unwrap()),
                        NonZeroU32::new(1).unwrap(),
                    ),
                    SqlTableIdentity {
                        catalog: catalog.unwrap_or("default_catalog").to_string(),
                        namespace: database.to_string(),
                        table: table.to_string(),
                    },
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            };
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    #[test]
    fn scalar_sequence_selects_exact_bigint_and_date_declarations() {
        for (sql, item) in [
            ("SELECT sequence(1, 3)", DataType::Int64),
            ("SELECT sequence(1, 3, 2)", DataType::Int64),
            (
                "SELECT sequence(DATE '2025-10-01', DATE '2025-10-03')",
                DataType::Date32,
            ),
        ] {
            let selected =
                analyze_projection_expr(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let ExprKind::FunctionCall { binding, .. } = &selected.kind else {
                panic!("selected sequence call")
            };
            assert_eq!(binding.function_id.as_str(), "builtin.scalar/sequence/v1");
            let DataType::List(actual) = selected.value_type.data_type() else {
                panic!("sequence result ARRAY")
            };
            assert_eq!(actual.data_type(), &item);
        }
        for sql in [
            "SELECT sequence('a', 'b')",
            "SELECT sequence(1)",
            "SELECT array_generate(1, 3)",
        ] {
            assert!(analyze_projection_expr(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn strict_unary_minus_preserves_column_type_and_null_domain() {
        for key_type in [
            DataType::Int64,
            DataType::Float64,
            DataType::Decimal128(18, 9),
            DataType::Decimal256(60, 2),
        ] {
            for nullable in [false, true] {
                let catalog = UnaryInputCatalog {
                    key_type: key_type.clone(),
                    nullable,
                    logical_type: None,
                };
                let expression = analyze_projection_expr_with_catalog(
                    "select -k from unary_input",
                    &catalog,
                    crate::functions::builtin_sql_function_catalog(),
                )
                .unwrap();
                assert_eq!(expression.value_type.data_type().clone(), key_type);
                assert_eq!(expression.value_type.nullable(), nullable);
                let operand = crate::common::negation::typed_operand(&expression)
                    .expect("expected a bound numeric negation");
                assert!(matches!(operand.kind, ExprKind::ColumnRef { .. }));
                assert_eq!(operand.value_type.data_type().clone(), key_type);
                assert_eq!(operand.value_type.nullable(), nullable);
            }
        }
        for nullable in [false, true] {
            let catalog = UnaryInputCatalog {
                key_type: DataType::Int64,
                nullable,
                logical_type: None,
            };
            let expression = analyze_projection_expr_with_catalog(
                "select bitnot(k) from unary_input",
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            assert_eq!(expression.value_type.data_type().clone(), DataType::Int64);
            assert!(expression.value_type.nullable());
            let operand = named_bitnot_operand(&expression);
            assert!(matches!(operand.kind, ExprKind::ColumnRef { .. }));
            assert_eq!(operand.value_type.data_type().clone(), DataType::Int64);
            assert_eq!(operand.value_type.nullable(), nullable);
        }
    }

    #[test]
    fn arithmetic_preserves_authored_largeint_and_nullable_null_lifts() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let catalog = UnaryInputCatalog {
            key_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: Some(novarocks_types::schema::SqlType::LargeInt),
        };
        let expected = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        for sql in [
            "select k+1 from unary_input",
            "select k+NULL from unary_input",
            "select NULL+k from unary_input",
            "select k+cast(NULL AS largeint) from unary_input",
        ] {
            let expression = analyze_projection_expr_with_catalog(
                sql,
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            assert_eq!(expression.value_type, expected, "{sql}");
            let ExprKind::BinaryOp { left, right, .. } = expression.kind else {
                panic!("expected exact arithmetic");
            };
            let operand = if matches!(left.kind, ExprKind::ColumnRef { .. }) {
                left
            } else {
                right
            };
            assert!(matches!(operand.kind, ExprKind::ColumnRef { .. }), "{sql}");
            assert_eq!(
                operand.value_type.logical_type(),
                ValueLogicalType::LargeInt
            );
            assert!(!operand.value_type.nullable());
        }
        let expression = analyze_projection_expr_with_catalog(
            "select element_at([],1)+k from unary_input",
            &catalog,
            crate::functions::builtin_sql_function_catalog(),
        )
        .unwrap();
        assert_eq!(expression.value_type, expected);
        let ExprKind::BinaryOp { left, right, .. } = expression.kind else {
            panic!("expected exact arithmetic");
        };
        assert!(matches!(right.kind, ExprKind::ColumnRef { .. }));
        let left = left.into_inner();
        let ExprKind::FunctionCall { binding, args, .. } = left.kind else {
            panic!("nonliteral NULL must invoke its exact lift owner");
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/value_domain_conversion/v1"
        );
        assert_eq!(left.value_type, expected);
        assert_eq!(args.len(), 1);
        assert_eq!(
            args[0].value_type,
            FunctionValueType::new(DataType::Null, true)
        );
        assert!(matches!(args[0].kind, ExprKind::FunctionCall { .. }));
        let error = analyze_projection_expr("select NULL+NULL").unwrap_err();
        assert_eq!(error, "multiple declared overloads match ambiguously");
        let result = analyze_projection_expr("select NULL%NULL").unwrap();
        assert_eq!(
            result.value_type,
            FunctionValueType::new(DataType::Int8, true)
        );
    }

    fn comparison_operands(expression: &TypedExpr) -> Vec<&TypedExpr> {
        if let Some((_, [left, right])) =
            crate::common::binary_operator::typed_comparison(expression)
        {
            return vec![left, right];
        }
        match &expression.kind {
            ExprKind::InList { expr, list, .. } => {
                std::iter::once(expr.as_ref()).chain(list.iter()).collect()
            }
            ExprKind::Between {
                expr, low, high, ..
            } => vec![expr, low, high],
            ExprKind::Case {
                operand: Some(operand),
                when_then,
                ..
            } => std::iter::once(operand.as_ref())
                .chain(when_then.iter().map(|(label, _)| label))
                .collect(),
            _ => panic!("expected comparison-bearing expression"),
        }
    }

    #[test]
    fn comparisons_freeze_exact_largeint_domains_without_identity_casts() {
        use novarocks_type_contract::ValueLogicalType;
        let catalog = UnaryInputCatalog {
            key_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: Some(novarocks_types::schema::SqlType::LargeInt),
        };
        for (sql, operand_nullability, result_nullable) in [
            ("select k=1 from unary_input", &[false, false][..], false),
            (
                "select k IN (1,cast(2 AS int)) from unary_input",
                &[false, false, true][..],
                true,
            ),
            (
                "select k BETWEEN 1 AND cast(2 AS int) from unary_input",
                &[false, false, true][..],
                true,
            ),
            (
                "select CASE k WHEN 1 THEN 7 WHEN cast(2 AS int) THEN 8 ELSE 9 END from unary_input",
                &[false, false, true][..],
                true,
            ),
        ] {
            let expression = analyze_projection_expr_with_catalog(
                sql,
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            let operands = comparison_operands(&expression);
            assert!(
                matches!(operands[0].kind, ExprKind::ColumnRef { .. }),
                "{sql}"
            );
            assert_eq!(operands.len(), operand_nullability.len());
            for (ordinal, operand) in operands.iter().enumerate() {
                assert_eq!(
                    operand.value_type.logical_type(),
                    ValueLogicalType::LargeInt,
                    "{sql}"
                );
                assert_eq!(
                    operand.value_type.data_type().clone(),
                    DataType::FixedSizeBinary(16),
                    "{sql}"
                );
                assert_eq!(
                    operand.value_type.nullable(),
                    operand_nullability[ordinal],
                    "{sql}"
                );
                if ordinal > 0 {
                    let ExprKind::FunctionCall { binding, args, .. } = &operand.kind else {
                        panic!("signed label needs an exact LARGEINT conversion: {sql}");
                    };
                    assert_eq!(
                        binding.function_id.as_str(),
                        "builtin.scalar/value_domain_conversion/v1"
                    );
                    assert_eq!(
                        args[0].value_type.logical_type(),
                        ValueLogicalType::Physical
                    );
                    assert!(matches!(
                        args[0].value_type.data_type(),
                        DataType::Int32 | DataType::Int64
                    ));
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &binding.selected.result_type
                    else {
                        panic!("scalar converter");
                    };
                    assert_eq!(result, &operand.value_type);
                    assert_eq!(result.nullable(), args[0].value_type.nullable(), "{sql}");
                    if operand_nullability[ordinal] {
                        assert!(
                            matches!(
                                args[0].kind,
                                ExprKind::Cast {
                                    target: DataType::Int32,
                                    ..
                                }
                            ),
                            "{sql}"
                        );
                    } else {
                        assert!(
                            matches!(args[0].kind, ExprKind::Literal(LiteralValue::Int(1))),
                            "{sql}"
                        );
                    }
                }
            }
            if sql.contains("CASE") {
                assert_eq!(
                    expression.value_type.logical_type(),
                    ValueLogicalType::Physical
                );
                assert_eq!(expression.value_type.data_type().clone(), DataType::Int32);
                assert!(expression.value_type.nullable());
            } else {
                assert_eq!(expression.value_type.data_type().clone(), DataType::Boolean);
                assert_eq!(expression.value_type.nullable(), result_nullable, "{sql}");
            }
        }
        let same = analyze_projection_expr_with_catalog(
            "select k=k from unary_input",
            &catalog,
            crate::functions::builtin_sql_function_catalog(),
        )
        .unwrap();
        for operand in comparison_operands(&same) {
            assert!(matches!(operand.kind, ExprKind::ColumnRef { .. }));
        }
    }

    #[test]
    fn comparison_null_targets_keep_largeint_identity_and_original_nullability() {
        use novarocks_type_contract::ValueLogicalType;
        let catalog = UnaryInputCatalog {
            key_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: Some(novarocks_types::schema::SqlType::LargeInt),
        };
        for sql in [
            "select k=NULL from unary_input",
            "select k=cast(NULL AS largeint) from unary_input",
            "select k IN (NULL,cast(NULL AS largeint)) from unary_input",
            "select k BETWEEN NULL AND cast(NULL AS largeint) from unary_input",
            "select CASE k WHEN NULL THEN 7 WHEN cast(NULL AS largeint) THEN 8 ELSE 9 END from unary_input",
        ] {
            let expression = analyze_projection_expr_with_catalog(
                sql,
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            let operands = comparison_operands(&expression);
            assert!(!operands[0].value_type.nullable(), "{sql}");
            for operand in operands.iter().skip(1) {
                assert_eq!(
                    operand.value_type.logical_type(),
                    ValueLogicalType::LargeInt,
                    "{sql}"
                );
                assert!(operand.value_type.nullable(), "{sql}");
                assert!(
                    matches!(operand.kind, ExprKind::Literal(LiteralValue::Null)),
                    "{sql}"
                );
            }
            assert!(expression.value_type.nullable(), "{sql}");
        }
        let expression = analyze_projection_expr_with_catalog(
            "select k=element_at([],1) from unary_input",
            &catalog,
            crate::functions::builtin_sql_function_catalog(),
        )
        .unwrap();
        let operands = comparison_operands(&expression);
        assert!(!operands[0].value_type.nullable());
        let ExprKind::FunctionCall { binding, args, .. } = &operands[1].kind else {
            panic!("nonliteral NULL must preserve its source invocation");
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/value_domain_conversion/v1"
        );
        assert!(matches!(args[0].kind, ExprKind::FunctionCall { .. }));
        assert_eq!(
            operands[1].value_type.logical_type(),
            ValueLogicalType::LargeInt
        );
        assert!(operands[1].value_type.nullable());
    }

    #[test]
    fn explicit_json_text_comparison_channels_invoke_exact_semantic_conversion() {
        use novarocks_type_contract::ValueLogicalType;
        let catalog = UnaryInputCatalog {
            key_type: DataType::Utf8,
            nullable: false,
            logical_type: Some(novarocks_types::schema::SqlType::Json),
        };
        for sql in [
            "select CAST(k AS VARCHAR)='text' from unary_input",
            "select CAST(k AS VARCHAR) IN ('text','other') from unary_input",
            "select CAST(k AS VARCHAR) BETWEEN 'a' AND 'z' from unary_input",
            "select CASE CAST(k AS VARCHAR) WHEN 'text' THEN 7 ELSE 9 END from unary_input",
        ] {
            let expression = analyze_projection_expr_with_catalog(
                sql,
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            let operands = comparison_operands(&expression);
            for (ordinal, operand) in operands.iter().enumerate() {
                assert_eq!(
                    operand.value_type.logical_type(),
                    ValueLogicalType::Physical,
                    "{sql}"
                );
                assert_eq!(
                    operand.value_type.data_type().clone(),
                    DataType::Utf8,
                    "{sql}"
                );
                // Explicit CAST declares a nullable result; literal labels
                // retain their original non-null declaration.
                assert_eq!(operand.value_type.nullable(), ordinal == 0, "{sql}");
            }
            let ExprKind::Cast {
                expr: original,
                cast_mode: Some(novarocks_type_contract::TrinoCastMode::Explicit),
                target: DataType::Utf8,
                ..
            } = &operands[0].kind
            else {
                panic!("expected explicit JSON to VARCHAR CAST: {sql}");
            };
            assert_eq!(original.value_type.logical_type(), ValueLogicalType::Json);
            assert!(matches!(original.kind, ExprKind::ColumnRef { .. }));
            for label in operands.iter().skip(1) {
                let ExprKind::Cast {
                    expr: original,
                    cast_mode: Some(novarocks_type_contract::TrinoCastMode::Implicit),
                    ..
                } = &label.kind
                else {
                    panic!("expected the selected comparison type for {sql}");
                };
                assert!(matches!(
                    original.kind,
                    ExprKind::Literal(LiteralValue::String(_))
                ));
                assert!(matches!(
                    original.value_type.value_type().kind(),
                    novarocks_type_contract::ValueKind::BoundedCharacter(_)
                ));
            }
        }
    }

    #[test]
    fn implicit_json_comparison_channels_reject_foreign_domains() {
        let catalog = UnaryInputCatalog {
            key_type: DataType::Utf8,
            nullable: false,
            logical_type: Some(novarocks_types::schema::SqlType::Json),
        };
        for sql in [
            "select k='text' from unary_input",
            "select k IN ('text','other') from unary_input",
            "select k BETWEEN 'a' AND 'z' from unary_input",
            "select CASE k WHEN 'text' THEN 7 ELSE 9 END from unary_input",
        ] {
            let error = analyze_projection_expr_with_catalog(
                sql,
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap_err();
            assert!(
                error.contains("Cannot apply operator: json"),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn strict_negation_requires_numeric_type_and_preserves_typed_null_operands() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        for sql in ["select -NULL", "select -element_at([],1)"] {
            let query = novarocks_parser::parser::parse_query(sql).unwrap();
            let error = super::super::analyze_with_function_catalog(
                &query,
                &EmptyCatalog,
                "default",
                crate::functions::builtin_sql_function_catalog(),
                crate::constant::test_constant_policy(),
                &control,
            )
            .expect_err("untyped negation operand");
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::AmbiguousFunctionCall
            );
            assert!(error.message().contains("ambiguously"), "{sql}: {error}");
        }

        let expected = novarocks_type_contract::FunctionValueType::new(DataType::Int16, true);
        for sql_operand in [
            "cast(NULL as smallint)",
            "cast(element_at([],1) as smallint)",
        ] {
            let original = analyze_projection_expr(&format!("select {sql_operand}")).unwrap();
            let expression = analyze_projection_expr(&format!("select -{sql_operand}")).unwrap();
            let operand = crate::common::negation::typed_operand(&expression)
                .expect("strict negation retains its exact binding and operand");
            assert_eq!(expression.value_type, expected);
            assert_eq!(operand.value_type, expected);
            assert!(
                crate::analysis::expr_identity::typed_expr_semantically_eq(
                    operand, &original, &control
                )
                .unwrap(),
                "{sql_operand}"
            );
        }
    }

    #[test]
    fn named_bitnot_resolves_unknown_null_without_erasing_a_nonliteral_argument() {
        use novarocks_type_contract::FunctionValueType;

        let expected = FunctionValueType::new(DataType::Int8, true);
        let literal = analyze_projection_expr("select bitnot(NULL)").unwrap();
        assert_eq!(literal.value_type, expected);
        let operand = named_bitnot_operand(&literal);
        assert_eq!(operand.value_type, expected);
        assert!(matches!(
            operand.kind,
            ExprKind::Literal(LiteralValue::Null)
        ));

        let nonliteral = analyze_projection_expr("select bitnot(element_at([],1))").unwrap();
        assert_eq!(nonliteral.value_type, expected);
        let operand = named_bitnot_operand(&nonliteral);
        assert_eq!(operand.value_type, expected);
        let ExprKind::Cast { expr, target, .. } = &operand.kind else {
            panic!("named bitnot must bind the Unknown argument at its selected integer width");
        };
        assert_eq!(target, &DataType::Int8);
        assert_eq!(
            expr.value_type,
            FunctionValueType::new(DataType::Null, true)
        );
        let ExprKind::FunctionCall { name, .. } = &expr.kind else {
            panic!("bitnot must keep the original nonliteral NULL argument");
        };
        assert_eq!(name, "element_at");
    }

    fn named_bitnot_operand(expression: &TypedExpr) -> &TypedExpr {
        let ExprKind::FunctionCall { name, args, .. } = &expression.kind else {
            panic!("expected analyzed scalar bitnot");
        };
        assert_eq!(name, "bitnot");
        let [operand] = args.as_slice() else {
            panic!("expected one bitnot argument");
        };
        operand
    }

    #[test]
    fn arithmetic_and_unary_do_not_infer_numeric_identity_from_fixed_binary() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        use std::{
            cell::{Cell, RefCell},
            collections::{HashMap, HashSet},
            rc::Rc,
        };
        let control = crate::compiler::SqlCompileControl::unbounded();
        let factory = Rc::new(RefCell::new(crate::column_id::ColumnRefFactory::new()));
        let context = super::super::AnalyzerContext {
            purpose: super::super::QueryAnalysisPurpose::Ordinary,
            grouping_scope: std::cell::RefCell::new(None),
            materialized_values: None,
            table_sequence: None,
            catalog: &EmptyCatalog,
            current_database: "default",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            control: &control,
            constant_policy: crate::constant::test_constant_policy(),
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
            factory,
            view_stack: Default::default(),
            ctes: HashMap::new(),
            pending_ctes: HashSet::new(),
            next_subquery_id: Cell::new(0),
            collected_subqueries: RefCell::new(Vec::new()),
            cte_registry: RefCell::new(crate::analysis::cte::CTERegistry::new()),
        };
        for logical_type in [ValueLogicalType::Physical, ValueLogicalType::Uuid] {
            let mut scope = context.new_scope();
            scope.add_column(
                None,
                "raw",
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    logical_type,
                )
                .unwrap(),
            );
            for sql in [
                "select raw+1",
                "select raw+NULL",
                "select -raw",
                "select bitnot(raw)",
            ] {
                let query = &novarocks_parser::parser::parse_query(sql).unwrap();
                let ast::SetExpr::Select(select) = query.body.as_ref() else {
                    panic!("select")
                };
                let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
                    panic!("expression")
                };
                assert!(projection_column.alias.is_none(), "anonymous projection");
                let expr = &projection_column.expression;
                let error = context.analyze_expr(expr, &scope).unwrap_err();
                assert!(
                    matches!(
                        error.kind(),
                        crate::analyze_error::AnalyzeErrorKind::TypeMismatch
                    ),
                    "{sql}: {error}"
                );
            }
        }
        let catalog = UnaryInputCatalog {
            key_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: Some(novarocks_types::schema::SqlType::LargeInt),
        };
        let registry = crate::functions::builtin_sql_function_catalog();
        let negate =
            analyze_projection_expr_with_catalog("select -k from unary_input", &catalog, registry)
                .unwrap();
        assert_eq!(negate.value_type.logical_type(), ValueLogicalType::LargeInt);
        assert!(!negate.value_type.nullable());

        let bitnot = analyze_projection_expr_with_catalog(
            "select bitnot(k) from unary_input",
            &catalog,
            registry,
        )
        .unwrap();
        assert_eq!(bitnot.value_type.logical_type(), ValueLogicalType::LargeInt);
        assert_eq!(
            bitnot.value_type.data_type().clone(),
            DataType::FixedSizeBinary(16)
        );
        assert!(bitnot.value_type.nullable());
        let operand = named_bitnot_operand(&bitnot);
        assert!(matches!(operand.kind, ExprKind::ColumnRef { .. }));
        assert_eq!(operand.value_type, negate.value_type);
    }

    #[test]
    fn signed_unary_plus_preserves_literals_nulls_and_real_negate_effects() {
        for (sql, carrier) in [
            ("SELECT +1", DataType::Int32),
            ("SELECT +2147483648", DataType::Int64),
            ("SELECT +(-2147483648)", DataType::Int32),
            ("SELECT +CAST(7 AS TINYINT)", DataType::Int8),
            ("SELECT +CAST(7 AS SMALLINT)", DataType::Int16),
            ("SELECT +CAST(NULL AS INTEGER)", DataType::Int32),
            ("SELECT +CAST(NULL AS BIGINT)", DataType::Int64),
        ] {
            let expected =
                analyze_projection_expr(&sql.replacen("SELECT +", "SELECT ", 1)).unwrap();
            let actual = analyze_projection_expr(sql).unwrap();
            assert_eq!(actual.value_type, expected.value_type, "{sql}");
            assert_eq!(actual.value_type.data_type(), &carrier, "{sql}");
            assert!(
                crate::analysis::expr_identity::typed_expr_semantically_eq(
                    &actual,
                    &expected,
                    &crate::compiler::SqlCompileControl::unbounded()
                )
                .unwrap(),
                "original expression identity: {sql}"
            );
        }
        let expression = analyze_projection_expr("SELECT + - -2147483648").unwrap();
        assert_eq!(expression.value_type.data_type(), &DataType::Int32);
        assert!(
            matches!(
                crate::common::negation::typed_operand(&expression).map(|operand| &operand.kind),
                Some(ExprKind::Literal(LiteralValue::Int(-2147483648)))
            ),
            "positive identity cannot erase the original INTEGER negate overflow"
        );
        assert_eq!(super::signed_int_literal_value(&expression), None);
    }

    #[test]
    fn signed_unary_plus_keeps_column_ids_and_anonymous_frozen_output_names() {
        for carrier in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for nullable in [false, true] {
                let catalog = UnaryInputCatalog {
                    key_type: carrier.clone(),
                    nullable,
                    logical_type: None,
                };
                let query = &novarocks_parser::parser::parse_query("SELECT k, +k FROM unary_input")
                    .unwrap();
                let (resolved, ctes, mut factory) =
                    super::super::analyze(query, &catalog, "default").unwrap();
                let fields = resolved.output_columns.clone();
                assert_eq!(fields[0].field_name.as_deref(), Some("k"));
                assert_eq!(
                    fields[1].field_name, None,
                    "AST positive expression remains anonymous"
                );
                let QueryBody::Select(select) = &resolved.body else {
                    panic!("select");
                };
                let ExprKind::ColumnRef {
                    column_id: first, ..
                } = select.projection[0].expr.kind
                else {
                    panic!("original column");
                };
                let ExprKind::ColumnRef {
                    column_id: positive,
                    ..
                } = select.projection[1].expr.kind
                else {
                    panic!("identity keeps original reference");
                };
                assert_eq!(first, positive);
                assert_eq!(
                    select.projection[0].expr.value_type,
                    select.projection[1].expr.value_type
                );
                assert_eq!(fields[1].value_type.data_type(), &carrier);
                assert_eq!(fields[1].value_type.nullable(), nullable);
                let plan = crate::planner::plan_query(
                    resolved,
                    ctes,
                    &mut factory,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
                let columns = crate::planner::plan_output_columns(&plan).unwrap();
                let root = crate::compiler::root_output::RootOutputSemantics::bind_analyzed(
                    &fields, &columns, &factory,
                )
                .unwrap();
                assert_eq!(
                    root.field_names(&columns).unwrap(),
                    &[Some("k".to_owned()), None]
                );
                let query = &novarocks_parser::parser::parse_query(
                    "SELECT k FROM (SELECT +k FROM unary_input) p",
                )
                .unwrap();
                let error = super::super::analyze(query, &catalog, "default").unwrap_err();
                assert_eq!(
                    error.kind(),
                    crate::analyze_error::AnalyzeErrorKind::UnknownColumn,
                    "source identity cannot grant the anonymous output a field name"
                );
            }
        }
    }

    #[test]
    fn signed_unary_plus_retains_admitted_constant_creator_field_and_selected_ordinal() {
        use arrow::array::Array;
        let value_type = novarocks_type_contract::FunctionValueType::new(DataType::Int32, true);
        let control = crate::compiler::SqlCompileControl::unbounded();
        let pool = novarocks_constant_contract::ConstantPool::try_new(
            std::sync::Arc::new(value_type.try_to_field("original_positive").unwrap()),
            value_type.clone(),
            arrow::array::Int32Array::from(vec![None, Some(7), None]).to_data(),
            crate::constant::test_constant_policy(),
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            &control,
        )
        .unwrap();
        let query = &novarocks_parser::parser::parse_query("SELECT +1").unwrap();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select");
        };
        let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("expression");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let syntax = &projection_column.expression;
        for ordinal in [1, 2] {
            let original = pool.value(ordinal).unwrap();
            let source = TypedExpr {
                kind: ExprKind::Constant(original.clone()),
                value_type: value_type.clone(),
            };
            let (_, mut operands) = super::super::expression_typing::type_value_node_with_catalog(
                crate::functions::builtin_sql_function_catalog(),
                novarocks_expression::typing::NodeShape::UnaryPlus,
                vec![source],
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                crate::constant::test_constant_policy(),
                &control,
                syntax.span(),
            )
            .unwrap();
            let actual = operands.pop().expect("original unary operand");
            assert_eq!(
                actual.value_type, value_type,
                "do not strengthen the original NULL promise"
            );
            let ExprKind::Constant(value) = actual.kind else {
                panic!("original CV, no new pool or cast");
            };
            assert_eq!(value.ordinal(), ordinal);
            assert!(std::sync::Arc::ptr_eq(
                value.pool().array(),
                original.pool().array()
            ));
            assert!(std::sync::Arc::ptr_eq(
                value.pool().field_ref(),
                original.pool().field_ref()
            ));
        }
    }

    #[test]
    fn unary_plus_rejects_unsupported_domains() {
        for sql in ["SELECT +1.5", "SELECT +'x'", "SELECT +true", "SELECT +NULL"] {
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            let error = super::super::analyze(query, &EmptyCatalog, "default").unwrap_err();
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::TypeMismatch,
                "{sql}"
            );
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("select");
            };
            assert_eq!(
                error.span(),
                Some(select.projection[0].span()),
                "original unary source span"
            );
        }
        let sql = "SELECT +CAST(1 AS LARGEINT)";
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        let error = super::super::analyze(&query, &EmptyCatalog, "default").unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
        let span = error.span().expect("retired type's source span");
        assert_eq!(&sql[span.start()..span.end()], "LARGEINT");

        let catalog = UnaryInputCatalog {
            key_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: None,
        };
        assert!(
            analyze_projection_expr_with_catalog(
                "SELECT +k FROM unary_input",
                &catalog,
                crate::functions::builtin_sql_function_catalog()
            )
            .unwrap_err()
            .contains("unary plus requires")
        );
    }

    #[test]
    fn strict_unary_typed_null_and_nonnull_literal_controls() {
        for (sql, data_type, nullable) in [
            (
                "select -cast(null as decimal(18,9))",
                DataType::Decimal128(18, 9),
                true,
            ),
            ("select bitnot(cast(null as bigint))", DataType::Int64, true),
            ("select -1.25", DataType::Decimal128(3, 2), false),
            ("select -1.25e2", DataType::Float64, false),
            ("select bitnot(1)", DataType::Int32, true),
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            assert_eq!(
                expression.value_type.data_type().clone(),
                data_type,
                "{sql}"
            );
            assert_eq!(expression.value_type.nullable(), nullable, "{sql}");
        }
    }

    #[test]
    fn strict_unary_decimal_key_is_nullable_in_exact_aggregate_channels() {
        let key_type = DataType::Decimal128(18, 9);
        for nullable in [false, true] {
            let catalog = UnaryInputCatalog {
                key_type: key_type.clone(),
                nullable,
                logical_type: None,
            };
            for sql in [
                "select max_by(v,-k) from unary_input",
                "select min_by(v,-k) from unary_input",
                "select array_agg(v order by -k) from unary_input",
            ] {
                let expression = analyze_projection_expr_with_catalog(
                    sql,
                    &catalog,
                    crate::functions::builtin_sql_function_catalog(),
                )
                .unwrap();
                let ExprKind::AggregateCall {
                    args,
                    order_by,
                    resolved,
                    ..
                } = expression.kind
                else {
                    panic!("expected aggregate");
                };
                let key = if order_by.is_empty() {
                    &args[1]
                } else {
                    &order_by[0].expr
                };
                assert_eq!(key.value_type.data_type().clone(), key_type, "{sql}");
                assert_eq!(key.value_type.nullable(), nullable, "{sql}");
                let novarocks_functions::FunctionArgumentType::Value(bound) =
                    &resolved.selected.argument_types[1]
                else {
                    panic!("expected scalar key channel");
                };
                assert_eq!(bound.data_type().clone(), key_type, "{sql}");
                assert_eq!(bound.nullable(), nullable, "{sql}");
            }
        }
    }

    #[test]
    fn signed_integer_literal_boundaries_preserve_exact_values_and_types() {
        use crate::analysis::{ExprKind, LiteralValue};

        for value in [i64::MIN, i64::MAX] {
            let expr = analyze_projection_expr(&format!("select {value}"))
                .expect("signed BIGINT boundary should analyze");
            assert_eq!(expr.value_type.data_type().clone(), DataType::Int64);
            assert!(!expr.value_type.nullable());
            assert!(
                matches!(expr.kind, ExprKind::Literal(LiteralValue::Int(actual)) if actual == value)
            );
        }

        for value in [
            i128::MIN,
            i128::MAX,
            i64::MIN as i128 - 1,
            i64::MAX as i128 + 1,
        ] {
            let expr = analyze_projection_expr(&format!("select cast('{value}' as largeint)"))
                .expect("explicit signed LARGEINT boundary should analyze");
            assert_eq!(
                expr.value_type.data_type().clone(),
                DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
            );
            assert!(
                expr.value_type.nullable(),
                "explicit text CAST keeps the original parse-failure NULL law"
            );
            let ExprKind::FunctionCall { binding, args, .. } = expr.kind else {
                panic!("exact LARGEINT domain converter");
            };
            assert_eq!(
                binding.function_id.as_str(),
                "builtin.scalar/value_domain_conversion/v1"
            );
            assert!(
                matches!(&args[0].kind, ExprKind::Literal(LiteralValue::String(text)) if text == &value.to_string())
            );
        }
    }

    #[test]
    fn ordinary_integer_boundaries_are_shared_by_arrays_and_typeof() {
        for (literal, expected) in [
            ("-2147483648", DataType::Int32),
            ("2147483647", DataType::Int32),
            ("2147483648", DataType::Int64),
            ("-2147483649", DataType::Int64),
            ("-(2147483648)", DataType::Int64),
            ("-(-2147483648)", DataType::Int32),
        ] {
            let expression = analyze_projection_expr(&format!("select {literal}")).unwrap();
            assert_eq!(expression.value_type.data_type(), &expected, "{literal}");
            let array = analyze_projection_expr(&format!("select [{literal}]")).unwrap();
            let DataType::List(item) = array.value_type.data_type() else {
                panic!("list");
            };
            assert_eq!(item.data_type(), &expected, "{literal}");
            let typeof_expression =
                analyze_projection_expr(&format!("select typeof({literal})")).unwrap();
            let ExprKind::Literal(LiteralValue::String(name)) = typeof_expression.kind else {
                panic!("type name");
            };
            assert_eq!(
                name,
                if expected == DataType::Int32 {
                    "int"
                } else {
                    "bigint"
                }
            );
        }
        for literal in ["9223372036854775808", "-9223372036854775809"] {
            assert!(novarocks_parser::parser::parse_query(&format!("select {literal}")).is_err());
        }
    }

    #[test]
    fn sql_decimal_type_producer_freezes_standard_cast_before_explicit_string_output() {
        use crate::analysis::ExprKind;
        let expression = analyze_projection_expr("SELECT CAST(CAST('100000000000000000000000000000000000.00' AS DECIMAL(38,2)) AS VARCHAR)").unwrap();
        assert_eq!(expression.value_type.data_type().clone(), DataType::Utf8);
        let ExprKind::Cast {
            expr: decimal,
            target: DataType::Utf8,
            ..
        } = &expression.kind
        else {
            panic!("expected explicit string output cast");
        };
        assert_eq!(
            decimal.value_type.data_type().clone(),
            DataType::Decimal128(38, 2)
        );
        assert!(matches!(
            decimal.kind,
            ExprKind::Cast {
                target: DataType::Decimal128(38, 2),
                ..
            }
        ));
        let expression =
            analyze_projection_expr("SELECT array_agg(CAST('1.00' AS DECIMAL(38,2)))").unwrap();
        let ExprKind::AggregateCall { args, resolved, .. } = expression.kind else {
            panic!("expected aggregate");
        };
        assert_eq!(
            args[0].value_type.data_type().clone(),
            DataType::Decimal128(38, 2)
        );
        let novarocks_functions::FunctionArgumentType::Value(bound) =
            &resolved.selected.argument_types[0]
        else {
            panic!("expected bound value argument");
        };
        assert_eq!(bound.data_type().clone(), DataType::Decimal128(38, 2));
        assert_eq!(bound.nullable(), args[0].value_type.nullable());
    }

    #[test]
    fn negative_decimal_and_exponent_literals_keep_existing_numeric_types() {
        use crate::analysis::{ExprKind, LiteralValue};

        let decimal = analyze_projection_expr("select -123456789.123456789")
            .expect("negative decimal should analyze");
        assert_eq!(
            decimal.value_type.data_type().clone(),
            DataType::Decimal128(18, 9)
        );
        assert!(matches!(
            decimal.kind,
            ExprKind::Literal(LiteralValue::Decimal(ref value))
                if value == "-123456789.123456789"
        ));

        for sql in ["select -1.25e2", "select -1.25E2"] {
            let exponent = analyze_projection_expr(sql).expect("negative exponent should analyze");
            assert_eq!(exponent.value_type.data_type().clone(), DataType::Float64);
            assert!(matches!(
                exponent.kind,
                ExprKind::Literal(LiteralValue::Float(value)) if value == -125.0
            ));
        }
    }

    #[test]
    fn hidden_aggregate_window_uses_user_visibility_error_before_capability_check() {
        let mut builder = novarocks_functions::EngineFunctionCatalogBuilder::new();
        let overload = novarocks_functions::AggregateOverloadMetadata::try_new(
            "iceberg/theta-stat/int64/v1",
            [DataType::Int64],
            DataType::Binary,
            DataType::Binary,
            "iceberg/theta-compact/v1",
        )
        .expect("hidden aggregate overload should be valid");
        let definition = novarocks_functions::FunctionDefinition::try_new_exact_aggregate(
            "hidden_theta_stat_fixture",
            novarocks_functions::FunctionVisibility::Hidden,
            novarocks_functions::FunctionVolatility::Immutable,
            [overload],
        )
        .expect("hidden aggregate definition should be valid");
        builder
            .register(definition)
            .expect("hidden aggregate should register");
        let catalog = builder.seal().expect("test catalog should seal");

        let error = analyze_projection_expr_with_function_catalog(
            "select hidden_theta_stat_fixture(1) over ()",
            &catalog,
        )
        .expect_err("hidden aggregate must not be admitted as a window function");

        assert!(error.contains("Unknown function"), "{error}");
        assert!(!error.contains("aggregate function"));
    }

    #[test]
    fn scalar_and_unknown_over_errors_remain_distinct() {
        let scalar = analyze_projection_expr("select abs(1) over ()")
            .expect_err("known scalar OVER must be rejected");
        assert!(scalar.contains("function abs is not supported with OVER clause"));

        let unknown = analyze_projection_expr("select sqlp8_unknown(1) over ()")
            .expect_err("unknown function OVER must remain unknown");
        assert!(unknown.contains("Unknown function: sqlp8_unknown"));
    }

    #[test]
    fn every_normalizes_to_the_executable_bool_and_aggregate() {
        let expr = analyze_projection_expr("select every(true)").expect("EVERY should analyze");
        let crate::analysis::ExprKind::AggregateCall { name, .. } = expr.kind else {
            panic!("expected aggregate call, got {:?}", expr.kind);
        };
        assert_eq!(name, "bool_and");
        assert_eq!(expr.value_type.data_type().clone(), DataType::Boolean);
    }

    #[test]
    fn sqlx1_function_array_sort_lambda_rejects_extended_clock_functions() {
        // array_sort validates its lambda body through this same helper.  The
        // seven entries below were missing from the old analyzer-local list
        // but were already unsafe for optimizer rewrites.
        for name in [
            "current_date",
            "curdate",
            "curtime",
            "localtime",
            "localtimestamp",
        ] {
            let body = crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::FunctionCall {
                    binding: crate::analysis::test_function_binding(
                        name,
                        &[],
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        false,
                        crate::functions::builtin_function_volatility(name),
                    ),
                    volatility: crate::functions::builtin_function_volatility(name),
                    name: name.to_string(),
                    args: (vec![]).into(),
                    distinct: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    false,
                ),
            };
            assert!(
                super::typed_expr_contains_nondeterministic_call(
                    crate::functions::builtin_sql_function_catalog(),
                    &body,
                ),
                "{name} must be rejected in array_sort lambda bodies"
            );
        }
    }

    fn analyze_manually_constructed_scalar_function(
        name: &str,
        sql: &str,
    ) -> Result<crate::analysis::TypedExpr, String> {
        let query =
            &mut novarocks_parser::parser::parse_query(sql).map_err(|error| error.to_string())?;
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            return Err("expected select".to_string());
        };
        let Some(ast::SelectItem::SingleColumn(projection_column)) = select.projection.first_mut()
        else {
            return Err("expected ordinary scalar function".to_string());
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let ast::Expr::FunctionCall(func) = &mut projection_column.expression else {
            return Err("expected ordinary scalar function".to_string());
        };
        let Some(last_name_part) = func.name.parts.last_mut() else {
            return Err("expected scalar function name".to_string());
        };
        last_name_part.value = name.to_string();

        let (resolved, _registry, _factory) =
            analyze(query, &EmptyCatalog, "default").map_err(|error| error.to_string())?;
        let QueryBody::Select(select) = resolved.body else {
            return Err("expected select".to_string());
        };
        select
            .projection
            .into_iter()
            .next()
            .map(|item| item.expr)
            .ok_or_else(|| "expected projection".to_string())
    }

    fn assert_substring_int32_arguments(sql: &str, expected_name: &str, expected_arity: usize) {
        let expr = analyze_projection_expr(sql).expect("substring should analyze");
        let crate::analysis::ExprKind::FunctionCall { name, args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        assert_eq!(name, expected_name);
        assert_eq!(args.len(), expected_arity);
        assert_eq!(args[0].value_type.data_type().clone(), DataType::Utf8);
        for arg in &args[1..] {
            assert_eq!(arg.value_type.data_type().clone(), DataType::Int32);
        }
    }

    #[test]
    fn substring_function_syntax_binds_integer_literals_to_int32() {
        for (sql, values) in [
            ("select substring('STARROCKS', 2, 3)", &[2, 3][..]),
            ("select substring('x', 2147483647)", &[2147483647][..]),
            ("select substring('x', -2147483648)", &[-2147483648][..]),
        ] {
            let expr = analyze_projection_expr(sql).expect("in-range literals should analyze");
            let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
                panic!("expected FunctionCall, got {:?}", expr.kind);
            };
            assert_eq!(args.len(), values.len() + 1);
            for (arg, expected) in args.iter().skip(1).zip(values) {
                assert_eq!(arg.value_type.data_type().clone(), DataType::Int32);
                assert!(!arg.value_type.nullable());
                assert!(matches!(
                    arg.kind,
                    crate::analysis::ExprKind::Literal(
                        crate::analysis::LiteralValue::Int(value)
                    ) if value == *expected
                ));
            }
        }
    }

    #[test]
    fn substring_normal_and_special_syntax_bind_identical_arguments() {
        use novarocks_type_contract::ValueKind;

        let ordinary = analyze_projection_expr("SELECT substring('x', 2, 3)").unwrap();
        let special = analyze_projection_expr("SELECT substring('x' FROM 2 FOR 3)").unwrap();
        assert_eq!(ordinary.value_type, special.value_type);
        for (expression, expected_name) in [(ordinary, "substring"), (special, "substr")] {
            let ExprKind::FunctionCall { name, args, .. } = &expression.kind else {
                panic!("expected the selected substring overload");
            };
            assert_eq!(name, expected_name);
            assert_eq!(args.len(), 3);
            assert_eq!(
                args[0].value_type.value_type().kind(),
                &ValueKind::String,
                "{name}"
            );
            assert!(!args[0].value_type.nullable());
            assert!(
                matches!(&args[0].kind, ExprKind::Literal(LiteralValue::String(value)) if value == "x")
            );
            for (argument, expected) in args.iter().skip(1).zip([2, 3]) {
                assert_eq!(argument.value_type.data_type(), &DataType::Int32);
                assert!(!argument.value_type.nullable());
                assert!(
                    matches!(argument.kind, ExprKind::Literal(LiteralValue::Int(value)) if value == expected)
                );
            }
        }
    }

    #[test]
    fn substring_special_syntax_uses_the_same_binding() {
        assert_substring_int32_arguments("select substring('STARROCKS' from 2 for 3)", "substr", 3);
        assert_substring_int32_arguments("select substring('STARROCKS' from 2)", "substr", 2);
    }

    #[test]
    fn substring_and_substr_reject_wrong_arity_during_analysis() {
        for name in ["substring", "substr"] {
            let err = analyze_projection_expr(&format!("select {name}('x')"))
                .expect_err("wrong arity must fail during analysis");
            assert!(
                err.contains("no matching declared overload"),
                "{name}: {err}"
            );
        }
    }

    #[test]
    fn unregistered_scalar_function_is_an_analyze_error() {
        let query = &novarocks_parser::parser::parse_query("select sqlp7_not_a_function(1)")
            .expect("test query should parse");

        let error = analyze(query, &EmptyCatalog, "default")
            .expect_err("an unregistered scalar function must fail during analysis");
        assert_eq!(error.code().as_str(), "sql.analyze.unknown_function");
        assert!(
            error.span().is_some(),
            "function AST span must be preserved"
        );
    }

    #[test]
    fn ordinary_function_ast_enforces_substring_arity_binding() {
        let err = analyze_manually_constructed_scalar_function("substring", "select concat('x')")
            .expect_err("ordinary Expr::Function must reach the scalar binder");
        assert!(err.contains("no matching declared overload"), "{err}");
    }

    #[test]
    fn substring_keeps_explicit_integer_casts_for_out_of_range_positions() {
        use novarocks_type_contract::TrinoCastMode;

        for (sql, value) in [
            (
                "SELECT substring('x', CAST(2147483648 AS INTEGER))",
                2147483648,
            ),
            (
                "SELECT substring('x' FROM CAST(-2147483649 AS INTEGER) FOR 1)",
                -2147483649,
            ),
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            let ExprKind::FunctionCall { args, .. } = &expression.kind else {
                panic!("expected substring: {sql}");
            };
            let ExprKind::Cast {
                expr: original,
                cast_mode: Some(TrinoCastMode::Explicit),
                target: DataType::Int32,
                ..
            } = &args[1].kind
            else {
                panic!("expected explicit CAST to INTEGER: {sql}");
            };
            assert_eq!(original.value_type.data_type(), &DataType::Int64);
            assert!(
                matches!(original.kind, ExprKind::Literal(LiteralValue::Int(actual)) if actual == value)
            );
            assert!(!args[1].value_type.nullable());
        }
    }

    #[test]
    fn scalar_binder_never_inserts_implicit_narrowing_for_substring() {
        use crate::analyze_error::AnalyzeErrorKind;
        use novarocks_type_contract::FunctionValueType;

        let error = super::bind_scalar_function_call(
            "substring",
            vec![
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String("x".to_string())),
                    value_type: FunctionValueType::new(DataType::Utf8, false),
                },
                TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: crate::column_id::ColumnId(42),
                        qualifier: None,
                        column: "offset".to_string(),
                    },
                    value_type: FunctionValueType::new(DataType::Int64, false),
                },
            ],
        )
        .err()
        .expect("the declared INTEGER overload must not implicitly narrow BIGINT");
        assert_eq!(error.kind(), AnalyzeErrorKind::UnknownFunction);
        assert_eq!(error.message(), "no matching declared overload");
    }

    #[test]
    fn substring_keeps_the_explicit_integer_cast_of_a_bigint_column() {
        let query = &novarocks_parser::parser::parse_query(
            "SELECT substring('x', CAST(offset AS INTEGER)) FROM offsets",
        )
        .expect("substring query should parse");
        let (resolved, _registry, _factory) = analyze(query, &BigintOffsetCatalog, "default")
            .expect("the explicit CAST supplies the declared INTEGER parameter");
        let QueryBody::Select(select) = resolved.body else {
            panic!("expected select");
        };
        let expr = select
            .projection
            .into_iter()
            .next()
            .expect("expected projection")
            .expr;
        let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        let offset = args.into_iter().nth(1).expect("expected offset argument");
        assert_eq!(offset.value_type.data_type().clone(), DataType::Int32);
        assert!(!offset.value_type.nullable());
        let crate::analysis::ExprKind::Cast {
            expr,
            target: DataType::Int32,
            cast_mode: Some(novarocks_type_contract::TrinoCastMode::Explicit),
            ..
        } = offset.kind
        else {
            panic!("expected BIGINT offset runtime cast");
        };
        assert!(
            !expr.value_type.nullable(),
            "test setup must use a genuinely non-null BIGINT ColumnRef"
        );
        assert!(matches!(
            expr.kind,
            crate::analysis::ExprKind::ColumnRef { .. }
        ));
    }

    #[test]
    fn group_concat_and_string_agg_keep_implicit_utf8_coercion() {
        for name in ["group_concat", "string_agg"] {
            let expr = analyze_projection_expr(&format!("select {name}(1, ',')"))
                .expect("aggregate should preserve implicit string coercion");
            let crate::analysis::ExprKind::AggregateCall { args, .. } = expr.kind else {
                panic!("expected AggregateCall, got {:?}", expr.kind);
            };
            assert!(!args.is_empty());
            assert!(
                args.iter()
                    .all(|arg| *arg.value_type.data_type() == DataType::Utf8)
            );
            assert!(args.iter().all(|arg| matches!(
                arg.kind,
                crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::String(_))
                    | crate::analysis::ExprKind::Cast {
                        target: DataType::Utf8,
                        ..
                    }
            )));
        }
    }

    #[test]
    fn canonical_concat_zero_arguments_keep_the_original_analysis_refusal() {
        for sql in ["SELECT group_concat()", "SELECT string_agg()"] {
            let error = analyze_projection_expr(sql).expect_err("zero value arguments are invalid");
            assert!(
                error.contains("group_concat should have at least one input."),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn ordered_aggregate_binding_freezes_every_update_channel() {
        let expr = analyze_projection_expr(
            "select string_agg('v' \n,\n '|'\norder by 1, cast(7 as bigint) \n)",
        )
        .expect("ordered group_concat should analyze");
        let crate::analysis::ExprKind::AggregateCall {
            args,
            order_by,
            resolved,
            ..
        } = expr.kind
        else {
            panic!("expected AggregateCall, got {:?}", expr.kind);
        };
        assert_eq!(args.len(), 2, "value and separator remain logical args");
        assert_eq!(order_by.len(), 2);
        let argument_types = resolved
            .selected
            .argument_types
            .iter()
            .map(|argument| match argument {
                novarocks_functions::FunctionArgumentType::Value(value) => {
                    value.data_type().clone()
                }
                novarocks_functions::FunctionArgumentType::Lambda { .. } => {
                    panic!("aggregate update arguments cannot be lambdas")
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            argument_types,
            [
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Int64,
            ]
        );
        let DataType::Struct(fields) = &crate::functions::aggregate_selection(&resolved)
            .intermediate_type
            .data_type()
        else {
            panic!("expected Struct intermediate for ordered group_concat");
        };
        assert_eq!(fields.len(), 4);
        for (field, input_type) in fields.iter().zip(argument_types.iter()) {
            let DataType::List(item) = field.data_type() else {
                panic!("expected List field in group_concat intermediate");
            };
            assert_eq!(item.data_type(), input_type);
        }
    }

    #[test]
    fn generic_fixed_offset_binding_uses_the_original_checked_literal_normalization() {
        let expression = analyze_projection_expr("SELECT lead('a', 1, 'b') OVER ()").unwrap();
        let ExprKind::WindowCall { args, binding, .. } = expression.kind else {
            panic!("window");
        };
        assert_eq!(args[1].value_type.data_type(), &DataType::Int64);
        assert!(!args[1].value_type.nullable());
        // The existing syntax-literal author normalizes a checked value before
        // its first canonical CV admission; already-admitted ConstantValue and
        // nonliteral inputs never enter signed_int_literal_value.
        assert!(matches!(
            args[1].kind,
            ExprKind::Literal(LiteralValue::Int(1))
        ));
        assert_eq!(
            binding.resolved().selected.argument_types[1],
            novarocks_functions::FunctionArgumentType::Value(args[1].value_type.clone())
        );
        // An already-admitted CV follows the separate runtime-cast path.
        // Keep its original creator's Field, array, and selected ordinal.
        use arrow::array::Array;
        let control = crate::compiler::SqlCompileControl::unbounded();
        let policy = crate::constant::test_constant_policy();
        let source_type = novarocks_type_contract::FunctionValueType::new(DataType::Int32, false);
        let pool = novarocks_constant_contract::ConstantPool::try_new(
            std::sync::Arc::new(source_type.try_to_field("original").unwrap()),
            source_type.clone(),
            arrow::array::Int32Array::from(vec![99, 7]).to_data(),
            policy,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            &control,
        )
        .unwrap();
        let original = pool.value(1).unwrap();
        let source = TypedExpr {
            kind: ExprKind::Constant(original.clone()),
            value_type: source_type.clone(),
        };
        let target = novarocks_type_contract::FunctionValueType::new(DataType::Int64, false);
        let converted = super::coerce_selected_function_argument(
            crate::functions::builtin_sql_function_catalog(),
            source,
            &target,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            policy,
            &control,
        )
        .unwrap();
        assert_eq!(converted.value_type, target);
        let ExprKind::Cast { expr: source, .. } = converted.kind else {
            panic!("admitted CV needs an actual runtime cast");
        };
        assert_eq!(source.value_type, source_type);
        let ExprKind::Constant(actual) = source.into_inner().kind else {
            panic!("original admitted constant");
        };
        assert_eq!(actual.ordinal(), 1);
        assert!(std::sync::Arc::ptr_eq(
            actual.pool().array(),
            original.pool().array()
        ));
        assert!(std::sync::Arc::ptr_eq(
            actual.pool().field_ref(),
            original.pool().field_ref()
        ));
    }

    #[test]
    fn fixed_wide_parameters_do_not_erase_original_unary_integer_overflow() {
        let safe = analyze_projection_expr("SELECT lead('a', - -1, 'b') OVER ()").unwrap();
        let ExprKind::WindowCall { args, .. } = safe.kind else {
            panic!("window");
        };
        assert!(
            matches!(args[1].kind, ExprKind::Literal(LiteralValue::Int(1))),
            "existing safe normalization remains"
        );
        assert_eq!(args[1].value_type.data_type(), &DataType::Int64);
        for sql in [
            "SELECT lead('a', - -2147483648, 'b') OVER ()",
            "SELECT lead('a', -(-2147483648), 'b') OVER ()",
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            let ExprKind::WindowCall { args, .. } = expression.kind else {
                panic!("window");
            };
            let ExprKind::Cast {
                expr: source,
                target,
                ..
            } = &args[1].kind
            else {
                panic!("runtime conversion keeps the original operation");
            };
            assert_eq!(target, &DataType::Int64);
            assert_eq!(source.value_type.data_type(), &DataType::Int32);
            let inner =
                crate::common::negation::typed_operand(source).expect("original numeric negation");
            let literal = if let ExprKind::Nested(inner) = &inner.kind {
                inner.as_ref()
            } else {
                inner
            };
            assert!(matches!(
                literal.kind,
                ExprKind::Literal(LiteralValue::Int(-2147483648))
            ));
            assert_eq!(
                super::signed_int_literal_value(source),
                None,
                "the original INTEGER overflow cannot become a BIGINT constant"
            );
        }
        for (carrier, minimum) in [
            (DataType::Int8, i64::from(i8::MIN)),
            (DataType::Int16, i64::from(i16::MIN)),
            (DataType::Int32, i64::from(i32::MIN)),
            (DataType::Int64, i64::MIN),
        ] {
            let literal = TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(minimum)),
                value_type: novarocks_type_contract::FunctionValueType::new(carrier.clone(), false),
            };
            let unary = crate::analyzer::negation_for_test(
                literal,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
            assert_eq!(super::signed_int_literal_value(&unary), None);
        }
    }

    #[test]
    fn substring_does_not_constant_fold_arithmetic_for_literal_range_checking() {
        let expr = analyze_projection_expr("select substring('x', 2147483647 + 1)")
            .expect("arithmetic expression should retain its runtime INTEGER arithmetic");
        let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        assert_eq!(args[1].value_type.data_type().clone(), DataType::Int32);
        let (operator, [left, right]) =
            crate::common::binary_operator::typed_operands(&args[1]).expect("runtime arithmetic");
        assert_eq!(operator, novarocks_type_contract::OperatorFunction::Add);
        assert_eq!(left.value_type.data_type(), &DataType::Int32);
        assert_eq!(right.value_type.data_type(), &DataType::Int32);
        assert!(matches!(
            left.kind,
            crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(2147483647))
        ));
        assert!(matches!(
            right.kind,
            crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(1))
        ));
    }

    #[test]
    fn substring_null_offset_binds_to_nullable_int32() {
        for sql in [
            "select substring('x', NULL)",
            "select substring('x' FROM NULL)",
        ] {
            let expr =
                analyze_projection_expr(sql).expect("NULL should bind to the signature target");
            let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
                panic!("expected FunctionCall, got {:?}", expr.kind);
            };
            assert_eq!(args[1].value_type.data_type().clone(), DataType::Int32);
            assert!(args[1].value_type.nullable());
        }
    }

    #[test]
    fn col_op_col_numeric_comparison_coerced_to_common_type() {
        // Int32 vs Int64 comparison: the analyzer should coerce BOTH operands to
        // Int64 (StarRocks ImplicitCastRule analog) before emitting the
        // comparison, so a mixed-type equi-join key carries matching types into
        // runtime-filter planning. (casts stand in for differently-typed columns,
        // since the test catalog has no tables.)
        let expr = analyze_projection_expr("select cast(1 as int) = cast(1 as bigint)")
            .expect("comparison should analyze");
        assert_eq!(expr.value_type.data_type().clone(), DataType::Boolean);
        match crate::common::binary_operator::typed_comparison(&expr) {
            Some((_, [left, right])) => {
                assert_eq!(
                    left.value_type.data_type().clone(),
                    DataType::Int64,
                    "left operand coerced to common type"
                );
                assert_eq!(
                    right.value_type.data_type().clone(),
                    DataType::Int64,
                    "right operand coerced to common type"
                );
            }
            other => panic!("expected bound comparison, got {:?}", other),
        }
    }

    #[test]
    fn date_arithmetic_refuses_integer_operands() {
        for sql in [
            "SELECT DATE '1999-01-01' + CAST(5 AS INTEGER)",
            "SELECT 5 + DATE '1999-01-01'",
            "SELECT DATE '1999-01-01' - 5",
        ] {
            let error = analyze_projection_expr(sql).unwrap_err();
            assert!(
                error.contains("No matching temporal arithmetic overload"),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn date_plus_day_interval_comparison_keeps_date_operands() {
        let expr = analyze_projection_expr(
            "SELECT DATE '1999-01-06' > DATE '1999-01-01' + INTERVAL '5' DAY",
        )
        .expect("date comparison should analyze");

        assert_eq!(expr.value_type.data_type().clone(), DataType::Boolean);
        match crate::common::binary_operator::typed_comparison(&expr) {
            Some((_, [left, right])) => {
                assert_eq!(left.value_type.data_type().clone(), DataType::Date32);
                assert_eq!(right.value_type.data_type().clone(), DataType::Date32);
            }
            other => panic!("expected bound comparison, got {:?}", other),
        }
    }

    fn analyze_variant_input(sql: &str) -> Result<TypedExpr, String> {
        analyze_projection_expr_with_catalog(
            sql,
            &UnaryInputCatalog {
                key_type: DataType::LargeBinary,
                nullable: false,
                logical_type: Some(novarocks_types::schema::SqlType::Variant),
            },
            crate::functions::builtin_sql_function_catalog(),
        )
    }

    #[test]
    fn variant_get_two_arg_static_type_is_variant_binary() {
        let expr = analyze_variant_input("select variant_get(k, '$.a') from unary_input")
            .expect("variant_get should analyze");

        assert_eq!(expr.value_type.data_type().clone(), DataType::LargeBinary);
        assert_eq!(
            expr.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Variant
        );
    }

    #[test]
    fn variant_get_rejects_non_literal_path_argument() {
        let err =
            analyze_variant_input("select variant_get(k, concat('$.', 'a')) from unary_input")
                .expect_err("path argument should be a string literal");

        assert_eq!(err, "variant_get path argument must be a string literal");
    }

    #[test]
    fn variant_get_literal_type_argument_sets_static_type() {
        let expr = analyze_variant_input("select variant_get(k, '$.a', 'bigint') from unary_input")
            .expect("variant_get should analyze");

        assert_eq!(expr.value_type.data_type().clone(), DataType::Int64);
        assert_eq!(
            expr.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
    }

    #[test]
    fn try_variant_get_rejects_non_literal_type_argument() {
        let err = analyze_variant_input(
            "select try_variant_get(k, '$.a', concat('big', 'int')) from unary_input",
        )
        .expect_err("type argument should be a string literal");

        assert_eq!(
            err,
            "try_variant_get type argument must be a string literal"
        );
    }

    #[test]
    fn abs_analysis_preserves_input_binding_and_freezes_promoted_result() {
        for (input, output) in [
            (DataType::Int8, DataType::Int16),
            (DataType::Int16, DataType::Int32),
            (DataType::Int32, DataType::Int64),
            (DataType::Int64, DataType::FixedSizeBinary(16)),
            (DataType::FixedSizeBinary(16), DataType::FixedSizeBinary(16)),
            (DataType::Float32, DataType::Float32),
            (DataType::Float64, DataType::Float64),
            (DataType::Decimal128(18, 3), DataType::Decimal128(18, 3)),
        ] {
            for nullable in [false, true] {
                let catalog = UnaryInputCatalog {
                    key_type: input.clone(),
                    nullable,
                    logical_type: if input == DataType::FixedSizeBinary(16) {
                        Some(novarocks_types::schema::SqlType::LargeInt)
                    } else {
                        None
                    },
                };
                let expression = analyze_projection_expr_with_catalog(
                    "select abs(k) from unary_input",
                    &catalog,
                    crate::functions::builtin_sql_function_catalog(),
                )
                .unwrap();
                assert_eq!(expression.value_type.data_type().clone(), output);
                assert_eq!(expression.value_type.nullable(), nullable);
                let ExprKind::FunctionCall {
                    name,
                    args,
                    binding,
                    ..
                } = &expression.kind
                else {
                    panic!("ABS must remain a bound scalar call");
                };
                assert_eq!(name, "abs");
                let [arg] = args.as_slice() else {
                    panic!("ABS must have one argument")
                };
                assert!(
                    matches!(arg.kind, ExprKind::ColumnRef { .. }),
                    "exact signed overloads must not receive a speculative input CAST"
                );
                assert_eq!(arg.value_type.data_type().clone(), input);
                assert_eq!(arg.value_type.nullable(), nullable);
                assert_eq!(
                    binding.selected.argument_types.as_ref(),
                    &[novarocks_functions::FunctionArgumentType::Value(
                        novarocks_functions::FunctionValueType::try_with_logical_type(
                            input.clone(),
                            nullable,
                            if input == DataType::FixedSizeBinary(16) {
                                novarocks_type_contract::ValueLogicalType::LargeInt
                            } else {
                                novarocks_type_contract::ValueLogicalType::Physical
                            },
                        )
                        .unwrap()
                    )]
                );
                let novarocks_functions::FunctionResultType::Scalar(result) =
                    &binding.selected.result_type
                else {
                    panic!("ABS has a scalar result");
                };
                assert_eq!(result.data_type().clone(), output);
                assert_eq!(result.nullable(), nullable);
            }
        }
    }

    #[test]
    fn abs_typed_null_and_outer_decimal_cast_keep_the_promoted_child() {
        for (spelling, input, output) in [
            ("tinyint", DataType::Int8, DataType::Int16),
            ("smallint", DataType::Int16, DataType::Int32),
            ("int", DataType::Int32, DataType::Int64),
            ("bigint", DataType::Int64, DataType::FixedSizeBinary(16)),
            (
                "largeint",
                DataType::FixedSizeBinary(16),
                DataType::FixedSizeBinary(16),
            ),
        ] {
            let expression =
                analyze_projection_expr(&format!("select abs(cast(null as {spelling}))")).unwrap();
            assert_eq!(expression.value_type.data_type().clone(), output);
            assert!(expression.value_type.nullable());
            let ExprKind::FunctionCall { args, .. } = expression.kind else {
                panic!("expected ABS call");
            };
            assert_eq!(args[0].value_type.data_type().clone(), input);
            assert!(args[0].value_type.nullable());
        }
        let expression =
            analyze_projection_expr("select cast(abs(cast(-128 as tinyint)) as decimal(38,15))")
                .unwrap();
        assert_eq!(
            expression.value_type.data_type().clone(),
            DataType::Decimal128(38, 15)
        );
        let ExprKind::Cast { expr, .. } = expression.kind else {
            panic!("expected outer Decimal CAST")
        };
        assert_eq!(expr.value_type.data_type().clone(), DataType::Int16);
        let ExprKind::FunctionCall { args, .. } = expr.into_inner().kind else {
            panic!("expected inner ABS")
        };
        assert_eq!(args[0].value_type.data_type().clone(), DataType::Int8);
    }

    #[test]
    fn concat_modes_control_argument_channels_ordinals_and_output_names() {
        fn resolved(
            sql: &str,
            mode: &str,
        ) -> Result<crate::analysis::ResolvedQuery, crate::analyze_error::AnalyzeError> {
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            super::super::analyze_with_function_catalog_and_sql_semantics(
                query,
                &EmptyCatalog,
                "default",
                crate::functions::builtin_sql_function_catalog(),
                &crate::sql_mode::SqlSemanticSettings::default()
                    .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(mode)),
                crate::constant::test_constant_policy(),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .map(|(query, _, _)| query)
        }
        for (mode, args_len, heading) in [
            ("32", 3, "group_concat('a','-' SEPARATOR ',')"),
            ("GROUP_CONCAT_LEGACY", 2, "group_concat('a' SEPARATOR '-')"),
        ] {
            let query = resolved("SELECT group_concat('a','-')", mode).unwrap();
            let QueryBody::Select(select) = query.body else {
                panic!("select");
            };
            assert_eq!(select.projection[0].output_name, heading);
            let crate::analysis::ExprKind::AggregateCall { args, .. } =
                &select.projection[0].expr.kind
            else {
                panic!("aggregate");
            };
            assert_eq!(args.len(), args_len);
        }
        resolved("SELECT group_concat('a','-' ORDER BY 2)", "32").unwrap();
        let error = resolved(
            "SELECT group_concat('a','-' ORDER BY 2)",
            "GROUP_CONCAT_LEGACY",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ORDER BY position 2 is not in group_concat output list")
        );
        for mode in ["32", "GROUP_CONCAT_LEGACY"] {
            let query = resolved("SELECT string_agg('a','-' \n,\n '|'\n\n)", mode).unwrap();
            let QueryBody::Select(select) = query.body else {
                panic!("select");
            };
            assert_eq!(
                select.projection[0].output_name,
                "group_concat('a','-' SEPARATOR '|')"
            );
            assert!(resolved("SELECT string_agg('a','-' ORDER BY 2)", mode).is_err());
        }
    }

    #[test]
    fn concat_array_family_preserves_the_exact_argument_shapes() {
        for (sql, list_arguments) in [
            (
                "SELECT concat('a', CAST(ARRAY[] AS ARRAY(VARCHAR)))",
                &[false, true][..],
            ),
            ("SELECT concat(ARRAY['a'], 'b')", &[true, false][..]),
            (
                "SELECT concat(ARRAY['a'], ARRAY['b'], ARRAY['c'])",
                &[true, true, true][..],
            ),
            (
                "SELECT concat(CAST(NULL AS VARCHAR), ARRAY['b'])",
                &[false, true][..],
            ),
            (
                "SELECT concat(CAST(9223372036854775807 AS BIGINT), CAST(ARRAY[1] AS ARRAY(BIGINT)))",
                &[false, true][..],
            ),
            ("SELECT concat(1, ARRAY[2147483648])", &[false, true][..]),
        ] {
            let expression =
                analyze_projection_expr(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let ExprKind::FunctionCall {
                name,
                args,
                binding,
                ..
            } = &expression.kind
            else {
                panic!("expected original concat call")
            };
            assert_eq!(name, "concat");
            assert_eq!(binding.function_id.as_str(), "builtin.scalar/concat/v1");
            assert!(matches!(
                expression.value_type.data_type(),
                DataType::List(_)
            ));
            assert_eq!(args.len(), list_arguments.len());
            for ((arg, target), &is_array) in args
                .iter()
                .zip(binding.selected.argument_types.iter())
                .zip(list_arguments)
            {
                let novarocks_functions::FunctionArgumentType::Value(value) = target else {
                    panic!("concat value argument")
                };
                assert_eq!(arg.value_type.data_type(), value.data_type());
                assert_eq!(
                    matches!(arg.value_type.data_type(), DataType::List(_)),
                    is_array
                );
            }
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("concat result")
            };
            assert_eq!(result, &expression.value_type);
        }
    }

    #[test]
    fn concat_array_incompatible_domain_fails_without_stringification() {
        for sql in [
            "SELECT concat(ARRAY['a'], map(ARRAY[1], ARRAY[2]))",
            "SELECT concat(ARRAY['a'], ARRAY[1])",
            "SELECT concat(ARRAY[1], ARRAY['a'])",
            "SELECT concat(ARRAY['a'], 1)",
            "SELECT concat(1, ARRAY['a'])",
        ] {
            let error = analyze_projection_expr(sql)
                .expect_err("incompatible array item shapes must fail during binding");
            assert_eq!(error, "no matching declared overload", "{sql}");
        }
        assert!(analyze_projection_expr("SELECT concat('a', ARRAY['b'], 'c')").is_err());
    }

    #[test]
    fn concat_without_an_array_retains_string_binding() {
        let expression = analyze_projection_expr("select concat('a','b')").expect("string concat");
        assert_eq!(expression.value_type.data_type().clone(), DataType::Utf8);
        assert!(matches!(expression.kind, ExprKind::FunctionCall { name, .. } if name == "concat"));
    }

    #[test]
    fn standard_decimal_multiply_preserves_the_original_overload_and_result_scale() {
        for (sql, expected) in [
            (
                "SELECT CAST(0 AS DECIMAL(30,10))*CAST(0 AS DECIMAL(18,9))",
                DataType::Decimal128(38, 9),
            ),
            (
                "SELECT CAST(NULL AS DECIMAL(30,10))*CAST(1 AS DECIMAL(18,9))",
                DataType::Decimal128(38, 9),
            ),
            (
                "SELECT CAST(1 AS DECIMAL(20,0))*CAST(1 AS INT)",
                DataType::Decimal128(30, 0),
            ),
            (
                "SELECT CAST(1 AS DECIMAL(38,20))*CAST(1 AS DECIMAL(20,19))",
                DataType::Decimal128(38, 19),
            ),
        ] {
            let expression = analyze_projection_expr(sql).unwrap();
            assert_eq!(expression.value_type.data_type(), &expected, "{sql}");
            let (operator, _) = crate::common::binary_operator::typed_operands(&expression)
                .expect("original selected multiplication");
            assert_eq!(
                operator,
                novarocks_type_contract::OperatorFunction::Multiply
            );
        }
        assert!(
            analyze_projection_expr("SELECT CAST(1 AS DECIMAL(76,0))*CAST(1 AS DECIMAL(76,0))")
                .is_err()
        );
    }

    #[test]
    fn standard_decimal_multiply_covers_cte_derived_correlated_and_union_siblings() {
        fn resolved(
            sql: &str,
            settings: &crate::sql_mode::SqlSemanticSettings,
        ) -> (
            crate::analysis::ResolvedQuery,
            crate::analysis::cte::CTERegistry,
        ) {
            let original_settings = settings.clone();
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            let (query, registry, _) =
                super::super::analyze_with_function_catalog_and_sql_semantics(
                    query,
                    &EmptyCatalog,
                    "default",
                    crate::functions::builtin_sql_function_catalog(),
                    settings,
                    crate::constant::test_constant_policy(),
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
            assert_eq!(settings, &original_settings);
            (query, registry)
        }
        let product = "CAST(0 AS DECIMAL(30,10))*CAST(0 AS DECIMAL(18,9))";
        let expected_type = DataType::Decimal128(38, 9);
        let settings = crate::sql_mode::SqlSemanticSettings::default();
        // All producers inherit the same query policy; identities stay independent.
        let (query, registry) = resolved(
            &format!(
                "WITH a AS (SELECT {product} x), b AS (SELECT {product} y) SELECT a.x,b.y,{product} z FROM a CROSS JOIN b"
            ),
            &settings,
        );
        assert_eq!(
            query
                .output_columns
                .iter()
                .map(|c| c.value_type.data_type().clone())
                .collect::<Vec<_>>(),
            vec![expected_type.clone(); 3]
        );
        assert_eq!(query.local_cte_ids.len(), 2);
        assert_ne!(query.local_cte_ids[0], query.local_cte_ids[1]);
        for id in &query.local_cte_ids {
            assert!(registry.get(*id).is_some());
        }
        let (query, _) = resolved(
            &format!("SELECT t.x,{product} y FROM (SELECT {product} x) t"),
            &settings,
        );
        assert_eq!(
            query.output_columns[0].value_type.data_type(),
            &expected_type
        );
        assert_eq!(
            query.output_columns[1].value_type.data_type(),
            &expected_type
        );
        let (query, _) = resolved(
            &format!("SELECT (SELECT {product}) x,(SELECT {product}) y,{product} z"),
            &settings,
        );
        assert_eq!(
            query
                .output_columns
                .iter()
                .map(|c| c.value_type.data_type().clone())
                .collect::<Vec<_>>(),
            vec![expected_type.clone(); 3]
        );
        let QueryBody::Select(select) = &query.body else {
            panic!("select");
        };
        assert_eq!(select.apply_specs.len(), 2);
        assert_ne!(
            select.apply_specs[0].subquery_id,
            select.apply_specs[1].subquery_id
        );
        assert_ne!(
            select.apply_specs[0].output_column.column_id,
            select.apply_specs[1].output_column.column_id
        );
        let (query, _) = resolved(
            "SELECT (SELECT CAST(i.n AS DECIMAL(30,10))*CAST(1 AS DECIMAL(18,9)) FROM (SELECT 1 n) i WHERE i.n=o.n) x, (SELECT CAST(i.n AS DECIMAL(30,10))*CAST(1 AS DECIMAL(18,9)) FROM (SELECT 1 n) i WHERE i.n=o.n) y FROM (SELECT 1 n) o",
            &settings,
        );
        assert_eq!(
            query.output_columns[0].value_type.data_type(),
            &expected_type
        );
        assert_eq!(
            query.output_columns[1].value_type.data_type(),
            &expected_type
        );
        let QueryBody::Select(select) = &query.body else {
            panic!("select");
        };
        assert_eq!(select.apply_specs.len(), 2);
        let Some(crate::analysis::Relation::Subquery { output_columns, .. }) = &select.from else {
            panic!("outer derived relation");
        };
        assert_eq!(output_columns.len(), 1);
        for spec in &select.apply_specs {
            assert_eq!(
                spec.correlation_column_ids,
                vec![output_columns[0].column_id]
            );
        }
        assert_ne!(
            select.apply_specs[0].subquery_id,
            select.apply_specs[1].subquery_id
        );
        // UNION policy is an explicit analysis input, never a first-branch hint.
        let (query, _) = resolved(
            &format!("SELECT {product} x UNION ALL SELECT {product} x"),
            &settings,
        );
        let QueryBody::SetOperation(set) = query.body else {
            panic!("set operation");
        };
        let QueryBody::Select(left) = set.left.into_inner().body else {
            panic!("left select");
        };
        let QueryBody::Select(right) = set.right.into_inner().body else {
            panic!("right select");
        };
        assert_eq!(
            left.projection[0].expr.value_type.data_type(),
            &expected_type
        );
        assert_eq!(
            right.projection[0].expr.value_type.data_type(),
            &expected_type
        );
        let legacy = crate::sql_mode::SqlSemanticSettings::default().with_sql_mode(
            crate::sql_mode::SqlMode::from_assignment("GROUP_CONCAT_LEGACY"),
        );
        for sql in [
            "WITH t AS (SELECT group_concat('a','-') x) SELECT x FROM t",
            "SELECT x FROM (SELECT group_concat('a','-') x) t",
            "SELECT group_concat('a','-') UNION ALL SELECT group_concat('b','-')",
        ] {
            resolved(sql, &legacy);
        }
    }

    #[test]
    fn standard_arithmetic_and_cast_types_survive_root_mode_inheritance() {
        use novarocks_type_contract::{OperatorFunction, TrinoCastMode};
        fn resolve(
            sql: &str,
            settings: &crate::sql_mode::SqlSemanticSettings,
        ) -> (
            crate::analysis::ResolvedQuery,
            crate::analysis::cte::CTERegistry,
        ) {
            let original_settings = settings.clone();
            let query = &novarocks_parser::parser::parse_query(sql).unwrap();
            let (query, registry, _) =
                super::super::analyze_with_function_catalog_and_sql_semantics(
                    query,
                    &EmptyCatalog,
                    "default",
                    crate::functions::builtin_sql_function_catalog(),
                    settings,
                    crate::constant::test_constant_policy(),
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
            assert_eq!(settings, &original_settings);
            (query, registry)
        }
        fn assert_standard_product(expression: &TypedExpr) {
            let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
                panic!("expected selected addition");
            };
            assert_eq!(binding.function_id, OperatorFunction::Add.function_id());
            assert_eq!(
                expression.value_type.data_type(),
                &DataType::Decimal128(38, 0)
            );
            for arg in args {
                assert!(matches!(
                    arg.kind,
                    ExprKind::Cast {
                        cast_mode: Some(TrinoCastMode::Explicit),
                        ..
                    }
                ));
            }
        }
        fn assert_project(query: &crate::analysis::ResolvedQuery) {
            let QueryBody::Select(select) = &query.body else {
                panic!("select");
            };
            assert_standard_product(&select.projection[0].expr);
        }
        let expression = "CAST(1 AS DECIMAL(38,0))+CAST(1 AS DECIMAL(9,0))";
        for mode in ["32", "ERROR_IF_OVERFLOW", "34359738368"] {
            let settings = crate::sql_mode::SqlSemanticSettings::default()
                .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(mode));
            let (query, _) = resolve(&format!("SELECT {expression} x"), &settings);
            assert_project(&query);
            let (query, _) = resolve(
                &format!("SELECT {expression} x UNION ALL SELECT {expression} x"),
                &settings,
            );
            let QueryBody::SetOperation(set) = &query.body else {
                panic!("union")
            };
            assert_project(&set.left);
            assert_project(&set.right);
            let (query, registry) = resolve(
                &format!(
                    "WITH a AS (SELECT {expression} x), b AS (SELECT {expression} y) SELECT a.x,b.y FROM a CROSS JOIN b"
                ),
                &settings,
            );
            assert_eq!(query.local_cte_ids.len(), 2);
            assert_ne!(query.local_cte_ids[0], query.local_cte_ids[1]);
            let a = registry
                .entries
                .iter()
                .find(|entry| entry.name == "a")
                .unwrap();
            let b = registry
                .entries
                .iter()
                .find(|entry| entry.name == "b")
                .unwrap();
            assert_project(&a.resolved_query);
            assert_project(&b.resolved_query);
            let (query, _) = resolve(
                &format!("SELECT ({expression}) x FROM (SELECT {expression} y) t"),
                &settings,
            );
            let QueryBody::Select(select) = &query.body else {
                panic!("select")
            };
            let Some(crate::analysis::Relation::Subquery { query: inner, .. }) = &select.from
            else {
                panic!("derived")
            };
            assert_project(inner);
            assert_standard_product(&select.projection[0].expr);
        }
    }
    #[test]
    fn explicit_array_nested_decimal_cast_keeps_its_report_boundary_at_analysis() {
        let query = &novarocks_parser::parser::parse_query(
            "WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' SELECT CAST(CAST([1] AS ARRAY<DECIMAL(4,0)>) AS ARRAY<DECIMAL(3,0)>)"
        ).unwrap();
        let (resolved, _, _) = super::super::analyze_with_function_catalog_and_sql_semantics(
            query,
            &EmptyCatalog,
            "default",
            crate::functions::builtin_sql_function_catalog(),
            &crate::sql_mode::SqlSemanticSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let QueryBody::Select(select) = &resolved.body else {
            panic!("select")
        };
        let mut expression = &select.projection[0].expr;
        for precision in [3, 4] {
            let ExprKind::Cast {
                expr,
                target,
                decimal_overflow_policy,
                cast_mode,
            } = &expression.kind
            else {
                panic!("explicit array conversion")
            };
            assert_eq!(
                *cast_mode,
                Some(novarocks_type_contract::TrinoCastMode::Explicit)
            );
            assert_eq!(
                *decimal_overflow_policy,
                novarocks_type_contract::DecimalOverflowPolicy::ReportError
            );
            let DataType::List(field) = target else {
                panic!("array target")
            };
            assert_eq!(field.data_type(), &DataType::Decimal128(precision, 0));
            assert_eq!(expression.value_type.data_type(), target);
            expression = expr;
        }
        let DataType::List(field) = expression.value_type.data_type() else {
            panic!("original array")
        };
        assert_eq!(field.data_type(), &DataType::Int32);
        // Analysis preserves the boundary. Its actual runtime capability is
        // still checked by the prepared conversion author during lowering.
    }

    #[test]
    fn nested_decimal_cast_preserves_array_admission_and_map_row_capability_errors() {
        for sql in [
            "SELECT CAST(MAP(ARRAY[1], ARRAY[DECIMAL '123.5']) AS MAP(INTEGER, DECIMAL(3,0)))",
            "SELECT CAST(ROW(DECIMAL '123.5') AS ROW(a DECIMAL(3,0)))",
        ] {
            let query = novarocks_parser::parser::parse_query(sql).unwrap();
            let error = super::super::analyze_with_function_catalog_and_sql_semantics(
                &query,
                &EmptyCatalog,
                "default",
                crate::functions::builtin_sql_function_catalog(),
                &crate::sql_mode::SqlSemanticSettings::default(),
                crate::constant::test_constant_policy(),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap_err();
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
            );
            assert!(
                error
                    .message()
                    .contains("cast requires an implemented exact Physical scalar domain"),
                "{error}"
            );
            assert_eq!(
                error.span(),
                Some(novarocks_parser::Span::new(7, sql.len()))
            );
        }
        use novarocks_type_contract::TrinoCastMode;
        for (name, mode) in [
            ("CAST", TrinoCastMode::Explicit),
            ("TRY_CAST", TrinoCastMode::Try),
        ] {
            let sql = format!("SELECT {name}(ARRAY[DECIMAL '123.5'] AS ARRAY(DECIMAL(3,0)))");
            let expression = analyze_projection_expr(&sql).unwrap();
            assert!(
                matches!(expression.kind, ExprKind::Cast { cast_mode: Some(actual), .. } if actual == mode)
            );
            assert_eq!(
                expression.value_type.child_at(0).unwrap().data_type(),
                &DataType::Decimal128(3, 0)
            );
            assert_eq!(expression.value_type.nullable(), mode == TrinoCastMode::Try);
        }
    }

    #[test]
    fn decimal_largeint_add_sub_seal_exact_result_without_operand_retagging() {
        let largeint = DataType::FixedSizeBinary(16);
        for op in ["+", "-"] {
            for reverse in [false, true] {
                let (left, right) = if reverse {
                    ("cast(1 as largeint)", "cast(1.25 as decimal(38,15))")
                } else {
                    ("cast(1.25 as decimal(38,15))", "cast(1 as largeint)")
                };
                let sql = format!("select {left} {op} {right}");
                let expr = analyze_projection_expr(&sql).unwrap();
                assert_eq!(
                    expr.value_type.data_type().clone(),
                    DataType::Decimal256(55, 15)
                );
                let ExprKind::BinaryOp { left, right, .. } = expr.kind else {
                    panic!("expected frozen arithmetic");
                };
                assert_eq!(
                    left.value_type.data_type().clone(),
                    if reverse {
                        largeint.clone()
                    } else {
                        DataType::Decimal128(38, 15)
                    }
                );
                assert_eq!(
                    right.value_type.data_type().clone(),
                    if reverse {
                        DataType::Decimal128(38, 15)
                    } else {
                        largeint.clone()
                    }
                );
            }
        }
        assert_eq!(analyze_projection_expr("select cast(0.000000000000000000000000000000000001 as decimal(38,36)) + cast(1 as largeint)").unwrap().value_type.data_type().clone(),DataType::Decimal256(76,36));
        for query in [
            "select cast(0 as decimal(38,37)) + cast(1 as largeint)",
            "select cast(1 as decimal(38,15)) * cast(1 as largeint)",
            "select cast(1 as decimal(38,15)) / cast(1 as largeint)",
            "select cast(1 as decimal(38,15)) % cast(1 as largeint)",
        ] {
            assert!(
                analyze_projection_expr(query)
                    .unwrap_err()
                    .contains("no frozen result rule"),
                "{query}"
            );
        }
    }
    #[test]
    fn unavailable_builtin_is_explicitly_rejected_before_false_filter_pruning() {
        let error = analyze_projection_expr("select reverse(ARRAY[1,2]) where false").unwrap_err();
        assert!(error.contains("builtin.scalar/reverse/(list"), "{error}");
        for sql in ["select isnull(1) where false", "select cosh(1) where false"] {
            let error = analyze_projection_expr(sql).unwrap_err();
            assert!(
                error.contains("no admitted selected scalar implementation"),
                "{sql}: {error}"
            );
        }
        for sql in [
            "select array_reverse([1,2]) where false",
            "select array_to_string([1,2], ',') where false",
            "select next_day('2020-01-01') where false",
            "select next_day(cast('2020-01-01' as date), 'Monday') where false",
        ] {
            let error = analyze_projection_expr(sql).unwrap_err();
            assert!(error.contains("Unknown function"), "{sql}: {error}");
        }
        let unknown =
            analyze_projection_expr("select not_an_advertised_builtin(1) where false").unwrap_err();
        assert!(unknown.contains("Unknown function"), "{unknown}");
    }

    #[test]
    fn authoritative_lowering_retains_alias_and_non_row_boundaries() {
        for (sql, expected) in [
            ("select every(true)", "bool_and"),
            ("select max_by(1,2)", "max_by"),
        ] {
            let typed = analyze_projection_expr(sql).unwrap();
            let ExprKind::AggregateCall { name, resolved, .. } = typed.kind else {
                panic!("{sql}: expected aggregate boundary")
            };
            assert_eq!(name, expected);
            assert_eq!(resolved.kind, novarocks_functions::FunctionKind::Aggregate);
            assert_eq!(
                resolved.semantics.intrinsic_row_error,
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated
            );
        }
        let typed = analyze_projection_expr("select row_number() over ()").unwrap();
        let ExprKind::WindowCall { binding, .. } = typed.kind else {
            panic!("expected window boundary")
        };
        assert_eq!(binding.kind, novarocks_functions::FunctionKind::Window);
        assert_eq!(
            binding.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated
        );
        assert!(matches!(
            analyze_projection_expr("select typeof(cast(1 as tinyint))")
                .unwrap()
                .kind,
            ExprKind::Literal(LiteralValue::String(_))
        ));
        let negation = analyze_projection_expr("select -cast(1 as smallint)").unwrap();
        let operand = crate::common::negation::typed_operand(&negation)
            .expect("unary minus selects the dedicated scalar operator");
        assert_eq!(negation.value_type, operand.value_type);
        assert!(matches!(
            operand.kind,
            ExprKind::Cast {
                target: DataType::Int16,
                ..
            }
        ));
        assert!(matches!(
            analyze_projection_expr("select case when true then 1 else 2 end")
                .unwrap()
                .kind,
            ExprKind::Case { .. }
        ));
        // Existing grouping-marker tests prove GROUPING is replaced by exact ColumnId.
    }

    #[test]
    fn full_outer_using_freezes_the_common_decimal_type_and_operand_conversions() {
        use crate::analysis::ExprKind;
        use novarocks_type_contract::TrinoCastMode;

        let sql = "SELECT k AS merged FROM (SELECT CAST(1 AS DECIMAL(3,0)) k) l FULL OUTER JOIN (SELECT CAST(1000 AS BIGINT) k) r USING(k)";
        let typed = analyze_projection_expr(sql).unwrap();
        let common_type = DataType::Decimal128(19, 0);
        assert_eq!(typed.value_type.data_type(), &common_type);
        let ExprKind::FunctionCall {
            name,
            args,
            binding,
            ..
        } = typed.kind
        else {
            panic!("expected synthesized COALESCE: {sql}");
        };
        assert_eq!(name, "coalesce");
        assert_eq!(args.len(), 2);
        assert!(
            binding
                .selected
                .argument_types
                .iter()
                .all(|arg| matches!(arg,
                    novarocks_functions::FunctionArgumentType::Value(value)
                        if value.data_type() == &common_type
                ))
        );
        for (arg, original_type) in args
            .iter()
            .zip([DataType::Decimal128(3, 0), DataType::Int64])
        {
            let ExprKind::Cast {
                cast_mode: Some(TrinoCastMode::Implicit),
                expr,
                target,
                ..
            } = &arg.kind
            else {
                panic!("expected widening implicit cast: {arg:?}");
            };
            assert!(matches!(expr.kind, ExprKind::ColumnRef { .. }));
            assert_eq!(expr.value_type.data_type(), &original_type);
            assert_eq!(target, &common_type);
        }
    }

    fn resolve_call_policy_query(sql: &str) -> crate::analysis::ResolvedQuery {
        let query = &novarocks_parser::parser::parse_query(sql).unwrap();
        super::super::analyze_with_function_catalog_and_sql_semantics(
            query,
            &EmptyCatalog,
            "default",
            crate::functions::builtin_sql_function_catalog(),
            &crate::sql_mode::SqlSemanticSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .0
    }

    fn expression_call_binding(expression: &TypedExpr) -> &crate::binding::SqlFunctionBinding {
        match &expression.kind {
            ExprKind::FunctionCall { binding, .. } => binding,
            ExprKind::AggregateCall { resolved, .. } => resolved,
            ExprKind::WindowCall {
                binding,
                aggregate_binding,
                ..
            } => {
                if let Some(aggregate) = aggregate_binding {
                    assert!(std::ptr::eq(binding.resolved(), aggregate.resolved()));
                    assert_eq!(
                        binding.decimal_overflow_policy(),
                        aggregate.decimal_overflow_policy()
                    );
                }
                binding
            }
            ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => expression_call_binding(expr),
            other => panic!("expected an actual resolved call: {other:?}"),
        }
    }

    #[test]
    fn query_policy_is_frozen_for_nested_scalar_aggregate_window_and_empty_array_calls() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let strict = "WITH SESSION sql_mode='ERROR_IF_OVERFLOW'";
        let relaxed = "WITH SESSION sql_mode=32";
        for expression in [
            "abs(1)",
            "max(1)",
            "row_number() OVER ()",
            "sum(1) OVER ()",
            "CAST([] AS ARRAY<INT>)",
        ] {
            for (root, expected) in [(strict, ReportError), (relaxed, OutputNull)] {
                let sql =
                    format!("{root} SELECT {expression} AS x FROM (SELECT {expression} AS y) q");
                let query = resolve_call_policy_query(&sql);
                let QueryBody::Select(select) = &query.body else {
                    panic!("select");
                };
                let Some(crate::analysis::Relation::Subquery { query: inner, .. }) = &select.from
                else {
                    panic!("derived query");
                };
                let QueryBody::Select(inner) = &inner.body else {
                    panic!("inner select");
                };
                let outer = expression_call_binding(&select.projection[0].expr);
                let inner = expression_call_binding(&inner.projection[0].expr);
                assert_eq!(outer.decimal_overflow_policy(), expected, "{sql}");
                assert_eq!(inner.decimal_overflow_policy(), expected, "{sql}");
                assert_eq!(outer.function_id, inner.function_id, "{sql}");
                assert_eq!(outer.selected, inner.selected, "{sql}");
                // The same query policy preserves equal binding facts at both call sites.
                assert_eq!(outer, inner, "{sql}");
            }
        }
    }

    #[test]
    fn unnest_and_nested_array_calls_inherit_query_policy() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let strict = "WITH SESSION sql_mode='ERROR_IF_OVERFLOW'";
        let relaxed = "WITH SESSION sql_mode=32";
        for (root, expected) in [(strict, ReportError), (relaxed, OutputNull)] {
            let sql = format!(
                "{root} SELECT abs(u.x) AS x FROM (SELECT [1] AS a) q CROSS JOIN UNNEST(q.a) AS u(x)"
            );
            let query = resolve_call_policy_query(&sql);
            let QueryBody::Select(select) = &query.body else {
                panic!("select");
            };
            let Some(crate::analysis::Relation::Join(join)) = &select.from else {
                panic!("join");
            };
            let crate::analysis::Relation::Unnest(unnest) = &join.right else {
                panic!("unnest");
            };
            assert_eq!(unnest.binding.decimal_overflow_policy(), expected, "{sql}");
            assert_eq!(
                expression_call_binding(&select.projection[0].expr).decimal_overflow_policy(),
                expected,
                "{sql}"
            );
            let crate::analysis::Relation::Subquery { query: inner, .. } = &join.left else {
                panic!("derived query");
            };
            let QueryBody::Select(inner) = &inner.body else {
                panic!("inner select");
            };
            assert_eq!(
                expression_call_binding(&inner.projection[0].expr).decimal_overflow_policy(),
                expected,
                "{sql}"
            );
        }
    }
    #[test]
    fn implicit_common_numeric_coercions_preserve_nonnull_literals_before_folding() {
        use novarocks_type_contract::{FunctionValueType, TrinoCastMode};
        let control = crate::compiler::SqlCompileControl::unbounded();
        for sql in [
            "SELECT 0.1 = 0.25",
            "SELECT 0.1 IN (0.25)",
            "SELECT 0.1 BETWEEN 0.25 AND 0.75",
            "SELECT CASE 0.1 WHEN 0.25 THEN 1 ELSE 0 END",
        ] {
            let query = resolve_call_policy_query(sql);
            let QueryBody::Select(select) = &query.body else {
                panic!("SELECT")
            };
            let output = &select.projection[0].expr;
            let coerced = match &output.kind {
                ExprKind::FunctionCall { binding, args, .. }
                    if binding.function_id
                        == novarocks_type_contract::OperatorFunction::Equal.function_id() =>
                {
                    &args[0]
                }
                ExprKind::InList { expr: left, .. } | ExprKind::Between { expr: left, .. } => {
                    left.as_ref()
                }
                ExprKind::Case {
                    operand: Some(left),
                    ..
                } => left.as_ref(),
                other => panic!("actual common-type consumer: {other:?}"),
            };
            if !matches!(output.kind, ExprKind::Case { .. }) {
                assert!(!output.value_type.nullable(), "{sql}");
            }
            assert_eq!(
                coerced.value_type,
                FunctionValueType::new(DataType::Decimal128(2, 2), false),
                "{sql}"
            );
            let ExprKind::Cast {
                cast_mode: Some(TrinoCastMode::Implicit),
                expr,
                target,
                ..
            } = &coerced.kind
            else {
                panic!("real CAST")
            };
            assert_eq!(
                expr.value_type,
                FunctionValueType::new(DataType::Decimal128(1, 1), false)
            );
            assert_eq!(target, &DataType::Decimal128(2, 2));
            novarocks_functions::PreparedCastRecipe::try_new_trino(
                &expr.value_type,
                &coerced.value_type,
                TrinoCastMode::Implicit,
                &control,
            )
            .expect("real prepared cast accepts the common-type producer declaration");
        }
        let query = resolve_call_policy_query("SELECT 0.25 = 0.75");
        let QueryBody::Select(select) = &query.body else {
            panic!("SELECT")
        };
        let output = &select.projection[0].expr;
        assert!(
            !output.value_type.nullable(),
            "equal carriers need no new row operation"
        );
        let Some((BinOp::Eq, [left, right])) =
            crate::common::binary_operator::typed_comparison(output)
        else {
            panic!("comparison")
        };
        assert!(matches!(left.kind, ExprKind::Literal(_)));
        assert!(matches!(right.kind, ExprKind::Literal(_)));
    }

    #[test]
    fn decimal_sink_guard_comparison_preserves_the_numeric_literal_cast_contract() {
        use novarocks_type_contract::FunctionValueType;
        let query = resolve_call_policy_query(
            "SELECT ABS(CAST(99999999.9949 AS DECIMAL(13,4))) > 99999999.9999",
        );
        let QueryBody::Select(select) = &query.body else {
            panic!("SELECT")
        };
        let Some((BinOp::Gt, [_, right])) =
            crate::common::binary_operator::typed_comparison(&select.projection[0].expr)
        else {
            panic!("comparison")
        };
        assert_eq!(
            right.value_type,
            FunctionValueType::new(DataType::Decimal128(13, 4), false)
        );
        let ExprKind::Cast {
            expr,
            cast_mode: Some(novarocks_type_contract::TrinoCastMode::Implicit),
            target: DataType::Decimal128(13, 4),
            ..
        } = &right.kind
        else {
            panic!("literal's genuine common-type CAST")
        };
        assert_eq!(
            expr.value_type,
            FunctionValueType::new(DataType::Decimal128(12, 4), false)
        );
    }
}

#[cfg(test)]
#[path = "group_concat_source_tests.rs"]
mod group_concat_source_tests;

/// Project one complete child and its exact physical layout through the same
/// pure SQL construction body and the original cancellation/deadline observer.
#[cfg(test)]
fn container_child_value_type_observed(
    source: &novarocks_type_contract::FunctionValueType,
    index: usize,
    control: &crate::compiler::SqlCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, AnalyzeError> {
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, TypedValueBinding, ValueLayout,
    };
    enum Error {
        Type(novarocks_type_contract::ExecutionValueTypeError),
        Wrapper(novarocks_type_contract::ValueTypeError),
        Control(novarocks_type_contract::CompileControlError),
    }
    impl From<novarocks_type_contract::ExecutionValueTypeError> for Error {
        fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
            Self::Type(error)
        }
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    let child = source
        .typed_binding()
        .borrowed()
        .child_at(index)
        .ok_or_else(|| {
            AnalyzeError::internal("container source lacks its complete child binding")
        })?;
    work.step().map_err(AnalyzeError::control)?;
    work.flush().map_err(AnalyzeError::control)?;
    let storage = source.typed_binding().layout().storage_lifetime();
    let carrier = child.data_type().clone();
    let value = child.value_type().clone();
    let result = (|| {
        let mut observe = |units| {
            for _ in 0..units {
                work.step().map_err(Error::Control)?;
            }
            work.flush().map_err(Error::Control)
        };
        let layout =
            ValueLayout::try_from_normalized_arrow_observed(carrier, storage, &mut observe)?;
        let binding = TypedValueBinding::try_new_observed(value, layout, &mut observe)?;
        novarocks_type_contract::FunctionValueType::try_from_typed_binding(binding)
            .map_err(Error::Wrapper)
    })();
    if let Err(Error::Control(error)) = &result {
        return Err(AnalyzeError::control(error.clone()));
    }
    work.finish().map_err(AnalyzeError::control)?;
    result.map_err(|error| match error {
        Error::Type(error) => AnalyzeError::internal(error.to_string()),
        Error::Wrapper(error) => AnalyzeError::internal(error.to_string()),
        Error::Control(error) => AnalyzeError::control(error),
    })
}

#[cfg(test)]
#[path = "resolve_expr/signed_cast_nullability_tests.rs"]
mod signed_cast_nullability_tests;
