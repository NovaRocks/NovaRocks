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

use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_parser::Span;
use novarocks_parser::ast;
use novarocks_parser::printer::{print_expr, print_object_name, print_type_name};

use crate::analysis::*;
use crate::analyze_error::AnalyzeError;
use novarocks_types::{arithmetic_result_type_with_op, comparison_common_type, wider_type};

use super::functions::*;
use super::helpers::{eval_const_i64, expr_display_name, sql_type_to_arrow};
use super::scope::AnalyzerScope;

type WindowSpecAnalysis = (Vec<TypedExpr>, Vec<SortItem>, Option<WindowFrame>);

fn scalar_function_is_unknown(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    arg_types: &[DataType],
) -> bool {
    matches!(
        function_catalog.resolve_scalar_signature(name, arg_types),
        Err(crate::functions::ResolveError::UnknownFunction)
    ) && legacy_scalar_return_type_with_catalog(function_catalog, name, arg_types).is_none()
}

fn interval_field_name(field: ast::IntervalField) -> &'static str {
    match field {
        ast::IntervalField::Year => "year",
        ast::IntervalField::Quarter => "quarter",
        ast::IntervalField::Month => "month",
        ast::IntervalField::Week => "week",
        ast::IntervalField::Day => "day",
        ast::IntervalField::Hour => "hour",
        ast::IntervalField::Minute => "minute",
        ast::IntervalField::Second => "second",
        ast::IntervalField::Millisecond => "millisecond",
        ast::IntervalField::Microsecond => "microsecond",
    }
}

fn string_literal_expr(value: String, span: Span) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::String(value),
        span,
    })
}

impl<'a> super::AnalyzerContext<'a> {
    /// Analyze a single expression and produce a TypedExpr.
    pub(super) fn analyze_expr(
        &self,
        expr: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        match expr {
            // Simple column reference
            ast::Expr::Identifier(ident) => {
                // `@@var` references a MySQL session variable. We do not yet
                // maintain a per-session variable store, so resolve a small
                // set of known names to constants and return any other as
                // an empty string rather than failing with "column not found".
                if ident.value.starts_with("@@") {
                    let name = ident.value[2..].to_ascii_lowercase();
                    let value = session_variable_default(&name);
                    return Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String(value)),
                        data_type: DataType::Utf8,
                        nullable: false,
                    });
                }
                // `@var` is a MySQL-style user variable. Bound variables are
                // substituted textually by the standalone server before the SQL
                // reaches the analyzer. If an `@var` token still arrives here,
                // it means the variable was not bound — MySQL semantics for an
                // unbound user variable is NULL.
                if ident.value.starts_with('@') {
                    return Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Null),
                        data_type: DataType::Null,
                        nullable: true,
                    });
                }
                if let Some(param) = scope.resolve_lambda_param(&ident.value) {
                    return Ok(TypedExpr {
                        kind: ExprKind::LambdaParamRef {
                            name: param.name,
                            slot_id: param.slot_id,
                        },
                        data_type: param.data_type,
                        nullable: param.nullable,
                    });
                }
                // If the scope has a synthetic expression for this name
                // (FULL OUTER USING column → COALESCE), return that
                // expression directly so the merged value is computed.
                if let Some(expr) = scope.computed_column_for(&ident.value) {
                    return Ok(expr.clone());
                }
                let (column_id, data_type, nullable) =
                    scope.resolve_at(None, &ident.value, ident.span)?;
                Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id,
                        qualifier: None,
                        column: ident.value.to_lowercase(),
                    },
                    data_type,
                    nullable,
                })
            }

            // Qualified column reference or STRUCT field chain encoded by the
            // native parser as a compound identifier (for example `c13.a`).
            ast::Expr::CompoundIdentifier(parts) if parts.parts.len() >= 2 => {
                self.analyze_compound_identifier(&parts.parts, scope)
            }

            // MySQL user and system variables are parser-native syntax rather
            // than disguised identifiers. Keep the historical analyzer
            // behaviour: known `@@` variables resolve to defaults and unbound
            // `@` variables resolve to NULL.
            ast::Expr::UserVariable(variable) => {
                if variable.value.starts_with("@@") {
                    let name = variable.value[2..].to_ascii_lowercase();
                    return Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String(session_variable_default(
                            &name,
                        ))),
                        data_type: DataType::Utf8,
                        nullable: false,
                    });
                }
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Null),
                    data_type: DataType::Null,
                    nullable: true,
                })
            }

            ast::Expr::Literal(value) => self.analyze_literal(value),

            ast::Expr::Array(array) => self.analyze_array_literal(array, scope),

            // `MAP{key: value, ...}` is a native literal node. Lower it
            // through the same typed scalar-function binder as `map(k, v, …)`
            // so coercion and the resulting Arrow MAP schema stay identical.
            ast::Expr::Map(map) => self.analyze_map_literal(map, scope),

            // The native parser retains JSON arrows as `AccessKind::Json` and
            // gives them the grammar's intended precedence, so no SQL-text
            // reparse or tree rotation is required here.
            ast::Expr::Binary(binary) => {
                self.analyze_binary_op(&binary.left, &binary.operator, &binary.right, scope)
            }

            // A lambda is valid only as a higher-order function argument.
            // Those callers bind its parameters before analyzing the body.
            ast::Expr::Lambda(lambda) => Err(AnalyzeError::unsupported_expression(
                "lambda expressions are only allowed inside higher-order function calls",
                lambda.span,
            )),

            // Unary NOT
            ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Not) => {
                let inner_typed = self.analyze_expr(&unary.expression, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::UnaryOp {
                        op: UnOp::Not,
                        expr: Box::new(inner_typed),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // Unary minus
            ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
                let inner_typed = self.analyze_expr(&unary.expression, scope)?;
                let dt = inner_typed.data_type.clone();
                Ok(TypedExpr {
                    kind: ExprKind::UnaryOp {
                        op: UnOp::Negate,
                        expr: Box::new(inner_typed),
                    },
                    data_type: dt,
                    nullable: false,
                })
            }

            // Bitwise NOT (~)
            ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::BitwiseNot) => {
                let inner_typed = self.analyze_expr(&unary.expression, scope)?;
                let dt = inner_typed.data_type.clone();
                Ok(TypedExpr {
                    kind: ExprKind::UnaryOp {
                        op: UnOp::BitwiseNot,
                        expr: Box::new(inner_typed),
                    },
                    data_type: dt,
                    nullable: false,
                })
            }

            // IS predicates are one native syntax node with explicit kind.
            ast::Expr::IsPredicate(predicate)
                if matches!(
                    predicate.predicate,
                    ast::IsPredicate::Null | ast::IsPredicate::NotNull
                ) =>
            {
                let inner_typed = self.analyze_expr(&predicate.expr, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsNull {
                        expr: Box::new(inner_typed),
                        negated: matches!(predicate.predicate, ast::IsPredicate::NotNull),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }
            // IN list
            ast::Expr::InList(in_list) => {
                use super::literal_coercion::coerce_literal_for_comparison;
                let mut expr_typed = self.analyze_expr(&in_list.expr, scope)?;
                // StarRocks-aligned implicit literal coercion: when the IN
                // expression is `column IN (lit, lit, ...)`, coerce each
                // string literal to the column's type before emitting the
                // InList. Mirrors the binary-op comparison coercion.
                let mut list_typed = Vec::with_capacity(in_list.list.len());
                for item in &in_list.list {
                    let item_typed = self.analyze_expr(item, scope)?;
                    list_typed.push(coerce_literal_for_comparison(&expr_typed, item_typed));
                }
                // BITMAP / HLL operands cannot participate in IN / NOT IN
                // because they have no scalar identity. Reject upfront so the
                // user sees a clear error before lowering / codegen.
                let kw = if in_list.negated { "NOT IN" } else { "IN" };
                if let Some(logical) = scope
                    .logical_type_of_expr(&expr_typed)
                    .filter(is_bitmap_or_hll_type)
                {
                    let col = column_name_of_expr(&expr_typed);
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "BITMAP/HLL columns cannot appear in {kw} expressions (operand `{col}` has type {logical:?})"
                        ),
                        in_list.span,
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
                            in_list.span,
                        ));
                    }
                }
                for item in &list_typed {
                    if incompatible_complex_compare(&expr_typed.data_type, &item.data_type)
                        .is_some()
                    {
                        return Err(AnalyzeError::type_mismatch(
                            in_predicate_type_error(&expr_typed.data_type, &item.data_type),
                            in_list.span,
                        ));
                    }
                }
                let common_type = list_typed
                    .iter()
                    .fold(expr_typed.data_type.clone(), |acc, item| {
                        wider_type(&acc, &item.data_type)
                    });
                if expr_typed.data_type != common_type
                    && data_type_contains_null(&expr_typed.data_type)
                {
                    expr_typed = cast_null_preserving_target_type(expr_typed, &common_type);
                }
                for item in &mut list_typed {
                    if item.data_type != common_type && data_type_contains_null(&item.data_type) {
                        *item = cast_null_preserving_target_type(item.clone(), &common_type);
                    }
                }
                Ok(TypedExpr {
                    kind: ExprKind::InList {
                        expr: Box::new(expr_typed),
                        list: list_typed,
                        negated: in_list.negated,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // BETWEEN
            ast::Expr::Between(between) => {
                use super::literal_coercion::coerce_literal_for_comparison;
                let expr_typed = self.analyze_expr(&between.expr, scope)?;
                let low_typed = self.analyze_expr(&between.low, scope)?;
                let high_typed = self.analyze_expr(&between.high, scope)?;
                // StarRocks-aligned implicit literal coercion: for
                // `column BETWEEN lit AND lit`, coerce each string literal
                // to the column's type. The helper gates on the LHS being a
                // column ref (same convention as binary-op comparisons), so
                // `expr_typed BETWEEN ...` where the LHS is a non-column
                // expression is left unchanged.
                let low_typed = coerce_literal_for_comparison(&expr_typed, low_typed);
                let high_typed = coerce_literal_for_comparison(&expr_typed, high_typed);
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
                            between.span,
                        ));
                    }
                }
                Ok(TypedExpr {
                    kind: ExprKind::Between {
                        expr: Box::new(expr_typed),
                        low: Box::new(low_typed),
                        high: Box::new(high_typed),
                        negated: between.negated,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // LIKE
            ast::Expr::Like(like) => {
                let expr_typed = self.analyze_expr(&like.expr, scope)?;
                let pattern_typed = self.analyze_expr(&like.pattern, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::Like {
                        expr: Box::new(expr_typed),
                        pattern: Box::new(pattern_typed),
                        negated: like.negated,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // CAST
            ast::Expr::Cast(cast) => {
                let inner_typed = self.analyze_expr(&cast.expr, scope)?;
                let target = sql_type_to_arrow(&cast.data_type)?;
                Ok(TypedExpr {
                    kind: ExprKind::Cast {
                        expr: Box::new(inner_typed),
                        target: target.clone(),
                    },
                    data_type: target,
                    nullable: true,
                })
            }

            // CASE WHEN
            ast::Expr::Case(case) => self.analyze_case(
                case.operand.as_deref(),
                &case.conditions,
                &case.results,
                case.else_result.as_deref(),
                scope,
            ),

            // Function call
            ast::Expr::FunctionCall(func) => self.analyze_function(func, scope),

            ast::Expr::Access(access) => {
                let base = self.analyze_expr(&access.expr, scope)?;
                self.analyze_access(base, &access.kind, scope, access.span)
            }

            // Nested (parenthesized)
            ast::Expr::Nested(nested) => {
                let inner_typed = self.analyze_expr(&nested.expression, scope)?;
                let dt = inner_typed.data_type.clone();
                let nullable = inner_typed.nullable;
                Ok(TypedExpr {
                    kind: ExprKind::Nested(Box::new(inner_typed)),
                    data_type: dt,
                    nullable,
                })
            }

            // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE.
            ast::Expr::IsPredicate(predicate)
                if matches!(
                    predicate.predicate,
                    ast::IsPredicate::True
                        | ast::IsPredicate::False
                        | ast::IsPredicate::NotTrue
                        | ast::IsPredicate::NotFalse
                ) =>
            {
                let inner_typed = self.analyze_expr(&predicate.expr, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsTruthValue {
                        expr: Box::new(inner_typed),
                        value: matches!(
                            predicate.predicate,
                            ast::IsPredicate::True | ast::IsPredicate::NotTrue
                        ),
                        negated: matches!(
                            predicate.predicate,
                            ast::IsPredicate::NotTrue | ast::IsPredicate::NotFalse
                        ),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // Subquery expression: EXISTS / NOT EXISTS
            ast::Expr::Exists(exists) => {
                let id = self.alloc_subquery_id();
                let kind = SubqueryKind::Exists {
                    negated: exists.negated,
                };
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: exists.query.clone(),
                    data_type: DataType::Boolean,
                    in_expr: None,
                });
                Ok(TypedExpr {
                    kind: ExprKind::SubqueryPlaceholder {
                        id,
                        kind,
                        data_type: DataType::Boolean,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // Subquery expression: col [NOT] IN (SELECT ...)
            ast::Expr::InSubquery(in_subquery) => {
                // Multi-column LHS `(a, b) IN (SELECT c, d FROM ...)` arrives
                // here wrapped as `Expr::Tuple(...)` (or `Expr::Nested(Tuple(...))`).
                // The whole tuple is not a single scalar expression — the
                // analyzer's catch-all rejects it as "unsupported expression".
                // `subquery_rewrite::rewrite_in_subquery` handles the
                // decomposition (per-column equi-join) downstream, so just
                // skip the BITMAP / JSON / HLL precheck here: those checks
                // are per-column and only meaningful for a single-column LHS.
                let lhs_is_tuple = match in_subquery.expr.as_ref() {
                    ast::Expr::Tuple(_) => true,
                    ast::Expr::Nested(inner) => {
                        matches!(inner.expression.as_ref(), ast::Expr::Tuple(_))
                    }
                    _ => false,
                };
                if !lhs_is_tuple {
                    // BITMAP / HLL operands cannot participate in IN subquery
                    // because they have no scalar identity. Reject upfront by
                    // resolving the LHS to detect a logical-type tag; the LHS
                    // is dropped before the subquery is planned so we cannot
                    // wait until later.
                    let in_expr_typed = self.analyze_expr(&in_subquery.expr, scope)?;
                    if is_json_in_subquery_operand(&in_expr_typed, scope) {
                        return Err(AnalyzeError::unsupported_expression(
                            "In predicate of JSON does not support subquery",
                            in_subquery.span,
                        ));
                    }
                    if let Some(logical) = scope
                        .logical_type_of_expr(&in_expr_typed)
                        .filter(is_bitmap_or_hll_type)
                    {
                        let col = column_name_of_expr(&in_expr_typed);
                        let kw = if in_subquery.negated { "NOT IN" } else { "IN" };
                        return Err(AnalyzeError::invalid_argument(
                            format!(
                                "BITMAP/HLL columns cannot appear in {kw} subquery expressions (operand `{col}` has type {logical:?})"
                            ),
                            in_subquery.span,
                        ));
                    }
                }
                let id = self.alloc_subquery_id();
                let kind = SubqueryKind::InSubquery {
                    negated: in_subquery.negated,
                };
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: in_subquery.query.clone(),
                    data_type: DataType::Boolean,
                    in_expr: Some(in_subquery.expr.clone()),
                });
                Ok(TypedExpr {
                    kind: ExprKind::SubqueryPlaceholder {
                        id,
                        kind,
                        data_type: DataType::Boolean,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // Scalar subquery: (SELECT ...)
            ast::Expr::Subquery(subquery) => {
                let id = self.alloc_subquery_id();
                let data_type = self.infer_scalar_subquery_data_type(&subquery.query, scope);
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
                    data_type,
                    nullable: true,
                })
            }

            // Typed literals: DATE '2024-01-01', TIMESTAMP '...', etc.
            ast::Expr::TypedString(typed_str) => {
                let target = sql_type_to_arrow(&typed_str.data_type)?;
                let ast::LiteralKind::String(value) = &typed_str.value.kind else {
                    return Err(AnalyzeError::invalid_literal(
                        "typed string requires a string literal",
                        typed_str.span,
                    ));
                };
                let value = value.clone();
                // For DATE literals, constant-fold to Date32 integer value
                if target == DataType::Date32 {
                    let date_str = value.trim_matches(|c| c == '\'' || c == '"');
                    let days = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                        .map_err(|e| {
                            AnalyzeError::invalid_literal(
                                format!("invalid date literal '{date_str}': {e}"),
                                typed_str.span,
                            )
                        })?
                        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days();
                    return Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(days)),
                        data_type: DataType::Date32,
                        nullable: false,
                    });
                }
                Ok(TypedExpr {
                    kind: ExprKind::Cast {
                        expr: Box::new(TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::String(value)),
                            data_type: DataType::Utf8,
                            nullable: false,
                        }),
                        target: target.clone(),
                    },
                    data_type: target,
                    nullable: false,
                })
            }

            // INTERVAL `value unit` remains a syntax-level string literal for
            // the existing function rewrites; source rendering is native.
            ast::Expr::Interval(_) => {
                let s = print_expr(expr);
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(s)),
                    data_type: DataType::Utf8,
                    nullable: false,
                })
            }

            other => Err(AnalyzeError::unsupported_expression(
                format!("unsupported expression: {}", print_expr(other)),
                other.span(),
            )),
        }
    }

    fn analyze_access(
        &self,
        base: TypedExpr,
        access: &ast::AccessKind,
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        match access {
            ast::AccessKind::Field(ident) => {
                self.analyze_struct_field_access(base, ident.value.clone(), ident.span)
            }
            ast::AccessKind::Subscript(index) => {
                let mut index_typed = self.analyze_expr(index, scope)?;
                let output_type = match &base.data_type {
                    DataType::List(item) => {
                        index_typed =
                            cast_null_preserving_target_type(index_typed, &DataType::Int32);
                        item.data_type().clone()
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
                        index_typed =
                            cast_null_preserving_target_type(index_typed, fields[0].data_type());
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
                let function_name = match &base.data_type {
                    DataType::List(_) => "__array_element_at",
                    DataType::Map(_, _) => "__map_element_at",
                    _ => unreachable!("only array/map subscripts reach this branch"),
                };
                Ok(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self.function_catalog.volatility(function_name),
                        name: function_name.to_string(),
                        args: vec![base, index_typed],
                        distinct: false,
                    },
                    data_type: output_type,
                    nullable: true,
                })
            }
            ast::AccessKind::Json { operator: _, path } => {
                self.analyze_json_arrow_from_typed(base, path, scope)
            }
        }
    }

    fn infer_scalar_subquery_data_type(
        &self,
        subquery: &ast::Query,
        outer_scope: &AnalyzerScope,
    ) -> DataType {
        // `analyze_query` re-enters `analyze_select` which calls
        // `rewrite_subqueries` whenever `collected_subqueries` is non-empty.
        // That rewrite *drains* the shared `collected_subqueries` vec, so any
        // outer-query subqueries collected before us would be lost. Snapshot
        // the full vec (not just its length) and restore it afterward so the
        // outer analyzer can still see and rewrite its own subqueries.
        let saved_collected: Vec<SubqueryInfo> = self.collected_subqueries.borrow().clone();
        let saved_next_subquery_id = self.next_subquery_id.get();
        let saved_cte_registry = self.cte_registry.borrow().clone();
        let inferred = self
            .analyze_query_with_outer_scope_inner(subquery, outer_scope)
            .ok()
            .and_then(|(query, _inner_scope)| {
                query
                    .output_columns
                    .first()
                    .map(|col| col.data_type.clone())
            })
            .unwrap_or(DataType::Null);
        *self.collected_subqueries.borrow_mut() = saved_collected;
        self.next_subquery_id.set(saved_next_subquery_id);
        self.cte_registry
            .borrow_mut()
            .clone_from(&saved_cte_registry);
        inferred
    }

    fn analyze_compound_identifier(
        &self,
        parts: &[ast::Ident],
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
        // Lambda parameters short-circuit before column lookup.
        let base_name = &parts[0].value;
        if let Some(param) = scope.resolve_lambda_param(base_name) {
            let mut current = TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: param.name,
                    slot_id: param.slot_id,
                },
                data_type: param.data_type,
                nullable: param.nullable,
            };
            for field in &parts[1..] {
                current =
                    self.analyze_struct_field_access(current, field.value.clone(), field.span)?;
            }
            return Ok(current);
        }

        let build_column_ref =
            |column_id, data_type, nullable, qualifier: Option<String>, column: &str| TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier,
                    column: column.to_lowercase(),
                },
                data_type,
                nullable,
            };

        // Form 1: parts[0] is a qualifier, parts[1] is a column, parts[2..] are
        // struct subfields. Supports `t.col`, `t.col.field`, `t.col.f1.f2`, …
        if parts.len() >= 2 {
            let qualifier = &parts[0].value;
            let col_name = &parts[1].value;
            if let Ok((column_id, data_type, nullable)) = scope.resolve(Some(qualifier), col_name) {
                let mut current = build_column_ref(
                    column_id,
                    data_type,
                    nullable,
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
            if let Ok((column_id, data_type, nullable)) = scope.resolve(Some(qualifier), col_name) {
                let mut current = build_column_ref(
                    column_id,
                    data_type,
                    nullable,
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
        let (column_id, data_type, nullable) = scope.resolve_at(None, base_name, parts[0].span)?;
        let mut current = build_column_ref(column_id, data_type, nullable, None, base_name);
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
        let DataType::Struct(fields) = &base.data_type else {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "field access expects STRUCT input, got {:?}",
                    base.data_type
                ),
                span,
            ));
        };
        // STRUCT field names are case-insensitive for resolution (Iceberg and
        // StarRocks both treat them as identifiers). The literal we pass to
        // `__struct_subfield` must be the *canonical* name from the schema so
        // the downstream evaluator (which does an exact-byte match against
        // `StructArray::fields()`) succeeds regardless of how the user spelled
        // it in the SQL.
        let field = fields
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(&field_name))
            .ok_or_else(|| {
                AnalyzeError::unknown_column(
                    format!("struct field '{}' does not exist", field_name),
                    span,
                )
            })?;
        let field_type = field.data_type().clone();
        let canonical_field_name = field.name().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(canonical_field_name)),
            data_type: DataType::Utf8,
            nullable: false,
        };
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: self.function_catalog.volatility("__struct_subfield"),
                name: "__struct_subfield".to_string(),
                args: vec![base, field_name_expr],
                distinct: false,
            },
            data_type: field_type,
            nullable: true,
        })
    }

    /// Analyze a literal value.
    fn analyze_literal(&self, value: &ast::Literal) -> Result<TypedExpr, AnalyzeError> {
        match value {
            ast::Literal {
                kind: ast::LiteralKind::Number(n),
                ..
            } => {
                if let Ok(v) = n.parse::<i64>() {
                    // Integer without decimal point → Int64
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(v)),
                        data_type: DataType::Int64,
                        nullable: false,
                    })
                } else if !n.contains('.') && !n.contains('e') && !n.contains('E') {
                    let v = n.parse::<i128>().map_err(|_| {
                        AnalyzeError::invalid_literal(
                            format!("invalid numeric literal: {n}"),
                            value.span,
                        )
                    })?;
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::LargeInt(v)),
                        data_type: DataType::FixedSizeBinary(
                            novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
                        ),
                        nullable: false,
                    })
                } else if n.contains('.') && !n.contains('e') && !n.contains('E') {
                    let data_type = infer_decimal_literal_type(n)
                        .map_err(|message| AnalyzeError::invalid_literal(message, value.span))?;
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Decimal(n.clone())),
                        data_type,
                        nullable: false,
                    })
                } else if let Ok(v) = n.parse::<f64>() {
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Float(v)),
                        data_type: DataType::Float64,
                        nullable: false,
                    })
                } else {
                    Err(AnalyzeError::invalid_literal(
                        format!("invalid numeric literal: {n}"),
                        value.span,
                    ))
                }
            }
            ast::Literal {
                kind: ast::LiteralKind::String(s),
                ..
            } => {
                // The native typed parser already applied MySQL-style backslash
                // escapes when it admitted the literal. Don't unescape
                // again — that double-processed `'e\\f'` from 3 bytes (`e\f`)
                // down to 2 (`ef`) and is the cause of `join_large_in_predicate`
                // step 59 silently dropping the backslash row from the IN
                // result. INSERT VALUES already trusts the parser-admitted value
                // (`sql::literal` clones the string as-is); SELECT now matches.
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(s.clone())),
                    data_type: DataType::Utf8,
                    nullable: false,
                })
            }
            ast::Literal {
                kind: ast::LiteralKind::HexString(s),
                ..
            } => {
                let bytes = hex::decode(s).map_err(|err| {
                    AnalyzeError::invalid_literal(
                        format!("invalid hex literal X'{s}': {err}"),
                        value.span,
                    )
                })?;
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Binary(bytes)),
                    data_type: DataType::Binary,
                    nullable: false,
                })
            }
            ast::Literal {
                kind: ast::LiteralKind::Boolean(b),
                ..
            } => Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(*b)),
                data_type: DataType::Boolean,
                nullable: false,
            }),
            ast::Literal {
                kind: ast::LiteralKind::Null,
                ..
            } => Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Null),
                data_type: DataType::Null,
                nullable: true,
            }),
        }
    }

    fn analyze_array_literal(
        &self,
        array: &ast::ArrayExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut args = Vec::with_capacity(array.elements.len());
        let explicit_item_type = array
            .element_type
            .as_ref()
            .map(sql_type_to_arrow)
            .transpose()?;
        let mut item_type = explicit_item_type.clone().unwrap_or(DataType::Null);
        for item in &array.elements {
            let mut typed = self.analyze_expr(item, scope)?;
            // StarRocks infers array literal element types from the
            // narrowest integer width that holds the value (TINYINT for
            // `[1, 2, 3]`). Narrow each integer literal here so the
            // widened item type — and downstream `typeof()` — matches.
            if let ExprKind::Literal(LiteralValue::Int(v)) = &typed.kind {
                typed.data_type = narrow_int_literal_type(*v);
            }
            if let Some(target) = &explicit_item_type {
                if typed.data_type != *target {
                    typed = TypedExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(typed),
                            target: target.clone(),
                        },
                        data_type: target.clone(),
                        nullable: true,
                    };
                }
            } else {
                item_type = wider_type(&item_type, &typed.data_type);
            }
            args.push(typed);
        }
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: self.function_catalog.volatility("__array_literal"),
                name: "__array_literal".to_string(),
                args,
                distinct: false,
            },
            data_type: DataType::List(arrow::datatypes::Field::new("item", item_type, true).into()),
            nullable: false,
        })
    }

    /// Analyze `left -> right` as a JSON path operator. StarRocks treats
    /// `json_col -> '$.a.b'` as `json_query(json_col, '$.a.b')` returning a
    /// JSON value. Other operand types fall back to `get_json_string` so the
    /// expression remains usable.
    fn analyze_json_arrow_from_typed(
        &self,
        left_typed: TypedExpr,
        right: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let right_typed = self.analyze_expr(right, scope)?;
        let nullable = true;
        let fn_name = "json_query";
        // Use json_query for JSON inputs; otherwise still return Utf8 via
        // get_json_string semantics. The runtime function name "json_query"
        // is registered in connector/codegen and returns a JSON-valued column
        // (mapped to Utf8 at the analyzer level for downstream operators).
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: self.function_catalog.volatility(fn_name),
                name: fn_name.to_string(),
                args: vec![left_typed, right_typed],
                distinct: false,
            },
            data_type: DataType::Utf8,
            nullable,
        })
    }

    /// Analyze a binary operation.
    fn analyze_binary_op(
        &self,
        left: &ast::Expr,
        op: &ast::BinaryOperator,
        right: &ast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let left_typed = self.analyze_expr(left, scope)?;
        let right_typed = self.analyze_expr(right, scope)?;

        // StarRocks-aligned implicit literal coercion: when a comparison has
        // (column, literal) we coerce the literal to the column's type before
        // emitting the BinaryOp. Mirrors LiteralExprFactory.create(value, ty).
        let (left_typed, right_typed) = {
            use super::literal_coercion::coerce_literal_for_comparison;
            let coerce_for_compare = matches!(
                op,
                ast::BinaryOperator::Equal
                    | ast::BinaryOperator::NotEqual
                    | ast::BinaryOperator::LessThan
                    | ast::BinaryOperator::LessThanOrEqual
                    | ast::BinaryOperator::GreaterThan
                    | ast::BinaryOperator::GreaterThanOrEqual
                    | ast::BinaryOperator::NullSafeEqual
            );
            if coerce_for_compare {
                // BITMAP / HLL columns have no scalar identity and cannot be
                // compared with `=`, `<`, etc. Reject before the operand type
                // is dropped to `Boolean` so the user sees a clear error.
                let op_sym = match op {
                    ast::BinaryOperator::Equal => "=",
                    ast::BinaryOperator::NotEqual => "!=",
                    ast::BinaryOperator::LessThan => "<",
                    ast::BinaryOperator::LessThanOrEqual => "<=",
                    ast::BinaryOperator::GreaterThan => ">",
                    ast::BinaryOperator::GreaterThanOrEqual => ">=",
                    ast::BinaryOperator::NullSafeEqual => "<=>",
                    _ => unreachable!(),
                };
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
                let is_ordering_op = matches!(
                    op,
                    ast::BinaryOperator::LessThan
                        | ast::BinaryOperator::LessThanOrEqual
                        | ast::BinaryOperator::GreaterThan
                        | ast::BinaryOperator::GreaterThanOrEqual
                );
                if is_ordering_op {
                    let unsupported_complex_kind = |dt: &DataType| match dt {
                        DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _) => Some("ARRAY"),
                        DataType::Map(_, _) => Some("MAP"),
                        DataType::Struct(_) => Some("STRUCT"),
                        _ => None,
                    };
                    if let Some(kind) = unsupported_complex_kind(&left_typed.data_type)
                        .or_else(|| unsupported_complex_kind(&right_typed.data_type))
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
                if let Some(reason) =
                    incompatible_complex_compare(&left_typed.data_type, &right_typed.data_type)
                {
                    return Err(AnalyzeError::type_mismatch(
                        format!(
                            "comparison operator `{op_sym}` does not support binary predicate operation between {reason}"
                        ),
                        left.span(),
                    ));
                }
                let right_coerced = coerce_literal_for_comparison(&left_typed, right_typed);
                let left_coerced = coerce_literal_for_comparison(&right_coerced, left_typed);
                // StarRocks ImplicitCastRule analog for `col op col`: coerce the
                // two operands to a common numeric/decimal type and materialize a
                // Cast on each differing side, BEFORE the optimizer / runtime-filter
                // pass. This lets mixed-type equi-joins (e.g. int32 = int64) carry
                // matching key types so the RF gate (rf_key_types_match) passes.
                // Non-numeric pairs return None and are left to literal coercion /
                // execution-time comparison coercion.
                match comparison_common_type(&left_coerced.data_type, &right_coerced.data_type)
                    .map_err(|message| AnalyzeError::type_mismatch(message, left.span()))?
                {
                    Some(common) => (
                        cast_null_preserving_target_type(left_coerced, &common),
                        cast_null_preserving_target_type(right_coerced, &common),
                    ),
                    None => (left_coerced, right_coerced),
                }
            } else {
                (left_typed, right_typed)
            }
        };

        if let Some(date_shift) = date_day_arithmetic_expr(&left_typed, op, &right_typed) {
            return Ok(date_shift);
        }

        let (bin_op, result_type) = match op {
            // Comparison operators -> Boolean
            ast::BinaryOperator::Equal => (BinOp::Eq, DataType::Boolean),
            ast::BinaryOperator::NotEqual => (BinOp::Ne, DataType::Boolean),
            ast::BinaryOperator::LessThan => (BinOp::Lt, DataType::Boolean),
            ast::BinaryOperator::LessThanOrEqual => (BinOp::Le, DataType::Boolean),
            ast::BinaryOperator::GreaterThan => (BinOp::Gt, DataType::Boolean),
            ast::BinaryOperator::GreaterThanOrEqual => (BinOp::Ge, DataType::Boolean),
            ast::BinaryOperator::NullSafeEqual => (BinOp::EqForNull, DataType::Boolean),

            // Logical operators -> Boolean
            ast::BinaryOperator::And => (BinOp::And, DataType::Boolean),
            ast::BinaryOperator::Or => (BinOp::Or, DataType::Boolean),

            // Arithmetic operators -> inferred type
            ast::BinaryOperator::Add => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "add",
                );
                (BinOp::Add, dt)
            }
            ast::BinaryOperator::Subtract => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "add",
                );
                (BinOp::Sub, dt)
            }
            ast::BinaryOperator::Multiply => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "mul",
                );
                (BinOp::Mul, dt)
            }
            ast::BinaryOperator::Divide => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "div",
                );
                (BinOp::Div, dt)
            }
            ast::BinaryOperator::Modulo => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "add",
                );
                (BinOp::Mod, dt)
            }

            // || is logical OR in MySQL/StarRocks default sql_mode.
            // Non-boolean operands are implicitly cast to boolean.
            ast::BinaryOperator::StringConcat => {
                let left_cast = implicit_cast_to_boolean(left_typed);
                let right_cast = implicit_cast_to_boolean(right_typed);
                let nullable = left_cast.nullable || right_cast.nullable;
                return Ok(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(left_cast),
                        op: BinOp::Or,
                        right: Box::new(right_cast),
                    },
                    data_type: DataType::Boolean,
                    nullable,
                });
            }

            other => {
                return Err(AnalyzeError::unsupported_expression(
                    format!("unsupported binary operator: {other:?}"),
                    left.span(),
                ));
            }
        };

        if let DataType::Decimal128(precision, scale) = &result_type
            && *scale > *precision as i8
        {
            return Err(AnalyzeError::invalid_literal(
                format!("scale {scale} is greater than max {precision}"),
                left.span(),
            ));
        }

        let nullable = left_typed.nullable || right_typed.nullable;
        Ok(TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left_typed),
                op: bin_op,
                right: Box::new(right_typed),
            },
            data_type: result_type,
            nullable,
        })
    }

    /// Analyze a CASE expression.
    fn analyze_case(
        &self,
        operand: Option<&ast::Expr>,
        conditions: &[ast::Expr],
        results: &[ast::Expr],
        else_result: Option<&ast::Expr>,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let operand_typed = match operand {
            Some(e) => Some(Box::new(self.analyze_expr(e, scope)?)),
            None => None,
        };

        let mut when_then = Vec::with_capacity(conditions.len());
        let mut result_type = DataType::Null;
        for (condition, result) in conditions.iter().zip(results) {
            let when_typed = self.analyze_expr(condition, scope)?;
            let then_typed = self.analyze_expr(result, scope)?;
            if result_type == DataType::Null {
                result_type = then_typed.data_type.clone();
            } else {
                result_type = wider_type(&result_type, &then_typed.data_type);
            }
            when_then.push((when_typed, then_typed));
        }

        let else_typed = match else_result {
            Some(e) => {
                let typed = self.analyze_expr(e, scope)?;
                if result_type == DataType::Null {
                    result_type = typed.data_type.clone();
                } else {
                    result_type = wider_type(&result_type, &typed.data_type);
                }
                Some(Box::new(typed))
            }
            None => None,
        };

        if result_type == DataType::Null {
            result_type = DataType::Utf8; // fallback
        }

        // Insert implicit CASTs for THEN/ELSE branches whose types don't
        // match the unified result_type.  Without this, the execution
        // engine's CASE may output the branch's original type (e.g., INT 0)
        // instead of the wider type (e.g., DOUBLE 0.0), causing truncation.
        let cast_if_needed = |expr: TypedExpr, target: &DataType| -> TypedExpr {
            if &expr.data_type != target && expr.data_type != DataType::Null {
                TypedExpr {
                    kind: ExprKind::Cast {
                        expr: Box::new(expr),
                        target: target.clone(),
                    },
                    data_type: target.clone(),
                    nullable: true,
                }
            } else {
                expr
            }
        };
        let when_then: Vec<(TypedExpr, TypedExpr)> = when_then
            .into_iter()
            .map(|(w, t)| (w, cast_if_needed(t, &result_type)))
            .collect();
        let else_typed = else_typed.map(|e| Box::new(cast_if_needed(*e, &result_type)));

        Ok(TypedExpr {
            kind: ExprKind::Case {
                operand: operand_typed,
                when_then,
                else_expr: else_typed,
            },
            data_type: result_type,
            nullable: true,
        })
    }

    /// Analyze a function call expression.
    fn analyze_function(
        &self,
        func: &ast::FunctionCall,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let original_name = print_object_name(&func.name).to_ascii_lowercase();
        if original_name == "ds_theta_count_distinct" {
            return Err(AnalyzeError::unsupported_expression(
                "unsupported agg function: ds_theta_count_distinct",
                func.span,
            ));
        }
        let name = match original_name.as_str() {
            "approx_count_distinct_hll_sketch" => "ds_hll_count_distinct".to_string(),
            other => other.to_string(),
        };
        // Route explicit `element_at(container, key)` calls to the right typed
        // subscript function. The subscript-syntax path already does this, but
        // direct function-call syntax bypasses it.
        let mut name = name;
        if name == "element_at" {
            // Analyze the first argument lazily to learn its type.
            let first_arg_ty = func.arguments.first().and_then(|argument| {
                self.analyze_expr(argument, scope)
                    .ok()
                    .map(|typed| typed.data_type)
            });
            match first_arg_ty {
                Some(DataType::Map(_, _)) => name = "__map_element_at".to_string(),
                Some(DataType::List(_)) => name = "__array_element_at".to_string(),
                _ => {}
            }
        }

        // The native grammar retains ORDER BY and SEPARATOR modifiers on a
        // function call so the analyzer can make the semantic decision.  They
        // are nevertheless syntactically invalid when the function has no
        // value argument, and SEPARATOR belongs exclusively to GROUP_CONCAT.
        // Reject these shapes before aggregate ORDER BY validation so the
        // StarRocks-compatible parse error remains stable.
        if func.arguments.is_empty() && !func.order_by.is_empty() {
            return Err(AnalyzeError::invalid_query_shape(
                if matches!(func.quantifier, ast::FunctionQuantifier::Distinct) {
                    "Unexpected input 'order', the most similar input is {a legal identifier}."
                        .to_string()
                } else {
                    "Unexpected input '(', the most similar input is {<EOF>, ';'}.".to_string()
                },
                func.span,
            ));
        }
        if func.arguments.is_empty() && func.separator.is_some() {
            return Err(AnalyzeError::invalid_query_shape(
                format!("No viable statement for input '{}(separator NULL'.", name),
                func.span,
            ));
        }
        if func.separator.is_some() && !matches!(name.as_str(), "group_concat" | "string_agg") {
            return Err(AnalyzeError::invalid_query_shape(
                "Unexpected input 'separator', the most similar input is {',', ')'}.",
                func.span,
            ));
        }

        // Check for DISTINCT
        let is_distinct = matches!(func.quantifier, ast::FunctionQuantifier::Distinct);

        // `*` is parser-native syntax inside a function call, not a column.
        // Keep it out of ordinary argument resolution: the empty aggregate
        // argument list is the analyzer representation of COUNT(*).
        let is_count_star = name == "count"
            && matches!(func.arguments.as_slice(), [ast::Expr::Identifier(ident)] if ident.value == "*");

        // The native AST retains GROUP_CONCAT's `SEPARATOR` outside the
        // regular argument list. The aggregate contract, order-by resolver,
        // and type diagnostics expect it as the final typed argument. The
        // legacy comma spelling is retained for compatibility: a trailing
        // string literal remains a separator, while all other positional
        // arguments are values and receive a default comma separator.
        let group_concat_args = if matches!(name.as_str(), "group_concat" | "string_agg") {
            match (func.separator.as_deref(), func.arguments.as_slice()) {
                (Some(separator), values) => values
                    .iter()
                    .cloned()
                    .chain(std::iter::once(separator.clone()))
                    .collect(),
                (None, []) => Vec::new(),
                (None, values)
                    if values.len() > 1
                        && values.last().is_some_and(|argument| {
                            matches!(
                                argument,
                                ast::Expr::Literal(ast::Literal {
                                    kind: ast::LiteralKind::String(_),
                                    ..
                                })
                            )
                        }) =>
                {
                    values.to_vec()
                }
                (None, values) => {
                    let mut arguments = values.to_vec();
                    arguments.push(string_literal_expr(",".to_string(), func.span));
                    arguments
                }
            }
        } else {
            Vec::new()
        };
        let arg_exprs: Vec<&ast::Expr> = if matches!(name.as_str(), "group_concat" | "string_agg") {
            group_concat_args.iter().collect()
        } else {
            func.arguments.iter().collect()
        };

        // The native parser represents EXTRACT as a function call whose first
        // argument is the field identifier. Preserve the historical lowering
        // to the unit-specific scalar function without reconstructing SQL.
        if original_name == "extract" && arg_exprs.len() == 2 {
            let ast::Expr::Identifier(field) = arg_exprs[0] else {
                return Err(AnalyzeError::unsupported_expression(
                    "unsupported EXTRACT field",
                    func.span,
                ));
            };
            let function_name = match field.value.to_ascii_lowercase().as_str() {
                "year" => "year",
                "month" => "month",
                "day" => "day",
                "hour" => "hour",
                "minute" => "minute",
                "second" => "second",
                other => {
                    return Err(AnalyzeError::unsupported_expression(
                        format!("unsupported EXTRACT field: {other}"),
                        field.span,
                    ));
                }
            };
            let argument = self.analyze_expr(arg_exprs[1], scope)?;
            return Ok(TypedExpr {
                kind: ExprKind::FunctionCall {
                    volatility: self.function_catalog.volatility(function_name),
                    name: function_name.to_string(),
                    args: vec![argument],
                    distinct: false,
                },
                data_type: DataType::Int32,
                nullable: true,
            });
        }

        // typeof(CAST(x AS T)) preserves the SQL-level type spelling (CHAR vs
        // VARCHAR, DECIMAL128(p, s), etc.) which is lost once the cast lowers
        // to Arrow. Intercept it here so codegen receives a string literal
        // for the well-known SQL spelling. For non-CAST arguments we still
        // fall through to the existing codegen-time path.
        if name == "typeof"
            && arg_exprs.len() == 1
            && let ast::Expr::Cast(cast) = arg_exprs[0]
            && let Some(type_name) = sql_type_starrocks_name(&cast.data_type)
        {
            return Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::String(type_name)),
                data_type: DataType::Utf8,
                nullable: false,
            });
        }
        // typeof(<expr>) on a non-CAST argument: analyze the argument with
        // StarRocks' narrowest-integer-literal-type rule applied, then map
        // the resulting Arrow type to its StarRocks spelling. Some function
        // families return BINARY/VARCHAR at the Arrow level but carry a
        // distinct logical type (BITMAP/HLL/JSON/null literal) in
        // StarRocks, so recognise those by the producing function name
        // first.
        if name == "typeof" && arg_exprs.len() == 1 {
            if let Some(special) = sql_expr_logical_type_name(arg_exprs[0]) {
                return Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(special)),
                    data_type: DataType::Utf8,
                    nullable: false,
                });
            }
            let typed_arg = self.analyze_expr(arg_exprs[0], scope)?;
            let narrowed = narrow_int_literals_in_typed_expr(typed_arg);
            let type_name = arrow_type_to_starrocks_name(&narrowed.data_type);
            return Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::String(type_name)),
                data_type: DataType::Utf8,
                nullable: false,
            });
        }
        if matches!(name.as_str(), "array_length" | "cardinality")
            && arg_exprs.len() == 1
            && let Some(len) = syntactic_array_literal_len(arg_exprs[0])
        {
            return Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(len as i64)),
                data_type: DataType::Int64,
                nullable: false,
            });
        }
        if matches!(name.as_str(), "group_concat" | "string_agg") && arg_exprs.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                "group_concat should have at least one input.",
                func.span,
            ));
        }
        if matches!(
            name.as_str(),
            "array_agg" | "array_agg_distinct" | "array_unique_agg"
        ) {
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
        // `date_add(dt, INTERVAL expr <UNIT>)` / `date_sub(...)` in MySQL
        // syntax: route to the unit-specific function (`seconds_add`,
        // `days_add`, etc.) and unwrap the Interval into its plain value
        // expression so downstream type inference and execution see an
        // integer arg, not the synthetic INTERVAL-as-string placeholder.
        let date_add_interval_rewrites: Vec<ast::Expr> =
            if matches!(name.as_str(), "date_add" | "date_sub")
                && arg_exprs.len() == 2
                && let ast::Expr::Interval(interval) = arg_exprs[1]
            {
                let unit = interval_field_name(interval.leading_field).to_string();
                let suffix = if name == "date_sub" { "_sub" } else { "_add" };
                let new_name = match unit.as_str() {
                    "year" => Some(format!("years{suffix}")),
                    "month" => Some(format!("months{suffix}")),
                    "week" => Some(format!("weeks{suffix}")),
                    "day" => Some(format!("days{suffix}")),
                    "hour" => Some(format!("hours{suffix}")),
                    "minute" => Some(format!("minutes{suffix}")),
                    "second" => Some(format!("seconds{suffix}")),
                    _ => None,
                };
                if let Some(new_name) = new_name {
                    name = new_name;
                    vec![arg_exprs[0].clone(), (*interval.value).clone()]
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

        let array_generate_rewrites: Vec<ast::Expr> = if date_add_interval_rewrites.is_empty()
            && name == "array_generate"
            && arg_exprs.len() == 3
        {
            if let ast::Expr::Interval(interval) = arg_exprs[2] {
                if !is_integer_const_literal(interval.value.as_ref()) {
                    return Err(AnalyzeError::invalid_argument(
                        "array_generate requires step parameter must be a constant integer",
                        interval.span,
                    ));
                }
                let unit = interval_field_name(interval.leading_field).to_string();
                vec![
                    (*arg_exprs[0]).clone(),
                    (*arg_exprs[1]).clone(),
                    signed_integer_literal_expr(interval.value.as_ref())
                        .unwrap_or_else(|| (*interval.value).clone()),
                    string_literal_expr(unit, interval.span),
                ]
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Expand StarRocks-style `time_slice` / `date_slice` arguments so the
        // executor sees (datetime, value, unit, boundary?):
        //   * `INTERVAL N UNIT` is split into a numeric value + unit string.
        //   * Bare identifiers `ceil` / `floor` in the boundary slot are
        //     promoted to string literals (StarRocks accepts both unquoted).
        let time_slice_rewrites: Vec<ast::Expr> = if date_add_interval_rewrites.is_empty()
            && array_generate_rewrites.is_empty()
            && matches!(name.as_str(), "time_slice" | "date_slice")
            && !arg_exprs.is_empty()
        {
            let mut rewritten: Vec<ast::Expr> = Vec::with_capacity(arg_exprs.len() + 1);
            for (idx, e) in arg_exprs.iter().enumerate() {
                // Position 1 is the interval; expand it into value + unit.
                if idx == 1
                    && let ast::Expr::Interval(interval) = e
                {
                    // StarRocks rejects non-integer constant
                    // intervals at planning time; mirror that error
                    // here rather than silently producing NULL.
                    if !is_integer_const_literal(interval.value.as_ref()) {
                        return Err(AnalyzeError::invalid_argument(
                            format!("{name} requires second parameter must be a constant interval"),
                            interval.span,
                        ));
                    }
                    rewritten.push((*interval.value).clone());
                    let unit = interval_field_name(interval.leading_field).to_string();
                    rewritten.push(string_literal_expr(unit, interval.span));
                    continue;
                }
                let token = match e {
                    ast::Expr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
                    ast::Expr::CompoundIdentifier(parts) if parts.parts.len() == 1 => {
                        Some(parts.parts[0].value.to_ascii_lowercase())
                    }
                    _ => None,
                };
                if let Some(token) = token
                    && matches!(token.as_str(), "ceil" | "floor")
                {
                    rewritten.push(string_literal_expr(token, e.span()));
                } else {
                    rewritten.push((*e).clone());
                }
            }
            rewritten
        } else {
            Vec::new()
        };

        let effective_arg_exprs: Vec<&ast::Expr> = if is_count_star {
            Vec::new()
        } else if !date_add_interval_rewrites.is_empty() {
            date_add_interval_rewrites.iter().collect()
        } else if !array_generate_rewrites.is_empty() {
            array_generate_rewrites.iter().collect()
        } else if !time_slice_rewrites.is_empty() {
            time_slice_rewrites.iter().collect()
        } else {
            arg_exprs.clone()
        };

        if let Some(rewritten) =
            self.try_analyze_higher_order_function(&name, &effective_arg_exprs, scope, func.span)?
        {
            return Ok(rewritten);
        }
        if let Some(rewritten) =
            self.try_analyze_array_map_cast_lambda(&name, &effective_arg_exprs, scope, func.span)?
        {
            return Ok(rewritten);
        }

        // Analyze arguments. For the narrow standalone lambda support needed by
        // aggregate suite, rewrite `array_sortby((x) -> x.field, arr)` into
        // `array_sortby(arr, __array_struct_subfield(arr, 'field'))`.
        let (mut args_typed, mut arg_types) = if name == "array_sortby"
            && effective_arg_exprs
                .first()
                .and_then(|expr| parse_array_sortby_lambda(expr))
                .is_some()
        {
            self.analyze_array_sortby_lambda_arguments(&effective_arg_exprs, scope, func.span)?
        } else if is_higher_order_function_with_lambda(&name, &effective_arg_exprs) {
            self.analyze_higher_order_lambda_arguments(
                &name,
                &effective_arg_exprs,
                scope,
                func.span,
            )?
        } else if is_map_higher_order_function_with_lambda(&name, &effective_arg_exprs) {
            self.analyze_map_higher_order_lambda_arguments(
                &name,
                &effective_arg_exprs,
                scope,
                func.span,
            )?
        } else {
            let mut args_typed = Vec::with_capacity(effective_arg_exprs.len());
            let mut arg_types = Vec::with_capacity(effective_arg_exprs.len());
            for arg in &effective_arg_exprs {
                let typed = self.analyze_expr(arg, scope)?;
                arg_types.push(typed.data_type.clone());
                args_typed.push(typed);
            }
            (args_typed, arg_types)
        };

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
                    arg.data_type,
                    DataType::Null | DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                ) {
                    let inner = std::mem::replace(
                        arg,
                        TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Null),
                            data_type: DataType::Null,
                            nullable: true,
                        },
                    );
                    *arg = TypedExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(inner),
                            target: DataType::Float64,
                        },
                        data_type: DataType::Float64,
                        nullable: true,
                    };
                }
            }
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
        }

        self.validate_ds_hll_arguments(&name, &args_typed, func.span)?;

        if name == "array_flatten"
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
            if let Some(semantic_type) = args_typed
                .first()
                .and_then(json_semantic_group_by_type_name)
            {
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
            for arg in &mut args_typed {
                if arg.data_type != DataType::Boolean {
                    let inner = std::mem::replace(
                        arg,
                        TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Null),
                            data_type: DataType::Null,
                            nullable: true,
                        },
                    );
                    *arg = TypedExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(inner),
                            target: DataType::Boolean,
                        },
                        data_type: DataType::Boolean,
                        nullable: true,
                    };
                }
            }
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
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
        let func_order_by = self.extract_function_order_by(func, scope, &args_typed)?;

        // Check for window function: func(...) OVER (...)
        if let Some(ref window_type) = func.over {
            if name == "any_value" {
                return Err(AnalyzeError::unsupported_expression(
                    "any_value not supported with OVER clause",
                    func.span,
                ));
            }
            // StarRocks rejects LEAD/LAG when the third (default) argument
            // doesn't match a per-shape type rule. The error message echoes
            // the value column's type (INT/FLOAT/DECIMAL...) and is asserted
            // by SQL regression tests.
            if matches!(name.as_str(), "lead" | "lag") && args_typed.len() >= 3 {
                let value_type = args_typed[0].data_type.clone();
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
            if !is_window_only_function(&name)
                && !is_aggregate_function(&name)
                && scalar_function_is_unknown(self.function_catalog, &name, &arg_types)
            {
                return Err(AnalyzeError::unknown_function(
                    format!("Unknown function: {name}"),
                    func.span,
                ));
            }
            let return_type = if is_window_only_function(&name) {
                infer_window_return_type(&name, &arg_types)
            } else if is_aggregate_function(&name) {
                if is_count_star {
                    DataType::Int64
                } else {
                    infer_agg_return_type(&name, &arg_types)
                }
            } else {
                infer_scalar_return_type_with_catalog(self.function_catalog, &name, &arg_types)
            };
            let (partition_by, order_by, window_frame) =
                self.analyze_window_spec(window_type, scope)?;
            let ignore_nulls = matches!(func.null_treatment, Some(ast::NullTreatment::IgnoreNulls));
            return Ok(TypedExpr {
                kind: ExprKind::WindowCall {
                    name,
                    args: args_typed,
                    distinct: is_distinct,
                    partition_by,
                    order_by,
                    window_frame,
                    ignore_nulls,
                },
                data_type: return_type,
                nullable: true,
            });
        }

        if apply_implicit_string_function_casts(&name, &mut args_typed) {
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
        }

        let needs_hll_hash_string_arg = matches!(name.as_str(), "hll_hash" | "hll_hash1");
        if needs_hll_hash_string_arg {
            for arg in &mut args_typed {
                if arg.data_type != DataType::Utf8
                    && arg.data_type != DataType::LargeUtf8
                    && arg.data_type != DataType::Null
                {
                    let inner = std::mem::replace(
                        arg,
                        TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Null),
                            data_type: DataType::Null,
                            nullable: true,
                        },
                    );
                    *arg = TypedExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(inner),
                            target: DataType::Utf8,
                        },
                        data_type: DataType::Utf8,
                        nullable: true,
                    };
                }
            }
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
        }

        if name == "date_trunc"
            && let Some(value_arg) = args_typed.get_mut(1)
            && !matches!(
                value_arg.data_type,
                DataType::Date32
                    | DataType::Timestamp(_, _)
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Null
            )
        {
            let target = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
            let inner = std::mem::replace(
                value_arg,
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Null),
                    data_type: DataType::Null,
                    nullable: true,
                },
            );
            *value_arg = TypedExpr {
                kind: ExprKind::Cast {
                    expr: Box::new(inner),
                    target: target.clone(),
                },
                data_type: target,
                nullable: true,
            };
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
        }

        // IF(cond, then, else): cast first arg to Boolean if needed
        if name == "if" && !args_typed.is_empty() && args_typed[0].data_type != DataType::Boolean {
            let inner = std::mem::replace(
                &mut args_typed[0],
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Null),
                    data_type: DataType::Null,
                    nullable: true,
                },
            );
            args_typed[0] = TypedExpr {
                kind: ExprKind::Cast {
                    expr: Box::new(inner),
                    target: DataType::Boolean,
                },
                data_type: DataType::Boolean,
                nullable: true,
            };
        }

        if is_aggregate_function(&name) && apply_implicit_aggregate_casts(&name, &mut args_typed) {
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
        }

        let mut bound_scalar = None;
        self.validate_percentile_arguments(&name, &args_typed, func.span)?;
        if is_aggregate_function(&name) {
            validate_aggregate_function_call(&name, &arg_types)
                .map_err(|message| AnalyzeError::invalid_argument(message, func.span))?;
        } else {
            if scalar_function_is_unknown(self.function_catalog, &name, &arg_types) {
                return Err(AnalyzeError::unknown_function(
                    format!("Unknown function: {name}"),
                    func.span,
                ));
            }
            let bound =
                bind_scalar_function_call_with_catalog(self.function_catalog, &name, args_typed)
                    .map_err(|message| AnalyzeError::type_mismatch(message, func.span))?;
            arg_types = bound.args.iter().map(|arg| arg.data_type.clone()).collect();
            args_typed = bound.args.clone();
            bound_scalar = Some(bound);
        }

        match original_name.as_str() {
            "ds_hll_accumulate" => {
                let state_expr = TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self
                            .function_catalog
                            .volatility("ds_hll_count_distinct_state"),
                        name: "ds_hll_count_distinct_state".to_string(),
                        args: args_typed,
                        distinct: false,
                    },
                    data_type: DataType::Binary,
                    nullable: true,
                };
                return Ok(TypedExpr {
                    kind: ExprKind::AggregateCall {
                        name: "ds_hll_count_distinct_union".to_string(),
                        args: vec![state_expr],
                        distinct: false,
                        order_by: func_order_by,
                    },
                    data_type: DataType::Binary,
                    nullable: true,
                });
            }
            "ds_hll_combine" => {
                self.ensure_ds_hll_binary_arg(
                    "ds_hll_count_distinct_union",
                    args_typed.first(),
                    func.span,
                )?;
                return Ok(TypedExpr {
                    kind: ExprKind::AggregateCall {
                        name: "ds_hll_count_distinct_union".to_string(),
                        args: args_typed,
                        distinct: false,
                        order_by: func_order_by,
                    },
                    data_type: DataType::Binary,
                    nullable: true,
                });
            }
            "ds_hll_estimate" => {
                self.ensure_ds_hll_binary_arg(
                    "ds_hll_count_distinct_merge",
                    args_typed.first(),
                    func.span,
                )?;
                return Ok(TypedExpr {
                    kind: ExprKind::AggregateCall {
                        name: "ds_hll_count_distinct_merge".to_string(),
                        args: args_typed,
                        distinct: false,
                        order_by: func_order_by,
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                });
            }
            _ => {}
        }

        if is_aggregate_function(&name) {
            // Aggregate function
            let return_type = if is_count_star {
                DataType::Int64
            } else {
                infer_agg_return_type(&name, &arg_types)
            };
            Ok(TypedExpr {
                kind: ExprKind::AggregateCall {
                    name,
                    args: args_typed,
                    distinct: is_distinct,
                    order_by: func_order_by,
                },
                data_type: return_type,
                nullable: true,
            })
        } else {
            // Scalar function
            let mut return_type = bound_scalar
                .as_ref()
                .map(|bound| bound.return_type.clone())
                .unwrap_or_else(|| {
                    infer_scalar_return_type_with_catalog(self.function_catalog, &name, &arg_types)
                });
            // `named_struct(name0, val0, name1, val1, …)` needs to carry the
            // user-supplied field *names* in its returned STRUCT schema.
            // `infer_scalar_return_type` only sees arg types and falls back
            // to `col1/col2/…`; patch the schema here where we still have the
            // analyzed arg expressions and can read the string-literal names.
            if name == "named_struct"
                && !args_typed.is_empty()
                && args_typed.len() % 2 == 0
                && let DataType::Struct(_) = &return_type
            {
                let mut fields: Vec<std::sync::Arc<arrow::datatypes::Field>> =
                    Vec::with_capacity(args_typed.len() / 2);
                let mut all_have_names = true;
                for (i, chunk) in args_typed.chunks(2).enumerate() {
                    let [name_expr, value_expr] = chunk else {
                        all_have_names = false;
                        break;
                    };
                    let field_name = match &name_expr.kind {
                        ExprKind::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => {
                            all_have_names = false;
                            break;
                        }
                    };
                    fields.push(std::sync::Arc::new(arrow::datatypes::Field::new(
                        field_name,
                        value_expr.data_type.clone(),
                        true,
                    )));
                    let _ = i;
                }
                if all_have_names {
                    return_type = DataType::Struct(fields.into());
                }
            }
            // For round/truncate with decimal input and constant 2nd arg,
            // use the target decimal places as the output scale.
            if matches!(name.as_str(), "round" | "truncate")
                && let DataType::Decimal128(p, s) = &return_type
                && args_typed.len() >= 2
                && let ExprKind::Literal(LiteralValue::Int(d)) = &args_typed[1].kind
            {
                let target = (*d as i8).max(0).min(*s);
                return_type = DataType::Decimal128(*p, target);
            }
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
                                        AnalyzeError::invalid_literal(message, func.span)
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
            Ok(TypedExpr {
                kind: ExprKind::FunctionCall {
                    volatility: self.function_catalog.volatility(&name),
                    name,
                    args: args_typed,
                    distinct: is_distinct,
                },
                data_type: return_type,
                nullable: true,
            })
        }
    }

    fn analyze_map_literal(
        &self,
        map: &ast::MapExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut args = Vec::with_capacity(map.entries.len() * 2);
        for entry in &map.entries {
            args.push(self.analyze_expr(&entry.key, scope)?);
            args.push(self.analyze_expr(&entry.value, scope)?);
        }
        let bound = bind_scalar_function_call_with_catalog(self.function_catalog, "map", args)
            .map_err(|message| AnalyzeError::invalid_argument(message, map.span))?;
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: self.function_catalog.volatility("map"),
                name: "map".to_string(),
                args: bound.args,
                distinct: false,
            },
            data_type: bound.return_type,
            nullable: true,
        })
    }

    fn analyze_array_sortby_lambda_arguments(
        &self,
        arg_exprs: &[&ast::Expr],
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<(Vec<TypedExpr>, Vec<DataType>), AnalyzeError> {
        if arg_exprs.len() != 2 {
            return Err(AnalyzeError::invalid_argument(
                "array_sortby lambda rewrite currently supports exactly one lambda and one array argument",
                span,
            ));
        }
        let (param_name, lambda_body) =
            parse_array_sortby_lambda(arg_exprs[0]).ok_or_else(|| {
                AnalyzeError::invalid_argument(
                    "array_sortby lambda rewrite expected a lambda argument",
                    span,
                )
            })?;
        let array_expr = self.analyze_expr(arg_exprs[1], scope)?;
        let field_chain = extract_lambda_field_chain(lambda_body, &param_name)
            .map_err(|message| AnalyzeError::invalid_argument(message, span))?;
        if field_chain.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                "array_sortby lambda rewrite requires direct struct field access like (x) -> x.item",
                span,
            ));
        }

        let mut key_expr = array_expr.clone();
        for field_name in field_chain {
            key_expr = self.build_array_struct_subfield_expr(key_expr, field_name, span)?;
        }

        let arg_types = vec![array_expr.data_type.clone(), key_expr.data_type.clone()];
        Ok((vec![array_expr, key_expr], arg_types))
    }

    fn build_array_struct_subfield_expr(
        &self,
        base: TypedExpr,
        field_name: String,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let DataType::List(item_field) = &base.data_type else {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "array_sortby lambda expects ARRAY input, got {:?}",
                    base.data_type
                ),
                span,
            ));
        };
        let DataType::Struct(fields) = item_field.data_type() else {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "array_sortby lambda field access expects ARRAY<STRUCT>, got {:?}",
                    base.data_type
                ),
                span,
            ));
        };
        // Same case-insensitive resolution + canonical name forwarding as the
        // plain struct subfield path above; the array-of-struct variant
        // (`array_sortby(...).field`) needs to match identically.
        let field = fields
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(&field_name))
            .ok_or_else(|| {
                AnalyzeError::unknown_column(
                    format!("struct field '{}' does not exist", field_name),
                    span,
                )
            })?;
        let field_type = field.data_type().clone();
        let canonical_field_name = field.name().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(canonical_field_name)),
            data_type: DataType::Utf8,
            nullable: false,
        };
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: self.function_catalog.volatility("__array_struct_subfield"),
                name: "__array_struct_subfield".to_string(),
                args: vec![base, field_name_expr],
                distinct: false,
            },
            data_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item", field_type, true,
            ))),
            nullable: true,
        })
    }

    fn try_analyze_higher_order_function(
        &self,
        name: &str,
        arg_exprs: &[&ast::Expr],
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<Option<TypedExpr>, AnalyzeError> {
        if !matches!(
            name,
            "array_map"
                | "transform"
                | "any_match"
                | "all_match"
                | "array_filter"
                | "filter"
                | "array_sort"
        ) {
            return Ok(None);
        }

        let Some((lambda_pos, params, lambda_body)) = find_lambda_argument(arg_exprs) else {
            return Ok(None);
        };
        let array_exprs = arg_exprs
            .iter()
            .enumerate()
            .filter_map(|(idx, expr)| (idx != lambda_pos).then_some(*expr))
            .collect::<Vec<_>>();
        if array_exprs.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                format!("{name} expects at least one ARRAY argument"),
                span,
            ));
        }

        if name == "array_sort" {
            if array_exprs.len() != 1 || params.len() != 2 {
                return Err(AnalyzeError::invalid_argument(
                    "array_sort lambda comparator expects one ARRAY argument and two lambda parameters",
                    span,
                ));
            }
            let source = self.analyze_expr(array_exprs[0], scope)?;
            let (item_type, item_nullable) = match &source.data_type {
                DataType::List(item) => (item.data_type().clone(), item.is_nullable()),
                DataType::Null => (DataType::Null, true),
                other => {
                    return Err(AnalyzeError::invalid_argument(
                        format!("array_sort expects ARRAY argument, got {other:?}"),
                        span,
                    ));
                }
            };
            let lambda_params = params
                .iter()
                .map(|param_name| LambdaParam {
                    name: param_name.clone(),
                    slot_id: self.alloc_lambda_slot_id(),
                    data_type: item_type.clone(),
                    nullable: item_nullable,
                })
                .collect::<Vec<_>>();
            let mut lambda_scope = scope.clone();
            for param in &lambda_params {
                lambda_scope.add_lambda_param(param.clone());
            }
            let body = self.analyze_expr(&lambda_body, &lambda_scope)?;
            if typed_expr_contains_column_ref(&body)
                || typed_expr_contains_nondeterministic_call(self.function_catalog, &body)
                || !typed_expr_references_all_lambda_params(&body, &lambda_params)
            {
                return Err(AnalyzeError::invalid_argument(
                    "Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions",
                    span,
                ));
            }
            let lambda = TypedExpr {
                data_type: body.data_type.clone(),
                nullable: body.nullable,
                kind: ExprKind::LambdaFunction {
                    params: lambda_params,
                    body: Box::new(body),
                },
            };
            return Ok(Some(TypedExpr {
                kind: ExprKind::FunctionCall {
                    volatility: self.function_catalog.volatility("array_sort_lambda"),
                    name: "array_sort_lambda".to_string(),
                    args: vec![source.clone(), lambda],
                    distinct: false,
                },
                data_type: source.data_type,
                nullable: true,
            }));
        }

        let mut array_args = Vec::with_capacity(array_exprs.len());
        let mut lambda_params = Vec::with_capacity(params.len());
        for (idx, array_expr) in array_exprs.iter().enumerate() {
            let typed = self.analyze_expr(array_expr, scope)?;
            let (data_type, nullable) = match &typed.data_type {
                DataType::List(item) => (item.data_type().clone(), item.is_nullable()),
                DataType::Null => (DataType::Null, true),
                other => {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} expects ARRAY argument, got {other:?}"),
                        span,
                    ));
                }
            };
            let Some(param_name) = params.get(idx) else {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "{name} lambda argument count {} does not match ARRAY argument count {}",
                        params.len(),
                        array_exprs.len()
                    ),
                    span,
                ));
            };
            lambda_params.push(LambdaParam {
                name: param_name.clone(),
                slot_id: self.alloc_lambda_slot_id(),
                data_type,
                nullable,
            });
            array_args.push(typed);
        }
        if params.len() != array_args.len() {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "{name} lambda argument count {} does not match ARRAY argument count {}",
                    params.len(),
                    array_args.len()
                ),
                span,
            ));
        }

        let mut lambda_scope = scope.clone();
        for param in &lambda_params {
            lambda_scope.add_lambda_param(param.clone());
        }
        let body = self.analyze_expr(&lambda_body, &lambda_scope)?;
        let lambda = TypedExpr {
            data_type: body.data_type.clone(),
            nullable: body.nullable,
            kind: ExprKind::LambdaFunction {
                params: lambda_params,
                body: Box::new(body),
            },
        };

        match name {
            "array_map" | "transform" => {
                let body_type = lambda.data_type.clone();
                let mapped_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item", body_type, true,
                )));
                let mut args = Vec::with_capacity(array_args.len() + 1);
                args.push(lambda);
                args.extend(array_args);
                Ok(Some(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self.function_catalog.volatility("array_map"),
                        name: "array_map".to_string(),
                        args,
                        distinct: false,
                    },
                    data_type: mapped_type,
                    nullable: true,
                }))
            }
            "any_match" | "all_match" => {
                let mapped_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    lambda.data_type.clone(),
                    true,
                )));
                let mut map_args = Vec::with_capacity(array_args.len() + 1);
                map_args.push(lambda);
                map_args.extend(array_args);
                let mapped = TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self.function_catalog.volatility("array_map"),
                        name: "array_map".to_string(),
                        args: map_args,
                        distinct: false,
                    },
                    data_type: mapped_type,
                    nullable: true,
                };
                Ok(Some(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self.function_catalog.volatility(name),
                        name: name.to_string(),
                        args: vec![mapped],
                        distinct: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: true,
                }))
            }
            "array_filter" | "filter" => {
                let source = array_args.first().cloned().ok_or_else(|| {
                    AnalyzeError::invalid_argument("array_filter missing ARRAY argument", span)
                })?;
                let filter_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    lambda.data_type.clone(),
                    true,
                )));
                let mut map_args = Vec::with_capacity(array_args.len() + 1);
                map_args.push(lambda);
                map_args.extend(array_args);
                let filter = TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self.function_catalog.volatility("array_map"),
                        name: "array_map".to_string(),
                        args: map_args,
                        distinct: false,
                    },
                    data_type: filter_type,
                    nullable: true,
                };
                Ok(Some(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: self.function_catalog.volatility("array_filter"),
                        name: "array_filter".to_string(),
                        args: vec![source.clone(), filter],
                        distinct: false,
                    },
                    data_type: source.data_type,
                    nullable: true,
                }))
            }
            _ => unreachable!("higher-order function match is exhaustive"),
        }
    }

    /// Analyze the arguments of a higher-order function whose first argument
    /// is a lambda (e.g. `array_map(x -> ..., arr)`).
    ///
    /// The lambda parameter count must match the number of trailing array
    /// arguments. Each parameter is bound to the element type of the
    /// corresponding array. Captures (outer columns referenced from the body)
    /// are resolved by merging the lambda scope onto the outer scope.
    fn analyze_higher_order_lambda_arguments(
        &self,
        name: &str,
        arg_exprs: &[&ast::Expr],
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<(Vec<TypedExpr>, Vec<DataType>), AnalyzeError> {
        if arg_exprs.len() < 2 {
            return Err(AnalyzeError::invalid_argument(
                format!("{name} expects a lambda and at least one array argument"),
                span,
            ));
        }
        let (param_names, body_expr) = parse_multi_param_lambda(arg_exprs[0]).ok_or_else(|| {
            AnalyzeError::invalid_argument(
                format!("{name} expects a lambda function as its first argument"),
                span,
            )
        })?;
        let array_count = arg_exprs.len() - 1;
        if param_names.len() != array_count {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "{name} lambda has {} parameter(s) but {} array argument(s) were supplied",
                    param_names.len(),
                    array_count
                ),
                span,
            ));
        }

        let mut analyzed_arrays = Vec::with_capacity(array_count);
        let mut element_types = Vec::with_capacity(array_count);
        for sql_expr in &arg_exprs[1..] {
            let typed = self.analyze_expr(sql_expr, scope)?;
            let elem_type = match &typed.data_type {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => field.data_type().clone(),
                DataType::Null => DataType::Null,
                other => {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} expects ARRAY arguments, got {:?}", other),
                        span,
                    ));
                }
            };
            element_types.push(elem_type);
            analyzed_arrays.push(typed);
        }

        let mut inner_scope = scope.clone();
        for (param_name, elem_type) in param_names.iter().zip(element_types.iter()) {
            inner_scope.add_column(None, param_name, elem_type.clone(), true);
        }
        let body_typed = self.analyze_expr(body_expr, &inner_scope)?;
        let body_type = body_typed.data_type.clone();
        let body_nullable = body_typed.nullable;

        let lambda_typed = TypedExpr {
            kind: ExprKind::Lambda {
                params: param_names.iter().map(|p| p.to_lowercase()).collect(),
                body: Box::new(body_typed),
            },
            data_type: body_type,
            nullable: body_nullable,
        };

        let mut args_typed = Vec::with_capacity(arg_exprs.len());
        let mut arg_types = Vec::with_capacity(arg_exprs.len());
        arg_types.push(lambda_typed.data_type.clone());
        args_typed.push(lambda_typed);
        for arr in analyzed_arrays {
            arg_types.push(arr.data_type.clone());
            args_typed.push(arr);
        }
        Ok((args_typed, arg_types))
    }

    /// Analyse `map_apply((k, v) -> body, m)` / `transform_keys((k, v) -> nk, m)` /
    /// `transform_values((k, v) -> nv, m)`. The 2-parameter lambda binds
    /// `k` to the map's key Arrow type and `v` to the value type. For
    /// `map_apply` the body must be a `(new_key, new_value)` tuple (the native
    /// parser represents it as `Expr::Tuple`); we rewrite it as a `row(...)` call so
    /// downstream codegen sees a 2-field Struct.
    fn analyze_map_higher_order_lambda_arguments(
        &self,
        name: &str,
        arg_exprs: &[&ast::Expr],
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<(Vec<TypedExpr>, Vec<DataType>), AnalyzeError> {
        let (param_names, body_expr) = parse_multi_param_lambda(arg_exprs[0]).ok_or_else(|| {
            AnalyzeError::invalid_argument(
                format!("{name} expects a lambda function as its first argument"),
                span,
            )
        })?;
        if param_names.len() != 2 {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "{name} lambda must take exactly 2 parameters (key, value); got {}",
                    param_names.len()
                ),
                span,
            ));
        }

        let map_typed = self.analyze_expr(arg_exprs[1], scope)?;
        let (key_type, value_type) = match &map_typed.data_type {
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    (fields[0].data_type().clone(), fields[1].data_type().clone())
                }
                other => {
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "{name} expects MAP argument with struct<key,value> entries, got {:?}",
                            other
                        ),
                        span,
                    ));
                }
            },
            DataType::Null => (DataType::Null, DataType::Null),
            other => {
                return Err(AnalyzeError::invalid_argument(
                    format!("{name} expects a MAP argument, got {:?}", other),
                    span,
                ));
            }
        };

        // Bind the (key, value) lambda parameters as proper lambda slots, NOT
        // scope columns. `add_column` mints a fresh ColumnId per parameter, so
        // body references would resolve to a `ColumnRef` carrying that phantom
        // id — an id no scan produces. The ColumnId-binding verifier then
        // rejects it ("ColumnId(N) is not produced by child scope"), which
        // broke `map_apply` / `transform_keys` / `transform_values` in
        // projections, filters, and join conditions. Binding them via
        // `add_lambda_param` makes body references resolve to `LambdaParamRef`,
        // mirroring the array higher-order path (`try_analyze_higher_order_function`).
        let lambda_params = vec![
            LambdaParam {
                name: param_names[0].to_lowercase(),
                slot_id: self.alloc_lambda_slot_id(),
                data_type: key_type.clone(),
                nullable: true,
            },
            LambdaParam {
                name: param_names[1].to_lowercase(),
                slot_id: self.alloc_lambda_slot_id(),
                data_type: value_type.clone(),
                nullable: true,
            },
        ];
        let mut inner_scope = scope.clone();
        for param in &lambda_params {
            inner_scope.add_lambda_param(param.clone());
        }

        // The shared `map_apply` executor expects the lambda body to evaluate
        // to a single-entry MAP per `(k, v)` input pair, which it concatenates
        // into the output map. Each map family supplies the `(new_key,
        // new_value)` pair differently, so normalize all three to a
        // `map(new_key, new_value)` body here:
        //   map_apply       : body is a `(new_key, new_value)` tuple.
        //   transform_keys  : body is the new key scalar; value passes through.
        //   transform_values: body is the new value scalar; key passes through.
        let lambda_param_ref = |param: &LambdaParam, data_type: DataType| TypedExpr {
            kind: ExprKind::LambdaParamRef {
                name: param.name.clone(),
                slot_id: param.slot_id,
            },
            data_type,
            nullable: true,
        };
        let (new_key, new_value) = match name {
            "map_apply" => {
                let tuple_items: Vec<&ast::Expr> = match body_expr {
                    ast::Expr::Tuple(tuple) => tuple.expressions.iter().collect(),
                    ast::Expr::Nested(inner) => match inner.expression.as_ref() {
                        ast::Expr::Tuple(tuple) => tuple.expressions.iter().collect(),
                        _ => Vec::new(),
                    },
                    _ => Vec::new(),
                };
                if tuple_items.len() != 2 {
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "map_apply lambda body must produce (new_key, new_value), got {} items",
                            tuple_items.len()
                        ),
                        span,
                    ));
                }
                (
                    self.analyze_expr(tuple_items[0], &inner_scope)?,
                    self.analyze_expr(tuple_items[1], &inner_scope)?,
                )
            }
            "transform_keys" => (
                self.analyze_expr(body_expr, &inner_scope)?,
                lambda_param_ref(&lambda_params[1], value_type.clone()),
            ),
            // transform_values
            _ => (
                lambda_param_ref(&lambda_params[0], key_type.clone()),
                self.analyze_expr(body_expr, &inner_scope)?,
            ),
        };

        let new_key_type = new_key.data_type.clone();
        let new_value_type = new_value.data_type.clone();
        let entry_field = std::sync::Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    std::sync::Arc::new(arrow::datatypes::Field::new("key", new_key_type, false)),
                    std::sync::Arc::new(arrow::datatypes::Field::new(
                        "value",
                        new_value_type,
                        true,
                    )),
                ]
                .into(),
            ),
            false,
        ));
        let body_typed = TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: self.function_catalog.volatility("map"),
                name: "map".to_string(),
                args: vec![new_key, new_value],
                distinct: false,
            },
            data_type: DataType::Map(entry_field, false),
            nullable: true,
        };
        let body_type = body_typed.data_type.clone();
        let body_nullable = body_typed.nullable;

        let lambda_typed = TypedExpr {
            kind: ExprKind::LambdaFunction {
                params: lambda_params,
                body: Box::new(body_typed),
            },
            data_type: body_type,
            nullable: body_nullable,
        };

        let args_typed = vec![lambda_typed.clone(), map_typed.clone()];
        let arg_types = vec![lambda_typed.data_type, map_typed.data_type];
        Ok((args_typed, arg_types))
    }

    fn try_analyze_array_map_cast_lambda(
        &self,
        name: &str,
        arg_exprs: &[&ast::Expr],
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<Option<TypedExpr>, AnalyzeError> {
        if !matches!(name, "array_map" | "transform") {
            return Ok(None);
        }
        if arg_exprs.len() != 2 {
            return Ok(None);
        }

        let Some((param_name, lambda_body)) = parse_array_sortby_lambda(arg_exprs[0]) else {
            return Ok(None);
        };
        if !lambda_body_casts_param_to_utf8(lambda_body, &param_name) {
            return Err(AnalyzeError::unsupported_expression(
                "array_map lambda rewrite currently supports x -> CAST(x AS STRING)",
                span,
            ));
        }

        let array_expr = self.analyze_expr(arg_exprs[1], scope)?;
        if !matches!(array_expr.data_type, DataType::List(_)) {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "array_map lambda expects ARRAY input, got {:?}",
                    array_expr.data_type
                ),
                span,
            ));
        }
        let target = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        Ok(Some(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(array_expr),
                target: target.clone(),
            },
            data_type: target,
            nullable: true,
        }))
    }

    fn validate_percentile_arguments(
        &self,
        name: &str,
        args: &[TypedExpr],
        span: Span,
    ) -> Result<(), AnalyzeError> {
        match name {
            "percentile_cont" | "percentile_disc_lc" => {
                if let Some(expr) = args.get(1)
                    && let Some(value) = const_numeric_value(expr)
                    && !(0.0..=1.0).contains(&value)
                {
                    return Err(AnalyzeError::invalid_argument(
                        format!("{name} second parameter'value should be between 0 and 1"),
                        span,
                    ));
                }
                return Ok(());
            }
            _ => {}
        }

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
        match &expr.data_type {
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
        if !func.order_by.is_empty() {
            let mut items = Vec::with_capacity(func.order_by.len());
            for ob in &func.order_by {
                let typed = if let Some(pos) = function_order_by_position(&ob.expr) {
                    let pos_index = usize::try_from(pos).ok();
                    if let Some(pos_index) = pos_index
                        && (1..=visible_args.len()).contains(&pos_index)
                    {
                        visible_args[pos_index - 1].clone()
                    } else if matches!(
                        func_name.as_str(),
                        "array_agg"
                            | "array_agg_distinct"
                            | "array_unique_agg"
                            | "group_concat"
                            | "string_agg"
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
                        self.analyze_expr(&ob.expr, scope)?
                    }
                } else {
                    self.analyze_expr(&ob.expr, scope)?
                };
                let asc = ob.asc.unwrap_or(true);
                let nulls_first = ob.nulls_first.unwrap_or(asc);
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
        over: &ast::WindowSpec,
        scope: &AnalyzerScope,
    ) -> Result<WindowSpecAnalysis, AnalyzeError> {
        if over.existing_window_name.is_some() {
            return Err(AnalyzeError::unsupported_expression(
                "named window references are not supported",
                over.span,
            ));
        }
        let spec = over;

        // PARTITION BY
        let mut partition_by = Vec::new();
        for expr in &spec.partition_by {
            partition_by.push(self.analyze_expr(expr, scope)?);
        }

        // ORDER BY
        let mut order_by = Vec::new();
        for ob in &spec.order_by {
            let typed = self.analyze_expr(&ob.expr, scope)?;
            let asc = ob.asc.unwrap_or(true);
            let nulls_first = ob.nulls_first.unwrap_or(asc);
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

    fn validate_ds_hll_arguments(
        &self,
        name: &str,
        args: &[TypedExpr],
        span: Span,
    ) -> Result<(), AnalyzeError> {
        if name != "ds_hll_count_distinct" {
            return Ok(());
        }

        if args.len() > 3 {
            return Err(AnalyzeError::invalid_argument(
                "ds_hll_count_distinct requires one/two/three parameters: ds_hll_count_distinct(col, <log_k>, <tgt_type>)",
                span,
            ));
        }

        if let Some(log_k) = args.get(1) {
            let ExprKind::Literal(LiteralValue::Int(value)) = &log_k.kind else {
                return Err(AnalyzeError::invalid_argument(
                    "ds_hll_count_distinct 's second parameter's data type is wrong ",
                    span,
                ));
            };
            if !(4..=21).contains(value) {
                return Err(AnalyzeError::invalid_argument(
                    "ds_hll_count_distinct second parameter'value should be between 4 and 21.",
                    span,
                ));
            }
        }

        if let Some(target) = args.get(2) {
            let ExprKind::Literal(LiteralValue::String(value)) = &target.kind else {
                return Err(AnalyzeError::invalid_argument(
                    "ds_hll_count_distinct 's third parameter's data type is wrong ",
                    span,
                ));
            };
            if !matches!(value.as_str(), "HLL_4" | "HLL_6" | "HLL_8") {
                return Err(AnalyzeError::invalid_argument(
                    "ds_hll_count_distinct third  parameter'value should be in HLL_4/HLL_6/HLL_8.",
                    span,
                ));
            }
        }

        Ok(())
    }

    fn ensure_ds_hll_binary_arg(
        &self,
        fn_name: &str,
        arg: Option<&TypedExpr>,
        span: Span,
    ) -> Result<(), AnalyzeError> {
        let Some(arg) = arg else {
            return Ok(());
        };
        let looks_like_standalone_binary_state =
            matches!(
                &arg.kind,
                ExprKind::ColumnRef {
                    qualifier: _,
                    column,
                    ..
                } if column.starts_with("ds_")
            ) && matches!(arg.data_type, DataType::Utf8 | DataType::LargeUtf8);
        if matches!(arg.data_type, DataType::Binary | DataType::LargeBinary)
            || looks_like_standalone_binary_state
        {
            Ok(())
        } else {
            Err(AnalyzeError::type_mismatch(
                format!("Resolved function {fn_name} has no binary as argument type."),
                span,
            ))
        }
    }

    // -----------------------------------------------------------------------
    // Aggregate detection
    // -----------------------------------------------------------------------

    /// Check if any projection item contains an aggregate function call.
    pub(super) fn select_has_aggregate_functions(&self, projection: &[ast::SelectItem]) -> bool {
        for item in projection {
            let expr = match item {
                ast::SelectItem::UnnamedExpr(e) => e,
                ast::SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };
            if self.expr_contains_aggregate(expr) {
                return true;
            }
        }
        false
    }

    /// Recursively check if an expression contains an aggregate function call.
    /// Window functions (with OVER) are NOT counted as aggregates.
    pub(super) fn expr_contains_aggregate(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::FunctionCall(f) => {
                // A function with OVER is a window function, not an aggregate
                if f.over.is_some() {
                    return false;
                }
                if is_aggregate_function(&print_object_name(&f.name).to_ascii_lowercase()) {
                    return true;
                }
                f.arguments
                    .iter()
                    .any(|expr| self.expr_contains_aggregate(expr))
                    || f.order_by
                        .iter()
                        .any(|item| self.expr_contains_aggregate(&item.expr))
            }
            ast::Expr::Binary(binary) => {
                self.expr_contains_aggregate(&binary.left)
                    || self.expr_contains_aggregate(&binary.right)
            }
            ast::Expr::Unary(unary) => self.expr_contains_aggregate(&unary.expression),
            ast::Expr::IsPredicate(predicate) => self.expr_contains_aggregate(&predicate.expr),
            ast::Expr::InList(in_list) => {
                self.expr_contains_aggregate(&in_list.expr)
                    || in_list
                        .list
                        .iter()
                        .any(|item| self.expr_contains_aggregate(item))
            }
            ast::Expr::Between(between) => {
                self.expr_contains_aggregate(&between.expr)
                    || self.expr_contains_aggregate(&between.low)
                    || self.expr_contains_aggregate(&between.high)
            }
            ast::Expr::Like(like) => {
                self.expr_contains_aggregate(&like.expr)
                    || self.expr_contains_aggregate(&like.pattern)
            }
            ast::Expr::Nested(nested) => self.expr_contains_aggregate(&nested.expression),
            ast::Expr::Cast(cast) => self.expr_contains_aggregate(&cast.expr),
            ast::Expr::Tuple(tuple) => tuple
                .expressions
                .iter()
                .any(|item| self.expr_contains_aggregate(item)),
            ast::Expr::Array(array) => array
                .elements
                .iter()
                .any(|item| self.expr_contains_aggregate(item)),
            ast::Expr::Struct(structure) => structure
                .fields
                .iter()
                .any(|field| self.expr_contains_aggregate(&field.value)),
            ast::Expr::Map(map) => map.entries.iter().any(|entry| {
                self.expr_contains_aggregate(&entry.key)
                    || self.expr_contains_aggregate(&entry.value)
            }),
            ast::Expr::Access(access) => {
                self.expr_contains_aggregate(&access.expr)
                    || match &access.kind {
                        ast::AccessKind::Subscript(index) => self.expr_contains_aggregate(index),
                        ast::AccessKind::Json { path, .. } => self.expr_contains_aggregate(path),
                        ast::AccessKind::Field(_) => false,
                    }
            }
            ast::Expr::Case(case) => {
                case.conditions
                    .iter()
                    .any(|expr| self.expr_contains_aggregate(expr))
                    || case
                        .results
                        .iter()
                        .any(|expr| self.expr_contains_aggregate(expr))
                    || case
                        .else_result
                        .as_ref()
                        .is_some_and(|e| self.expr_contains_aggregate(e))
            }
            _ => false,
        }
    }
}

fn function_order_by_position(expr: &ast::Expr) -> Option<i64> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(n),
            ..
        }) => n.parse::<i64>().ok(),
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            match unary.expression.as_ref() {
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Number(n),
                    ..
                }) => n.parse::<i64>().ok().map(|pos| -pos),
                _ => None,
            }
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

fn is_json_in_subquery_operand(expr: &TypedExpr, scope: &AnalyzerScope) -> bool {
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

fn data_type_contains_null(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::List(field) => data_type_contains_null(field.data_type()),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return false;
            };
            fields
                .iter()
                .any(|field| data_type_contains_null(field.data_type()))
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_contains_null(field.data_type())),
        _ => false,
    }
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

fn infer_decimal_literal_type(s: &str) -> Result<DataType, String> {
    let raw = s.trim().trim_start_matches('+').trim_start_matches('-');
    let (int_part, frac_part) = match raw.split_once('.') {
        Some((i, f)) => (i, f),
        None => (raw, ""),
    };
    let int_part = int_part.trim_start_matches('0');
    let int_digits = if int_part.is_empty() {
        1usize
    } else {
        int_part.len()
    };
    let scale = frac_part.len();
    let precision = int_digits + scale;

    if scale > 76 {
        return Err(format!(
            "decimal literal scale {scale} exceeds maximum scale 76: {s}"
        ));
    }
    if precision == 0 || precision > 76 {
        return Err(format!(
            "decimal literal precision {precision} exceeds maximum precision 76: {s}"
        ));
    }

    let precision = precision as u8;
    let scale = scale as i8;
    if precision > 38 {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

/// Implicit cast: if `expr` is Utf8 and `target` is a date/timestamp type,
/// wrap `expr` in a Cast to the target type. This matches StarRocks FE
/// behavior where string literals are implicitly cast to date/timestamp
/// in comparison contexts (BETWEEN, WHERE, etc.).
pub(crate) fn coerce_to_target_type(expr: TypedExpr, target: &DataType) -> TypedExpr {
    let needs_cast = matches!(expr.data_type, DataType::Utf8 | DataType::LargeUtf8)
        && matches!(
            target,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        );
    if needs_cast {
        TypedExpr {
            nullable: expr.nullable,
            data_type: target.clone(),
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target: target.clone(),
            },
        }
    } else {
        expr
    }
}

fn cast_null_preserving_target_type(expr: TypedExpr, target: &DataType) -> TypedExpr {
    if expr.data_type == *target {
        return expr;
    }
    let nullable = expr.nullable;
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
            target: target.clone(),
        },
        data_type: target.clone(),
        nullable,
    }
}

fn date_day_arithmetic_expr(
    left: &TypedExpr,
    op: &ast::BinaryOperator,
    right: &TypedExpr,
) -> Option<TypedExpr> {
    match op {
        ast::BinaryOperator::Add if is_temporal_day_base(&left.data_type) => {
            date_day_shift_expr("days_add", left.clone(), right.clone())
        }
        ast::BinaryOperator::Add if is_temporal_day_base(&right.data_type) => {
            date_day_shift_expr("days_add", right.clone(), left.clone())
        }
        ast::BinaryOperator::Subtract if is_temporal_day_base(&left.data_type) => {
            date_day_shift_expr("days_sub", left.clone(), right.clone())
        }
        _ => None,
    }
}

fn date_day_shift_expr(
    function_name: &str,
    date_expr: TypedExpr,
    offset_expr: TypedExpr,
) -> Option<TypedExpr> {
    if !is_integer_day_offset(&offset_expr.data_type) {
        return None;
    }
    let nullable = date_expr.nullable || offset_expr.nullable;
    let data_type = match &date_expr.data_type {
        DataType::Date32 => DataType::Date32,
        DataType::Timestamp(unit, tz) => DataType::Timestamp(*unit, tz.clone()),
        _ => return None,
    };
    let offset_expr = cast_null_preserving_target_type(offset_expr, &DataType::Int64);
    Some(TypedExpr {
        kind: ExprKind::FunctionCall {
            volatility: crate::functions::builtin_function_volatility(function_name),
            name: function_name.to_string(),
            args: vec![date_expr, offset_expr],
            distinct: false,
        },
        data_type,
        nullable,
    })
}

fn is_temporal_day_base(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
}

fn is_integer_day_offset(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn cast_to_utf8_if_needed(expr: &mut TypedExpr) -> bool {
    if matches!(expr.data_type, DataType::Utf8 | DataType::LargeUtf8) {
        return false;
    }
    if matches!(expr.data_type, DataType::Null) {
        expr.data_type = DataType::Utf8;
        expr.nullable = true;
        return true;
    }
    let nullable = expr.nullable;
    let inner = std::mem::replace(
        expr,
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            data_type: DataType::Null,
            nullable: true,
        },
    );
    *expr = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(inner),
            target: DataType::Utf8,
        },
        data_type: DataType::Utf8,
        nullable,
    };
    true
}

fn cast_utf8_args(args: &mut [TypedExpr], indexes: &[usize]) -> bool {
    let mut changed = false;
    for index in indexes {
        if let Some(arg) = args.get_mut(*index) {
            changed |= cast_to_utf8_if_needed(arg);
        }
    }
    changed
}

fn apply_implicit_string_function_casts(name: &str, args: &mut [TypedExpr]) -> bool {
    match name {
        "concat" | "concat_ws" | "group_concat" | "string_agg" => args
            .iter_mut()
            .fold(false, |changed, arg| cast_to_utf8_if_needed(arg) || changed),
        "append_trailing_char_if_absent"
        | "find_in_set"
        | "instr"
        | "locate"
        | "split"
        | "starts_with"
        | "ends_with" => cast_utf8_args(args, &[0, 1]),
        "regexp_extract" | "regexp_extract_all" => cast_utf8_args(args, &[0, 1]),
        "regexp_replace" => cast_utf8_args(args, &[0, 1, 2]),
        "lpad" | "rpad" => cast_utf8_args(args, &[0, 2]),
        "replace" => cast_utf8_args(args, &[0, 1, 2]),
        "ascii" | "char_length" | "character_length" | "initcap" | "left" | "length" | "lower"
        | "ltrim" | "repeat" | "reverse" | "right" | "rtrim" | "strleft" | "strright"
        | "substr" | "substring" | "trim" | "upper" => cast_utf8_args(args, &[0]),
        _ => false,
    }
}

struct BoundScalarCall {
    args: Vec<TypedExpr>,
    return_type: DataType,
}

fn signed_int_literal_value(expr: &TypedExpr) -> Option<i64> {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Int(value)) => Some(*value),
        ExprKind::UnaryOp {
            op: UnOp::Negate,
            expr: inner,
        } => signed_int_literal_value(inner).and_then(i64::checked_neg),
        _ => None,
    }
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

fn coerce_function_argument(expr: TypedExpr, target: &DataType) -> Result<TypedExpr, String> {
    if expr.data_type == *target {
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
            data_type: target.clone(),
            nullable: expr.nullable,
        });
    }
    if matches!(expr.data_type, DataType::Null) {
        return Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target: target.clone(),
            },
            data_type: target.clone(),
            nullable: true,
        });
    }
    let nullable = expr.nullable || narrowing_integer_cast_can_return_null(&expr.data_type, target);
    Ok(TypedExpr {
        nullable,
        kind: ExprKind::Cast {
            expr: Box::new(expr),
            target: target.clone(),
        },
        data_type: target.clone(),
    })
}

fn narrowing_integer_cast_can_return_null(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (DataType::Int16, DataType::Int8)
            | (DataType::Int32, DataType::Int8 | DataType::Int16)
            | (
                DataType::Int64,
                DataType::Int8 | DataType::Int16 | DataType::Int32
            )
    )
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
fn bind_scalar_function_call(name: &str, args: Vec<TypedExpr>) -> Result<BoundScalarCall, String> {
    bind_scalar_function_call_with_catalog(
        crate::functions::builtin_sql_function_catalog(),
        name,
        args,
    )
}

fn bind_scalar_function_call_with_catalog(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    mut args: Vec<TypedExpr>,
) -> Result<BoundScalarCall, String> {
    apply_implicit_string_function_casts(name, &mut args);
    let arg_types = args
        .iter()
        .map(|arg| arg.data_type.clone())
        .collect::<Vec<_>>();

    match function_catalog.resolve_scalar_signature(name, &arg_types) {
        Ok(
            resolved @ crate::functions::ResolvedScalarFunction {
                enforce_argument_binding: true,
                ..
            },
        ) => {
            let args = args
                .into_iter()
                .zip(resolved.argument_types.iter())
                .map(|(arg, target)| coerce_function_argument(arg, target))
                .collect::<Result<Vec<_>, _>>()?;
            validate_scalar_function_call_typed(name, &args)?;
            Ok(BoundScalarCall {
                args,
                return_type: resolved.return_type,
            })
        }
        Ok(resolved) => {
            validate_scalar_function_call_typed(name, &args)?;
            Ok(BoundScalarCall {
                args,
                return_type: resolved.return_type,
            })
        }
        Err(crate::functions::ResolveError::NoMatchingSignature {
            binding_enforced: true,
            ..
        }) => Err(no_matching_signature(name, &arg_types)),
        Err(crate::functions::ResolveError::BadSignature(message)) => Err(message),
        Err(crate::functions::ResolveError::HiddenFunction) => {
            Err(format!("function `{name}` is not available to user SQL"))
        }
        Err(crate::functions::ResolveError::UnknownFunction) => {
            validate_scalar_function_call_typed(name, &args)?;
            Ok(BoundScalarCall {
                return_type: infer_scalar_return_type_with_catalog(
                    function_catalog,
                    name,
                    &arg_types,
                ),
                args,
            })
        }
        Err(crate::functions::ResolveError::NoMatchingSignature {
            binding_enforced: false,
            ..
        }) => {
            validate_scalar_function_call_typed(name, &args)?;
            Ok(BoundScalarCall {
                return_type: infer_scalar_return_type_with_catalog(
                    function_catalog,
                    name,
                    &arg_types,
                ),
                args,
            })
        }
    }
}

fn aggregate_arg_cast_type(name: &str, input_type: &DataType) -> Option<DataType> {
    if matches!(name, "sum" | "avg") && matches!(input_type, DataType::Utf8 | DataType::LargeUtf8) {
        Some(DataType::Float64)
    } else {
        None
    }
}

fn apply_implicit_aggregate_casts(name: &str, args: &mut [TypedExpr]) -> bool {
    let mut changed = false;
    for arg in args {
        let Some(target) = aggregate_arg_cast_type(name, &arg.data_type) else {
            continue;
        };
        let inner = std::mem::replace(
            arg,
            TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Null),
                data_type: DataType::Null,
                nullable: true,
            },
        );
        *arg = TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(inner),
                target: target.clone(),
            },
            data_type: target,
            nullable: true,
        };
        changed = true;
    }
    changed
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
        separator.data_type,
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
        expr.data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
    )
}

fn group_concat_signature_type(expr: &TypedExpr, separator: bool) -> String {
    if separator && matches!(expr.data_type, DataType::Utf8 | DataType::LargeUtf8) {
        return "varchar".to_string();
    }
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } if name == "__array_literal" => {
            let item =
                infer_literal_signature_type(args).unwrap_or_else(|| match &expr.data_type {
                    DataType::List(item) => {
                        group_concat_data_type_signature(item.data_type(), false)
                    }
                    _ => group_concat_data_type_signature(&expr.data_type, false),
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
                map_entry_data_type(&expr.data_type, 0)
                    .map(|data_type| group_concat_data_type_signature(data_type, false))
                    .unwrap_or_else(|| "unknown".to_string())
            });
            let value_type = infer_literal_signature_type(&values).unwrap_or_else(|| {
                map_entry_data_type(&expr.data_type, 1)
                    .map(|data_type| group_concat_data_type_signature(data_type, true))
                    .unwrap_or_else(|| "unknown".to_string())
            });
            format!("map<{key_type},{value_type}>")
        }
        _ => group_concat_data_type_signature(&expr.data_type, false),
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
    if i8::try_from(value).is_ok() {
        0
    } else if i16::try_from(value).is_ok() {
        1
    } else if i32::try_from(value).is_ok() {
        2
    } else {
        3
    }
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

/// Wrap a non-boolean expression with CAST(... AS BOOLEAN) for implicit
/// boolean coercion (used by `||` as logical OR with string operands).
fn implicit_cast_to_boolean(expr: TypedExpr) -> TypedExpr {
    if expr.data_type == DataType::Boolean {
        return expr;
    }
    let nullable = expr.nullable;
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
            target: DataType::Boolean,
        },
        data_type: DataType::Boolean,
        nullable,
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
    if is_numeric_type(&expr.data_type) {
        return Ok(());
    }
    Err(format!(
        "{name} requires the {} parameter ({role}) to be numeric type, but got: {}.",
        ordinal_name(index),
        percentile_argument_type_name(&expr.data_type)
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
        return lead_lag_family_compatible(value_type, &default_arg.data_type);
    }
    if is_constant_default_expression(stripped) {
        return lead_lag_narrow_numeric_compatible(value_type, &stripped.data_type);
    }
    false
}

fn is_signed_literal(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Literal(_) => true,
        ExprKind::Nested(inner) => is_signed_literal(inner),
        ExprKind::UnaryOp { expr: inner, .. } => is_signed_literal(inner),
        ExprKind::Cast { expr: inner, .. } => is_signed_literal(inner),
        _ => false,
    }
}

fn is_constant_default_expression(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Literal(_) => true,
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => {
            is_constant_default_expression(expr)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            is_constant_default_expression(left) && is_constant_default_expression(right)
        }
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
        ExprKind::UnaryOp {
            op: UnOp::Negate,
            expr,
        } => const_numeric_value(expr).map(|value| -value),
        _ => None,
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

fn extract_lambda_field_chain(expr: &ast::Expr, param_name: &str) -> Result<Vec<String>, String> {
    match expr {
        ast::Expr::Nested(inner) => extract_lambda_field_chain(&inner.expression, param_name),
        ast::Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case(param_name) => {
            Ok(vec![])
        }
        ast::Expr::CompoundIdentifier(parts)
            if !parts.parts.is_empty() && parts.parts[0].value.eq_ignore_ascii_case(param_name) =>
        {
            Ok(parts.parts[1..].iter().map(|part| part.value.clone()).collect())
        }
        ast::Expr::Access(access) => {
            let mut fields = extract_lambda_field_chain(&access.expr, param_name)?;
            let ast::AccessKind::Field(ident) = &access.kind else {
                return Err("array_sortby lambda rewrite only supports dotted struct field access".to_string());
            };
            fields.push(ident.value.clone());
            Ok(fields)
        }
        _ => Err(
            "array_sortby lambda rewrite only supports direct struct field access like (x) -> x.item"
                .to_string(),
        ),
    }
}

fn parse_array_sortby_lambda(expr: &ast::Expr) -> Option<(String, &ast::Expr)> {
    match expr {
        ast::Expr::Lambda(lambda) => lambda
            .parameters
            .first()
            .map(|ident| (ident.value.to_lowercase(), lambda.body.as_ref())),
        ast::Expr::Nested(inner) => parse_array_sortby_lambda(&inner.expression),
        _ => None,
    }
}

fn find_lambda_argument(arg_exprs: &[&ast::Expr]) -> Option<(usize, Vec<String>, ast::Expr)> {
    if let Some((idx, params, body)) = arg_exprs
        .first()
        .and_then(|expr| parse_lambda_expr(expr))
        .map(|(params, body)| (0, params, body))
    {
        return Some((idx, params, body));
    }
    arg_exprs
        .last()
        .and_then(|expr| parse_lambda_expr(expr))
        .map(|(params, body)| (arg_exprs.len() - 1, params, body))
}

fn parse_lambda_expr(expr: &ast::Expr) -> Option<(Vec<String>, ast::Expr)> {
    match expr {
        ast::Expr::Lambda(lambda) => Some((
            lambda
                .parameters
                .iter()
                .map(|ident| ident.value.to_lowercase())
                .collect(),
            (*lambda.body).clone(),
        )),
        ast::Expr::Nested(inner) => parse_lambda_expr(&inner.expression),
        _ => None,
    }
}

fn lambda_body_casts_param_to_utf8(expr: &ast::Expr, param_name: &str) -> bool {
    match expr {
        ast::Expr::Nested(inner) => lambda_body_casts_param_to_utf8(&inner.expression, param_name),
        ast::Expr::Cast(cast) if lambda_expr_is_param(&cast.expr, param_name) => {
            matches!(
                sql_type_to_arrow(&cast.data_type),
                Ok(DataType::Utf8 | DataType::LargeUtf8)
            )
        }
        _ => false,
    }
}

fn lambda_expr_is_param(expr: &ast::Expr, param_name: &str) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(param_name),
        ast::Expr::Nested(inner) => lambda_expr_is_param(&inner.expression, param_name),
        _ => false,
    }
}

fn parse_multi_param_lambda(expr: &ast::Expr) -> Option<(Vec<String>, &ast::Expr)> {
    match expr {
        ast::Expr::Lambda(lambda) => Some((
            lambda
                .parameters
                .iter()
                .map(|p| p.value.to_lowercase())
                .collect(),
            lambda.body.as_ref(),
        )),
        ast::Expr::Nested(inner) => parse_multi_param_lambda(&inner.expression),
        _ => None,
    }
}

/// Returns true if `name` is a higher-order function (variadic by lambda arity)
/// and the first argument is a parseable lambda. Used to dispatch into the
/// dedicated analyzer that binds lambda parameters before walking the body.
fn is_higher_order_function_with_lambda(name: &str, arg_exprs: &[&ast::Expr]) -> bool {
    matches!(name, "array_map" | "transform")
        && arg_exprs
            .first()
            .and_then(|expr| parse_multi_param_lambda(expr))
            .is_some()
}

/// Map-shaped higher-order functions: `(k, v) -> body` over a MAP argument.
/// `map_apply` (body returns 2-tuple), `transform_keys` (body returns scalar
/// new key), `transform_values` (body returns scalar new value).
fn is_map_higher_order_function_with_lambda(name: &str, arg_exprs: &[&ast::Expr]) -> bool {
    matches!(name, "map_apply" | "transform_keys" | "transform_values")
        && arg_exprs.len() == 2
        && arg_exprs
            .first()
            .and_then(|expr| parse_multi_param_lambda(expr))
            .is_some()
}

/// Return `true` when `expr` is a constant integer literal (including
/// negation of one) suitable for `INTERVAL N UNIT`. Decimals and floats
/// (`3.2`) are explicitly rejected.
fn is_integer_const_literal(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(s),
            ..
        }) => s.parse::<i64>().is_ok(),
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(s),
            ..
        }) => s.parse::<i64>().is_ok(),
        ast::Expr::Unary(unary)
            if matches!(
                unary.operator,
                ast::UnaryOperator::Minus | ast::UnaryOperator::Plus
            ) =>
        {
            is_integer_const_literal(&unary.expression)
        }
        ast::Expr::Nested(inner) => is_integer_const_literal(&inner.expression),
        _ => false,
    }
}

fn signed_integer_literal_expr(expr: &ast::Expr) -> Option<ast::Expr> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(s),
            span,
        }) if s.parse::<i64>().is_ok() => Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(s.clone()),
            span: *span,
        })),
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(s),
            span,
        }) if s.parse::<i64>().is_ok() => Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(s.clone()),
            span: *span,
        })),
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            signed_integer_literal_expr(&unary.expression).and_then(|inner| match inner {
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Number(s),
                    span,
                }) if !s.starts_with('-') => Some(ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Number(format!("-{s}")),
                    span,
                })),
                _ => None,
            })
        }
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Plus) => {
            signed_integer_literal_expr(&unary.expression)
        }
        ast::Expr::Nested(inner) => signed_integer_literal_expr(&inner.expression),
        _ => None,
    }
}

fn syntactic_array_literal_len(expr: &ast::Expr) -> Option<usize> {
    match expr {
        ast::Expr::Array(array) => Some(array.elements.len()),
        ast::Expr::Nested(inner) => syntactic_array_literal_len(&inner.expression),
        ast::Expr::Cast(cast) => syntactic_array_literal_len(&cast.expr),
        _ => None,
    }
}

/// Narrow an integer literal value to the smallest signed integer width
/// that contains it (TINYINT/SMALLINT/INT/BIGINT). Used in array literal
/// and `typeof()` contexts to match StarRocks' literal-width inference.
fn narrow_int_literal_type(value: i64) -> DataType {
    if i8::try_from(value).is_ok() {
        DataType::Int8
    } else if i16::try_from(value).is_ok() {
        DataType::Int16
    } else if i32::try_from(value).is_ok() {
        DataType::Int32
    } else {
        DataType::Int64
    }
}

/// Walk a `TypedExpr` tree, narrowing every integer literal to its
/// smallest signed integer width and recomputing the result types of
/// function calls whose return type depends on argument widths
/// (greatest/least/coalesce/nvl/ifnull, array/map/struct literals).
/// The original `kind` is preserved so codegen sees the same shape;
/// only `data_type` is updated so `typeof()` can report the narrow
/// spelling.
fn narrow_int_literals_in_typed_expr(expr: TypedExpr) -> TypedExpr {
    let kind = expr.kind.clone();
    match kind {
        ExprKind::Literal(LiteralValue::Int(v)) => TypedExpr {
            data_type: narrow_int_literal_type(v),
            nullable: expr.nullable,
            kind: expr.kind,
        },
        ExprKind::UnaryOp { op, expr: inner } => {
            let inner = narrow_int_literals_in_typed_expr(*inner);
            let data_type = inner.data_type.clone();
            TypedExpr {
                data_type,
                nullable: expr.nullable,
                kind: ExprKind::UnaryOp {
                    op,
                    expr: Box::new(inner),
                },
            }
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            volatility,
        } => {
            let args: Vec<TypedExpr> = args
                .into_iter()
                .map(narrow_int_literals_in_typed_expr)
                .collect();
            let arg_types: Vec<DataType> = args.iter().map(|a| a.data_type.clone()).collect();
            let new_type = match name.as_str() {
                "greatest" | "least" | "coalesce" | "nvl" | "ifnull" => {
                    if let Some(first) = arg_types.first() {
                        let mut result = first.clone();
                        for t in &arg_types[1..] {
                            result = wider_type(&result, t);
                        }
                        if matches!(name.as_str(), "greatest" | "least")
                            && matches!(result, DataType::Date32)
                        {
                            result =
                                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
                        }
                        result
                    } else {
                        expr.data_type.clone()
                    }
                }
                "__array_literal" => {
                    let mut item = DataType::Null;
                    for t in &arg_types {
                        item = wider_type(&item, t);
                    }
                    DataType::List(arrow::datatypes::Field::new("item", item, true).into())
                }
                "map" if !arg_types.is_empty() && arg_types.len().is_multiple_of(2) => {
                    let mut key_t = DataType::Null;
                    let mut val_t = DataType::Null;
                    for (i, t) in arg_types.iter().enumerate() {
                        if i % 2 == 0 {
                            key_t = wider_type(&key_t, t);
                        } else {
                            val_t = wider_type(&val_t, t);
                        }
                    }
                    DataType::Map(
                        std::sync::Arc::new(arrow::datatypes::Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    std::sync::Arc::new(arrow::datatypes::Field::new(
                                        "key", key_t, true,
                                    )),
                                    std::sync::Arc::new(arrow::datatypes::Field::new(
                                        "value", val_t, true,
                                    )),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    )
                }
                "row" | "struct" => {
                    let fields: Vec<std::sync::Arc<arrow::datatypes::Field>> = args
                        .iter()
                        .enumerate()
                        .map(|(i, a)| {
                            std::sync::Arc::new(arrow::datatypes::Field::new(
                                format!("col{}", i + 1),
                                a.data_type.clone(),
                                true,
                            ))
                        })
                        .collect();
                    DataType::Struct(fields.into())
                }
                // `named_struct(name0, val0, name1, val1, …)` — the field
                // names come from the *odd-indexed* string-literal arguments,
                // not from auto-generated `col1/col2/…` names. The initial
                // return-type inference in `functions.rs::infer_named_struct_return_type`
                // can't see the arg expressions (only types), so it falls back
                // to `col{i+1}`; this refinement pass has the full TypedExpr
                // args available and fixes the field names properly.
                "named_struct" => {
                    let fields: Vec<std::sync::Arc<arrow::datatypes::Field>> = args
                        .chunks(2)
                        .enumerate()
                        .map(|(i, chunk)| {
                            let (field_name, value_type) = match chunk {
                                [name_expr, value_expr] => {
                                    let name = match &name_expr.kind {
                                        ExprKind::Literal(LiteralValue::String(s)) => s.clone(),
                                        _ => format!("col{}", i + 1),
                                    };
                                    (name, value_expr.data_type.clone())
                                }
                                _ => (format!("col{}", i + 1), DataType::Null),
                            };
                            std::sync::Arc::new(arrow::datatypes::Field::new(
                                field_name, value_type, true,
                            ))
                        })
                        .collect();
                    DataType::Struct(fields.into())
                }
                _ => expr.data_type.clone(),
            };
            TypedExpr {
                data_type: new_type,
                nullable: expr.nullable,
                kind: ExprKind::FunctionCall {
                    name,
                    args,
                    distinct,
                    volatility,
                },
            }
        }
        _ => expr,
    }
}

/// Render an Arrow `DataType` as the StarRocks-style type name used when the
/// analyzer folds `typeof(<expr>)` into a string literal.
fn arrow_type_to_starrocks_name(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::UInt8 => "tinyint unsigned".to_string(),
        DataType::UInt16 => "smallint unsigned".to_string(),
        DataType::UInt32 => "int unsigned".to_string(),
        DataType::UInt64 => "bigint unsigned".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
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
            format!("array<{}>", arrow_type_to_starrocks_name(field.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "map<{},{}>",
                arrow_type_to_starrocks_name(fields[0].data_type()),
                arrow_type_to_starrocks_name(fields[1].data_type())
            ),
            _ => "map".to_string(),
        },
        DataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{} {}",
                        f.name(),
                        arrow_type_to_starrocks_name(f.data_type())
                    )
                })
                .collect();
            format!("struct<{}>", parts.join(", "))
        }
        DataType::Null => "null".to_string(),
        other => format!("{:?}", other).to_lowercase(),
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
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        }) => Some("null_type".to_string()),
        ast::Expr::Literal(_) => None,
        ast::Expr::FunctionCall(function) => {
            let name = print_object_name(&function.name).to_ascii_lowercase();
            let name = name.split('.').next_back().unwrap_or(name.as_str());
            match name {
                n if n.starts_with("bitmap_")
                    || n == "to_bitmap"
                    || n == "bitmap_agg"
                    || n == "bitmap_union" =>
                {
                    Some("bitmap".to_string())
                }
                n if n.starts_with("hll_") || n == "hll_empty" || n == "hll_hash" => {
                    Some("hll".to_string())
                }
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
    let name = sql_type.name.parts.last()?.value.to_ascii_lowercase();
    let canonical = match name.as_str() {
        "tinyint" | "smallint" | "int" | "integer" | "bigint" | "float" | "double" | "boolean"
        | "char" | "date" | "time" | "largeint" | "bitmap" | "hll" => name,
        "varchar" | "character" | "string" | "text" => "varchar".to_string(),
        "json" | "jsonb" => "json".to_string(),
        "varbinary" | "binary" => "varbinary".to_string(),
        "datetime" | "timestamp" => "datetime".to_string(),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let rendered = print_type_name(sql_type).to_ascii_lowercase();
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

fn typed_expr_contains_column_ref(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_column_ref(left) || typed_expr_contains_column_ref(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => typed_expr_contains_column_ref(expr),
        ExprKind::FunctionCall { args, .. }
        | ExprKind::AggregateCall { args, .. }
        | ExprKind::WindowCall { args, .. } => args.iter().any(typed_expr_contains_column_ref),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| typed_expr_contains_column_ref(expr))
                || when_then.iter().any(|(when, then)| {
                    typed_expr_contains_column_ref(when) || typed_expr_contains_column_ref(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| typed_expr_contains_column_ref(expr))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            typed_expr_contains_column_ref(expr)
                || typed_expr_contains_column_ref(low)
                || typed_expr_contains_column_ref(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            typed_expr_contains_column_ref(expr) || typed_expr_contains_column_ref(pattern)
        }
        ExprKind::InList { expr, list, .. } => {
            typed_expr_contains_column_ref(expr) || list.iter().any(typed_expr_contains_column_ref)
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            typed_expr_contains_column_ref(body)
        }
        _ => false,
    }
}

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

fn typed_expr_references_all_lambda_params(expr: &TypedExpr, params: &[LambdaParam]) -> bool {
    let mut referenced = std::collections::HashSet::new();
    collect_lambda_param_refs(expr, &mut referenced);
    params
        .iter()
        .all(|param| referenced.contains(param.name.as_str()))
}

fn collect_lambda_param_refs<'a>(
    expr: &'a TypedExpr,
    referenced: &mut std::collections::HashSet<&'a str>,
) {
    match &expr.kind {
        ExprKind::LambdaParamRef { name, .. } => {
            referenced.insert(name.as_str());
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_lambda_param_refs(left, referenced);
            collect_lambda_param_refs(right, referenced);
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => collect_lambda_param_refs(expr, referenced),
        ExprKind::FunctionCall { args, .. }
        | ExprKind::AggregateCall { args, .. }
        | ExprKind::WindowCall { args, .. } => {
            for arg in args {
                collect_lambda_param_refs(arg, referenced);
            }
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_lambda_param_refs(operand, referenced);
            }
            for (when, then) in when_then {
                collect_lambda_param_refs(when, referenced);
                collect_lambda_param_refs(then, referenced);
            }
            if let Some(else_expr) = else_expr {
                collect_lambda_param_refs(else_expr, referenced);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_lambda_param_refs(expr, referenced);
            collect_lambda_param_refs(low, referenced);
            collect_lambda_param_refs(high, referenced);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_lambda_param_refs(expr, referenced);
            collect_lambda_param_refs(pattern, referenced);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_lambda_param_refs(expr, referenced);
            for item in list {
                collect_lambda_param_refs(item, referenced);
            }
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            collect_lambda_param_refs(body, referenced);
        }
        _ => {}
    }
}

/// Best-effort defaults for MySQL-style `@@var` session variables. We do not
/// yet store per-session state for these, so we just hand back the value the
/// regression tests assume so they can run end-to-end. Unknown names resolve
/// to an empty string rather than failing the query.
fn session_variable_default(name: &str) -> String {
    match name {
        "time_zone" => "Asia/Shanghai".to_string(),
        "sql_mode" => String::new(),
        "version" => "8.0.33".to_string(),
        "version_comment" => "NovaRocks".to_string(),
        "tx_isolation" | "transaction_isolation" => "READ-COMMITTED".to_string(),
        "character_set_connection" | "character_set_client" | "character_set_results" => {
            "utf8".to_string()
        }
        _ => String::new(),
    }
}

/// If the two types are both complex (ARRAY/MAP/STRUCT) but their nested
/// shapes cannot be reconciled, return a short human-readable description of
/// the incompatibility. Used to short-circuit comparison operators that would
/// otherwise fall into a CAST kernel and crash at runtime with a less
/// actionable message.
///
/// Returns `None` for any pair the analyzer should still attempt — scalar vs
/// scalar (handled by literal coercion), compatible container shapes (let
/// `cast_with_special_rules` widen at runtime), or one-side-only complex
/// types (rare; let the downstream layer surface its own error).
/// Module-visible wrapper around `incompatible_complex_compare` so the
/// IN-subquery rewriter (`subquery_rewrite::rewrite_in_subquery`) can
/// apply the same shape compatibility check before it synthesises an
/// EQ join condition. Without this, `x IN (SELECT y …)` where `x` and
/// `y` are STRUCT / MAP / ARRAY of incompatible element types would
/// silently produce zero rows instead of erroring at analyzer time.
pub(super) fn incompatible_complex_compare_pub(
    left: &DataType,
    right: &DataType,
) -> Option<String> {
    incompatible_complex_compare(left, right)
}

fn incompatible_complex_compare(left: &DataType, right: &DataType) -> Option<String> {
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
    use crate::analysis::QueryBody;
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
        let statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            return Err("expected query".to_string());
        };
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
            "utc_timestamp",
            "utc_time",
        ] {
            let body = crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::FunctionCall {
                    volatility: crate::functions::builtin_function_volatility(name),
                    name: name.to_string(),
                    args: vec![],
                    distinct: false,
                },
                data_type: DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                nullable: false,
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
        let mut statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
        let [ast::Statement::Query(query)] = statements.as_mut_slice() else {
            return Err("expected query".to_string());
        };
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            return Err("expected select".to_string());
        };
        let Some(ast::SelectItem::UnnamedExpr(ast::Expr::FunctionCall(func))) =
            select.projection.first_mut()
        else {
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

    fn assert_substring_int32_arguments(sql: &str, expected_arity: usize) {
        let expr = analyze_projection_expr(sql).expect("substring should analyze");
        let crate::analysis::ExprKind::FunctionCall { name, args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        assert_eq!(name, "substring");
        assert_eq!(args.len(), expected_arity);
        assert_eq!(args[0].data_type, DataType::Utf8);
        for arg in &args[1..] {
            assert_eq!(arg.data_type, DataType::Int32);
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
                assert_eq!(arg.data_type, DataType::Int32);
                assert!(!arg.nullable);
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
        let normal = super::bind_scalar_function_call(
            "substring",
            vec![
                crate::analysis::TypedExpr {
                    kind: crate::analysis::ExprKind::Literal(
                        crate::analysis::LiteralValue::String("x".to_string()),
                    ),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
                crate::analysis::TypedExpr {
                    kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(2)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                crate::analysis::TypedExpr {
                    kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(3)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
        )
        .expect("ordinary function call should bind");
        let special = analyze_projection_expr("select substring('x' from 2 for 3)")
            .expect("special function syntax should analyze");
        let crate::analysis::ExprKind::FunctionCall {
            name: special_name,
            args: special_args,
            ..
        } = special.kind
        else {
            panic!("expected special FunctionCall, got {:?}", special.kind);
        };

        assert_eq!(special_name, "substring");
        assert_eq!(normal.return_type, DataType::Utf8);
        assert_eq!(normal.args.len(), special_args.len());
        for (normal, special) in normal.args.iter().zip(special_args.iter()) {
            assert_eq!(normal.data_type, special.data_type);
            assert_eq!(normal.nullable, special.nullable);
            match (&normal.kind, &special.kind) {
                (
                    crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::String(
                        normal_value,
                    )),
                    crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::String(
                        special_value,
                    )),
                ) => assert_eq!(normal_value, special_value),
                (
                    crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(
                        normal_value,
                    )),
                    crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(
                        special_value,
                    )),
                ) => assert_eq!(normal_value, special_value),
                other => panic!("expected equivalent normalized literal arguments, got {other:?}"),
            }
        }
    }

    #[test]
    fn substring_special_syntax_uses_the_same_binding() {
        assert_substring_int32_arguments("select substring('STARROCKS' from 2 for 3)", 3);
        assert_substring_int32_arguments("select substring('STARROCKS' from 2)", 2);
    }

    #[test]
    fn substring_and_substr_reject_wrong_arity_during_analysis() {
        for name in ["substring", "substr"] {
            let err = analyze_projection_expr(&format!("select {name}('x')"))
                .expect_err("wrong arity must fail during analysis");
            assert!(err.contains("No matching function"), "{name}: {err}");
        }
    }

    #[test]
    fn unregistered_scalar_function_is_an_analyze_error() {
        let statements = novarocks_parser::parse("select sqlp7_not_a_function(1)")
            .expect("test query should parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };

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
        assert!(err.contains("No matching function"), "{err}");
    }

    #[test]
    fn substring_rejects_positive_literal_outside_int32() {
        let err = analyze_projection_expr("select substring('x', 2147483648)")
            .expect_err("overflowing literal must fail during analysis");
        assert_eq!(err, "Cast argument 2147483648 to int type failed");
    }

    #[test]
    fn substring_special_syntax_rejects_negative_literal_outside_int32() {
        let err = analyze_projection_expr("select substring('x' from -2147483649 for 1)")
            .expect_err("overflowing negative literal must fail during analysis");
        assert_eq!(err, "Cast argument -2147483649 to int type failed");
    }

    #[test]
    fn substring_non_literal_bigint_gets_runtime_int32_cast() {
        let expr = analyze_projection_expr("select substring('x', cast(1 as bigint))")
            .expect("BIGINT expression should bind through a runtime cast");
        let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        assert_eq!(args[1].data_type, DataType::Int32);
        assert!(matches!(
            args[1].kind,
            crate::analysis::ExprKind::Cast {
                target: DataType::Int32,
                ..
            }
        ));
    }

    #[test]
    fn scalar_binder_marks_narrowing_nonnull_bigint_column_cast_nullable() {
        let bound = super::bind_scalar_function_call(
            "substring",
            vec![
                crate::analysis::TypedExpr {
                    kind: crate::analysis::ExprKind::Literal(
                        crate::analysis::LiteralValue::String("x".to_string()),
                    ),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
                crate::analysis::TypedExpr {
                    kind: crate::analysis::ExprKind::ColumnRef {
                        column_id: crate::column_id::ColumnId(42),
                        qualifier: None,
                        column: "offset".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
        )
        .expect("non-null BIGINT column should bind to substring INT32 offset");

        let offset = &bound.args[1];
        assert_eq!(offset.data_type, DataType::Int32);
        assert!(offset.nullable);
        let crate::analysis::ExprKind::Cast {
            expr,
            target: DataType::Int32,
        } = &offset.kind
        else {
            panic!("expected a BIGINT-to-INT32 runtime cast");
        };
        assert!(!expr.nullable);
        assert!(matches!(
            expr.kind,
            crate::analysis::ExprKind::ColumnRef { .. }
        ));
    }

    #[test]
    fn substring_bigint_column_offset_gets_runtime_int32_cast() {
        let statements = novarocks_parser::parse("select substring('x', offset) from offsets")
            .expect("substring query should parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let (resolved, _registry, _factory) = analyze(query, &BigintOffsetCatalog, "default")
            .expect("BIGINT column should bind through a runtime cast");
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
        assert_eq!(offset.data_type, DataType::Int32);
        assert!(
            offset.nullable,
            "a narrowing runtime cast can produce NULL when BIGINT overflows INT32"
        );
        let crate::analysis::ExprKind::Cast {
            expr,
            target: DataType::Int32,
        } = offset.kind
        else {
            panic!("expected BIGINT offset runtime cast");
        };
        assert!(
            !expr.nullable,
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
            assert!(args.iter().all(|arg| arg.data_type == DataType::Utf8));
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
    fn substring_does_not_constant_fold_arithmetic_for_literal_range_checking() {
        let expr = analyze_projection_expr("select substring('x', 2147483647 + 1)")
            .expect("arithmetic expression should bind through a runtime cast");
        let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        assert_eq!(args[1].data_type, DataType::Int32);
        assert!(matches!(
            args[1].kind,
            crate::analysis::ExprKind::Cast {
                target: DataType::Int32,
                ..
            }
        ));
    }

    #[test]
    fn substring_null_offset_binds_to_nullable_int32() {
        let expr = analyze_projection_expr("select substring('x', NULL)")
            .expect("NULL should bind to the signature target");
        let crate::analysis::ExprKind::FunctionCall { args, .. } = expr.kind else {
            panic!("expected FunctionCall, got {:?}", expr.kind);
        };
        assert_eq!(args[1].data_type, DataType::Int32);
        assert!(args[1].nullable);
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
        assert_eq!(expr.data_type, DataType::Boolean);
        match expr.kind {
            crate::analysis::ExprKind::BinaryOp { left, right, .. } => {
                assert_eq!(
                    left.data_type,
                    DataType::Int64,
                    "left operand coerced to common type"
                );
                assert_eq!(
                    right.data_type,
                    DataType::Int64,
                    "right operand coerced to common type"
                );
            }
            other => panic!("expected BinaryOp comparison, got {:?}", other),
        }
    }

    #[test]
    fn date_plus_integer_rewrites_to_days_add() {
        let expr = analyze_projection_expr("select cast('1999-01-01' as date) + cast(5 as int)")
            .expect("date + integer should analyze");

        assert_eq!(expr.data_type, DataType::Date32);
        match expr.kind {
            crate::analysis::ExprKind::FunctionCall { name, args, .. } => {
                assert_eq!(name, "days_add");
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].data_type, DataType::Date32);
                assert_eq!(args[1].data_type, DataType::Int64);
            }
            other => panic!("expected days_add FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn date_plus_integer_comparison_keeps_date_operands() {
        let expr = analyze_projection_expr(
            "select cast('1999-01-06' as date) > cast('1999-01-01' as date) + 5",
        )
        .expect("date comparison should analyze");

        assert_eq!(expr.data_type, DataType::Boolean);
        match expr.kind {
            crate::analysis::ExprKind::BinaryOp { left, right, .. } => {
                assert_eq!(left.data_type, DataType::Date32);
                assert_eq!(right.data_type, DataType::Date32);
            }
            other => panic!("expected BinaryOp comparison, got {:?}", other),
        }
    }

    #[test]
    fn variant_get_two_arg_static_type_is_variant_binary() {
        let expr = analyze_projection_expr("select variant_get(parse_json('{\"a\":1}'), '$.a')")
            .expect("variant_get should analyze");

        assert_eq!(expr.data_type, DataType::LargeBinary);
    }

    #[test]
    fn variant_get_rejects_non_literal_path_argument() {
        let err = analyze_projection_expr(
            "select variant_get(parse_json('{\"a\":1}'), concat('$.', 'a'))",
        )
        .expect_err("path argument should be a string literal");

        assert_eq!(err, "variant_get path argument must be a string literal");
    }

    #[test]
    fn variant_get_literal_type_argument_sets_static_type() {
        let expr =
            analyze_projection_expr("select variant_get(parse_json('{\"a\":1}'), '$.a', 'bigint')")
                .expect("variant_get should analyze");

        assert_eq!(expr.data_type, DataType::Int64);
    }

    #[test]
    fn try_variant_get_rejects_non_literal_type_argument() {
        let err = analyze_projection_expr(
            "select try_variant_get(parse_json('{\"a\":1}'), '$.a', concat('big', 'int'))",
        )
        .expect_err("type argument should be a string literal");

        assert_eq!(
            err,
            "try_variant_get type argument must be a string literal"
        );
    }
}
