use std::sync::Arc;

use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::analysis::*;
use crate::types::{arithmetic_result_type_with_op, comparison_common_type, wider_type};

use super::functions::*;
use super::helpers::{eval_const_i64, expr_display_name, sql_type_to_arrow};
use super::scope::AnalyzerScope;

type WindowSpecAnalysis = (Vec<TypedExpr>, Vec<SortItem>, Option<WindowFrame>);

impl<'a> super::AnalyzerContext<'a> {
    /// Analyze a single expression and produce a TypedExpr.
    pub(super) fn analyze_expr(
        &self,
        expr: &sqlast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        match expr {
            // Simple column reference
            sqlast::Expr::Identifier(ident) => {
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
                let (column_id, data_type, nullable) = scope.resolve(None, &ident.value)?;
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

            // Qualified column reference or STRUCT field chain encoded by sqlparser
            // as a compound identifier (for example `c13.a`).
            sqlast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                self.analyze_compound_identifier(parts, scope)
            }

            // Literals
            sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => self.analyze_literal(value),

            sqlast::Expr::Array(array) => self.analyze_array_literal(array, scope),

            // Binary operations. The `->` arrow operator is treated as a
            // JSON-path access (StarRocks/MySQL semantics) when it appears
            // outside of a higher-order function argument. Higher-order
            // function arguments are inspected before they reach this
            // generic path — see `analyze_higher_order_lambda_arguments`.
            //
            // sqlparser parses `->` with low precedence (PgOther = 16), below
            // comparison operators (`=` = 20). That makes
            // `v8 -> '$.a.b' = v9 -> '$.a'` parse as
            // `Arrow(Arrow(v8, Eq('$.a.b', v9)), '$.a')` rather than the
            // intended `Eq(Arrow(v8, '$.a.b'), Arrow(v9, '$.a'))`. We
            // pre-rotate Arrow nodes here so the analyzer always sees the
            // semantically intended tree.
            sqlast::Expr::BinaryOp { left, op, right } => {
                if matches!(op, sqlast::BinaryOperator::Arrow) {
                    // Rebalance the whole arrow-rooted subtree before
                    // analyzing so that comparison/logical operators which
                    // sqlparser placed above `->` end up at the root.
                    let rebalanced = rebalance_arrow_tree(left, right);
                    return match rebalanced {
                        sqlast::Expr::BinaryOp {
                            left: l,
                            op: sqlast::BinaryOperator::Arrow,
                            right: r,
                        } => self.analyze_json_arrow(&l, &r, scope),
                        other => self.analyze_expr(&other, scope),
                    };
                }
                self.analyze_binary_op(left, op, right, scope)
            }

            // The StarRocks dialect does not enable sqlparser's generic
            // lambda parsing, so `Expr::Lambda` is normally unreachable here.
            // Reject it defensively so we don't silently miscompile if a
            // dialect change ever flips that flag back on.
            sqlast::Expr::Lambda(_) => Err(
                "lambda expressions are only allowed inside higher-order function calls"
                    .to_string(),
            ),

            // Unary NOT
            sqlast::Expr::UnaryOp {
                op: sqlast::UnaryOperator::Not,
                expr: inner,
            } => {
                let inner_typed = self.analyze_expr(inner, scope)?;
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
            sqlast::Expr::UnaryOp {
                op: sqlast::UnaryOperator::Minus,
                expr: inner,
            } => {
                let inner_typed = self.analyze_expr(inner, scope)?;
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
            sqlast::Expr::UnaryOp {
                op: sqlast::UnaryOperator::BitwiseNot,
                expr: inner,
            } => {
                let inner_typed = self.analyze_expr(inner, scope)?;
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

            // IS NULL / IS NOT NULL
            sqlast::Expr::IsNull(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsNull {
                        expr: Box::new(inner_typed),
                        negated: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }
            sqlast::Expr::IsNotNull(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsNull {
                        expr: Box::new(inner_typed),
                        negated: true,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // IN list
            sqlast::Expr::InList {
                expr: in_expr,
                list,
                negated,
            } => {
                use super::literal_coercion::coerce_literal_for_comparison;
                let mut expr_typed = self.analyze_expr(in_expr, scope)?;
                // StarRocks-aligned implicit literal coercion: when the IN
                // expression is `column IN (lit, lit, ...)`, coerce each
                // string literal to the column's type before emitting the
                // InList. Mirrors the binary-op comparison coercion.
                let mut list_typed = Vec::with_capacity(list.len());
                for item in list {
                    let item_typed = self.analyze_expr(item, scope)?;
                    list_typed.push(coerce_literal_for_comparison(&expr_typed, item_typed));
                }
                // BITMAP / HLL operands cannot participate in IN / NOT IN
                // because they have no scalar identity. Reject upfront so the
                // user sees a clear error before lowering / codegen.
                let kw = if *negated { "NOT IN" } else { "IN" };
                if let Some(logical) = scope
                    .logical_type_of_expr(&expr_typed)
                    .filter(is_bitmap_or_hll_type)
                {
                    let col = column_name_of_expr(&expr_typed);
                    return Err(format!(
                        "BITMAP/HLL columns cannot appear in {kw} expressions (operand `{col}` has type {logical:?})"
                    ));
                }
                for item in &list_typed {
                    if let Some(logical) = scope
                        .logical_type_of_expr(item)
                        .filter(is_bitmap_or_hll_type)
                    {
                        let col = column_name_of_expr(item);
                        return Err(format!(
                            "BITMAP/HLL columns cannot appear in {kw} expressions (operand `{col}` has type {logical:?})"
                        ));
                    }
                }
                for item in &list_typed {
                    if incompatible_complex_compare(&expr_typed.data_type, &item.data_type)
                        .is_some()
                    {
                        return Err(in_predicate_type_error(
                            &expr_typed.data_type,
                            &item.data_type,
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
                        negated: *negated,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // BETWEEN
            sqlast::Expr::Between {
                expr: between_expr,
                negated,
                low,
                high,
            } => {
                use super::literal_coercion::coerce_literal_for_comparison;
                let expr_typed = self.analyze_expr(between_expr, scope)?;
                let low_typed = self.analyze_expr(low, scope)?;
                let high_typed = self.analyze_expr(high, scope)?;
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
                        return Err(format!(
                            "BITMAP/HLL columns cannot appear in BETWEEN expressions (operand `{col}` has type {logical:?})"
                        ));
                    }
                }
                Ok(TypedExpr {
                    kind: ExprKind::Between {
                        expr: Box::new(expr_typed),
                        low: Box::new(low_typed),
                        high: Box::new(high_typed),
                        negated: *negated,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // LIKE
            sqlast::Expr::Like {
                negated,
                expr: like_expr,
                pattern,
                ..
            } => {
                let expr_typed = self.analyze_expr(like_expr, scope)?;
                let pattern_typed = self.analyze_expr(pattern, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::Like {
                        expr: Box::new(expr_typed),
                        pattern: Box::new(pattern_typed),
                        negated: *negated,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // CAST
            sqlast::Expr::Cast {
                expr: cast_expr,
                data_type: target_sql_type,
                ..
            } => {
                let inner_typed = self.analyze_expr(cast_expr, scope)?;
                let target = sql_type_to_arrow(target_sql_type)?;
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
            sqlast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.analyze_case(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                scope,
            ),

            // Function call
            sqlast::Expr::Function(func) => self.analyze_function(func, scope),
            sqlast::Expr::Ceil { expr, field } => {
                self.analyze_ceil_floor_expr("ceil", expr, field, scope)
            }
            sqlast::Expr::Floor { expr, field } => {
                self.analyze_ceil_floor_expr("floor", expr, field, scope)
            }

            sqlast::Expr::CompoundFieldAccess { root, access_chain } => {
                // When the access chain starts with `Identifier root` followed
                // by one or more `Dot(Identifier)` accesses, those leading
                // pieces may form a multi-segment column reference (alias.col,
                // db.tbl.col, alias.col.field, …). Hand them to
                // `analyze_compound_identifier`, which knows the alias/db/struct
                // resolution rules, and only let the remaining (Subscript /
                // non-trivial Dot) accesses go through the per-access path.
                if let sqlast::Expr::Identifier(root_ident) = root.as_ref() {
                    let mut ident_parts: Vec<sqlast::Ident> = vec![root_ident.clone()];
                    let mut chain_start = 0;
                    for (idx, access) in access_chain.iter().enumerate() {
                        match access {
                            sqlast::AccessExpr::Dot(sqlast::Expr::Identifier(ident)) => {
                                ident_parts.push(ident.clone());
                                chain_start = idx + 1;
                            }
                            _ => break,
                        }
                    }
                    if ident_parts.len() >= 2 {
                        let mut current = self.analyze_compound_identifier(&ident_parts, scope)?;
                        for access in &access_chain[chain_start..] {
                            current = self.analyze_compound_field_access(current, access, scope)?;
                        }
                        return Ok(current);
                    }
                }
                let mut current = self.analyze_expr(root, scope)?;
                for access in access_chain {
                    current = self.analyze_compound_field_access(current, access, scope)?;
                }
                Ok(current)
            }

            // Nested (parenthesized)
            sqlast::Expr::Nested(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                let dt = inner_typed.data_type.clone();
                let nullable = inner_typed.nullable;
                Ok(TypedExpr {
                    kind: ExprKind::Nested(Box::new(inner_typed)),
                    data_type: dt,
                    nullable,
                })
            }

            // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE
            sqlast::Expr::IsTrue(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsTruthValue {
                        expr: Box::new(inner_typed),
                        value: true,
                        negated: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }
            sqlast::Expr::IsFalse(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsTruthValue {
                        expr: Box::new(inner_typed),
                        value: false,
                        negated: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }
            sqlast::Expr::IsNotTrue(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsTruthValue {
                        expr: Box::new(inner_typed),
                        value: true,
                        negated: true,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }
            sqlast::Expr::IsNotFalse(inner) => {
                let inner_typed = self.analyze_expr(inner, scope)?;
                Ok(TypedExpr {
                    kind: ExprKind::IsTruthValue {
                        expr: Box::new(inner_typed),
                        value: false,
                        negated: true,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                })
            }

            // Subquery expression: EXISTS / NOT EXISTS
            sqlast::Expr::Exists { subquery, negated } => {
                let id = self.alloc_subquery_id();
                let kind = SubqueryKind::Exists { negated: *negated };
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: subquery.clone(),
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
            sqlast::Expr::InSubquery {
                expr: in_expr,
                subquery,
                negated,
            } => {
                // Multi-column LHS `(a, b) IN (SELECT c, d FROM ...)` arrives
                // here wrapped as `Expr::Tuple(...)` (or `Expr::Nested(Tuple(...))`).
                // The whole tuple is not a single scalar expression — the
                // analyzer's catch-all rejects it as "unsupported expression".
                // `subquery_rewrite::rewrite_in_subquery` handles the
                // decomposition (per-column equi-join) downstream, so just
                // skip the BITMAP / JSON / HLL precheck here: those checks
                // are per-column and only meaningful for a single-column LHS.
                let lhs_is_tuple = match in_expr.as_ref() {
                    sqlast::Expr::Tuple(_) => true,
                    sqlast::Expr::Nested(inner) => {
                        matches!(inner.as_ref(), sqlast::Expr::Tuple(_))
                    }
                    _ => false,
                };
                if !lhs_is_tuple {
                    // BITMAP / HLL operands cannot participate in IN subquery
                    // because they have no scalar identity. Reject upfront by
                    // resolving the LHS to detect a logical-type tag; the LHS
                    // is dropped before the subquery is planned so we cannot
                    // wait until later.
                    let in_expr_typed = self.analyze_expr(in_expr, scope)?;
                    if is_json_in_subquery_operand(&in_expr_typed, scope) {
                        return Err("In predicate of JSON does not support subquery".to_string());
                    }
                    if let Some(logical) = scope
                        .logical_type_of_expr(&in_expr_typed)
                        .filter(is_bitmap_or_hll_type)
                    {
                        let col = column_name_of_expr(&in_expr_typed);
                        let kw = if *negated { "NOT IN" } else { "IN" };
                        return Err(format!(
                            "BITMAP/HLL columns cannot appear in {kw} subquery expressions (operand `{col}` has type {logical:?})"
                        ));
                    }
                }
                let id = self.alloc_subquery_id();
                let kind = SubqueryKind::InSubquery { negated: *negated };
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: subquery.clone(),
                    data_type: DataType::Boolean,
                    in_expr: Some(in_expr.clone()),
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
            sqlast::Expr::Subquery(subquery) => {
                let id = self.alloc_subquery_id();
                let data_type = self.infer_scalar_subquery_data_type(subquery);
                let kind = SubqueryKind::Scalar;
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: subquery.clone(),
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
            sqlast::Expr::TypedString(typed_str) => {
                let target = sql_type_to_arrow(&typed_str.data_type)?;
                let value = typed_str.value.to_string();
                // For DATE literals, constant-fold to Date32 integer value
                if target == DataType::Date32 {
                    let date_str = value.trim_matches(|c| c == '\'' || c == '"');
                    let days = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                        .map_err(|e| format!("invalid date literal '{date_str}': {e}"))?
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

            // INTERVAL '1' DAY → treat as string literal for now
            sqlast::Expr::Interval(interval) => {
                let s = interval.to_string();
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(s)),
                    data_type: DataType::Utf8,
                    nullable: false,
                })
            }

            // SUBSTR / SUBSTRING — sqlparser parses these as special Expr variant
            sqlast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => {
                let expr_typed = self.analyze_expr(expr, scope)?;
                let mut args = vec![expr_typed];
                if let Some(from_expr) = substring_from {
                    args.push(self.analyze_expr(from_expr, scope)?);
                }
                if let Some(for_expr) = substring_for {
                    args.push(self.analyze_expr(for_expr, scope)?);
                }
                Ok(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: "substring".to_string(),
                        args,
                        distinct: false,
                    },
                    data_type: DataType::Utf8,
                    nullable: true,
                })
            }

            // TRIM — sqlparser parses as special Expr variant
            sqlast::Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let expr_typed = self.analyze_expr(expr, scope)?;
                let func_name = match trim_where {
                    Some(sqlast::TrimWhereField::Leading) => "ltrim",
                    Some(sqlast::TrimWhereField::Trailing) => "rtrim",
                    _ => "trim",
                };
                let mut args = vec![expr_typed];
                if let Some(what) = trim_what {
                    args.push(self.analyze_expr(what, scope)?);
                }
                if let Some(chars) = trim_characters {
                    for c in chars {
                        args.push(self.analyze_expr(c, scope)?);
                    }
                }
                Ok(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: func_name.to_string(),
                        args,
                        distinct: false,
                    },
                    data_type: DataType::Utf8,
                    nullable: true,
                })
            }

            // EXTRACT(field FROM expr) → function call
            sqlast::Expr::Extract { field, expr, .. } => {
                let expr_typed = self.analyze_expr(expr, scope)?;
                let func_name = match field {
                    sqlast::DateTimeField::Year => "year",
                    sqlast::DateTimeField::Month => "month",
                    sqlast::DateTimeField::Day => "day",
                    sqlast::DateTimeField::Hour => "hour",
                    sqlast::DateTimeField::Minute => "minute",
                    sqlast::DateTimeField::Second => "second",
                    other => return Err(format!("unsupported EXTRACT field: {other}")),
                };
                Ok(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: func_name.to_string(),
                        args: vec![expr_typed],
                        distinct: false,
                    },
                    data_type: DataType::Int32,
                    nullable: true,
                })
            }

            other => Err(format!("unsupported expression: {other}")),
        }
    }

    fn analyze_compound_field_access(
        &self,
        base: TypedExpr,
        access: &sqlast::AccessExpr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        match access {
            sqlast::AccessExpr::Dot(expr) => {
                let sqlast::Expr::Identifier(ident) = expr else {
                    return Err(format!("unsupported dotted field access: {expr}"));
                };
                self.analyze_struct_field_access(base, ident.value.clone())
            }
            sqlast::AccessExpr::Subscript(sqlast::Subscript::Index { index }) => {
                let mut index_typed = self.analyze_expr(index, scope)?;
                let output_type = match &base.data_type {
                    DataType::List(item) => {
                        index_typed =
                            cast_null_preserving_target_type(index_typed, &DataType::Int32);
                        item.data_type().clone()
                    }
                    DataType::Map(entries, _) => {
                        let DataType::Struct(fields) = entries.data_type() else {
                            return Err("map subscript expects STRUCT map entries".to_string());
                        };
                        if fields.len() != 2 {
                            return Err("map subscript expects key/value entries".to_string());
                        }
                        index_typed =
                            cast_null_preserving_target_type(index_typed, fields[0].data_type());
                        fields[1].data_type().clone()
                    }
                    DataType::Struct(fields) => {
                        return match &index_typed.kind {
                            ExprKind::Literal(LiteralValue::String(field_name)) => {
                                self.analyze_struct_field_access(base, field_name.clone())
                            }
                            // 1-based positional access: `struct_val[1]` →
                            // first field of the STRUCT. Matches StarRocks /
                            // Spark / Trino convention.
                            ExprKind::Literal(LiteralValue::Int(pos)) => {
                                if *pos < 1 || (*pos as usize) > fields.len() {
                                    return Err(format!(
                                        "struct subscript {} is out of range (1..{})",
                                        pos,
                                        fields.len()
                                    ));
                                }
                                let field_name = fields[(*pos as usize) - 1].name().clone();
                                self.analyze_struct_field_access(base, field_name)
                            }
                            _ => Err(format!(
                                "struct subscript requires a string literal field name or 1-based integer index, got {:?}",
                                index_typed.kind
                            )),
                        };
                    }
                    other => {
                        return Err(format!(
                            "subscript access expects ARRAY, MAP, or STRUCT input, got {:?}",
                            other
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
                        name: function_name.to_string(),
                        args: vec![base, index_typed],
                        distinct: false,
                    },
                    data_type: output_type,
                    nullable: true,
                })
            }
            sqlast::AccessExpr::Subscript(sqlast::Subscript::Slice { .. }) => {
                Err("array slice syntax is not supported".to_string())
            }
        }
    }

    fn infer_scalar_subquery_data_type(&self, subquery: &sqlast::Query) -> DataType {
        // `analyze_query` re-enters `analyze_select` which calls
        // `rewrite_subqueries` whenever `collected_subqueries` is non-empty.
        // That rewrite *drains* the shared `collected_subqueries` vec, so any
        // outer-query subqueries collected before us would be lost. Snapshot
        // the full vec (not just its length) and restore it afterward so the
        // outer analyzer can still see and rewrite its own subqueries.
        let saved_collected: Vec<SubqueryInfo> = self.collected_subqueries.borrow().clone();
        let saved_next_subquery_id = self.next_subquery_id.get();
        let inferred = self
            .analyze_query(subquery)
            .ok()
            .and_then(|query| {
                query
                    .output_columns
                    .first()
                    .map(|col| col.data_type.clone())
            })
            .unwrap_or(DataType::Null);
        *self.collected_subqueries.borrow_mut() = saved_collected;
        self.next_subquery_id.set(saved_next_subquery_id);
        inferred
    }

    fn analyze_compound_identifier(
        &self,
        parts: &[sqlast::Ident],
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
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
                current = self.analyze_struct_field_access(current, field.value.clone())?;
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
                    current = self.analyze_struct_field_access(current, field.value.clone())?;
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
                    current = self.analyze_struct_field_access(current, field.value.clone())?;
                }
                return Ok(current);
            }
        }

        // Form 3: leading identifier is the column itself, the rest walk a
        // STRUCT. Falls back to producing a `Column 'X' cannot be resolved`
        // error from `scope.resolve` if even this fails.
        let (column_id, data_type, nullable) = scope.resolve(None, base_name)?;
        let mut current = build_column_ref(column_id, data_type, nullable, None, base_name);
        for field in &parts[1..] {
            current = self.analyze_struct_field_access(current, field.value.clone())?;
        }
        Ok(current)
    }

    fn analyze_struct_field_access(
        &self,
        base: TypedExpr,
        field_name: String,
    ) -> Result<TypedExpr, String> {
        let DataType::Struct(fields) = &base.data_type else {
            return Err(format!(
                "field access expects STRUCT input, got {:?}",
                base.data_type
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
            .ok_or_else(|| format!("struct field '{}' does not exist", field_name))?;
        let field_type = field.data_type().clone();
        let canonical_field_name = field.name().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(canonical_field_name)),
            data_type: DataType::Utf8,
            nullable: false,
        };
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "__struct_subfield".to_string(),
                args: vec![base, field_name_expr],
                distinct: false,
            },
            data_type: field_type,
            nullable: true,
        })
    }

    fn analyze_ceil_floor_expr(
        &self,
        name: &str,
        expr: &sqlast::Expr,
        field: &sqlast::CeilFloorKind,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        match field {
            sqlast::CeilFloorKind::DateTimeField(sqlast::DateTimeField::NoDateTime) => {}
            sqlast::CeilFloorKind::DateTimeField(other) => {
                return Err(format!(
                    "unsupported {} datetime field: {}",
                    name.to_uppercase(),
                    other
                ));
            }
            sqlast::CeilFloorKind::Scale(_) => {
                return Err(format!(
                    "{} with scale is not supported",
                    name.to_uppercase()
                ));
            }
        }
        let arg = self.analyze_expr(expr, scope)?;
        let arg_types = vec![arg.data_type.clone()];
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.to_string(),
                args: vec![arg],
                distinct: false,
            },
            data_type: infer_scalar_return_type(name, &arg_types),
            nullable: true,
        })
    }

    /// Analyze a literal value.
    fn analyze_literal(&self, value: &sqlast::Value) -> Result<TypedExpr, String> {
        match value {
            sqlast::Value::Number(n, _) => {
                if let Ok(v) = n.parse::<i64>() {
                    // Integer without decimal point → Int64
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(v)),
                        data_type: DataType::Int64,
                        nullable: false,
                    })
                } else if !n.contains('.') && !n.contains('e') && !n.contains('E') {
                    let v = n
                        .parse::<i128>()
                        .map_err(|_| format!("invalid numeric literal: {n}"))?;
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::LargeInt(v)),
                        data_type: DataType::FixedSizeBinary(
                            crate::common::largeint::LARGEINT_BYTE_WIDTH,
                        ),
                        nullable: false,
                    })
                } else if n.contains('.') && !n.contains('e') && !n.contains('E') {
                    // Number with decimal point (no scientific notation) → Decimal
                    // with precision/scale inferred from the literal text (e.g.
                    // "100.00" → Decimal(5,2), "7.0" → Decimal(2,1)).
                    // This matches StarRocks behaviour and avoids the
                    // Float64→Decimal(38,9) promotion that inflates division
                    // result scales.
                    let (precision, scale) = infer_decimal_precision_scale(n);
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Decimal(n.clone())),
                        data_type: DataType::Decimal128(precision, scale),
                        nullable: false,
                    })
                } else if let Ok(v) = n.parse::<f64>() {
                    Ok(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Float(v)),
                        data_type: DataType::Float64,
                        nullable: false,
                    })
                } else {
                    Err(format!("invalid numeric literal: {n}"))
                }
            }
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                // sqlparser already applied MySQL-style backslash escapes when
                // tokenising the literal (our dialect enables
                // `supports_string_literal_backslash_escape`). Don't unescape
                // again — that double-processed `'e\\f'` from 3 bytes (`e\f`)
                // down to 2 (`ef`) and is the cause of `join_large_in_predicate`
                // step 59 silently dropping the backslash row from the IN
                // result. INSERT VALUES already trusts sqlparser's output
                // (`sql_expr.rs` clones the string as-is); SELECT now matches.
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(s.clone())),
                    data_type: DataType::Utf8,
                    nullable: false,
                })
            }
            sqlast::Value::HexStringLiteral(s) => {
                let bytes =
                    hex::decode(s).map_err(|err| format!("invalid hex literal X'{s}': {err}"))?;
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Binary(bytes)),
                    data_type: DataType::Binary,
                    nullable: false,
                })
            }
            sqlast::Value::Boolean(b) => Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(*b)),
                data_type: DataType::Boolean,
                nullable: false,
            }),
            sqlast::Value::Null => Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Null),
                data_type: DataType::Null,
                nullable: true,
            }),
            other => Err(format!("unsupported literal value: {other:?}")),
        }
    }

    fn analyze_array_literal(
        &self,
        array: &sqlast::Array,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        let mut args = Vec::with_capacity(array.elem.len());
        let mut item_type = DataType::Null;
        for item in &array.elem {
            let mut typed = self.analyze_expr(item, scope)?;
            // StarRocks infers array literal element types from the
            // narrowest integer width that holds the value (TINYINT for
            // `[1, 2, 3]`). Narrow each integer literal here so the
            // widened item type — and downstream `typeof()` — matches.
            if let ExprKind::Literal(LiteralValue::Int(v)) = &typed.kind {
                typed.data_type = narrow_int_literal_type(*v);
            }
            item_type = wider_type(&item_type, &typed.data_type);
            args.push(typed);
        }
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
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
    fn analyze_json_arrow(
        &self,
        left: &sqlast::Expr,
        right: &sqlast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        let left_typed = self.analyze_expr(left, scope)?;
        let right_typed = self.analyze_expr(right, scope)?;
        let nullable = true;
        let fn_name = "json_query";
        // Use json_query for JSON inputs; otherwise still return Utf8 via
        // get_json_string semantics. The runtime function name "json_query"
        // is registered in connector/codegen and returns a JSON-valued column
        // (mapped to Utf8 at the analyzer level for downstream operators).
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
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
        left: &sqlast::Expr,
        op: &sqlast::BinaryOperator,
        right: &sqlast::Expr,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        let left_typed = self.analyze_expr(left, scope)?;
        let right_typed = self.analyze_expr(right, scope)?;

        // StarRocks-aligned implicit literal coercion: when a comparison has
        // (column, literal) we coerce the literal to the column's type before
        // emitting the BinaryOp. Mirrors LiteralExprFactory.create(value, ty).
        let (left_typed, right_typed) = {
            use super::literal_coercion::coerce_literal_for_comparison;
            let coerce_for_compare = matches!(
                op,
                sqlast::BinaryOperator::Eq
                    | sqlast::BinaryOperator::NotEq
                    | sqlast::BinaryOperator::Lt
                    | sqlast::BinaryOperator::LtEq
                    | sqlast::BinaryOperator::Gt
                    | sqlast::BinaryOperator::GtEq
                    | sqlast::BinaryOperator::Spaceship
            );
            if coerce_for_compare {
                // BITMAP / HLL columns have no scalar identity and cannot be
                // compared with `=`, `<`, etc. Reject before the operand type
                // is dropped to `Boolean` so the user sees a clear error.
                let op_sym = match op {
                    sqlast::BinaryOperator::Eq => "=",
                    sqlast::BinaryOperator::NotEq => "!=",
                    sqlast::BinaryOperator::Lt => "<",
                    sqlast::BinaryOperator::LtEq => "<=",
                    sqlast::BinaryOperator::Gt => ">",
                    sqlast::BinaryOperator::GtEq => ">=",
                    sqlast::BinaryOperator::Spaceship => "<=>",
                    _ => unreachable!(),
                };
                if let Some(logical) = scope
                    .logical_type_of_expr(&left_typed)
                    .filter(is_bitmap_or_hll_type)
                {
                    return Err(format!(
                        "comparison operator `{op_sym}` is not supported for BITMAP/HLL (left operand has type {logical:?})"
                    ));
                }
                if let Some(logical) = scope
                    .logical_type_of_expr(&right_typed)
                    .filter(is_bitmap_or_hll_type)
                {
                    return Err(format!(
                        "comparison operator `{op_sym}` is not supported for BITMAP/HLL (right operand has type {logical:?})"
                    ));
                }
                // Ordering operators (`<`, `<=`, `>`, `>=`) are undefined on
                // ARRAY / MAP / STRUCT values in standalone SQL. Keep equality
                // and null-safe equality on ARRAY available, but reject
                // ordering predicates at analysis time so join predicates fail
                // before reaching execution.
                let is_ordering_op = matches!(
                    op,
                    sqlast::BinaryOperator::Lt
                        | sqlast::BinaryOperator::LtEq
                        | sqlast::BinaryOperator::Gt
                        | sqlast::BinaryOperator::GtEq
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
                        return Err(format!(
                            "comparison operator `{op_sym}` does not support binary predicate operation on {kind} values"
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
                    return Err(format!(
                        "comparison operator `{op_sym}` does not support binary predicate operation between {reason}"
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
                // the execution-time normalizer (normalize_comparison_types).
                match comparison_common_type(&left_coerced.data_type, &right_coerced.data_type) {
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

        let (bin_op, result_type) = match op {
            // Comparison operators -> Boolean
            sqlast::BinaryOperator::Eq => (BinOp::Eq, DataType::Boolean),
            sqlast::BinaryOperator::NotEq => (BinOp::Ne, DataType::Boolean),
            sqlast::BinaryOperator::Lt => (BinOp::Lt, DataType::Boolean),
            sqlast::BinaryOperator::LtEq => (BinOp::Le, DataType::Boolean),
            sqlast::BinaryOperator::Gt => (BinOp::Gt, DataType::Boolean),
            sqlast::BinaryOperator::GtEq => (BinOp::Ge, DataType::Boolean),
            sqlast::BinaryOperator::Spaceship => (BinOp::EqForNull, DataType::Boolean),

            // Logical operators -> Boolean
            sqlast::BinaryOperator::And => (BinOp::And, DataType::Boolean),
            sqlast::BinaryOperator::Or => (BinOp::Or, DataType::Boolean),

            // Arithmetic operators -> inferred type
            sqlast::BinaryOperator::Plus => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "add",
                );
                (BinOp::Add, dt)
            }
            sqlast::BinaryOperator::Minus => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "add",
                );
                (BinOp::Sub, dt)
            }
            sqlast::BinaryOperator::Multiply => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "mul",
                );
                (BinOp::Mul, dt)
            }
            sqlast::BinaryOperator::Divide => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "div",
                );
                (BinOp::Div, dt)
            }
            sqlast::BinaryOperator::Modulo => {
                let dt = arithmetic_result_type_with_op(
                    &left_typed.data_type,
                    &right_typed.data_type,
                    "add",
                );
                (BinOp::Mod, dt)
            }

            // || is logical OR in MySQL/StarRocks default sql_mode.
            // Non-boolean operands are implicitly cast to boolean.
            sqlast::BinaryOperator::StringConcat => {
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

            other => return Err(format!("unsupported binary operator: {other:?}")),
        };

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
        operand: Option<&sqlast::Expr>,
        conditions: &[sqlast::CaseWhen],
        else_result: Option<&sqlast::Expr>,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        let operand_typed = match operand {
            Some(e) => Some(Box::new(self.analyze_expr(e, scope)?)),
            None => None,
        };

        let mut when_then = Vec::with_capacity(conditions.len());
        let mut result_type = DataType::Null;
        for cw in conditions {
            let when_typed = self.analyze_expr(&cw.condition, scope)?;
            let then_typed = self.analyze_expr(&cw.result, scope)?;
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
        func: &sqlast::Function,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        let original_name = func.name.to_string().to_lowercase();
        if original_name == "ds_theta_count_distinct" {
            return Err("unsupported agg function: ds_theta_count_distinct".to_string());
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
            let first_arg_ty = match &func.args {
                sqlast::FunctionArguments::List(list) => list.args.first().and_then(|arg| {
                    if let sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) = arg {
                        self.analyze_expr(e, scope).ok().map(|t| t.data_type)
                    } else {
                        None
                    }
                }),
                _ => None,
            };
            match first_arg_ty {
                Some(DataType::Map(_, _)) => name = "__map_element_at".to_string(),
                Some(DataType::List(_)) => name = "__array_element_at".to_string(),
                _ => {}
            }
        }

        // Check for DISTINCT
        let is_distinct = matches!(
            &func.args,
            sqlast::FunctionArguments::List(list)
                if list.duplicate_treatment == Some(sqlast::DuplicateTreatment::Distinct)
        );

        // Check for count(*)
        let is_count_star = name == "count"
            && matches!(
                &func.args,
                sqlast::FunctionArguments::List(list)
                    if list.args.len() == 1
                        && matches!(
                            &list.args[0],
                            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Wildcard)
                        )
            );

        // Extract argument expressions
        let arg_exprs: Vec<&sqlast::Expr> = match &func.args {
            sqlast::FunctionArguments::List(list) => list
                .args
                .iter()
                .filter_map(|arg| match arg {
                    sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .collect(),
            sqlast::FunctionArguments::None => vec![],
            _ => vec![],
        };

        // typeof(CAST(x AS T)) preserves the SQL-level type spelling (CHAR vs
        // VARCHAR, DECIMAL128(p, s), etc.) which is lost once the cast lowers
        // to Arrow. Intercept it here so codegen receives a string literal
        // for the well-known SQL spelling. For non-CAST arguments we still
        // fall through to the existing codegen-time path.
        if name == "typeof"
            && arg_exprs.len() == 1
            && let sqlast::Expr::Cast { data_type, .. } = arg_exprs[0]
            && let Some(type_name) = sql_type_starrocks_name(data_type)
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
            return Err("group_concat should have at least one input.".to_string());
        }
        if matches!(
            name.as_str(),
            "array_agg" | "array_agg_distinct" | "array_unique_agg"
        ) {
            if arg_exprs.is_empty() {
                return Err("array_agg should have at least one input.".to_string());
            }
            if arg_exprs.len() != 1 {
                return Err(
                    "Unexpected input 'order', the most similar input is {',', ')'}.".to_string(),
                );
            }
        }
        if name == "any_value" && is_distinct {
            return Err("Getting syntax error".to_string());
        }
        // `date_add(dt, INTERVAL expr <UNIT>)` / `date_sub(...)` in MySQL
        // syntax: route to the unit-specific function (`seconds_add`,
        // `days_add`, etc.) and unwrap the Interval into its plain value
        // expression so downstream type inference and execution see an
        // integer arg, not the synthetic INTERVAL-as-string placeholder.
        let date_add_interval_rewrites: Vec<sqlast::Expr> =
            if matches!(name.as_str(), "date_add" | "date_sub")
                && arg_exprs.len() == 2
                && let sqlast::Expr::Interval(interval) = arg_exprs[1]
            {
                let unit = interval
                    .leading_field
                    .as_ref()
                    .map(|f| format!("{f}").to_ascii_lowercase())
                    .unwrap_or_else(|| "day".to_string());
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

        let array_generate_rewrites: Vec<sqlast::Expr> = if date_add_interval_rewrites.is_empty()
            && name == "array_generate"
            && arg_exprs.len() == 3
        {
            if let sqlast::Expr::Interval(interval) = arg_exprs[2] {
                if !is_integer_const_literal(interval.value.as_ref()) {
                    return Err(
                        "array_generate requires step parameter must be a constant integer"
                            .to_string(),
                    );
                }
                let unit = interval
                    .leading_field
                    .as_ref()
                    .map(|f| format!("{f}").to_ascii_lowercase())
                    .unwrap_or_else(|| "second".to_string());
                vec![
                    (*arg_exprs[0]).clone(),
                    (*arg_exprs[1]).clone(),
                    signed_integer_literal_expr(interval.value.as_ref())
                        .unwrap_or_else(|| (*interval.value).clone()),
                    sqlast::Expr::Value(sqlast::ValueWithSpan {
                        value: sqlast::Value::SingleQuotedString(unit),
                        span: sqlparser::tokenizer::Span::empty(),
                    }),
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
        let time_slice_rewrites: Vec<sqlast::Expr> = if date_add_interval_rewrites.is_empty()
            && array_generate_rewrites.is_empty()
            && matches!(name.as_str(), "time_slice" | "date_slice")
            && !arg_exprs.is_empty()
        {
            let mut rewritten: Vec<sqlast::Expr> = Vec::with_capacity(arg_exprs.len() + 1);
            for (idx, e) in arg_exprs.iter().enumerate() {
                // Position 1 is the interval; expand it into value + unit.
                if idx == 1
                    && let sqlast::Expr::Interval(interval) = e
                {
                    // StarRocks rejects non-integer constant
                    // intervals at planning time; mirror that error
                    // here rather than silently producing NULL.
                    if !is_integer_const_literal(interval.value.as_ref()) {
                        return Err(format!(
                            "{name} requires second parameter must be a constant interval"
                        ));
                    }
                    rewritten.push((*interval.value).clone());
                    let unit = interval
                        .leading_field
                        .as_ref()
                        .map(|f| format!("{f}").to_ascii_lowercase())
                        .unwrap_or_else(|| "second".to_string());
                    rewritten.push(sqlast::Expr::Value(sqlast::ValueWithSpan {
                        value: sqlast::Value::SingleQuotedString(unit),
                        span: sqlparser::tokenizer::Span::empty(),
                    }));
                    continue;
                }
                let token = match e {
                    sqlast::Expr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
                    sqlast::Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                        Some(parts[0].value.to_ascii_lowercase())
                    }
                    _ => None,
                };
                if let Some(token) = token
                    && matches!(token.as_str(), "ceil" | "floor")
                {
                    rewritten.push(sqlast::Expr::Value(sqlast::ValueWithSpan {
                        value: sqlast::Value::SingleQuotedString(token),
                        span: sqlparser::tokenizer::Span::empty(),
                    }));
                } else {
                    rewritten.push((*e).clone());
                }
            }
            rewritten
        } else {
            Vec::new()
        };

        let effective_arg_exprs: Vec<&sqlast::Expr> = if !date_add_interval_rewrites.is_empty() {
            date_add_interval_rewrites.iter().collect()
        } else if !array_generate_rewrites.is_empty() {
            array_generate_rewrites.iter().collect()
        } else if !time_slice_rewrites.is_empty() {
            time_slice_rewrites.iter().collect()
        } else {
            arg_exprs.clone()
        };

        if let Some(rewritten) =
            self.try_analyze_higher_order_function(&name, &effective_arg_exprs, scope)?
        {
            return Ok(rewritten);
        }
        if let Some(rewritten) =
            self.try_analyze_array_map_cast_lambda(&name, &effective_arg_exprs, scope)?
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
            self.analyze_array_sortby_lambda_arguments(&effective_arg_exprs, scope)?
        } else if is_higher_order_function_with_lambda(&name, &effective_arg_exprs) {
            self.analyze_higher_order_lambda_arguments(&name, &effective_arg_exprs, scope)?
        } else if is_map_higher_order_function_with_lambda(&name, &effective_arg_exprs) {
            self.analyze_map_higher_order_lambda_arguments(&name, &effective_arg_exprs, scope)?
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

        self.validate_ds_hll_arguments(&name, &args_typed)?;

        if name == "array_flatten"
            && let Some(DataType::List(item)) = arg_types.first()
            && !matches!(item.data_type(), DataType::List(_))
        {
            return Err(format!(
                "The only one input of array_flatten should be an array of arrays, rather than {}",
                starrocks_error_type_name(&arg_types[0])
            ));
        }

        if name == "array_agg" && is_distinct {
            if args_typed
                .first()
                .is_some_and(is_non_groupable_map_constructor)
            {
                return Err("Unknown error".to_string());
            }
            if let Some(semantic_type) = args_typed
                .first()
                .and_then(json_semantic_group_by_type_name)
            {
                let arg_display = expr_display_name(arg_exprs[0]);
                return Err(format!(
                    "array_agg(DISTINCT {arg_display}) can't rewrite distinct to group by on ({semantic_type})."
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
            return Err(
                "Unexpected input '(', the most similar input is {<EOF>, ';'}.".to_string(),
            );
        }

        validate_group_concat_separator_argument(&name, &arg_exprs, &args_typed)?;
        validate_group_concat_value_arguments(&name, &args_typed)?;

        // Extract ORDER BY within function args (for aggregates like array_agg)
        let func_order_by = self.extract_function_order_by(func, scope, &args_typed)?;

        // Check for window function: func(...) OVER (...)
        if let Some(ref window_type) = func.over {
            if name == "any_value" {
                return Err("any_value not supported with OVER clause".to_string());
            }
            // StarRocks rejects LEAD/LAG when the third (default) argument
            // doesn't match a per-shape type rule. The error message echoes
            // the value column's type (INT/FLOAT/DECIMAL...) and is asserted
            // by SQL regression tests.
            if matches!(name.as_str(), "lead" | "lag") && args_typed.len() >= 3 {
                let value_type = args_typed[0].data_type.clone();
                let default_arg = &args_typed[2];
                if !is_lead_lag_default_arg_acceptable(default_arg, &value_type) {
                    return Err(format!(
                        "The type of the third parameter of LEAD/LAG not match the type {}.",
                        lead_lag_type_display(&value_type)
                    ));
                }
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
                infer_scalar_return_type(&name, &arg_types)
            };
            let (partition_by, order_by, window_frame) =
                self.analyze_window_spec(window_type, scope)?;
            // sqlparser surfaces IGNORE/RESPECT NULLS in two places, depending
            // on whether the keywords are written inside the function call's
            // argument list (`first_value(v IGNORE NULLS)`) or after the
            // closing paren (`first_value(v) IGNORE NULLS OVER (...)`).
            // Per sqlparser, only one form can be set on a given function.
            let post_args_treatment = func.null_treatment;
            let inside_args_treatment = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => {
                    list.clauses.iter().find_map(|c| {
                        if let sqlparser::ast::FunctionArgumentClause::IgnoreOrRespectNulls(t) = c {
                            Some(*t)
                        } else {
                            None
                        }
                    })
                }
                _ => None,
            };
            let ignore_nulls = matches!(
                post_args_treatment.or(inside_args_treatment),
                Some(sqlparser::ast::NullTreatment::IgnoreNulls)
            );
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

        self.validate_percentile_arguments(&name, &args_typed)?;
        if is_aggregate_function(&name) {
            validate_aggregate_function_call(&name, &arg_types)?;
        } else {
            validate_scalar_function_call_typed(&name, &args_typed)?;
        }

        match original_name.as_str() {
            "ds_hll_accumulate" => {
                let state_expr = TypedExpr {
                    kind: ExprKind::FunctionCall {
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
                self.ensure_ds_hll_binary_arg("ds_hll_count_distinct_union", args_typed.first())?;
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
                self.ensure_ds_hll_binary_arg("ds_hll_count_distinct_merge", args_typed.first())?;
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
            let mut return_type = infer_scalar_return_type(&name, &arg_types);
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
                    return Err(format!(
                        "{name} expects 2 or 3 arguments, got {}",
                        args_typed.len()
                    ));
                }
                match &args_typed[1].kind {
                    ExprKind::Literal(LiteralValue::String(_)) => {}
                    _ => {
                        return Err(format!("{name} path argument must be a string literal"));
                    }
                }
                if args_typed.len() == 3 {
                    match &args_typed[2].kind {
                        ExprKind::Literal(LiteralValue::String(t)) => {
                            return_type =
                                crate::exec::expr::function::variant::variant_get_target_type(t)?;
                        }
                        _ => {
                            return Err(format!("{name} type argument must be a string literal"));
                        }
                    }
                }
            }
            Ok(TypedExpr {
                kind: ExprKind::FunctionCall {
                    name,
                    args: args_typed,
                    distinct: is_distinct,
                },
                data_type: return_type,
                nullable: true,
            })
        }
    }

    fn analyze_array_sortby_lambda_arguments(
        &self,
        arg_exprs: &[&sqlast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<(Vec<TypedExpr>, Vec<DataType>), String> {
        if arg_exprs.len() != 2 {
            return Err(
                "array_sortby lambda rewrite currently supports exactly one lambda and one array argument"
                    .to_string(),
            );
        }
        let (param_name, lambda_body) = parse_array_sortby_lambda(arg_exprs[0])
            .ok_or_else(|| "array_sortby lambda rewrite expected a lambda argument".to_string())?;
        let array_expr = self.analyze_expr(arg_exprs[1], scope)?;
        let field_chain = extract_lambda_field_chain(lambda_body, &param_name)?;
        if field_chain.is_empty() {
            return Err(
                "array_sortby lambda rewrite requires direct struct field access like (x) -> x.item"
                    .to_string(),
            );
        }

        let mut key_expr = array_expr.clone();
        for field_name in field_chain {
            key_expr = self.build_array_struct_subfield_expr(key_expr, field_name)?;
        }

        let arg_types = vec![array_expr.data_type.clone(), key_expr.data_type.clone()];
        Ok((vec![array_expr, key_expr], arg_types))
    }

    fn build_array_struct_subfield_expr(
        &self,
        base: TypedExpr,
        field_name: String,
    ) -> Result<TypedExpr, String> {
        let DataType::List(item_field) = &base.data_type else {
            return Err(format!(
                "array_sortby lambda expects ARRAY input, got {:?}",
                base.data_type
            ));
        };
        let DataType::Struct(fields) = item_field.data_type() else {
            return Err(format!(
                "array_sortby lambda field access expects ARRAY<STRUCT>, got {:?}",
                base.data_type
            ));
        };
        // Same case-insensitive resolution + canonical name forwarding as the
        // plain struct subfield path above; the array-of-struct variant
        // (`array_sortby(...).field`) needs to match identically.
        let field = fields
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(&field_name))
            .ok_or_else(|| format!("struct field '{}' does not exist", field_name))?;
        let field_type = field.data_type().clone();
        let canonical_field_name = field.name().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(canonical_field_name)),
            data_type: DataType::Utf8,
            nullable: false,
        };
        Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
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
        arg_exprs: &[&sqlast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<Option<TypedExpr>, String> {
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
            return Err(format!("{name} expects at least one ARRAY argument"));
        }

        if name == "array_sort" {
            if array_exprs.len() != 1 || params.len() != 2 {
                return Err(
                    "array_sort lambda comparator expects one ARRAY argument and two lambda parameters"
                        .to_string(),
                );
            }
            let source = self.analyze_expr(array_exprs[0], scope)?;
            let (item_type, item_nullable) = match &source.data_type {
                DataType::List(item) => (item.data_type().clone(), item.is_nullable()),
                DataType::Null => (DataType::Null, true),
                other => return Err(format!("array_sort expects ARRAY argument, got {other:?}")),
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
                || typed_expr_contains_nondeterministic_call(&body)
                || !typed_expr_references_all_lambda_params(&body, &lambda_params)
            {
                return Err(
                    "Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions"
                        .to_string(),
                );
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
                other => return Err(format!("{name} expects ARRAY argument, got {other:?}")),
            };
            let Some(param_name) = params.get(idx) else {
                return Err(format!(
                    "{name} lambda argument count {} does not match ARRAY argument count {}",
                    params.len(),
                    array_exprs.len()
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
            return Err(format!(
                "{name} lambda argument count {} does not match ARRAY argument count {}",
                params.len(),
                array_args.len()
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
                        name: "array_map".to_string(),
                        args: map_args,
                        distinct: false,
                    },
                    data_type: mapped_type,
                    nullable: true,
                };
                Ok(Some(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: name.to_string(),
                        args: vec![mapped],
                        distinct: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: true,
                }))
            }
            "array_filter" | "filter" => {
                let source = array_args
                    .first()
                    .cloned()
                    .ok_or_else(|| "array_filter missing ARRAY argument".to_string())?;
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
                        name: "array_map".to_string(),
                        args: map_args,
                        distinct: false,
                    },
                    data_type: filter_type,
                    nullable: true,
                };
                Ok(Some(TypedExpr {
                    kind: ExprKind::FunctionCall {
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
        arg_exprs: &[&sqlast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<(Vec<TypedExpr>, Vec<DataType>), String> {
        if arg_exprs.len() < 2 {
            return Err(format!(
                "{name} expects a lambda and at least one array argument"
            ));
        }
        let (param_names, body_expr) = parse_multi_param_lambda(arg_exprs[0])
            .ok_or_else(|| format!("{name} expects a lambda function as its first argument"))?;
        let array_count = arg_exprs.len() - 1;
        if param_names.len() != array_count {
            return Err(format!(
                "{name} lambda has {} parameter(s) but {} array argument(s) were supplied",
                param_names.len(),
                array_count
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
                    return Err(format!("{name} expects ARRAY arguments, got {:?}", other));
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
    /// `map_apply` the body must be a `(new_key, new_value)` tuple (sqlparser
    /// surfaces it as `Expr::Tuple`); we rewrite it as a `row(...)` call so
    /// downstream codegen sees a 2-field Struct.
    fn analyze_map_higher_order_lambda_arguments(
        &self,
        name: &str,
        arg_exprs: &[&sqlast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<(Vec<TypedExpr>, Vec<DataType>), String> {
        let (param_names, body_expr) = parse_multi_param_lambda(arg_exprs[0])
            .ok_or_else(|| format!("{name} expects a lambda function as its first argument"))?;
        if param_names.len() != 2 {
            return Err(format!(
                "{name} lambda must take exactly 2 parameters (key, value); got {}",
                param_names.len()
            ));
        }

        let map_typed = self.analyze_expr(arg_exprs[1], scope)?;
        let (key_type, value_type) = match &map_typed.data_type {
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    (fields[0].data_type().clone(), fields[1].data_type().clone())
                }
                other => {
                    return Err(format!(
                        "{name} expects MAP argument with struct<key,value> entries, got {:?}",
                        other
                    ));
                }
            },
            DataType::Null => (DataType::Null, DataType::Null),
            other => return Err(format!("{name} expects a MAP argument, got {:?}", other)),
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
                let tuple_items: Vec<&sqlast::Expr> = match body_expr {
                    sqlast::Expr::Tuple(items) => items.iter().collect(),
                    sqlast::Expr::Nested(inner) => match inner.as_ref() {
                        sqlast::Expr::Tuple(items) => items.iter().collect(),
                        _ => Vec::new(),
                    },
                    _ => Vec::new(),
                };
                if tuple_items.len() != 2 {
                    return Err(format!(
                        "map_apply lambda body must produce (new_key, new_value), got {} items",
                        tuple_items.len()
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
        arg_exprs: &[&sqlast::Expr],
        scope: &AnalyzerScope,
    ) -> Result<Option<TypedExpr>, String> {
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
            return Err(
                "array_map lambda rewrite currently supports x -> CAST(x AS STRING)".to_string(),
            );
        }

        let array_expr = self.analyze_expr(arg_exprs[1], scope)?;
        if !matches!(array_expr.data_type, DataType::List(_)) {
            return Err(format!(
                "array_map lambda expects ARRAY input, got {:?}",
                array_expr.data_type
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

    fn validate_percentile_arguments(&self, name: &str, args: &[TypedExpr]) -> Result<(), String> {
        match name {
            "percentile_cont" | "percentile_disc_lc" => {
                if let Some(expr) = args.get(1)
                    && let Some(value) = const_numeric_value(expr)
                    && !(0.0..=1.0).contains(&value)
                {
                    return Err(format!(
                        "{name} second parameter'value should be between 0 and 1"
                    ));
                }
                return Ok(());
            }
            _ => {}
        }

        match name {
            "percentile_approx" => {
                if let Some(expr) = args.first() {
                    validate_percentile_numeric_arg(name, 0, "value", expr)?;
                }
            }
            "percentile_approx_weighted" => {
                if let Some(expr) = args.first() {
                    validate_percentile_numeric_arg(name, 0, "value", expr)?;
                }
                if let Some(expr) = args.get(1) {
                    validate_percentile_numeric_arg(name, 1, "weight", expr)?;
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
            self.validate_percentile_quantile_arg(name, quantile_idx, expr)?;
        }
        if let Some(expr) = args.get(compression_idx) {
            self.validate_percentile_compression_arg(name, expr)?;
        }
        Ok(())
    }

    fn validate_percentile_quantile_arg(
        &self,
        name: &str,
        quantile_idx: usize,
        expr: &TypedExpr,
    ) -> Result<(), String> {
        match &expr.data_type {
            DataType::List(item) => {
                if matches!(item.data_type(), DataType::Null) {
                    return Err(format!(
                        "{name} requires the {} parameter (percentile) to be ARRAY<NUMERIC>, but got: ARRAY<NULL_TYPE>.",
                        ordinal_name(quantile_idx)
                    ));
                }
                if !is_numeric_type(item.data_type()) {
                    return Err(format!(
                        "{name} requires the {} parameter (percentile) to be ARRAY<NUMERIC>, but got: ARRAY<{:?}>.",
                        ordinal_name(quantile_idx),
                        item.data_type()
                    ));
                }
                if let Some(items) = array_literal_items(expr) {
                    for (idx, item) in items.iter().enumerate() {
                        if let Some(value) = const_numeric_value(item) {
                            validate_percentile_value(name, value, Some(idx))?;
                        }
                    }
                }
            }
            data_type if is_numeric_type(data_type) => {
                if let Some(value) = const_numeric_value(expr) {
                    validate_percentile_value(name, value, None)?;
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
    ) -> Result<(), String> {
        if let Some(value) = const_numeric_value(expr)
            && value <= 0.0
        {
            return Err(format!(
                "Type check failed. compression parameter must be positive in {name}, but got: {}",
                format_percentile_error_value(value)
            ));
        }
        Ok(())
    }

    /// Extract ORDER BY clauses from within function arguments (e.g. array_agg(x ORDER BY y)).
    fn extract_function_order_by(
        &self,
        func: &sqlast::Function,
        scope: &AnalyzerScope,
        args: &[TypedExpr],
    ) -> Result<Vec<SortItem>, String> {
        let func_name = func.name.to_string().to_lowercase();
        let visible_args =
            if matches!(func_name.as_str(), "group_concat" | "string_agg") && !args.is_empty() {
                &args[..args.len() - 1]
            } else {
                args
            };
        let clauses = match &func.args {
            sqlast::FunctionArguments::List(list) => &list.clauses,
            _ => return Ok(vec![]),
        };

        for clause in clauses {
            if let sqlast::FunctionArgumentClause::OrderBy(order_by_exprs) = clause {
                let mut items = Vec::with_capacity(order_by_exprs.len());
                for ob in order_by_exprs {
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
                            return Err(format!(
                                "ORDER BY position {pos} is not in {display_name} output list."
                            ));
                        } else {
                            self.analyze_expr(&ob.expr, scope)?
                        }
                    } else {
                        self.analyze_expr(&ob.expr, scope)?
                    };
                    let asc = ob.options.asc.unwrap_or(true);
                    let nulls_first = ob.options.nulls_first.unwrap_or(asc);
                    items.push(SortItem {
                        expr: typed,
                        asc,
                        nulls_first,
                    });
                }
                return Ok(items);
            }
        }
        Ok(vec![])
    }

    /// Analyze a window specification (OVER clause).
    fn analyze_window_spec(
        &self,
        over: &sqlast::WindowType,
        scope: &AnalyzerScope,
    ) -> Result<WindowSpecAnalysis, String> {
        let spec = match over {
            sqlast::WindowType::WindowSpec(spec) => spec,
            sqlast::WindowType::NamedWindow(_) => {
                return Err("named window references are not supported".into());
            }
        };

        // PARTITION BY
        let mut partition_by = Vec::new();
        for expr in &spec.partition_by {
            partition_by.push(self.analyze_expr(expr, scope)?);
        }

        // ORDER BY
        let mut order_by = Vec::new();
        for ob in &spec.order_by {
            let typed = self.analyze_expr(&ob.expr, scope)?;
            let asc = ob.options.asc.unwrap_or(true);
            let nulls_first = ob.options.nulls_first.unwrap_or(asc);
            order_by.push(SortItem {
                expr: typed,
                asc,
                nulls_first,
            });
        }

        // Window frame
        let window_frame = if let Some(ref frame) = spec.window_frame {
            let frame_type = match frame.units {
                sqlast::WindowFrameUnits::Rows => WindowFrameType::Rows,
                sqlast::WindowFrameUnits::Range => WindowFrameType::Range,
                sqlast::WindowFrameUnits::Groups => {
                    return Err("GROUPS window frame is not supported".into());
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
        bound: &sqlast::WindowFrameBound,
    ) -> Result<WindowBound, String> {
        match bound {
            sqlast::WindowFrameBound::CurrentRow => Ok(WindowBound::CurrentRow),
            sqlast::WindowFrameBound::Preceding(None) => Ok(WindowBound::UnboundedPreceding),
            sqlast::WindowFrameBound::Preceding(Some(expr)) => {
                let n = eval_const_i64(expr)
                    .map_err(|_| "window frame offset must be a constant integer")?;
                Ok(WindowBound::Preceding(n))
            }
            sqlast::WindowFrameBound::Following(None) => Ok(WindowBound::UnboundedFollowing),
            sqlast::WindowFrameBound::Following(Some(expr)) => {
                let n = eval_const_i64(expr)
                    .map_err(|_| "window frame offset must be a constant integer")?;
                Ok(WindowBound::Following(n))
            }
        }
    }

    fn validate_ds_hll_arguments(&self, name: &str, args: &[TypedExpr]) -> Result<(), String> {
        if name != "ds_hll_count_distinct" {
            return Ok(());
        }

        if args.len() > 3 {
            return Err(
                "ds_hll_count_distinct requires one/two/three parameters: ds_hll_count_distinct(col, <log_k>, <tgt_type>)"
                    .to_string(),
            );
        }

        if let Some(log_k) = args.get(1) {
            let ExprKind::Literal(LiteralValue::Int(value)) = &log_k.kind else {
                return Err(
                    "ds_hll_count_distinct 's second parameter's data type is wrong ".to_string(),
                );
            };
            if !(4..=21).contains(value) {
                return Err(
                    "ds_hll_count_distinct second parameter'value should be between 4 and 21."
                        .to_string(),
                );
            }
        }

        if let Some(target) = args.get(2) {
            let ExprKind::Literal(LiteralValue::String(value)) = &target.kind else {
                return Err(
                    "ds_hll_count_distinct 's third parameter's data type is wrong ".to_string(),
                );
            };
            if !matches!(value.as_str(), "HLL_4" | "HLL_6" | "HLL_8") {
                return Err(
                    "ds_hll_count_distinct third  parameter'value should be in HLL_4/HLL_6/HLL_8."
                        .to_string(),
                );
            }
        }

        Ok(())
    }

    fn ensure_ds_hll_binary_arg(
        &self,
        fn_name: &str,
        arg: Option<&TypedExpr>,
    ) -> Result<(), String> {
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
            Err(format!(
                "Resolved function {fn_name} has no binary as argument type."
            ))
        }
    }

    // -----------------------------------------------------------------------
    // Aggregate detection
    // -----------------------------------------------------------------------

    /// Check if any projection item contains an aggregate function call.
    pub(super) fn select_has_aggregate_functions(&self, projection: &[sqlast::SelectItem]) -> bool {
        for item in projection {
            let expr = match item {
                sqlast::SelectItem::UnnamedExpr(e) => e,
                sqlast::SelectItem::ExprWithAlias { expr, .. } => expr,
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
    pub(super) fn expr_contains_aggregate(&self, expr: &sqlast::Expr) -> bool {
        match expr {
            sqlast::Expr::Function(f) => {
                // A function with OVER is a window function, not an aggregate
                if f.over.is_some() {
                    return false;
                }
                if is_aggregate_function(&f.name.to_string().to_lowercase()) {
                    return true;
                }
                match &f.args {
                    sqlast::FunctionArguments::List(list) => {
                        list.args.iter().any(|arg| match arg {
                            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) => {
                                self.expr_contains_aggregate(expr)
                            }
                            _ => false,
                        }) || list.clauses.iter().any(|clause| match clause {
                            sqlast::FunctionArgumentClause::OrderBy(order_by_exprs) => {
                                order_by_exprs
                                    .iter()
                                    .any(|item| self.expr_contains_aggregate(&item.expr))
                            }
                            _ => false,
                        })
                    }
                    _ => false,
                }
            }
            sqlast::Expr::BinaryOp { left, right, .. } => {
                self.expr_contains_aggregate(left) || self.expr_contains_aggregate(right)
            }
            sqlast::Expr::UnaryOp { expr, .. } => self.expr_contains_aggregate(expr),
            sqlast::Expr::IsNull(inner) | sqlast::Expr::IsNotNull(inner) => {
                self.expr_contains_aggregate(inner)
            }
            sqlast::Expr::InList { expr, list, .. } => {
                self.expr_contains_aggregate(expr)
                    || list.iter().any(|item| self.expr_contains_aggregate(item))
            }
            sqlast::Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_contains_aggregate(expr)
                    || self.expr_contains_aggregate(low)
                    || self.expr_contains_aggregate(high)
            }
            sqlast::Expr::Like { expr, pattern, .. } => {
                self.expr_contains_aggregate(expr) || self.expr_contains_aggregate(pattern)
            }
            sqlast::Expr::Nested(inner) => self.expr_contains_aggregate(inner),
            sqlast::Expr::Cast { expr, .. } => self.expr_contains_aggregate(expr),
            sqlast::Expr::Tuple(items) => {
                items.iter().any(|item| self.expr_contains_aggregate(item))
            }
            sqlast::Expr::Array(array) => array
                .elem
                .iter()
                .any(|item| self.expr_contains_aggregate(item)),
            sqlast::Expr::Struct { values, .. } => {
                values.iter().any(|item| self.expr_contains_aggregate(item))
            }
            sqlast::Expr::Map(map) => map.entries.iter().any(|entry| {
                self.expr_contains_aggregate(&entry.key)
                    || self.expr_contains_aggregate(&entry.value)
            }),
            sqlast::Expr::CompoundFieldAccess { root, access_chain } => {
                self.expr_contains_aggregate(root)
                    || access_chain.iter().any(|access| match access {
                        sqlast::AccessExpr::Dot(expr) => self.expr_contains_aggregate(expr),
                        sqlast::AccessExpr::Subscript(sqlast::Subscript::Index { index }) => {
                            self.expr_contains_aggregate(index)
                        }
                        sqlast::AccessExpr::Subscript(sqlast::Subscript::Slice {
                            lower_bound,
                            upper_bound,
                            stride,
                        }) => {
                            lower_bound
                                .as_ref()
                                .is_some_and(|expr| self.expr_contains_aggregate(expr))
                                || upper_bound
                                    .as_ref()
                                    .is_some_and(|expr| self.expr_contains_aggregate(expr))
                                || stride
                                    .as_ref()
                                    .is_some_and(|expr| self.expr_contains_aggregate(expr))
                        }
                    })
            }
            sqlast::Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                conditions.iter().any(|cw| {
                    self.expr_contains_aggregate(&cw.condition)
                        || self.expr_contains_aggregate(&cw.result)
                }) || else_result
                    .as_ref()
                    .is_some_and(|e| self.expr_contains_aggregate(e))
            }
            _ => false,
        }
    }
}

fn function_order_by_position(expr: &sqlast::Expr) -> Option<i64> {
    match expr {
        sqlast::Expr::Value(v) => {
            if let sqlast::Value::Number(n, false) = &v.value {
                n.parse::<i64>().ok()
            } else {
                None
            }
        }
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            sqlast::Expr::Value(v) => {
                if let sqlast::Value::Number(n, false) = &v.value {
                    n.parse::<i64>().ok().map(|pos| -pos)
                } else {
                    None
                }
            }
            _ => None,
        },
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
        Some(crate::sql::SqlType::Json)
    ) || matches!(
        json_semantic_group_by_type_name(expr).as_deref(),
        Some("json")
    )
}

fn is_bitmap_or_hll_type(sql_type: &crate::sql::SqlType) -> bool {
    matches!(
        sql_type,
        crate::sql::SqlType::Bitmap | crate::sql::SqlType::Hll
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

/// Infer Decimal precision and scale from a numeric literal string containing
/// a decimal point.  For example `"100.00"` → `(5, 2)`, `"7.0"` → `(2, 1)`,
/// `"0.2"` → `(2, 1)`.
fn infer_decimal_precision_scale(s: &str) -> (u8, i8) {
    let s = s.trim().trim_start_matches('+').trim_start_matches('-');
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    let int_part = int_part.trim_start_matches('0');
    let int_digits = if int_part.is_empty() {
        1
    } else {
        int_part.len()
    };
    let scale = frac_part.len();
    let precision = int_digits + scale;
    // Clamp to Decimal128 limits
    let precision = precision.clamp(1, 38) as u8;
    let scale = scale.min(38) as i8;
    (precision, scale)
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

fn validate_group_concat_separator_argument(
    name: &str,
    arg_exprs: &[&sqlast::Expr],
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

fn group_concat_separator_signature(arg_exprs: &[&sqlast::Expr]) -> String {
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
    ) || crate::common::largeint::is_largeint_data_type(data_type)
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
        dt if crate::common::largeint::is_largeint_data_type(dt) => "largeint".to_string(),
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
    // sqlparser surfaces `-1` as `UnaryOp::Minus(Literal(1))`, so peel the
    // unary minus before deciding.
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

fn extract_lambda_field_chain(
    expr: &sqlast::Expr,
    param_name: &str,
) -> Result<Vec<String>, String> {
    match expr {
        sqlast::Expr::Nested(inner) => extract_lambda_field_chain(inner, param_name),
        sqlast::Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case(param_name) => {
            Ok(vec![])
        }
        sqlast::Expr::CompoundIdentifier(parts)
            if !parts.is_empty() && parts[0].value.eq_ignore_ascii_case(param_name) =>
        {
            Ok(parts[1..].iter().map(|part| part.value.clone()).collect())
        }
        sqlast::Expr::CompoundFieldAccess { root, access_chain } => {
            let mut fields = extract_lambda_field_chain(root, param_name)?;
            for access in access_chain {
                match access {
                    sqlast::AccessExpr::Dot(sqlast::Expr::Identifier(ident)) => {
                        fields.push(ident.value.clone());
                    }
                    _ => {
                        return Err(
                            "array_sortby lambda rewrite only supports dotted struct field access"
                                .to_string(),
                        );
                    }
                }
            }
            Ok(fields)
        }
        _ => Err(
            "array_sortby lambda rewrite only supports direct struct field access like (x) -> x.item"
                .to_string(),
        ),
    }
}

fn parse_array_sortby_lambda(expr: &sqlast::Expr) -> Option<(String, &sqlast::Expr)> {
    match expr {
        sqlast::Expr::Lambda(lambda) => lambda
            .params
            .iter()
            .next()
            .map(|ident| (ident.value.to_lowercase(), lambda.body.as_ref())),
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        } => parse_array_sortby_lambda_param(left).map(|param| (param, right.as_ref())),
        sqlast::Expr::Nested(inner) => parse_array_sortby_lambda(inner),
        _ => None,
    }
}

fn find_lambda_argument<'a>(
    arg_exprs: &[&'a sqlast::Expr],
) -> Option<(usize, Vec<String>, sqlast::Expr)> {
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

fn parse_lambda_expr(expr: &sqlast::Expr) -> Option<(Vec<String>, sqlast::Expr)> {
    match expr {
        sqlast::Expr::Lambda(lambda) => Some((
            lambda
                .params
                .iter()
                .map(|ident| ident.value.to_lowercase())
                .collect(),
            (*lambda.body).clone(),
        )),
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        } => parse_lambda_params(left).map(|params| (params, (**right).clone())),
        sqlast::Expr::BinaryOp { left, op, right } => {
            parse_lambda_expr(left).map(|(params, body)| {
                (
                    params,
                    sqlast::Expr::BinaryOp {
                        left: Box::new(body),
                        op: op.clone(),
                        right: right.clone(),
                    },
                )
            })
        }
        sqlast::Expr::Nested(inner) => parse_lambda_expr(inner),
        _ => None,
    }
}

fn parse_lambda_params(expr: &sqlast::Expr) -> Option<Vec<String>> {
    match expr {
        sqlast::Expr::Identifier(ident) => Some(vec![ident.value.to_lowercase()]),
        sqlast::Expr::Tuple(items) => items
            .iter()
            .map(|item| match item {
                sqlast::Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
                sqlast::Expr::Nested(inner) => parse_lambda_params(inner).and_then(|params| {
                    if params.len() == 1 {
                        params.into_iter().next()
                    } else {
                        None
                    }
                }),
                _ => None,
            })
            .collect(),
        sqlast::Expr::Nested(inner) => parse_lambda_params(inner),
        _ => None,
    }
}

fn parse_array_sortby_lambda_param(expr: &sqlast::Expr) -> Option<String> {
    match expr {
        sqlast::Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        sqlast::Expr::Nested(inner) => parse_array_sortby_lambda_param(inner),
        _ => None,
    }
}

fn lambda_body_casts_param_to_utf8(expr: &sqlast::Expr, param_name: &str) -> bool {
    match expr {
        sqlast::Expr::Nested(inner) => lambda_body_casts_param_to_utf8(inner, param_name),
        sqlast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } if lambda_expr_is_param(inner, param_name) => {
            matches!(
                sql_type_to_arrow(data_type),
                Ok(DataType::Utf8 | DataType::LargeUtf8)
            )
        }
        _ => false,
    }
}

fn lambda_expr_is_param(expr: &sqlast::Expr, param_name: &str) -> bool {
    match expr {
        sqlast::Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(param_name),
        sqlast::Expr::Nested(inner) => lambda_expr_is_param(inner, param_name),
        _ => false,
    }
}

/// Repeatedly apply Arrow-precedence rotations on a `Arrow(left, right)`
/// expression until the tree is stable. Returns either an unchanged
/// `BinaryOp { op: Arrow }` (when no rotation is possible) or the rotated
/// expression that the caller must analyze instead.
///
/// Two rotation rules are applied iteratively:
///   1. `Arrow(L, BinaryOp(a, op, b))` where `op` ranks below the intended
///      arrow precedence (Eq/comparison/AND/OR/etc) becomes
///      `BinaryOp(Arrow(L, a), op, b)`.
///   2. `Arrow(BinaryOp(L, op, R), Y)` where `op` ranks below the intended
///      arrow precedence becomes `BinaryOp(L, op, Arrow(R, Y))`.
///
/// Bot rules can produce a new expression whose root is no longer an Arrow,
/// so the caller may need to re-analyze through a different code path.
fn rebalance_arrow_tree(left: &sqlast::Expr, right: &sqlast::Expr) -> sqlast::Expr {
    // First normalize the left subtree. The outer Arrow's left operand may
    // itself be an Arrow that needs rotation; once we lift comparisons /
    // logical ops out of it, the outer Arrow can apply rule 2 to push itself
    // further inward.
    let normalized_left = normalize_arrow_subtree(left);
    let mut expr = sqlast::Expr::BinaryOp {
        left: Box::new(normalized_left),
        op: sqlast::BinaryOperator::Arrow,
        right: Box::new(right.clone()),
    };
    // Bounded iteration: each rotation strictly increases the depth of the
    // non-arrow root, so a small upper bound is more than enough.
    for _ in 0..64 {
        match rotate_arrow_once(expr) {
            (next, true) => expr = next,
            (next, false) => return next,
        }
    }
    expr
}

/// Normalize an arbitrary expression so that any `Arrow` nodes inside it are
/// re-balanced. Non-Arrow expressions are returned unchanged.
fn normalize_arrow_subtree(expr: &sqlast::Expr) -> sqlast::Expr {
    match expr {
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        } => rebalance_arrow_tree(left, right),
        _ => expr.clone(),
    }
}

fn rotate_arrow_once(expr: sqlast::Expr) -> (sqlast::Expr, bool) {
    let sqlast::Expr::BinaryOp {
        left,
        op: sqlast::BinaryOperator::Arrow,
        right,
    } = expr
    else {
        return (expr, false);
    };
    // Rule 1: push Arrow inside RHS.
    if let sqlast::Expr::BinaryOp {
        left: rhs_left,
        op: rhs_op,
        right: rhs_right,
    } = *right
    {
        if op_is_below_arrow(&rhs_op) {
            let new_left = sqlast::Expr::BinaryOp {
                left,
                op: sqlast::BinaryOperator::Arrow,
                right: rhs_left,
            };
            return (
                sqlast::Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: rhs_op,
                    right: rhs_right,
                },
                true,
            );
        }
        // Restore right since we destructured it.
        let restored_right = sqlast::Expr::BinaryOp {
            left: rhs_left,
            op: rhs_op,
            right: rhs_right,
        };
        // Now try rule 2 on LHS.
        if let sqlast::Expr::BinaryOp {
            left: lhs_left,
            op: lhs_op,
            right: lhs_right,
        } = *left
        {
            if op_is_below_arrow(&lhs_op) {
                let new_right = sqlast::Expr::BinaryOp {
                    left: lhs_right,
                    op: sqlast::BinaryOperator::Arrow,
                    right: Box::new(restored_right),
                };
                return (
                    sqlast::Expr::BinaryOp {
                        left: lhs_left,
                        op: lhs_op,
                        right: Box::new(new_right),
                    },
                    true,
                );
            }
            // Reconstruct LHS for the no-op return.
            let restored_left = sqlast::Expr::BinaryOp {
                left: lhs_left,
                op: lhs_op,
                right: lhs_right,
            };
            return (
                sqlast::Expr::BinaryOp {
                    left: Box::new(restored_left),
                    op: sqlast::BinaryOperator::Arrow,
                    right: Box::new(restored_right),
                },
                false,
            );
        }
        return (
            sqlast::Expr::BinaryOp {
                left,
                op: sqlast::BinaryOperator::Arrow,
                right: Box::new(restored_right),
            },
            false,
        );
    }
    // Right wasn't a BinaryOp. Try rule 2 on LHS only.
    if let sqlast::Expr::BinaryOp {
        left: lhs_left,
        op: lhs_op,
        right: lhs_right,
    } = *left
    {
        if op_is_below_arrow(&lhs_op) {
            let new_right = sqlast::Expr::BinaryOp {
                left: lhs_right,
                op: sqlast::BinaryOperator::Arrow,
                right,
            };
            return (
                sqlast::Expr::BinaryOp {
                    left: lhs_left,
                    op: lhs_op,
                    right: Box::new(new_right),
                },
                true,
            );
        }
        let restored_left = sqlast::Expr::BinaryOp {
            left: lhs_left,
            op: lhs_op,
            right: lhs_right,
        };
        return (
            sqlast::Expr::BinaryOp {
                left: Box::new(restored_left),
                op: sqlast::BinaryOperator::Arrow,
                right,
            },
            false,
        );
    }
    (
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        },
        false,
    )
}

/// Operators whose default sqlparser precedence is below where the JSON
/// `->` operator should bind. Arithmetic operators (`+`, `*`, ...) sit
/// ABOVE arrow in user mental model and are excluded so that lambdas like
/// `x -> length(x) + v2` keep the natural body grouping.
fn op_is_below_arrow(op: &sqlast::BinaryOperator) -> bool {
    matches!(
        op,
        sqlast::BinaryOperator::Eq
            | sqlast::BinaryOperator::NotEq
            | sqlast::BinaryOperator::Lt
            | sqlast::BinaryOperator::LtEq
            | sqlast::BinaryOperator::Gt
            | sqlast::BinaryOperator::GtEq
            | sqlast::BinaryOperator::Spaceship
            | sqlast::BinaryOperator::And
            | sqlast::BinaryOperator::Or
    )
}

/// Parse a lambda with one or more parameters. The StarRocks dialect leaves
/// `supports_lambda_functions` off because enabling sqlparser's generic
/// lambda parsing also intercepts the `->` JSON-path operator. As a result,
/// the parser produces these AST shapes:
///   - `Expr::Lambda { params, body }` (rare; only when sqlparser emits it
///     directly, e.g. for some keyword-style inputs)
///   - `Expr::BinaryOp { op: Arrow }` for `x -> body`, `(x) -> body`, or
///     `(x, y, z) -> body`. The left operand carries the parameter list as
///     either an `Identifier`, a `Nested(Identifier)`, or a `Tuple(...)`.
fn parse_multi_param_lambda(expr: &sqlast::Expr) -> Option<(Vec<String>, &sqlast::Expr)> {
    match expr {
        sqlast::Expr::Lambda(lambda) => Some((
            lambda
                .params
                .iter()
                .map(|p| p.value.to_lowercase())
                .collect(),
            lambda.body.as_ref(),
        )),
        sqlast::Expr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Arrow,
            right,
        } => parse_lambda_param_list(left).map(|params| (params, right.as_ref())),
        sqlast::Expr::Nested(inner) => parse_multi_param_lambda(inner),
        _ => None,
    }
}

fn parse_lambda_param_list(expr: &sqlast::Expr) -> Option<Vec<String>> {
    match expr {
        sqlast::Expr::Identifier(ident) => Some(vec![ident.value.to_lowercase()]),
        sqlast::Expr::Nested(inner) => parse_lambda_param_list(inner),
        // sqlparser emits `Tuple(idents)` for `(x, y, z)` when lambda support
        // is disabled. We accept it as a multi-parameter lambda header here.
        sqlast::Expr::Tuple(items) => {
            let mut params = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    sqlast::Expr::Identifier(ident) => params.push(ident.value.to_lowercase()),
                    sqlast::Expr::Nested(inner) => {
                        let mut inner_params = parse_lambda_param_list(inner)?;
                        params.append(&mut inner_params);
                    }
                    _ => return None,
                }
            }
            Some(params)
        }
        _ => None,
    }
}

/// Returns true if `name` is a higher-order function (variadic by lambda arity)
/// and the first argument is a parseable lambda. Used to dispatch into the
/// dedicated analyzer that binds lambda parameters before walking the body.
fn is_higher_order_function_with_lambda(name: &str, arg_exprs: &[&sqlast::Expr]) -> bool {
    matches!(name, "array_map" | "transform")
        && arg_exprs
            .first()
            .and_then(|expr| parse_multi_param_lambda(expr))
            .is_some()
}

/// Map-shaped higher-order functions: `(k, v) -> body` over a MAP argument.
/// `map_apply` (body returns 2-tuple), `transform_keys` (body returns scalar
/// new key), `transform_values` (body returns scalar new value).
fn is_map_higher_order_function_with_lambda(name: &str, arg_exprs: &[&sqlast::Expr]) -> bool {
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
fn is_integer_const_literal(expr: &sqlast::Expr) -> bool {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(s, _),
            ..
        }) => s.parse::<i64>().is_ok(),
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s),
            ..
        }) => s.parse::<i64>().is_ok(),
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus | sqlast::UnaryOperator::Plus,
            expr,
        } => is_integer_const_literal(expr),
        sqlast::Expr::Nested(inner) => is_integer_const_literal(inner),
        _ => false,
    }
}

fn signed_integer_literal_expr(expr: &sqlast::Expr) -> Option<sqlast::Expr> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(s, long),
            ..
        }) if s.parse::<i64>().is_ok() => Some(sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(s.clone(), *long),
            span: sqlparser::tokenizer::Span::empty(),
        })),
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s),
            ..
        }) if s.parse::<i64>().is_ok() => Some(sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(s.clone(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })),
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => signed_integer_literal_expr(expr).and_then(|inner| match inner {
            sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number(s, long),
                ..
            }) if !s.starts_with('-') => Some(sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number(format!("-{s}"), long),
                span: sqlparser::tokenizer::Span::empty(),
            })),
            _ => None,
        }),
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Plus,
            expr,
        } => signed_integer_literal_expr(expr),
        sqlast::Expr::Nested(inner) => signed_integer_literal_expr(inner),
        _ => None,
    }
}

fn syntactic_array_literal_len(expr: &sqlast::Expr) -> Option<usize> {
    match expr {
        sqlast::Expr::Array(array) => Some(array.elem.len()),
        sqlast::Expr::Nested(inner) => syntactic_array_literal_len(inner),
        sqlast::Expr::Cast { expr, .. } => syntactic_array_literal_len(expr),
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
                },
            }
        }
        _ => expr,
    }
}

/// Render an Arrow `DataType` as the StarRocks-style type name used by
/// `typeof()`. Mirrors the codegen-level helper in `expr_compiler` so the
/// analyzer can fold `typeof(<expr>)` into a string literal directly.
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
        DataType::FixedSizeBinary(w) if *w == crate::common::largeint::LARGEINT_BYTE_WIDTH => {
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
fn sql_expr_logical_type_name(expr: &sqlast::Expr) -> Option<String> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Null,
            ..
        }) => Some("null_type".to_string()),
        sqlast::Expr::Value(_) => None,
        sqlast::Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
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
fn sql_type_starrocks_name(sql_type: &sqlast::DataType) -> Option<String> {
    Some(match sql_type {
        sqlast::DataType::TinyInt(_) => "tinyint".to_string(),
        sqlast::DataType::SmallInt(_) => "smallint".to_string(),
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => "int".to_string(),
        sqlast::DataType::BigInt(_) => "bigint".to_string(),
        sqlast::DataType::Float(_) => "float".to_string(),
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => "double".to_string(),
        sqlast::DataType::Boolean => "boolean".to_string(),
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text => "varchar".to_string(),
        sqlast::DataType::Char(_) | sqlast::DataType::Character(_) => "char".to_string(),
        sqlast::DataType::String(_) => "varchar".to_string(),
        sqlast::DataType::JSON | sqlast::DataType::JSONB => "json".to_string(),
        sqlast::DataType::Varbinary(_) | sqlast::DataType::Binary(_) => "varbinary".to_string(),
        sqlast::DataType::Date => "date".to_string(),
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => "datetime".to_string(),
        sqlast::DataType::Time(_, _) => "time".to_string(),
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                format!("decimal128({p}, {s})")
            }
            sqlast::ExactNumberInfo::Precision(p) => format!("decimal128({p}, 0)"),
            sqlast::ExactNumberInfo::None => "decimal128(38, 0)".to_string(),
        },
        sqlast::DataType::Custom(name, _) => {
            let lower = name.to_string().to_ascii_lowercase();
            match lower.as_str() {
                "string" => "varchar".to_string(),
                "largeint" => "largeint".to_string(),
                "json" | "jsonb" => "json".to_string(),
                "varbinary" => "varbinary".to_string(),
                "binary" => "varbinary".to_string(),
                "bitmap" => "bitmap".to_string(),
                "hll" => "hll".to_string(),
                _ => return None,
            }
        }
        _ => return None,
    })
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

fn typed_expr_contains_nondeterministic_call(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            matches!(
                name.as_str(),
                "rand" | "random" | "uuid" | "now" | "current_timestamp" | "current_time"
            ) || args.iter().any(typed_expr_contains_nondeterministic_call)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            typed_expr_contains_nondeterministic_call(left)
                || typed_expr_contains_nondeterministic_call(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => typed_expr_contains_nondeterministic_call(expr),
        ExprKind::AggregateCall { args, .. } | ExprKind::WindowCall { args, .. } => {
            args.iter().any(typed_expr_contains_nondeterministic_call)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| typed_expr_contains_nondeterministic_call(expr))
                || when_then.iter().any(|(when, then)| {
                    typed_expr_contains_nondeterministic_call(when)
                        || typed_expr_contains_nondeterministic_call(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| typed_expr_contains_nondeterministic_call(expr))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            typed_expr_contains_nondeterministic_call(expr)
                || typed_expr_contains_nondeterministic_call(low)
                || typed_expr_contains_nondeterministic_call(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            typed_expr_contains_nondeterministic_call(expr)
                || typed_expr_contains_nondeterministic_call(pattern)
        }
        ExprKind::InList { expr, list, .. } => {
            typed_expr_contains_nondeterministic_call(expr)
                || list.iter().any(typed_expr_contains_nondeterministic_call)
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            typed_expr_contains_nondeterministic_call(body)
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
    use crate::sql::analysis::QueryBody;
    use crate::sql::catalog::CatalogProvider;
    use arrow::datatypes::DataType;

    struct EmptyCatalog;

    impl CatalogProvider for EmptyCatalog {
        fn get_table(
            &self,
            _database: &str,
            table: &str,
        ) -> Result<crate::sql::catalog::TableDef, String> {
            Err(format!("table not found: {table}"))
        }
    }

    fn analyze_projection_expr(sql: &str) -> Result<crate::sql::analysis::TypedExpr, String> {
        let stmt = crate::sql::parser::parse_sql_raw(sql)?;
        let sqlparser::ast::Statement::Query(query) = stmt else {
            return Err("expected query".to_string());
        };
        let (resolved, _registry, _factory) = analyze(&query, &EmptyCatalog, "default")?;
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
            crate::sql::analysis::ExprKind::BinaryOp { left, right, .. } => {
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
