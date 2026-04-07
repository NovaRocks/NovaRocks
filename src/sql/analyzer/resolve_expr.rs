use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::ir::*;
use crate::sql::types::{arithmetic_result_type_with_op, wider_type};

use super::functions::*;
use super::helpers::{eval_const_i64, sql_type_to_arrow};
use super::scope::AnalyzerScope;

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
                let (data_type, nullable) = scope.resolve(None, &ident.value)?;
                Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: ident.value.to_lowercase(),
                    },
                    data_type,
                    nullable,
                })
            }

            // Qualified column reference: table.column or db.table.column
            sqlast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let qualifier = &parts[0].value;
                let col_name = &parts[1].value;
                let (data_type, nullable) = scope.resolve(Some(qualifier), col_name)?;
                Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(qualifier.to_lowercase()),
                        column: col_name.to_lowercase(),
                    },
                    data_type,
                    nullable,
                })
            }
            // Three-part reference: db.table.column — ignore the db part
            sqlast::Expr::CompoundIdentifier(parts) if parts.len() == 3 => {
                let qualifier = &parts[1].value;
                let col_name = &parts[2].value;
                let (data_type, nullable) = scope.resolve(Some(qualifier), col_name)?;
                Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(qualifier.to_lowercase()),
                        column: col_name.to_lowercase(),
                    },
                    data_type,
                    nullable,
                })
            }

            // Literals
            sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => self.analyze_literal(value),

            // Binary operations
            sqlast::Expr::BinaryOp { left, op, right } => {
                self.analyze_binary_op(left, op, right, scope)
            }

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
                let expr_typed = self.analyze_expr(in_expr, scope)?;
                let mut list_typed = Vec::with_capacity(list.len());
                for item in list {
                    list_typed.push(self.analyze_expr(item, scope)?);
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
                let expr_typed = self.analyze_expr(between_expr, scope)?;
                let low_typed = self.analyze_expr(low, scope)?;
                let high_typed = self.analyze_expr(high, scope)?;
                // Implicit cast: when comparing date/timestamp with string,
                // cast the string to the date/timestamp type.
                let low_typed = coerce_to_target_type(low_typed, &expr_typed.data_type);
                let high_typed = coerce_to_target_type(high_typed, &expr_typed.data_type);
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
                // We don't know the exact scalar type yet; it will be resolved
                // during subquery rewriting. Use Null as placeholder.
                let kind = SubqueryKind::Scalar;
                self.collected_subqueries.borrow_mut().push(SubqueryInfo {
                    id,
                    kind: kind.clone(),
                    subquery: subquery.clone(),
                    data_type: DataType::Null,
                    in_expr: None,
                });
                // Return a placeholder with Null type; the rewrite pass will
                // replace it with a ColumnRef of the proper type.
                Ok(TypedExpr {
                    kind: ExprKind::SubqueryPlaceholder {
                        id,
                        kind,
                        data_type: DataType::Null,
                    },
                    data_type: DataType::Null,
                    nullable: true,
                })
            }

            // Typed literals: DATE '2024-01-01', TIMESTAMP '...', etc.
            sqlast::Expr::TypedString(typed_str) => {
                let target = sql_type_to_arrow(&typed_str.data_type)?;
                let value = typed_str.value.to_string();
                // For DATE literals, constant-fold to Date32 integer value
                if target == DataType::Date32 {
                    let date_str = value.trim_matches('\'');
                    let days = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                        .map_err(|e| format!("invalid date literal '{date_str}': {e}"))?
                        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i64;
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
                Ok(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String(s.clone())),
                    data_type: DataType::Utf8,
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
        let name = func.name.to_string().to_lowercase();

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

        // Analyze arguments
        let mut args_typed = Vec::with_capacity(arg_exprs.len());
        let mut arg_types = Vec::with_capacity(arg_exprs.len());
        for arg in &arg_exprs {
            let typed = self.analyze_expr(arg, scope)?;
            arg_types.push(typed.data_type.clone());
            args_typed.push(typed);
        }

        // Extract ORDER BY within function args (for aggregates like array_agg)
        let func_order_by = self.extract_function_order_by(func, scope)?;

        // Check for window function: func(...) OVER (...)
        if let Some(ref window_type) = func.over {
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
            return Ok(TypedExpr {
                kind: ExprKind::WindowCall {
                    name,
                    args: args_typed,
                    distinct: is_distinct,
                    partition_by,
                    order_by,
                    window_frame,
                },
                data_type: return_type,
                nullable: true,
            });
        }

        // Implicit cast: for string functions like concat/concat_ws, auto-cast
        // non-string arguments to Utf8.
        let needs_string_args = matches!(
            name.as_str(),
            "concat" | "concat_ws" | "group_concat" | "string_agg"
        );
        if needs_string_args {
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
            // Update arg_types after cast
            arg_types = args_typed.iter().map(|a| a.data_type.clone()).collect();
        }

        // IF(cond, then, else): cast first arg to Boolean if needed
        if name == "if" && args_typed.len() >= 1 && args_typed[0].data_type != DataType::Boolean {
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
            let return_type = infer_scalar_return_type(&name, &arg_types);
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

    /// Extract ORDER BY clauses from within function arguments (e.g. array_agg(x ORDER BY y)).
    fn extract_function_order_by(
        &self,
        func: &sqlast::Function,
        scope: &AnalyzerScope,
    ) -> Result<Vec<SortItem>, String> {
        let clauses = match &func.args {
            sqlast::FunctionArguments::List(list) => &list.clauses,
            _ => return Ok(vec![]),
        };

        for clause in clauses {
            if let sqlast::FunctionArgumentClause::OrderBy(order_by_exprs) = clause {
                let mut items = Vec::with_capacity(order_by_exprs.len());
                for ob in order_by_exprs {
                    let typed = self.analyze_expr(&ob.expr, scope)?;
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
    ) -> Result<(Vec<TypedExpr>, Vec<SortItem>, Option<WindowFrame>), String> {
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
    fn expr_contains_aggregate(&self, expr: &sqlast::Expr) -> bool {
        match expr {
            sqlast::Expr::Function(f) => {
                // A function with OVER is a window function, not an aggregate
                if f.over.is_some() {
                    return false;
                }
                is_aggregate_function(&f.name.to_string().to_lowercase())
            }
            sqlast::Expr::BinaryOp { left, right, .. } => {
                self.expr_contains_aggregate(left) || self.expr_contains_aggregate(right)
            }
            sqlast::Expr::UnaryOp { expr, .. } => self.expr_contains_aggregate(expr),
            sqlast::Expr::Nested(inner) => self.expr_contains_aggregate(inner),
            sqlast::Expr::Cast { expr, .. } => self.expr_contains_aggregate(expr),
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
    let precision = precision.max(1).min(38) as u8;
    let scale = scale.min(38) as i8;
    (precision, scale)
}

/// Implicit cast: if `expr` is Utf8 and `target` is a date/timestamp type,
/// wrap `expr` in a Cast to the target type. This matches StarRocks FE
/// behavior where string literals are implicitly cast to date/timestamp
/// in comparison contexts (BETWEEN, WHERE, etc.).
fn coerce_to_target_type(expr: TypedExpr, target: &DataType) -> TypedExpr {
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
