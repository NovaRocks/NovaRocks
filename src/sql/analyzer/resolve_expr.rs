use std::sync::Arc;

use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::analysis::*;
use crate::sql::types::{arithmetic_result_type_with_op, wider_type};

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

            // Qualified column reference or STRUCT field chain encoded by sqlparser
            // as a compound identifier (for example `c13.a`).
            sqlast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                self.analyze_compound_identifier(parts, scope)
            }

            // Literals
            sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => self.analyze_literal(value),

            sqlast::Expr::Array(array) => self.analyze_array_literal(array, scope),

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
            sqlast::Expr::Ceil { expr, field } => {
                self.analyze_ceil_floor_expr("ceil", expr, field, scope)
            }
            sqlast::Expr::Floor { expr, field } => {
                self.analyze_ceil_floor_expr("floor", expr, field, scope)
            }

            sqlast::Expr::CompoundFieldAccess { root, access_chain } => {
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
                    DataType::Struct(_) => {
                        return match &index_typed.kind {
                            ExprKind::Literal(LiteralValue::String(field_name)) => {
                                self.analyze_struct_field_access(base, field_name.clone())
                            }
                            _ => Err(format!(
                                "struct subscript requires a string literal field name, got {:?}",
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

    fn analyze_compound_identifier(
        &self,
        parts: &[sqlast::Ident],
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        if parts.len() == 2 {
            let qualifier = &parts[0].value;
            let col_name = &parts[1].value;
            if let Ok((data_type, nullable)) = scope.resolve(Some(qualifier), col_name) {
                return Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(qualifier.to_lowercase()),
                        column: col_name.to_lowercase(),
                    },
                    data_type,
                    nullable,
                });
            }
        } else if parts.len() == 3 {
            let qualifier = &parts[1].value;
            let col_name = &parts[2].value;
            if let Ok((data_type, nullable)) = scope.resolve(Some(qualifier), col_name) {
                return Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(qualifier.to_lowercase()),
                        column: col_name.to_lowercase(),
                    },
                    data_type,
                    nullable,
                });
            }
        }

        let base_name = &parts[0].value;
        let (data_type, nullable) = scope.resolve(None, base_name)?;
        let mut current = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: base_name.to_lowercase(),
            },
            data_type,
            nullable,
        };
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
        let field = fields
            .iter()
            .find(|field| field.name() == &field_name)
            .ok_or_else(|| format!("struct field '{}' does not exist", field_name))?;
        let field_type = field.data_type().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(field_name)),
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

    fn analyze_array_literal(
        &self,
        array: &sqlast::Array,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, String> {
        let mut args = Vec::with_capacity(array.elem.len());
        let mut item_type = DataType::Null;
        for item in &array.elem {
            let typed = self.analyze_expr(item, scope)?;
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
        if let Some(rewritten) = self.try_analyze_array_map_cast_lambda(&name, &arg_exprs, scope)? {
            return Ok(rewritten);
        }

        // Analyze arguments. For the narrow standalone lambda support needed by
        // aggregate suite, rewrite `array_sortby((x) -> x.field, arr)` into
        // `array_sortby(arr, __array_struct_subfield(arr, 'field'))`.
        let (mut args_typed, mut arg_types) = if name == "array_sortby"
            && arg_exprs
                .first()
                .and_then(|expr| parse_array_sortby_lambda(expr))
                .is_some()
        {
            self.analyze_array_sortby_lambda_arguments(&arg_exprs, scope)?
        } else {
            let mut args_typed = Vec::with_capacity(arg_exprs.len());
            let mut arg_types = Vec::with_capacity(arg_exprs.len());
            for arg in &arg_exprs {
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
            validate_scalar_function_call(&name, &arg_types)?;
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
        let field = fields
            .iter()
            .find(|field| field.name() == &field_name)
            .ok_or_else(|| format!("struct field '{}' does not exist", field_name))?;
        let field_type = field.data_type().clone();
        let field_name_expr = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(field_name)),
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
