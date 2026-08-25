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

//! Deterministic expression-to-protobuf mapping for the native boundary.

use arrow::datatypes::DataType;
use arrow_buffer::i256;

use super::plan::encode_type;
use novarocks_proto::{common, expr};
use novarocks_sql::plan_read::{
    BinOp, LiteralValue, SortItem, SqlExpressionRead, SqlExpressionReadKind, TypedExpr, UnOp,
    WindowBound, WindowFrame, WindowFrameType, expression_read,
};
use novarocks_types::largeint;

pub(crate) fn encode_expr(e: &TypedExpr) -> Result<expr::Expr, String> {
    let read = expression_read(e);
    Ok(expr::Expr {
        r#type: Some(encode_type(&read.data_type)?),
        nullable: read.nullable,
        kind: Some(encode_expr_kind(&read)?),
    })
}

fn encode_expr_kind(e: &SqlExpressionRead) -> Result<expr::expr::Kind, String> {
    use expr::expr::Kind;

    Ok(match &e.kind {
        SqlExpressionReadKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => Kind::ColumnRef(expr::ColumnRef {
            column_id: column_id.0,
            qualifier: qualifier.clone(),
            column: Some(column.clone()),
        }),
        SqlExpressionReadKind::LambdaParamRef { name, slot_id } => {
            Kind::LambdaParamRef(expr::LambdaParamRef {
                slot_id: *slot_id,
                name: Some(name.clone()),
            })
        }
        SqlExpressionReadKind::Literal(value) => Kind::Literal(expr::LiteralExpr {
            value: Some(encode_literal(value, &e.data_type)?),
        }),
        SqlExpressionReadKind::BinaryOp { left, op, right } => {
            Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: encode_bin_op(*op) as i32,
                left: Some(Box::new(encode_expr(left)?)),
                right: Some(Box::new(encode_expr(right)?)),
            }))
        }
        SqlExpressionReadKind::UnaryOp { op, expr: inner } => {
            Kind::UnaryOp(Box::new(expr::UnaryOpExpr {
                op: encode_un_op(*op) as i32,
                operand: Some(Box::new(encode_expr(inner)?)),
            }))
        }
        SqlExpressionReadKind::FunctionCall {
            name,
            args,
            distinct,
        } => Kind::FunctionCall(expr::FunctionCall {
            function_name: name.clone(),
            args: encode_exprs(args)?,
            distinct: *distinct,
        }),
        SqlExpressionReadKind::LambdaFunction { params, body } => {
            Kind::Lambda(Box::new(expr::LambdaExpr {
                params: params
                    .iter()
                    .map(|param| {
                        Ok(expr::LambdaParam {
                            slot_id: param.slot_id,
                            name: Some(param.name.clone()),
                            r#type: Some(encode_type(&param.data_type)?),
                            nullable: param.nullable,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                body: Some(Box::new(encode_expr(body)?)),
            }))
        }
        SqlExpressionReadKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => Kind::AggregateCall(expr::AggregateCall {
            function_name: name.clone(),
            args: encode_exprs(args)?,
            distinct: *distinct,
            order_by: encode_sort_items(order_by)?,
        }),
        SqlExpressionReadKind::Cast {
            expr: inner,
            target,
        } => Kind::Cast(Box::new(expr::CastExpr {
            operand: Some(Box::new(encode_expr(inner)?)),
            target: Some(encode_type(target)?),
        })),
        SqlExpressionReadKind::IsNull {
            expr: inner,
            negated,
        } => Kind::IsNull(Box::new(expr::IsNullExpr {
            operand: Some(Box::new(encode_expr(inner)?)),
            negated: *negated,
        })),
        SqlExpressionReadKind::InList {
            expr: inner,
            list,
            negated,
        } => Kind::InList(Box::new(expr::InListExpr {
            operand: Some(Box::new(encode_expr(inner)?)),
            list: encode_exprs(list)?,
            negated: *negated,
        })),
        SqlExpressionReadKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Kind::Between(Box::new(expr::BetweenExpr {
            operand: Some(Box::new(encode_expr(inner)?)),
            low: Some(Box::new(encode_expr(low)?)),
            high: Some(Box::new(encode_expr(high)?)),
            negated: *negated,
        })),
        SqlExpressionReadKind::Like {
            expr: inner,
            pattern,
            negated,
        } => Kind::Like(Box::new(expr::LikeExpr {
            operand: Some(Box::new(encode_expr(inner)?)),
            pattern: Some(Box::new(encode_expr(pattern)?)),
            negated: *negated,
        })),
        SqlExpressionReadKind::Case {
            operand,
            when_then,
            else_expr,
        } => Kind::CaseExpr(Box::new(expr::CaseExpr {
            operand: operand
                .as_ref()
                .map(|operand| encode_expr(operand).map(Box::new))
                .transpose()?,
            when_then: when_then
                .iter()
                .map(|(when, then)| {
                    Ok(expr::WhenThen {
                        when: Some(encode_expr(when)?),
                        then: Some(encode_expr(then)?),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| encode_expr(else_expr).map(Box::new))
                .transpose()?,
        })),
        SqlExpressionReadKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => Kind::IsTruth(Box::new(expr::IsTruthExpr {
            operand: Some(Box::new(encode_expr(inner)?)),
            value: *value,
            negated: *negated,
        })),
        SqlExpressionReadKind::Nested(inner) => Kind::Nested(Box::new(expr::NestedExpr {
            inner: Some(Box::new(encode_expr(inner)?)),
        })),
        SqlExpressionReadKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => Kind::WindowCall(expr::WindowCall {
            function_name: name.clone(),
            args: encode_exprs(args)?,
            distinct: *distinct,
            partition_by: encode_exprs(partition_by)?,
            order_by: encode_sort_items(order_by)?,
            frame: window_frame.as_ref().map(encode_window_frame).transpose()?,
            ignore_nulls: *ignore_nulls,
        }),
        SqlExpressionReadKind::SubqueryPlaceholder { id } => {
            return Err(format!(
                "unexpected SubqueryPlaceholder (id={id}) in FE proto expression encoder"
            ));
        }
        SqlExpressionReadKind::Lambda => {
            return Err(
                "SQL lambda expression cannot be encoded as native proto without parameter slot/type bindings; use LambdaFunction"
                    .to_string(),
            );
        }
    })
}

fn encode_exprs(values: &[TypedExpr]) -> Result<Vec<expr::Expr>, String> {
    values.iter().map(encode_expr).collect()
}

pub(super) fn encode_sort_items(values: &[SortItem]) -> Result<Vec<expr::SortItem>, String> {
    values
        .iter()
        .map(|item| {
            Ok(expr::SortItem {
                expr: Some(encode_expr(&item.expr)?),
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub(super) fn encode_window_frame(frame: &WindowFrame) -> Result<expr::WindowFrame, String> {
    Ok(expr::WindowFrame {
        frame_type: match frame.frame_type {
            WindowFrameType::Rows => expr::WindowFrameType::Rows as i32,
            WindowFrameType::Range => expr::WindowFrameType::Range as i32,
        },
        start: Some(encode_window_bound(&frame.start)),
        end: Some(encode_window_bound(&frame.end)),
    })
}

pub(super) fn encode_window_bound(bound: &WindowBound) -> expr::WindowBound {
    use expr::window_bound::Bound;

    expr::WindowBound {
        bound: Some(match bound {
            WindowBound::UnboundedPreceding => Bound::UnboundedPreceding(true),
            WindowBound::Preceding(value) => Bound::Preceding(*value),
            WindowBound::CurrentRow => Bound::CurrentRow(true),
            WindowBound::Following(value) => Bound::Following(*value),
            WindowBound::UnboundedFollowing => Bound::UnboundedFollowing(true),
        }),
    }
}

fn encode_literal(
    value: &LiteralValue,
    data_type: &DataType,
) -> Result<common::LiteralValue, String> {
    use common::literal_value::Value;

    Ok(common::LiteralValue {
        value: Some(match value {
            LiteralValue::Null => Value::NullValue(true),
            LiteralValue::Bool(value) => Value::BoolValue(*value),
            LiteralValue::Int(value) if matches!(data_type, DataType::Date32) => {
                let value = i32::try_from(*value)
                    .map_err(|_| format!("Date32 literal {value} is outside i32 range"))?;
                Value::Date32Value(value)
            }
            LiteralValue::Int(value) => Value::IntValue(*value),
            LiteralValue::LargeInt(value) => {
                Value::LargeintValue(largeint::i128_to_be_bytes(*value).to_vec())
            }
            LiteralValue::Float(value) => Value::FloatValue(*value),
            LiteralValue::Decimal(value) => {
                Value::DecimalValue(encode_decimal_literal(value, data_type)?)
            }
            LiteralValue::String(value) => Value::StringValue(value.clone()),
            LiteralValue::Binary(value) => Value::BinaryValue(value.clone()),
        }),
    })
}

fn encode_decimal_literal(
    value: &str,
    data_type: &DataType,
) -> Result<common::DecimalLiteral, String> {
    let (precision, scale, bytes) = match data_type {
        DataType::Decimal128(precision, scale) => {
            validate_decimal_literal(*precision, *scale, 38, "Decimal128")?;
            let unscaled = parse_decimal_unscaled_i128(value, *precision, *scale)?;
            (*precision, *scale, unscaled.to_be_bytes().to_vec())
        }
        DataType::Decimal256(precision, scale) => {
            validate_decimal_literal(*precision, *scale, 76, "Decimal256")?;
            let unscaled = parse_decimal_unscaled_i256(value, *precision, *scale)?;
            (*precision, *scale, unscaled.to_be_bytes().to_vec())
        }
        other => {
            return Err(format!(
                "decimal literal requires Decimal128/Decimal256 type, got {other:?}"
            ));
        }
    };
    Ok(common::DecimalLiteral {
        value: bytes,
        precision: u32::from(precision),
        scale: i32::from(scale),
    })
}

fn validate_decimal_literal(
    precision: u8,
    scale: i8,
    max_precision: u8,
    label: &str,
) -> Result<(), String> {
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{label} precision {precision} must be between 1 and {max_precision}"
        ));
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "{label} scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

fn parse_decimal_unscaled_i128(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
    let (negative, digits) = normalized_decimal_digits(value, precision, scale)?;
    let signed = if negative && digits != "0" {
        format!("-{digits}")
    } else {
        digits
    };
    signed
        .parse::<i128>()
        .map_err(|err| format!("decimal literal '{value}' is outside supported range: {err}"))
}

fn parse_decimal_unscaled_i256(value: &str, precision: u8, scale: i8) -> Result<i256, String> {
    let (negative, digits) = normalized_decimal_digits(value, precision, scale)?;
    let ten = i256::from_i128(10);
    let mut out = i256::ZERO;
    for ch in digits.bytes() {
        let digit = (ch - b'0') as i128;
        out = out
            .checked_mul(ten)
            .and_then(|v| v.checked_add(i256::from_i128(digit)))
            .ok_or_else(|| format!("decimal literal '{value}' is outside i256 range"))?;
    }
    if negative {
        out = out
            .checked_neg()
            .ok_or_else(|| format!("decimal literal '{value}' is outside i256 range"))?;
    }
    Ok(out)
}

fn normalized_decimal_digits(
    value: &str,
    precision: u8,
    scale: i8,
) -> Result<(bool, String), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("decimal literal is empty".to_string());
    }

    let (negative, digits_part) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if digits_part.is_empty() {
        return Err(format!("malformed decimal literal '{value}'"));
    }

    let mut split = digits_part.split('.');
    let int_part = split.next().unwrap_or_default();
    let frac_part = split.next();
    if split.next().is_some() {
        return Err(format!("malformed decimal literal '{value}'"));
    }
    let frac_part = frac_part.unwrap_or_default();
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(format!("malformed decimal literal '{value}'"));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("malformed decimal literal '{value}'"));
    }
    if frac_part.len() > scale as usize {
        return Err(format!(
            "decimal literal '{value}' scale {} exceeds target scale {scale}",
            frac_part.len()
        ));
    }

    let mut digits = String::with_capacity(int_part.len() + scale as usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..scale as usize {
        digits.push('0');
    }
    let significant = digits.trim_start_matches('0');
    if significant.len() > precision as usize {
        return Err(format!(
            "decimal literal '{value}' exceeds target precision {precision}"
        ));
    }
    let digits = if significant.is_empty() {
        "0"
    } else {
        significant
    }
    .to_string();
    Ok((negative, digits))
}

fn encode_bin_op(op: BinOp) -> expr::BinaryOp {
    match op {
        BinOp::Add => expr::BinaryOp::Add,
        BinOp::Sub => expr::BinaryOp::Sub,
        BinOp::Mul => expr::BinaryOp::Mul,
        BinOp::Div => expr::BinaryOp::Div,
        BinOp::Mod => expr::BinaryOp::Mod,
        BinOp::Eq => expr::BinaryOp::Eq,
        BinOp::Ne => expr::BinaryOp::Ne,
        BinOp::Lt => expr::BinaryOp::Lt,
        BinOp::Le => expr::BinaryOp::Le,
        BinOp::Gt => expr::BinaryOp::Gt,
        BinOp::Ge => expr::BinaryOp::Ge,
        BinOp::EqForNull => expr::BinaryOp::EqForNull,
        BinOp::And => expr::BinaryOp::And,
        BinOp::Or => expr::BinaryOp::Or,
    }
}

fn encode_un_op(op: UnOp) -> expr::UnaryOp {
    match op {
        UnOp::Not => expr::UnaryOp::Not,
        UnOp::Negate => expr::UnaryOp::Negate,
        UnOp::BitwiseNot => expr::UnaryOp::BitwiseNot,
    }
}
