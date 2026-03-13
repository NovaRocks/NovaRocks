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
use arrow::datatypes::DataType;
use chrono::{NaiveDate, NaiveDateTime};
use tracing::debug;

use crate::connector::{MinMaxPredicate, MinMaxPredicateValue};
use crate::exec::expr::LiteralValue;
use crate::exprs;
use crate::lower::expr::literals::{
    build_decimal_literal, parse_date_literal, parse_decimal_literal,
};
use crate::lower::layout::Layout;
use crate::lower::type_lowering::{arrow_type_from_desc, primitive_type_from_node};
use crate::types;

/// Parse a min/max conjunct TExpr into MinMaxPredicate used for row group pruning.
pub(crate) fn parse_min_max_conjunct(
    expr: &exprs::TExpr,
    layout: &Layout,
) -> Result<Option<MinMaxPredicate>, String> {
    if expr.nodes.is_empty() {
        return Ok(None);
    }

    let root = &expr.nodes[0];
    if root.node_type != exprs::TExprNodeType::BINARY_PRED {
        return Ok(None);
    }

    let Some(opcode) = root.opcode else {
        return Ok(None);
    };

    let predicate_type = if opcode == crate::opcodes::TExprOpcode::LE {
        "Le"
    } else if opcode == crate::opcodes::TExprOpcode::GE {
        "Ge"
    } else if opcode == crate::opcodes::TExprOpcode::LT {
        "Lt"
    } else if opcode == crate::opcodes::TExprOpcode::GT {
        "Gt"
    } else if opcode == crate::opcodes::TExprOpcode::EQ {
        "Eq"
    } else {
        return Ok(None);
    };

    if expr.nodes.len() < 3 {
        return Ok(None);
    }

    let left_node = &expr.nodes[1];
    let right_node = &expr.nodes[2];

    if left_node.node_type != exprs::TExprNodeType::SLOT_REF {
        return Ok(None);
    }
    let Some(slot_ref) = &left_node.slot_ref else {
        return Ok(None);
    };

    let column = get_column_name_from_slot(slot_ref, layout)?;
    let value = match extract_literal_value(right_node) {
        Ok(value) => value,
        Err(err) => {
            debug!(
                "skip min/max predicate pruning for slot {} because rhs is not a supported scalar literal: {}",
                column, err
            );
            return Ok(None);
        }
    };

    let predicate = match predicate_type {
        "Le" => MinMaxPredicate::Le { column, value },
        "Ge" => MinMaxPredicate::Ge { column, value },
        "Lt" => MinMaxPredicate::Lt { column, value },
        "Gt" => MinMaxPredicate::Gt { column, value },
        "Eq" => MinMaxPredicate::Eq { column, value },
        _ => return Ok(None),
    };

    Ok(Some(predicate))
}

fn get_column_name_from_slot(
    slot_ref: &exprs::TSlotRef,
    layout: &Layout,
) -> Result<String, String> {
    let key = (slot_ref.tuple_id, slot_ref.slot_id);
    let idx = layout
        .index
        .get(&key)
        .ok_or_else(|| format!("slot not found in layout: {:?}", key))?;

    Ok(idx.to_string())
}

fn extract_literal_value(node: &exprs::TExprNode) -> Result<MinMaxPredicateValue, String> {
    match node.node_type {
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let v = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing value".to_string())?
                .value;
            extract_int_literal(node, v)
        }
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => {
            let raw = node
                .large_int_literal
                .as_ref()
                .ok_or_else(|| "LARGE_INT_LITERAL missing value".to_string())?
                .value
                .trim()
                .to_string();
            let v = raw
                .parse::<i128>()
                .map_err(|_| format!("failed to parse LARGE_INT_LITERAL '{}'", raw))?;
            extract_large_int_literal(node, v)
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let raw = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing value".to_string())?
                .value
                .clone();
            match build_decimal_literal(node, &raw)? {
                LiteralValue::Decimal128 {
                    value,
                    precision,
                    scale,
                } => Ok(MinMaxPredicateValue::Decimal128 {
                    value,
                    precision,
                    scale,
                }),
                LiteralValue::Decimal256 { .. } => {
                    Err("min/max predicate does not support DECIMAL256 literal".to_string())
                }
                other => Err(format!(
                    "DECIMAL_LITERAL lowered to unexpected value for min/max predicate: {:?}",
                    other
                )),
            }
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let v = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing value".to_string())?
                .value
                .0;
            extract_float_literal(node, v)
        }
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let v = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing value".to_string())?
                .value;
            Ok(MinMaxPredicateValue::Boolean(v))
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL => {
            let v = node
                .string_literal
                .as_ref()
                .ok_or_else(|| "STRING_LITERAL missing value".to_string())?
                .value
                .clone();
            extract_string_literal(node, &v)
        }
        t if t == exprs::TExprNodeType::BINARY_LITERAL => {
            let v = node
                .binary_literal
                .as_ref()
                .ok_or_else(|| "BINARY_LITERAL missing value".to_string())?
                .value
                .clone();
            if matches!(
                arrow_type_from_desc(&node.type_),
                Some(DataType::FixedSizeBinary(_))
            ) {
                Ok(MinMaxPredicateValue::FixedLenByteArray(v))
            } else {
                Ok(MinMaxPredicateValue::ByteArray(v))
            }
        }
        t if t == exprs::TExprNodeType::DATE_LITERAL => {
            let v = node
                .date_literal
                .as_ref()
                .ok_or_else(|| "DATE_LITERAL missing value".to_string())?
                .value
                .clone();
            extract_date_literal(node, &v)
        }
        t if t == exprs::TExprNodeType::NULL_LITERAL => {
            Err("min/max predicate does not support NULL literal".to_string())
        }
        _ => Err(format!(
            "unsupported literal type in min/max predicate: {:?}",
            node.node_type
        )),
    }
}

fn extract_int_literal(
    node: &exprs::TExprNode,
    value: i64,
) -> Result<MinMaxPredicateValue, String> {
    match primitive_type_from_node(node) {
        Some(t)
            if t == types::TPrimitiveType::TINYINT
                || t == types::TPrimitiveType::SMALLINT
                || t == types::TPrimitiveType::INT =>
        {
            let v = i32::try_from(value).map_err(|_| {
                format!("INT_LITERAL out of range for INT32-compatible type: {value}")
            })?;
            Ok(MinMaxPredicateValue::Int32(v))
        }
        Some(t) if t == types::TPrimitiveType::BIGINT => Ok(MinMaxPredicateValue::Int64(value)),
        Some(t) if t == types::TPrimitiveType::LARGEINT => {
            Ok(MinMaxPredicateValue::LargeInt(i128::from(value)))
        }
        Some(t) if t == types::TPrimitiveType::DATE => {
            let v = i32::try_from(value)
                .map_err(|_| format!("INT_LITERAL out of range for DATE: {value}"))?;
            Ok(MinMaxPredicateValue::Date32(v))
        }
        Some(t) if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME => {
            Ok(MinMaxPredicateValue::DateTimeMicros(value))
        }
        Some(t) if is_decimal_type(&t) => {
            let (precision, scale) = decimal_params_from_node(node)?;
            let scaled = scale_integer(i128::from(value), scale).ok_or_else(|| {
                format!(
                    "INT_LITERAL cannot be represented as DECIMAL({}, {})",
                    precision, scale
                )
            })?;
            if !fits_decimal_precision(scaled, precision) {
                return Err(format!(
                    "INT_LITERAL {} exceeds DECIMAL precision {}",
                    value, precision
                ));
            }
            Ok(MinMaxPredicateValue::Decimal128 {
                value: scaled,
                precision,
                scale,
            })
        }
        Some(other) => Err(format!(
            "unsupported INT_LITERAL primitive type for min/max predicate: {:?}",
            other
        )),
        None => Ok(MinMaxPredicateValue::Int64(value)),
    }
}

fn extract_large_int_literal(
    node: &exprs::TExprNode,
    value: i128,
) -> Result<MinMaxPredicateValue, String> {
    match primitive_type_from_node(node) {
        Some(t)
            if t == types::TPrimitiveType::TINYINT
                || t == types::TPrimitiveType::SMALLINT
                || t == types::TPrimitiveType::INT =>
        {
            let v = i32::try_from(value).map_err(|_| {
                format!(
                    "LARGE_INT_LITERAL out of range for INT32-compatible type: {}",
                    value
                )
            })?;
            Ok(MinMaxPredicateValue::Int32(v))
        }
        Some(t) if t == types::TPrimitiveType::BIGINT => {
            let v = i64::try_from(value)
                .map_err(|_| format!("LARGE_INT_LITERAL out of range for BIGINT: {}", value))?;
            Ok(MinMaxPredicateValue::Int64(v))
        }
        Some(t) if t == types::TPrimitiveType::LARGEINT => {
            Ok(MinMaxPredicateValue::LargeInt(value))
        }
        Some(t) if t == types::TPrimitiveType::DATE => {
            let v = i32::try_from(value)
                .map_err(|_| format!("LARGE_INT_LITERAL out of range for DATE: {}", value))?;
            Ok(MinMaxPredicateValue::Date32(v))
        }
        Some(t) if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME => {
            let v = i64::try_from(value)
                .map_err(|_| format!("LARGE_INT_LITERAL out of range for DATETIME: {}", value))?;
            Ok(MinMaxPredicateValue::DateTimeMicros(v))
        }
        Some(t) if is_decimal_type(&t) => {
            let (precision, scale) = decimal_params_from_node(node)?;
            let scaled = scale_integer(value, scale).ok_or_else(|| {
                format!(
                    "LARGE_INT_LITERAL cannot be represented as DECIMAL({}, {})",
                    precision, scale
                )
            })?;
            if !fits_decimal_precision(scaled, precision) {
                return Err(format!(
                    "LARGE_INT_LITERAL {} exceeds DECIMAL precision {}",
                    value, precision
                ));
            }
            Ok(MinMaxPredicateValue::Decimal128 {
                value: scaled,
                precision,
                scale,
            })
        }
        Some(other) => Err(format!(
            "unsupported LARGE_INT_LITERAL primitive type for min/max predicate: {:?}",
            other
        )),
        None => Ok(MinMaxPredicateValue::LargeInt(value)),
    }
}

fn extract_float_literal(
    node: &exprs::TExprNode,
    value: f64,
) -> Result<MinMaxPredicateValue, String> {
    match primitive_type_from_node(node) {
        Some(t) if t == types::TPrimitiveType::FLOAT => {
            Ok(MinMaxPredicateValue::Float(value as f32))
        }
        Some(t) if t == types::TPrimitiveType::DOUBLE => Ok(MinMaxPredicateValue::Double(value)),
        Some(other) => Err(format!(
            "unsupported FLOAT_LITERAL primitive type for min/max predicate: {:?}",
            other
        )),
        None => Ok(MinMaxPredicateValue::Double(value)),
    }
}

fn extract_string_literal(
    node: &exprs::TExprNode,
    value: &str,
) -> Result<MinMaxPredicateValue, String> {
    match primitive_type_from_node(node) {
        Some(t) if t == types::TPrimitiveType::DATE => {
            Ok(MinMaxPredicateValue::Date32(parse_date_literal(value)?))
        }
        Some(t) if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME => Ok(
            MinMaxPredicateValue::DateTimeMicros(parse_datetime_literal_micros(value)?),
        ),
        Some(t) if t == types::TPrimitiveType::BOOLEAN => parse_bool_literal(value)
            .map(MinMaxPredicateValue::Boolean)
            .ok_or_else(|| {
                format!(
                    "failed to parse BOOLEAN literal '{}' for min/max predicate",
                    value
                )
            }),
        Some(t)
            if t == types::TPrimitiveType::TINYINT
                || t == types::TPrimitiveType::SMALLINT
                || t == types::TPrimitiveType::INT =>
        {
            let parsed = value
                .trim()
                .parse::<i32>()
                .map_err(|e| format!("failed to parse INT literal '{}': {}", value, e))?;
            Ok(MinMaxPredicateValue::Int32(parsed))
        }
        Some(t) if t == types::TPrimitiveType::BIGINT => {
            let parsed = value
                .trim()
                .parse::<i64>()
                .map_err(|e| format!("failed to parse BIGINT literal '{}': {}", value, e))?;
            Ok(MinMaxPredicateValue::Int64(parsed))
        }
        Some(t) if t == types::TPrimitiveType::LARGEINT => {
            let parsed = value
                .trim()
                .parse::<i128>()
                .map_err(|e| format!("failed to parse LARGEINT literal '{}': {}", value, e))?;
            Ok(MinMaxPredicateValue::LargeInt(parsed))
        }
        Some(t) if t == types::TPrimitiveType::FLOAT => {
            let parsed = value
                .trim()
                .parse::<f32>()
                .map_err(|e| format!("failed to parse FLOAT literal '{}': {}", value, e))?;
            Ok(MinMaxPredicateValue::Float(parsed))
        }
        Some(t) if t == types::TPrimitiveType::DOUBLE => {
            let parsed = value
                .trim()
                .parse::<f64>()
                .map_err(|e| format!("failed to parse DOUBLE literal '{}': {}", value, e))?;
            Ok(MinMaxPredicateValue::Double(parsed))
        }
        Some(t) if is_decimal_type(&t) => {
            let (precision, scale) = decimal_params_from_node(node)?;
            let parsed = parse_decimal_literal(value, precision, scale)?;
            Ok(MinMaxPredicateValue::Decimal128 {
                value: parsed,
                precision,
                scale,
            })
        }
        Some(_) | None => Ok(MinMaxPredicateValue::ByteArray(value.as_bytes().to_vec())),
    }
}

fn extract_date_literal(
    node: &exprs::TExprNode,
    value: &str,
) -> Result<MinMaxPredicateValue, String> {
    match primitive_type_from_node(node) {
        Some(t)
            if t == types::TPrimitiveType::DATE
                || t == types::TPrimitiveType::DATETIME
                || t == types::TPrimitiveType::TIME =>
        {
            if t == types::TPrimitiveType::DATE {
                Ok(MinMaxPredicateValue::Date32(parse_date_literal(value)?))
            } else {
                Ok(MinMaxPredicateValue::DateTimeMicros(
                    parse_datetime_literal_micros(value)?,
                ))
            }
        }
        Some(_) => Ok(MinMaxPredicateValue::ByteArray(value.as_bytes().to_vec())),
        None => Ok(MinMaxPredicateValue::Date32(parse_date_literal(value)?)),
    }
}

fn decimal_params_from_node(node: &exprs::TExprNode) -> Result<(u8, i8), String> {
    match arrow_type_from_desc(&node.type_) {
        Some(DataType::Decimal128(precision, scale)) => Ok((precision, scale)),
        Some(DataType::Decimal256(_, _)) => {
            Err("min/max predicate does not support DECIMAL256 literal".to_string())
        }
        Some(other) => Err(format!(
            "min/max predicate decimal literal type mismatch: {:?}",
            other
        )),
        None => Err("min/max predicate decimal literal missing decimal type metadata".to_string()),
    }
}

fn parse_datetime_literal_micros(value: &str) -> Result<i64, String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("empty DATETIME literal".to_string());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid DATETIME literal '{}'", value))?;
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid DATETIME literal '{}'", value))
}

fn parse_bool_literal(value: &str) -> Option<bool> {
    match value.trim() {
        "0" | "false" | "FALSE" => Some(false),
        "1" | "true" | "TRUE" => Some(true),
        _ => None,
    }
}

fn is_decimal_type(ltype: &types::TPrimitiveType) -> bool {
    matches!(
        *ltype,
        types::TPrimitiveType::DECIMAL
            | types::TPrimitiveType::DECIMALV2
            | types::TPrimitiveType::DECIMAL32
            | types::TPrimitiveType::DECIMAL64
            | types::TPrimitiveType::DECIMAL128
            | types::TPrimitiveType::DECIMAL256
    )
}

fn scale_integer(value: i128, target_scale: i8) -> Option<i128> {
    if target_scale < 0 {
        return None;
    }
    let mut factor = 1i128;
    for _ in 0..u32::try_from(target_scale).ok()? {
        factor = factor.checked_mul(10)?;
    }
    value.checked_mul(factor)
}

fn fits_decimal_precision(value: i128, precision: u8) -> bool {
    if precision == 0 {
        return false;
    }
    let mut n = value.unsigned_abs();
    let mut digits = 1usize;
    while n >= 10 {
        n /= 10;
        digits += 1;
    }
    digits <= usize::from(precision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_dummy_type() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn default_t_expr_node() -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: create_dummy_type(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn single_slot_layout() -> Layout {
        Layout {
            order: vec![(1, 1)],
            index: HashMap::from([((1, 1), 0usize)]),
        }
    }

    #[test]
    fn parse_min_max_conjunct_skips_non_literal_rhs() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::CAST_EXPR,
                    num_children: 1,
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjunct(&expr, &single_slot_layout()).expect("parse");
        assert!(
            parsed.is_none(),
            "non-literal rhs should not produce min/max pruning"
        );
    }

    #[test]
    fn parse_min_max_conjunct_keeps_scalar_literal_rhs() {
        let expr = exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(crate::opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 1,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::INT_LITERAL,
                    int_literal: Some(exprs::TIntLiteral { value: 7 }),
                    ..default_t_expr_node()
                },
            ],
        };

        let parsed = parse_min_max_conjunct(&expr, &single_slot_layout()).expect("parse");
        assert_eq!(
            parsed,
            Some(MinMaxPredicate::Eq {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int64(7),
            })
        );
    }
}
