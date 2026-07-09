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
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use tracing::debug;

use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
use crate::thrift::exprs;
use crate::thrift::types;
use crate::types::arrow_thrift::{
    THRIFT_TIME_UNIT_NANOS, thrift_desc_to_arrow_type as arrow_type_from_desc,
};

/// Parse thrift min/max conjuncts into native pruning predicates.
pub(crate) fn parse_min_max_conjuncts_with_column_resolver<F>(
    expr: &exprs::TExpr,
    mut resolve_column: F,
) -> Result<Vec<MinMaxPredicate>, String>
where
    F: FnMut(&exprs::TSlotRef) -> Result<String, String>,
{
    if expr.nodes.is_empty() {
        return Ok(Vec::new());
    }

    let mut predicates = Vec::new();
    parse_min_max_node(&expr.nodes, 0, &mut resolve_column, &mut predicates)?;
    Ok(predicates)
}

fn parse_min_max_node<F>(
    nodes: &[exprs::TExprNode],
    idx: usize,
    resolve_column: &mut F,
    predicates: &mut Vec<MinMaxPredicate>,
) -> Result<usize, String>
where
    F: FnMut(&exprs::TSlotRef) -> Result<String, String>,
{
    let node = nodes
        .get(idx)
        .ok_or_else(|| format!("malformed TExpr: missing node at index {idx}"))?;

    if node.node_type == exprs::TExprNodeType::COMPOUND_PRED
        && node.opcode == Some(crate::thrift::opcodes::TExprOpcode::COMPOUND_AND)
    {
        let child_count = child_count(node)?;
        let mut next = idx + 1;
        for _ in 0..child_count {
            next = parse_min_max_node(nodes, next, resolve_column, predicates)?;
        }
        return Ok(next);
    }

    if node.node_type == exprs::TExprNodeType::BINARY_PRED {
        if let Some(predicate) = parse_binary_min_max_predicate(nodes, idx, resolve_column)? {
            predicates.push(predicate);
        }
    }

    skip_subtree(nodes, idx)
}

fn parse_binary_min_max_predicate<F>(
    nodes: &[exprs::TExprNode],
    idx: usize,
    resolve_column: &mut F,
) -> Result<Option<MinMaxPredicate>, String>
where
    F: FnMut(&exprs::TSlotRef) -> Result<String, String>,
{
    let root = nodes
        .get(idx)
        .ok_or_else(|| format!("malformed TExpr: missing binary predicate at index {idx}"))?;
    let Some(opcode) = root.opcode else {
        return Ok(None);
    };

    let predicate_type = if opcode == crate::thrift::opcodes::TExprOpcode::LE {
        "Le"
    } else if opcode == crate::thrift::opcodes::TExprOpcode::GE {
        "Ge"
    } else if opcode == crate::thrift::opcodes::TExprOpcode::LT {
        "Lt"
    } else if opcode == crate::thrift::opcodes::TExprOpcode::GT {
        "Gt"
    } else if opcode == crate::thrift::opcodes::TExprOpcode::EQ {
        "Eq"
    } else {
        return Ok(None);
    };

    if child_count(root)? != 2 {
        return Ok(None);
    }

    let left_idx = idx + 1;
    let right_idx = skip_subtree(nodes, left_idx)?;
    let Some(left_node) = nodes.get(left_idx) else {
        return Ok(None);
    };
    let Some(right_node) = nodes.get(right_idx) else {
        return Ok(None);
    };

    if left_node.node_type != exprs::TExprNodeType::SLOT_REF {
        return Ok(None);
    }
    let Some(slot_ref) = &left_node.slot_ref else {
        return Ok(None);
    };

    let column = resolve_column(slot_ref)?;
    let value = match extract_min_max_literal_value(root, left_node, right_node) {
        Ok(Some(value)) => value,
        Ok(None) => return Ok(None),
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

fn extract_min_max_literal_value(
    root: &exprs::TExprNode,
    left_node: &exprs::TExprNode,
    right_node: &exprs::TExprNode,
) -> Result<Option<MinMaxPredicateValue>, String> {
    let left_type = arrow_type_from_desc(&left_node.type_);
    let compare_type = root
        .child_type_desc
        .as_ref()
        .and_then(arrow_type_from_desc)
        .or_else(|| root.child_type.and_then(arrow_type_from_primitive));

    if let (Some(left_type), Some(compare_type)) = (left_type.as_ref(), compare_type.as_ref()) {
        if left_type != compare_type {
            debug!(
                "skip min/max predicate pruning because comparison type {:?} differs from scan column type {:?}",
                compare_type, left_type
            );
            return Ok(None);
        }
        if is_utf8_type(left_type) {
            return extract_literal_as_utf8_bytes(right_node).map(Some);
        }
    }

    Ok(Some(extract_literal_value(right_node)?))
}

fn child_count(node: &exprs::TExprNode) -> Result<usize, String> {
    usize::try_from(node.num_children).map_err(|_| {
        format!(
            "malformed TExpr: negative child count {}",
            node.num_children
        )
    })
}

fn skip_subtree(nodes: &[exprs::TExprNode], idx: usize) -> Result<usize, String> {
    let node = nodes
        .get(idx)
        .ok_or_else(|| format!("malformed TExpr: missing node at index {idx}"))?;
    let mut next = idx + 1;
    for _ in 0..child_count(node)? {
        next = skip_subtree(nodes, next)?;
    }
    Ok(next)
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
            build_decimal_literal_value(node, &raw)
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

fn extract_literal_as_utf8_bytes(node: &exprs::TExprNode) -> Result<MinMaxPredicateValue, String> {
    let value = match node.node_type {
        t if t == exprs::TExprNodeType::STRING_LITERAL => node
            .string_literal
            .as_ref()
            .ok_or_else(|| "STRING_LITERAL missing value".to_string())?
            .value
            .clone(),
        t if t == exprs::TExprNodeType::INT_LITERAL => node
            .int_literal
            .as_ref()
            .ok_or_else(|| "INT_LITERAL missing value".to_string())?
            .value
            .to_string(),
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => node
            .large_int_literal
            .as_ref()
            .ok_or_else(|| "LARGE_INT_LITERAL missing value".to_string())?
            .value
            .trim()
            .to_string(),
        t if t == exprs::TExprNodeType::DATE_LITERAL => node
            .date_literal
            .as_ref()
            .ok_or_else(|| "DATE_LITERAL missing value".to_string())?
            .value
            .clone(),
        t if t == exprs::TExprNodeType::NULL_LITERAL => {
            return Err("min/max predicate does not support NULL literal".to_string());
        }
        other => {
            return Err(format!(
                "unsupported literal type for VARCHAR min/max predicate: {:?}",
                other
            ));
        }
    };
    Ok(MinMaxPredicateValue::ByteArray(value.into_bytes()))
}

fn is_utf8_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

fn primitive_type_from_node(node: &exprs::TExprNode) -> Option<types::TPrimitiveType> {
    primitive_type_from_desc(&node.type_)
}

fn primitive_type_from_desc(desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
    let nodes = desc.types.as_ref()?;
    let first = nodes.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    Some(scalar.type_)
}

fn arrow_type_from_primitive(primitive: types::TPrimitiveType) -> Option<DataType> {
    crate::types::arrow_thrift::thrift_desc_to_arrow_type(
        &crate::types::arrow_thrift::thrift_type_desc_from_primitive(primitive),
    )
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
            if time_unit_from_node(node) == Some(THRIFT_TIME_UNIT_NANOS) {
                Ok(MinMaxPredicateValue::DateTimeNanos(value))
            } else {
                Ok(MinMaxPredicateValue::DateTimeMicros(value))
            }
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
            if time_unit_from_node(node) == Some(THRIFT_TIME_UNIT_NANOS) {
                Ok(MinMaxPredicateValue::DateTimeNanos(v))
            } else {
                Ok(MinMaxPredicateValue::DateTimeMicros(v))
            }
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
        Some(t) if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME => {
            if time_unit_from_node(node) == Some(THRIFT_TIME_UNIT_NANOS) {
                Ok(MinMaxPredicateValue::DateTimeNanos(
                    parse_datetime_literal_nanos(value)?,
                ))
            } else {
                Ok(MinMaxPredicateValue::DateTimeMicros(
                    parse_datetime_literal_micros(value)?,
                ))
            }
        }
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
            } else if time_unit_from_node(node) == Some(THRIFT_TIME_UNIT_NANOS) {
                Ok(MinMaxPredicateValue::DateTimeNanos(
                    parse_datetime_literal_nanos(value)?,
                ))
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

fn build_decimal_literal_value(
    node: &exprs::TExprNode,
    value: &str,
) -> Result<MinMaxPredicateValue, String> {
    let data_type = arrow_type_from_desc(&node.type_)
        .ok_or_else(|| "DECIMAL_LITERAL missing/unsupported type descriptor".to_string())?;
    match data_type {
        DataType::Decimal128(precision, scale) => {
            let is_decimalv2 = primitive_type_from_node(node)
                .map(|t| t == types::TPrimitiveType::DECIMALV2)
                .unwrap_or(false);
            let integer_bytes = if is_decimalv2 {
                None
            } else {
                node.decimal_literal
                    .as_ref()
                    .and_then(|literal| literal.integer_value.as_deref())
                    .filter(|bytes| !bytes.is_empty())
            };
            if let Some(bytes) = integer_bytes {
                let sign_byte = if bytes.last().map(|b| b & 0x80 != 0).unwrap_or(false) {
                    0xFF_u8
                } else {
                    0x00
                };
                let mut le_bytes = [sign_byte; 16];
                let len = bytes.len().min(16);
                le_bytes[..len].copy_from_slice(&bytes[..len]);
                return Ok(MinMaxPredicateValue::Decimal128 {
                    value: i128::from_le_bytes(le_bytes),
                    precision,
                    scale,
                });
            }

            let (parsed, precision, scale) = match parse_decimal_literal(value, precision, scale) {
                Ok(parsed) => (parsed, precision, scale),
                Err(err) if err.contains("exceeds scale") || err.contains("exceeds precision") => {
                    let (parsed, inferred_precision, inferred_scale) =
                        parse_decimal_literal_inferred(value)?;
                    (parsed, inferred_precision, inferred_scale)
                }
                Err(err) => return Err(err),
            };
            Ok(MinMaxPredicateValue::Decimal128 {
                value: parsed,
                precision,
                scale,
            })
        }
        DataType::Decimal256(_, _) => {
            Err("min/max predicate does not support DECIMAL256 literal".to_string())
        }
        other => Err(format!(
            "DECIMAL_LITERAL lowered to unexpected value for min/max predicate: {:?}",
            other
        )),
    }
}

fn parse_date_literal(value: &str) -> Result<i32, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE_LITERAL '{}'", value))
}

fn parse_decimal_literal(value: &str, _precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {}", scale));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", value));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", value));
    }
    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", value));
    }
    let scale = scale as usize;
    if frac_part.len() > scale {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds scale {}",
            value, scale
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale - frac_part.len()) {
        digits.push('0');
    }
    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    Ok(unsigned.saturating_mul(sign))
}

fn parse_decimal_literal_inferred(value: &str) -> Result<(i128, u8, i8), String> {
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }

    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", value));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", value));
    }
    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", value));
    }

    let scale_usize = frac_part.len();
    if scale_usize > 38 {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds scale {}",
            value, scale_usize
        ));
    }
    let scale = i8::try_from(scale_usize)
        .map_err(|_| format!("DECIMAL_LITERAL '{}' exceeds scale {}", value, scale_usize))?;

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > 38 {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds DECIMAL128 precision",
            value
        ));
    }
    let parsed = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL_LITERAL '{}'", value))?
        .saturating_mul(sign);
    let precision = u8::try_from(digits_final.len()).unwrap_or(38).max(1);
    Ok((parsed, precision, scale))
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

/// Read the DATETIME time-unit code from an expression node's scalar type, if
/// present. `None` means microsecond (default).
fn time_unit_from_node(node: &exprs::TExprNode) -> Option<i32> {
    node.type_
        .types
        .as_ref()?
        .first()?
        .scalar_type
        .as_ref()?
        .time_unit
}

fn parse_datetime_literal_nanos(value: &str) -> Result<i64, String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("empty DATETIME literal".to_string());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| format!("DATETIME literal '{value}' out of nanosecond range"));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| format!("DATETIME literal '{value}' out of nanosecond range"));
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid DATETIME literal '{}'", value))?;
        return dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| format!("DATETIME literal '{value}' out of nanosecond range"));
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
    use crate::thrift::opcodes;

    fn scalar_type_desc(
        primitive: types::TPrimitiveType,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> types::TTypeDesc {
        types::TTypeDesc::new(vec![types::TTypeNode::new(
            types::TTypeNodeType::SCALAR,
            types::TScalarType::new(primitive, None, precision, scale, None),
            None,
            None,
        )])
    }

    fn dummy_type_desc() -> types::TTypeDesc {
        types::TTypeDesc::new(vec![types::TTypeNode::new(
            types::TTypeNodeType::SCALAR,
            types::TScalarType::new(types::TPrimitiveType::INT, None, None, None, None),
            None,
            None,
        )])
    }

    fn default_t_expr_node() -> exprs::TExprNode {
        exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: dummy_type_desc(),
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

    fn decimal_predicate_expr(
        left_type: types::TTypeDesc,
        compare_child_type: types::TPrimitiveType,
    ) -> exprs::TExpr {
        exprs::TExpr {
            nodes: vec![
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    opcode: Some(opcodes::TExprOpcode::EQ),
                    num_children: 2,
                    child_type: Some(compare_child_type),
                    child_type_desc: None,
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::SLOT_REF,
                    type_: left_type,
                    slot_ref: Some(exprs::TSlotRef {
                        slot_id: 9,
                        tuple_id: 1,
                    }),
                    ..default_t_expr_node()
                },
                exprs::TExprNode {
                    node_type: exprs::TExprNodeType::DECIMAL_LITERAL,
                    type_: scalar_type_desc(types::TPrimitiveType::DECIMALV2, None, None),
                    decimal_literal: Some(exprs::TDecimalLiteral::new(
                        "12.345678901".to_string(),
                        None::<Vec<u8>>,
                    )),
                    ..default_t_expr_node()
                },
            ],
        }
    }

    #[test]
    fn decimalv2_child_type_without_desc_matches_legacy_decimalv2_column() {
        let expr = decimal_predicate_expr(
            scalar_type_desc(types::TPrimitiveType::DECIMALV2, None, None),
            types::TPrimitiveType::DECIMALV2,
        );

        let parsed =
            parse_min_max_conjuncts_with_column_resolver(&expr, |_| Ok("price".to_string()))
                .expect("parse decimalv2 predicate");

        assert_eq!(
            parsed,
            vec![MinMaxPredicate::Eq {
                column: "price".to_string(),
                value: MinMaxPredicateValue::Decimal128 {
                    value: 12_345_678_901,
                    precision: 27,
                    scale: 9,
                },
            }]
        );
    }

    #[test]
    fn decimalv2_child_type_without_desc_still_rejects_mismatched_decimal_column() {
        let expr = decimal_predicate_expr(
            scalar_type_desc(types::TPrimitiveType::DECIMAL128, Some(10), Some(2)),
            types::TPrimitiveType::DECIMALV2,
        );

        let parsed =
            parse_min_max_conjuncts_with_column_resolver(&expr, |_| Ok("price".to_string()))
                .expect("parse decimalv2 predicate");

        assert!(
            parsed.is_empty(),
            "mismatched decimal predicate: {parsed:?}"
        );
    }
}
