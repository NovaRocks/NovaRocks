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
use crate::exec::expr::LiteralValue;
use crate::lower::type_lowering::{arrow_type_from_desc, primitive_type_from_node};
use crate::types;
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::exprs;

/// Parse date literal string to days since epoch.
pub(crate) fn parse_date_literal(value: &str) -> Result<i32, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE_LITERAL '{}'", value))
}

/// Parse decimal literal string to i128 value.
pub(crate) fn parse_decimal_literal(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
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
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            value, precision
        ));
    }
    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    Ok(unsigned.saturating_mul(sign))
}

pub(crate) fn parse_decimal_literal_i256(
    value: &str,
    precision: u8,
    scale: i8,
) -> Result<i256, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {}", scale));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut negative = false;
    if let Some(rest) = s.strip_prefix('-') {
        negative = true;
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
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            value, precision
        ));
    }

    let ten = i256::from_i128(10);
    let mut out = i256::ZERO;
    for ch in digits_final.bytes() {
        let digit = (ch - b'0') as i128;
        out = out
            .checked_mul(ten)
            .and_then(|v| v.checked_add(i256::from_i128(digit)))
            .ok_or_else(|| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    }
    if negative {
        out = out
            .checked_neg()
            .ok_or_else(|| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    }
    Ok(out)
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
    let precision_usize = digits_final.len().max(1);
    if precision_usize > 38 {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            value, precision_usize
        ));
    }
    let precision = u8::try_from(precision_usize).map_err(|_| {
        format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            value, precision_usize
        )
    })?;
    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    Ok((unsigned.saturating_mul(sign), precision, scale))
}

fn parse_decimal_literal_inferred_i256(value: &str) -> Result<(i256, u8, i8), String> {
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut negative = false;
    if let Some(rest) = s.strip_prefix('-') {
        negative = true;
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
    if scale_usize > 76 {
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
    let precision_usize = digits_final.len().max(1);
    if precision_usize > 76 {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            value, precision_usize
        ));
    }
    let precision = u8::try_from(precision_usize).map_err(|_| {
        format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            value, precision_usize
        )
    })?;

    let ten = i256::from_i128(10);
    let mut out = i256::ZERO;
    for ch in digits_final.bytes() {
        let digit = (ch - b'0') as i128;
        out = out
            .checked_mul(ten)
            .and_then(|v| v.checked_add(i256::from_i128(digit)))
            .ok_or_else(|| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    }
    if negative {
        out = out
            .checked_neg()
            .ok_or_else(|| format!("failed to parse DECIMAL_LITERAL '{}'", value))?;
    }
    Ok((out, precision, scale))
}

/// Build decimal literal from node and value string.
pub(crate) fn build_decimal_literal(
    node: &exprs::TExprNode,
    value: &str,
) -> Result<LiteralValue, String> {
    let data_type = arrow_type_from_desc(&node.type_)
        .ok_or_else(|| "DECIMAL_LITERAL missing/unsupported type descriptor".to_string())?;
    match data_type {
        DataType::Decimal128(precision, scale) => {
            let (parsed, precision, scale) = match parse_decimal_literal(value, precision, scale) {
                Ok(parsed) => (parsed, precision, scale),
                Err(err) if err.contains("exceeds scale") || err.contains("exceeds precision") => {
                    // FE sometimes assigns a narrow intermediate decimal type for literals and
                    // relies on later casts. Keep lowering permissive and preserve the literal
                    // value with an inferred decimal metadata.
                    let (parsed, inferred_precision, inferred_scale) =
                        parse_decimal_literal_inferred(value)?;
                    (parsed, inferred_precision, inferred_scale)
                }
                Err(err) => return Err(err),
            };
            Ok(LiteralValue::Decimal128 {
                value: parsed,
                precision,
                scale,
            })
        }
        DataType::Decimal256(precision, scale) => {
            let (parsed, precision, scale) =
                match parse_decimal_literal_i256(value, precision, scale) {
                    Ok(parsed) => (parsed, precision, scale),
                    Err(err)
                        if err.contains("exceeds scale") || err.contains("exceeds precision") =>
                    {
                        let (parsed, inferred_precision, inferred_scale) =
                            parse_decimal_literal_inferred_i256(value)?;
                        (parsed, inferred_precision, inferred_scale)
                    }
                    Err(err) => return Err(err),
                };
            Ok(LiteralValue::Decimal256 {
                value: parsed,
                precision,
                scale,
            })
        }
        other => Err(format!("DECIMAL_LITERAL type mismatch: {:?}", other)),
    }
}

/// Build int literal from node and value.
pub(crate) fn build_int_literal(
    node: &exprs::TExprNode,
    value: i64,
) -> Result<LiteralValue, String> {
    match primitive_type_from_node(node) {
        Some(t) if t == types::TPrimitiveType::TINYINT => {
            let v = i8::try_from(value)
                .map_err(|_| format!("INT_LITERAL out of range for TINYINT: {value}"))?;
            Ok(LiteralValue::Int8(v))
        }
        Some(t) if t == types::TPrimitiveType::SMALLINT => {
            let v = i16::try_from(value)
                .map_err(|_| format!("INT_LITERAL out of range for SMALLINT: {value}"))?;
            Ok(LiteralValue::Int16(v))
        }
        Some(t) if t == types::TPrimitiveType::INT => {
            let v = i32::try_from(value)
                .map_err(|_| format!("INT_LITERAL out of range for INT: {value}"))?;
            Ok(LiteralValue::Int32(v))
        }
        Some(t) if t == types::TPrimitiveType::BIGINT => Ok(LiteralValue::Int64(value)),
        Some(t) if t == types::TPrimitiveType::LARGEINT => {
            Ok(LiteralValue::LargeInt(value as i128))
        }
        _ => Ok(LiteralValue::Int64(value)),
    }
}

/// Build large int literal from node and value string.
pub(crate) fn build_large_int_literal(
    node: &exprs::TExprNode,
    value: &str,
) -> Result<LiteralValue, String> {
    let parsed = value
        .trim()
        .parse::<i128>()
        .map_err(|_| format!("failed to parse LARGE_INT_LITERAL '{}'", value))?;
    match primitive_type_from_node(node) {
        Some(t) if t == types::TPrimitiveType::LARGEINT => Ok(LiteralValue::LargeInt(parsed)),
        Some(t) if t == types::TPrimitiveType::BIGINT => i64::try_from(parsed)
            .map(LiteralValue::Int64)
            .map_err(|_| format!("LARGE_INT_LITERAL '{}' exceeds BIGINT range", value)),
        Some(other) => Err(format!(
            "LARGE_INT_LITERAL type mismatch: expected LARGEINT/BIGINT, got {:?}",
            other
        )),
        None => Ok(LiteralValue::LargeInt(parsed)),
    }
}

/// Build float literal from node and value.
pub(crate) fn build_float_literal(node: &exprs::TExprNode, value: f64) -> LiteralValue {
    match primitive_type_from_node(node) {
        Some(t) if t == types::TPrimitiveType::FLOAT => LiteralValue::Float32(value as f32),
        _ => LiteralValue::Float64(value),
    }
}

/// Build date literal from node and value string.
pub(crate) fn build_date_literal(
    node: &exprs::TExprNode,
    value: &str,
) -> Result<LiteralValue, String> {
    match primitive_type_from_node(node) {
        Some(t) if t == types::TPrimitiveType::DATE => {
            let days = parse_date_literal(value)?;
            Ok(LiteralValue::Date32(days))
        }
        None => {
            let days = parse_date_literal(value)?;
            Ok(LiteralValue::Date32(days))
        }
        _ => Ok(LiteralValue::Utf8(value.to_string())),
    }
}
