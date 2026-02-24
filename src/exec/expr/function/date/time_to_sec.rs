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
use super::common::{extract_datetime_array, time_to_seconds};
use crate::exec::chunk::Chunk;
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

const SEC_TO_TIME_CAP_SECONDS: i64 = 839 * 3600 + 59 * 60 + 59;

fn parse_hms_duration_to_seconds(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if raw.starts_with('-') {
        return None;
    }
    let s = raw.strip_prefix('+').unwrap_or(raw);
    let mut parts = s.split(':');
    let hour = parts.next()?.parse::<i64>().ok()?;
    let minute = parts.next()?.parse::<i64>().ok()?;
    let second = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some()
        || minute >= 60
        || second >= 60
        || hour < 0
        || minute < 0
        || second < 0
    {
        return None;
    }
    Some(hour * 3600 + minute * 60 + second)
}

fn parse_from_strings(string_arr: &StringArray) -> Vec<Option<i64>> {
    let mut out = Vec::with_capacity(string_arr.len());
    for i in 0..string_arr.len() {
        let value = if string_arr.is_null(i) {
            None
        } else {
            let s = string_arr.value(i);
            parse_hms_duration_to_seconds(s)
        };
        out.push(value);
    }
    out
}

fn parse_direct_string_array(array: &ArrayRef) -> Option<Vec<Option<i64>>> {
    let str_arr = array.as_any().downcast_ref::<StringArray>()?;
    Some(parse_from_strings(str_arr))
}

fn strip_cast_wrappers(arena: &ExprArena, mut expr_id: ExprId) -> ExprId {
    loop {
        match arena.node(expr_id) {
            Some(
                ExprNode::Cast(child)
                | ExprNode::CastTime(child)
                | ExprNode::CastTimeFromDatetime(child),
            ) => expr_id = *child,
            _ => return expr_id,
        }
    }
}

fn parse_from_sec_to_time_source(
    arena: &ExprArena,
    arg_expr: ExprId,
    chunk: &Chunk,
) -> Result<Option<Vec<Option<i64>>>, String> {
    let inner = strip_cast_wrappers(arena, arg_expr);
    let Some(ExprNode::FunctionCall { kind, args }) = arena.node(inner) else {
        return Ok(None);
    };
    let FunctionKind::Date(name) = kind else {
        return Ok(None);
    };
    if *name != "sec_to_time" || args.len() != 1 {
        return Ok(None);
    }

    let source = arena.eval(args[0], chunk)?;
    let source = source
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "sec_to_time source for time_to_sec must be int".to_string())?;

    let mut out = Vec::with_capacity(source.len());
    for i in 0..source.len() {
        if source.is_null(i) {
            out.push(None);
        } else {
            out.push(Some(
                source
                    .value(i)
                    .clamp(-SEC_TO_TIME_CAP_SECONDS, SEC_TO_TIME_CAP_SECONDS),
            ));
        }
    }
    Ok(Some(out))
}

fn parse_from_immediate_cast_string_source(
    arena: &ExprArena,
    arg_expr: ExprId,
    chunk: &Chunk,
) -> Result<Option<Vec<Option<i64>>>, String> {
    let child = match arena.node(arg_expr) {
        Some(
            ExprNode::Cast(child)
            | ExprNode::CastTime(child)
            | ExprNode::CastTimeFromDatetime(child),
        ) => *child,
        _ => return Ok(None),
    };
    let source = arena.eval(child, chunk)?;
    Ok(parse_direct_string_array(&source))
}

fn parse_from_cast_source(
    arena: &ExprArena,
    arg_expr: ExprId,
    chunk: &Chunk,
) -> Result<Option<Vec<Option<i64>>>, String> {
    let mut current = arg_expr;
    let mut seen_cast = false;
    loop {
        match arena.node(current) {
            Some(
                ExprNode::Cast(child)
                | ExprNode::CastTime(child)
                | ExprNode::CastTimeFromDatetime(child),
            ) => {
                current = *child;
                seen_cast = true;
            }
            _ => break,
        }
    }
    if !seen_cast {
        return Ok(None);
    }
    let source = arena.eval(current, chunk)?;
    if let Some(out) = parse_direct_string_array(&source) {
        return Ok(Some(out));
    }
    let source_utf8 = cast(&source, &DataType::Utf8).map_err(|e| e.to_string())?;
    Ok(parse_direct_string_array(&source_utf8))
}

pub fn eval_time_to_sec(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arg_expr = args[0];

    if let Some(out) = parse_from_sec_to_time_source(arena, arg_expr, chunk)? {
        return Ok(Arc::new(Int64Array::from(out)) as ArrayRef);
    }

    let arr = arena.eval(arg_expr, chunk)?;

    if let Some(out) = parse_direct_string_array(&arr) {
        return Ok(Arc::new(Int64Array::from(out)) as ArrayRef);
    }

    let mut out = Vec::with_capacity(arr.len());
    let dts = extract_datetime_array(&arr)?;
    for dt in dts {
        out.push(dt.map(|d| time_to_seconds(d.time())));
    }

    // For implicit CAST(string AS TIME), FE lowers to a CAST node and expects strict
    // TIME-string parsing semantics (no datetime-prefix acceptance).
    if let Some(source_out) = parse_from_immediate_cast_string_source(arena, arg_expr, chunk)? {
        for (idx, value) in out.iter_mut().enumerate() {
            if idx < source_out.len() {
                *value = source_out[idx];
            }
        }
        return Ok(Arc::new(Int64Array::from(out)) as ArrayRef);
    }

    if out.iter().any(Option::is_none) {
        if let Some(source_out) = parse_from_cast_source(arena, arg_expr, chunk)? {
            for (idx, value) in out.iter_mut().enumerate() {
                if value.is_none() && idx < source_out.len() {
                    *value = source_out[idx];
                }
            }
        }
    }

    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::parse_hms_duration_to_seconds;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;
    use crate::exec::expr::function::date::test_utils::{
        chunk_len_1, literal_i64, literal_string, typed_null,
    };
    use crate::exec::expr::{ExprArena, ExprId};
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_time_to_sec_logic() {
        assert_date_function_logic("time_to_sec");
    }

    #[test]
    fn test_time_to_sec_duration_parser() {
        assert_eq!(parse_hms_duration_to_seconds("00:00:00"), Some(0));
        assert_eq!(parse_hms_duration_to_seconds("23:59:59"), Some(86399));
        assert_eq!(parse_hms_duration_to_seconds("25:00:00"), Some(90000));
        assert_eq!(parse_hms_duration_to_seconds("-00:00:01"), None);
        assert_eq!(parse_hms_duration_to_seconds("0000-00-00 23:59:59"), None);
        assert_eq!(parse_hms_duration_to_seconds("1970-01-01 01:01:01"), None);
    }

    #[test]
    fn test_time_to_sec_recovers_from_failed_cast_time_literal() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64: ExprId = typed_null(&mut arena, DataType::Int64);
        let literal = literal_string(&mut arena, "00:00:00");
        let cast_time = arena.push_typed(
            ExprNode::Cast(literal),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let parsed_from_source =
            super::parse_from_cast_source(&arena, cast_time, &chunk).expect("parse cast source");
        assert_eq!(parsed_from_source, Some(vec![Some(0)]));
        let out = super::eval_time_to_sec(&arena, expr_i64, &[cast_time], &chunk)
            .expect("time_to_sec eval");
        let arr = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 0);
    }

    #[test]
    fn test_time_to_sec_cast_string_with_datetime_prefix_returns_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64: ExprId = typed_null(&mut arena, DataType::Int64);
        let literal = literal_string(&mut arena, "1970-01-01 01:01:01");
        let cast_time = arena.push_typed(
            ExprNode::Cast(literal),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let out = super::eval_time_to_sec(&arena, expr_i64, &[cast_time], &chunk)
            .expect("time_to_sec eval");
        let arr = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_time_to_sec_explicit_datetime_cast_preserves_time_part() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64: ExprId = typed_null(&mut arena, DataType::Int64);
        let literal = literal_string(&mut arena, "1970-01-01 01:01:01");
        let cast_datetime = arena.push_typed(
            ExprNode::Cast(literal),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let cast_time = arena.push_typed(
            ExprNode::Cast(cast_datetime),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let out = super::eval_time_to_sec(&arena, expr_i64, &[cast_time], &chunk)
            .expect("time_to_sec eval");
        let arr = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 3661);
    }

    #[test]
    fn test_time_to_sec_sec_to_time_negative_roundtrip() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64: ExprId = typed_null(&mut arena, DataType::Int64);
        let seconds = literal_i64(&mut arena, -1);
        let sec_to_time = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Date("sec_to_time"),
                args: vec![seconds],
            },
            DataType::Utf8,
        );
        let out = super::eval_time_to_sec(&arena, expr_i64, &[sec_to_time], &chunk)
            .expect("time_to_sec eval");
        let arr = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), -1);
    }

    #[test]
    fn test_time_to_sec_sec_to_time_negative_clamps_like_sec_to_time() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr_i64: ExprId = typed_null(&mut arena, DataType::Int64);
        let seconds = literal_i64(&mut arena, -2_147_483_648);
        let sec_to_time = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Date("sec_to_time"),
                args: vec![seconds],
            },
            DataType::Utf8,
        );
        let out = super::eval_time_to_sec(&arena, expr_i64, &[sec_to_time], &chunk)
            .expect("time_to_sec eval");
        let arr = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), -3_023_999);
    }
}
