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
use super::common::{extract_datetime_array, extract_i64_array, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::Duration;
use std::sync::Arc;

#[inline]
fn eval_add_duration<F>(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    dur_fn: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64) -> Duration,
{
    let date_arr = arena.eval(args[0], chunk)?;
    let delta_arr = arena.eval(args[1], chunk)?;
    let dts = extract_datetime_array(&date_arr)?;
    let deltas = extract_i64_array(&delta_arr, "duration add")?;
    if dts.len() != deltas.len() && dts.len() != 1 && deltas.len() != 1 {
        return Err("duration add argument length mismatch".to_string());
    }
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let len = dts.len().max(deltas.len());
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let dt = if dts.len() == 1 { dts[0] } else { dts[i] };
        let delta_v = if deltas.len() == 1 {
            deltas[0]
        } else {
            deltas[i]
        };
        let Some(delta_v) = delta_v else {
            out.push(None);
            continue;
        };
        let delta = dur_fn(delta_v);
        let v = dt.map(|d| d + delta);
        let v = v.and_then(|d| to_timestamp_value(d, &output_type).ok());
        out.push(v);
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

pub fn eval_seconds_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, chrono::Duration::seconds)
}

pub fn eval_seconds_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, |v| chrono::Duration::seconds(-v))
}

pub fn eval_minutes_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, chrono::Duration::minutes)
}

pub fn eval_minutes_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, |v| chrono::Duration::minutes(-v))
}

pub fn eval_hours_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, chrono::Duration::hours)
}

pub fn eval_hours_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, |v| chrono::Duration::hours(-v))
}

pub fn eval_milliseconds_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, chrono::Duration::milliseconds)
}

pub fn eval_milliseconds_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, |v| {
        chrono::Duration::milliseconds(-v)
    })
}

pub fn eval_microseconds_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, chrono::Duration::microseconds)
}

pub fn eval_microseconds_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_add_duration(arena, expr, args, chunk, |v| {
        chrono::Duration::microseconds(-v)
    })
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_seconds_add_logic() {
        assert_date_function_logic("seconds_add");
    }

    #[test]
    fn test_seconds_sub_logic() {
        assert_date_function_logic("seconds_sub");
    }

    #[test]
    fn test_minutes_add_logic() {
        assert_date_function_logic("minutes_add");
    }

    #[test]
    fn test_minutes_sub_logic() {
        assert_date_function_logic("minutes_sub");
    }

    #[test]
    fn test_hours_add_logic() {
        assert_date_function_logic("hours_add");
    }

    #[test]
    fn test_hours_sub_logic() {
        assert_date_function_logic("hours_sub");
    }

    #[test]
    fn test_milliseconds_add_logic() {
        assert_date_function_logic("milliseconds_add");
    }

    #[test]
    fn test_milliseconds_sub_logic() {
        assert_date_function_logic("milliseconds_sub");
    }

    #[test]
    fn test_microseconds_add_logic() {
        assert_date_function_logic("microseconds_add");
    }

    #[test]
    fn test_microseconds_sub_logic() {
        assert_date_function_logic("microseconds_sub");
    }
}
