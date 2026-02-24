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
fn eval_with_factor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    factor: i64,
) -> Result<ArrayRef, String> {
    let date_arr = arena.eval(args[0], chunk)?;
    let days_arr = arena.eval(args[1], chunk)?;
    let dts = extract_datetime_array(&date_arr)?;
    let days = extract_i64_array(&days_arr, "date_add")?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let len = dts.len().max(days.len());
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let dt = if dts.len() == 1 { dts[0] } else { dts[i] };
        let day = if days.len() == 1 { days[0] } else { days[i] };
        let Some(day) = day else {
            out.push(None);
            continue;
        };
        let delta = day * factor;
        let v = dt.map(|d| d + Duration::days(delta));
        let v = v.and_then(|d| to_timestamp_value(d, &output_type).ok());
        out.push(v);
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

pub fn eval_date_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_adddate(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_days_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 1)
}

pub fn eval_weeks_add(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, 7)
}

pub fn eval_date_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -1)
}

pub fn eval_subdate(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -1)
}

pub fn eval_days_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -1)
}

pub fn eval_weeks_sub(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_with_factor(arena, expr, args, chunk, -7)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_date_add_logic() {
        assert_date_function_logic("date_add");
    }

    #[test]
    fn test_adddate_logic() {
        assert_date_function_logic("adddate");
    }

    #[test]
    fn test_days_add_logic() {
        assert_date_function_logic("days_add");
    }

    #[test]
    fn test_weeks_add_logic() {
        assert_date_function_logic("weeks_add");
    }

    #[test]
    fn test_date_sub_logic() {
        assert_date_function_logic("date_sub");
    }

    #[test]
    fn test_subdate_logic() {
        assert_date_function_logic("subdate");
    }

    #[test]
    fn test_days_sub_logic() {
        assert_date_function_logic("days_sub");
    }

    #[test]
    fn test_weeks_sub_logic() {
        assert_date_function_logic("weeks_sub");
    }
}
