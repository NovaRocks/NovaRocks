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
use super::common::{time_from_local_now, time_from_utc_now, to_timestamp_value};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use std::sync::Arc;

#[inline]
fn eval_current_time_inner(
    arena: &ExprArena,
    expr: ExprId,
    chunk: &Chunk,
    utc: bool,
) -> Result<ArrayRef, String> {
    let t = if utc {
        time_from_utc_now()
    } else {
        time_from_local_now()
    };
    let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let dt = date.and_time(t);
    let ts = to_timestamp_value(
        dt,
        arena
            .data_type(expr)
            .unwrap_or(&DataType::Timestamp(TimeUnit::Microsecond, None)),
    )?;
    let values = vec![Some(ts); chunk.len()];
    Ok(Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef)
}

pub fn eval_current_time(
    arena: &ExprArena,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_current_time_inner(arena, expr, chunk, false)
}

pub fn eval_curtime(
    arena: &ExprArena,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_current_time_inner(arena, expr, chunk, false)
}

pub fn eval_utc_time(
    arena: &ExprArena,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_current_time_inner(arena, expr, chunk, true)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_current_time_logic() {
        assert_date_function_logic("current_time");
    }

    #[test]
    fn test_curtime_logic() {
        assert_date_function_logic("curtime");
    }

    #[test]
    fn test_utc_time_logic() {
        assert_date_function_logic("utc_time");
    }
}
