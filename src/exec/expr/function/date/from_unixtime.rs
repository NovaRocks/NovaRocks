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
use super::common::to_timestamp_value;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Utc};
use std::sync::Arc;

#[inline]
fn eval_from_unixtime_inner(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    divisor: i64,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let arr = arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "from_unixtime expects int".to_string())?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let mut out = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            out.push(None);
            continue;
        }
        let val = arr.value(i);
        let secs = val.div_euclid(divisor);
        let sub = val.rem_euclid(divisor) as u32;
        let nanos_per_unit = (1_000_000_000 / divisor) as u32;
        let dt = DateTime::<Utc>::from_timestamp(secs, sub * nanos_per_unit).map(|d| d.naive_utc());
        let v = dt.and_then(|d| to_timestamp_value(d, &output_type).ok());
        out.push(v);
    }
    Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef)
}

pub fn eval_from_unixtime(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_from_unixtime_inner(arena, expr, args, chunk, 1_000_000)
}

pub fn eval_from_unixtime_ms(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_from_unixtime_inner(arena, expr, args, chunk, 1_000)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_from_unixtime_logic() {
        assert_date_function_logic("from_unixtime");
    }

    #[test]
    fn test_from_unixtime_ms_logic() {
        assert_date_function_logic("from_unixtime_ms");
    }
}
