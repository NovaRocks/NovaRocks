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
use super::common::{NumericArrayView, cast_output, value_at_f64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::function::date::common::{
    extract_datetime_array, naive_to_timestamp_micros,
};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

fn finite_or_null(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn is_datetime_compatible_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8 | DataType::Null
    )
}

fn datetime_value_at(
    values: &[Option<chrono::NaiveDateTime>],
    row: usize,
    len: usize,
) -> Option<chrono::NaiveDateTime> {
    let idx = if values.len() == 1 && len > 1 { 0 } else { row };
    values.get(idx).copied().flatten()
}

fn eval_greatest_least_datetime(
    arena: &ExprArena,
    expr: ExprId,
    arrays: &[ArrayRef],
    len: usize,
    greatest: bool,
) -> Result<ArrayRef, String> {
    let mut values_per_arg = Vec::with_capacity(arrays.len());
    for array in arrays {
        if matches!(array.data_type(), DataType::Null) {
            values_per_arg.push(vec![None; array.len()]);
        } else {
            values_per_arg.push(extract_datetime_array(array)?);
        }
    }

    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let mut acc: Option<chrono::NaiveDateTime> = None;
        for arg_values in &values_per_arg {
            let v = datetime_value_at(arg_values, row, len);
            match (acc, v) {
                (Some(a), Some(b)) => {
                    acc = Some(if greatest {
                        if a >= b { a } else { b }
                    } else if a <= b {
                        a
                    } else {
                        b
                    });
                }
                (None, Some(b)) => acc = Some(b),
                (_, None) => {
                    acc = None;
                    break;
                }
            }
        }
        values.push(acc.map(naive_to_timestamp_micros));
    }

    let ts = Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    if matches!(output_type, DataType::Utf8) {
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| "failed to downcast datetime extrema result".to_string())?;
        let out = (0..ts.len())
            .map(|i| {
                if ts.is_null(i) {
                    None
                } else {
                    crate::exec::expr::function::date::common::timestamp_to_naive(
                        &TimeUnit::Microsecond,
                        ts.value(i),
                    )
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                }
            })
            .collect::<Vec<_>>();
        return Ok(Arc::new(StringArray::from(out)) as ArrayRef);
    }
    cast_output(ts, Some(&output_type))
}

fn eval_greatest_least(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    greatest: bool,
) -> Result<ArrayRef, String> {
    let mut arrays = Vec::with_capacity(args.len());
    for arg in args {
        arrays.push(arena.eval(*arg, chunk)?);
    }

    if !arrays.is_empty()
        && arrays
            .iter()
            .all(|array| is_datetime_compatible_type(array.data_type()))
    {
        return eval_greatest_least_datetime(arena, expr, &arrays, chunk.len(), greatest);
    }

    let mut views = Vec::with_capacity(args.len());
    for array in &arrays {
        views.push(NumericArrayView::new(array)?);
    }
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let mut acc: Option<f64> = None;
        for view in &views {
            let v = value_at_f64(view, row, len).and_then(finite_or_null);
            match (acc, v) {
                (Some(a), Some(b)) => {
                    acc = Some(if greatest { a.max(b) } else { a.min(b) });
                }
                (None, Some(b)) => acc = Some(b),
                (_, None) => {
                    acc = None;
                    break;
                }
            }
        }
        values.push(acc);
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    cast_output(out, arena.data_type(expr))
}

pub fn eval_greatest(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_greatest_least(arena, expr, args, chunk, true)
}

pub fn eval_least(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_greatest_least(arena, expr, args, chunk, false)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::math::test_utils::assert_math_function_logic;

    #[test]
    fn test_extrema_logic() {
        assert_math_function_logic("greatest");
        assert_math_function_logic("least");
    }
}
