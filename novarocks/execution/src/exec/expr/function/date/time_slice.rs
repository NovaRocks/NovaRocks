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
use super::common::{extract_datetime_array, extract_i64_array};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::builtin::calendar_slice::{CalendarSliceDomain, evaluate_legacy_slice};
#[inline]
fn eval_time_slice_inner<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    date_output: bool,
) -> Result<ArrayRef, __Eval::Error> {
    let name = if date_output {
        "date_slice"
    } else {
        "time_slice"
    };
    if !matches!(args.len(), 3 | 4) {
        return Err((format!("{name} expects value, count, unit and optional boundary")).into());
    }
    let output_type = if date_output {
        DataType::Date32
    } else {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    };
    if arena.data_type(expr) != Some(&output_type) {
        return Err((format!("{name} result type differs from its frozen temporal domain")).into());
    }
    // SQL materializes temporal coercions before binding.
    let dt_arr = arena.eval(args[0], chunk)?;
    let interval_arr = arena.eval(args[1], chunk)?;
    let unit_arr = arena.eval(args[2], chunk)?;
    let boundary_arr = if args.len() == 4 {
        Some(arena.eval(args[3], chunk)?)
    } else {
        None
    };
    if dt_arr.data_type() != &output_type || interval_arr.data_type() != &DataType::Int32 {
        return Err(
            (format!("{name} arguments differ from their frozen temporal/INT32 domains")).into(),
        );
    }
    let dts = extract_datetime_array(&dt_arr)?;
    let interval_values = extract_i64_array(&interval_arr, "time_slice")
        .map_err(|_| "time_slice expects int interval".to_string())?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "time_slice expects unit string".to_string())?;
    let boundary_arr = boundary_arr
        .as_ref()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("{name} expects boundary string"))
        })
        .transpose()?;
    for length in [interval_values.len(), unit_arr.len()]
        .into_iter()
        .chain(boundary_arr.map(|array| array.len()))
    {
        if length != 1 && length != dts.len() {
            return Err((format!("{name} argument lengths differ")).into());
        }
    }
    Ok((evaluate_legacy_slice(
        if date_output {
            CalendarSliceDomain::Date
        } else {
            CalendarSliceDomain::Time
        },
        &dts,
        &interval_values,
        unit_arr,
        boundary_arr,
    ))?)
}

pub fn eval_time_slice<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_time_slice_inner(arena, expr, args, chunk, false))?)
}

pub fn eval_date_slice<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_time_slice_inner(arena, expr, args, chunk, true))?)
}
