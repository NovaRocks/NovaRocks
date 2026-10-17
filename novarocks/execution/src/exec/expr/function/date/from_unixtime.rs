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
use super::common::extract_i64_array;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use novarocks_functions::builtin::calendar_unixtime::{
    TimeZoneSpec, evaluate_legacy_from_unixtime, parse_tz,
};
fn project_local(utc: DateTime<Utc>) -> NaiveDateTime {
    utc.with_timezone(&Local).naive_local()
}
fn eval_from_unixtime_inner<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    units_per_second: i64,
) -> Result<ArrayRef, __Eval::Error> {
    let values = extract_i64_array(&arena.eval(args[0], chunk)?, "from_unixtime")?;
    let has_format_arg = args.len() > 1;
    let output_type = match arena.data_type(expr).cloned() {
        Some(DataType::Null) | None if has_format_arg => DataType::Utf8,
        Some(DataType::Null) | None => DataType::Timestamp(TimeUnit::Microsecond, None),
        Some(data_type) => data_type,
    };
    let format_array = if has_format_arg {
        Some(arena.eval(args[1], chunk)?)
    } else {
        None
    };
    let format = format_array
        .as_ref()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "from_unixtime expects string format".to_string())
        })
        .transpose()?;
    let timezone_array = if has_format_arg && args.len() > 2 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let timezone = timezone_array
        .as_ref()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "from_unixtime expects string timezone".to_string())
        })
        .transpose()?;
    let default_zone = arena
        .session_time_zone()
        .and_then(parse_tz)
        .unwrap_or(TimeZoneSpec::Local);
    Ok((evaluate_legacy_from_unixtime(
        &values,
        format,
        timezone,
        default_zone,
        units_per_second,
        &output_type,
        project_local,
    ))?)
}
#[inline]
pub fn eval_from_unixtime<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_from_unixtime_inner(arena, expr, args, chunk, 1))?)
}
pub fn eval_from_unixtime_ms<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_from_unixtime_inner(arena, expr, args, chunk, 1000))?)
}
