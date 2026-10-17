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
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarDurationShift, CalendarOperation, evaluate_legacy_calendar,
    legacy_validate_datetime_source,
};
use std::sync::Arc;

fn eval_add_duration<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    operation: CalendarDurationShift,
) -> Result<ArrayRef, __Eval::Error> {
    let date = arena.eval(args[0], chunk)?;
    let interval = arena.eval(args[1], chunk)?;
    legacy_validate_datetime_source(&date)?;
    let intervals = extract_i64_array(&interval, "duration add")?;
    // Preserve the original malformed-caller length error before all-row selection.
    if date.len() != intervals.len() && date.len() != 1 && intervals.len() != 1 {
        return Err(("duration add argument length mismatch".to_string()).into());
    }
    let rows = date.len().max(intervals.len());
    let interval: ArrayRef = Arc::new(Int64Array::from(intervals));
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    Ok((evaluate_legacy_calendar(
        CalendarOperation::DurationShift(operation),
        &[date, interval],
        &output_type,
        rows,
    ))?)
}

pub fn eval_seconds_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(arena, expr, args, chunk, CalendarDurationShift::SecondsAdd))?)
}

pub fn eval_seconds_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(arena, expr, args, chunk, CalendarDurationShift::SecondsSub))?)
}

pub fn eval_minutes_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(arena, expr, args, chunk, CalendarDurationShift::MinutesAdd))?)
}

pub fn eval_minutes_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(arena, expr, args, chunk, CalendarDurationShift::MinutesSub))?)
}

pub fn eval_hours_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(arena, expr, args, chunk, CalendarDurationShift::HoursAdd))?)
}

pub fn eval_hours_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(arena, expr, args, chunk, CalendarDurationShift::HoursSub))?)
}

pub fn eval_milliseconds_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(
        arena,
        expr,
        args,
        chunk,
        CalendarDurationShift::MillisecondsAdd,
    ))?)
}

pub fn eval_milliseconds_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(
        arena,
        expr,
        args,
        chunk,
        CalendarDurationShift::MillisecondsSub,
    ))?)
}

pub fn eval_microseconds_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(
        arena,
        expr,
        args,
        chunk,
        CalendarDurationShift::MicrosecondsAdd,
    ))?)
}

pub fn eval_microseconds_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_add_duration(
        arena,
        expr,
        args,
        chunk,
        CalendarDurationShift::MicrosecondsSub,
    ))?)
}
