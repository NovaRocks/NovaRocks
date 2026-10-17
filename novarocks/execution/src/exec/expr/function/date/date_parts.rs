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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

use novarocks_functions::{
    Selection,
    builtin::calendar_parts_shared::{
        CalendarPartOp as DatePart, legacy_calendar_names, legacy_calendar_parts,
    },
};

#[inline]
fn eval_part_int<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    part: DatePart,
) -> Result<ArrayRef, __Eval::Error> {
    let arr = arena.eval(args[0], chunk)?;
    let out = legacy_calendar_parts(part, &arr, Selection::all(arr.len()))?;
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    let expected_type = arena.data_type(expr).cloned().unwrap_or(DataType::Int64);
    if out.data_type() == &expected_type {
        return Ok(out);
    }
    Ok((cast(&out, &expected_type).map_err(|e| {
        format!(
            "date part output cast failed from {:?} to {:?}: {}",
            out.data_type(),
            expected_type,
            e
        )
    }))?)
}

#[inline]
fn eval_part_string<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    part: DatePart,
) -> Result<ArrayRef, __Eval::Error> {
    let arr = arena.eval(args[0], chunk)?;
    let out = legacy_calendar_names(part, &arr, Selection::all(arr.len()))?;
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_day<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Day))?)
}

pub fn eval_dayofmonth<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Day))?)
}

pub fn eval_dayofweek<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::DayOfWeek))?)
}

pub fn eval_dayofyear<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::DayOfYear))?)
}

pub fn eval_dayofweek_iso<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::DayOfWeekIso))?)
}

pub fn eval_weekday<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::WeekDay))?)
}

pub fn eval_dayname<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_string(arena, args, chunk, DatePart::DayName))?)
}

pub fn eval_hour<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Hour))?)
}

pub fn eval_minute<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Minute))?)
}

pub fn eval_month<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Month))?)
}

pub fn eval_monthname<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_string(arena, args, chunk, DatePart::MonthName))?)
}

pub fn eval_quarter<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Quarter))?)
}

pub fn eval_second<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Second))?)
}

pub fn eval_week<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::WeekOfYear))?)
}

pub fn eval_weekofyear<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::WeekOfYear))?)
}

pub fn eval_year<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::Year))?)
}

pub fn eval_yearweek<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_part_int(arena, expr, args, chunk, DatePart::YearWeek))?)
}
