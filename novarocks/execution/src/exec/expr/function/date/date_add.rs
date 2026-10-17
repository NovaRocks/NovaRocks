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
    CalendarOperation, evaluate_legacy_calendar, legacy_validate_datetime_source,
};
use std::sync::Arc;

fn eval_with_factor<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    factor: i64,
) -> Result<ArrayRef, __Eval::Error> {
    let date = arena.eval(args[0], chunk)?;
    let interval = arena.eval(args[1], chunk)?;
    legacy_validate_datetime_source(&date)?;
    let intervals = extract_i64_array(&interval, "date_add")?;
    let rows = date.len().max(intervals.len());
    let interval: ArrayRef = Arc::new(Int64Array::from(intervals));
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    Ok((evaluate_legacy_calendar(
        CalendarOperation::DaysShift(factor),
        &[date, interval],
        &output_type,
        rows,
    ))?)
}

pub fn eval_date_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, 1))?)
}

pub fn eval_adddate<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, 1))?)
}

pub fn eval_days_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, 1))?)
}

pub fn eval_weeks_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, 7))?)
}

pub fn eval_date_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, -1))?)
}

pub fn eval_subdate<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, -1))?)
}

pub fn eval_days_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, -1))?)
}

pub fn eval_weeks_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_with_factor(arena, expr, args, chunk, -7))?)
}
