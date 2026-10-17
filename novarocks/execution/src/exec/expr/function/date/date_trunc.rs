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
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarOperation, evaluate_legacy_calendar,
};

#[inline]
fn eval_date_trunc_inner<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let unit = arena.eval(args[0], chunk)?;
    unit.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "date_trunc expects string unit".to_string())?;
    let date = arena.eval(args[1], chunk)?;
    let rows = date.len();
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    Ok((evaluate_legacy_calendar(CalendarOperation::Trunc, &[unit, date], &output_type, rows))?)
}

pub fn eval_date_trunc<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_date_trunc_inner(arena, expr, args, chunk))?)
}

pub fn eval_date_floor<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_date_trunc_inner(arena, expr, args, chunk))?)
}

pub fn eval_alignment_timestamp<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if args.len() != 2 {
        return Err(("alignment_timestamp expects 2 args".to_string()).into());
    }
    Ok((eval_date_trunc_inner(arena, expr, args, chunk))?)
}
