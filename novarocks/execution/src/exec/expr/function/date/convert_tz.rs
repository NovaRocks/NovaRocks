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
    CalendarOperation, evaluate_legacy_calendar, legacy_validate_datetime_source,
};

pub fn eval_convert_tz<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let date = arena.eval(args[0], chunk)?;
    let from = arena.eval(args[1], chunk)?;
    let to = arena.eval(args[2], chunk)?;
    legacy_validate_datetime_source(&date)?;
    from.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "convert_tz expects string".to_string())?;
    to.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "convert_tz expects string".to_string())?;
    let output_type = arena
        .data_type(expr)
        .cloned()
        .unwrap_or(DataType::Timestamp(TimeUnit::Microsecond, None));
    let rows = date.len();
    Ok((evaluate_legacy_calendar(
        CalendarOperation::ConvertTz,
        &[date, from, to],
        &output_type,
        rows,
    ))?)
}
