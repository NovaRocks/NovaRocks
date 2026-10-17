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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarOperation, evaluate_legacy_calendar,
};
pub use novarocks_functions::builtin::calendar_extended_shared::{
    FROM_DAYS_MAX_VALID, calendar_from_days_value as from_days_value,
    calendar_zero_date_sentinel_date32 as zero_date_sentinel_date32,
};

pub fn eval_from_days<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let value = arena.eval(args[0], chunk)?;
    let rows = value.len();
    Ok((evaluate_legacy_calendar(
        CalendarOperation::FromDays,
        &[value],
        &DataType::Date32,
        rows,
    ))?)
}
