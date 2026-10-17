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
use super::common::datetime_from_local_now;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use novarocks_functions::builtin::calendar_extended_shared::{
    CalendarOperation, calendar_unix_seconds, evaluate_legacy_calendar,
};
use std::sync::Arc;

pub fn eval_unix_timestamp<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if args.is_empty() {
        // Preserve the existing clock source; no pure owner admits this form yet.
        let now = calendar_unix_seconds(datetime_from_local_now());
        return Ok(Arc::new(Int64Array::from(vec![Some(now); chunk.len()])) as ArrayRef);
    }
    let source = arena.eval(args[0], chunk)?;
    let rows = source.len();
    Ok((evaluate_legacy_calendar(
        CalendarOperation::UnixTimestamp,
        &[source],
        &DataType::Int64,
        rows,
    ))?)
}
