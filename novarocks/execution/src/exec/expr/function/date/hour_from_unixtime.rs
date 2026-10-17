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
use arrow::array::ArrayRef;
use chrono::{Local, NaiveDateTime, TimeZone};
fn project_local(seconds: i64) -> Option<NaiveDateTime> {
    Local
        .timestamp_opt(seconds, 0)
        .single()
        .map(|dt| dt.naive_local())
}
pub fn eval_hour_from_unixtime<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let arr = arena.eval(args[0], chunk)?;
    let values = extract_i64_array(&arr, "hour_from_unixtime")?;
    Ok(
        novarocks_functions::builtin::calendar_unixtime::evaluate_legacy_hour_from_unixtime(
            &values,
            project_local,
        ),
    )
}
