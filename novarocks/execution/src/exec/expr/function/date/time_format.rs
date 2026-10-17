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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use novarocks_functions::{
    EvaluationCheckpoints,
    builtin::calendar_time_text_shared::{self as shared, LegacyControl, TimeTextError},
};
pub fn eval_time_format<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let override_values = if let Some(ExprNode::Cast(child, _)) = arena.node(args[0]) {
        if matches!(arena.data_type(*child), Some(DataType::Utf8)) {
            let raw = arena.eval(*child, chunk)?;
            let raw = raw
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "time_format expects string".to_string())?;
            Some(shared::clock_strings(raw, &mut work).map_err(TimeTextError::into_legacy)?)
        } else {
            None
        }
    } else {
        None
    };
    let time = arena.eval(args[0], chunk)?;
    let format = arena.eval(args[1], chunk)?;
    let format = format
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "time_format expects string format".to_string())?;
    let seconds = if let Some(v) = override_values {
        v
    } else {
        shared::format_argument(&time, &mut work).map_err(TimeTextError::into_legacy)?
    };
    Ok(
        (shared::format_output(&seconds, format, chunk.len(), &mut work)
            .map_err(TimeTextError::into_legacy))?,
    )
}
