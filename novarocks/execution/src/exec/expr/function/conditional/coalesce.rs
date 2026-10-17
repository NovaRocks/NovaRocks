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
use novarocks_functions::control_values::CoalesceStep;

pub fn eval_coalesce<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if args.is_empty() {
        return Err(("coalesce: requires at least one argument".to_string()).into());
    }
    let mut arrays = Vec::with_capacity(args.len());
    for &arg in args {
        arrays.push(arena.eval(arg, chunk)?);
    }
    let output_type = arena
        .data_type(expr)
        .ok_or_else(|| "coalesce: missing output type".to_string())?;
    match novarocks_functions::control_values::coalesce_legacy(arrays, output_type)? {
        CoalesceStep::Complete(values) => Ok(values),
        CoalesceStep::ReevaluateTail(keep_alive) => {
            let result = eval_coalesce(arena, expr, &args[1..], chunk);
            drop(keep_alive);
            Ok((result)?)
        }
    }
}
