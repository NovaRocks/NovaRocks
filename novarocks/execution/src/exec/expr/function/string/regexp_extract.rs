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
use arrow::array::{Array, ArrayRef, StringArray};

pub fn eval_regexp_extract<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    let idx_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_extract expects string".to_string())?;
    let _p_arr = pat_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_extract expects string".to_string())?;
    let _indices = super::common::downcast_int_arg_array(&idx_arr, "regexp_extract")?;
    let len = s_arr.len();
    Ok(
        (novarocks_functions::builtin::string_extended::evaluate_legacy(
            novarocks_functions::builtin::string_extended::StringOperation::RegexpExtract,
            &[str_arr, pat_arr, idx_arr],
            len,
        ))?,
    )
}
