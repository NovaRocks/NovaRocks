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
use std::sync::Arc;

use super::common::downcast_int_arg_array;

pub fn eval_split_part<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let delim_arr = arena.eval(args[1], chunk)?;
    let idx_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "split_part expects string".to_string())?;
    let d_arr = delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "split_part expects string".to_string())?;
    let i_arr = downcast_int_arg_array(&idx_arr, "split_part")?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || d_arr.is_null(i) || i_arr.is_null(i) {
            // Match StarRocks behavior for split_part: any NULL input yields empty string.
            out.push(Some(String::new()));
            continue;
        }
        let s = s_arr.value(i);
        let delim = d_arr.value(i);
        let idx = i_arr.value(i);
        out.push(Some(split_part_impl(s, delim, idx)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

fn split_part_impl(s: &str, delim: &str, idx: i64) -> String {
    novarocks_functions::string_split_part_core::original(s, delim, idx)
}
