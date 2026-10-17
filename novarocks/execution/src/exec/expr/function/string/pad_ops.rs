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
use novarocks_functions::string_repeat_pad_core::{OriginalPadProjection, pad_into};
use std::sync::Arc;

fn eval_pad_impl<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    left: bool,
) -> Result<ArrayRef, __Eval::Error> {
    let str_arr = arena.eval(args[0], chunk)?;
    let len_arr = arena.eval(args[1], chunk)?;
    let pad_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "pad expects string".to_string())?;
    let len_arr = super::common::downcast_int_arg_array(&len_arr, "pad")?;
    let p_arr = pad_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "pad expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || len_arr.is_null(i) || p_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let target_len = len_arr.value(i);
        match pad_into(
            s,
            target_len,
            || p_arr.value(i),
            left,
            OriginalPadProjection::new(&mut out),
        ) {
            Ok(()) => (),
            Err(impossible) => match impossible {},
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_lpad<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_pad_impl(arena, args, chunk, true))?)
}

pub fn eval_rpad<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_pad_impl(arena, args, chunk, false))?)
}
