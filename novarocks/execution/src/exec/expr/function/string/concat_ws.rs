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

use novarocks_functions::builtin::string_concat_core::{
    concat_ws_length_observed, emit_concat_ws_observed,
};
use std::convert::Infallible;

pub fn eval_concat_ws(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let sep_arr = arena.eval(args[0], chunk)?;
    let sep = sep_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "concat_ws expects string separator".to_string())?;
    let mut arrays = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        arrays.push(
            arena
                .eval(*arg, chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "concat_ws expects string".to_string())?
                .clone(),
        );
    }
    let len = chunk.len();
    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        let mut context = (sep, &arrays, row);
        let length = match concat_ws_length_observed(
            arrays.len() + 1,
            &mut context,
            |context, index| {
                let array = if index == 0 {
                    context.0
                } else {
                    &context.1[index - 1]
                };
                Ok::<_, Infallible>(if array.is_null(context.2) {
                    None
                } else {
                    Some(array.value(context.2))
                })
            },
            |_, _| Ok(()),
            |_, _| Ok(()),
        ) {
            Ok(length) => length,
            Err(never) => match never {},
        };
        let Some(total_len) = length else {
            out.push(None);
            continue;
        };
        let mut buf = String::with_capacity(total_len);
        let mut context = (sep, &arrays, row, &mut buf);
        match emit_concat_ws_observed(
            arrays.len() + 1,
            &mut context,
            |context| Ok::<_, Infallible>(context.0.value(context.2)),
            |context, index| {
                let array = &context.1[index - 1];
                Ok(if array.is_null(context.2) {
                    None
                } else {
                    Some(array.value(context.2))
                })
            },
            |context, text| {
                context.3.push_str(text);
                Ok(())
            },
            |_, _| Ok(()),
        ) {
            Ok(()) => {}
            Err(never) => match never {},
        }
        out.push(Some(buf));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
