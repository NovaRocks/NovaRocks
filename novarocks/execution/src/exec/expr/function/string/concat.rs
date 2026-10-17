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
    concat_length_observed, emit_concat_observed,
};
use std::convert::Infallible;

pub fn eval_concat<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let mut arrays = Vec::with_capacity(args.len());
    for arg in args {
        arrays.push(
            arena
                .eval(*arg, chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "concat expects string".to_string())?
                .clone(),
        );
    }
    let len = chunk.len();
    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        let mut context = (&arrays, row);
        let length = match concat_length_observed(arrays.len(), &mut context, |context, index| {
            let array = &context.0[index];
            Ok::<_, Infallible>(if array.is_null(context.1) {
                None
            } else {
                Some(array.value(context.1))
            })
        }) {
            Ok(length) => length,
            Err(never) => match never {},
        };
        if let Some(total_len) = length {
            let mut buf = String::with_capacity(total_len);
            let mut context = (&arrays, row, &mut buf);
            match emit_concat_observed(
                arrays.len(),
                &mut context,
                |context, index| Ok::<_, Infallible>(context.0[index].value(context.1)),
                |context, text| {
                    context.2.push_str(text);
                    Ok(())
                },
                |_, _| Ok(()),
            ) {
                Ok(()) => {}
                Err(never) => match never {},
            }
            out.push(Some(buf));
        } else {
            out.push(None);
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
