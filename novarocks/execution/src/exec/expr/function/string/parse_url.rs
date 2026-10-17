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

pub fn eval_parse_url<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let url_arr = arena.eval(args[0], chunk)?;
    let part_arr = arena.eval(args[1], chunk)?;
    let url_arr = url_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "parse_url expects string".to_string())?;
    let part_arr = part_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "parse_url expects string".to_string())?;
    let key_arr = if args.len() == 3 {
        Some(
            arena
                .eval(args[2], chunk)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "parse_url expects string".to_string())?
                .clone(),
        )
    } else {
        None
    };
    let len = url_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if url_arr.is_null(i) || part_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let val = novarocks_functions::builtin::string_parse_url_shared::parse_value(
            url_arr.value(i),
            part_arr.value(i),
            &mut || {
                key_arr.as_ref().map(|key_arr| {
                    if key_arr.is_null(i) {
                        None
                    } else {
                        Some(key_arr.value(i))
                    }
                })
            },
        );
        out.push(val);
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
