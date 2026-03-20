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
use arrow::array::{Array, ArrayRef, BinaryBuilder, StringArray};
use std::sync::Arc;

pub fn eval_unhex(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "unhex expects string".to_string())?;
    let mut builder = BinaryBuilder::new();
    for i in 0..s_arr.len() {
        if s_arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = s_arr.value(i);
        let value = hex::decode(s).unwrap_or_default();
        builder.append_value(value);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
