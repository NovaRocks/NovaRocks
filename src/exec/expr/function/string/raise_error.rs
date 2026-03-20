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
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use std::sync::Arc;

pub fn eval_raise_error(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = arena.eval(args[0], chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "raise_error expects string".to_string())?;
    // Any non-null value raises an error with that message.
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            return Err(arr.value(i).to_string());
        }
    }
    // All inputs are NULL: return a null boolean column of the same length.
    let nulls: Vec<Option<bool>> = vec![None; arr.len()];
    Ok(Arc::new(BooleanArray::from(nulls)) as ArrayRef)
}
