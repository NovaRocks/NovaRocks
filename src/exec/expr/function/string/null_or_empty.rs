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
use arrow::array::{Array, ArrayRef, BooleanArray, ListArray, StringArray};
use std::sync::Arc;

pub fn eval_null_or_empty(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = arena.eval(args[0], chunk)?;
    let len = input.len();

    // Try StringArray first
    if let Some(s_arr) = input.as_any().downcast_ref::<StringArray>() {
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            if s_arr.is_null(i) {
                out.push(Some(true));
            } else {
                out.push(Some(s_arr.value(i).is_empty()));
            }
        }
        return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
    }

    // Try ListArray (array types)
    if let Some(list_arr) = input.as_any().downcast_ref::<ListArray>() {
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            if list_arr.is_null(i) {
                out.push(Some(true));
            } else {
                out.push(Some(list_arr.value(i).len() == 0));
            }
        }
        return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
    }

    // For all-null input (NullArray), everything is null_or_empty
    if input.null_count() == len {
        let out: Vec<Option<bool>> = vec![Some(true); len];
        return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
    }

    Err(format!(
        "null_or_empty expects string or array, got {:?}",
        input.data_type()
    ))
}
