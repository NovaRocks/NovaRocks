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
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_field(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let first = arena.eval(args[0], chunk)?;
    let first = if first.data_type() == &DataType::Utf8 {
        first
    } else {
        cast(first.as_ref(), &DataType::Utf8).map_err(|_| "field expects string".to_string())?
    };
    let first_arr = first
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "field expects string".to_string())?;
    let mut arrays = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let arr = arena.eval(*arg, chunk)?;
        let arr = if arr.data_type() == &DataType::Utf8 {
            arr
        } else {
            cast(arr.as_ref(), &DataType::Utf8).map_err(|_| "field expects string".to_string())?
        };
        arrays.push(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "field expects string".to_string())?
                .clone(),
        );
    }
    let len = first_arr.len();
    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        if first_arr.is_null(row) {
            out.push(Some(0_i32));
            continue;
        }
        let target = first_arr.value(row);
        let mut idx = 0_i32;
        for (i, arr) in arrays.iter().enumerate() {
            if !arr.is_null(row) && arr.value(row) == target {
                idx = (i + 1) as i32;
                break;
            }
        }
        out.push(Some(idx));
    }
    Ok(Arc::new(arrow::array::Int32Array::from(out)) as ArrayRef)
}
