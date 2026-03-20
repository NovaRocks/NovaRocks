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
use arrow::array::{Array, ArrayRef, Int32Array};
use std::sync::Arc;

pub fn eval_map_size(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = chunk;
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_arr
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| format!("map_size expects MapArray, got {:?}", map_arr.data_type()))?;
    let offsets = map.value_offsets();

    let mut out = Vec::with_capacity(map.len());
    for row in 0..map.len() {
        if map.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(offsets[row + 1] - offsets[row]));
        }
    }
    let out = Arc::new(Int32Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "map_size")
}
