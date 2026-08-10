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
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, MapArray};
use std::sync::Arc;

pub fn eval_cardinality(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;

    let mut out = Vec::with_capacity(arr.len());
    if let Some(map) = arr.as_any().downcast_ref::<MapArray>() {
        let offsets = map.value_offsets();
        for row in 0..map.len() {
            if map.is_null(row) {
                out.push(None);
            } else {
                out.push(Some(offsets[row + 1] - offsets[row]));
            }
        }
    } else if let Some(list) = arr.as_any().downcast_ref::<ListArray>() {
        let offsets = list.value_offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                out.push(None);
            } else {
                out.push(Some(offsets[row + 1] - offsets[row]));
            }
        }
    } else {
        return Err(format!(
            "cardinality expects ARRAY or MAP, got {:?}",
            arr.data_type()
        ));
    }

    let out = Arc::new(Int32Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "cardinality")
}
