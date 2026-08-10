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
use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::Field;
use std::sync::Arc;

pub fn eval_map_entries(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_arr
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| {
            format!(
                "map_entries expects MapArray, got {:?}",
                map_arr.data_type()
            )
        })?;
    let entries = Arc::new(map.entries().clone()) as ArrayRef;
    let list = ListArray::new(
        Arc::new(Field::new("item", map.entries().data_type().clone(), true)),
        map.offsets().clone(),
        entries,
        map.nulls().cloned(),
    );
    let out = Arc::new(list) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "map_entries")
}
