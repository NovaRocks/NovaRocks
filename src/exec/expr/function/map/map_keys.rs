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
use arrow::array::{Array, ArrayRef, ListArray, UInt32Array};
use arrow::compute::take;
use std::sync::Arc;

pub fn eval_map_keys(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_arr
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| format!("map_keys expects MapArray, got {:?}", map_arr.data_type()))?;
    let field = super::common::output_list_field(
        arena.data_type(expr),
        map.keys().data_type(),
        "map_keys",
    )?;
    let (sorted_offsets, sorted_indices) = super::common::sorted_map_offsets_and_indices(map)?;
    let sorted_indices = UInt32Array::from(sorted_indices);
    let sorted_keys = take(map.keys().as_ref(), &sorted_indices, None)
        .map_err(|e| format!("map_keys: failed to reorder keys: {}", e))?;
    let list = ListArray::new(field, sorted_offsets, sorted_keys, map.nulls().cloned());
    let out = Arc::new(list) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "map_keys")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{ArrayRef, Int64Array, Int64Builder, MapBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn single_row_map() -> (Chunk, DataType) {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(10);
        builder.keys().append_value(2);
        builder.values().append_value(20);
        builder.append(true).unwrap();
        let map = Arc::new(builder.finish()) as ArrayRef;
        let map_type = map.data_type().clone();
        let field = field_with_slot_id(Field::new("m", map_type.clone(), true), SlotId::new(1));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![map]).unwrap();
        (Chunk::new(batch), map_type)
    }

    #[test]
    fn test_map_keys() {
        let (chunk, map_type) = single_row_map();
        let mut arena = ExprArena::default();
        let arg = slot_id_expr(&mut arena, 1, map_type);
        let expr = typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let out = eval_map_function("map_keys", &arena, expr, &[arg], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }
}
