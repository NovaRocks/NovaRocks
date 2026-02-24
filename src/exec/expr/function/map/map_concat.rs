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
use arrow::array::{Array, ArrayRef, MapArray, StructArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_map_concat(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("map_concat expects at least one argument".to_string());
    }

    let mut map_arrays = Vec::<ArrayRef>::with_capacity(args.len());
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        arr.as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| format!("map_concat expects MapArray, got {:?}", arr.data_type()))?;
        map_arrays.push(arr);
    }

    let first_map = map_arrays[0]
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "map_concat internal type mismatch".to_string())?;
    let mut all_keys = Vec::<ArrayRef>::with_capacity(map_arrays.len());
    let mut all_values = Vec::<ArrayRef>::with_capacity(map_arrays.len());

    let (map_field, ordered) = super::common::output_map_field(
        arena.data_type(expr),
        first_map.keys().data_type(),
        first_map.values().data_type(),
        "map_concat",
    )?;
    let DataType::Struct(fields) = map_field.data_type() else {
        return Err("map_concat map entries type must be Struct".to_string());
    };
    if fields.len() != 2 {
        return Err("map_concat map entries type must have 2 fields".to_string());
    }

    for arr in &map_arrays {
        let map = arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_concat internal map downcast failed".to_string())?;
        let mut keys = map.keys().clone();
        let mut values = map.values().clone();
        if keys.data_type() != fields[0].data_type() {
            keys = cast(&keys, fields[0].data_type()).map_err(|e| {
                format!(
                    "map_concat failed to cast keys to output type {:?}: {}",
                    fields[0].data_type(),
                    e
                )
            })?;
        }
        if values.data_type() != fields[1].data_type() {
            values = cast(&values, fields[1].data_type()).map_err(|e| {
                format!(
                    "map_concat failed to cast values to output type {:?}: {}",
                    fields[1].data_type(),
                    e
                )
            })?;
        }
        all_keys.push(keys);
        all_values.push(values);
    }

    let keys_data: Vec<arrow_data::ArrayData> = all_keys.iter().map(|v| v.to_data()).collect();
    let values_data: Vec<arrow_data::ArrayData> = all_values.iter().map(|v| v.to_data()).collect();
    let key_refs: Vec<&arrow_data::ArrayData> = keys_data.iter().collect();
    let value_refs: Vec<&arrow_data::ArrayData> = values_data.iter().collect();
    let mut keys_mutable = MutableArrayData::new(key_refs, false, 0);
    let mut values_mutable = MutableArrayData::new(value_refs, false, 0);

    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let mut row_null = true;
        let mut picked = Vec::<(usize, usize)>::new();
        for src_idx in (0..map_arrays.len()).rev() {
            let map = map_arrays[src_idx]
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "map_concat internal map downcast failed".to_string())?;
            let row_idx = super::common::row_index(row, map.len());
            if map.is_null(row_idx) {
                continue;
            }
            row_null = false;

            let offsets = map.value_offsets();
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            for idx in start..end {
                let mut duplicated = false;
                for &(prev_src, prev_idx) in &picked {
                    if super::common::compare_keys_at(
                        &all_keys[src_idx],
                        idx,
                        &all_keys[prev_src],
                        prev_idx,
                    )? {
                        duplicated = true;
                        break;
                    }
                }
                if !duplicated {
                    picked.push((src_idx, idx));
                }
            }
        }

        if row_null {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }

        for (src, idx) in picked {
            keys_mutable.extend(src, idx, idx + 1);
            values_mutable.extend(src, idx, idx + 1);
            current += 1;
        }
        if current > i32::MAX as i64 {
            return Err("map_concat offset overflow".to_string());
        }
        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let out_keys = make_array(keys_mutable.freeze());
    let out_values = make_array(values_mutable.freeze());
    let out_entries = StructArray::new(fields.clone(), vec![out_keys, out_values], None);
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(out_offsets.into()),
        out_entries,
        null_builder.finish(),
        ordered,
    )
    .map_err(|e| format!("map_concat: {}", e))?;
    Ok(Arc::new(out) as ArrayRef)
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
    use std::collections::HashMap;

    fn chunk_with_two_maps() -> (Chunk, DataType, DataType) {
        let mut m1 = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        m1.keys().append_value(1);
        m1.values().append_value(10);
        m1.keys().append_value(2);
        m1.values().append_value(20);
        m1.append(true).unwrap();
        m1.append(false).unwrap();
        let map1 = Arc::new(m1.finish()) as ArrayRef;
        let map1_type = map1.data_type().clone();

        let mut m2 = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        m2.keys().append_value(2);
        m2.values().append_value(200);
        m2.keys().append_value(3);
        m2.values().append_value(300);
        m2.append(true).unwrap();
        m2.append(false).unwrap();
        let map2 = Arc::new(m2.finish()) as ArrayRef;
        let map2_type = map2.data_type().clone();

        let fields = vec![
            field_with_slot_id(Field::new("m1", map1_type.clone(), true), SlotId::new(1)),
            field_with_slot_id(Field::new("m2", map2_type.clone(), true), SlotId::new(2)),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![map1, map2]).unwrap();
        (Chunk::new(batch), map1_type, map2_type)
    }

    #[test]
    fn test_map_concat_rightmost_overrides() {
        let (chunk, map1_type, map2_type) = chunk_with_two_maps();
        let mut arena = ExprArena::default();
        let map1 = slot_id_expr(&mut arena, 1, map1_type.clone());
        let map2 = slot_id_expr(&mut arena, 2, map2_type);
        let expr = typed_null(&mut arena, map1_type);
        let out = eval_map_function("map_concat", &arena, expr, &[map1, map2], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();

        assert_eq!(out.value_length(0), 3);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let start = out.value_offsets()[0] as usize;
        let end = out.value_offsets()[1] as usize;
        let mut kv = HashMap::new();
        for idx in start..end {
            kv.insert(keys.value(idx), values.value(idx));
        }
        assert_eq!(kv.get(&1), Some(&10));
        assert_eq!(kv.get(&2), Some(&200));
        assert_eq!(kv.get(&3), Some(&300));
    }

    #[test]
    fn test_map_concat_all_null_row_is_null() {
        let (chunk, map1_type, map2_type) = chunk_with_two_maps();
        let mut arena = ExprArena::default();
        let map1 = slot_id_expr(&mut arena, 1, map1_type.clone());
        let map2 = slot_id_expr(&mut arena, 2, map2_type);
        let expr = typed_null(&mut arena, map1_type);
        let out = eval_map_function("map_concat", &arena, expr, &[map1, map2], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(out.is_null(1));
    }
}
