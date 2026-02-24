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

pub fn eval_distinct_map_keys(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        format!(
            "distinct_map_keys expects MapArray, got {:?}",
            map_arr.data_type()
        )
    })?;

    let mut keys = map.keys().clone();
    let mut values = map.values().clone();
    let (map_field, ordered) = super::common::output_map_field(
        arena.data_type(expr),
        keys.data_type(),
        values.data_type(),
        "distinct_map_keys",
    )?;
    let DataType::Struct(fields) = map_field.data_type() else {
        return Err("distinct_map_keys map entries type must be Struct".to_string());
    };
    if fields.len() != 2 {
        return Err("distinct_map_keys map entries type must have 2 fields".to_string());
    }
    if keys.data_type() != fields[0].data_type() {
        keys = cast(&keys, fields[0].data_type()).map_err(|e| {
            format!(
                "distinct_map_keys failed to cast keys to output type {:?}: {}",
                fields[0].data_type(),
                e
            )
        })?;
    }
    if values.data_type() != fields[1].data_type() {
        values = cast(&values, fields[1].data_type()).map_err(|e| {
            format!(
                "distinct_map_keys failed to cast values to output type {:?}: {}",
                fields[1].data_type(),
                e
            )
        })?;
    }

    let keys_data = keys.to_data();
    let values_data = values.to_data();
    let mut keys_mutable = MutableArrayData::new(vec![&keys_data], false, 0);
    let mut values_mutable = MutableArrayData::new(vec![&values_data], false, 0);

    let offsets = map.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let row_idx = super::common::row_index(row, map.len());
        if map.is_null(row_idx) {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut selected_rev = Vec::<usize>::new();
        for idx in (start..end).rev() {
            let mut duplicated = false;
            for &picked in &selected_rev {
                if super::common::compare_keys_at(&keys, idx, &keys, picked)? {
                    duplicated = true;
                    break;
                }
            }
            if !duplicated {
                selected_rev.push(idx);
            }
        }

        selected_rev.reverse();
        for idx in selected_rev {
            keys_mutable.extend(0, idx, idx + 1);
            values_mutable.extend(0, idx, idx + 1);
            current += 1;
        }
        if current > i32::MAX as i64 {
            return Err("distinct_map_keys offset overflow".to_string());
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
    .map_err(|e| format!("distinct_map_keys: {}", e))?;
    Ok(Arc::new(out) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{Int64Array, Int64Builder, MapBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn chunk_with_map_rows() -> (Chunk, DataType) {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(10);
        builder.keys().append_value(2);
        builder.values().append_value(20);
        builder.keys().append_value(1);
        builder.values().append_value(30);
        builder.append(true).unwrap();
        builder.append(false).unwrap();

        let map = Arc::new(builder.finish()) as ArrayRef;
        let map_type = map.data_type().clone();
        let field = field_with_slot_id(Field::new("m", map_type.clone(), true), SlotId::new(1));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![map]).unwrap();
        (Chunk::new(batch), map_type)
    }

    #[test]
    fn test_distinct_map_keys_keep_last_value() {
        let (chunk, map_type) = chunk_with_map_rows();
        let mut arena = ExprArena::default();
        let arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let expr = typed_null(&mut arena, map_type);
        let out = eval_map_function("distinct_map_keys", &arena, expr, &[arg], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 2);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 2);
        assert_eq!(values.value(0), 20);
        assert_eq!(keys.value(1), 1);
        assert_eq!(values.value(1), 30);
    }

    #[test]
    fn test_distinct_map_keys_keep_null_row() {
        let (chunk, map_type) = chunk_with_map_rows();
        let mut arena = ExprArena::default();
        let arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let expr = typed_null(&mut arena, map_type);
        let out = eval_map_function("distinct_map_keys", &arena, expr, &[arg], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(out.is_null(1));
    }
}
