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
use arrow::array::{Array, ArrayRef, BooleanArray, ListArray, MapArray, StructArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_map_filter(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let filter_arr = arena.eval(args[1], chunk)?;
    let map = map_arr
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| format!("map_filter expects MapArray, got {:?}", map_arr.data_type()))?;
    let filter = filter_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "map_filter expects ListArray, got {:?}",
                filter_arr.data_type()
            )
        })?;

    let mut keys = map.keys().clone();
    let mut values = map.values().clone();
    let (map_field, ordered) = super::common::output_map_field(
        arena.data_type(expr),
        keys.data_type(),
        values.data_type(),
        "map_filter",
    )?;
    let DataType::Struct(fields) = map_field.data_type() else {
        return Err("map_filter map entries type must be Struct".to_string());
    };
    if fields.len() != 2 {
        return Err("map_filter map entries type must have 2 fields".to_string());
    }
    if keys.data_type() != fields[0].data_type() {
        keys = cast(&keys, fields[0].data_type()).map_err(|e| {
            format!(
                "map_filter failed to cast keys to output type {:?}: {}",
                fields[0].data_type(),
                e
            )
        })?;
    }
    if values.data_type() != fields[1].data_type() {
        values = cast(&values, fields[1].data_type()).map_err(|e| {
            format!(
                "map_filter failed to cast values to output type {:?}: {}",
                fields[1].data_type(),
                e
            )
        })?;
    }

    let mut filter_values = filter.values().clone();
    if filter_values.data_type() != &DataType::Boolean {
        filter_values = cast(&filter_values, &DataType::Boolean).map_err(|e| {
            format!(
                "map_filter failed to cast filter array element to BOOLEAN: {}",
                e
            )
        })?;
    }
    let filter_values = filter_values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            "map_filter failed to downcast filter element array to BooleanArray".to_string()
        })?;

    let keys_data = keys.to_data();
    let values_data = values.to_data();
    let mut keys_mutable = MutableArrayData::new(vec![&keys_data], false, 0);
    let mut values_mutable = MutableArrayData::new(vec![&values_data], false, 0);

    let map_offsets = map.value_offsets();
    let filter_offsets = filter.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let map_row = super::common::row_index(row, map.len());
        if map.is_null(map_row) {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }

        let map_start = map_offsets[map_row] as usize;
        let map_end = map_offsets[map_row + 1] as usize;
        let filter_row = super::common::row_index(row, filter.len());
        if filter.is_null(filter_row) {
            out_offsets.push(current as i32);
            null_builder.append_non_null();
            continue;
        }

        let filter_start = filter_offsets[filter_row] as usize;
        let filter_end = filter_offsets[filter_row + 1] as usize;
        let mut filter_idx = filter_start;

        for map_idx in map_start..map_end {
            if filter_idx < filter_end
                && !filter_values.is_null(filter_idx)
                && filter_values.value(filter_idx)
            {
                keys_mutable.extend(0, map_idx, map_idx + 1);
                values_mutable.extend(0, map_idx, map_idx + 1);
                current += 1;
            }
            filter_idx += 1;
        }
        if current > i32::MAX as i64 {
            return Err("map_filter offset overflow".to_string());
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
    .map_err(|e| format!("map_filter: {}", e))?;
    Ok(Arc::new(out) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{
        ArrayRef, BooleanBuilder, Int64Array, Int64Builder, ListBuilder, MapBuilder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn chunk_with_map_and_filter() -> (Chunk, DataType, DataType) {
        let mut map_builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        map_builder.keys().append_value(1);
        map_builder.values().append_value(10);
        map_builder.keys().append_value(2);
        map_builder.values().append_value(20);
        map_builder.keys().append_value(3);
        map_builder.values().append_value(30);
        map_builder.append(true).unwrap();
        map_builder.append(false).unwrap();
        let map = Arc::new(map_builder.finish()) as ArrayRef;
        let map_type = map.data_type().clone();

        let mut filter_builder = ListBuilder::new(BooleanBuilder::new());
        filter_builder.values().append_value(true);
        filter_builder.values().append_null();
        filter_builder.values().append_value(false);
        filter_builder.append(true);
        filter_builder.append(false);
        let filter = Arc::new(filter_builder.finish()) as ArrayRef;
        let filter_type = filter.data_type().clone();

        let fields = vec![
            field_with_slot_id(Field::new("m", map_type.clone(), true), SlotId::new(1)),
            field_with_slot_id(Field::new("f", filter_type.clone(), true), SlotId::new(2)),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![map, filter]).unwrap();
        (Chunk::new(batch), map_type, filter_type)
    }

    #[test]
    fn test_map_filter_basic() {
        let (chunk, map_type, filter_type) = chunk_with_map_and_filter();
        let mut arena = ExprArena::default();
        let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let filter_arg = slot_id_expr(&mut arena, 2, filter_type);
        let expr = typed_null(&mut arena, map_type);
        let out =
            eval_map_function("map_filter", &arena, expr, &[map_arg, filter_arg], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 1);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(values.value(0), 10);
    }

    #[test]
    fn test_map_filter_keeps_map_nullability() {
        let (chunk, map_type, filter_type) = chunk_with_map_and_filter();
        let mut arena = ExprArena::default();
        let map_arg = slot_id_expr(&mut arena, 1, map_type.clone());
        let filter_arg = slot_id_expr(&mut arena, 2, filter_type);
        let expr = typed_null(&mut arena, map_type);
        let out =
            eval_map_function("map_filter", &arena, expr, &[map_arg, filter_arg], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(out.is_null(1));
    }
}
