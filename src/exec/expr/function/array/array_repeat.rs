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
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, make_array, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn cast_repeat_count(array: ArrayRef) -> Result<ArrayRef, String> {
    if array.data_type() == &DataType::Int64 {
        return Ok(array);
    }
    cast(&array, &DataType::Int64).map_err(|e| {
        format!(
            "array_repeat failed to cast repeat count {:?} -> BIGINT: {}",
            array.data_type(),
            e
        )
    })
}

pub fn eval_array_repeat(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let source = arena.eval(args[0], chunk)?;
    let count_array = cast_repeat_count(arena.eval(args[1], chunk)?)?;
    let counts = count_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "array_repeat internal count downcast failure".to_string())?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => Arc::new(Field::new("item", source.data_type().clone(), true)),
    };
    let item_type = output_field.data_type().clone();
    let mut values = if source.data_type() == &DataType::Null && item_type != DataType::Null {
        new_null_array(&item_type, source.len())
    } else if source.data_type() == &item_type {
        source
    } else {
        cast(&source, &item_type).map_err(|e| {
            format!(
                "array_repeat failed to cast source type {:?} -> {:?}: {}",
                source.data_type(),
                item_type,
                e
            )
        })?
    };
    if values.is_empty() && chunk.len() > 0 {
        values = new_null_array(&item_type, chunk.len());
    }

    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);
    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut null_builder = NullBufferBuilder::new(chunk.len());
    let mut current: i64 = 0;

    for row in 0..chunk.len() {
        let source_row = super::common::row_index(row, values.len());
        let count_row = super::common::row_index(row, counts.len());
        if counts.is_null(count_row) {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let repeat_count = counts.value(count_row);
        if repeat_count > 0 {
            for _ in 0..repeat_count {
                mutable.extend(0, source_row, source_row + 1);
            }
            current = current
                .checked_add(repeat_count)
                .ok_or_else(|| "array_repeat offset overflow".to_string())?;
            if current > i32::MAX as i64 {
                return Err("array_repeat offset overflow".to_string());
            }
        }
        null_builder.append_non_null();
        offsets.push(current as i32);
    }

    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        make_array(mutable.freeze()),
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, literal_i64, typed_null};
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;

    #[test]
    fn test_array_repeat_basic_and_negative() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

        let expr = typed_null(&mut arena, list_type.clone());
        let source = literal_i64(&mut arena, 1);
        let count = literal_i64(&mut arena, 5);
        let out = eval_array_function("array_repeat", &arena, expr, &[source, count], &chunk)
            .expect("array_repeat should succeed");
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 1, 1, 1, 1]);

        let expr2 = typed_null(&mut arena, list_type);
        let neg_count = literal_i64(&mut arena, -1);
        let out2 = eval_array_function("array_repeat", &arena, expr2, &[source, neg_count], &chunk)
            .expect("array_repeat with negative should succeed");
        let out2 = out2.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out2.value_length(0), 0);
    }

    #[test]
    fn test_array_repeat_null_behaviour() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

        let expr = typed_null(&mut arena, list_type.clone());
        let source_null = typed_null(&mut arena, DataType::Int64);
        let count = literal_i64(&mut arena, 3);
        let out = eval_array_function("array_repeat", &arena, expr, &[source_null, count], &chunk)
            .expect("array_repeat should succeed");
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.is_null(0));
        assert!(values.is_null(1));
        assert!(values.is_null(2));

        let expr2 = typed_null(&mut arena, list_type);
        let source = literal_i64(&mut arena, 2);
        let count_null = typed_null(&mut arena, DataType::Int64);
        let out2 =
            eval_array_function("array_repeat", &arena, expr2, &[source, count_null], &chunk)
                .expect("array_repeat should succeed");
        let out2 = out2.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(out2.is_null(0));
    }
}
