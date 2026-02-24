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
use arrow::array::{Array, ArrayRef, ListArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_array_append(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let target = arena.eval(args[1], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_append expects ListArray, got {:?}", arr.data_type()))?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_append output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut values = list.values().clone();
    if values.data_type() != &target_item_type {
        values = cast(&values, &target_item_type).map_err(|e| {
            format!(
                "array_append failed to cast element type {:?} -> {:?}: {}",
                values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let mut targets = target;
    if targets.data_type() != &target_item_type {
        targets = cast(&targets, &target_item_type).map_err(|e| {
            format!(
                "array_append failed to cast target type {:?} -> {:?}: {}",
                targets.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let values_data = values.to_data();
    let targets_data = targets.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data, &targets_data], false, 0);

    let list_offsets = list.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let list_row = super::common::row_index(row, list.len());
        if list.is_null(list_row) {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }

        let start = list_offsets[list_row] as usize;
        let end = list_offsets[list_row + 1] as usize;
        if end > start {
            mutable.extend(0, start, end);
            current += (end - start) as i64;
        }

        let target_row = super::common::row_index(row, targets.len());
        mutable.extend(1, target_row, target_row + 1);
        current += 1;
        if current > i32::MAX as i64 {
            return Err("array_append offset overflow".to_string());
        }

        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let out_values = make_array(mutable.freeze());
    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(out_offsets.into()),
        out_values,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, literal_i64, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_append_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, v2],
            },
            list_type,
        );
        let target = literal_i64(&mut arena, 3);

        let out =
            eval_array_function("array_append", &arena, expr, &[arr, target], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_array_append_null_array_and_null_element() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

        let expr = typed_null(&mut arena, list_type.clone());
        let arr_null = typed_null(&mut arena, list_type.clone());
        let target = literal_i64(&mut arena, 1);
        let out_null =
            eval_array_function("array_append", &arena, expr, &[arr_null, target], &chunk).unwrap();
        let out_null = out_null.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(out_null.is_null(0));

        let expr2 = typed_null(&mut arena, list_type.clone());
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let arr = arena.push_typed(ExprNode::ArrayExpr { elements: vec![v1] }, list_type);
        let target_null = typed_null(&mut arena, DataType::Int64);
        let out = eval_array_function("array_append", &arena, expr2, &[arr, target_null], &chunk)
            .unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));
    }
}
