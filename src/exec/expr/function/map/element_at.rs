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
use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::take;

pub fn eval_element_at(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let key_arr = arena.eval(args[1], chunk)?;
    let check_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let map = map_arr
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| format!("element_at expects MapArray, got {:?}", map_arr.data_type()))?;
    let check_arr = check_arr
        .as_ref()
        .map(|a| {
            a.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "element_at check flag must be BOOLEAN".to_string())
        })
        .transpose()?;

    if key_arr.len() != 1 && key_arr.len() != map.len() {
        return Err(format!(
            "element_at key length mismatch: map rows={}, key rows={}",
            map.len(),
            key_arr.len()
        ));
    }
    if let Some(flags) = check_arr {
        if flags.len() != 1 && flags.len() != map.len() {
            return Err(format!(
                "element_at check flag length mismatch: map rows={}, check rows={}",
                map.len(),
                flags.len()
            ));
        }
    }

    let keys = map.keys();
    let values = map.values();
    let offsets = map.value_offsets();

    let mut indices = Vec::with_capacity(map.len());
    for row in 0..map.len() {
        let key_idx = super::common::row_index(row, key_arr.len());
        let check_idx = check_arr.map(|flags| super::common::row_index(row, flags.len()));
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);

        if map.is_null(row) {
            indices.push(None);
            continue;
        }

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut found = None;
        if key_arr.is_null(key_idx) {
            for i in (start..end).rev() {
                if keys.is_null(i) {
                    found = Some(i as u32);
                    break;
                }
            }
        } else {
            for i in (start..end).rev() {
                if super::common::compare_keys_at(keys, i, &key_arr, key_idx)? {
                    found = Some(i as u32);
                    break;
                }
            }
        }

        if found.is_none() && check_out_of_bounds {
            return Err("Key not present in map".to_string());
        }
        if let Some(v) = found {
            indices.push(Some(v));
        } else {
            indices.push(None);
        }
    }

    let indices = UInt32Array::from(indices);
    let out = take(values.as_ref(), &indices, None).map_err(|e| e.to_string())?;
    super::common::cast_output(out, arena.data_type(expr), "element_at")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::typed_null;
    use crate::exec::expr::{ExprId, ExprNode, LiteralValue};
    use arrow::array::{ArrayRef, Int64Array, Int64Builder, MapBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

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
    fn test_element_at_found() {
        let (chunk, map_type) = single_row_map();
        let mut arena = ExprArena::default();
        let map_arg = arena.push_typed(
            crate::exec::expr::ExprNode::SlotId(SlotId::new(1)),
            map_type,
        );
        let key = literal_i64(&mut arena, 2);
        let expr = typed_null(&mut arena, DataType::Int64);
        let out = eval_map_function("element_at", &arena, expr, &[map_arg, key], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 20);
    }

    #[test]
    fn test_element_at_not_found() {
        let (chunk, map_type) = single_row_map();
        let mut arena = ExprArena::default();
        let map_arg = arena.push_typed(
            crate::exec::expr::ExprNode::SlotId(SlotId::new(1)),
            map_type,
        );
        let key = literal_i64(&mut arena, 9);
        let expr = typed_null(&mut arena, DataType::Int64);
        let out = eval_element_at(&arena, expr, &[map_arg, key], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out.is_null(0));
    }

    #[test]
    fn test_element_at_not_found_with_check_errors() {
        let (chunk, map_type) = single_row_map();
        let mut arena = ExprArena::default();
        let map_arg = arena.push_typed(
            crate::exec::expr::ExprNode::SlotId(SlotId::new(1)),
            map_type,
        );
        let key = literal_i64(&mut arena, 9);
        let check_true = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let expr = typed_null(&mut arena, DataType::Int64);
        let err = eval_element_at(&arena, expr, &[map_arg, key, check_true], &chunk).unwrap_err();
        assert!(err.contains("Key not present in map"));
    }
}
