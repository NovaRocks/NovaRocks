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
use crate::exec::expr::cast::cast_with_special_rules;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray, make_array};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_map_from_arrays(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = chunk;
    let key_arr = arena.eval(args[0], chunk)?;
    let value_arr = arena.eval(args[1], chunk)?;

    let key_list = key_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "map_from_arrays expects ListArray keys, got {:?}",
                key_arr.data_type()
            )
        })?;
    let value_list = value_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "map_from_arrays expects ListArray values, got {:?}",
                value_arr.data_type()
            )
        })?;

    if key_list.len() != value_list.len() {
        return Err(format!(
            "map_from_arrays length mismatch: keys={} values={}",
            key_list.len(),
            value_list.len()
        ));
    }

    let key_values = key_list.values();
    let value_values = value_list.values();
    let key_offsets = key_list.value_offsets();
    let value_offsets = value_list.value_offsets();

    let key_values_data = key_values.to_data();
    let value_values_data = value_values.to_data();
    let mut key_mutable = MutableArrayData::new(vec![&key_values_data], false, key_values.len());
    let mut value_mutable =
        MutableArrayData::new(vec![&value_values_data], false, value_values.len());

    let mut offsets = Vec::with_capacity(key_list.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(key_list.len());

    for row in 0..key_list.len() {
        if key_list.is_null(row) || value_list.is_null(row) {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let key_start = key_offsets[row] as usize;
        let key_end = key_offsets[row + 1] as usize;
        let value_start = value_offsets[row] as usize;
        let value_end = value_offsets[row + 1] as usize;
        let key_len = key_end - key_start;
        let value_len = value_end - value_start;
        if key_len != value_len {
            return Err("Key and value arrays must be the same length".to_string());
        }

        if key_len > 0 {
            for idx in 0..key_len {
                let key_idx = key_start + idx;
                let value_idx = value_start + idx;
                key_mutable.extend(0, key_idx, key_idx + 1);
                value_mutable.extend(0, value_idx, value_idx + 1);
            }
            current += key_len as i64;
            if current > i32::MAX as i64 {
                return Err("map_from_arrays offset overflow".to_string());
            }
        }
        offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let mut out_keys = make_array(key_mutable.freeze());
    let mut out_values = make_array(value_mutable.freeze());
    let (map_field, ordered) = super::common::output_map_field(
        arena.data_type(expr),
        key_values.data_type(),
        value_values.data_type(),
        "map_from_arrays",
    )?;
    let DataType::Struct(fields) = map_field.data_type() else {
        return Err("map_from_arrays map entries type must be Struct".to_string());
    };
    if fields.len() != 2 {
        return Err("map_from_arrays map entries type must have 2 fields".to_string());
    }

    let key_field = fields[0].clone();
    let value_field = fields[1].clone();
    if out_keys.data_type() != key_field.data_type() {
        out_keys = cast_with_special_rules(&out_keys, key_field.data_type()).map_err(|e| {
            format!(
                "map_from_arrays failed to cast keys to output type {:?}: {}",
                key_field.data_type(),
                e
            )
        })?;
    }
    if out_values.data_type() != value_field.data_type() {
        out_values =
            cast_with_special_rules(&out_values, value_field.data_type()).map_err(|e| {
                format!(
                    "map_from_arrays failed to cast values to output type {:?}: {}",
                    value_field.data_type(),
                    e
                )
            })?;
    }

    let entries_fields = if out_keys.null_count() > 0 && !key_field.is_nullable() {
        let mut adjusted = fields.iter().cloned().collect::<Vec<_>>();
        adjusted[0] = Arc::new(Field::new(
            key_field.name(),
            key_field.data_type().clone(),
            true,
        ));
        arrow::datatypes::Fields::from(adjusted)
    } else {
        fields.clone()
    };
    let entries_fields = if out_values.data_type() != value_field.data_type() {
        let mut adjusted = entries_fields.iter().cloned().collect::<Vec<_>>();
        adjusted[1] = Arc::new(Field::new(
            value_field.name(),
            out_values.data_type().clone(),
            value_field.is_nullable(),
        ));
        arrow::datatypes::Fields::from(adjusted)
    } else {
        entries_fields
    };
    let entries = StructArray::new(entries_fields.clone(), vec![out_keys, out_values], None);
    let map_field = Arc::new(Field::new(
        map_field.name(),
        DataType::Struct(entries_fields),
        map_field.is_nullable(),
    ));
    let map = MapArray::new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        null_builder.finish(),
        ordered,
    );

    Ok(Arc::new(map) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::Field;

    #[test]
    fn test_map_from_arrays_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let map_entry_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("key", DataType::Int64, false)),
            Arc::new(Field::new("value", DataType::Int64, true)),
        ]));
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", map_entry_type, false)),
            false,
        );

        let key1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let key2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let val1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let val2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

        let keys = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![key1, key2],
            },
            list_type.clone(),
        );
        let values = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![val1, val2],
            },
            list_type,
        );
        let expr = typed_null(&mut arena, map_type);

        let out =
            eval_map_function("map_from_arrays", &arena, expr, &[keys, values], &chunk).unwrap();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_length(0), 2);
    }

    #[test]
    fn test_map_from_arrays_length_mismatch() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        );

        let key = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let val1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let val2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
        let keys = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![key],
            },
            list_type.clone(),
        );
        let values = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![val1, val2],
            },
            list_type,
        );
        let expr = typed_null(&mut arena, map_type);

        let err =
            eval_map_from_arrays(&arena, expr, &[keys, values], &chunk).expect_err("must fail");
        assert!(err.contains("same length"));
    }

    #[test]
    fn test_map_from_arrays_keeps_null_key_entries() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        );

        let null_key = typed_null(&mut arena, DataType::Int64);
        let key2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let val1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let val2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);

        let keys = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![null_key, key2],
            },
            list_type.clone(),
        );
        let values = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![val1, val2],
            },
            list_type,
        );
        let expr = typed_null(&mut arena, map_type);

        let out =
            eval_map_function("map_from_arrays", &arena, expr, &[keys, values], &chunk).unwrap();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_length(0), 2);
        let key_arr = map.keys();
        assert!(key_arr.is_null(0));
    }
}
