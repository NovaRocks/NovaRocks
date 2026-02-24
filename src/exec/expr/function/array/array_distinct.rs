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
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, make_array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

fn hash_value_with_null(values: &ArrayRef, idx: usize) -> Result<u64, String> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_value_impl(values, idx, &mut hasher)?;
    Ok(hasher.finish())
}

fn hash_value_impl(
    values: &ArrayRef,
    idx: usize,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) -> Result<(), String> {
    if values.is_null(idx) {
        0u8.hash(hasher);
        return Ok(());
    }
    1u8.hash(hasher);
    values.data_type().hash(hasher);
    match values.data_type() {
        DataType::Boolean => {
            values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int8 => {
            values
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int16 => {
            values
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int32 => {
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int64 => {
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Float32 => {
            values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?
                .value(idx)
                .to_bits()
                .hash(hasher);
        }
        DataType::Float64 => {
            values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?
                .value(idx)
                .to_bits()
                .hash(hasher);
        }
        DataType::Utf8 => {
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Date32 => {
            values
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Decimal128(_, _) => {
            values
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let arr = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            arr.value(idx).hash(hasher);
        }
        DataType::List(_) => {
            let list = values
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            let offsets = list.value_offsets();
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            end.saturating_sub(start).hash(hasher);
            let child_values = list.values();
            for i in start..end {
                hash_value_impl(&child_values, i, hasher)?;
            }
        }
        DataType::Struct(_) => {
            let s = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            s.num_columns().hash(hasher);
            for col_idx in 0..s.num_columns() {
                hash_value_impl(s.column(col_idx), idx, hasher)?;
            }
        }
        DataType::Map(_, _) => {
            let map = values
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
            let offsets = map.value_offsets();
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            end.saturating_sub(start).hash(hasher);
            let keys = map.keys();
            let vals = map.values();
            for i in start..end {
                hash_value_impl(&keys, i, hasher)?;
                hash_value_impl(&vals, i, hasher)?;
            }
        }
        other => return Err(format!("array_distinct hash unsupported type: {:?}", other)),
    }
    Ok(())
}

pub fn eval_array_distinct(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "array_distinct expects ListArray, got {:?}",
            arr.data_type()
        )
    })?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_distinct output type must be List, got {:?}",
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
                "array_distinct failed to cast element type {:?} -> {:?}: {}",
                values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);

    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    let offsets = list.value_offsets();
    let int64_values = values.as_any().downcast_ref::<Int64Array>();
    for row in 0..chunk.len() {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut picked = Vec::<usize>::new();

        if let Some(typed) = int64_values {
            let mut seen = HashSet::with_capacity(end.saturating_sub(start));
            let mut null_seen = false;
            for idx in start..end {
                if typed.is_null(idx) {
                    if !null_seen {
                        picked.push(idx);
                        null_seen = true;
                    }
                    continue;
                }
                if seen.insert(typed.value(idx)) {
                    picked.push(idx);
                }
            }
        } else {
            let mut buckets = HashMap::<u64, Vec<usize>>::new();
            for idx in start..end {
                let hash = hash_value_with_null(&values, idx)?;
                let mut duplicated = false;
                if let Some(candidates) = buckets.get(&hash) {
                    for &prev in candidates {
                        if super::common::compare_values_with_null(
                            &values, idx, &values, prev, true,
                        )? {
                            duplicated = true;
                            break;
                        }
                    }
                }
                if !duplicated {
                    picked.push(idx);
                    buckets.entry(hash).or_default().push(idx);
                }
            }
        }

        for idx in picked {
            mutable.extend(0, idx, idx + 1);
            current += 1;
        }
        if current > i32::MAX as i64 {
            return Err("array_distinct offset overflow".to_string());
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
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_distinct_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, v2, v1, v2],
            },
            list_type,
        );

        let out = eval_array_function("array_distinct", &arena, expr, &[arr], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }

    #[test]
    fn test_array_distinct_keep_single_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let null_elem = typed_null(&mut arena, DataType::Int64);
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![null_elem, v1, null_elem, v2, v1],
            },
            list_type,
        );

        let out = eval_array_function("array_distinct", &arena, expr, &[arr], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 1);
        assert_eq!(values.value(2), 2);
    }
}
