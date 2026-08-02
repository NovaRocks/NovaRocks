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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::mv::aggregate_state::state_codec::{
    KeyValue, MultisetEntry, decode_multiset_self_describing, decode_multiset_with_key_type,
    encode_multiset, key_type_tag_for_data_type, read_key, union_multisets,
};

use super::common::{binary_value_or_empty, row_count, row_index};

pub(crate) fn min_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    min_max_state_union("min_state_union", a, b)
}

pub(crate) fn max_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    min_max_state_union("max_state_union", a, b)
}

pub(crate) fn min_state_visible_key_value(
    state: &[u8],
    output_type: &DataType,
) -> Result<Option<KeyValue>, String> {
    visible_key_value(state, output_type, false)
}

pub(crate) fn max_state_visible_key_value(
    state: &[u8],
    output_type: &DataType,
) -> Result<Option<KeyValue>, String> {
    visible_key_value(state, output_type, true)
}

pub(crate) fn eval_min_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_min_max_state_union("min_state_union", min_state_union, arena, args, chunk)
}

pub(crate) fn eval_max_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_min_max_state_union("max_state_union", max_state_union, arena, args, chunk)
}

pub(crate) fn eval_min_state_visible(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_min_max_state_visible("min_state_visible", false, arena, expr, args, chunk)
}

pub(crate) fn eval_max_state_visible(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_min_max_state_visible("max_state_visible", true, arena, expr, args, chunk)
}

fn min_max_state_union(fn_name: &str, a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    let (left_type, left_entries) = decode_multiset_self_describing(a)?;
    let (right_type, right_entries) = decode_multiset_self_describing(b)?;
    let key_type = match (a.is_empty(), b.is_empty()) {
        (true, true) => return Ok(Vec::new()),
        (false, true) => left_type,
        (true, false) => right_type,
        (false, false) => {
            if key_type_tag_for_data_type(&left_type)? != key_type_tag_for_data_type(&right_type)? {
                return Err(format!("{fn_name} key type tag mismatch"));
            }
            left_type
        }
    };
    let entries = union_multisets(&left_entries, &right_entries)?;
    encode_multiset(&entries, &key_type)
}

fn eval_min_max_state_union(
    fn_name: &str,
    op: fn(&[u8], &[u8]) -> Result<Vec<u8>, String>,
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("{fn_name} expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_min_max_state_union_arrays(fn_name, op, &lhs, &rhs)
}

fn eval_min_max_state_visible(
    fn_name: &str,
    pick_max: bool,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if !(1..=2).contains(&args.len()) {
        return Err(format!(
            "{fn_name} expects 1 or 2 arguments, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    // See sum_state_visible: a second typed NULL witness freezes the target
    // MV column type for first-refresh state decoding without exposing state
    // payloads outside the BE.
    let output_type = args
        .get(1)
        .and_then(|witness| arena.data_type(*witness))
        .cloned()
        .or_else(|| arena.data_type(expr).cloned())
        .unwrap_or(DataType::Int64);
    eval_min_max_state_visible_array(fn_name, pick_max, &input, &output_type)
}

fn eval_min_max_state_union_arrays(
    fn_name: &str,
    op: fn(&[u8], &[u8]) -> Result<Vec<u8>, String>,
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count(fn_name, lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(lhs, row_index(row, lhs.len())?, fn_name, 0)?;
        let right = binary_value_or_empty(rhs, row_index(row, rhs.len())?, fn_name, 1)?;
        builder.append_value(op(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn eval_min_max_state_visible_array(
    fn_name: &str,
    pick_max: bool,
    input: &ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    match output_type {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Bool(v) => Ok(*v),
                _ => Err(format!("{fn_name} Boolean output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Int8 => Ok(Arc::new(Int8Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Int8(v) => Ok(*v),
                _ => Err(format!("{fn_name} Int8 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Int16(v) => Ok(*v),
                _ => Err(format!("{fn_name} Int16 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Int32(v) => Ok(*v),
                _ => Err(format!("{fn_name} Int32 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Int64 | DataType::Null => Ok(Arc::new(Int64Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            &DataType::Int64,
            |value| match value {
                KeyValue::Int64(v) => Ok(*v),
                _ => Err(format!("{fn_name} Int64 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Float32(bits) => Ok(f32::from_bits(*bits)),
                _ => Err(format!("{fn_name} Float32 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Float64(bits) => Ok(f64::from_bits(*bits)),
                _ => Err(format!("{fn_name} Float64 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Decimal128(precision, scale) => {
            let values =
                visible_values(fn_name, pick_max, input, output_type, |value| match value {
                    KeyValue::Decimal128(v) => Ok(*v),
                    _ => Err(format!("{fn_name} Decimal128 output type mismatch")),
                })?;
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array) as ArrayRef)
        }
        DataType::Date32 => Ok(Arc::new(Date32Array::from(visible_values(
            fn_name,
            pick_max,
            input,
            output_type,
            |value| match value {
                KeyValue::Date32(v) => Ok(*v),
                _ => Err(format!("{fn_name} Date32 output type mismatch")),
            },
        )?)) as ArrayRef),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(visible_values(
                fn_name,
                pick_max,
                input,
                output_type,
                |value| match value {
                    KeyValue::Timestamp(v) => Ok(*v),
                    _ => Err(format!("{fn_name} Timestamp output type mismatch")),
                },
            )?)) as ArrayRef)
        }
        DataType::Utf8 => build_visible_string(fn_name, pick_max, input, output_type, false),
        DataType::LargeUtf8 => build_visible_string(fn_name, pick_max, input, output_type, true),
        other => Err(format!(
            "{fn_name} unsupported visible output type {other:?}"
        )),
    }
}

fn visible_values<T, F>(
    fn_name: &str,
    pick_max: bool,
    input: &ArrayRef,
    output_type: &DataType,
    convert: F,
) -> Result<Vec<Option<T>>, String>
where
    F: Fn(&KeyValue) -> Result<T, String>,
{
    let mut values = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        let state = binary_value_or_empty(input, row, fn_name, 0)?;
        match visible_key_value(state, output_type, pick_max)? {
            Some(value) => values.push(Some(convert(&value)?)),
            None => values.push(None),
        }
    }
    Ok(values)
}

fn build_visible_string(
    fn_name: &str,
    pick_max: bool,
    input: &ArrayRef,
    output_type: &DataType,
    large: bool,
) -> Result<ArrayRef, String> {
    let values = visible_values(fn_name, pick_max, input, output_type, |value| match value {
        KeyValue::Utf8(v) => Ok(v.clone()),
        _ => Err(format!("{fn_name} Utf8 output type mismatch")),
    })?;
    if large {
        Ok(Arc::new(LargeStringArray::from(values)) as ArrayRef)
    } else {
        Ok(Arc::new(StringArray::from(values)) as ArrayRef)
    }
}

fn visible_key_value(
    state: &[u8],
    output_type: &DataType,
    pick_max: bool,
) -> Result<Option<KeyValue>, String> {
    let entries = decode_multiset_with_key_type(state, output_type)?;
    let mut best = None;
    for entry in entries.into_iter().filter(|entry| entry.count > 0) {
        let value = decode_entry_key(&entry, output_type)?;
        if let Some(current) = &best {
            let ordering = compare_key_values(&value, current)?;
            let replace = if pick_max {
                ordering == Ordering::Greater
            } else {
                ordering == Ordering::Less
            };
            if replace {
                best = Some(value);
            }
        } else {
            best = Some(value);
        }
    }
    Ok(best)
}

fn decode_entry_key(entry: &MultisetEntry, output_type: &DataType) -> Result<KeyValue, String> {
    let mut cursor = entry.key_bytes.as_slice();
    let value = read_key(&mut cursor, output_type)?;
    if !cursor.is_empty() {
        return Err("min/max state key has trailing bytes".to_string());
    }
    Ok(value)
}

fn compare_key_values(left: &KeyValue, right: &KeyValue) -> Result<Ordering, String> {
    match (left, right) {
        (KeyValue::Bool(a), KeyValue::Bool(b)) => Ok(a.cmp(b)),
        (KeyValue::Int8(a), KeyValue::Int8(b)) => Ok(a.cmp(b)),
        (KeyValue::Int16(a), KeyValue::Int16(b)) => Ok(a.cmp(b)),
        (KeyValue::Int32(a), KeyValue::Int32(b)) => Ok(a.cmp(b)),
        (KeyValue::Int64(a), KeyValue::Int64(b)) => Ok(a.cmp(b)),
        (KeyValue::Float32(a), KeyValue::Float32(b)) => {
            Ok(f32::from_bits(*a).total_cmp(&f32::from_bits(*b)))
        }
        (KeyValue::Float64(a), KeyValue::Float64(b)) => {
            Ok(f64::from_bits(*a).total_cmp(&f64::from_bits(*b)))
        }
        (KeyValue::Decimal128(a), KeyValue::Decimal128(b)) => Ok(a.cmp(b)),
        (KeyValue::Date32(a), KeyValue::Date32(b)) => Ok(a.cmp(b)),
        (KeyValue::Timestamp(a), KeyValue::Timestamp(b)) => Ok(a.cmp(b)),
        (KeyValue::Utf8(a), KeyValue::Utf8(b)) => Ok(a.cmp(b)),
        _ => Err("min/max state key type mismatch".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryBuilder, Float64Array, Int64Array};

    use super::*;
    use crate::mv::aggregate_state::state_codec::{decode_multiset_with_key_type, write_key_at};

    fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
        let mut builder = BinaryBuilder::new();
        for value in values {
            match value {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }

    fn key_bytes(array: ArrayRef) -> Vec<u8> {
        let mut out = Vec::new();
        write_key_at(&mut out, &array, 0).unwrap();
        out
    }

    fn int64_state(entries: &[(i64, i64)]) -> Vec<u8> {
        let entries = entries
            .iter()
            .map(|(value, count)| MultisetEntry {
                key_bytes: key_bytes(Arc::new(Int64Array::from(vec![Some(*value)]))),
                count: *count,
            })
            .collect::<Vec<_>>();
        encode_multiset(&entries, &DataType::Int64).unwrap()
    }

    fn float64_state(entries: &[(f64, i64)]) -> Vec<u8> {
        let entries = entries
            .iter()
            .map(|(value, count)| MultisetEntry {
                key_bytes: key_bytes(Arc::new(Float64Array::from(vec![Some(*value)]))),
                count: *count,
            })
            .collect::<Vec<_>>();
        encode_multiset(&entries, &DataType::Float64).unwrap()
    }

    #[test]
    fn min_state_union_preserves_negative_counts() {
        let left = int64_state(&[(3, 2), (5, -1)]);
        let right = int64_state(&[(3, -2), (5, -2)]);

        let out = min_state_union(&left, &right).unwrap();
        let entries = decode_multiset_with_key_type(&out, &DataType::Int64).unwrap();

        assert_eq!(
            entries,
            vec![MultisetEntry {
                key_bytes: 5i64.to_le_bytes().to_vec(),
                count: -3,
            }]
        );
    }

    #[test]
    fn max_state_union_rejects_mismatched_key_tags() {
        let left = int64_state(&[(3, 1)]);
        let right = encode_multiset(
            &[MultisetEntry {
                key_bytes: vec![1],
                count: 1,
            }],
            &DataType::Boolean,
        )
        .unwrap();

        let err = max_state_union(&left, &right).unwrap_err();

        assert!(err.contains("key type tag mismatch"));
    }

    #[test]
    fn min_max_state_visible_filters_non_positive_counts() {
        let input = binary_array(&[Some(int64_state(&[(5, 2), (3, -4), (1, 1)]))]);

        let min_out =
            eval_min_max_state_visible_array("min_state_visible", false, &input, &DataType::Int64)
                .unwrap();
        let max_out =
            eval_min_max_state_visible_array("max_state_visible", true, &input, &DataType::Int64)
                .unwrap();

        assert_eq!(
            min_out
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            max_out
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
    }

    #[test]
    fn min_max_state_visible_returns_null_for_empty_or_negative_only_state() {
        let input = binary_array(&[Some(Vec::new()), Some(int64_state(&[(3, -1)])), None]);

        let out =
            eval_min_max_state_visible_array("min_state_visible", false, &input, &DataType::Int64)
                .unwrap();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();

        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn min_max_state_visible_uses_sql_float_nan_order() {
        let input = binary_array(&[Some(float64_state(&[(f64::NAN, 1), (5.0, 1)]))]);

        let min_out = eval_min_max_state_visible_array(
            "min_state_visible",
            false,
            &input,
            &DataType::Float64,
        )
        .unwrap();
        let max_out =
            eval_min_max_state_visible_array("max_state_visible", true, &input, &DataType::Float64)
                .unwrap();
        let min_arr = min_out.as_any().downcast_ref::<Float64Array>().unwrap();
        let max_arr = max_out.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(min_arr.value(0), 5.0);
        assert!(max_arr.value(0).is_nan());
    }
}
