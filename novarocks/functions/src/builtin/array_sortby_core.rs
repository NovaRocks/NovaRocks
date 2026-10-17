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
//! The original ARRAY_SORTBY math, shared by legacy and installed invocation owners.
use crate::largeint;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, make_array,
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_cast::cast;
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::{DataType, FieldRef};
use std::{cmp::Ordering, fmt, sync::Arc};

pub(crate) enum SortByOperation<'a> {
    KeyArrays {
        count: usize,
    },
    KeyLists {
        count: usize,
    },
    TargetTypeClone {
        original: &'a DataType,
    },
    Cast {
        values: &'a ArrayRef,
        target: &'a DataType,
    },
    KeyValues {
        count: usize,
    },
    SourceData {
        values: &'a ArrayRef,
    },
    Mutable {
        source: &'a ArrayData,
    },
    Offsets {
        rows: usize,
    },
    NullBuilder {
        rows: usize,
    },
    KeyStarts {
        count: usize,
    },
    Positions {
        count: usize,
    },
    Extend {
        source_index: usize,
    },
    Freeze,
    ListResult {
        values: &'a ArrayRef,
        field: &'a FieldRef,
    },
}
/// The port supplies original diagnostics and typed operation observations.
/// Source/result custody is owned by the actual invocation host, not this math.
pub(crate) trait SortByPort {
    type Failure;
    fn diagnostic(&mut self, original: fmt::Arguments<'_>) -> Self::Failure;
    fn step(&mut self) -> Result<(), Self::Failure>;
    fn before_operation(&mut self, original: SortByOperation<'_>) -> Result<(), Self::Failure>;
    fn after_operation(&mut self) -> Result<(), Self::Failure>;
}
struct LegacyPort<E>(std::marker::PhantomData<E>);
impl<E: From<String>> SortByPort for LegacyPort<E> {
    type Failure = E;
    fn diagnostic(&mut self, original: fmt::Arguments<'_>) -> E {
        fmt::format(original).into()
    }
    fn step(&mut self) -> Result<(), E> {
        Ok(())
    }
    fn before_operation(&mut self, _: SortByOperation<'_>) -> Result<(), E> {
        Ok(())
    }
    fn after_operation(&mut self) -> Result<(), E> {
        Ok(())
    }
}
fn is_logically_null(array: &dyn Array, index: usize) -> bool {
    matches!(array.data_type(), DataType::Null) || array.is_null(index)
}

pub fn compare_values_ordered(
    values: &ArrayRef,
    left_idx: usize,
    right_idx: usize,
) -> Result<Ordering, String> {
    if is_logically_null(values.as_ref(), left_idx) || is_logically_null(values.as_ref(), right_idx)
    {
        return Err("array ordered compare does not accept null indices".to_string());
    }
    match values.data_type() {
        DataType::Int8 => {
            let arr = values.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Int16 => {
            let arr = values.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Int32 => {
            let arr = values.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Int64 => {
            let arr = values.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Float32 => {
            let arr = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr
                .value(left_idx)
                .partial_cmp(&arr.value(right_idx))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Float64 => {
            let arr = values.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr
                .value(left_idx)
                .partial_cmp(&arr.value(right_idx))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Boolean => {
            let arr = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Utf8 => {
            let arr = values.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(left_idx).cmp(arr.value(right_idx)))
        }
        DataType::Date32 => {
            let arr = values.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Decimal128(_, _) => {
            let arr = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            let left = largeint::i128_from_be_bytes(arr.value(left_idx))
                .map_err(|e| format!("array ordered compare LARGEINT decode failed: {}", e))?;
            let right = largeint::i128_from_be_bytes(arr.value(right_idx))
                .map_err(|e| format!("array ordered compare LARGEINT decode failed: {}", e))?;
            Ok(left.cmp(&right))
        }
        other => Err(format!(
            "array ordered compare unsupported type: {:?}",
            other
        )),
    }
}

fn compare_key_positions(
    key_values: &[ArrayRef],
    key_starts: &[Option<usize>],
    left_pos: usize,
    right_pos: usize,
) -> Result<Ordering, String> {
    for (idx, key_values) in key_values.iter().enumerate() {
        let Some(key_start) = key_starts[idx] else {
            continue;
        };
        let left_idx = key_start + left_pos;
        let right_idx = key_start + right_pos;

        let left_null = is_logically_null(key_values.as_ref(), left_idx);
        let right_null = is_logically_null(key_values.as_ref(), right_idx);
        let ord = match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => compare_values_ordered(key_values, left_idx, right_idx)?,
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn insertion_sort_by_keys<P: SortByPort>(
    key_values: &[ArrayRef],
    key_starts: &[Option<usize>],
    indices: &mut [usize],
    port: &mut P,
) -> Result<(), P::Failure> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            port.step()?;
            let left = indices[j - 1];
            let right = indices[j];
            let ord = compare_key_positions(key_values, key_starts, left, right)
                .map_err(|cause| port.diagnostic(format_args!("{}", cause)))?;

            if ord == Ordering::Greater {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

/// Keys remain lazy so source validation still precedes original key evaluation.
pub fn evaluate(
    source: &ArrayRef,
    key_count: usize,
    key: impl FnMut(usize) -> Result<ArrayRef, String>,
    output_type: Option<&DataType>,
    rows: usize,
) -> Result<ArrayRef, String> {
    evaluate_fallible(source, key_count, key, output_type, rows)
}
/// Preserve the caller's error when evaluating a demanded key.
pub fn evaluate_fallible<E: From<String>>(
    source: &ArrayRef,
    key_count: usize,
    key: impl FnMut(usize) -> Result<ArrayRef, E>,
    output_type: Option<&DataType>,
    rows: usize,
) -> Result<ArrayRef, E> {
    evaluate_with_port(
        source,
        key_count,
        key,
        output_type,
        rows,
        &mut LegacyPort(std::marker::PhantomData),
    )
}
pub(crate) fn evaluate_with_port<P: SortByPort>(
    source: &ArrayRef,
    key_count: usize,
    mut key: impl FnMut(usize) -> Result<ArrayRef, P::Failure>,
    output_type: Option<&DataType>,
    rows: usize,
    port: &mut P,
) -> Result<ArrayRef, P::Failure> {
    let src = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        port.diagnostic(format_args!(
            "array_sortby expects ListArray src, got {:?}",
            source.data_type()
        ))
    })?;
    port.before_operation(SortByOperation::KeyArrays { count: key_count })?;
    let mut key_arrays = Vec::with_capacity(key_count);
    port.after_operation()?;
    for ordinal in 0..key_count {
        port.step()?;
        key_arrays.push(key(ordinal)?);
    }
    port.before_operation(SortByOperation::KeyLists {
        count: key_arrays.len(),
    })?;
    let mut key_lists = Vec::with_capacity(key_arrays.len());
    port.after_operation()?;
    for key_arr in &key_arrays {
        let key = key_arr
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                port.diagnostic(format_args!(
                    "array_sortby expects ListArray key, got {:?}",
                    key_arr.data_type()
                ))
            })?;
        key_lists.push(key);
    }

    let output_field = match output_type {
        Some(DataType::List(field)) => field.clone(),
        _ => match src.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(port.diagnostic(format_args!(
                    "array_sortby output type must be List, got {:?}",
                    other
                )));
            }
        },
    };
    port.before_operation(SortByOperation::TargetTypeClone {
        original: output_field.data_type(),
    })?;
    let target_item_type = output_field.data_type().clone();
    port.after_operation()?;

    let mut src_values = src.values().clone();
    if src_values.data_type() != &target_item_type {
        port.before_operation(SortByOperation::Cast {
            values: &src_values,
            target: &target_item_type,
        })?;
        src_values = cast(&src_values, &target_item_type).map_err(|e| {
            port.diagnostic(format_args!(
                "array_sortby failed to cast src element {:?} -> {:?}: {}",
                src_values.data_type(),
                target_item_type,
                e
            ))
        })?;
        port.after_operation()?;
    }
    port.before_operation(SortByOperation::KeyValues {
        count: key_lists.len(),
    })?;
    let key_values: Vec<ArrayRef> = key_lists.iter().map(|key| key.values().clone()).collect();
    port.after_operation()?;

    port.before_operation(SortByOperation::SourceData {
        values: &src_values,
    })?;
    let src_values_data = src_values.to_data();
    port.after_operation()?;
    port.before_operation(SortByOperation::Mutable {
        source: &src_values_data,
    })?;
    let mut mutable = MutableArrayData::new(vec![&src_values_data], false, 0);
    port.after_operation()?;

    let src_offsets = src.value_offsets();
    port.before_operation(SortByOperation::Offsets { rows })?;
    let mut out_offsets = Vec::with_capacity(rows + 1);
    port.after_operation()?;
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    port.before_operation(SortByOperation::NullBuilder { rows })?;
    let mut null_builder = NullBufferBuilder::new(rows);
    port.after_operation()?;

    for row in 0..rows {
        port.step()?;
        let src_row = super::map_lookup_core::row_index(row, src.len());
        if src.is_null(src_row) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let src_start = src_offsets[src_row] as usize;
        let src_end = src_offsets[src_row + 1] as usize;
        let src_len = src_end.saturating_sub(src_start);

        port.before_operation(SortByOperation::KeyStarts {
            count: key_lists.len(),
        })?;
        let mut key_starts = Vec::with_capacity(key_lists.len());
        port.after_operation()?;
        for key in &key_lists {
            let key_row = super::map_lookup_core::row_index(row, key.len());
            if key.is_null(key_row) {
                key_starts.push(None);
                continue;
            }
            let key_offsets = key.value_offsets();
            let key_start = key_offsets[key_row] as usize;
            let key_end = key_offsets[key_row + 1] as usize;
            let key_len = key_end.saturating_sub(key_start);
            if src_len != key_len {
                return Err(port.diagnostic(format_args!(
                    "Expr evaluate meet error: Input arrays' size are not equal in array_sortby."
                )));
            }
            key_starts.push(Some(key_start));
        }

        port.before_operation(SortByOperation::Positions { count: src_len })?;
        let mut positions: Vec<usize> = (0..src_len).collect();
        port.after_operation()?;
        insertion_sort_by_keys(&key_values, &key_starts, &mut positions, port)?;
        for pos in positions {
            let src_idx = src_start + pos;
            port.step()?;
            port.before_operation(SortByOperation::Extend {
                source_index: src_idx,
            })?;
            mutable.extend(0, src_idx, src_idx + 1);
            port.after_operation()?;
            current += 1;
        }

        if current > i32::MAX as i64 {
            return Err(port.diagnostic(format_args!("array_sortby offset overflow")));
        }
        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    port.before_operation(SortByOperation::Freeze)?;
    let out_values = make_array(mutable.freeze());
    port.after_operation()?;
    port.before_operation(SortByOperation::ListResult {
        values: &out_values,
        field: &output_field,
    })?;
    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(out_offsets.into()),
        out_values,
        null_builder.finish(),
    );
    let output = Arc::new(out) as ArrayRef;
    port.after_operation()?;
    Ok(output)
}

#[cfg(test)]
#[path = "array_sortby_core_tests.rs"]
mod tests;
