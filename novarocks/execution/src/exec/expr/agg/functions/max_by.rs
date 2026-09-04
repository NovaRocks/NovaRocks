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
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use std::sync::Arc;

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;
use super::common::{
    TrackedAggScalarValue, aggregate_vec_with_capacity, build_scalar_array,
    compare_tracked_scalar_values, tracked_scalar_from_array, tracked_scalar_to_output,
};

pub(super) struct MaxMinByAgg;

#[derive(Debug)]
struct MaxMinByState {
    allocator: AggregateAllocator,
    key: Option<TrackedAggScalarValue>,
    value: Option<TrackedAggScalarValue>,
}

impl MaxMinByState {
    fn new(allocator: AggregateAllocator) -> Self {
        Self {
            allocator,
            key: None,
            value: None,
        }
    }
}

fn is_max_kind(kind: &AggKind) -> bool {
    matches!(kind, AggKind::MaxBy | AggKind::MaxByV2)
}

fn allow_null_value(kind: &AggKind) -> bool {
    matches!(kind, AggKind::MaxByV2 | AggKind::MinByV2)
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "max_by" => Some(AggKind::MaxBy),
        "max_by_v2" => Some(AggKind::MaxByV2),
        "min_by" => Some(AggKind::MinBy),
        "min_by_v2" => Some(AggKind::MinByV2),
        _ => None,
    }
}

fn need_len(input: &[u8], need: usize, label: &str) -> Result<(), String> {
    if input.len() < need {
        Err(format!("max_by/min_by {} decode failed", label))
    } else {
        Ok(())
    }
}

fn read_u32(input: &mut &[u8], label: &str) -> Result<u32, String> {
    need_len(input, 4, label)?;
    let value = u32::from_le_bytes(input[..4].try_into().unwrap());
    *input = &input[4..];
    Ok(value)
}

fn encode_tracked_scalar_value(
    value: &TrackedAggScalarValue,
    output: &mut Vec<u8>,
) -> Result<(), String> {
    match value {
        TrackedAggScalarValue::Bool(value) => {
            output.push(1);
            output.push(*value as u8);
        }
        TrackedAggScalarValue::Int64(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Float64(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        TrackedAggScalarValue::Utf8(value) => {
            output.push(4);
            let len = u32::try_from(value.len())
                .map_err(|_| "max_by/min_by tracked UTF-8 value too large".to_string())?;
            output.extend_from_slice(&len.to_le_bytes());
            output.extend_from_slice(value);
        }
        TrackedAggScalarValue::Date32(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Timestamp(value) => {
            output.push(6);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Decimal128(value) => {
            output.push(7);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Struct(values) => {
            output.push(8);
            let len = u32::try_from(values.len())
                .map_err(|_| "max_by/min_by tracked struct too large".to_string())?;
            output.extend_from_slice(&len.to_le_bytes());
            for value in values {
                encode_tracked_scalar(value, output)?;
            }
        }
        TrackedAggScalarValue::Map(entries) => {
            output.push(9);
            let len = u32::try_from(entries.len())
                .map_err(|_| "max_by/min_by tracked map too large".to_string())?;
            output.extend_from_slice(&len.to_le_bytes());
            for (key, value) in entries {
                encode_tracked_scalar(key, output)?;
                encode_tracked_scalar(value, output)?;
            }
        }
        TrackedAggScalarValue::List(values) => {
            output.push(10);
            let len = u32::try_from(values.len())
                .map_err(|_| "max_by/min_by tracked list too large".to_string())?;
            output.extend_from_slice(&len.to_le_bytes());
            for value in values {
                encode_tracked_scalar(value, output)?;
            }
        }
        TrackedAggScalarValue::Decimal256(value) => {
            output.push(11);
            let text = value.to_string();
            let len = u32::try_from(text.len())
                .map_err(|_| "max_by/min_by decimal256 too large".to_string())?;
            output.extend_from_slice(&len.to_le_bytes());
            output.extend_from_slice(text.as_bytes());
        }
        TrackedAggScalarValue::Binary(value) => {
            output.push(12);
            let len = u32::try_from(value.len())
                .map_err(|_| "max_by/min_by tracked binary too large".to_string())?;
            output.extend_from_slice(&len.to_le_bytes());
            output.extend_from_slice(value);
        }
    }
    Ok(())
}

fn encode_tracked_scalar(
    value: &Option<TrackedAggScalarValue>,
    output: &mut Vec<u8>,
) -> Result<(), String> {
    match value {
        Some(value) => {
            output.push(1);
            encode_tracked_scalar_value(value, output)
        }
        None => {
            output.push(0);
            Ok(())
        }
    }
}

fn decode_tracked_scalar_value(
    input: &mut &[u8],
    allocator: &AggregateAllocator,
) -> Result<TrackedAggScalarValue, String> {
    need_len(input, 1, "tracked scalar")?;
    let tag = input[0];
    *input = &input[1..];
    match tag {
        1 => {
            need_len(input, 1, "tracked bool")?;
            let value = input[0] != 0;
            *input = &input[1..];
            Ok(TrackedAggScalarValue::Bool(value))
        }
        2 => {
            need_len(input, 8, "tracked int64")?;
            let value = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(TrackedAggScalarValue::Int64(value))
        }
        3 => {
            need_len(input, 8, "tracked float64")?;
            let value = f64::from_bits(u64::from_le_bytes(input[..8].try_into().unwrap()));
            *input = &input[8..];
            Ok(TrackedAggScalarValue::Float64(value))
        }
        4 | 12 => {
            let len = read_u32(input, "tracked bytes")? as usize;
            need_len(input, len, "tracked bytes")?;
            let value = aggregate_bytes(allocator.clone(), &input[..len])?;
            *input = &input[len..];
            if tag == 4 {
                std::str::from_utf8(&value).map_err(|error| error.to_string())?;
                Ok(TrackedAggScalarValue::Utf8(value))
            } else {
                Ok(TrackedAggScalarValue::Binary(value))
            }
        }
        5 => {
            need_len(input, 4, "tracked date32")?;
            let value = i32::from_le_bytes(input[..4].try_into().unwrap());
            *input = &input[4..];
            Ok(TrackedAggScalarValue::Date32(value))
        }
        6 => {
            need_len(input, 8, "tracked timestamp")?;
            let value = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(TrackedAggScalarValue::Timestamp(value))
        }
        7 => {
            need_len(input, 16, "tracked decimal128")?;
            let value = i128::from_le_bytes(input[..16].try_into().unwrap());
            *input = &input[16..];
            Ok(TrackedAggScalarValue::Decimal128(value))
        }
        8 | 10 => {
            let len = read_u32(input, "tracked sequence")? as usize;
            let operation = if tag == 8 {
                "reserve tracked aggregate struct decode"
            } else {
                "reserve tracked aggregate list decode"
            };
            let mut values = aggregate_vec_with_capacity(allocator, len, operation)?;
            for _ in 0..len {
                values.push(decode_tracked_scalar(input, allocator)?);
            }
            if tag == 8 {
                Ok(TrackedAggScalarValue::Struct(values))
            } else {
                Ok(TrackedAggScalarValue::List(values))
            }
        }
        9 => {
            let len = read_u32(input, "tracked map")? as usize;
            let mut entries = aggregate_vec_with_capacity(
                allocator,
                len,
                "reserve tracked aggregate map decode",
            )?;
            for _ in 0..len {
                entries.push((
                    decode_tracked_scalar(input, allocator)?,
                    decode_tracked_scalar(input, allocator)?,
                ));
            }
            Ok(TrackedAggScalarValue::Map(entries))
        }
        11 => {
            let len = read_u32(input, "tracked decimal256")? as usize;
            need_len(input, len, "tracked decimal256")?;
            let text = std::str::from_utf8(&input[..len]).map_err(|error| error.to_string())?;
            *input = &input[len..];
            Ok(TrackedAggScalarValue::Decimal256(
                text.parse::<i256>()
                    .map_err(|_| "max_by/min_by decimal256 decode failed".to_string())?,
            ))
        }
        _ => Err("max_by/min_by tracked scalar decode failed: unknown tag".to_string()),
    }
}

fn decode_tracked_scalar(
    input: &mut &[u8],
    allocator: &AggregateAllocator,
) -> Result<Option<TrackedAggScalarValue>, String> {
    need_len(input, 1, "tracked scalar")?;
    let has_value = input[0];
    *input = &input[1..];
    match has_value {
        0 => Ok(None),
        1 => decode_tracked_scalar_value(input, allocator).map(Some),
        _ => Err("max_by/min_by tracked scalar decode failed: invalid null flag".to_string()),
    }
}

impl AggregateFunction for MaxMinByAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let kind = kind_from_name(func.name.as_str())
            .ok_or_else(|| format!("unsupported max_by/min_by: {}", func.name))?;
        let data_type = input_type.ok_or_else(|| "max_by/min_by input type missing".to_string())?;

        if input_is_intermediate {
            let sig = super::super::agg_type_signature(func)
                .ok_or_else(|| "max_by/min_by type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "max_by/min_by output type signature missing".to_string())?;
            return Ok(AggSpec {
                kind,
                output_type: output_type.clone(),
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        match data_type {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err("max_by/min_by expects 2 arguments".to_string());
                }
                let value_type = fields[0].data_type().clone();
                Ok(AggSpec {
                    kind,
                    output_type: value_type,
                    intermediate_type: DataType::Binary,
                    input_arg_type: None,
                    count_all: false,
                })
            }
            other => Err(format!(
                "max_by/min_by expects struct input, got {:?}",
                other
            )),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MaxBy | AggKind::MinBy | AggKind::MaxByV2 | AggKind::MinByV2 => (
                std::mem::size_of::<MaxMinByState>(),
                std::mem::align_of::<MaxMinByState>(),
            ),
            other => unreachable!("unexpected kind for max_by/min_by: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "max_by/min_by input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "max_by/min_by merge input missing".to_string())?;
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked max_by/min_by state requires tracker-aware init");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked max_by/min_by state requires a memory tracker".to_string()
        })?;
        unsafe {
            ptr.cast::<MaxMinByState>()
                .write(MaxMinByState::new(AggregateAllocator::new(tracker)))
        };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MaxMinByState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("max_by/min_by batch input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "max_by/min_by expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err("max_by/min_by expects 2 arguments".to_string());
        }
        let value_arr = struct_arr.column(0);
        let key_arr = struct_arr.column(1);
        let is_max = is_max_kind(&spec.kind);
        let allow_null = allow_null_value(&spec.kind);

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MaxMinByState) };
            let key = tracked_scalar_from_array(key_arr, row, &state.allocator)?;
            let Some(key) = key else { continue };
            let value = tracked_scalar_from_array(value_arr, row, &state.allocator)?;
            if value.is_none() && !allow_null {
                continue;
            }

            let should_update = match state.key.as_ref() {
                None => true,
                Some(current) => {
                    let ordering = compare_tracked_scalar_values(&key, current)?;
                    if is_max {
                        ordering == std::cmp::Ordering::Greater
                    } else {
                        ordering == std::cmp::Ordering::Less
                    }
                }
            };
            if should_update {
                state.key = Some(key);
                state.value = value;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(arr) = input else {
            return Err("max_by/min_by merge input type mismatch".to_string());
        };
        let is_max = is_max_kind(&spec.kind);
        let allow_null = allow_null_value(&spec.kind);
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            let mut slice = bytes;
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MaxMinByState) };
            let key = decode_tracked_scalar(&mut slice, &state.allocator)?
                .ok_or_else(|| "max_by/min_by merge missing key".to_string())?;
            let value = decode_tracked_scalar(&mut slice, &state.allocator)?;
            if !slice.is_empty() {
                return Err("max_by/min_by merge input has trailing bytes".to_string());
            }
            if value.is_none() && !allow_null {
                continue;
            }
            let should_update = match state.key.as_ref() {
                None => true,
                Some(current) => {
                    let ordering = compare_tracked_scalar_values(&key, current)?;
                    if is_max {
                        ordering == std::cmp::Ordering::Greater
                    } else {
                        ordering == std::cmp::Ordering::Less
                    }
                }
            };
            if should_update {
                state.key = Some(key);
                state.value = value;
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const MaxMinByState) };
                if state.key.is_none() {
                    builder.append_null();
                    continue;
                }
                let mut buf = Vec::new();
                encode_tracked_scalar(&state.key, &mut buf)?;
                encode_tracked_scalar(&state.value, &mut buf)?;
                builder.append_value(&buf);
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const MaxMinByState) };
            if state.key.is_some() {
                values.push(
                    state
                        .value
                        .as_ref()
                        .map(tracked_scalar_to_output)
                        .transpose()?,
                );
            } else {
                values.push(None);
            }
        }
        build_scalar_array(&spec.output_type, values)
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::AggScalarValue;
    use super::*;
    use arrow::array::{Array, Int64Array, ListArray, MapArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    fn utf8_max_by_spec() -> (AggSpec, DataType) {
        let struct_type = DataType::Struct(
            vec![
                Field::new("v", DataType::Utf8, true),
                Field::new("k", DataType::Utf8, true),
            ]
            .into(),
        );
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Utf8),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        (spec, struct_type)
    }

    fn utf8_struct_batch(values: Vec<&str>, keys: Vec<&str>) -> ArrayRef {
        let fields = vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("k", DataType::Utf8, true),
        ];
        Arc::new(StructArray::new(
            fields.into(),
            vec![
                Arc::new(StringArray::from(values)) as ArrayRef,
                Arc::new(StringArray::from(keys)) as ArrayRef,
            ],
            None,
        ))
    }

    #[test]
    fn test_max_by_spec() {
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let struct_type = DataType::Struct(
            vec![
                Field::new("v", DataType::Int64, true),
                Field::new("k", DataType::Int64, true),
            ]
            .into(),
        );
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MaxBy));
    }

    #[test]
    fn test_max_min_by_variants() {
        let values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let keys = Arc::new(Int64Array::from(vec![1, 3, 2])) as ArrayRef;
        let fields = vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("k", DataType::Int64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values, keys], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        for (name, expected) in [
            ("max_by", "b"),
            ("min_by", "a"),
            ("max_by_v2", "b"),
            ("min_by_v2", "a"),
        ] {
            let func = AggFunction {
                name: name.to_string(),
                inputs: vec![],
                input_is_intermediate: false,
                types: Some(crate::exec::node::aggregate::AggTypeSignature {
                    intermediate_type: Some(DataType::Binary),
                    output_type: Some(DataType::Utf8),
                    input_arg_type: None,
                }),
                ..Default::default()
            };
            let spec = MaxMinByAgg
                .build_spec_from_type(&func, Some(&struct_type), false)
                .unwrap();

            let tracker = MemTracker::new_root(format!("{name}-test"));
            let mut state = MaybeUninit::<MaxMinByState>::uninit();
            MaxMinByAgg
                .init_state_with_tracker(
                    &spec,
                    state.as_mut_ptr().cast(),
                    Some(Arc::clone(&tracker)),
                )
                .unwrap();
            let state_ptr = state.as_mut_ptr() as AggStatePtr;
            let state_ptrs = vec![state_ptr; 3];
            MaxMinByAgg
                .update_batch(&spec, 0, &state_ptrs, &input)
                .unwrap();
            let out = MaxMinByAgg
                .build_array(&spec, 0, &[state_ptr], false)
                .unwrap();
            MaxMinByAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

            let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(out_arr.value(0), expected);
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn max_by_tracks_update_replacement_and_drop_exactly() {
        let (spec, _) = utf8_max_by_spec();
        let tracker = MemTracker::new_root("max-by-update-test");
        let mut state = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = utf8_struct_batch(vec!["old"], vec!["a"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&initial))
            .unwrap();
        assert_eq!(tracker.current(), 4);

        let replacement = utf8_struct_batch(vec!["replacement"], vec!["z"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&replacement))
            .unwrap();
        assert_eq!(tracker.current(), 12);

        let output = MaxMinByAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "replacement"
        );

        MaxMinByAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn max_by_merge_uses_destination_allocator_and_releases_both_states() {
        let (spec, _) = utf8_max_by_spec();
        let source_tracker = MemTracker::new_root("max-by-merge-source-test");
        let destination_tracker = MemTracker::new_root("max-by-merge-destination-test");
        let mut source = MaybeUninit::<MaxMinByState>::uninit();
        let mut destination = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                source.as_mut_ptr().cast(),
                Some(Arc::clone(&source_tracker)),
            )
            .unwrap();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                destination.as_mut_ptr().cast(),
                Some(Arc::clone(&destination_tracker)),
            )
            .unwrap();
        let source_ptr = source.as_mut_ptr() as AggStatePtr;
        let destination_ptr = destination.as_mut_ptr() as AggStatePtr;

        let source_input = utf8_struct_batch(vec!["merged"], vec!["z"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[source_ptr], &AggInputView::Any(&source_input))
            .unwrap();
        let destination_input = utf8_struct_batch(vec!["old"], vec!["a"]);
        MaxMinByAgg
            .update_batch(
                &spec,
                0,
                &[destination_ptr],
                &AggInputView::Any(&destination_input),
            )
            .unwrap();

        let intermediate = MaxMinByAgg
            .build_array(&spec, 0, &[source_ptr], true)
            .unwrap();
        let intermediate = Some(intermediate);
        let merge_view = MaxMinByAgg.build_merge_view(&spec, &intermediate).unwrap();
        MaxMinByAgg
            .merge_batch(&spec, 0, &[destination_ptr], &merge_view)
            .unwrap();
        assert_eq!(destination_tracker.current(), 7);
        let output = MaxMinByAgg
            .build_array(&spec, 0, &[destination_ptr], false)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "merged"
        );

        MaxMinByAgg.drop_state(&spec, source.as_mut_ptr().cast());
        MaxMinByAgg.drop_state(&spec, destination.as_mut_ptr().cast());
        assert_eq!(source_tracker.current(), 0);
        assert_eq!(destination_tracker.current(), 0);
    }

    #[test]
    fn max_by_oom_preserves_prior_state_and_exact_charge() {
        let (spec, _) = utf8_max_by_spec();
        let tracker = MemTracker::new_root("max-by-oom-test");
        tracker.install_limit_once(8).unwrap();
        let mut state = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = utf8_struct_batch(vec!["aa"], vec!["aa"]);
        MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&initial))
            .unwrap();
        assert_eq!(tracker.current(), 4);

        // The candidate would retain only 7 bytes, but replacing the 4-byte
        // state requires admitting the complete 11-byte old + new peak.
        let rejected = utf8_struct_batch(vec!["bbbb"], vec!["zzz"]);
        let error = MaxMinByAgg
            .update_batch(&spec, 0, &[state_ptr], &AggInputView::Any(&rejected))
            .unwrap_err();
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), 4);

        let output = MaxMinByAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "aa"
        );

        MaxMinByAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn max_by_nested_value_update_and_merge_use_exact_allocators() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let map_entry_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Binary, true),
        ]));
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", map_entry_type, false)),
            false,
        );
        let value_type = DataType::Struct(Fields::from(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("items", list_type, true),
            Field::new("attributes", map_type, true),
        ]));
        let packed_fields = Fields::from(vec![
            Field::new("v", value_type.clone(), true),
            Field::new("k", DataType::Utf8, true),
        ]);
        let packed_type = DataType::Struct(packed_fields.clone());
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(value_type.clone()),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&packed_type), false)
            .unwrap();
        let nested_value = AggScalarValue::Struct(vec![
            Some(AggScalarValue::Utf8("root".to_string())),
            Some(AggScalarValue::Binary(vec![1, 2, 3])),
            Some(AggScalarValue::List(vec![
                Some(AggScalarValue::Utf8("first".to_string())),
                None,
            ])),
            Some(AggScalarValue::Map(vec![(
                Some(AggScalarValue::Utf8("key".to_string())),
                Some(AggScalarValue::Binary(vec![4, 5])),
            )])),
        ]);
        let value_array = build_scalar_array(&value_type, vec![Some(nested_value)]).unwrap();
        let key_array = Arc::new(StringArray::from(vec!["z"])) as ArrayRef;
        let packed = Arc::new(StructArray::new(
            packed_fields,
            vec![value_array, key_array],
            None,
        )) as ArrayRef;

        let source_tracker = MemTracker::new_root("nested-max-by-source-test");
        let destination_tracker = MemTracker::new_root("nested-max-by-destination-test");
        let mut source = MaybeUninit::<MaxMinByState>::uninit();
        let mut destination = MaybeUninit::<MaxMinByState>::uninit();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                source.as_mut_ptr().cast(),
                Some(Arc::clone(&source_tracker)),
            )
            .unwrap();
        MaxMinByAgg
            .init_state_with_tracker(
                &spec,
                destination.as_mut_ptr().cast(),
                Some(Arc::clone(&destination_tracker)),
            )
            .unwrap();
        let source_ptr = source.as_mut_ptr() as AggStatePtr;
        let destination_ptr = destination.as_mut_ptr() as AggStatePtr;
        MaxMinByAgg
            .update_batch(&spec, 0, &[source_ptr], &AggInputView::Any(&packed))
            .unwrap();
        assert!(source_tracker.current() > 0);

        let intermediate = MaxMinByAgg
            .build_array(&spec, 0, &[source_ptr], true)
            .unwrap();
        let intermediate = Some(intermediate);
        let merge_view = MaxMinByAgg.build_merge_view(&spec, &intermediate).unwrap();
        MaxMinByAgg
            .merge_batch(&spec, 0, &[destination_ptr], &merge_view)
            .unwrap();
        assert!(destination_tracker.current() > 0);

        let output = MaxMinByAgg
            .build_array(&spec, 0, &[destination_ptr], false)
            .unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "root"
        );
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[1, 2, 3]
        );
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            2
        );
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value_length(0),
            1
        );

        MaxMinByAgg.drop_state(&spec, source.as_mut_ptr().cast());
        MaxMinByAgg.drop_state(&spec, destination.as_mut_ptr().cast());
        assert_eq!(source_tracker.current(), 0);
        assert_eq!(destination_tracker.current(), 0);
    }
}
