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
use std::collections::BTreeSet;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, ListArray, StringArray, StringBuilder, StructArray,
};
use arrow::datatypes::DataType;
use base64::Engine;
use serde_json::json;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct DictMergeAgg;

const DEFAULT_DICT_THRESHOLD: i32 = 255;

#[derive(Debug, Default)]
struct DictMergeState {
    values: BTreeSet<String>,
    over_limit: bool,
    threshold: i32,
    threshold_initialized: bool,
}

impl DictMergeState {
    fn max_values_allowed(&self) -> usize {
        self.threshold().saturating_add(1) as usize
    }

    fn threshold(&self) -> i32 {
        if self.threshold_initialized {
            self.threshold
        } else {
            DEFAULT_DICT_THRESHOLD
        }
    }

    fn set_threshold_from_i64(&mut self, threshold: i64) -> Result<(), String> {
        let threshold = i32::try_from(threshold)
            .map_err(|_| format!("dict_merge threshold overflow: {}", threshold))?;
        if threshold < 0 {
            return Err(format!(
                "dict_merge threshold must be non-negative: {}",
                threshold
            ));
        }
        self.threshold = threshold;
        self.threshold_initialized = true;
        if self.values.len() > self.max_values_allowed() {
            self.over_limit = true;
        }
        Ok(())
    }

    fn set_threshold_if_present(&mut self, threshold: Option<i64>) -> Result<(), String> {
        if let Some(v) = threshold {
            self.set_threshold_from_i64(v)
        } else {
            Ok(())
        }
    }

    fn insert_value(&mut self, value: &str) {
        if self.over_limit {
            return;
        }
        self.values.insert(value.to_string());
        if self.values.len() > self.max_values_allowed() {
            self.over_limit = true;
        }
    }
}

fn build_fake_values(threshold: i32) -> Vec<String> {
    let mut out = Vec::with_capacity((threshold + 1).max(0) as usize);
    for idx in 0..=threshold {
        out.push(idx.to_string());
    }
    out.sort();
    out
}

fn extract_threshold_row(array: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
    let view = IntArrayView::new(array)?;
    Ok(view.value_at(row))
}

fn update_from_value_array_row(
    state: &mut DictMergeState,
    value_array: &ArrayRef,
    row: usize,
) -> Result<(), String> {
    match value_array.data_type() {
        DataType::Utf8 => {
            let arr = value_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    "dict_merge failed to downcast value array to StringArray".to_string()
                })?;
            if !arr.is_null(row) {
                state.insert_value(arr.value(row));
            }
            Ok(())
        }
        DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {
            let list = value_array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    "dict_merge failed to downcast value array to ListArray".to_string()
                })?;
            if list.is_null(row) {
                return Ok(());
            }
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "dict_merge list items must be UTF8".to_string())?;
            for idx in start..end {
                if !values.is_null(idx) {
                    state.insert_value(values.value(idx));
                }
            }
            Ok(())
        }
        other => Err(format!(
            "dict_merge expects string or array<string> value input, got {:?}",
            other
        )),
    }
}

fn encode_base64_no_pad(input: &str) -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(input.as_bytes())
}

fn decode_base64_no_pad(input: &str) -> Result<String, String> {
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(input))
        .map_err(|e| format!("dict_merge decode base64 failed: {}", e))?;
    String::from_utf8(bytes).map_err(|e| format!("dict_merge decoded bytes are not UTF8: {}", e))
}

fn serialize_intermediate_state(state: &DictMergeState) -> Result<String, String> {
    let values = if state.over_limit {
        Vec::<String>::new()
    } else {
        state.values.iter().cloned().collect::<Vec<_>>()
    };
    serde_json::to_string(&json!({
        "t": state.threshold(),
        "o": state.over_limit,
        "v": values,
    }))
    .map_err(|e| format!("dict_merge serialize intermediate failed: {}", e))
}

fn merge_from_intermediate_json(
    state: &mut DictMergeState,
    value: &serde_json::Value,
) -> Result<bool, String> {
    let Some(obj) = value.as_object() else {
        return Ok(false);
    };
    let Some(threshold_raw) = obj.get("t").and_then(|v| v.as_i64()) else {
        return Ok(false);
    };
    state.set_threshold_from_i64(threshold_raw)?;

    if obj.get("o").and_then(|v| v.as_bool()).unwrap_or(false) {
        state.over_limit = true;
        return Ok(true);
    }

    if let Some(values) = obj.get("v").and_then(|v| v.as_array()) {
        for item in values {
            if state.over_limit {
                break;
            }
            let Some(text) = item.as_str() else {
                return Err("dict_merge intermediate values must be strings".to_string());
            };
            state.insert_value(text);
        }
    }
    Ok(true)
}

fn merge_from_final_dict_json(
    state: &mut DictMergeState,
    value: &serde_json::Value,
) -> Result<bool, String> {
    let Some(list) = value
        .get("2")
        .and_then(|v| v.get("lst"))
        .and_then(|v| v.as_array())
    else {
        return Ok(false);
    };

    if list.len() < 2 {
        return Err("dict_merge final dict payload is malformed".to_string());
    }
    let value_type = list[0].as_str().unwrap_or_default();
    if !value_type.eq_ignore_ascii_case("str") {
        return Err(format!(
            "dict_merge final dict payload type mismatch: expected str, got {}",
            value_type
        ));
    }

    for item in list.iter().skip(2) {
        if state.over_limit {
            break;
        }
        let encoded = item
            .as_str()
            .ok_or_else(|| "dict_merge final dict values must be strings".to_string())?;
        let decoded = decode_base64_no_pad(encoded)?;
        state.insert_value(&decoded);
    }
    Ok(true)
}

fn merge_serialized_state(state: &mut DictMergeState, raw: &str) -> Result<(), String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| format!("dict_merge parse intermediate payload failed: {}", e))?;

    if merge_from_intermediate_json(state, &value)? {
        return Ok(());
    }
    if merge_from_final_dict_json(state, &value)? {
        return Ok(());
    }

    Err("dict_merge merge payload format is not recognized".to_string())
}

fn finalize_dict_json(state: &DictMergeState) -> Result<Option<String>, String> {
    if !state.over_limit && state.values.is_empty() {
        return Ok(None);
    }

    let values = if state.over_limit {
        build_fake_values(state.threshold())
    } else {
        state.values.iter().cloned().collect::<Vec<_>>()
    };

    if values.is_empty() {
        return Ok(None);
    }

    let mut string_lst = Vec::with_capacity(values.len() + 2);
    string_lst.push(json!("str"));
    string_lst.push(json!(values.len() as i32));
    for value in &values {
        string_lst.push(json!(encode_base64_no_pad(value)));
    }

    let mut id_lst = Vec::with_capacity(values.len() + 2);
    id_lst.push(json!("i32"));
    id_lst.push(json!(values.len() as i32));
    for idx in 0..values.len() {
        id_lst.push(json!((idx + 1) as i32));
    }

    serde_json::to_string(&json!({
        "2": {"lst": string_lst},
        "3": {"lst": id_lst},
    }))
    .map(Some)
    .map_err(|e| format!("dict_merge serialize final payload failed: {}", e))
}

fn append_serialized_intermediate(
    builder_utf8: &mut Option<StringBuilder>,
    builder_binary: &mut Option<BinaryBuilder>,
    payload: &str,
) {
    if let Some(builder) = builder_utf8.as_mut() {
        builder.append_value(payload);
    }
    if let Some(builder) = builder_binary.as_mut() {
        builder.append_value(payload.as_bytes());
    }
}

fn append_final_payload(
    builder_utf8: &mut Option<StringBuilder>,
    builder_binary: &mut Option<BinaryBuilder>,
    payload: Option<String>,
) {
    match payload {
        Some(value) => {
            if let Some(builder) = builder_utf8.as_mut() {
                builder.append_value(&value);
            }
            if let Some(builder) = builder_binary.as_mut() {
                builder.append_value(value.as_bytes());
            }
        }
        None => {
            if let Some(builder) = builder_utf8.as_mut() {
                builder.append_null();
            }
            if let Some(builder) = builder_binary.as_mut() {
                builder.append_null();
            }
        }
    }
}

impl AggregateFunction for DictMergeAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let sig = super::super::agg_type_signature(func)
            .ok_or_else(|| "dict_merge type signature missing".to_string())?;
        let output_type = sig.output_type.as_ref().cloned().unwrap_or(DataType::Utf8);
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or(DataType::Utf8);

        if input_is_intermediate {
            if let Some(t) = input_type {
                match t {
                    DataType::Utf8 | DataType::Binary => {}
                    DataType::Struct(fields) if fields.len() == 2 => {}
                    other => {
                        return Err(format!(
                            "dict_merge merge expects varchar/binary or struct input, got {:?}",
                            other
                        ));
                    }
                }
            }
            return Ok(AggSpec {
                kind: AggKind::DictMerge,
                output_type,
                intermediate_type,
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let input_type = input_type.ok_or_else(|| "dict_merge input type missing".to_string())?;
        let DataType::Struct(fields) = input_type else {
            return Err(format!(
                "dict_merge expects 2-arg struct input, got {:?}",
                input_type
            ));
        };
        if fields.len() != 2 {
            return Err(format!(
                "dict_merge expects 2 arguments, got {}",
                fields.len()
            ));
        }

        let value_type = fields[0].data_type();
        match value_type {
            DataType::Utf8 => {}
            DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {}
            other => {
                return Err(format!(
                    "dict_merge first argument must be string or array<string>, got {:?}",
                    other
                ));
            }
        }

        let threshold_type = fields[1].data_type();
        match threshold_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {}
            other => {
                return Err(format!(
                    "dict_merge threshold argument must be integer, got {:?}",
                    other
                ));
            }
        }

        Ok(AggSpec {
            kind: AggKind::DictMerge,
            output_type,
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<DictMergeState>(),
            std::mem::align_of::<DictMergeState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "dict_merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "dict_merge merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut DictMergeState, DictMergeState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut DictMergeState);
        }
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("dict_merge input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "dict_merge expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err(format!(
                "dict_merge expects 2 arguments, got {}",
                struct_arr.num_columns()
            ));
        }

        let value_arr = struct_arr.column(0).clone();
        let threshold_arr = struct_arr.column(1).clone();

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut DictMergeState) };
            state.set_threshold_if_present(extract_threshold_row(&threshold_arr, row)?)?;
            if state.over_limit {
                continue;
            }
            update_from_value_array_row(state, &value_arr, row)?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("dict_merge merge input type mismatch".to_string());
        };

        let (value_arr, threshold_arr_opt): (ArrayRef, Option<ArrayRef>) =
            if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
                if struct_arr.num_columns() != 2 {
                    return Err(format!(
                        "dict_merge merge struct expects 2 arguments, got {}",
                        struct_arr.num_columns()
                    ));
                }
                (
                    struct_arr.column(0).clone(),
                    Some(struct_arr.column(1).clone()),
                )
            } else {
                ((*array).clone(), None)
            };

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut DictMergeState) };
            if let Some(threshold_arr) = threshold_arr_opt.as_ref() {
                state.set_threshold_if_present(extract_threshold_row(threshold_arr, row)?)?;
            }
            if state.over_limit {
                continue;
            }

            match value_arr.data_type() {
                DataType::Utf8 => {
                    let arr = value_arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "dict_merge merge expects UTF8 payload".to_string())?;
                    if arr.is_null(row) {
                        continue;
                    }
                    merge_serialized_state(state, arr.value(row))?;
                }
                DataType::Binary => {
                    let arr = value_arr
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| "dict_merge merge expects Binary payload".to_string())?;
                    if arr.is_null(row) {
                        continue;
                    }
                    let raw = std::str::from_utf8(arr.value(row)).map_err(|e| {
                        format!("dict_merge merge payload is not UTF8 encoded JSON: {}", e)
                    })?;
                    merge_serialized_state(state, raw)?;
                }
                DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {
                    update_from_value_array_row(state, &value_arr, row)?;
                }
                other => {
                    return Err(format!(
                        "dict_merge merge input type mismatch: expected UTF8/Binary/List<Utf8>, got {:?}",
                        other
                    ));
                }
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
            let mut utf8_builder =
                matches!(spec.intermediate_type, DataType::Utf8).then(StringBuilder::new);
            let mut binary_builder =
                matches!(spec.intermediate_type, DataType::Binary).then(BinaryBuilder::new);
            if utf8_builder.is_none() && binary_builder.is_none() {
                return Err(format!(
                    "dict_merge intermediate output type must be Utf8 or Binary, got {:?}",
                    spec.intermediate_type
                ));
            }

            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const DictMergeState) };
                let payload = serialize_intermediate_state(state)?;
                append_serialized_intermediate(&mut utf8_builder, &mut binary_builder, &payload);
            }

            if let Some(mut builder) = utf8_builder {
                return Ok(Arc::new(builder.finish()));
            }
            if let Some(mut builder) = binary_builder {
                return Ok(Arc::new(builder.finish()));
            }
            return Err("dict_merge failed to build intermediate output".to_string());
        }

        let mut utf8_builder = matches!(spec.output_type, DataType::Utf8).then(StringBuilder::new);
        let mut binary_builder =
            matches!(spec.output_type, DataType::Binary).then(BinaryBuilder::new);
        if utf8_builder.is_none() && binary_builder.is_none() {
            return Err(format!(
                "dict_merge output type must be Utf8 or Binary, got {:?}",
                spec.output_type
            ));
        }

        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const DictMergeState) };
            let payload = finalize_dict_json(state)?;
            append_final_payload(&mut utf8_builder, &mut binary_builder, payload);
        }

        if let Some(mut builder) = utf8_builder {
            return Ok(Arc::new(builder.finish()));
        }
        if let Some(mut builder) = binary_builder {
            return Ok(Arc::new(builder.finish()));
        }
        Err("dict_merge failed to build output".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_DICT_THRESHOLD, DictMergeState, finalize_dict_json, merge_serialized_state,
        serialize_intermediate_state,
    };

    #[test]
    fn finalize_payload_uses_sorted_base64_strings() {
        let mut state = DictMergeState::default();
        state
            .set_threshold_from_i64(DEFAULT_DICT_THRESHOLD as i64)
            .unwrap();
        state.insert_value("shanghai");
        state.insert_value("beijing");

        let output = finalize_dict_json(&state)
            .expect("finalize")
            .expect("non-null output");
        let value: serde_json::Value = serde_json::from_str(&output).expect("parse json");

        let string_list = value
            .get("2")
            .and_then(|v| v.get("lst"))
            .and_then(|v| v.as_array())
            .expect("string list");
        assert_eq!(string_list[0], serde_json::json!("str"));
        assert_eq!(string_list[1], serde_json::json!(2));
        assert_eq!(string_list[2], serde_json::json!("YmVpamluZw"));
        assert_eq!(string_list[3], serde_json::json!("c2hhbmdoYWk"));
    }

    #[test]
    fn merge_intermediate_payload_recovers_values() {
        let mut left = DictMergeState::default();
        left.set_threshold_from_i64(255).unwrap();
        left.insert_value("beijing");
        let raw = serialize_intermediate_state(&left).expect("serialize");

        let mut right = DictMergeState::default();
        right.set_threshold_from_i64(255).unwrap();
        merge_serialized_state(&mut right, &raw).expect("merge");

        assert!(right.values.contains("beijing"));
    }
}
