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
use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringBuilder, StructArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::i256;
use base64::Engine;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::{largeint, sketch_hash::prehash_array_value};
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, compare_scalar_values, scalar_from_array, scalar_to_string};
use std::cmp::Ordering;
use std::sync::Arc;

pub(super) struct HistogramAgg;
pub(super) struct HistogramHllNdvAgg;

const HLL_COLUMN_PRECISION: usize = 14;
const HLL_REGISTERS_COUNT: usize = 16 * 1024;
const HLL_EXPLICIT_INT64_NUM: usize = 160;

#[derive(Default)]
struct HistogramState {
    values: Vec<AggScalarValue>,
    bucket_num: Option<i64>,
    sample_ratio: Option<f64>,
    ndv_estimator: Option<String>,
}

#[derive(Clone)]
struct Bucket {
    lower: AggScalarValue,
    upper: AggScalarValue,
    count: i64,
    upper_repeats: i64,
    count_in_bucket: i64,
    distinct_count: i64,
}

impl Bucket {
    fn new(value: AggScalarValue) -> Self {
        Self {
            lower: value.clone(),
            upper: value,
            count: 1,
            upper_repeats: 1,
            count_in_bucket: 1,
            distinct_count: 0,
        }
    }
}

#[derive(Default)]
struct HistogramHllNdvState {
    buckets_json: Option<String>,
    buckets: Vec<HistogramHllNdvBucket>,
    hlls: Vec<HistogramBucketHllState>,
}

#[derive(Clone)]
struct HistogramHllNdvBucket {
    lower: AggScalarValue,
    upper: AggScalarValue,
    count: i64,
    upper_repeats: i64,
}

struct HistogramBucketHllState {
    explicit_hashes: Option<Vec<u64>>,
    registers: Option<Box<[u8; HLL_REGISTERS_COUNT]>>,
}

#[derive(Serialize, Deserialize)]
struct HistogramHllNdvIntermediatePayload {
    #[serde(default)]
    b: Option<String>,
    #[serde(default)]
    s: Vec<HistogramHllNdvIntermediateBucketState>,
}

#[derive(Serialize, Deserialize)]
struct HistogramHllNdvIntermediateBucketState {
    #[serde(default)]
    e: Option<Vec<u64>>,
    #[serde(default)]
    r: Option<String>,
}

impl Default for HistogramBucketHllState {
    fn default() -> Self {
        Self {
            explicit_hashes: Some(Vec::new()),
            registers: None,
        }
    }
}

enum JsonArrayView<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> JsonArrayView<'a> {
    fn new(array: &'a ArrayRef, context: &str) -> Result<Self, String> {
        match array.data_type() {
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| format!("{context}: failed to downcast StringArray"))?;
                Ok(Self::Utf8(arr))
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| format!("{context}: failed to downcast LargeStringArray"))?;
                Ok(Self::LargeUtf8(arr))
            }
            other => Err(format!("{context}: expected string input, got {:?}", other)),
        }
    }

    fn value_at(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            Self::LargeUtf8(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
        }
    }
}

impl AggregateFunction for HistogramAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        if input_is_intermediate {
            return Err("histogram does not support merge aggregation".to_string());
        }
        let input_type = input_type.ok_or_else(|| "histogram input type missing".to_string())?;
        let DataType::Struct(fields) = input_type else {
            return Err("histogram expects struct input".to_string());
        };
        if !(fields.len() == 3 || fields.len() == 4) {
            return Err("histogram expects 3 or 4 arguments".to_string());
        }

        let sig = super::super::agg_type_signature(func)
            .ok_or_else(|| "histogram type signature missing".to_string())?;
        let output_type = sig
            .output_type
            .as_ref()
            .ok_or_else(|| "histogram output type missing".to_string())?;
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or_else(|| output_type.clone());

        Ok(AggSpec {
            kind: AggKind::Histogram,
            output_type: output_type.clone(),
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<HistogramState>(),
            std::mem::align_of::<HistogramState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "histogram input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "histogram merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut HistogramState, HistogramState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut HistogramState);
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
            return Err("histogram input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "histogram expects struct input".to_string())?;
        if !(struct_arr.num_columns() == 3 || struct_arr.num_columns() == 4) {
            return Err("histogram expects 3 or 4 arguments".to_string());
        }

        let value_arr = struct_arr.column(0);
        let bucket_arr = struct_arr.column(1);
        let ratio_arr = struct_arr.column(2);
        let estimator_arr = if struct_arr.num_columns() == 4 {
            Some(
                struct_arr
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "histogram estimator expects string".to_string())?,
            )
        } else {
            None
        };

        let bucket_view = IntArrayView::new(bucket_arr)?;
        let ratio_view = FloatArrayView::new(ratio_arr)?;

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut HistogramState) };

            if let Some(bucket_num) = bucket_view.value_at(row) {
                if bucket_num <= 0 {
                    return Err("histogram bucket_num must be positive".to_string());
                }
                if let Some(prev) = state.bucket_num {
                    if prev != bucket_num {
                        return Err("histogram bucket_num must be constant".to_string());
                    }
                } else {
                    state.bucket_num = Some(bucket_num);
                }
            }

            if let Some(sample_ratio) = ratio_view.value_at(row) {
                if let Some(prev) = state.sample_ratio {
                    if (prev - sample_ratio).abs() > f64::EPSILON {
                        return Err("histogram sample_ratio must be constant".to_string());
                    }
                } else {
                    state.sample_ratio = Some(sample_ratio);
                }
            }

            if let Some(estimator_arr) = estimator_arr {
                if !estimator_arr.is_null(row) {
                    let name = estimator_arr.value(row).to_string();
                    let name = name.to_uppercase();
                    if let Some(prev) = state.ndv_estimator.as_ref() {
                        if prev != &name {
                            return Err("histogram ndv estimator must be constant".to_string());
                        }
                    } else {
                        state.ndv_estimator = Some(name);
                    }
                }
            }

            let value = scalar_from_array(value_arr, row)?;
            if let Some(v) = value {
                state.values.push(v);
            }
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        _offset: usize,
        _state_ptrs: &[AggStatePtr],
        _input: &AggInputView,
    ) -> Result<(), String> {
        Err("histogram does not support merge aggregation".to_string())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            return Err("histogram does not support intermediate output".to_string());
        }
        let value_type = spec
            .input_arg_type
            .as_ref()
            .ok_or_else(|| "histogram input type missing".to_string())?;

        let mut builder = StringBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const HistogramState) };
            if state.values.is_empty() {
                builder.append_null();
                continue;
            }
            let bucket_num = state
                .bucket_num
                .ok_or_else(|| "histogram bucket_num missing".to_string())?;
            let sample_ratio = state
                .sample_ratio
                .ok_or_else(|| "histogram sample_ratio missing".to_string())?;

            let bucket_size = (state.values.len() as i64) / bucket_num;
            let has_ndv = state.ndv_estimator.is_some();
            let sample_factor = if sample_ratio == 0.0 {
                0.0
            } else {
                1.0 / sample_ratio
            };

            let buckets = if has_ndv {
                build_buckets_with_ndv(
                    &state.values,
                    bucket_size,
                    sample_ratio,
                    state.ndv_estimator.as_deref().unwrap(),
                )?
            } else {
                build_buckets_without_ndv(&state.values, bucket_size)?
            };

            let mut json = String::from("[");
            for bucket in buckets {
                let lower = scalar_to_string(&bucket.lower, value_type)?;
                let upper = scalar_to_string(&bucket.upper, value_type)?;
                let count = (bucket.count as f64 * sample_factor).trunc() as i64;
                let upper_repeats = (bucket.upper_repeats as f64 * sample_factor).trunc() as i64;
                if has_ndv {
                    json.push_str(&format!(
                        "[\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"],",
                        lower, upper, count, upper_repeats, bucket.distinct_count
                    ));
                } else {
                    json.push_str(&format!(
                        "[\"{}\",\"{}\",\"{}\",\"{}\"],",
                        lower, upper, count, upper_repeats
                    ));
                }
            }
            if json.ends_with(',') {
                json.pop();
            }
            json.push(']');
            builder.append_value(json);
        }
        Ok(Arc::new(builder.finish()))
    }
}

impl AggregateFunction for HistogramHllNdvAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type =
            input_type.ok_or_else(|| "histogram_hll_ndv input type missing".to_string())?;
        if input_is_intermediate {
            match input_type {
                DataType::Utf8 | DataType::LargeUtf8 => {}
                other => {
                    return Err(format!(
                        "histogram_hll_ndv merge expects string input, got {:?}",
                        other
                    ));
                }
            }
        } else {
            let DataType::Struct(fields) = input_type else {
                return Err("histogram_hll_ndv expects struct input".to_string());
            };
            if fields.len() != 2 {
                return Err("histogram_hll_ndv expects 2 arguments".to_string());
            }
        }

        let sig = super::super::agg_type_signature(func)
            .ok_or_else(|| "histogram_hll_ndv type signature missing".to_string())?;
        let output_type = sig
            .output_type
            .as_ref()
            .ok_or_else(|| "histogram_hll_ndv output type missing".to_string())?;
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or_else(|| output_type.clone());

        Ok(AggSpec {
            kind: AggKind::HistogramHllNdv,
            output_type: output_type.clone(),
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<HistogramHllNdvState>(),
            std::mem::align_of::<HistogramHllNdvState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "histogram_hll_ndv input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "histogram_hll_ndv merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut HistogramHllNdvState,
                HistogramHllNdvState::default(),
            );
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut HistogramHllNdvState);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("histogram_hll_ndv input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "histogram_hll_ndv expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err("histogram_hll_ndv expects 2 arguments".to_string());
        }

        let value_arr = struct_arr.column(0);
        let bucket_arr = struct_arr.column(1);
        let bucket_view = JsonArrayView::new(bucket_arr, "histogram_hll_ndv bucket json")?;
        let value_type = spec
            .input_arg_type
            .as_ref()
            .ok_or_else(|| "histogram_hll_ndv input type missing".to_string())?;

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state =
                unsafe { &mut *((base as *mut u8).add(offset) as *mut HistogramHllNdvState) };

            if let Some(bucket_json) = bucket_view.value_at(row) {
                ensure_histogram_hll_ndv_buckets(state, bucket_json, value_type)?;
            }

            let value = scalar_from_array(value_arr, row)?;
            let Some(value) = value else {
                continue;
            };

            if state.buckets_json.is_none() {
                return Err("histogram_hll_ndv bucket specification missing".to_string());
            }

            let Some(bucket_idx) = find_histogram_hll_ndv_bucket(&state.buckets, &value)? else {
                continue;
            };
            let Some(hash) = prehash_array_value(value_arr, row, "histogram_hll_ndv")? else {
                continue;
            };
            update_bucket_register_from_hash(&mut state.hlls[bucket_idx], hash);
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
        let AggInputView::Any(array) = input else {
            return Err("histogram_hll_ndv merge input type mismatch".to_string());
        };
        let value_type = spec
            .input_arg_type
            .as_ref()
            .ok_or_else(|| "histogram_hll_ndv input type missing".to_string())?;
        let payload_view = JsonArrayView::new(array, "histogram_hll_ndv merge payload")?;

        for (row, &base) in state_ptrs.iter().enumerate() {
            let Some(payload) = payload_view.value_at(row) else {
                continue;
            };
            let state =
                unsafe { &mut *((base as *mut u8).add(offset) as *mut HistogramHllNdvState) };
            merge_histogram_hll_ndv_intermediate(state, payload, value_type)?;
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
        let value_type = spec
            .input_arg_type
            .as_ref()
            .ok_or_else(|| "histogram_hll_ndv input type missing".to_string())?;

        let mut builder = StringBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const HistogramHllNdvState) };
            if output_intermediate {
                builder.append_value(serialize_histogram_hll_ndv_intermediate(state)?);
                continue;
            }
            if state.buckets.is_empty() {
                builder.append_null();
                continue;
            }

            let mut json = String::from("[");
            for (bucket, hll) in state.buckets.iter().zip(state.hlls.iter()) {
                let lower = scalar_to_string(&bucket.lower, value_type)?;
                let upper = scalar_to_string(&bucket.upper, value_type)?;
                let mut distinct_count = estimate_bucket_cardinality(hll);
                if distinct_count == 0 {
                    distinct_count = 1;
                }
                json.push_str(&format!(
                    "[\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"],",
                    lower, upper, bucket.count, bucket.upper_repeats, distinct_count
                ));
            }
            if json.ends_with(',') {
                json.pop();
            }
            json.push(']');
            builder.append_value(json);
        }
        Ok(Arc::new(builder.finish()))
    }
}

fn build_buckets_without_ndv(
    values: &[AggScalarValue],
    bucket_size: i64,
) -> Result<Vec<Bucket>, String> {
    let mut buckets: Vec<Bucket> = Vec::new();
    for v in values {
        if buckets.is_empty() {
            buckets.push(Bucket::new(v.clone()));
            continue;
        }
        let last = buckets.last_mut().expect("bucket");
        let is_equal = compare_scalar_values(v, &last.upper)? == std::cmp::Ordering::Equal;
        if is_equal {
            last.count += 1;
            last.count_in_bucket += 1;
            last.upper_repeats += 1;
        } else if last.count_in_bucket >= bucket_size {
            let mut bucket = Bucket::new(v.clone());
            bucket.count = last.count + 1;
            buckets.push(bucket);
        } else {
            last.upper = v.clone();
            last.count += 1;
            last.count_in_bucket += 1;
            last.upper_repeats = 1;
        }
    }
    Ok(buckets)
}

fn build_buckets_with_ndv(
    values: &[AggScalarValue],
    bucket_size: i64,
    sample_ratio: f64,
    estimator: &str,
) -> Result<Vec<Bucket>, String> {
    let mut buckets: Vec<Bucket> = Vec::new();
    let mut sample_distinct: i64 = 0;
    let mut count_once: i64 = 0;
    let mut new_upper = false;

    for v in values {
        if buckets.is_empty() {
            buckets.push(Bucket::new(v.clone()));
            sample_distinct = 1;
            count_once = 1;
            new_upper = true;
            continue;
        }
        let last = buckets.last_mut().expect("bucket");
        let is_equal = compare_scalar_values(v, &last.upper)? == std::cmp::Ordering::Equal;
        if is_equal {
            last.count += 1;
            last.count_in_bucket += 1;
            last.upper_repeats += 1;
            if new_upper {
                count_once -= 1;
                new_upper = false;
            }
        } else {
            new_upper = true;
            if last.count_in_bucket >= bucket_size {
                last.distinct_count = estimate_ndv(
                    estimator,
                    last.count_in_bucket,
                    sample_distinct,
                    count_once,
                    sample_ratio,
                )?;
                let mut bucket = Bucket::new(v.clone());
                bucket.count = last.count + 1;
                buckets.push(bucket);
                sample_distinct = 1;
                count_once = 1;
            } else {
                last.upper = v.clone();
                last.count += 1;
                last.count_in_bucket += 1;
                last.upper_repeats = 1;
                sample_distinct += 1;
                count_once += 1;
            }
        }
    }

    if let Some(last) = buckets.last_mut() {
        last.distinct_count = estimate_ndv(
            estimator,
            last.count_in_bucket,
            sample_distinct,
            count_once,
            sample_ratio,
        )?;
    }
    Ok(buckets)
}

fn estimate_ndv(
    estimator: &str,
    sample_row: i64,
    sample_distinct: i64,
    count_once: i64,
    sample_ratio: f64,
) -> Result<i64, String> {
    if sample_ratio == 0.0 {
        return Ok(0);
    }
    let estimator = estimator.to_uppercase();
    let result = match estimator.as_str() {
        "DUJ1" => {
            let denom = sample_row as f64 - count_once as f64 + (count_once as f64 * sample_ratio);
            if denom == 0.0 {
                0
            } else {
                (sample_row as f64 * sample_distinct as f64 / denom) as i64
            }
        }
        "LINEAR" => (sample_distinct as f64 / sample_ratio) as i64,
        "POLYNOMIAL" => {
            let denom = 1.0 - (1.0 - sample_ratio).powi(3);
            if denom == 0.0 {
                0
            } else {
                (sample_distinct as f64 / denom) as i64
            }
        }
        "GEE" => {
            let factor = (1.0 / sample_ratio).sqrt() - 1.0;
            (sample_distinct as f64 + factor * count_once as f64) as i64
        }
        _ => return Err("histogram: unknown ndv estimator".to_string()),
    };
    Ok(result)
}

fn ensure_histogram_hll_ndv_buckets(
    state: &mut HistogramHllNdvState,
    bucket_json: &str,
    value_type: &DataType,
) -> Result<(), String> {
    if let Some(prev) = state.buckets_json.as_ref() {
        if prev != bucket_json {
            return Err("histogram_hll_ndv bucket specification must be constant".to_string());
        }
        return Ok(());
    }

    let buckets = parse_histogram_hll_ndv_buckets(bucket_json, value_type)?;
    state.buckets_json = Some(bucket_json.to_string());
    state.hlls = std::iter::repeat_with(HistogramBucketHllState::default)
        .take(buckets.len())
        .collect();
    state.buckets = buckets;
    Ok(())
}

fn serialize_histogram_hll_ndv_intermediate(
    state: &HistogramHllNdvState,
) -> Result<String, String> {
    let bucket_states = state
        .hlls
        .iter()
        .map(|hll| {
            let registers = hll.registers.as_ref().map(|registers| {
                base64::engine::general_purpose::STANDARD_NO_PAD.encode(registers.as_slice())
            });
            HistogramHllNdvIntermediateBucketState {
                e: hll.explicit_hashes.clone(),
                r: registers,
            }
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&HistogramHllNdvIntermediatePayload {
        b: state.buckets_json.clone(),
        s: bucket_states,
    })
    .map_err(|e| format!("histogram_hll_ndv serialize intermediate failed: {}", e))
}

fn merge_histogram_hll_ndv_intermediate(
    state: &mut HistogramHllNdvState,
    payload: &str,
    value_type: &DataType,
) -> Result<(), String> {
    let payload: HistogramHllNdvIntermediatePayload = serde_json::from_str(payload)
        .map_err(|e| format!("histogram_hll_ndv parse intermediate payload failed: {}", e))?;

    if let Some(bucket_json) = payload.b.as_deref() {
        ensure_histogram_hll_ndv_buckets(state, bucket_json, value_type)?;
    } else if !payload.s.is_empty() {
        return Err("histogram_hll_ndv intermediate payload missing bucket metadata".to_string());
    }

    if payload.s.is_empty() {
        return Ok(());
    }
    if state.hlls.len() != payload.s.len() {
        return Err(format!(
            "histogram_hll_ndv intermediate bucket count mismatch: expected {}, got {}",
            state.hlls.len(),
            payload.s.len()
        ));
    }

    for (dst, src) in state.hlls.iter_mut().zip(payload.s.iter()) {
        merge_histogram_hll_ndv_bucket_state(dst, src)?;
    }
    Ok(())
}

fn merge_histogram_hll_ndv_bucket_state(
    dst: &mut HistogramBucketHllState,
    src: &HistogramHllNdvIntermediateBucketState,
) -> Result<(), String> {
    if let Some(explicit_hashes) = src.e.as_ref() {
        for &hash_value in explicit_hashes {
            update_bucket_register_from_hash(dst, hash_value);
        }
    }
    if let Some(registers_b64) = src.r.as_ref() {
        let registers = decode_histogram_hll_ndv_registers(registers_b64)?;
        merge_histogram_hll_ndv_registers(dst, &registers);
    }
    Ok(())
}

fn decode_histogram_hll_ndv_registers(raw: &str) -> Result<Box<[u8; HLL_REGISTERS_COUNT]>, String> {
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(raw)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(raw))
        .map_err(|e| format!("histogram_hll_ndv decode registers failed: {}", e))?;
    if bytes.len() != HLL_REGISTERS_COUNT {
        return Err(format!(
            "histogram_hll_ndv register payload size mismatch: expected {}, got {}",
            HLL_REGISTERS_COUNT,
            bytes.len()
        ));
    }
    let mut registers = Box::new([0u8; HLL_REGISTERS_COUNT]);
    registers.as_mut_slice().copy_from_slice(&bytes);
    Ok(registers)
}

fn merge_histogram_hll_ndv_registers(
    dst: &mut HistogramBucketHllState,
    src: &[u8; HLL_REGISTERS_COUNT],
) {
    if let Some(explicit_hashes) = dst.explicit_hashes.take() {
        for hash_value in explicit_hashes {
            update_bucket_register_only(dst, hash_value);
        }
    }
    let dst_registers = ensure_bucket_registers(dst);
    for (dst_register, src_register) in dst_registers.iter_mut().zip(src.iter()) {
        if *dst_register < *src_register {
            *dst_register = *src_register;
        }
    }
}

fn parse_histogram_hll_ndv_buckets(
    bucket_json: &str,
    value_type: &DataType,
) -> Result<Vec<HistogramHllNdvBucket>, String> {
    let parsed: Value = serde_json::from_str(bucket_json).map_err(|_| {
        "histogram_hll_ndv: can't parse JSON specification of histogram buckets.".to_string()
    })?;
    let items = parsed
        .as_array()
        .ok_or_else(|| "histogram_hll_ndv bucket json must be an array".to_string())?;
    let mut buckets = Vec::with_capacity(items.len());
    for item in items {
        let fields = item
            .as_array()
            .ok_or_else(|| "histogram_hll_ndv bucket entry must be an array".to_string())?;
        if !(fields.len() == 4 || fields.len() == 5) {
            return Err("histogram_hll_ndv bucket entry expects 4 or 5 fields".to_string());
        }

        let lower_text = json_bucket_field_as_str(&fields[0], 0)?;
        let upper_text = json_bucket_field_as_str(&fields[1], 1)?;
        let count = json_bucket_field_as_i64(&fields[2], 2)?;
        let upper_repeats = json_bucket_field_as_i64(&fields[3], 3)?;
        let lower = parse_histogram_bucket_scalar(lower_text, value_type)?;
        let upper = parse_histogram_bucket_scalar(upper_text, value_type)?;
        if compare_scalar_values(&lower, &upper)? == Ordering::Greater {
            return Err("histogram_hll_ndv bucket lower bound exceeds upper bound".to_string());
        }
        buckets.push(HistogramHllNdvBucket {
            lower,
            upper,
            count,
            upper_repeats,
        });
    }
    Ok(buckets)
}

fn json_bucket_field_as_str<'a>(value: &'a Value, index: usize) -> Result<&'a str, String> {
    value
        .as_str()
        .ok_or_else(|| format!("histogram_hll_ndv bucket field {index} must be string"))
}

fn json_bucket_field_as_i64(value: &Value, index: usize) -> Result<i64, String> {
    match value {
        Value::String(text) => text
            .trim()
            .parse::<i64>()
            .map_err(|e| format!("histogram_hll_ndv bucket field {index} parse failed: {e}")),
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| format!("histogram_hll_ndv bucket field {index} must fit in i64")),
        _ => Err(format!(
            "histogram_hll_ndv bucket field {index} must be string or number"
        )),
    }
}

fn parse_histogram_bucket_date(text: &str) -> Result<i32, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE_LITERAL '{}'", text))
}

fn parse_histogram_bucket_decimal(text: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {}", scale));
    }
    let mut s = text.trim();
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", text));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", text));
    }
    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", text));
    }
    let scale = scale as usize;
    if frac_part.len() > scale {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds scale {}",
            text, scale
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale - frac_part.len()) {
        digits.push('0');
    }
    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            text, precision
        ));
    }
    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL_LITERAL '{}'", text))?;
    Ok(unsigned.saturating_mul(sign))
}

fn parse_histogram_bucket_decimal256(text: &str, precision: u8, scale: i8) -> Result<i256, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {}", scale));
    }
    let mut s = text.trim();
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut negative = false;
    if let Some(rest) = s.strip_prefix('-') {
        negative = true;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL_LITERAL".to_string());
    }
    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", text));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", text));
    }
    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL_LITERAL '{}'", text));
    }
    let scale = scale as usize;
    if frac_part.len() > scale {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds scale {}",
            text, scale
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale - frac_part.len()) {
        digits.push('0');
    }
    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL_LITERAL '{}' exceeds precision {}",
            text, precision
        ));
    }

    let ten = i256::from_i128(10);
    let mut out = i256::ZERO;
    for ch in digits_final.bytes() {
        let digit = (ch - b'0') as i128;
        out = out
            .checked_mul(ten)
            .and_then(|v| v.checked_add(i256::from_i128(digit)))
            .ok_or_else(|| format!("failed to parse DECIMAL_LITERAL '{}'", text))?;
    }
    if negative {
        out = out
            .checked_neg()
            .ok_or_else(|| format!("failed to parse DECIMAL_LITERAL '{}'", text))?;
    }
    Ok(out)
}

fn parse_histogram_bucket_scalar(
    text: &str,
    data_type: &DataType,
) -> Result<AggScalarValue, String> {
    let text = text.trim();
    match data_type {
        DataType::Boolean => parse_histogram_bucket_bool(text).map(AggScalarValue::Bool),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            text.parse::<i64>().map(AggScalarValue::Int64).map_err(|e| {
                format!("histogram_hll_ndv failed to parse integer bucket value '{text}': {e}")
            })
        }
        DataType::Float32 | DataType::Float64 => text
            .parse::<f64>()
            .map(AggScalarValue::Float64)
            .map_err(|e| {
                format!("histogram_hll_ndv failed to parse float bucket value '{text}': {e}")
            }),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            Ok(AggScalarValue::Utf8(text.to_string()))
        }
        DataType::Date32 => Ok(AggScalarValue::Date32(parse_histogram_bucket_date(text)?)),
        DataType::Timestamp(unit, _) => Ok(AggScalarValue::Timestamp(
            parse_histogram_bucket_timestamp(text, unit)?,
        )),
        DataType::Decimal128(precision, scale) => Ok(AggScalarValue::Decimal128(
            parse_histogram_bucket_decimal(text, *precision, *scale as i8)?,
        )),
        DataType::Decimal256(precision, scale) => Ok(AggScalarValue::Decimal256(
            parse_histogram_bucket_decimal256(text, *precision, *scale as i8)?,
        )),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => Ok(
            AggScalarValue::Decimal128(text.parse::<i128>().map_err(|e| {
                format!("histogram_hll_ndv failed to parse LARGEINT bucket value '{text}': {e}")
            })?),
        ),
        other => Err(format!(
            "histogram_hll_ndv does not support bucket type {:?}",
            other
        )),
    }
}

fn parse_histogram_bucket_bool(text: &str) -> Result<bool, String> {
    match text.to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => Err(format!(
            "histogram_hll_ndv failed to parse boolean bucket value '{}'",
            text
        )),
    }
}

fn parse_histogram_bucket_timestamp(text: &str, unit: &TimeUnit) -> Result<i64, String> {
    let parsed = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| {
            format!("histogram_hll_ndv failed to parse timestamp bucket value '{text}': {e}")
        })?;
    let utc = parsed.and_utc();
    let seconds = utc.timestamp();
    let nanos = utc.timestamp_subsec_nanos() as i64;
    Ok(match unit {
        TimeUnit::Second => seconds,
        TimeUnit::Millisecond => seconds
            .saturating_mul(1_000)
            .saturating_add(nanos / 1_000_000),
        TimeUnit::Microsecond => seconds
            .saturating_mul(1_000_000)
            .saturating_add(nanos / 1_000),
        TimeUnit::Nanosecond => seconds.saturating_mul(1_000_000_000).saturating_add(nanos),
    })
}

fn find_histogram_hll_ndv_bucket(
    buckets: &[HistogramHllNdvBucket],
    value: &AggScalarValue,
) -> Result<Option<usize>, String> {
    let mut left = 0usize;
    let mut right = buckets.len();
    while left < right {
        let mid = left + (right - left) / 2;
        match compare_scalar_values(value, &buckets[mid].upper)? {
            Ordering::Greater => left = mid + 1,
            Ordering::Less | Ordering::Equal => right = mid,
        }
    }
    if left >= buckets.len() {
        return Ok(None);
    }
    if compare_scalar_values(value, &buckets[left].lower)? == Ordering::Less {
        return Ok(None);
    }
    Ok(Some(left))
}

fn ensure_bucket_registers(state: &mut HistogramBucketHllState) -> &mut [u8; HLL_REGISTERS_COUNT] {
    state
        .registers
        .get_or_insert_with(|| Box::new([0u8; HLL_REGISTERS_COUNT]))
}

fn update_bucket_register_only(state: &mut HistogramBucketHllState, hash_value: u64) {
    let registers = ensure_bucket_registers(state);
    let idx = (hash_value % HLL_REGISTERS_COUNT as u64) as usize;
    let mut shifted = hash_value >> HLL_COLUMN_PRECISION;
    shifted |= 1_u64 << (64 - HLL_COLUMN_PRECISION);
    let rank = shifted.trailing_zeros() as u8 + 1;
    if registers[idx] < rank {
        registers[idx] = rank;
    }
}

fn update_bucket_register_from_hash(state: &mut HistogramBucketHllState, hash_value: u64) {
    if hash_value == 0 {
        return;
    }
    if let Some(explicit) = state.explicit_hashes.as_mut() {
        if explicit.contains(&hash_value) {
            return;
        }
        if explicit.len() < HLL_EXPLICIT_INT64_NUM {
            explicit.push(hash_value);
            return;
        }
        let existing = state.explicit_hashes.take().unwrap_or_default();
        for existing_hash in existing {
            update_bucket_register_only(state, existing_hash);
        }
    }
    update_bucket_register_only(state, hash_value);
}

fn estimate_bucket_cardinality(state: &HistogramBucketHllState) -> i64 {
    if let Some(explicit) = state.explicit_hashes.as_ref() {
        return explicit.len() as i64;
    }
    let Some(registers) = state.registers.as_ref() else {
        return 0;
    };

    let num_streams = HLL_REGISTERS_COUNT as f64;
    let alpha = match HLL_REGISTERS_COUNT {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / num_streams),
    };

    let mut harmonic_mean = 0.0f64;
    let mut zero_registers = 0usize;
    for register in registers.iter() {
        harmonic_mean += 2_f64.powi(-(*register as i32));
        if *register == 0 {
            zero_registers += 1;
        }
    }

    if harmonic_mean == 0.0 {
        return 0;
    }

    let mut estimate = alpha * num_streams * num_streams / harmonic_mean;
    if estimate <= num_streams * 2.5 && zero_registers != 0 {
        estimate = num_streams * (num_streams / zero_registers as f64).ln();
    } else if HLL_REGISTERS_COUNT == 16 * 1024 && estimate < 72_000.0 {
        let bias = 5.9119e-18 * estimate.powi(4) - 1.4253e-12 * estimate.powi(3)
            + 1.2940e-7 * estimate.powi(2)
            - 5.2921e-3 * estimate
            + 83.3216;
        estimate -= estimate * (bias / 100.0);
    }

    estimate.max(0.0).round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    fn gen_values() -> Vec<i64> {
        let mut values = Vec::new();
        for i in 0..1024 {
            if i == 100 {
                values.push(100);
            }
            if i == 200 {
                values.push(200);
            }
            values.push(i);
        }
        values
    }

    fn run_histogram_test(values: Vec<i64>, with_ndv: bool) -> String {
        let len = values.len();
        let value_arr = Arc::new(Int64Array::from(values)) as ArrayRef;
        let bucket_arr = Arc::new(Int32Array::from(vec![10; len])) as ArrayRef;
        let ratio_arr = Arc::new(Float64Array::from(vec![1.0; len])) as ArrayRef;

        let mut fields = vec![
            Field::new("f0", DataType::Int64, true),
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Float64, true),
        ];
        let mut arrays: Vec<ArrayRef> = vec![value_arr, bucket_arr, ratio_arr];

        if with_ndv {
            let estimator_arr = Arc::new(StringArray::from(vec![Some("DUJ1"); len])) as ArrayRef;
            fields.push(Field::new("f3", DataType::Utf8, true));
            arrays.push(estimator_arr);
        }

        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), arrays, None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;

        let func = AggFunction {
            name: "histogram".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Utf8),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Int64),
            }),
        };

        let spec = HistogramAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<HistogramState>::uninit();
        HistogramAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; len];
        let input = AggInputView::Any(&array_ref);

        HistogramAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = HistogramAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        HistogramAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        out_arr.value(0).to_string()
    }

    fn run_histogram_hll_ndv_test(values: Vec<i64>, bucket_json: &str) -> String {
        let len = values.len();
        let value_arr = Arc::new(Int64Array::from(values)) as ArrayRef;
        let bucket_arr = Arc::new(StringArray::from(vec![Some(bucket_json); len])) as ArrayRef;

        let fields = vec![
            Field::new("f0", DataType::Int64, true),
            Field::new("f1", DataType::Utf8, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![value_arr, bucket_arr], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;

        let func = AggFunction {
            name: "histogram_hll_ndv".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Utf8),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Int64),
            }),
        };

        let spec = HistogramHllNdvAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<HistogramHllNdvState>::uninit();
        HistogramHllNdvAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; len];
        let input = AggInputView::Any(&array_ref);

        HistogramHllNdvAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = HistogramHllNdvAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        HistogramHllNdvAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        out_arr.value(0).to_string()
    }

    #[test]
    fn histogram_empty_state_returns_empty_json() {
        let func = AggFunction {
            name: "histogram".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Utf8),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Int64),
            }),
        };
        let input_type = DataType::Struct(Fields::from(vec![
            Field::new("f0", DataType::Int64, true),
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Float64, true),
        ]));
        let spec = HistogramAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let mut state = MaybeUninit::<HistogramState>::uninit();
        HistogramAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let out = HistogramAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        HistogramAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out_arr.value(0), "[]");
    }

    #[test]
    fn test_histogram_without_ndv() {
        let actual = run_histogram_test(gen_values(), false);
        let expected = r#"[["0","100","102","2"],["101","201","204","1"],["202","303","306","1"],["304","405","408","1"],["406","507","510","1"],["508","609","612","1"],["610","711","714","1"],["712","813","816","1"],["814","915","918","1"],["916","1017","1020","1"],["1018","1023","1026","1"]]"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_histogram_with_ndv() {
        let actual = run_histogram_test(gen_values(), true);
        let expected = r#"[["0","100","102","2","101"],["101","201","204","1","101"],["202","303","306","1","102"],["304","405","408","1","102"],["406","507","510","1","102"],["508","609","612","1","102"],["610","711","714","1","102"],["712","813","816","1","102"],["814","915","918","1","102"],["916","1017","1020","1","102"],["1018","1023","1026","1","6"]]"#;
        assert_eq!(actual, expected);
    }

    #[test]
    fn histogram_hll_ndv_matches_starrocks_bigint_behavior() {
        let bucket_json = concat!(
            "[[\"0\",\"100\",\"102\",\"2\"],[\"101\",\"201\",\"204\",\"1\"],",
            "[\"202\",\"303\",\"306\",\"1\"],[\"304\",\"405\",\"408\",\"1\"],",
            "[\"406\",\"507\",\"510\",\"1\"],[\"508\",\"609\",\"612\",\"1\"],",
            "[\"610\",\"711\",\"714\",\"1\"],[\"712\",\"813\",\"816\",\"1\"],",
            "[\"814\",\"915\",\"918\",\"1\"],[\"916\",\"1017\",\"1020\",\"1\"],",
            "[\"1018\",\"1023\",\"1026\",\"1\"]]"
        );

        let actual = run_histogram_hll_ndv_test(gen_values(), bucket_json);
        let expected = concat!(
            "[[\"0\",\"100\",\"102\",\"2\",\"101\"],[\"101\",\"201\",\"204\",\"1\",\"101\"],",
            "[\"202\",\"303\",\"306\",\"1\",\"102\"],[\"304\",\"405\",\"408\",\"1\",\"102\"],",
            "[\"406\",\"507\",\"510\",\"1\",\"102\"],[\"508\",\"609\",\"612\",\"1\",\"102\"],",
            "[\"610\",\"711\",\"714\",\"1\",\"102\"],[\"712\",\"813\",\"816\",\"1\",\"102\"],",
            "[\"814\",\"915\",\"918\",\"1\",\"102\"],[\"916\",\"1017\",\"1020\",\"1\",\"102\"],",
            "[\"1018\",\"1023\",\"1026\",\"1\",\"6\"]]"
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn histogram_hll_ndv_merge_intermediate_matches_single_stage() {
        let bucket_json = concat!(
            "[[\"0\",\"100\",\"102\",\"2\"],[\"101\",\"201\",\"204\",\"1\"],",
            "[\"202\",\"303\",\"306\",\"1\"],[\"304\",\"405\",\"408\",\"1\"],",
            "[\"406\",\"507\",\"510\",\"1\"],[\"508\",\"609\",\"612\",\"1\"],",
            "[\"610\",\"711\",\"714\",\"1\"],[\"712\",\"813\",\"816\",\"1\"],",
            "[\"814\",\"915\",\"918\",\"1\"],[\"916\",\"1017\",\"1020\",\"1\"],",
            "[\"1018\",\"1023\",\"1026\",\"1\"]]"
        );

        let len = gen_values().len();
        let left = gen_values().into_iter().take(len / 2).collect::<Vec<_>>();
        let right = gen_values().into_iter().skip(len / 2).collect::<Vec<_>>();

        let expected = run_histogram_hll_ndv_test(gen_values(), bucket_json);

        let func = AggFunction {
            name: "histogram_hll_ndv".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Utf8),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Int64),
            }),
        };
        let raw_input_type = DataType::Struct(Fields::from(vec![
            Field::new("f0", DataType::Int64, true),
            Field::new("f1", DataType::Utf8, true),
        ]));
        let raw_spec = HistogramHllNdvAgg
            .build_spec_from_type(&func, Some(&raw_input_type), false)
            .unwrap();

        let mut left_state = MaybeUninit::<HistogramHllNdvState>::uninit();
        let mut right_state = MaybeUninit::<HistogramHllNdvState>::uninit();
        HistogramHllNdvAgg.init_state(&raw_spec, left_state.as_mut_ptr() as *mut u8);
        HistogramHllNdvAgg.init_state(&raw_spec, right_state.as_mut_ptr() as *mut u8);
        let left_state_ptr = left_state.as_mut_ptr() as AggStatePtr;
        let right_state_ptr = right_state.as_mut_ptr() as AggStatePtr;

        for (values, state_ptr) in [(&left, left_state_ptr), (&right, right_state_ptr)] {
            let row_count = values.len();
            let value_arr = Arc::new(Int64Array::from(values.clone())) as ArrayRef;
            let bucket_arr =
                Arc::new(StringArray::from(vec![Some(bucket_json); row_count])) as ArrayRef;
            let struct_arr = StructArray::new(
                Fields::from(vec![
                    Field::new("f0", DataType::Int64, true),
                    Field::new("f1", DataType::Utf8, true),
                ]),
                vec![value_arr, bucket_arr],
                None,
            );
            let array_ref = Arc::new(struct_arr) as ArrayRef;
            let input = AggInputView::Any(&array_ref);
            HistogramHllNdvAgg
                .update_batch(&raw_spec, 0, &vec![state_ptr; row_count], &input)
                .unwrap();
        }

        let intermediate = HistogramHllNdvAgg
            .build_array(&raw_spec, 0, &[left_state_ptr, right_state_ptr], true)
            .unwrap();
        let intermediate_arr = intermediate.as_any().downcast_ref::<StringArray>().unwrap();
        let merge_input = Arc::new(StringArray::from(vec![
            Some(intermediate_arr.value(0)),
            Some(intermediate_arr.value(1)),
        ])) as ArrayRef;

        let merge_spec = HistogramHllNdvAgg
            .build_spec_from_type(&func, Some(&DataType::Utf8), true)
            .unwrap();
        let mut merged_state = MaybeUninit::<HistogramHllNdvState>::uninit();
        HistogramHllNdvAgg.init_state(&merge_spec, merged_state.as_mut_ptr() as *mut u8);
        let merged_state_ptr = merged_state.as_mut_ptr() as AggStatePtr;
        HistogramHllNdvAgg
            .merge_batch(
                &merge_spec,
                0,
                &[merged_state_ptr, merged_state_ptr],
                &AggInputView::Any(&merge_input),
            )
            .unwrap();
        let merged = HistogramHllNdvAgg
            .build_array(&merge_spec, 0, &[merged_state_ptr], false)
            .unwrap();

        HistogramHllNdvAgg.drop_state(&raw_spec, left_state.as_mut_ptr() as *mut u8);
        HistogramHllNdvAgg.drop_state(&raw_spec, right_state.as_mut_ptr() as *mut u8);
        HistogramHllNdvAgg.drop_state(&merge_spec, merged_state.as_mut_ptr() as *mut u8);

        let merged_arr = merged.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(merged_arr.value(0), expected);
    }
}
