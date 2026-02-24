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
use arrow::array::{ArrayRef, StringArray, StringBuilder, StructArray};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, compare_scalar_values, scalar_from_array, scalar_to_string};
use std::sync::Arc;

pub(super) struct HistogramAgg;

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
            let bucket_num = state
                .bucket_num
                .ok_or_else(|| "histogram bucket_num missing".to_string())?;
            let sample_ratio = state
                .sample_ratio
                .ok_or_else(|| "histogram sample_ratio missing".to_string())?;

            if state.values.is_empty() {
                builder.append_value("[]");
                continue;
            }

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
}
