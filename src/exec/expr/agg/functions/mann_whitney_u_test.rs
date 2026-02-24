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
use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, StringArray, StringBuilder, StructArray,
};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, scalar_from_array};
use libm::erf;
use std::cmp::Ordering;
use std::sync::Arc;

pub(super) struct MannWhitneyUTestAgg;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum TestingAlternative {
    Unknown = 0,
    TwoSided = 1,
    Less = 2,
    Greater = 3,
}

impl TestingAlternative {
    fn from_str(value: &str) -> Self {
        match value {
            "two-sided" => TestingAlternative::TwoSided,
            "less" => TestingAlternative::Less,
            "greater" => TestingAlternative::Greater,
            _ => TestingAlternative::Unknown,
        }
    }
}

#[derive(Clone, Debug)]
struct MannWhitneyState {
    alternative: TestingAlternative,
    continuity_correction: i64,
    stats: [Vec<f64>; 2],
    sorted: bool,
}

impl Default for MannWhitneyState {
    fn default() -> Self {
        Self {
            alternative: TestingAlternative::Unknown,
            continuity_correction: 0,
            stats: [Vec::new(), Vec::new()],
            sorted: true,
        }
    }
}

impl MannWhitneyState {
    fn is_uninitialized(&self) -> bool {
        self.alternative == TestingAlternative::Unknown
    }

    fn init(&mut self, alternative: TestingAlternative, continuity_correction: i64) {
        self.alternative = alternative;
        self.continuity_correction = continuity_correction;
    }

    fn update(&mut self, value: f64, treatment: bool) {
        let idx = if treatment { 1 } else { 0 };
        self.stats[idx].push(value);
        self.sorted = false;
    }

    fn sort_if_needed(&mut self) {
        if !self.sorted {
            for stats in &mut self.stats {
                stats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            }
            self.sorted = true;
        }
    }

    fn merge(&mut self, mut other: MannWhitneyState) -> Result<(), String> {
        if self.is_uninitialized() {
            *self = other;
            return Ok(());
        }
        if other.is_uninitialized() {
            return Ok(());
        }
        if self.alternative != other.alternative
            || self.continuity_correction != other.continuity_correction
        {
            return Err("mann_whitney_u_test state parameters mismatch".to_string());
        }
        self.sort_if_needed();
        other.sort_if_needed();
        for idx in 0..2 {
            let mut merged = Vec::with_capacity(self.stats[idx].len() + other.stats[idx].len());
            let mut i = 0;
            let mut j = 0;
            let left = &self.stats[idx];
            let right = &other.stats[idx];
            while i < left.len() && j < right.len() {
                if left[i] <= right[j] {
                    merged.push(left[i]);
                    i += 1;
                } else {
                    merged.push(right[j]);
                    j += 1;
                }
            }
            merged.extend_from_slice(&left[i..]);
            merged.extend_from_slice(&right[j..]);
            self.stats[idx] = merged;
        }
        self.sorted = true;
        Ok(())
    }

    fn serialize(&mut self) -> Vec<u8> {
        self.sort_if_needed();
        let mut out = Vec::new();
        out.push(self.alternative as u8);
        out.extend_from_slice(&self.continuity_correction.to_le_bytes());
        let len0 = self.stats[0].len() as u32;
        let len1 = self.stats[1].len() as u32;
        out.extend_from_slice(&len0.to_le_bytes());
        out.extend_from_slice(&len1.to_le_bytes());
        for v in &self.stats[0] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in &self.stats[1] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    fn deserialize(mut data: &[u8]) -> Result<Self, String> {
        if data.len() < 1 + 8 + 4 + 4 {
            return Err("mann_whitney_u_test deserialize: buffer too short".to_string());
        }
        let alternative = match data[0] {
            1 => TestingAlternative::TwoSided,
            2 => TestingAlternative::Less,
            3 => TestingAlternative::Greater,
            _ => TestingAlternative::Unknown,
        };
        data = &data[1..];
        let mut cc_bytes = [0u8; 8];
        cc_bytes.copy_from_slice(&data[..8]);
        let continuity_correction = i64::from_le_bytes(cc_bytes);
        data = &data[8..];
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&data[..4]);
        let len0 = u32::from_le_bytes(len_bytes) as usize;
        data = &data[4..];
        len_bytes.copy_from_slice(&data[..4]);
        let len1 = u32::from_le_bytes(len_bytes) as usize;
        data = &data[4..];

        let mut stats0 = Vec::with_capacity(len0);
        for _ in 0..len0 {
            if data.len() < 8 {
                return Err("mann_whitney_u_test deserialize: buffer too short".to_string());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[..8]);
            stats0.push(f64::from_le_bytes(bytes));
            data = &data[8..];
        }
        let mut stats1 = Vec::with_capacity(len1);
        for _ in 0..len1 {
            if data.len() < 8 {
                return Err("mann_whitney_u_test deserialize: buffer too short".to_string());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[..8]);
            stats1.push(f64::from_le_bytes(bytes));
            data = &data[8..];
        }

        Ok(Self {
            alternative,
            continuity_correction,
            stats: [stats0, stats1],
            sorted: true,
        })
    }

    fn format_number(value: f64) -> String {
        if let Some(num) = serde_json::Number::from_f64(value) {
            let mut out = num.to_string();
            if out.ends_with(".0") {
                out.truncate(out.len().saturating_sub(2));
            }
            out
        } else {
            "null".to_string()
        }
    }

    fn build_result(&mut self) -> String {
        if self.is_uninitialized() {
            return r#"{"Logical Error": "state not initialized."}"#.to_string();
        }
        self.sort_if_needed();
        let n1 = self.stats[0].len() as f64;
        let n2 = self.stats[1].len() as f64;
        let size = n1 + n2;
        if size == 0.0 {
            return r#"{"Error": "No data in samples."}"#.to_string();
        }

        let mut combined = Vec::with_capacity(size as usize);
        for v in &self.stats[0] {
            combined.push((*v, 0usize));
        }
        for v in &self.stats[1] {
            combined.push((*v, 1usize));
        }
        combined.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let mut r1 = 0.0;
        let mut tie_num = 0.0;
        let mut left = 0usize;
        while left < combined.len() {
            let mut right = left + 1;
            while right < combined.len() && combined[right].0 == combined[left].0 {
                right += 1;
            }
            let count_equal = (right - left) as f64;
            if count_equal == size {
                return r#"{"Error": "All numbers in both samples are identical."}"#.to_string();
            }
            let adjusted = (left as f64 + right as f64 + 1.0) / 2.0;
            tie_num += count_equal.powi(3) - count_equal;
            for idx in left..right {
                if combined[idx].1 == 0 {
                    r1 += adjusted;
                }
            }
            left = right;
        }

        let tie_correction = 1.0 - (tie_num / (size.powi(3) - size));
        let u1 = n1 * n2 + (n1 * (n1 + 1.0)) / 2.0 - r1;
        let u2 = n1 * n2 - u1;
        let meanrank = n1 * n2 / 2.0 + 0.5 * (self.continuity_correction as f64);
        let sd = (tie_correction * n1 * n2 * (n1 + n2 + 1.0) / 12.0).sqrt();
        if !sd.is_finite() || sd.abs() < 1e-7 {
            return format!(r#"{{"Logical Error": "sd({}) is not a valid value."}}"#, sd);
        }

        let u = match self.alternative {
            TestingAlternative::TwoSided => u1.max(u2),
            TestingAlternative::Less => u1,
            TestingAlternative::Greater => u2,
            TestingAlternative::Unknown => u1.max(u2),
        };
        let mut z = (u - meanrank) / sd;
        if self.alternative == TestingAlternative::TwoSided {
            z = z.abs();
        }

        let cdf = normal_cdf(z);
        let p_value = if self.alternative == TestingAlternative::TwoSided {
            2.0 - 2.0 * cdf
        } else {
            1.0 - cdf
        };

        format!(
            "[{}, {}]",
            Self::format_number(u2),
            Self::format_number(p_value)
        )
    }
}

fn mann_whitney_numeric_to_f64(value: &AggScalarValue) -> Option<f64> {
    match value {
        AggScalarValue::Int64(v) => Some(*v as f64),
        AggScalarValue::Float64(v) => Some(*v),
        AggScalarValue::Decimal128(v) => Some(*v as f64),
        _ => None,
    }
}

fn normal_cdf(z: f64) -> f64 {
    0.5 * (1.0 + erf(z / std::f64::consts::SQRT_2))
}

impl AggregateFunction for MannWhitneyUTestAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let sig = super::super::agg_type_signature(func)
            .ok_or_else(|| "mann_whitney_u_test type signature missing".to_string())?;
        let output_type = sig
            .output_type
            .as_ref()
            .ok_or_else(|| "mann_whitney_u_test output type missing".to_string())?;
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .ok_or_else(|| "mann_whitney_u_test intermediate type missing".to_string())?;

        if input_is_intermediate {
            return Ok(AggSpec {
                kind: AggKind::MannWhitneyUTest,
                output_type: output_type.clone(),
                intermediate_type,
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let input_type =
            input_type.ok_or_else(|| "mann_whitney_u_test input type missing".to_string())?;
        let DataType::Struct(fields) = input_type else {
            return Err("mann_whitney_u_test expects struct input".to_string());
        };
        if !(fields.len() == 2 || fields.len() == 3 || fields.len() == 4) {
            return Err("mann_whitney_u_test expects 2 to 4 arguments".to_string());
        }

        Ok(AggSpec {
            kind: AggKind::MannWhitneyUTest,
            output_type: output_type.clone(),
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<MannWhitneyState>(),
            std::mem::align_of::<MannWhitneyState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "mann_whitney_u_test input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "mann_whitney_u_test merge input missing".to_string())?;
        let merge_array = if let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>() {
            struct_arr
                .columns()
                .first()
                .ok_or_else(|| "mann_whitney_u_test merge struct input has no fields".to_string())?
        } else {
            arr
        };
        let bin = merge_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut MannWhitneyState, MannWhitneyState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MannWhitneyState);
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
            return Err("mann_whitney_u_test input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "mann_whitney_u_test expects struct input".to_string())?;
        if !(struct_arr.num_columns() == 2
            || struct_arr.num_columns() == 3
            || struct_arr.num_columns() == 4)
        {
            return Err("mann_whitney_u_test expects 2 to 4 arguments".to_string());
        }

        let value_arr = struct_arr.column(0);
        let treatment_arr = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "mann_whitney_u_test expects boolean treatment".to_string())?;

        let alternative_arr = if struct_arr.num_columns() >= 3 {
            Some(
                struct_arr
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "mann_whitney_u_test expects string alternative".to_string())?,
            )
        } else {
            None
        };

        let continuity_arr = if struct_arr.num_columns() == 4 {
            Some(IntArrayView::new(struct_arr.column(3))?)
        } else {
            None
        };

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
            if state.is_uninitialized() {
                let mut alternative = TestingAlternative::TwoSided;
                let mut continuity = 1;
                if let Some(alt_arr) = alternative_arr {
                    if !alt_arr.is_null(row) {
                        let alt = alt_arr.value(row).to_lowercase();
                        let parsed = TestingAlternative::from_str(&alt);
                        if parsed == TestingAlternative::Unknown {
                            return Err(format!("Logical Error: invalid alternative `{}`.", alt));
                        }
                        alternative = parsed;
                    }
                }
                if let Some(cont_arr) = continuity_arr.as_ref() {
                    if let Some(v) = cont_arr.value_at(row) {
                        if v < 0 {
                            return Err(
                                "Logical Error: continuity_correction must be non-negative."
                                    .to_string(),
                            );
                        }
                        continuity = v;
                    }
                }
                state.init(alternative, continuity);
            }

            if treatment_arr.is_null(row) {
                continue;
            }
            let Some(value) = scalar_from_array(value_arr, row)? else {
                continue;
            };
            let Some(x) = mann_whitney_numeric_to_f64(&value) else {
                return Err("mann_whitney_u_test expects numeric input".to_string());
            };
            if x.is_nan() || x.is_infinite() {
                continue;
            }
            let treatment = treatment_arr.value(row);
            state.update(x, treatment);
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
        let AggInputView::Binary(arr) = input else {
            return Err("mann_whitney_u_test merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            let other = MannWhitneyState::deserialize(bytes)?;
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
            if state.is_uninitialized() {
                *state = other;
            } else {
                state.merge(other)?;
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
                let bytes = state.serialize();
                builder.append_value(bytes);
            }
            return Ok(Arc::new(builder.finish()));
        }

        let mut builder = StringBuilder::new();
        for &base in group_states {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MannWhitneyState) };
            if state.is_uninitialized() || (state.stats[0].is_empty() && state.stats[1].is_empty())
            {
                builder.append_null();
                continue;
            }
            let value = state.build_result();
            builder.append_value(value);
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    #[test]
    fn test_mann_whitney_simple() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
        let treatment = Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef;

        let fields = vec![
            Field::new("f0", DataType::Float64, true),
            Field::new("f1", DataType::Boolean, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values, treatment], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;

        let func = AggFunction {
            name: "mann_whitney_u_test".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Float64),
            }),
        };

        let spec = MannWhitneyUTestAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<MannWhitneyState>::uninit();
        MannWhitneyUTestAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        let input = AggInputView::Any(&array_ref);

        MannWhitneyUTestAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = MannWhitneyUTestAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MannWhitneyUTestAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        let json_val: serde_json::Value = serde_json::from_str(out_arr.value(0)).unwrap();
        let arr = json_val.as_array().unwrap();
        let u2 = arr[0].as_f64().unwrap();
        let p = arr[1].as_f64().unwrap();
        assert!((u2 - 0.0).abs() < 1e-9);
        assert!((p - 1.0).abs() < 1e-9);
    }
}
