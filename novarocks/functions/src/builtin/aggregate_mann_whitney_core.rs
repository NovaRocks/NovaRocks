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
//! The original Mann–Whitney state, sample transitions, codec and formatter.
//! Callers supply storage; this module owns no execution context or tracker.
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork, scalar_from_array,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow_schema::DataType;
use libm::erf;
use std::cmp::Ordering;

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

#[derive(Debug)]
pub struct MannWhitneyState<A: ScalarStateAllocator> {
    pub allocator: A,
    alternative: TestingAlternative,
    continuity_correction: i64,
    stats: [ScalarVec<f64, A>; 2],
    sorted: bool,
}

impl<A: ScalarStateAllocator> MannWhitneyState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            alternative: TestingAlternative::Unknown,
            continuity_correction: 0,
            stats: [
                ScalarVec::new_in(allocator.clone()),
                ScalarVec::new_in(allocator),
            ],
            sorted: true,
        }
    }
    pub fn is_uninitialized(&self) -> bool {
        self.alternative == TestingAlternative::Unknown
    }

    pub fn has_samples(&self) -> bool {
        !self.stats[0].is_empty() || !self.stats[1].is_empty()
    }

    fn init(&mut self, alternative: TestingAlternative, continuity_correction: i64) {
        self.alternative = alternative;
        self.continuity_correction = continuity_correction;
    }

    fn update(&mut self, value: f64, treatment: bool) -> Result<(), ScalarStateError> {
        let idx = if treatment { 1 } else { 0 };
        self.stats[idx].try_reserve(1).map_err(|_| {
            self.allocator
                .scalar_allocation_error("reserve Mann-Whitney sample")
        })?;
        self.stats[idx].push(value);
        self.sorted = false;
        Ok(())
    }

    fn sort_if_needed(&mut self) {
        if !self.sorted {
            for stats in &mut self.stats {
                stats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            }
            self.sorted = true;
        }
    }

    pub fn merge(&mut self, mut other: Self) -> Result<(), ScalarStateError> {
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
            return Err("mann_whitney_u_test state parameters mismatch"
                .to_string()
                .into());
        }
        self.sort_if_needed();
        other.sort_if_needed();
        for idx in 0..2 {
            let capacity = self.stats[idx]
                .len()
                .checked_add(other.stats[idx].len())
                .ok_or_else(|| "mann_whitney_u_test sample count overflow".to_string())?;
            let mut merged = ScalarVec::new_in(self.allocator.clone());
            merged.try_reserve_exact(capacity).map_err(|_| {
                self.allocator
                    .scalar_allocation_error("reserve merged Mann-Whitney sample")
            })?;
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

    pub fn serialize(&mut self) -> Vec<u8> {
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

    pub fn deserialize_in(mut data: &[u8], allocator: A) -> Result<Self, ScalarStateError> {
        if data.len() < 1 + 8 + 4 + 4 {
            return Err("mann_whitney_u_test deserialize: buffer too short"
                .to_string()
                .into());
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

        let mut stats0 = ScalarVec::new_in(allocator.clone());
        stats0.try_reserve_exact(len0).map_err(|_| {
            allocator.scalar_allocation_error("reserve decoded Mann-Whitney control sample")
        })?;
        for _ in 0..len0 {
            if data.len() < 8 {
                return Err("mann_whitney_u_test deserialize: buffer too short"
                    .to_string()
                    .into());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[..8]);
            stats0.push(f64::from_le_bytes(bytes));
            data = &data[8..];
        }
        let mut stats1 = ScalarVec::new_in(allocator.clone());
        stats1.try_reserve_exact(len1).map_err(|_| {
            allocator.scalar_allocation_error("reserve decoded Mann-Whitney treatment sample")
        })?;
        for _ in 0..len1 {
            if data.len() < 8 {
                return Err("mann_whitney_u_test deserialize: buffer too short"
                    .to_string()
                    .into());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[..8]);
            stats1.push(f64::from_le_bytes(bytes));
            data = &data[8..];
        }

        Ok(Self {
            allocator,
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

    pub fn build_result(&mut self) -> String {
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
            for (_, sample_id) in &combined[left..right] {
                if *sample_id == 0 {
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

enum IntArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
}

impl<'a> IntArrayView<'a> {
    fn new(array: &'a ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| "failed to downcast to Int64Array".to_string()),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| "failed to downcast to Int32Array".to_string()),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(|| "failed to downcast to Int16Array".to_string()),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(|| "failed to downcast to Int8Array".to_string()),
            other => Err(format!("unsupported int input type: {:?}", other)),
        }
    }

    fn value_at(&self, row: usize) -> Option<i64> {
        match self {
            IntArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            IntArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
        }
    }
}

enum TreatmentArrayView<'a> {
    Boolean(&'a BooleanArray),
    Int(IntArrayView<'a>),
}

impl<'a> TreatmentArrayView<'a> {
    fn new(array: &'a ArrayRef) -> Result<Self, String> {
        if matches!(array.data_type(), DataType::Boolean) {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            return Ok(Self::Boolean(arr));
        }
        Ok(Self::Int(IntArrayView::new(array).map_err(|_| {
            "mann_whitney_u_test expects boolean or integer treatment".to_string()
        })?))
    }

    fn value_at(&self, row: usize) -> Option<bool> {
        match self {
            Self::Boolean(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            Self::Int(arr) => arr.value_at(row).map(|value| value != 0),
        }
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

/// Borrowed evaluated arguments. Each address is supplied by the caller;
/// the legacy packed batch uses one row for every argument.
pub struct MannWhitneyInput<'a> {
    values: &'a ArrayRef,
    treatment: TreatmentArrayView<'a>,
    alternative: Option<&'a StringArray>,
    continuity: Option<IntArrayView<'a>>,
}
impl<'a> MannWhitneyInput<'a> {
    pub fn try_new(arguments: &'a [ArrayRef]) -> Result<Self, String> {
        if !(arguments.len() == 2 || arguments.len() == 3 || arguments.len() == 4) {
            return Err("mann_whitney_u_test expects 2 to 4 arguments".to_string());
        }

        let values = &arguments[0];
        let treatment = TreatmentArrayView::new(&arguments[1])?;

        let alternative = if arguments.len() >= 3 {
            Some(
                arguments[2]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "mann_whitney_u_test expects string alternative".to_string())?,
            )
        } else {
            None
        };

        let continuity = if arguments.len() == 4 {
            Some(IntArrayView::new(&arguments[3])?)
        } else {
            None
        };

        Ok(Self {
            values,
            treatment,
            alternative,
            continuity,
        })
    }
    pub fn update_row<A: ScalarStateAllocator>(
        &self,
        state: &mut MannWhitneyState<A>,
        addresses: [usize; 4],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        if state.is_uninitialized() {
            let mut alternative = TestingAlternative::TwoSided;
            let mut continuity = 1;
            if let Some(alt_arr) = self.alternative
                && !alt_arr.is_null(addresses[2])
            {
                let alt = alt_arr.value(addresses[2]).to_lowercase();
                let parsed = TestingAlternative::from_str(&alt);
                if parsed == TestingAlternative::Unknown {
                    return Err(format!("Logical Error: invalid alternative `{}`.", alt).into());
                }
                alternative = parsed;
            }
            if let Some(cont_arr) = self.continuity.as_ref()
                && let Some(v) = cont_arr.value_at(addresses[3])
            {
                if v < 0 {
                    return Err("Logical Error: continuity_correction must be non-negative."
                        .to_string()
                        .into());
                }
                continuity = v;
            }
            state.init(alternative, continuity);
        }

        let Some(treatment) = self.treatment.value_at(addresses[1]) else {
            return Ok(());
        };
        let Some(value) =
            scalar_from_array(self.values, addresses[0], work).map_err(|error| match error {
                ScalarStateError::Kernel(_) => error,
                other => ScalarStateError::Legacy(other.to_string()),
            })?
        else {
            return Ok(());
        };
        let Some(x) = mann_whitney_numeric_to_f64(&value) else {
            return Err("mann_whitney_u_test expects numeric input"
                .to_string()
                .into());
        };
        if x.is_nan() || x.is_infinite() {
            return Ok(());
        }
        state.update(x, treatment)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::{AllocError, Allocator, Global};
    use std::{alloc::Layout, ptr::NonNull, sync::Arc};

    #[derive(Clone, Debug)]
    struct Storage;
    // SAFETY: this test storage delegates exact blocks and layouts to Global.
    unsafe impl Allocator for Storage {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            Global.allocate(layout)
        }
        unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
            unsafe { Global.deallocate(pointer, layout) };
        }
    }
    impl ScalarStateAllocator for Storage {
        fn scalar_allocation_error(&self, operation: &str) -> ScalarStateError {
            format!("test allocation rejected: {operation}").into()
        }
    }
    #[test]
    fn mann_whitney_core_borrows_independent_argument_addresses() {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.0])),
            Arc::new(BooleanArray::from(vec![false, true])),
            Arc::new(StringArray::from(vec!["less"])),
            Arc::new(Int64Array::from(vec![0])),
        ];
        let input = MannWhitneyInput::try_new(&arguments).unwrap();
        let mut state = MannWhitneyState::new(Storage);
        let mut work = ScalarWork::new(None);
        input
            .update_row(&mut state, [1, 0, 0, 0], &mut work)
            .unwrap();
        input
            .update_row(&mut state, [0, 1, 0, 0], &mut work)
            .unwrap();
        assert_eq!(
            hex::encode(state.serialize()),
            "02000000000000000001000000010000000000000000000040000000000000f03f"
        );
    }
}
