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
use crate::KernelFailure;
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

/// Borrowed source extents, without inspecting samples or serialized headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MannStateExtents {
    pub(crate) sample_lengths: [usize; 2],
    pub(crate) sample_capacities: [usize; 2],
    pub(crate) sorted: bool,
    pub(crate) initialized: bool,
}

/// Reached allocation frontiers of the ONE original algorithm. Events borrow
/// facts only; an observer must retain its loans outside an escaping error.
pub(crate) enum MannResourceEvent<'a> {
    StaticDiagnostic {
        message: &'static str,
        treatment_replacement: bool,
    },
    IntSetupDiagnostic {
        data_type: &'a DataType,
        treatment_replacement: bool,
    },
    FirstAlternative {
        source: &'a str,
    },
    ScalarRead {
        array: &'a ArrayRef,
        row: usize,
    },
    MergeScratch {
        left: MannStateExtents,
        right: MannStateExtents,
    },
}
pub(crate) trait MannResources {
    fn before(
        &mut self,
        event: MannResourceEvent<'_>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure>;
}
struct NoopMannResources;
impl MannResources for NoopMannResources {
    fn before(
        &mut self,
        _: MannResourceEvent<'_>,
        _: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
}
fn static_error<O: MannResources>(
    message: &'static str,
    treatment_replacement: bool,
    work: &mut ScalarWork<'_, '_>,
    resources: &mut O,
) -> ScalarStateError {
    match resources.before(
        MannResourceEvent::StaticDiagnostic {
            message,
            treatment_replacement,
        },
        work,
    ) {
        Ok(()) => message.to_string().into(),
        Err(cause) => cause.into(),
    }
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

    pub(crate) fn extents(&self) -> MannStateExtents {
        MannStateExtents {
            sample_lengths: self.stats.each_ref().map(|stats| stats.len()),
            sample_capacities: self.stats.each_ref().map(|stats| stats.capacity()),
            sorted: self.sorted,
            initialized: !self.is_uninitialized(),
        }
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

    fn sort_if_needed(&mut self, work: &mut ScalarWork<'_, '_>) -> Result<(), ScalarStateError> {
        if !self.sorted {
            for stats in &mut self.stats {
                work.flush()?;
                stats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                work.flush()?;
            }
            self.sorted = true;
        }
        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<(), ScalarStateError> {
        self.merge_with_work(other, &mut ScalarWork::new(None))
    }

    pub fn merge_with_work(
        &mut self,
        other: Self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        self.merge_with_resources(other, work, &mut NoopMannResources)
    }

    pub(crate) fn merge_with_resources<O: MannResources>(
        &mut self,
        mut other: Self,
        work: &mut ScalarWork<'_, '_>,
        resources: &mut O,
    ) -> Result<(), ScalarStateError> {
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
            return Err(static_error(
                "mann_whitney_u_test state parameters mismatch",
                false,
                work,
                resources,
            ));
        }
        resources.before(
            MannResourceEvent::MergeScratch {
                left: self.extents(),
                right: other.extents(),
            },
            work,
        )?;
        self.sort_if_needed(work)?;
        other.sort_if_needed(work)?;
        for idx in 0..2 {
            let capacity = self.stats[idx]
                .len()
                .checked_add(other.stats[idx].len())
                .ok_or_else(|| {
                    static_error(
                        "mann_whitney_u_test sample count overflow",
                        false,
                        work,
                        resources,
                    )
                })?;
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
                work.step()?;
                if left[i] <= right[j] {
                    merged.push(left[i]);
                    i += 1;
                } else {
                    merged.push(right[j]);
                    j += 1;
                }
            }
            for value in &left[i..] {
                work.step()?;
                merged.push(*value);
            }
            for value in &right[j..] {
                work.step()?;
                merged.push(*value);
            }
            self.stats[idx] = merged;
        }
        self.sorted = true;
        work.flush()?;
        Ok(())
    }

    pub fn serialize(&mut self) -> Vec<u8> {
        self.serialize_with_work(&mut ScalarWork::new(None))
            .expect("unobserved Mann-Whitney serialization cannot fail")
    }

    pub fn serialize_with_work(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Vec<u8>, ScalarStateError> {
        self.sort_if_needed(work)?;
        let mut out = Vec::new();
        out.push(self.alternative as u8);
        out.extend_from_slice(&self.continuity_correction.to_le_bytes());
        let len0 = self.stats[0].len() as u32;
        let len1 = self.stats[1].len() as u32;
        out.extend_from_slice(&len0.to_le_bytes());
        out.extend_from_slice(&len1.to_le_bytes());
        for v in &self.stats[0] {
            work.step()?;
            out.extend_from_slice(&v.to_le_bytes());
        }
        for v in &self.stats[1] {
            work.step()?;
            out.extend_from_slice(&v.to_le_bytes());
        }
        work.flush()?;
        Ok(out)
    }

    pub fn deserialize_in(data: &[u8], allocator: A) -> Result<Self, ScalarStateError> {
        Self::deserialize_in_with_work(data, allocator, &mut ScalarWork::new(None))
    }

    pub fn deserialize_in_with_work(
        data: &[u8],
        allocator: A,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, ScalarStateError> {
        Self::deserialize_in_with_resources(data, allocator, work, &mut NoopMannResources)
    }

    pub(crate) fn deserialize_in_with_resources<O: MannResources>(
        mut data: &[u8],
        allocator: A,
        work: &mut ScalarWork<'_, '_>,
        resources: &mut O,
    ) -> Result<Self, ScalarStateError> {
        if data.len() < 1 + 8 + 4 + 4 {
            return Err(static_error(
                "mann_whitney_u_test deserialize: buffer too short",
                false,
                work,
                resources,
            ));
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
                return Err(static_error(
                    "mann_whitney_u_test deserialize: buffer too short",
                    false,
                    work,
                    resources,
                ));
            }
            work.step()?;
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
                return Err(static_error(
                    "mann_whitney_u_test deserialize: buffer too short",
                    false,
                    work,
                    resources,
                ));
            }
            work.step()?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[..8]);
            stats1.push(f64::from_le_bytes(bytes));
            data = &data[8..];
        }

        work.flush()?;
        Ok(Self {
            allocator,
            alternative,
            continuity_correction,
            stats: [stats0, stats1],
            sorted: true,
        })
    }

    fn format_number(
        value: f64,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<String, ScalarStateError> {
        work.flush()?;
        let out = if let Some(num) = serde_json::Number::from_f64(value) {
            let mut out = num.to_string();
            if out.ends_with(".0") {
                out.truncate(out.len().saturating_sub(2));
            }
            out
        } else {
            "null".to_string()
        };
        work.flush()?;
        Ok(out)
    }

    pub fn build_result(&mut self) -> String {
        self.build_result_with_work(&mut ScalarWork::new(None))
            .expect("unobserved Mann-Whitney result construction cannot fail")
    }

    pub fn build_result_with_work(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<String, ScalarStateError> {
        if self.is_uninitialized() {
            work.flush()?;
            let out = r#"{"Logical Error": "state not initialized."}"#.to_string();
            work.flush()?;
            return Ok(out);
        }
        self.sort_if_needed(work)?;
        let n1 = self.stats[0].len() as f64;
        let n2 = self.stats[1].len() as f64;
        let size = n1 + n2;
        if size == 0.0 {
            work.flush()?;
            let out = r#"{"Error": "No data in samples."}"#.to_string();
            work.flush()?;
            return Ok(out);
        }

        let mut combined = Vec::with_capacity(size as usize);
        for v in &self.stats[0] {
            work.step()?;
            combined.push((*v, 0usize));
        }
        for v in &self.stats[1] {
            work.step()?;
            combined.push((*v, 1usize));
        }
        work.flush()?;
        combined.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        work.flush()?;

        let mut r1 = 0.0;
        let mut tie_num = 0.0;
        let mut left = 0usize;
        while left < combined.len() {
            work.step()?;
            let mut right = left + 1;
            while right < combined.len() {
                work.step()?;
                if !(combined[right].0 == combined[left].0) {
                    break;
                }
                right += 1;
            }
            let count_equal = (right - left) as f64;
            if count_equal == size {
                work.flush()?;
                let out = r#"{"Error": "All numbers in both samples are identical."}"#.to_string();
                work.flush()?;
                return Ok(out);
            }
            let adjusted = (left as f64 + right as f64 + 1.0) / 2.0;
            tie_num += count_equal.powi(3) - count_equal;
            for (_, sample_id) in &combined[left..right] {
                work.step()?;
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
            work.flush()?;
            let out = format!(r#"{{"Logical Error": "sd({}) is not a valid value."}}"#, sd);
            work.flush()?;
            return Ok(out);
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

        work.flush()?;
        let cdf = normal_cdf(z);
        work.flush()?;
        let p_value = if self.alternative == TestingAlternative::TwoSided {
            2.0 - 2.0 * cdf
        } else {
            1.0 - cdf
        };

        let u2 = Self::format_number(u2, work)?;
        let p_value = Self::format_number(p_value, work)?;
        work.flush()?;
        let out = format!("[{}, {}]", u2, p_value);
        work.flush()?;
        Ok(out)
    }
}

enum IntArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
}

impl<'a> IntArrayView<'a> {
    fn new_with_resources<O: MannResources>(
        array: &'a ArrayRef,
        treatment_replacement: bool,
        work: &mut ScalarWork<'_, '_>,
        resources: &mut O,
    ) -> Result<Self, ScalarStateError> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| {
                    static_error(
                        "failed to downcast to Int64Array",
                        treatment_replacement,
                        work,
                        resources,
                    )
                }),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| {
                    static_error(
                        "failed to downcast to Int32Array",
                        treatment_replacement,
                        work,
                        resources,
                    )
                }),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(|| {
                    static_error(
                        "failed to downcast to Int16Array",
                        treatment_replacement,
                        work,
                        resources,
                    )
                }),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(|| {
                    static_error(
                        "failed to downcast to Int8Array",
                        treatment_replacement,
                        work,
                        resources,
                    )
                }),
            other => {
                resources.before(
                    MannResourceEvent::IntSetupDiagnostic {
                        data_type: other,
                        treatment_replacement,
                    },
                    work,
                )?;
                Err(format!("unsupported int input type: {:?}", other).into())
            }
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
    fn new_with_resources<O: MannResources>(
        array: &'a ArrayRef,
        work: &mut ScalarWork<'_, '_>,
        resources: &mut O,
    ) -> Result<Self, ScalarStateError> {
        if matches!(array.data_type(), DataType::Boolean) {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    static_error("failed to downcast to BooleanArray", false, work, resources)
                })?;
            return Ok(Self::Boolean(arr));
        }
        Ok(Self::Int(
            IntArrayView::new_with_resources(array, true, work, resources).map_err(|error| {
                match error {
                    // Only the original integer data error receives the original
                    // replacement. An observer's typed refusal remains primary.
                    ScalarStateError::Legacy(_) => {
                        "mann_whitney_u_test expects boolean or integer treatment"
                            .to_string()
                            .into()
                    }
                    other => other,
                }
            })?,
        ))
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
        let channels = std::array::from_fn(|index| arguments.get(index));
        match Self::try_new_channels_with_work(
            channels,
            arguments.len(),
            &mut ScalarWork::new(None),
        ) {
            Ok(input) => Ok(input),
            Err(ScalarStateError::Legacy(message)) => Err(message),
            Err(_) => unreachable!("unobserved Mann-Whitney input cannot fail control"),
        }
    }

    pub fn try_new_channels_with_work(
        channels: [Option<&'a ArrayRef>; 4],
        arity: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, ScalarStateError> {
        Self::try_new_channels_with_resources(channels, arity, work, &mut NoopMannResources)
    }

    pub(crate) fn try_new_channels_with_resources<O: MannResources>(
        channels: [Option<&'a ArrayRef>; 4],
        arity: usize,
        work: &mut ScalarWork<'_, '_>,
        resources: &mut O,
    ) -> Result<Self, ScalarStateError> {
        work.flush()?;
        if !(2..=4).contains(&arity) || channels[..arity.min(4)].iter().any(Option::is_none) {
            return Err(static_error(
                "mann_whitney_u_test expects 2 to 4 arguments",
                false,
                work,
                resources,
            ));
        }
        let values = channels[0].unwrap();
        let treatment =
            TreatmentArrayView::new_with_resources(channels[1].unwrap(), work, resources)?;

        let alternative = if arity >= 3 {
            Some(
                channels[2]
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        static_error(
                            "mann_whitney_u_test expects string alternative",
                            false,
                            work,
                            resources,
                        )
                    })?,
            )
        } else {
            None
        };

        let continuity = if arity == 4 {
            Some(IntArrayView::new_with_resources(
                channels[3].unwrap(),
                false,
                work,
                resources,
            )?)
        } else {
            None
        };

        work.flush()?;
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
        self.update_row_with_resources(state, addresses, work, &mut NoopMannResources)
    }

    pub(crate) fn update_row_with_resources<A: ScalarStateAllocator, O: MannResources>(
        &self,
        state: &mut MannWhitneyState<A>,
        addresses: [usize; 4],
        work: &mut ScalarWork<'_, '_>,
        resources: &mut O,
    ) -> Result<(), ScalarStateError> {
        work.step()?;
        if state.is_uninitialized() {
            let mut alternative = TestingAlternative::TwoSided;
            let mut continuity = 1;
            if let Some(alt_arr) = self.alternative
                && !alt_arr.is_null(addresses[2])
            {
                work.flush()?;
                resources.before(
                    MannResourceEvent::FirstAlternative {
                        source: alt_arr.value(addresses[2]),
                    },
                    work,
                )?;
                let alt = alt_arr.value(addresses[2]).to_lowercase();
                work.flush()?;
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
                    return Err(static_error(
                        "Logical Error: continuity_correction must be non-negative.",
                        false,
                        work,
                        resources,
                    ));
                }
                continuity = v;
            }
            state.init(alternative, continuity);
        }

        let Some(treatment) = self.treatment.value_at(addresses[1]) else {
            return Ok(());
        };
        resources.before(
            MannResourceEvent::ScalarRead {
                array: self.values,
                row: addresses[0],
            },
            work,
        )?;
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
    use crate::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
    use allocator_api2::alloc::{AllocError, Allocator, Global};
    use std::{
        alloc::Layout,
        ptr::NonNull,
        sync::{Arc, Mutex},
        time::Duration,
    };

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

    #[derive(Default)]
    struct Control {
        units: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut seen = self.units.lock().unwrap();
            let ordinal = seen.len();
            if let Some((at, _)) = &self.refusal {
                assert!(ordinal <= *at, "callback after the originating refusal");
            }
            seen.push(units);
            match &self.refusal {
                Some((at, cause)) if *at == ordinal => Err(cause.clone()),
                _ => Ok(()),
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            unreachable!()
        }
    }
    fn sample_state(count: usize, start: usize) -> MannWhitneyState<Storage> {
        let arguments: Vec<ArrayRef> = vec![
            Arc::new(arrow_array::Float64Array::from_iter_values(
                (start..start + count).map(|v| v as f64),
            )),
            Arc::new(BooleanArray::from(
                (0..count).map(|v| v % 2 != 0).collect::<Vec<_>>(),
            )),
        ];
        let input = MannWhitneyInput::try_new(&arguments).unwrap();
        let mut state = MannWhitneyState::new(Storage);
        let mut work = ScalarWork::new(None);
        for row in 0..count {
            input.update_row(&mut state, [row; 4], &mut work).unwrap();
        }
        state
    }
    #[derive(Clone, Copy)]
    enum Operation {
        Update,
        Merge,
        Serialize,
        Deserialize,
        Result,
    }
    fn observe_operation(operation: Operation, control: &Control) -> Result<(), ScalarStateError> {
        let mut state = sample_state(440, 0);
        let mut checkpoints = crate::kernel_input::EvaluationCheckpoints::new(control);
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        match operation {
            Operation::Update => {
                let arguments: Vec<ArrayRef> = vec![
                    Arc::new(arrow_array::Float64Array::from_iter_values(
                        (0..440).map(|v| v as f64),
                    )),
                    Arc::new(BooleanArray::from(vec![false; 440])),
                    Arc::new(StringArray::from(vec!["LESS"])),
                ];
                let channels = std::array::from_fn(|index| arguments.get(index));
                let input = MannWhitneyInput::try_new_channels_with_work(channels, 3, &mut work)?;
                let mut state = MannWhitneyState::new(Storage);
                for row in 0..440 {
                    input.update_row(&mut state, [row, row, 0, 0], &mut work)?;
                }
            }
            Operation::Merge => {
                let mut state = sample_state(260, 0);
                state.merge_with_work(sample_state(180, 400), &mut work)?;
            }
            Operation::Serialize => {
                let _ = state.serialize_with_work(&mut work)?;
            }
            Operation::Deserialize => {
                let bytes = state.serialize();
                let _ = MannWhitneyState::deserialize_in_with_work(&bytes, Storage, &mut work)?;
            }
            Operation::Result => {
                let _ = state.build_result_with_work(&mut work)?;
            }
        }
        work.flush()
    }
    #[test]
    fn mann_whitney_control_counts_actual_samples_and_final_false_tie_comparisons() {
        for (operation, expected) in [
            (Operation::Update, 880),
            (Operation::Merge, 440),
            (Operation::Serialize, 440),
            (Operation::Deserialize, 440),
            (Operation::Result, 1759),
        ] {
            let control = Control::default();
            observe_operation(operation, &control).unwrap();
            let units = control.units.lock().unwrap();
            assert_eq!(units.iter().copied().sum::<u32>(), expected);
            assert!(units.iter().any(|units| *units == 256));
            assert!(units.iter().any(|units| *units == 0));
        }
    }
    #[test]
    fn mann_whitney_control_every_actual_callback_keeps_all_seven_causes_without_tail() {
        let diagnostic = KernelDiagnostic::new(&"d".repeat(crate::MAX_ROW_ERROR_MESSAGE_BYTES));
        let causes = [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(diagnostic.clone()),
            KernelFailure::Internal(diagnostic.clone()),
            KernelFailure::Operational(diagnostic),
            KernelFailure::InstanceFailed,
        ];
        for operation in [
            Operation::Update,
            Operation::Merge,
            Operation::Serialize,
            Operation::Deserialize,
            Operation::Result,
        ] {
            let reference = Control::default();
            observe_operation(operation, &reference).unwrap();
            let callbacks = reference.units.lock().unwrap().len();
            for at in 0..callbacks {
                for cause in &causes {
                    let control = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    let actual = observe_operation(operation, &control).unwrap_err();
                    assert!(matches!(actual, ScalarStateError::Kernel(actual) if actual == *cause));
                    assert_eq!(control.units.lock().unwrap().len(), at + 1);
                }
            }
        }
    }
    #[test]
    fn mann_whitney_control_channels_keep_one_validator_and_independent_addresses() {
        let values: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.0]));
        let treatment: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
        let alternative: ArrayRef = Arc::new(StringArray::from(vec!["less"]));
        let continuity: ArrayRef = Arc::new(Int64Array::from(vec![0]));
        let channels = [
            Some(&values),
            Some(&treatment),
            Some(&alternative),
            Some(&continuity),
        ];
        let input =
            MannWhitneyInput::try_new_channels_with_work(channels, 4, &mut ScalarWork::new(None))
                .unwrap();
        let mut state = MannWhitneyState::new(Storage);
        input
            .update_row(&mut state, [1, 0, 0, 0], &mut ScalarWork::new(None))
            .unwrap();
        input
            .update_row(&mut state, [0, 1, 0, 0], &mut ScalarWork::new(None))
            .unwrap();
        assert_eq!(state.extents().sample_lengths, [1, 1]);
        assert!(state.extents().initialized);
        assert!(!state.extents().sorted);
        assert!(
            state
                .extents()
                .sample_capacities
                .into_iter()
                .all(|capacity| capacity >= 1)
        );
        assert_eq!(
            hex::encode(state.serialize()),
            "02000000000000000001000000010000000000000000000040000000000000f03f"
        );
        assert!(state.extents().sorted);
        for (channels, arity) in [
            (channels, 1),
            (channels, 5),
            ([Some(&values), None, None, None], 2),
        ] {
            let error = match MannWhitneyInput::try_new_channels_with_work(
                channels,
                arity,
                &mut ScalarWork::new(None),
            ) {
                Err(error) => error,
                Ok(_) => panic!("invalid channel shape"),
            };
            assert_eq!(
                error.to_string(),
                "mann_whitney_u_test expects 2 to 4 arguments"
            );
        }
    }
    #[test]
    fn mann_whitney_control_original_errors_have_no_optional_footer() {
        let invalid: ArrayRef = Arc::new(StringArray::from(vec!["invalid"]));
        let values: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![1.0]));
        let treatment: ArrayRef = Arc::new(BooleanArray::from(vec![None]));
        let args = [values, treatment, invalid];
        let input = MannWhitneyInput::try_new(&args).unwrap();
        let reference = Control::default();
        let mut checkpoints = crate::kernel_input::EvaluationCheckpoints::new(&reference);
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        let error = input
            .update_row(&mut MannWhitneyState::new(Storage), [0; 4], &mut work)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Logical Error: invalid alternative `invalid`."
        );
        let callbacks = reference.units.lock().unwrap().len();
        let control = Control {
            refusal: Some((callbacks, KernelFailure::Cancelled)),
            ..Default::default()
        };
        let mut checkpoints = crate::kernel_input::EvaluationCheckpoints::new(&control);
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        assert_eq!(
            input
                .update_row(&mut MannWhitneyState::new(Storage), [0; 4], &mut work)
                .unwrap_err()
                .to_string(),
            error.to_string()
        );
        assert_eq!(control.units.lock().unwrap().len(), callbacks);
        // Original short codec failure precedes allocation or sample traversal.
        let control = Control {
            refusal: Some((0, KernelFailure::Cancelled)),
            ..Default::default()
        };
        let mut checkpoints = crate::kernel_input::EvaluationCheckpoints::new(&control);
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        assert_eq!(
            MannWhitneyState::deserialize_in_with_work(&[0], Storage, &mut work)
                .unwrap_err()
                .to_string(),
            "mann_whitney_u_test deserialize: buffer too short"
        );
        assert!(control.units.lock().unwrap().is_empty());
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

#[cfg(test)]
#[path = "aggregate_mann_whitney_core/resource_tests.rs"]
mod resource_tests;
