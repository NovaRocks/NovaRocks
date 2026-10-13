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
//! Original N admission, stable sort and scalar codec, over explicit storage.
use super::aggregate_by_core::ByEncodeBuffer;
use crate::aggregate_scalar::{
    self as scalar, AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork,
    TrackedAggScalarValue,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use arrow_buffer::i256;
use arrow_schema::DataType;
use std::cmp::Ordering;

/// Source-qualified consumers provide an owned scope around the original sort.
/// This interface does not itself grant memory or cover other N operations.
pub trait NSortResources {
    type Lease;
    fn before_sort(
        &mut self,
        len: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, crate::KernelFailure>;

    /// A reached comparator may create one original discarded diagnostic.
    /// Default callers retain their original unobserved comparison behavior.
    fn before_comparison(
        &mut self,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<Self::Lease>, crate::KernelFailure> {
        Ok(None)
    }
}
struct NoopSortResources;
impl NSortResources for NoopSortResources {
    type Lease = ();
    fn before_sort(
        &mut self,
        _len: usize,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        Ok(())
    }
}
/// A reached decimal codec call owns its external scope until the original
/// call and its temporary output end. Default callers add no observations.
pub trait NCodecResources {
    type Lease;
    fn before_decimal256_format(
        &mut self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, crate::KernelFailure>;
    fn after_decimal256_format(
        &mut self,
        _text: &str,
        _capacity: usize,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        Ok(())
    }
    fn before_decimal256_parse(
        &mut self,
        text_bytes: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self::Lease, crate::KernelFailure>;
    fn after_decimal256_parse(
        &mut self,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        Ok(())
    }
}
struct NoopCodecResources;
impl NCodecResources for NoopCodecResources {
    type Lease = ();
    fn before_decimal256_format(
        &mut self,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        Ok(())
    }
    fn before_decimal256_parse(
        &mut self,
        _text_bytes: usize,
        _work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), crate::KernelFailure> {
        Ok(())
    }
}
#[derive(Debug)]
pub struct NState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub initialized: bool,
    pub limit: usize,
    pub values: ScalarVec<TrackedAggScalarValue<A>, A>,
    pub failed: bool,
    payload_bytes: usize,
}
impl<A: ScalarStateAllocator> NState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            values: ScalarVec::new_in(allocator.clone()),
            allocator,
            initialized: false,
            limit: 0,
            failed: false,
            payload_bytes: 0,
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<TrackedAggScalarValue<A>>()
            + self.payload_bytes
    }
    pub fn latch_failure(&mut self) {
        self.failed = true;
        self.payload_bytes = 0;
        self.values = ScalarVec::new_in(self.allocator.clone());
    }
    pub fn init_limit_if_needed(&mut self, limit: usize) -> Result<(), ScalarStateError> {
        if !self.initialized {
            self.initialized = true;
            self.limit = limit;
            return Ok(());
        }
        if self.limit != limit {
            return Err(format!(
                "min_n/max_n limit mismatch while merging states: {} vs {}",
                self.limit, limit
            )
            .into());
        }
        Ok(())
    }
    pub fn update_from_arrays(
        &mut self,
        values: &ArrayRef,
        value_row: usize,
        limits: &ArrayRef,
        limit_row: usize,
        keep_smallest: bool,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        self.update_from_arrays_with_sort_resources(
            values,
            value_row,
            limits,
            limit_row,
            keep_smallest,
            &mut NoopSortResources,
            work,
        )
    }
    pub fn update_from_arrays_with_sort_resources<R: NSortResources>(
        &mut self,
        values: &ArrayRef,
        value_row: usize,
        limits: &ArrayRef,
        limit_row: usize,
        keep_smallest: bool,
        resources: &mut R,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let limit = parse_limit(limits, limit_row, work)?;
        self.init_limit_if_needed(limit)?;
        let Some(value) =
            scalar::tracked_scalar_from_array(values, value_row, &self.allocator, work)?
        else {
            return Ok(());
        };
        self.push_value_with_sort_resources(value, keep_smallest, resources, work)
    }
    pub fn merge_from_array(
        &mut self,
        array: &ArrayRef,
        row: usize,
        keep_smallest: bool,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        self.merge_from_array_with_sort_resources(
            array,
            row,
            keep_smallest,
            &mut NoopSortResources,
            work,
        )
    }
    pub fn merge_from_array_with_sort_resources<R: NSortResources>(
        &mut self,
        array: &ArrayRef,
        row: usize,
        keep_smallest: bool,
        resources: &mut R,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        self.merge_from_array_with_resources(
            array,
            row,
            keep_smallest,
            resources,
            &mut NoopCodecResources,
            work,
        )
    }
    pub fn merge_from_array_with_resources<S: NSortResources, C: NCodecResources>(
        &mut self,
        array: &ArrayRef,
        row: usize,
        keep_smallest: bool,
        sort_resources: &mut S,
        codec_resources: &mut C,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        let Some((limit, values)) = decode_payload_at_with_codec_resources(
            array,
            row,
            &self.allocator,
            codec_resources,
            work,
        )?
        else {
            return Ok(());
        };
        if limit == 0 {
            return Ok(());
        }
        self.init_limit_if_needed(limit)?;
        for value in values {
            work.step()?;
            self.push_value_with_sort_resources(value, keep_smallest, sort_resources, work)?;
        }
        Ok(())
    }
    fn sort_values<R: NSortResources>(
        &mut self,
        resources: &mut R,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        work.flush()?;
        let lease = resources.before_sort(self.values.len(), work)?;
        let mut primary = None;
        // Keep the original library stable sort and its error->Equal rule.
        // Only observed host-control errors remain outer typed failures.
        self.values.sort_by(|left, right| {
            if primary.is_some() {
                return Ordering::Equal;
            }
            let comparison_lease = match resources.before_comparison(work) {
                Ok(lease) => lease,
                Err(cause) => {
                    primary = Some(ScalarStateError::Kernel(cause));
                    return Ordering::Equal;
                }
            };
            let ordering = match scalar::compare_tracked_scalar_values(left, right, work) {
                Ok(order) => order,
                Err(ScalarStateError::Legacy(_)) => Ordering::Equal,
                Err(error) => {
                    primary = Some(error);
                    Ordering::Equal
                }
            };
            // The discarded Legacy String has ended with the match statement.
            drop(comparison_lease);
            ordering
        });
        // The library's scratch has ended before the caller's owned scope.
        drop(lease);
        if let Some(error) = primary {
            return Err(error);
        }
        work.flush()?;
        Ok(())
    }
    pub fn push_value(
        &mut self,
        value: TrackedAggScalarValue<A>,
        keep_smallest: bool,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        self.push_value_with_sort_resources(value, keep_smallest, &mut NoopSortResources, work)
    }
    pub fn push_value_with_sort_resources<R: NSortResources>(
        &mut self,
        value: TrackedAggScalarValue<A>,
        keep_smallest: bool,
        resources: &mut R,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        if self.limit == 0 {
            return Ok(());
        }
        work.flush()?;
        self.values.try_reserve(1).map_err(|_| {
            self.values
                .allocator()
                .scalar_allocation_error("reserve min_n/max_n values")
        })?;
        work.flush()?;
        let retained = scalar::tracked_scalar_heap_capacity(&value, work)?;
        self.values.push(value);
        self.payload_bytes = self
            .payload_bytes
            .checked_add(retained)
            .ok_or(crate::KernelFailure::ResourceExhausted)?;
        self.sort_values(resources, work)?;
        if !keep_smallest {
            work.flush()?;
            self.values.reverse();
            work.flush()?;
        }
        if self.values.len() > self.limit {
            for value in &self.values[self.limit..] {
                work.step()?;
                self.payload_bytes -= scalar::tracked_scalar_heap_capacity(value, work)?;
            }
            work.flush()?;
            self.values.truncate(self.limit);
            work.flush()?;
        }
        self.sort_values(resources, work)?;
        Ok(())
    }
    pub fn serialize<B: ByEncodeBuffer>(
        &self,
        out: &mut B,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        self.serialize_with_codec_resources(out, &mut NoopCodecResources, work)
    }
    pub fn serialize_with_codec_resources<B: ByEncodeBuffer, R: NCodecResources>(
        &self,
        out: &mut B,
        resources: &mut R,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        out.append(&(self.limit as u32).to_le_bytes(), work)?;
        out.append(&(self.values.len() as u32).to_le_bytes(), work)?;
        for value in &self.values {
            work.step()?;
            encode_scalar(out, value, resources, work)?;
        }
        Ok(())
    }
    pub fn output(
        &self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<AggScalarValue, ScalarStateError> {
        let mut list = Vec::with_capacity(self.values.len());
        for value in &self.values {
            work.step()?;
            list.push(Some(scalar::tracked_scalar_to_output(value, work)?));
        }
        Ok(AggScalarValue::List(list))
    }
}
pub fn parse_limit(
    array: &ArrayRef,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, ScalarStateError> {
    let Some(value) = scalar::scalar_from_array(array, row, work)? else {
        return Err("min_n/max_n limit cannot be null".to_string().into());
    };
    let AggScalarValue::Int64(value) = value else {
        return Err("min_n/max_n limit must be integer".to_string().into());
    };
    if value <= 0 {
        return Err(format!("min_n/max_n limit must be positive, got {}", value).into());
    }
    usize::try_from(value).map_err(|_| "min_n/max_n limit overflow".to_string().into())
}
pub fn decode_payload_at<A: ScalarStateAllocator>(
    array: &ArrayRef,
    row: usize,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<(usize, ScalarVec<TrackedAggScalarValue<A>, A>)>, ScalarStateError> {
    decode_payload_at_with_codec_resources(array, row, allocator, &mut NoopCodecResources, work)
}
pub fn decode_payload_at_with_codec_resources<A: ScalarStateAllocator, R: NCodecResources>(
    array: &ArrayRef,
    row: usize,
    allocator: &A,
    resources: &mut R,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Option<(usize, ScalarVec<TrackedAggScalarValue<A>, A>)>, ScalarStateError> {
    work.step()?;
    let payload = match array.data_type() {
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "min_n/max_n merge input must be BinaryArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row))
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "min_n/max_n merge input must be StringArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row).as_bytes())
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "min_n/max_n merge input must be LargeBinaryArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "min_n/max_n merge input must be LargeStringArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row).as_bytes())
        }
        other => {
            return Err(format!(
                "min_n/max_n merge input must be binary-like, got {:?}",
                other
            )
            .into());
        }
    };
    let Some(payload) = payload else {
        return Ok(None);
    };

    let mut pos = 0usize;
    let limit = read_u32(payload, &mut pos, "limit")? as usize;
    let count = read_u32(payload, &mut pos, "count")? as usize;
    let mut values = scalar::aggregate_vec_with_capacity(
        allocator,
        count,
        "reserve decoded min_n/max_n values",
        work,
    )?;
    for _ in 0..count {
        work.step()?;
        values.push(decode_scalar(
            payload, &mut pos, allocator, resources, work,
        )?);
    }
    Ok(Some((limit, values)))
}

fn read_u32(bytes: &[u8], pos: &mut usize, label: &str) -> Result<u32, ScalarStateError> {
    need_len(bytes, *pos, 4, label)?;
    let value = u32::from_le_bytes(
        bytes[*pos..*pos + 4]
            .try_into()
            .map_err(|_| format!("min_n/max_n decode {} failed", label))?,
    );
    *pos += 4;
    Ok(value)
}

fn need_len(bytes: &[u8], pos: usize, need: usize, label: &str) -> Result<(), ScalarStateError> {
    if pos + need > bytes.len() {
        return Err(format!(
            "min_n/max_n decode {} overflow: pos={} need={} len={}",
            label,
            pos,
            need,
            bytes.len()
        )
        .into());
    }
    Ok(())
}

fn encode_scalar<A: ScalarStateAllocator, B: ByEncodeBuffer, R: NCodecResources>(
    out: &mut B,
    value: &TrackedAggScalarValue<A>,
    resources: &mut R,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    match value {
        TrackedAggScalarValue::Bool(v) => {
            out.push(1)?;
            out.push(if *v { 1 } else { 0 })?;
        }
        TrackedAggScalarValue::Int64(v) => {
            out.push(2)?;
            out.append(&v.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Float64(v) => {
            out.push(3)?;
            out.append(&v.to_bits().to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Utf8(v) => {
            out.push(4)?;
            let len = u32::try_from(v.len())
                .map_err(|_| "min_n/max_n utf8 length overflow".to_string())?;
            out.append(&len.to_le_bytes(), work)?;
            out.append(v, work)?;
        }
        TrackedAggScalarValue::Date32(v) => {
            out.push(5)?;
            out.append(&v.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Timestamp(v) => {
            out.push(6)?;
            out.append(&v.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Decimal128(v) => {
            out.push(7)?;
            out.append(&v.to_le_bytes(), work)?;
        }
        TrackedAggScalarValue::Decimal256(v) => {
            out.push(11)?;
            let lease = resources.before_decimal256_format(work)?;
            let text = v.to_string();
            resources.after_decimal256_format(&text, text.capacity(), work)?;
            let len = u32::try_from(text.len())
                .map_err(|_| "min_n/max_n decimal256 length overflow".to_string())?;
            out.append(&len.to_le_bytes(), work)?;
            out.append(text.as_bytes(), work)?;
            // The returned OWN String ends before either reservation.
            // On an error, reverse declaration order preserves the same rule.
            drop(text);
            drop(lease);
        }
        other => {
            return Err(
                format!("min_n/max_n does not support serialized value {:?}", other).into(),
            );
        }
    }
    Ok(())
}

fn decode_scalar<A: ScalarStateAllocator, R: NCodecResources>(
    bytes: &[u8],
    pos: &mut usize,
    allocator: &A,
    resources: &mut R,
    work: &mut ScalarWork<'_, '_>,
) -> Result<TrackedAggScalarValue<A>, ScalarStateError> {
    work.step()?;
    need_len(bytes, *pos, 1, "tag")?;
    let tag = bytes[*pos];
    *pos += 1;
    match tag {
        1 => {
            need_len(bytes, *pos, 1, "bool")?;
            let value = bytes[*pos] != 0;
            *pos += 1;
            Ok(TrackedAggScalarValue::Bool(value))
        }
        2 => {
            need_len(bytes, *pos, 8, "int64")?;
            let value = i64::from_le_bytes(
                bytes[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| "min_n/max_n int64 decode failed".to_string())?,
            );
            *pos += 8;
            Ok(TrackedAggScalarValue::Int64(value))
        }
        3 => {
            need_len(bytes, *pos, 8, "float64")?;
            let bits = u64::from_le_bytes(
                bytes[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| "min_n/max_n float64 decode failed".to_string())?,
            );
            *pos += 8;
            Ok(TrackedAggScalarValue::Float64(f64::from_bits(bits)))
        }
        4 => {
            let len = read_u32(bytes, pos, "utf8_len")? as usize;
            need_len(bytes, *pos, len, "utf8")?;
            let raw = &bytes[*pos..*pos + len];
            for _ in raw {
                work.step()?;
            }
            work.flush()?;
            std::str::from_utf8(raw)
                .map_err(|e| format!("min_n/max_n utf8 decode failed: {}", e))?;
            let value = scalar::scalar_bytes(allocator.clone(), raw, work)?;
            *pos += len;
            Ok(TrackedAggScalarValue::Utf8(value))
        }
        5 => {
            need_len(bytes, *pos, 4, "date32")?;
            let value = i32::from_le_bytes(
                bytes[*pos..*pos + 4]
                    .try_into()
                    .map_err(|_| "min_n/max_n date32 decode failed".to_string())?,
            );
            *pos += 4;
            Ok(TrackedAggScalarValue::Date32(value))
        }
        6 => {
            need_len(bytes, *pos, 8, "timestamp")?;
            let value = i64::from_le_bytes(
                bytes[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| "min_n/max_n timestamp decode failed".to_string())?,
            );
            *pos += 8;
            Ok(TrackedAggScalarValue::Timestamp(value))
        }
        7 => {
            need_len(bytes, *pos, 16, "decimal128")?;
            let value = i128::from_le_bytes(
                bytes[*pos..*pos + 16]
                    .try_into()
                    .map_err(|_| "min_n/max_n decimal128 decode failed".to_string())?,
            );
            *pos += 16;
            Ok(TrackedAggScalarValue::Decimal128(value))
        }
        11 => {
            let len = read_u32(bytes, pos, "decimal256_len")? as usize;
            need_len(bytes, *pos, len, "decimal256")?;
            for _ in &bytes[*pos..*pos + len] {
                work.step()?;
            }
            work.flush()?;
            let text = std::str::from_utf8(&bytes[*pos..*pos + len])
                .map_err(|e| format!("min_n/max_n decimal256 decode failed: {}", e))?;
            *pos += len;
            let lease = resources.before_decimal256_parse(text.len(), work)?;
            let value = text
                .parse::<i256>()
                .map_err(|_| "min_n/max_n decimal256 parse failed".to_string())?;
            resources.after_decimal256_parse(work)?;
            drop(lease);
            Ok(TrackedAggScalarValue::Decimal256(value))
        }
        other => Err(format!("min_n/max_n decode unknown tag {}", other).into()),
    }
}

#[cfg(test)]
#[path = "aggregate_n_core/sort_resource_tests.rs"]
mod sort_resource_tests;

#[cfg(test)]
#[path = "aggregate_n_core/comparison_resource_tests.rs"]
mod comparison_resource_tests;
