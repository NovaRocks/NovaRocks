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
//! Bitset-based runtime membership filter.
//!
//! Responsibilities:
//! - Implements compact bitmap membership checks for integral and dictionary-like domains.
//! - Supports filter build and probe operations with low-overhead bit tests.
//!
//! Key exported interfaces:
//! - Types: `RuntimeBitsetFilter`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;

use super::min_max::RuntimeMinMaxFilter;

const DEFAULT_L2_CACHE_SIZE: usize = 1 * 1024 * 1024;
const DEFAULT_L3_CACHE_SIZE: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
/// Bitset-based runtime filter for exact membership tests on compact value domains.
pub(crate) struct RuntimeBitsetFilter {
    filter_id: i32,
    slot_id: SlotId,
    ltype: crate::types::TPrimitiveType,
    has_null: bool,
    join_mode: i8,
    size: u64,
    min_value: i64,
    max_value: i64,
    bitset: Vec<u8>,
    min_max: RuntimeMinMaxFilter,
}

impl RuntimeBitsetFilter {
    pub(in crate::exec::runtime_filter) fn new(
        filter_id: i32,
        slot_id: SlotId,
        ltype: crate::types::TPrimitiveType,
        has_null: bool,
        join_mode: i8,
        size: u64,
        min_value: i64,
        max_value: i64,
        bitset: Vec<u8>,
        min_max: RuntimeMinMaxFilter,
    ) -> Self {
        Self {
            filter_id,
            slot_id,
            ltype,
            has_null,
            join_mode,
            size,
            min_value,
            max_value,
            bitset,
            min_max,
        }
    }

    pub(crate) fn filter_id(&self) -> i32 {
        self.filter_id
    }

    pub(crate) fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub(crate) fn ltype(&self) -> crate::types::TPrimitiveType {
        self.ltype
    }

    pub(crate) fn has_null(&self) -> bool {
        self.has_null
    }

    pub(crate) fn join_mode(&self) -> i8 {
        self.join_mode
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(in crate::exec::runtime_filter) fn min_value(&self) -> i64 {
        self.min_value
    }

    pub(in crate::exec::runtime_filter) fn max_value(&self) -> i64 {
        self.max_value
    }

    pub(in crate::exec::runtime_filter) fn bitset(&self) -> &[u8] {
        &self.bitset
    }

    pub(crate) fn min_max(&self) -> &RuntimeMinMaxFilter {
        &self.min_max
    }

    #[allow(dead_code)]
    pub(crate) fn min_max_mut(&mut self) -> &mut RuntimeMinMaxFilter {
        &mut self.min_max
    }

    pub(crate) fn set_min_max(&mut self, min_max: RuntimeMinMaxFilter) {
        self.min_max = min_max;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bitset.is_empty()
    }

    pub(crate) fn with_slot_id(&self, slot_id: SlotId) -> Self {
        if self.slot_id == slot_id {
            return self.clone();
        }
        Self {
            filter_id: self.filter_id,
            slot_id,
            ltype: self.ltype,
            has_null: self.has_null,
            join_mode: self.join_mode,
            size: self.size,
            min_value: self.min_value,
            max_value: self.max_value,
            bitset: self.bitset.clone(),
            min_max: self.min_max.clone(),
        }
    }

    pub(crate) fn merge_from(&mut self, other: &RuntimeBitsetFilter) -> Result<(), String> {
        if self.filter_id != other.filter_id || self.ltype != other.ltype {
            return Err("runtime bitset filter metadata mismatch".to_string());
        }
        if self.min_value != other.min_value || self.max_value != other.max_value {
            return Err("runtime bitset filter range mismatch".to_string());
        }
        if self.bitset.len() != other.bitset.len() {
            return Err("runtime bitset filter size mismatch".to_string());
        }
        self.has_null |= other.has_null;
        self.size = self.size.saturating_add(other.size);
        self.min_max.merge_from(&other.min_max)?;
        for (dst, src) in self.bitset.iter_mut().zip(other.bitset.iter()) {
            *dst |= *src;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn filter_chunk(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        if self.is_empty() {
            return Ok(Some(chunk));
        }
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }
        if !chunk.slot_id_to_index().contains_key(&self.slot_id) {
            return Ok(Some(chunk));
        }
        let array = chunk.column_by_slot_id(self.slot_id)?;
        let len = chunk.len();
        let mut keep = vec![true; len];
        apply_bitset_filter(self, &self.ltype, self.has_null, array, &mut keep)?;

        if keep.iter().all(|v| *v) {
            return Ok(Some(chunk));
        }
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }
        let mask = BooleanArray::from(keep);
        let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(filtered_batch)))
    }

    pub(crate) fn filter_chunk_with_array(
        &self,
        array: &ArrayRef,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        if self.is_empty() {
            return Ok(Some(chunk));
        }
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }
        if array.len() != chunk.len() {
            return Err("runtime bitset filter array length mismatch".to_string());
        }
        let len = chunk.len();
        let mut keep = vec![true; len];
        apply_bitset_filter(self, &self.ltype, self.has_null, array.clone(), &mut keep)?;

        if keep.iter().all(|v| *v) {
            return Ok(Some(chunk));
        }
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }
        let mask = BooleanArray::from(keep);
        let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(filtered_batch)))
    }
}

pub(crate) fn maybe_build_runtime_bitset_filter(
    filter_id: i32,
    slot_id: SlotId,
    ltype: crate::types::TPrimitiveType,
    join_mode: i8,
    size: u64,
    arrays: &[ArrayRef],
    min_max: RuntimeMinMaxFilter,
) -> Result<Option<RuntimeBitsetFilter>, String> {
    if arrays.is_empty() || size == 0 {
        return Ok(None);
    }
    if !supports_runtime_bitset_ltype(&ltype) {
        return Ok(None);
    }

    let (has_null, min_value, max_value) = match scan_bitset_build_stats(&ltype, arrays)? {
        Some(v) => v,
        None => return Ok(None),
    };
    if min_value > max_value {
        return Ok(None);
    }

    let value_interval = match (max_value as i128)
        .checked_sub(min_value as i128)
        .and_then(|v| v.checked_add(1))
    {
        Some(v) if v > 0 => v as u128,
        _ => return Ok(None),
    };
    let bitset_bytes_u128 = (value_interval + 7) / 8;
    let bitset_bytes = match usize::try_from(bitset_bytes_u128) {
        Ok(v) if v > 0 => v,
        _ => return Ok(None),
    };
    let bloom_bytes = usize::try_from(size).unwrap_or(usize::MAX);
    if !should_use_bitset(bitset_bytes, bloom_bytes) {
        return Ok(None);
    }

    let mut bitset = vec![0u8; bitset_bytes];
    fill_bitset_from_arrays(&ltype, arrays, min_value, max_value, &mut bitset)?;
    Ok(Some(RuntimeBitsetFilter::new(
        filter_id, slot_id, ltype, has_null, join_mode, size, min_value, max_value, bitset, min_max,
    )))
}

fn supports_runtime_bitset_ltype(ltype: &crate::types::TPrimitiveType) -> bool {
    use crate::types::TPrimitiveType;
    matches!(
        ltype,
        t if *t == TPrimitiveType::BOOLEAN
            || *t == TPrimitiveType::TINYINT
            || *t == TPrimitiveType::SMALLINT
            || *t == TPrimitiveType::INT
            || *t == TPrimitiveType::BIGINT
            || *t == TPrimitiveType::DATE
    )
}

fn should_use_bitset(bitset_memory_usage: usize, bloom_memory_usage: usize) -> bool {
    bitset_memory_usage <= bloom_memory_usage
        || (bitset_memory_usage <= bloom_memory_usage.saturating_mul(4)
            && bitset_memory_usage <= DEFAULT_L2_CACHE_SIZE)
        || (bitset_memory_usage <= bloom_memory_usage.saturating_mul(2)
            && bitset_memory_usage <= DEFAULT_L3_CACHE_SIZE)
}

fn scan_bitset_build_stats(
    ltype: &crate::types::TPrimitiveType,
    arrays: &[ArrayRef],
) -> Result<Option<(bool, i64, i64)>, String> {
    let mut has_null = false;
    let mut has_value = false;
    let mut min_value = i64::MAX;
    let mut max_value = i64::MIN;
    let mut update = |value: i64| {
        has_value = true;
        if value < min_value {
            min_value = value;
        }
        if value > max_value {
            max_value = value;
        }
    };

    for array in arrays {
        match array.data_type() {
            DataType::Boolean if *ltype == crate::types::TPrimitiveType::BOOLEAN => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Boolean".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    update(if arr.value(i) { 1 } else { 0 });
                }
            }
            DataType::Int8 if *ltype == crate::types::TPrimitiveType::TINYINT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int8".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    update(arr.value(i) as i64);
                }
            }
            DataType::Int16 if *ltype == crate::types::TPrimitiveType::SMALLINT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int16".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    update(arr.value(i) as i64);
                }
            }
            DataType::Int32 if *ltype == crate::types::TPrimitiveType::INT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    update(arr.value(i) as i64);
                }
            }
            DataType::Int64 if *ltype == crate::types::TPrimitiveType::BIGINT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int64".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    update(arr.value(i));
                }
            }
            DataType::Date32 if *ltype == crate::types::TPrimitiveType::DATE => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Date32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    update(arr.value(i) as i64);
                }
            }
            _ => {
                return Err(format!(
                    "runtime bitset build unsupported type mapping: ltype={:?} data_type={:?}",
                    ltype,
                    array.data_type()
                ));
            }
        }
    }

    if !has_value {
        return Ok(None);
    }
    Ok(Some((has_null, min_value, max_value)))
}

fn fill_bitset_from_arrays(
    ltype: &crate::types::TPrimitiveType,
    arrays: &[ArrayRef],
    min_value: i64,
    max_value: i64,
    bitset: &mut [u8],
) -> Result<(), String> {
    let mut set_value = |value: i64| {
        if value < min_value || value > max_value {
            return;
        }
        let offset = (value - min_value) as u64;
        let byte_idx = (offset / 8) as usize;
        if byte_idx >= bitset.len() {
            return;
        }
        let bit_idx = (offset % 8) as u8;
        bitset[byte_idx] |= 1u8 << bit_idx;
    };

    for array in arrays {
        match array.data_type() {
            DataType::Boolean if *ltype == crate::types::TPrimitiveType::BOOLEAN => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Boolean".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    set_value(if arr.value(i) { 1 } else { 0 });
                }
            }
            DataType::Int8 if *ltype == crate::types::TPrimitiveType::TINYINT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int8".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    set_value(arr.value(i) as i64);
                }
            }
            DataType::Int16 if *ltype == crate::types::TPrimitiveType::SMALLINT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int16".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    set_value(arr.value(i) as i64);
                }
            }
            DataType::Int32 if *ltype == crate::types::TPrimitiveType::INT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    set_value(arr.value(i) as i64);
                }
            }
            DataType::Int64 if *ltype == crate::types::TPrimitiveType::BIGINT => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Int64".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    set_value(arr.value(i));
                }
            }
            DataType::Date32 if *ltype == crate::types::TPrimitiveType::DATE => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "runtime bitset build type mismatch for Date32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    set_value(arr.value(i) as i64);
                }
            }
            _ => {
                return Err(format!(
                    "runtime bitset build unsupported type mapping: ltype={:?} data_type={:?}",
                    ltype,
                    array.data_type()
                ));
            }
        }
    }
    Ok(())
}

fn apply_bitset_filter(
    filter: &RuntimeBitsetFilter,
    ltype: &crate::types::TPrimitiveType,
    has_null: bool,
    array: ArrayRef,
    keep: &mut [bool],
) -> Result<(), String> {
    let len = array.len();
    if keep.len() != len {
        return Err("runtime bitset filter selection size mismatch".to_string());
    }
    let min_value = filter.min_value;
    let max_value = filter.max_value;
    let bitset = &filter.bitset;
    let test_value = |value: i64| -> bool {
        if value < min_value || value > max_value {
            return false;
        }
        let offset = (value - min_value) as u64;
        let byte_idx = (offset / 8) as usize;
        if byte_idx >= bitset.len() {
            return false;
        }
        let bit_idx = (offset % 8) as u8;
        (bitset[byte_idx] & (1u8 << bit_idx)) != 0
    };
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "runtime bitset filter type mismatch for Boolean".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = if arr.value(i) { 1 } else { 0 };
                keep[i] = test_value(v);
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "runtime bitset filter type mismatch for Int8".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                keep[i] = test_value(arr.value(i) as i64);
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "runtime bitset filter type mismatch for Int16".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                keep[i] = test_value(arr.value(i) as i64);
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "runtime bitset filter type mismatch for Int32".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                keep[i] = test_value(arr.value(i) as i64);
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "runtime bitset filter type mismatch for Int64".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                keep[i] = test_value(arr.value(i));
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "runtime bitset filter type mismatch for Date32".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                keep[i] = test_value(arr.value(i) as i64);
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "runtime bitset filter type mismatch for TimestampSecond".to_string()
                    })?;
                for i in 0..len {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        keep[i] = has_null;
                        continue;
                    }
                    let v = arr.value(i).saturating_mul(1_000_000);
                    keep[i] = test_value(v);
                }
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "runtime bitset filter type mismatch for TimestampMillisecond".to_string()
                    })?;
                for i in 0..len {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        keep[i] = has_null;
                        continue;
                    }
                    let v = arr.value(i).saturating_mul(1_000);
                    keep[i] = test_value(v);
                }
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "runtime bitset filter type mismatch for TimestampMicrosecond".to_string()
                    })?;
                for i in 0..len {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        keep[i] = has_null;
                        continue;
                    }
                    keep[i] = test_value(arr.value(i));
                }
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "runtime bitset filter type mismatch for TimestampNanosecond".to_string()
                    })?;
                for i in 0..len {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        keep[i] = has_null;
                        continue;
                    }
                    let v = arr.value(i) / 1_000;
                    keep[i] = test_value(v);
                }
            }
        },
        _ => {
            return Err(format!(
                "unsupported runtime bitset filter type: {:?}",
                ltype
            ));
        }
    }
    Ok(())
}
