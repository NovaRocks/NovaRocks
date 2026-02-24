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
//! Bloom-filter runtime filter implementation.
//!
//! Responsibilities:
//! - Implements StarRocks-compatible block bloom filter encoding and probe semantics.
//! - Supports merge/probe operations for high-cardinality membership pruning.
//!
//! Key exported interfaces:
//! - Types: `RuntimeBloomFilter`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;

use super::codec::{read_i32_le, read_u32_le};
use super::min_max::RuntimeMinMaxFilter;

const CRC_HASH_SEED1: u64 = 0x811C9DC5;
const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

#[derive(Clone, Debug)]
pub(in crate::exec::runtime_filter) struct SimdBlockFilter {
    log_num_buckets: i32,
    directory_mask: u32,
    directory: Vec<u32>,
}

impl SimdBlockFilter {
    pub(in crate::exec::runtime_filter) fn empty() -> Self {
        Self {
            log_num_buckets: 0,
            directory_mask: 0,
            directory: Vec::new(),
        }
    }

    fn init(num_elements: u64) -> Self {
        let nums = num_elements.max(1);
        let log_heap_space = (nums as f64).log2().ceil() as i32;
        let log_num_buckets = std::cmp::max(1, log_heap_space - 5);
        let directory_mask = ((1u64 << std::cmp::min(63, log_num_buckets as u32)) - 1) as u32;
        let bucket_count = 1usize << log_num_buckets.max(1);
        let directory = vec![0u32; bucket_count * 8];
        Self {
            log_num_buckets,
            directory_mask,
            directory,
        }
    }

    pub(in crate::exec::runtime_filter) fn can_use(&self) -> bool {
        !self.directory.is_empty()
    }

    pub(in crate::exec::runtime_filter) fn merge_from(
        &mut self,
        other: &SimdBlockFilter,
    ) -> Result<(), String> {
        if !self.can_use() || !other.can_use() {
            return Ok(());
        }
        if self.log_num_buckets != other.log_num_buckets
            || self.directory_mask != other.directory_mask
            || self.directory.len() != other.directory.len()
        {
            return Err("runtime bloom filter merge size mismatch".to_string());
        }
        for (dst, src) in self.directory.iter_mut().zip(other.directory.iter()) {
            *dst |= *src;
        }
        Ok(())
    }

    fn insert_hash(&mut self, hash: u64) {
        if !self.can_use() {
            return;
        }
        let bucket_idx = (hash as u32) & self.directory_mask;
        let key = (hash >> (self.log_num_buckets as u32)) as u32;
        let masks = make_mask(key);
        let base = bucket_idx as usize * 8;
        for i in 0..8 {
            self.directory[base + i] |= masks[i];
        }
    }

    fn test_hash(&self, hash: u64) -> bool {
        if !self.can_use() {
            return true;
        }
        let bucket_idx = (hash as u32) & self.directory_mask;
        let key = (hash >> (self.log_num_buckets as u32)) as u32;
        let masks = make_mask(key);
        let base = bucket_idx as usize * 8;
        for i in 0..8 {
            if (self.directory[base + i] & masks[i]) == 0 {
                return false;
            }
        }
        true
    }

    pub(in crate::exec::runtime_filter) fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.log_num_buckets.to_le_bytes());
        buf.extend_from_slice(&self.directory_mask.to_le_bytes());
        let data_size = (self.directory.len() * 4) as i32;
        buf.extend_from_slice(&data_size.to_le_bytes());
        for value in &self.directory {
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }

    pub(in crate::exec::runtime_filter) fn deserialize(
        data: &[u8],
        offset: &mut usize,
    ) -> Result<Self, String> {
        let log_num_buckets = read_i32_le(data, offset)?;
        let directory_mask = read_u32_le(data, offset)?;
        let data_size = read_i32_le(data, offset)? as usize;
        if data.len() < *offset + data_size {
            return Err("runtime bloom filter data truncated".to_string());
        }
        if data_size % 4 != 0 {
            return Err("runtime bloom filter data size invalid".to_string());
        }
        let mut directory = Vec::with_capacity(data_size / 4);
        for _ in 0..(data_size / 4) {
            directory.push(read_u32_le(data, offset)?);
        }
        Ok(Self {
            log_num_buckets,
            directory_mask,
            directory,
        })
    }
}

#[derive(Clone, Debug)]
/// Bloom-based runtime membership filter compatible with StarRocks wire semantics.
pub(crate) struct RuntimeBloomFilter {
    filter_id: i32,
    slot_id: SlotId,
    ltype: crate::types::TPrimitiveType,
    has_null: bool,
    join_mode: i8,
    size: u64,
    bf: Option<SimdBlockFilter>,
    min_max: RuntimeMinMaxFilter,
}

impl RuntimeBloomFilter {
    pub(in crate::exec::runtime_filter) fn new(
        filter_id: i32,
        slot_id: SlotId,
        ltype: crate::types::TPrimitiveType,
        has_null: bool,
        join_mode: i8,
        size: u64,
        bf: Option<SimdBlockFilter>,
        min_max: RuntimeMinMaxFilter,
    ) -> Self {
        Self {
            filter_id,
            slot_id,
            ltype,
            has_null,
            join_mode,
            size,
            bf,
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
        self.bf.is_none()
    }

    pub(crate) fn can_use_bf(&self) -> bool {
        self.bf.is_some()
    }

    pub(in crate::exec::runtime_filter) fn bf(&self) -> Option<&SimdBlockFilter> {
        self.bf.as_ref()
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
            bf: self.bf.clone(),
            min_max: self.min_max.clone(),
        }
    }

    pub(crate) fn merge_from(&mut self, other: &RuntimeBloomFilter) -> Result<(), String> {
        if self.filter_id != other.filter_id || self.ltype != other.ltype {
            return Err("runtime bloom filter metadata mismatch".to_string());
        }
        self.has_null |= other.has_null;
        self.size = self.size.saturating_add(other.size);
        self.min_max.merge_from(&other.min_max)?;
        match (&mut self.bf, &other.bf) {
            (Some(dst), Some(src)) => dst.merge_from(src)?,
            (None, Some(src)) => {
                self.bf = Some(src.clone());
            }
            _ => {}
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
        let bf = self.bf.as_ref().expect("bloom filter");
        apply_bloom_filter(bf, &self.ltype, self.has_null, array, &mut keep)?;

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
            return Err("runtime bloom filter array length mismatch".to_string());
        }
        let len = chunk.len();
        let mut keep = vec![true; len];
        let bf = self.bf.as_ref().expect("bloom filter");
        apply_bloom_filter(bf, &self.ltype, self.has_null, array.clone(), &mut keep)?;

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

    #[allow(dead_code)]
    pub(crate) fn build_from_array(
        filter_id: i32,
        slot_id: SlotId,
        ltype: crate::types::TPrimitiveType,
        array: &ArrayRef,
        join_mode: i8,
    ) -> Result<Self, String> {
        let size = array.len() as u64;
        let min_max = RuntimeMinMaxFilter::from_arrays(ltype, &[array.clone()])?;
        if size == 0 {
            return Ok(Self::new(
                filter_id, slot_id, ltype, false, join_mode, 0, None, min_max,
            ));
        }
        let mut bf = SimdBlockFilter::init(size);
        let has_null = build_bloom_from_array(&ltype, array, &mut bf)?;
        Ok(Self::new(
            filter_id,
            slot_id,
            ltype,
            has_null,
            join_mode,
            size,
            Some(bf),
            min_max,
        ))
    }

    pub(crate) fn with_capacity(
        filter_id: i32,
        slot_id: SlotId,
        ltype: crate::types::TPrimitiveType,
        join_mode: i8,
        size: u64,
        min_max: RuntimeMinMaxFilter,
    ) -> Self {
        let bf = if size > 0 {
            Some(SimdBlockFilter::init(size))
        } else {
            None
        };
        Self::new(
            filter_id, slot_id, ltype, false, join_mode, size, bf, min_max,
        )
    }

    pub(crate) fn insert_array(&mut self, array: &ArrayRef) -> Result<(), String> {
        let Some(bf) = self.bf.as_mut() else {
            return Ok(());
        };
        let has_null = build_bloom_from_array(&self.ltype, array, bf)?;
        if has_null {
            self.has_null = true;
        }
        Ok(())
    }
}

fn make_mask(key: u32) -> [u32; 8] {
    let mut masks = [0u32; 8];
    for i in 0..8 {
        let mut v = key.wrapping_mul(SALT[i]);
        v >>= 27;
        masks[i] = 1u32 << v;
    }
    masks
}

fn phmap_mix_4(a: u32) -> u32 {
    let kmul: u64 = 0xcc9e2d51;
    let l = (a as u64).wrapping_mul(kmul);
    (l ^ (l >> 32)) as u32
}

fn phmap_mix_8(a: u64) -> u64 {
    let k: u64 = 0xde5fb9d2630458e9;
    let prod = (a as u128) * (k as u128);
    let l = prod as u64;
    let h = (prod >> 64) as u64;
    h.wrapping_add(l)
}

fn crc32c_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0x82f63b78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

fn crc_hash_32(data: &[u8], seed: u32) -> u32 {
    let crc = crc32c_update(seed, data);
    phmap_mix_4(crc)
}

fn crc_hash_64_unmixed(data: &[u8], seed: u64) -> u64 {
    if data.len() < 8 {
        return crc_hash_32(data, seed as u32) as u64;
    }
    let mut crc = seed as u32;
    let mut pos = 0usize;
    let words = data.len() / 8;
    for _ in 0..words {
        crc = crc32c_update(crc, &data[pos..pos + 8]);
        pos += 8;
    }
    let start = data.len() - 8;
    crc = crc32c_update(crc, &data[start..start + 8]);
    crc as u64
}

fn crc_hash_64(data: &[u8], seed: u64) -> u64 {
    phmap_mix_8(crc_hash_64_unmixed(data, seed))
}

fn slice_hash(data: &[u8]) -> u64 {
    crc_hash_64(data, CRC_HASH_SEED1)
}

fn decimal_hash(value: i128) -> u64 {
    slice_hash(&value.to_le_bytes())
}

fn apply_bloom_filter(
    bf: &SimdBlockFilter,
    ltype: &crate::types::TPrimitiveType,
    has_null: bool,
    array: ArrayRef,
    keep: &mut [bool],
) -> Result<(), String> {
    let len = array.len();
    if keep.len() != len {
        return Err("runtime bloom filter selection size mismatch".to_string());
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Boolean".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = if arr.value(i) { 1u64 } else { 0u64 };
                let hash = phmap_mix_8(v);
                keep[i] = bf.test_hash(hash);
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int8".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int16".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int32".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int64".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i) as u64;
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Float32".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i).to_bits() as u64;
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Float64".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i).to_bits();
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Date32".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                keep[i] = bf.test_hash(phmap_mix_8(v));
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampSecond".to_string()
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
                    keep[i] = bf.test_hash(phmap_mix_8(v as u64));
                }
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampMillisecond".to_string()
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
                    keep[i] = bf.test_hash(phmap_mix_8(v as u64));
                }
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampMicrosecond".to_string()
                    })?;
                for i in 0..len {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        keep[i] = has_null;
                        continue;
                    }
                    let v = arr.value(i);
                    keep[i] = bf.test_hash(phmap_mix_8(v as u64));
                }
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampNanosecond".to_string()
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
                    keep[i] = bf.test_hash(phmap_mix_8(v as u64));
                }
            }
        },
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Utf8".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let hash = slice_hash(arr.value(i).as_bytes());
                keep[i] = bf.test_hash(hash);
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Decimal128".to_string())?;
            for i in 0..len {
                if !keep[i] {
                    continue;
                }
                if arr.is_null(i) {
                    keep[i] = has_null;
                    continue;
                }
                let hash = decimal_hash(arr.value(i));
                keep[i] = bf.test_hash(hash);
            }
        }
        _ => {
            return Err(format!(
                "unsupported runtime bloom filter type: {:?}",
                ltype
            ));
        }
    }
    Ok(())
}

fn build_bloom_from_array(
    ltype: &crate::types::TPrimitiveType,
    array: &ArrayRef,
    bf: &mut SimdBlockFilter,
) -> Result<bool, String> {
    let len = array.len();
    let mut has_null = false;
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Boolean".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = if arr.value(i) { 1u64 } else { 0u64 };
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int8".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int16".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int32".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Int64".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i) as u64;
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Float32".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i).to_bits() as u64;
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Float64".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i).to_bits();
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Date32".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                let v = arr.value(i) as i64 as u64;
                bf.insert_hash(phmap_mix_8(v));
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampSecond".to_string()
                    })?;
                for i in 0..len {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    let v = arr.value(i).saturating_mul(1_000_000);
                    bf.insert_hash(phmap_mix_8(v as u64));
                }
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampMillisecond".to_string()
                    })?;
                for i in 0..len {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    let v = arr.value(i).saturating_mul(1_000);
                    bf.insert_hash(phmap_mix_8(v as u64));
                }
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampMicrosecond".to_string()
                    })?;
                for i in 0..len {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    let v = arr.value(i);
                    bf.insert_hash(phmap_mix_8(v as u64));
                }
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "runtime bloom filter type mismatch for TimestampNanosecond".to_string()
                    })?;
                for i in 0..len {
                    if arr.is_null(i) {
                        has_null = true;
                        continue;
                    }
                    let v = arr.value(i) / 1_000;
                    bf.insert_hash(phmap_mix_8(v as u64));
                }
            }
        },
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Utf8".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                bf.insert_hash(slice_hash(arr.value(i).as_bytes()));
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "runtime bloom filter type mismatch for Decimal128".to_string())?;
            for i in 0..len {
                if arr.is_null(i) {
                    has_null = true;
                    continue;
                }
                bf.insert_hash(decimal_hash(arr.value(i)));
            }
        }
        _ => {
            return Err(format!(
                "unsupported runtime bloom filter type: {:?}",
                ltype
            ));
        }
    }
    Ok(has_null)
}
