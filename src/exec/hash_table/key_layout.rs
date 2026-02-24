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
use arrow::array::Array;
use arrow::datatypes::DataType;

use crate::exec::expr::agg::IntArrayView;

use super::key_builder::GroupKeyArrayView;
use super::key_strategy::is_compressible_key_type;

pub(crate) struct CompressedKeyContext {
    pub(crate) bases: Vec<i128>,
    pub(crate) used_bits: Vec<u8>,
    pub(crate) null_offsets: Vec<u16>,
    pub(crate) value_offsets: Vec<u16>,
}

pub(crate) fn build_compressed_key_context(
    views: &[GroupKeyArrayView<'_>],
    types: &[DataType],
) -> Result<CompressedKeyContext, String> {
    if views.len() != types.len() {
        return Err("compressed key type length mismatch".to_string());
    }
    for data_type in types {
        if !is_compressible_key_type(data_type) {
            return Err(format!("compressed key unsupported type: {:?}", data_type));
        }
    }
    let mut bases = Vec::with_capacity(types.len());
    let mut used_bits = Vec::with_capacity(types.len());
    for view in views {
        let range = min_max_i128(view)?;
        let (base, max_value) = match range {
            Some((min, max)) => (min, max),
            None => (0, 0),
        };
        let diff = max_value.saturating_sub(base);
        let bits = bits_for_range(diff);
        bases.push(base);
        used_bits.push(bits);
    }

    let mut null_offsets = Vec::with_capacity(types.len());
    let mut value_offsets = Vec::with_capacity(types.len());
    let mut total_bits: usize = 0;
    for bits in &used_bits {
        null_offsets.push(
            u16::try_from(total_bits).map_err(|_| "compressed key offset overflow".to_string())?,
        );
        total_bits = total_bits
            .checked_add(1)
            .ok_or_else(|| "compressed key bit size overflow".to_string())?;
        value_offsets.push(
            u16::try_from(total_bits).map_err(|_| "compressed key offset overflow".to_string())?,
        );
        total_bits = total_bits
            .checked_add(*bits as usize)
            .ok_or_else(|| "compressed key bit size overflow".to_string())?;
    }

    if total_bits > 128 {
        return Err("compressed key too wide".to_string());
    }
    Ok(CompressedKeyContext {
        bases,
        used_bits,
        null_offsets,
        value_offsets,
    })
}

pub(crate) fn compressed_key_is_valid(
    ctx: &CompressedKeyContext,
    views: &[GroupKeyArrayView<'_>],
    row: usize,
) -> Result<bool, String> {
    Ok(compressed_key_bits(ctx, views, row)?.is_some())
}

fn compressed_key_bits(
    ctx: &CompressedKeyContext,
    views: &[GroupKeyArrayView<'_>],
    row: usize,
) -> Result<Option<u128>, String> {
    let mut bits: u128 = 0;
    for (idx, view) in views.iter().enumerate() {
        let null_offset = *ctx
            .null_offsets
            .get(idx)
            .ok_or_else(|| "compressed null offset missing".to_string())?;
        let value_offset = *ctx
            .value_offsets
            .get(idx)
            .ok_or_else(|| "compressed value offset missing".to_string())?;
        let used = *ctx
            .used_bits
            .get(idx)
            .ok_or_else(|| "compressed bits missing".to_string())?;
        let base = *ctx
            .bases
            .get(idx)
            .ok_or_else(|| "compressed base missing".to_string())?;
        let (value, is_null) = match view {
            GroupKeyArrayView::Int(view) => (view.value_at(row).map(|v| v as i128), false),
            GroupKeyArrayView::Date32(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampSecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampMillisecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampMicrosecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::TimestampNanosecond(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some(arr.value(row) as i128), false)
                }
            }
            GroupKeyArrayView::Boolean(arr) => {
                if arr.is_null(row) {
                    (None, true)
                } else {
                    (Some((if arr.value(row) { 1 } else { 0 }) as i128), false)
                }
            }
            _ => {
                return Err("compressed key unsupported type".to_string());
            }
        };

        if is_null || value.is_none() {
            bits |= 1u128 << u32::from(null_offset);
            continue;
        }
        let value = value.expect("compressed value");
        let diff = value - base;
        if diff < 0 {
            return Ok(None);
        }
        let diff = diff as u128;
        let max_diff = if used == 0 {
            0
        } else if used == 128 {
            return Err("compressed bits overflow".to_string());
        } else {
            (1u128 << used) - 1
        };
        if diff > max_diff {
            return Ok(None);
        }
        if used > 0 {
            bits |= diff << u32::from(value_offset);
        }
    }
    Ok(Some(bits))
}

fn min_max_i128(view: &GroupKeyArrayView<'_>) -> Result<Option<(i128, i128)>, String> {
    let mut min: Option<i128> = None;
    let mut max: Option<i128> = None;
    match view {
        GroupKeyArrayView::Int(view) => match view {
            IntArrayView::Int64(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
            IntArrayView::Int32(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
            IntArrayView::Int16(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
            IntArrayView::Int8(arr) => {
                for row in 0..arr.len() {
                    if !arr.is_null(row) {
                        let value = arr.value(row) as i128;
                        min = Some(min.map_or(value, |m| m.min(value)));
                        max = Some(max.map_or(value, |m| m.max(value)));
                    }
                }
            }
        },
        GroupKeyArrayView::Date32(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampSecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampMillisecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampMicrosecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::TimestampNanosecond(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = arr.value(row) as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        GroupKeyArrayView::Boolean(arr) => {
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    let value = if arr.value(row) { 1 } else { 0 };
                    let value = value as i128;
                    min = Some(min.map_or(value, |m| m.min(value)));
                    max = Some(max.map_or(value, |m| m.max(value)));
                }
            }
        }
        _ => {
            return Err("compressed key min/max unsupported type".to_string());
        }
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok(Some((min, max))),
        _ => Ok(None),
    }
}

fn bits_for_range(range: i128) -> u8 {
    if range <= 0 {
        0
    } else {
        let value = range as u128;
        (128 - value.leading_zeros()) as u8
    }
}
