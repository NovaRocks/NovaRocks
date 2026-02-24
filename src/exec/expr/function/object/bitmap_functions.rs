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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanBuilder, FixedSizeBinaryArray, Int8Array,
    Int16Array, Int32Array, Int64Array, Int64Builder, LargeBinaryArray, LargeStringArray,
    StringArray, StringBuilder, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use base64::Engine;

use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

fn row_index(
    row: usize,
    len: usize,
    fn_name: &str,
    arg_idx: usize,
    chunk_len: usize,
) -> Result<usize, String> {
    if len == 1 {
        return Ok(0);
    }
    if len == chunk_len {
        return Ok(row);
    }
    Err(format!(
        "{} arg {} row count mismatch: arg_len={} chunk_len={}",
        fn_name, arg_idx, len, chunk_len
    ))
}

fn downcast_binary<'a>(
    array: &'a ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<&'a BinaryArray, String> {
    array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        format!(
            "{} expects BITMAP/BINARY input for arg {}, got {:?}",
            fn_name,
            arg_idx,
            array.data_type()
        )
    })
}

fn i64_arg_at(
    array: &ArrayRef,
    row: usize,
    fn_name: &str,
    arg_idx: usize,
) -> Result<Option<i64>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!("{} downcast Int8Array failed for arg {}", fn_name, arg_idx)
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i64::from(arr.value(row))))
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                format!("{} downcast Int16Array failed for arg {}", fn_name, arg_idx)
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i64::from(arr.value(row))))
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                format!("{} downcast Int32Array failed for arg {}", fn_name, arg_idx)
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i64::from(arr.value(row))))
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                format!("{} downcast Int64Array failed for arg {}", fn_name, arg_idx)
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(arr.value(row)))
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                format!("{} downcast UInt8Array failed for arg {}", fn_name, arg_idx)
            })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i64::from(arr.value(row))))
            }
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| {
                    format!(
                        "{} downcast UInt16Array failed for arg {}",
                        fn_name, arg_idx
                    )
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i64::from(arr.value(row))))
            }
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    format!(
                        "{} downcast UInt32Array failed for arg {}",
                        fn_name, arg_idx
                    )
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(i64::from(arr.value(row))))
            }
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    format!(
                        "{} downcast UInt64Array failed for arg {}",
                        fn_name, arg_idx
                    )
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let value = i64::try_from(arr.value(row)).map_err(|_| {
                    format!(
                        "{} arg {} value out of BIGINT range: {}",
                        fn_name,
                        arg_idx,
                        arr.value(row)
                    )
                })?;
                Ok(Some(value))
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "{} downcast FixedSizeBinaryArray failed for arg {}",
                        fn_name, arg_idx
                    )
                })?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let value = largeint::i128_from_be_bytes(arr.value(row)).map_err(|e| {
                    format!("{} arg {} decode LARGEINT failed: {}", fn_name, arg_idx, e)
                })?;
                let value = i64::try_from(value).map_err(|_| {
                    format!(
                        "{} arg {} value out of BIGINT range: {}",
                        fn_name, arg_idx, value
                    )
                })?;
                Ok(Some(value))
            }
        }
        other => Err(format!(
            "{} expects BIGINT-compatible input for arg {}, got {:?}",
            fn_name, arg_idx, other
        )),
    }
}

fn encode_bitmap_values(values: &[u64]) -> Result<Vec<u8>, String> {
    let set: BTreeSet<u64> = values.iter().copied().collect();
    super::bitmap_common::encode_internal_bitmap(&set)
}

fn bitmap_minmax_impl(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
    pick_max: bool,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let arr = downcast_binary(&input, fn_name, 0)?;

    let mut values_i128: Vec<Option<i128>> = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len(), fn_name, 0, chunk.len())?;
        if arr.is_null(idx) {
            values_i128.push(None);
            continue;
        }
        let values = match super::bitmap_common::decode_bitmap(arr.value(idx)) {
            Ok(values) => values,
            Err(_) => {
                values_i128.push(None);
                continue;
            }
        };
        let value = if pick_max {
            values.last().copied()
        } else {
            values.first().copied()
        };
        values_i128.push(value.map(|v| v as i128));
    }

    match arena.data_type(expr) {
        Some(DataType::FixedSizeBinary(width)) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            largeint::array_from_i128(&values_i128)
        }
        _ => {
            let mut out = Int64Builder::new();
            for value in values_i128 {
                match value {
                    Some(value) => {
                        let value = i64::try_from(value).map_err(|_| {
                            format!("{} value out of BIGINT range: {}", fn_name, value)
                        })?;
                        out.append_value(value);
                    }
                    None => out.append_null(),
                }
            }
            Ok(Arc::new(out.finish()) as ArrayRef)
        }
    }
}

pub fn eval_bitmap_empty(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for _ in 0..chunk.len() {
        builder.append_value([super::bitmap_common::BITMAP_TYPE_EMPTY]);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_from_string(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BinaryBuilder::new();

    macro_rules! parse_utf8_array {
        ($arr:expr) => {{
            for row in 0..chunk.len() {
                let idx = row_index(row, $arr.len(), "bitmap_from_string", 0, chunk.len())?;
                if $arr.is_null(idx) {
                    builder.append_null();
                    continue;
                }
                match super::bitmap_common::parse_bitmap_string($arr.value(idx)) {
                    Ok(values) => {
                        builder.append_value(super::bitmap_common::encode_internal_bitmap(&values)?)
                    }
                    Err(_) => builder.append_null(),
                }
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    macro_rules! parse_binary_array {
        ($arr:expr) => {{
            for row in 0..chunk.len() {
                let idx = row_index(row, $arr.len(), "bitmap_from_string", 0, chunk.len())?;
                if $arr.is_null(idx) {
                    builder.append_null();
                    continue;
                }
                let Ok(text) = std::str::from_utf8($arr.value(idx)) else {
                    builder.append_null();
                    continue;
                };
                match super::bitmap_common::parse_bitmap_string(text) {
                    Ok(values) => {
                        builder.append_value(super::bitmap_common::encode_internal_bitmap(&values)?)
                    }
                    Err(_) => builder.append_null(),
                }
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    if let Some(arr) = input.as_any().downcast_ref::<StringArray>() {
        parse_utf8_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<LargeStringArray>() {
        parse_utf8_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<BinaryArray>() {
        parse_binary_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        parse_binary_array!(arr);
    }

    Err(format!(
        "bitmap_from_string expects VARCHAR/BINARY input, got {:?}",
        input.data_type()
    ))
}

pub fn eval_bitmap_count(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let arr = downcast_binary(&input, "bitmap_count", 0)?;
    let mut builder = Int64Builder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len(), "bitmap_count", 0, chunk.len())?;
        if arr.is_null(idx) {
            builder.append_value(0);
            continue;
        }
        match super::bitmap_common::decode_bitmap(arr.value(idx)) {
            Ok(values) => {
                let count = i64::try_from(values.len())
                    .map_err(|_| format!("bitmap_count cardinality overflow: {}", values.len()))?;
                builder.append_value(count);
            }
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_min(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    bitmap_minmax_impl(arena, expr, args, chunk, "bitmap_min", false)
}

pub fn eval_bitmap_max(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    bitmap_minmax_impl(arena, expr, args, chunk, "bitmap_max", true)
}

pub fn eval_bitmap_and(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    let lhs = downcast_binary(&lhs, "bitmap_and", 0)?;
    let rhs = downcast_binary(&rhs, "bitmap_and", 1)?;

    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let lhs_idx = row_index(row, lhs.len(), "bitmap_and", 0, chunk.len())?;
        let rhs_idx = row_index(row, rhs.len(), "bitmap_and", 1, chunk.len())?;
        if lhs.is_null(lhs_idx) || rhs.is_null(rhs_idx) {
            builder.append_null();
            continue;
        }
        let left = match super::bitmap_common::decode_bitmap(lhs.value(lhs_idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let right = match super::bitmap_common::decode_bitmap(rhs.value(rhs_idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let values: Vec<u64> = left
            .iter()
            .filter(|value| right.contains(value))
            .copied()
            .collect();
        builder.append_value(encode_bitmap_values(&values)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_has_any(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    let lhs = downcast_binary(&lhs, "bitmap_has_any", 0)?;
    let rhs = downcast_binary(&rhs, "bitmap_has_any", 1)?;

    let mut builder = BooleanBuilder::new();
    for row in 0..chunk.len() {
        let lhs_idx = row_index(row, lhs.len(), "bitmap_has_any", 0, chunk.len())?;
        let rhs_idx = row_index(row, rhs.len(), "bitmap_has_any", 1, chunk.len())?;
        if lhs.is_null(lhs_idx) || rhs.is_null(rhs_idx) {
            builder.append_null();
            continue;
        }
        let left = match super::bitmap_common::decode_bitmap(lhs.value(lhs_idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let right = match super::bitmap_common::decode_bitmap(rhs.value(rhs_idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let has_any = if left.len() <= right.len() {
            left.iter().any(|value| right.contains(value))
        } else {
            right.iter().any(|value| left.contains(value))
        };
        builder.append_value(has_any);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_sub_bitmap(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let bitmap = arena.eval(args[0], chunk)?;
    let offset = arena.eval(args[1], chunk)?;
    let len = arena.eval(args[2], chunk)?;

    let bitmap = downcast_binary(&bitmap, "sub_bitmap", 0)?;
    let mut builder = BinaryBuilder::new();

    for row in 0..chunk.len() {
        let bitmap_idx = row_index(row, bitmap.len(), "sub_bitmap", 0, chunk.len())?;
        let offset_idx = row_index(row, offset.len(), "sub_bitmap", 1, chunk.len())?;
        let len_idx = row_index(row, len.len(), "sub_bitmap", 2, chunk.len())?;
        let offset = i64_arg_at(&offset, offset_idx, "sub_bitmap", 1)?;
        let len = i64_arg_at(&len, len_idx, "sub_bitmap", 2)?;
        if bitmap.is_null(bitmap_idx) || offset.is_none() || len.is_none() {
            builder.append_null();
            continue;
        }
        let len = len.expect("checked");
        if len <= 0 {
            builder.append_null();
            continue;
        }
        let values = match super::bitmap_common::decode_bitmap(bitmap.value(bitmap_idx)) {
            Ok(values) => values.into_iter().collect::<Vec<_>>(),
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let cardinality = i64::try_from(values.len())
            .map_err(|_| format!("sub_bitmap cardinality overflow: len={}", values.len()))?;
        let offset = offset.expect("checked");
        let offset_abs = offset.checked_abs().unwrap_or(i64::MAX);
        let out_of_range =
            (offset > 0 && offset >= cardinality) || (offset < 0 && offset_abs > cardinality);
        if values.is_empty() || out_of_range {
            builder.append_null();
            continue;
        }

        let start = if offset < 0 {
            cardinality + offset
        } else {
            offset
        };
        if start < 0 {
            builder.append_null();
            continue;
        }
        let start = usize::try_from(start)
            .map_err(|_| format!("sub_bitmap start index overflow: {}", start))?;
        let take = usize::try_from(len).unwrap_or(usize::MAX);
        let selected: Vec<u64> = values.into_iter().skip(start).take(take).collect();
        if selected.is_empty() {
            builder.append_null();
            continue;
        }
        builder.append_value(encode_bitmap_values(&selected)?);
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_subset_limit(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let bitmap = arena.eval(args[0], chunk)?;
    let range_start = arena.eval(args[1], chunk)?;
    let limit = arena.eval(args[2], chunk)?;

    let bitmap = downcast_binary(&bitmap, "bitmap_subset_limit", 0)?;
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let bitmap_idx = row_index(row, bitmap.len(), "bitmap_subset_limit", 0, chunk.len())?;
        let start_idx = row_index(
            row,
            range_start.len(),
            "bitmap_subset_limit",
            1,
            chunk.len(),
        )?;
        let limit_idx = row_index(row, limit.len(), "bitmap_subset_limit", 2, chunk.len())?;
        let range_start = i64_arg_at(&range_start, start_idx, "bitmap_subset_limit", 1)?;
        let limit = i64_arg_at(&limit, limit_idx, "bitmap_subset_limit", 2)?;
        if bitmap.is_null(bitmap_idx) || range_start.is_none() || limit.is_none() {
            builder.append_null();
            continue;
        }
        let values = match super::bitmap_common::decode_bitmap(bitmap.value(bitmap_idx)) {
            Ok(values) => values.into_iter().collect::<Vec<_>>(),
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        if values.is_empty() {
            builder.append_null();
            continue;
        }
        let mut range_start = range_start.expect("checked");
        let limit = limit.expect("checked");
        if range_start < 0 {
            range_start = 0;
        }
        let range_start = range_start as u64;
        let selected = if limit < 0 {
            let abs_limit = limit.checked_abs().unwrap_or(i64::MAX) as usize;
            let mut out = Vec::new();
            for &value in values.iter().rev() {
                if value > range_start {
                    continue;
                }
                out.push(value);
                if out.len() >= abs_limit {
                    break;
                }
            }
            out.reverse();
            out
        } else if limit == 0 {
            Vec::new()
        } else {
            let take = usize::try_from(limit).unwrap_or(usize::MAX);
            values
                .into_iter()
                .filter(|value| *value >= range_start)
                .take(take)
                .collect::<Vec<_>>()
        };
        if selected.is_empty() {
            builder.append_null();
            continue;
        }
        builder.append_value(encode_bitmap_values(&selected)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_subset_in_range(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let bitmap = arena.eval(args[0], chunk)?;
    let range_start = arena.eval(args[1], chunk)?;
    let range_end = arena.eval(args[2], chunk)?;

    let bitmap = downcast_binary(&bitmap, "bitmap_subset_in_range", 0)?;
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let bitmap_idx = row_index(row, bitmap.len(), "bitmap_subset_in_range", 0, chunk.len())?;
        let start_idx = row_index(
            row,
            range_start.len(),
            "bitmap_subset_in_range",
            1,
            chunk.len(),
        )?;
        let end_idx = row_index(
            row,
            range_end.len(),
            "bitmap_subset_in_range",
            2,
            chunk.len(),
        )?;
        let range_start = i64_arg_at(&range_start, start_idx, "bitmap_subset_in_range", 1)?;
        let range_end = i64_arg_at(&range_end, end_idx, "bitmap_subset_in_range", 2)?;
        if bitmap.is_null(bitmap_idx) || range_start.is_none() || range_end.is_none() {
            builder.append_null();
            continue;
        }
        let values = match super::bitmap_common::decode_bitmap(bitmap.value(bitmap_idx)) {
            Ok(values) => values.into_iter().collect::<Vec<_>>(),
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        if values.is_empty() {
            builder.append_null();
            continue;
        }
        let mut range_start = range_start.expect("checked");
        let range_end = range_end.expect("checked");
        if range_start < 0 {
            range_start = 0;
        }
        if range_start >= range_end {
            builder.append_null();
            continue;
        }
        if range_end <= 0 {
            builder.append_null();
            continue;
        }
        let range_start = range_start as u64;
        let range_end = range_end as u64;
        let selected: Vec<u64> = values
            .into_iter()
            .filter(|value| *value >= range_start && *value < range_end)
            .collect();
        if selected.is_empty() {
            builder.append_null();
            continue;
        }
        builder.append_value(encode_bitmap_values(&selected)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_to_binary(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let arr = downcast_binary(&input, "bitmap_to_binary", 0)?;
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len(), "bitmap_to_binary", 0, chunk.len())?;
        if arr.is_null(idx) {
            builder.append_null();
            continue;
        }
        let values = match super::bitmap_common::decode_bitmap(arr.value(idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        builder.append_value(super::bitmap_common::encode_external_bitmap(&values)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_from_binary(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let arr = downcast_binary(&input, "bitmap_from_binary", 0)?;
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len(), "bitmap_from_binary", 0, chunk.len())?;
        if arr.is_null(idx) {
            builder.append_null();
            continue;
        }
        let payload = arr.value(idx);
        if payload.is_empty() {
            builder.append_null();
            continue;
        }
        let values = match super::bitmap_common::decode_external_bitmap(payload) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        builder.append_value(super::bitmap_common::encode_internal_bitmap(&values)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_bitmap_to_base64(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let arr = downcast_binary(&input, "bitmap_to_base64", 0)?;
    let mut builder = StringBuilder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, arr.len(), "bitmap_to_base64", 0, chunk.len())?;
        if arr.is_null(idx) {
            builder.append_null();
            continue;
        }
        let values = match super::bitmap_common::decode_bitmap(arr.value(idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let binary = super::bitmap_common::encode_external_bitmap(&values)?;
        builder.append_value(base64::engine::general_purpose::STANDARD.encode(binary));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
