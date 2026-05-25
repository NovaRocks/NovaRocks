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
    let arr_opt = as_binary_or_null_array(&input, fn_name)?;

    let len = arr_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut values_i128: Vec<Option<i128>> = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let idx = row_index(row, len, fn_name, 0, chunk.len())?;
        if arr_opt.is_none_or(|arr| arr.is_null(idx)) {
            values_i128.push(None);
            continue;
        }
        let arr = arr_opt.unwrap();
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
    // NullArray (literal NULL) → all-null output
    if input.data_type() == &DataType::Null {
        for _ in 0..chunk.len() {
            builder.append_null();
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
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
    let arr_opt = as_binary_or_null_array(&input, "bitmap_count")?;
    let len = arr_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = Int64Builder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, len, "bitmap_count", 0, chunk.len())?;
        if arr_opt.is_none_or(|arr| arr.is_null(idx)) {
            // NULL bitmap input → NULL output (consistent with StarRocks NULL semantics)
            builder.append_null();
            continue;
        }
        let arr = arr_opt.unwrap();
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
    let lhs_opt = as_binary_or_null_array(&lhs, "bitmap_and")?;
    let rhs_opt = as_binary_or_null_array(&rhs, "bitmap_and")?;

    let lhs_len = lhs_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let rhs_len = rhs_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let lhs_idx = row_index(row, lhs_len, "bitmap_and", 0, chunk.len())?;
        let rhs_idx = row_index(row, rhs_len, "bitmap_and", 1, chunk.len())?;
        if lhs_opt.is_none_or(|a| a.is_null(lhs_idx)) || rhs_opt.is_none_or(|a| a.is_null(rhs_idx))
        {
            builder.append_null();
            continue;
        }
        let left = match super::bitmap_common::decode_bitmap(lhs_opt.unwrap().value(lhs_idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let right = match super::bitmap_common::decode_bitmap(rhs_opt.unwrap().value(rhs_idx)) {
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
    let lhs_opt = as_binary_or_null_array(&lhs, "bitmap_has_any")?;
    let rhs_opt = as_binary_or_null_array(&rhs, "bitmap_has_any")?;

    let lhs_len = lhs_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let rhs_len = rhs_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = BooleanBuilder::new();
    for row in 0..chunk.len() {
        let lhs_idx = row_index(row, lhs_len, "bitmap_has_any", 0, chunk.len())?;
        let rhs_idx = row_index(row, rhs_len, "bitmap_has_any", 1, chunk.len())?;
        if lhs_opt.is_none_or(|a| a.is_null(lhs_idx)) || rhs_opt.is_none_or(|a| a.is_null(rhs_idx))
        {
            builder.append_null();
            continue;
        }
        let left = match super::bitmap_common::decode_bitmap(lhs_opt.unwrap().value(lhs_idx)) {
            Ok(values) => values,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let right = match super::bitmap_common::decode_bitmap(rhs_opt.unwrap().value(rhs_idx)) {
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

    let bitmap_opt = as_binary_or_null_array(&bitmap, "sub_bitmap")?;
    let bitmap_len = bitmap_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = BinaryBuilder::new();

    for row in 0..chunk.len() {
        let bitmap_idx = row_index(row, bitmap_len, "sub_bitmap", 0, chunk.len())?;
        let offset_idx = row_index(row, offset.len(), "sub_bitmap", 1, chunk.len())?;
        let len_idx = row_index(row, len.len(), "sub_bitmap", 2, chunk.len())?;
        let offset = i64_arg_at(&offset, offset_idx, "sub_bitmap", 1)?;
        let len = i64_arg_at(&len, len_idx, "sub_bitmap", 2)?;
        if bitmap_opt.is_none_or(|a| a.is_null(bitmap_idx)) || offset.is_none() || len.is_none() {
            builder.append_null();
            continue;
        }
        let len = len.expect("checked");
        if len <= 0 {
            builder.append_null();
            continue;
        }
        let values =
            match super::bitmap_common::decode_bitmap(bitmap_opt.unwrap().value(bitmap_idx)) {
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

    let bitmap_opt = as_binary_or_null_array(&bitmap, "bitmap_subset_limit")?;
    let bitmap_len = bitmap_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let bitmap_idx = row_index(row, bitmap_len, "bitmap_subset_limit", 0, chunk.len())?;
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
        if bitmap_opt.is_none_or(|a| a.is_null(bitmap_idx))
            || range_start.is_none()
            || limit.is_none()
        {
            builder.append_null();
            continue;
        }
        let values =
            match super::bitmap_common::decode_bitmap(bitmap_opt.unwrap().value(bitmap_idx)) {
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

    let bitmap_opt = as_binary_or_null_array(&bitmap, "bitmap_subset_in_range")?;
    let bitmap_len = bitmap_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let bitmap_idx = row_index(row, bitmap_len, "bitmap_subset_in_range", 0, chunk.len())?;
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
        if bitmap_opt.is_none_or(|a| a.is_null(bitmap_idx))
            || range_start.is_none()
            || range_end.is_none()
        {
            builder.append_null();
            continue;
        }
        let values =
            match super::bitmap_common::decode_bitmap(bitmap_opt.unwrap().value(bitmap_idx)) {
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
    let arr_opt = as_binary_or_null_array(&input, "bitmap_to_binary")?;
    let len = arr_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = BinaryBuilder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, len, "bitmap_to_binary", 0, chunk.len())?;
        if arr_opt.is_none_or(|arr| arr.is_null(idx)) {
            builder.append_null();
            continue;
        }
        let arr = arr_opt.unwrap();
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
    use arrow::datatypes::DataType as DT;
    let input = arena.eval(args[0], chunk)?;
    let input_len = input.len();
    let mut builder = BinaryBuilder::new();
    // Match StarRocks: `bitmap_from_binary` accepts both VARBINARY and
    // VARCHAR — the latter is treated as raw bytes (StarRocks VARCHAR is
    // 8-bit-clean), which is how the user may have stored bitmap binary
    // in a STRING column. Returns NULL on malformed input.
    let bytes_per_row: Result<Vec<Option<Vec<u8>>>, String> = match input.data_type() {
        DT::Null => Ok(vec![None; chunk.len()]),
        DT::Binary => {
            let arr = input.as_any().downcast_ref::<BinaryArray>().unwrap();
            (0..chunk.len())
                .map(|row| {
                    let idx = row_index(row, input_len, "bitmap_from_binary", 0, chunk.len())?;
                    Ok(if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value(idx).to_vec())
                    })
                })
                .collect()
        }
        DT::LargeBinary => {
            let arr = input.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            (0..chunk.len())
                .map(|row| {
                    let idx = row_index(row, input_len, "bitmap_from_binary", 0, chunk.len())?;
                    Ok(if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value(idx).to_vec())
                    })
                })
                .collect()
        }
        DT::Utf8 => {
            let arr = input.as_any().downcast_ref::<StringArray>().unwrap();
            (0..chunk.len())
                .map(|row| {
                    let idx = row_index(row, input_len, "bitmap_from_binary", 0, chunk.len())?;
                    Ok(if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value(idx).as_bytes().to_vec())
                    })
                })
                .collect()
        }
        DT::LargeUtf8 => {
            let arr = input.as_any().downcast_ref::<LargeStringArray>().unwrap();
            (0..chunk.len())
                .map(|row| {
                    let idx = row_index(row, input_len, "bitmap_from_binary", 0, chunk.len())?;
                    Ok(if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value(idx).as_bytes().to_vec())
                    })
                })
                .collect()
        }
        other => {
            return Err(format!(
                "bitmap_from_binary expects BITMAP/BINARY/VARCHAR input, got {other:?}"
            ));
        }
    };
    for opt_payload in bytes_per_row? {
        let Some(payload) = opt_payload else {
            builder.append_null();
            continue;
        };
        if payload.is_empty() {
            builder.append_null();
            continue;
        }
        match super::bitmap_common::decode_external_bitmap(&payload) {
            Ok(values) => {
                builder.append_value(super::bitmap_common::encode_internal_bitmap(&values)?);
            }
            Err(_) => builder.append_null(),
        }
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
    let arr_opt = as_binary_or_null_array(&input, "bitmap_to_base64")?;
    let len = arr_opt.map(|a| a.len()).unwrap_or(chunk.len());
    let mut builder = StringBuilder::new();
    for row in 0..chunk.len() {
        let idx = row_index(row, len, "bitmap_to_base64", 0, chunk.len())?;
        if arr_opt.is_none_or(|arr| arr.is_null(idx)) {
            builder.append_null();
            continue;
        }
        let arr = arr_opt.unwrap();
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

// ──────────────────────────────────────────────────────────────────────────────
// Binary bitmap set-operation helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Return a `BinaryArray` view of `arr`, or `None` if it is an all-null `NullArray`.
/// Any other array type that is not BinaryArray returns an `Err`.
fn as_binary_or_null_array<'a>(
    arr: &'a ArrayRef,
    fn_name: &str,
) -> Result<Option<&'a BinaryArray>, String> {
    use arrow::datatypes::DataType as DT;
    match arr.data_type() {
        DT::Null => Ok(None),
        DT::Binary => Ok(Some(arr.as_any().downcast_ref::<BinaryArray>().unwrap())),
        other => Err(format!(
            "{fn_name} expects BITMAP/BINARY input, got {other:?}"
        )),
    }
}

fn bitmap_binary_op(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    op: impl Fn(&BTreeSet<u64>, &BTreeSet<u64>) -> BTreeSet<u64>,
) -> Result<ArrayRef, String> {
    let lhs_bin = as_binary_or_null_array(lhs, "bitmap op")?;
    let rhs_bin = as_binary_or_null_array(rhs, "bitmap op")?;

    // Determine result length from whichever side is not a bare NullArray.
    let len = match (lhs_bin, rhs_bin) {
        (Some(l), Some(r)) => {
            if l.len() != r.len() {
                return Err(format!(
                    "bitmap op length mismatch: lhs={} rhs={}",
                    l.len(),
                    r.len()
                ));
            }
            l.len()
        }
        (Some(l), None) => l.len(),
        (None, Some(r)) => r.len(),
        (None, None) => lhs.len(),
    };

    let mut out = BinaryBuilder::new();
    for i in 0..len {
        let lhs_null = lhs_bin.is_none_or(|a| a.is_null(i));
        let rhs_null = rhs_bin.is_none_or(|a| a.is_null(i));
        if lhs_null || rhs_null {
            out.append_null();
            continue;
        }
        let a = super::bitmap_common::decode_bitmap(lhs_bin.unwrap().value(i))?;
        let b = super::bitmap_common::decode_bitmap(rhs_bin.unwrap().value(i))?;
        let merged = op(&a, &b);
        out.append_value(super::bitmap_common::encode_internal_bitmap(&merged)?);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

pub(crate) fn eval_bitmap_or_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.union(b).copied().collect())
}

pub(crate) fn eval_bitmap_xor_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| {
        a.symmetric_difference(b).copied().collect()
    })
}

pub(crate) fn eval_bitmap_andnot_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.difference(b).copied().collect())
}

pub(crate) fn eval_bitmap_intersect_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    bitmap_binary_op(lhs, rhs, |a, b| a.intersection(b).copied().collect())
}

pub(crate) fn eval_bitmap_contains_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    use arrow::datatypes::DataType as DT;
    // Handle all-null literal for the bitmap argument.
    if lhs.data_type() == &DT::Null || rhs.data_type() == &DT::Null {
        let len = if lhs.data_type() != &DT::Null {
            lhs.len()
        } else if rhs.data_type() != &DT::Null {
            rhs.len()
        } else {
            lhs.len()
        };
        let mut out = BooleanBuilder::new();
        for _ in 0..len {
            out.append_null();
        }
        return Ok(Arc::new(out.finish()) as ArrayRef);
    }
    let lhs = lhs
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "bitmap_contains expects BITMAP/BINARY input for arg 1".to_string())?;
    let rhs = rhs
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "bitmap_contains expects BIGINT input for arg 2".to_string())?;
    if lhs.len() != rhs.len() {
        return Err(format!(
            "bitmap_contains length mismatch: lhs={} rhs={}",
            lhs.len(),
            rhs.len()
        ));
    }
    let mut out = BooleanBuilder::new();
    for i in 0..lhs.len() {
        if lhs.is_null(i) || rhs.is_null(i) {
            out.append_null();
            continue;
        }
        let a = super::bitmap_common::decode_bitmap(lhs.value(i))?;
        let v = rhs.value(i);
        out.append_value(v >= 0 && a.contains(&(v as u64)));
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

pub fn eval_bitmap_or(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!("bitmap_or expects 2 arguments, got {}", args.len()));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_or_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_xor(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "bitmap_xor expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_xor_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_andnot(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "bitmap_andnot expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_andnot_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_intersect(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "bitmap_intersect expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_intersect_arrays(&lhs, &rhs)
}

pub fn eval_bitmap_contains(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "bitmap_contains expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_bitmap_contains_arrays(&lhs, &rhs)
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests for binary bitmap operations
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod bitmap_binary_op_tests {
    use super::*;
    use crate::exec::expr::function::object::bitmap_common::{
        decode_bitmap, encode_internal_bitmap,
    };
    use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Int64Array};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn encode(values: &[u64]) -> Vec<u8> {
        let set: BTreeSet<u64> = values.iter().copied().collect();
        encode_internal_bitmap(&set).expect("encode")
    }

    fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
        let mut b = BinaryBuilder::new();
        for v in values {
            match v {
                Some(bs) => b.append_value(bs),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish()) as ArrayRef
    }

    fn decode_row(arr: &BinaryArray, row: usize) -> Vec<u64> {
        decode_bitmap(arr.value(row))
            .expect("decode")
            .into_iter()
            .collect()
    }

    #[test]
    fn bitmap_or_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2])), Some(encode(&[]))]);
        let rhs = binary_array(&[Some(encode(&[3])), Some(encode(&[42]))]);
        let out = eval_bitmap_or_arrays(&lhs, &rhs).expect("or");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![1, 2, 3]);
        assert_eq!(decode_row(arr, 1), vec![42]);
    }

    #[test]
    fn bitmap_xor_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2, 3, 4]))]);
        let out = eval_bitmap_xor_arrays(&lhs, &rhs).expect("xor");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![1, 4]);
    }

    #[test]
    fn bitmap_andnot_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2]))]);
        let out = eval_bitmap_andnot_arrays(&lhs, &rhs).expect("andnot");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![1, 3]);
    }

    #[test]
    fn bitmap_intersect_scalar_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2, 3, 4]))]);
        let out = eval_bitmap_intersect_arrays(&lhs, &rhs).expect("intersect");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![2, 3]);
    }

    #[test]
    fn bitmap_contains_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 5, 9])), Some(encode(&[1, 5, 9]))]);
        let rhs = Arc::new(Int64Array::from(vec![5, 2])) as ArrayRef;
        let out = eval_bitmap_contains_arrays(&lhs, &rhs).expect("contains");
        let arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn bitmap_or_propagates_nulls() {
        let lhs = binary_array(&[None]);
        let rhs = binary_array(&[Some(encode(&[1]))]);
        let out = eval_bitmap_or_arrays(&lhs, &rhs).expect("or");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(arr.is_null(0));
    }

    // ── helpers for refactored functions ──────────────────────────────────────

    fn null_array(len: usize) -> ArrayRef {
        Arc::new(arrow::array::NullArray::new(len)) as ArrayRef
    }

    fn eval_bitmap_and_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
        let lhs_opt = as_binary_or_null_array(lhs, "bitmap_and")?;
        let rhs_opt = as_binary_or_null_array(rhs, "bitmap_and")?;
        let lhs_len = lhs_opt.map(|a| a.len()).unwrap_or(lhs.len());
        let rhs_len = rhs_opt.map(|a| a.len()).unwrap_or(rhs.len());
        let chunk_len = lhs_len.max(rhs_len);
        let mut builder = BinaryBuilder::new();
        for row in 0..chunk_len {
            let lhs_idx = if lhs_len == 1 { 0 } else { row };
            let rhs_idx = if rhs_len == 1 { 0 } else { row };
            if lhs_opt.is_none_or(|a| a.is_null(lhs_idx))
                || rhs_opt.is_none_or(|a| a.is_null(rhs_idx))
            {
                builder.append_null();
                continue;
            }
            let left = decode_bitmap(lhs_opt.unwrap().value(lhs_idx)).unwrap();
            let right = decode_bitmap(rhs_opt.unwrap().value(rhs_idx)).unwrap();
            let values: Vec<u64> = left.iter().filter(|v| right.contains(v)).copied().collect();
            builder.append_value(
                encode_internal_bitmap(&values.iter().copied().collect::<BTreeSet<_>>()).unwrap(),
            );
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    fn eval_bitmap_has_any_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
        use arrow::array::BooleanBuilder;
        let lhs_opt = as_binary_or_null_array(lhs, "bitmap_has_any")?;
        let rhs_opt = as_binary_or_null_array(rhs, "bitmap_has_any")?;
        let lhs_len = lhs_opt.map(|a| a.len()).unwrap_or(lhs.len());
        let rhs_len = rhs_opt.map(|a| a.len()).unwrap_or(rhs.len());
        let chunk_len = lhs_len.max(rhs_len);
        let mut builder = BooleanBuilder::new();
        for row in 0..chunk_len {
            let lhs_idx = if lhs_len == 1 { 0 } else { row };
            let rhs_idx = if rhs_len == 1 { 0 } else { row };
            if lhs_opt.is_none_or(|a| a.is_null(lhs_idx))
                || rhs_opt.is_none_or(|a| a.is_null(rhs_idx))
            {
                builder.append_null();
                continue;
            }
            let left = decode_bitmap(lhs_opt.unwrap().value(lhs_idx)).unwrap();
            let right = decode_bitmap(rhs_opt.unwrap().value(rhs_idx)).unwrap();
            let has_any = left.iter().any(|v| right.contains(v));
            builder.append_value(has_any);
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    fn eval_bitmap_count_arrays(input: &ArrayRef) -> Result<ArrayRef, String> {
        use arrow::array::Int64Builder;
        let arr_opt = as_binary_or_null_array(input, "bitmap_count")?;
        let len = arr_opt.map(|a| a.len()).unwrap_or(input.len());
        let mut builder = Int64Builder::new();
        for i in 0..len {
            if arr_opt.is_none_or(|a| a.is_null(i)) {
                builder.append_null();
                continue;
            }
            match decode_bitmap(arr_opt.unwrap().value(i)) {
                Ok(values) => builder.append_value(values.len() as i64),
                Err(_) => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    // ── NULL propagation tests for refactored functions ───────────────────────

    #[test]
    fn as_binary_or_null_array_handles_null_array() {
        let arr = null_array(3);
        let result = as_binary_or_null_array(&arr, "test").expect("should not error");
        assert!(result.is_none(), "NullArray should map to None");
    }

    #[test]
    fn as_binary_or_null_array_rejects_wrong_type() {
        let arr = Arc::new(Int64Array::from(vec![1i64])) as ArrayRef;
        assert!(as_binary_or_null_array(&arr, "test").is_err());
    }

    #[test]
    fn bitmap_and_propagates_null_literal_lhs() {
        let lhs = null_array(1);
        let rhs = binary_array(&[Some(encode(&[1]))]);
        let out = eval_bitmap_and_arrays(&lhs, &rhs).expect("and");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn bitmap_and_propagates_null_literal_rhs() {
        let lhs = binary_array(&[Some(encode(&[1]))]);
        let rhs = null_array(1);
        let out = eval_bitmap_and_arrays(&lhs, &rhs).expect("and");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn bitmap_and_propagates_null_value() {
        let lhs = binary_array(&[None]);
        let rhs = binary_array(&[Some(encode(&[1]))]);
        let out = eval_bitmap_and_arrays(&lhs, &rhs).expect("and");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn bitmap_and_basic() {
        let lhs = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let rhs = binary_array(&[Some(encode(&[2, 3, 4]))]);
        let out = eval_bitmap_and_arrays(&lhs, &rhs).expect("and");
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(decode_row(arr, 0), vec![2, 3]);
    }

    #[test]
    fn bitmap_has_any_propagates_null_literal() {
        let lhs = null_array(1);
        let rhs = binary_array(&[Some(encode(&[1]))]);
        let out = eval_bitmap_has_any_arrays(&lhs, &rhs).expect("has_any");
        assert!(out.is_null(0));
    }

    #[test]
    fn bitmap_has_any_propagates_null_value() {
        let lhs = binary_array(&[None]);
        let rhs = binary_array(&[Some(encode(&[1]))]);
        let out = eval_bitmap_has_any_arrays(&lhs, &rhs).expect("has_any");
        assert!(out.is_null(0));
    }

    #[test]
    fn bitmap_count_propagates_null_literal() {
        let input = null_array(1);
        let out = eval_bitmap_count_arrays(&input).expect("count");
        assert!(out.is_null(0));
    }

    #[test]
    fn bitmap_count_propagates_null_value() {
        let input = binary_array(&[None]);
        let out = eval_bitmap_count_arrays(&input).expect("count");
        assert!(out.is_null(0));
    }

    #[test]
    fn bitmap_count_basic() {
        let input = binary_array(&[Some(encode(&[1, 2, 3]))]);
        let out = eval_bitmap_count_arrays(&input).expect("count");
        let arr = out
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 3);
    }
}
