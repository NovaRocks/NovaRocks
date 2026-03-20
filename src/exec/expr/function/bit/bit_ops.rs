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
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int64Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

fn to_i64_array(array: &ArrayRef, fn_name: &str, arg_idx: usize) -> Result<Int64Array, String> {
    let casted = cast(array, &DataType::Int64).map_err(|e| {
        format!(
            "{}: failed to cast arg{} to BIGINT: {}",
            fn_name, arg_idx, e
        )
    })?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| format!("{}: arg{} is not BIGINT", fn_name, arg_idx))
}

fn to_i128_values(
    array: &ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<Vec<Option<i128>>, String> {
    match array.data_type() {
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| format!("{}: arg{} is not LARGEINT", fn_name, arg_idx))?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(largeint::i128_from_be_bytes(arr.value(row))?));
                }
            }
            Ok(out)
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| format!("{}: arg{} is not UINT64", fn_name, arg_idx))?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(arr.value(row) as i128));
                }
            }
            Ok(out)
        }
        DataType::Decimal128(_, scale) if *scale == 0 => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{}: arg{} is not DECIMAL128", fn_name, arg_idx))?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(arr.value(row)));
                }
            }
            Ok(out)
        }
        DataType::Null => Ok(vec![None; array.len()]),
        _ => {
            let casted = to_i64_array(array, fn_name, arg_idx)?;
            let mut out = Vec::with_capacity(casted.len());
            for row in 0..casted.len() {
                if casted.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(casted.value(row) as i128));
                }
            }
            Ok(out)
        }
    }
}

fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    cast(&out, target).map_err(|e| format!("{}: failed to cast output: {}", fn_name, e))
}

fn cast_largeint_output(
    values: &[Option<i128>],
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    match output_type {
        None => largeint::array_from_i128(values),
        Some(t) if largeint::is_largeint_data_type(t) => largeint::array_from_i128(values),
        Some(t) => {
            let out_i64: Vec<Option<i64>> = values.iter().map(|v| v.map(|x| x as i64)).collect();
            let out = Arc::new(Int64Array::from(out_i64)) as ArrayRef;
            cast_output(out, Some(t), fn_name)
        }
    }
}

fn use_largeint_path(arena: &ExprArena, expr: ExprId, args: &[ExprId]) -> bool {
    if arena
        .data_type(expr)
        .map(largeint::is_largeint_data_type)
        .unwrap_or(false)
    {
        return true;
    }
    args.iter().any(|arg| {
        arena
            .data_type(*arg)
            .map(largeint::is_largeint_data_type)
            .unwrap_or(false)
    })
}

fn eval_unary_i64<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64) -> i64,
{
    let array = arena.eval(args[0], chunk)?;
    let values = to_i64_array(&array, fn_name, 0)?;

    let mut out = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(func(values.value(row))));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    cast_output(out, arena.data_type(expr), fn_name)
}

fn eval_unary_i128<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i128) -> i128,
{
    let array = arena.eval(args[0], chunk)?;
    let values = to_i128_values(&array, fn_name, 0)?;
    let out: Vec<Option<i128>> = values.into_iter().map(|v| v.map(&func)).collect();
    cast_largeint_output(&out, arena.data_type(expr), fn_name)
}

fn eval_binary_i64<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64, i64) -> i64,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i64_array(&left, fn_name, 0)?;
    let right = to_i64_array(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(func(left.value(row), right.value(row))));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    cast_output(out, arena.data_type(expr), fn_name)
}

fn eval_binary_i128<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i128, i128) -> i128,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i128_values(&left, fn_name, 0)?;
    let right = to_i128_values(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        out.push(match (left[row], right[row]) {
            (Some(l), Some(r)) => Some(func(l, r)),
            _ => None,
        });
    }

    cast_largeint_output(&out, arena.data_type(expr), fn_name)
}

fn eval_shift_i64<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64, u32) -> i64,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i64_array(&left, fn_name, 0)?;
    let right = to_i64_array(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(func(left.value(row), right.value(row) as u32)));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    cast_output(out, arena.data_type(expr), fn_name)
}

fn eval_shift_i128<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i128, u32) -> i128,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i128_values(&left, fn_name, 0)?;
    let right = to_i128_values(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        out.push(match (left[row], right[row]) {
            (Some(l), Some(r)) => Some(func(l, r as u32)),
            _ => None,
        });
    }

    cast_largeint_output(&out, arena.data_type(expr), fn_name)
}

pub fn eval_bit_shift_left(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_shift_i128("bit_shift_left", arena, expr, args, chunk, |a, b| {
            a.wrapping_shl(b)
        });
    }
    eval_shift_i64("bit_shift_left", arena, expr, args, chunk, |a, b| {
        a.wrapping_shl(b)
    })
}

pub fn eval_bit_shift_right(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_shift_i128("bit_shift_right", arena, expr, args, chunk, |a, b| {
            a.wrapping_shr(b)
        });
    }
    eval_shift_i64("bit_shift_right", arena, expr, args, chunk, |a, b| {
        a.wrapping_shr(b)
    })
}

pub fn eval_bit_shift_right_logical(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_shift_i128(
            "bit_shift_right_logical",
            arena,
            expr,
            args,
            chunk,
            |a, b| ((a as u128).wrapping_shr(b)) as i128,
        );
    }
    eval_shift_i64(
        "bit_shift_right_logical",
        arena,
        expr,
        args,
        chunk,
        |a, b| ((a as u64).wrapping_shr(b)) as i64,
    )
}

pub fn eval_bitand(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_binary_i128("bitand", arena, expr, args, chunk, |a, b| a & b);
    }
    eval_binary_i64("bitand", arena, expr, args, chunk, |a, b| a & b)
}

pub fn eval_bitnot(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_unary_i128("bitnot", arena, expr, args, chunk, |a| !a);
    }
    eval_unary_i64("bitnot", arena, expr, args, chunk, |a| !a)
}

pub fn eval_bitor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_binary_i128("bitor", arena, expr, args, chunk, |a, b| a | b);
    }
    eval_binary_i64("bitor", arena, expr, args, chunk, |a, b| a | b)
}

pub fn eval_bitxor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_binary_i128("bitxor", arena, expr, args, chunk, |a, b| a ^ b);
    }
    eval_binary_i64("bitxor", arena, expr, args, chunk, |a, b| a ^ b)
}
