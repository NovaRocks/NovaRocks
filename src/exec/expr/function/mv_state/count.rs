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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Builder, LargeBinaryArray};
use arrow::datatypes::DataType;

use crate::connector::starrocks::managed::state_codec::{decode_count_state, encode_count_state};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

pub(crate) fn count_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    let left = decode_count_state(a)?;
    let right = decode_count_state(b)?;
    let total = left
        .checked_add(right)
        .ok_or_else(|| "count_state_union count overflow".to_string())?;
    if total == 0 {
        Ok(Vec::new())
    } else {
        Ok(encode_count_state(total))
    }
}

pub(crate) fn count_state_visible(s: &[u8]) -> Result<i64, String> {
    decode_count_state(s)
}

pub(crate) fn eval_count_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "count_state_union expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_count_state_union_arrays(&lhs, &rhs)
}

pub(crate) fn eval_count_state_visible(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "count_state_visible expects 1 argument, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    eval_count_state_visible_array(&input)
}

pub(crate) fn eval_count_state_union_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count("count_state_union", lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(lhs, row_index(row, lhs.len())?, "count_state_union", 0)?;
        let right = binary_value_or_empty(rhs, row_index(row, rhs.len())?, "count_state_union", 1)?;
        builder.append_value(count_state_union(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub(crate) fn eval_count_state_visible_array(input: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        let state = binary_value_or_empty(input, row, "count_state_visible", 0)?;
        builder.append_value(count_state_visible(state)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn row_count(fn_name: &str, lhs_len: usize, rhs_len: usize) -> Result<usize, String> {
    if lhs_len == rhs_len {
        return Ok(lhs_len);
    }
    if lhs_len == 1 {
        return Ok(rhs_len);
    }
    if rhs_len == 1 {
        return Ok(lhs_len);
    }
    Err(format!(
        "{} row count mismatch: lhs_len={} rhs_len={}",
        fn_name, lhs_len, rhs_len
    ))
}

fn row_index(row: usize, len: usize) -> Result<usize, String> {
    if len == 1 {
        Ok(0)
    } else if row < len {
        Ok(row)
    } else {
        Err(format!("row index {} out of bounds for len {}", row, len))
    }
}

fn binary_value_or_empty<'a>(
    array: &'a ArrayRef,
    row: usize,
    fn_name: &str,
    arg_idx: usize,
) -> Result<&'a [u8], String> {
    match array.data_type() {
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "{} downcast BinaryArray failed for arg {}",
                        fn_name, arg_idx
                    )
                })?;
            if arr.is_null(row) {
                Ok(&[])
            } else {
                Ok(arr.value(row))
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "{} downcast LargeBinaryArray failed for arg {}",
                        fn_name, arg_idx
                    )
                })?;
            if arr.is_null(row) {
                Ok(&[])
            } else {
                Ok(arr.value(row))
            }
        }
        other => Err(format!(
            "{} expects Binary input for arg {}, got {:?}",
            fn_name, arg_idx, other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::state_codec::{
        decode_count_state, encode_count_state,
    };
    use arrow::array::{Array, BinaryArray, BinaryBuilder, Int64Array};
    use std::sync::Arc;

    fn binary_array(values: &[Option<Vec<u8>>]) -> arrow::array::ArrayRef {
        let mut builder = BinaryBuilder::new();
        for value in values {
            match value {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn count_state_union_sums_counts() {
        let out = count_state_union(&encode_count_state(2), &encode_count_state(3)).unwrap();
        assert_eq!(decode_count_state(&out).unwrap(), 5);
    }

    #[test]
    fn count_state_union_treats_empty_left_as_zero() {
        let out = count_state_union(&[], &encode_count_state(7)).unwrap();
        assert_eq!(decode_count_state(&out).unwrap(), 7);
    }

    #[test]
    fn count_state_union_cancelled_total_returns_empty_state() {
        let out = count_state_union(&encode_count_state(2), &encode_count_state(-2)).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn count_state_visible_decodes_count() {
        assert_eq!(count_state_visible(&encode_count_state(42)).unwrap(), 42);
    }

    #[test]
    fn count_state_visible_empty_returns_zero() {
        assert_eq!(count_state_visible(&[]).unwrap(), 0);
    }

    #[test]
    fn count_state_union_arrays_treat_null_inputs_as_empty_state() {
        let lhs = binary_array(&[
            Some(encode_count_state(2)),
            None,
            Some(encode_count_state(4)),
        ]);
        let rhs = binary_array(&[
            Some(encode_count_state(3)),
            Some(encode_count_state(5)),
            None,
        ]);

        let out = eval_count_state_union_arrays(&lhs, &rhs).unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(decode_count_state(arr.value(0)).unwrap(), 5);
        assert_eq!(decode_count_state(arr.value(1)).unwrap(), 5);
        assert_eq!(decode_count_state(arr.value(2)).unwrap(), 4);
    }

    #[test]
    fn count_state_visible_arrays_treat_null_as_zero() {
        let input = binary_array(&[Some(encode_count_state(9)), None]);

        let out = eval_count_state_visible_array(&input).unwrap();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(arr.value(0), 9);
        assert_eq!(arr.value(1), 0);
    }
}
