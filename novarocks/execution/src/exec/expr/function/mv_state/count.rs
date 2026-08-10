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

use arrow::array::{Array, ArrayRef, BinaryBuilder, BooleanBuilder, Int64Array, Int64Builder};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::mv::state_codec::{decode_count_state, encode_count_state};

use super::common::{binary_value_or_empty, row_count, row_index};

pub fn count_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
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

pub fn count_state_visible(s: &[u8]) -> Result<i64, String> {
    decode_count_state(s)
}

pub fn state_all_zero(s: &[u8]) -> Result<bool, String> {
    count_state_visible(s).map(|count| count == 0)
}

pub fn eval_count_state_union(
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

pub fn eval_count_state_visible(
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

pub fn eval_state_all_zero(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "state_all_zero expects 1 argument, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    eval_state_all_zero_array(&input)
}

pub fn eval_count_state_union_arrays(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef, String> {
    let rows = row_count("count_state_union", lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(lhs, row_index(row, lhs.len())?, "count_state_union", 0)?;
        let right = binary_value_or_empty(rhs, row_index(row, rhs.len())?, "count_state_union", 1)?;
        builder.append_value(count_state_union(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_count_state_visible_array(input: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        let state = binary_value_or_empty(input, row, "count_state_visible", 0)?;
        builder.append_value(count_state_visible(state)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_state_all_zero_array(input: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = BooleanBuilder::new();
    match input.data_type() {
        arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
            for row in 0..input.len() {
                let state = binary_value_or_empty(input, row, "state_all_zero", 0)?;
                builder.append_value(state_all_zero(state)?);
            }
        }
        arrow::datatypes::DataType::Int64 => {
            let counts = input
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "state_all_zero downcast Int64Array failed for arg 0".to_string())?;
            for row in 0..input.len() {
                builder.append_value(counts.is_null(row) || counts.value(row) == 0);
            }
        }
        other => {
            return Err(format!(
                "state_all_zero expects Binary, LargeBinary, or Int64 input for arg 0, got {other:?}"
            ));
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::mv::state_codec::{decode_count_state, encode_count_state};
    use arrow::array::{Array, BinaryArray, BinaryBuilder, BooleanArray, Int64Array};
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

    #[test]
    fn state_all_zero_treats_empty_and_null_count_state_as_zero() {
        assert!(state_all_zero(&[]).unwrap());

        let input = binary_array(&[Some(Vec::new()), None]);
        let out = eval_state_all_zero_array(&input).unwrap();
        let arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(arr.value(0));
        assert!(arr.value(1));
    }

    #[test]
    fn state_all_zero_accepts_int64_retraction_count() {
        let input = Arc::new(Int64Array::from(vec![Some(0), Some(2), Some(-1), None]));
        let out = eval_state_all_zero_array(&(input as ArrayRef)).unwrap();
        let arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(arr.value(0));
        assert!(!arr.value(1));
        assert!(!arr.value(2));
        assert!(arr.value(3));
    }

    #[test]
    fn state_all_zero_rejects_positive_and_negative_count_states() {
        assert!(!state_all_zero(&encode_count_state(2)).unwrap());
        assert!(!state_all_zero(&encode_count_state(-1)).unwrap());
    }

    #[test]
    fn state_all_zero_does_not_treat_zero_visible_sum_as_empty_group() {
        let net_zero_sum = crate::exec::mv::state_codec::encode_sum_int64(2, 0);
        let (rows, sum) = crate::exec::mv::state_codec::decode_sum_int64(&net_zero_sum).unwrap();
        assert_eq!((rows, sum), (2, 0));

        assert!(!state_all_zero(&encode_count_state(2)).unwrap());
    }

    #[test]
    fn state_all_zero_rejects_invalid_count_state_bytes() {
        let err = state_all_zero(&[0x01]).expect_err("invalid count state should fail");
        assert!(err.contains("Count"), "{err}");
    }
}
