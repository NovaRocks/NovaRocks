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

use arrow::array::{ArrayRef, BinaryBuilder, Decimal128Builder, Int64Builder};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::mv::aggregate_state::state_codec::{
    decode_sum_decimal128, decode_sum_int64, encode_sum_decimal128, encode_sum_int64,
};

use super::common::{binary_value_or_empty, row_count, row_index};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SumStateLayout {
    Empty,
    Int64,
    Decimal128,
}

pub(crate) fn sum_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    match (sum_state_layout(a)?, sum_state_layout(b)?) {
        (SumStateLayout::Empty, SumStateLayout::Empty) => Ok(Vec::new()),
        (SumStateLayout::Int64, SumStateLayout::Empty)
        | (SumStateLayout::Empty, SumStateLayout::Int64)
        | (SumStateLayout::Int64, SumStateLayout::Int64) => {
            let (left_count, left_sum) = decode_sum_int64(a)?;
            let (right_count, right_sum) = decode_sum_int64(b)?;
            let row_count = left_count
                .checked_add(right_count)
                .ok_or_else(|| "sum_state_union row count overflow".to_string())?;
            let sum = left_sum
                .checked_add(right_sum)
                .ok_or_else(|| "sum_state_union int64 sum overflow".to_string())?;
            if row_count == 0 && sum == 0 {
                Ok(Vec::new())
            } else {
                Ok(encode_sum_int64(row_count, sum))
            }
        }
        (SumStateLayout::Decimal128, SumStateLayout::Empty)
        | (SumStateLayout::Empty, SumStateLayout::Decimal128)
        | (SumStateLayout::Decimal128, SumStateLayout::Decimal128) => {
            let (left_count, left_sum) = decode_sum_decimal128(a)?;
            let (right_count, right_sum) = decode_sum_decimal128(b)?;
            let row_count = left_count
                .checked_add(right_count)
                .ok_or_else(|| "sum_state_union row count overflow".to_string())?;
            let sum = left_sum
                .checked_add(right_sum)
                .ok_or_else(|| "sum_state_union decimal128 sum overflow".to_string())?;
            if row_count == 0 && sum == 0 {
                Ok(Vec::new())
            } else {
                Ok(encode_sum_decimal128(row_count, sum))
            }
        }
        (left, right) => Err(format!(
            "sum_state_union layout mismatch: left={left:?} right={right:?}"
        )),
    }
}

pub(crate) fn eval_sum_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "sum_state_union expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_sum_state_union_arrays(&lhs, &rhs)
}

pub(crate) fn eval_sum_state_visible(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if !(1..=2).contains(&args.len()) {
        return Err(format!(
            "sum_state_visible expects 1 or 2 arguments, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    // First-refresh physicalization may attach a typed NULL witness to keep
    // the opaque state decoder aligned with the target MV column. The native
    // plan still carries the historical one-argument function metadata, so
    // the witness—not the legacy call result annotation—is authoritative for
    // the BE-local decoder when it is present.
    let output_type = args
        .get(1)
        .and_then(|witness| arena.data_type(*witness))
        .cloned()
        .or_else(|| arena.data_type(expr).cloned())
        .unwrap_or(DataType::Int64);
    eval_sum_state_visible_array(&input, &output_type)
}

pub(crate) fn eval_sum_state_union_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count("sum_state_union", lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(lhs, row_index(row, lhs.len())?, "sum_state_union", 0)?;
        let right = binary_value_or_empty(rhs, row_index(row, rhs.len())?, "sum_state_union", 1)?;
        builder.append_value(sum_state_union(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub(crate) fn eval_sum_state_visible_array(
    input: &ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    match output_type {
        DataType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(input.len())
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for row in 0..input.len() {
                let state = binary_value_or_empty(input, row, "sum_state_visible", 0)?;
                match sum_state_visible_as_decimal128_for_output(state)? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 | DataType::Null => {
            let mut builder = Int64Builder::new();
            for row in 0..input.len() {
                let state = binary_value_or_empty(input, row, "sum_state_visible", 0)?;
                match sum_state_visible_as_int64(state)? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(format!(
            "sum_state_visible expects Int64 or Decimal128 output type, got {other:?}"
        )),
    }
}

fn sum_state_layout(bytes: &[u8]) -> Result<SumStateLayout, String> {
    match bytes.len() {
        0 => Ok(SumStateLayout::Empty),
        17 => Ok(SumStateLayout::Int64),
        25 => Ok(SumStateLayout::Decimal128),
        len => Err(format!("sum_state invalid state length: {len}")),
    }
}

fn sum_state_visible_as_int64(s: &[u8]) -> Result<Option<i64>, String> {
    let (row_count, sum) = decode_sum_int64(s)?;
    if row_count == 0 {
        Ok(None)
    } else {
        Ok(Some(sum))
    }
}

fn sum_state_visible_as_decimal128(s: &[u8]) -> Result<Option<i128>, String> {
    let (row_count, sum) = decode_sum_decimal128(s)?;
    if row_count == 0 {
        Ok(None)
    } else {
        Ok(Some(sum))
    }
}

// SUM's stored state layout is chosen by the aggregate input type, while SQL
// may widen an integral SUM to DECIMAL in the visible MV schema. Decode the
// opaque state by its self-identifying fixed width, then widen only the
// returned BE value. This keeps the provider-owned state opaque to FE while
// avoiding an invalid assumption that output and state layouts coincide.
fn sum_state_visible_as_decimal128_for_output(s: &[u8]) -> Result<Option<i128>, String> {
    match sum_state_layout(s)? {
        SumStateLayout::Empty => Ok(None),
        SumStateLayout::Int64 => sum_state_visible_as_int64(s).map(|value| value.map(i128::from)),
        SumStateLayout::Decimal128 => sum_state_visible_as_decimal128(s),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Decimal128Array, Int64Array};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::mv::aggregate_state::state_codec::{
        decode_sum_decimal128, decode_sum_int64, encode_sum_decimal128, encode_sum_int64,
    };

    fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
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
    fn sum_state_union_int64_sums_row_count_and_sum() {
        let out = sum_state_union(&encode_sum_int64(2, 30), &encode_sum_int64(3, -5)).unwrap();
        assert_eq!(decode_sum_int64(&out).unwrap(), (5, 25));
    }

    #[test]
    fn sum_state_union_decimal_sums_row_count_and_sum() {
        let out = sum_state_union(
            &encode_sum_decimal128(2, 1_000_000),
            &encode_sum_decimal128(3, -250_000),
        )
        .unwrap();
        assert_eq!(decode_sum_decimal128(&out).unwrap(), (5, 750_000));
    }

    #[test]
    fn sum_state_union_empty_left_returns_right_state() {
        let right = encode_sum_int64(7, 42);
        let out = sum_state_union(&[], &right).unwrap();
        assert_eq!(decode_sum_int64(&out).unwrap(), (7, 42));
    }

    #[test]
    fn sum_state_union_cancelled_zero_count_and_sum_returns_empty() {
        let out = sum_state_union(&encode_sum_int64(2, 30), &encode_sum_int64(-2, -30)).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn sum_state_union_zero_count_nonzero_sum_stays_non_empty() {
        let out = sum_state_union(&encode_sum_int64(2, 30), &encode_sum_int64(-2, -20)).unwrap();
        assert_eq!(decode_sum_int64(&out).unwrap(), (0, 10));
    }

    #[test]
    fn sum_state_union_mismatched_layouts_error() {
        let err = sum_state_union(&encode_sum_int64(1, 10), &encode_sum_decimal128(1, 10))
            .expect_err("mixed int64 and decimal states should fail");
        assert!(err.contains("layout mismatch"));
    }

    #[test]
    fn sum_state_visible_empty_and_zero_count_return_none() {
        assert_eq!(sum_state_visible_as_int64(&[]).unwrap(), None);
        assert_eq!(
            sum_state_visible_as_int64(&encode_sum_int64(0, 0)).unwrap(),
            None
        );
        assert_eq!(
            sum_state_visible_as_decimal128(&encode_sum_decimal128(0, 12345)).unwrap(),
            None
        );
    }

    #[test]
    fn sum_state_visible_int64_and_decimal_return_values() {
        assert_eq!(
            sum_state_visible_as_int64(&encode_sum_int64(2, 25)).unwrap(),
            Some(25)
        );
        assert_eq!(
            sum_state_visible_as_decimal128(&encode_sum_decimal128(2, 750_000)).unwrap(),
            Some(750_000)
        );
    }

    #[test]
    fn sum_state_union_arrays_treat_null_inputs_as_empty_state() {
        let lhs = binary_array(&[Some(encode_sum_int64(2, 30)), None]);
        let rhs = binary_array(&[Some(encode_sum_int64(3, -5)), Some(encode_sum_int64(4, 9))]);

        let out = eval_sum_state_union_arrays(&lhs, &rhs).unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(decode_sum_int64(arr.value(0)).unwrap(), (5, 25));
        assert_eq!(decode_sum_int64(arr.value(1)).unwrap(), (4, 9));
    }

    #[test]
    fn sum_state_visible_array_returns_nullable_int64_values() {
        let input = binary_array(&[
            Some(encode_sum_int64(2, 25)),
            Some(encode_sum_int64(0, 99)),
            None,
        ]);

        let out = eval_sum_state_visible_array(&input, &DataType::Int64).unwrap();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(arr.value(0), 25);
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn sum_state_visible_array_returns_nullable_decimal128_values() {
        let input = binary_array(&[
            Some(encode_sum_decimal128(2, 750_000)),
            Some(encode_sum_decimal128(0, 12345)),
            None,
        ]);

        let out = eval_sum_state_visible_array(&input, &DataType::Decimal128(18, 6)).unwrap();
        let arr = out.as_any().downcast_ref::<Decimal128Array>().unwrap();

        assert_eq!(arr.value(0), 750_000);
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
        assert_eq!(arr.data_type(), &DataType::Decimal128(18, 6));
    }
}
