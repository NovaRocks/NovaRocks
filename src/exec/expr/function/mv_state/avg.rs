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

use arrow::array::{Array, ArrayRef, BinaryBuilder, Decimal128Builder, Float64Builder, Int64Array};
use arrow::datatypes::DataType;

use crate::connector::starrocks::managed::state_codec::{decode_avg_decimal128, decode_avg_int64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::decimal::{div_round_i128, pow10_i128};
use crate::exec::expr::{ExprArena, ExprId};

use super::common::{binary_value_or_empty, row_count, row_index};

pub(crate) fn avg_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    super::sum::sum_state_union(a, b)
}

pub(crate) fn eval_avg_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "avg_state_union expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_avg_state_union_arrays(&lhs, &rhs)
}

pub(crate) fn eval_avg_state_visible(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if !(1..=2).contains(&args.len()) {
        return Err(format!(
            "avg_state_visible expects 1 or 2 arguments, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    let input_scale = if args.len() == 2 {
        Some(arena.eval(args[1], chunk)?)
    } else {
        None
    };
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Float64);
    eval_avg_state_visible_array(&input, input_scale.as_ref(), &output_type)
}

pub(crate) fn eval_avg_state_union_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count("avg_state_union", lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(lhs, row_index(row, lhs.len())?, "avg_state_union", 0)?;
        let right = binary_value_or_empty(rhs, row_index(row, rhs.len())?, "avg_state_union", 1)?;
        builder.append_value(avg_state_union(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub(crate) fn eval_avg_state_visible_array(
    input: &ArrayRef,
    input_scale: Option<&ArrayRef>,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let rows = if let Some(input_scale) = input_scale {
        row_count("avg_state_visible", input.len(), input_scale.len())?
    } else {
        input.len()
    };
    match output_type {
        DataType::Float64 | DataType::Null => {
            if input_scale.is_some() {
                return Err(
                    "avg_state_visible input decimal scale requires Decimal128 output type"
                        .to_string(),
                );
            }
            let mut builder = Float64Builder::new();
            for row in 0..rows {
                let state = binary_value_or_empty(
                    input,
                    row_index(row, input.len())?,
                    "avg_state_visible",
                    0,
                )?;
                match avg_state_visible_as_float64(state)? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Decimal128(precision, scale) => {
            let input_scale = input_scale.ok_or_else(|| {
                "avg_state_visible Decimal128 output requires input decimal scale".to_string()
            })?;
            let mut builder = Decimal128Builder::with_capacity(rows)
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for row in 0..rows {
                let state = binary_value_or_empty(
                    input,
                    row_index(row, input.len())?,
                    "avg_state_visible",
                    0,
                )?;
                let input_scale = int64_value(
                    input_scale,
                    row_index(row, input_scale.len())?,
                    "avg_state_visible",
                    1,
                )?;
                match avg_state_visible_as_decimal128(state, input_scale, *scale)? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(format!(
            "avg_state_visible expects Float64 or Decimal128 output type, got {other:?}"
        )),
    }
}

fn avg_state_visible_as_float64(s: &[u8]) -> Result<Option<f64>, String> {
    let (row_count, sum) = decode_avg_int64(s)?;
    if row_count == 0 {
        Ok(None)
    } else {
        Ok(Some(sum as f64 / row_count as f64))
    }
}

fn avg_state_visible_as_decimal128(
    s: &[u8],
    input_scale: i64,
    output_scale: i8,
) -> Result<Option<i128>, String> {
    let input_scale = validate_decimal_scale("input", input_scale)?;
    let output_scale = validate_decimal_scale("output", i64::from(output_scale))?;
    let (row_count, sum) = decode_avg_decimal128(s)?;
    if row_count == 0 {
        Ok(None)
    } else {
        let mut scaled_sum = sum;
        let scale_diff = output_scale - input_scale;
        if scale_diff != 0 {
            let factor = pow10_i128(scale_diff.unsigned_abs() as usize)?;
            if scale_diff > 0 {
                scaled_sum = scaled_sum
                    .checked_mul(factor)
                    .ok_or_else(|| "decimal overflow".to_string())?;
            } else {
                scaled_sum /= factor;
            }
        }
        Ok(Some(div_round_i128(scaled_sum, row_count as i128)))
    }
}

fn validate_decimal_scale(label: &str, scale: i64) -> Result<i32, String> {
    if !(0..=38).contains(&scale) {
        return Err(format!(
            "avg_state_visible {label} decimal scale out of range: {scale}"
        ));
    }
    Ok(scale as i32)
}

fn int64_value(array: &ArrayRef, row: usize, fn_name: &str, arg_idx: usize) -> Result<i64, String> {
    let DataType::Int64 = array.data_type() else {
        return Err(format!(
            "{fn_name} expects Int64 input for arg {arg_idx}, got {:?}",
            array.data_type()
        ));
    };
    let arr = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("{fn_name} downcast Int64Array failed for arg {arg_idx}"))?;
    if arr.is_null(row) {
        return Err(format!(
            "{fn_name} input decimal scale is NULL at row {row}"
        ));
    }
    Ok(arr.value(row))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, Decimal128Array, Float64Array, Int64Array,
    };
    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::starrocks::managed::state_codec::{
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

    fn int64_array(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn avg_state_union_reuses_sum_state_layout() {
        let out = avg_state_union(&encode_sum_int64(2, 30), &encode_sum_int64(3, 45)).unwrap();
        assert_eq!(decode_sum_int64(&out).unwrap(), (5, 75));

        let decimal_out = avg_state_union(
            &encode_sum_decimal128(2, 1_000_000),
            &encode_sum_decimal128(3, 500_000),
        )
        .unwrap();
        assert_eq!(decode_sum_decimal128(&decimal_out).unwrap(), (5, 1_500_000));
    }

    #[test]
    fn avg_state_visible_float64_returns_null_for_empty_or_zero_count() {
        assert_eq!(avg_state_visible_as_float64(&[]).unwrap(), None);
        assert_eq!(
            avg_state_visible_as_float64(&encode_sum_int64(0, 99)).unwrap(),
            None
        );
    }

    #[test]
    fn avg_state_visible_float64_divides_sum_by_row_count() {
        assert_eq!(
            avg_state_visible_as_float64(&encode_sum_int64(4, 10)).unwrap(),
            Some(2.5)
        );
    }

    #[test]
    fn avg_state_visible_decimal128_rescales_before_dividing_with_rounding() {
        assert_eq!(
            avg_state_visible_as_decimal128(&encode_sum_decimal128(2, 3_000_000), 6, 12).unwrap(),
            Some(1_500_000_000_000)
        );
    }

    #[test]
    fn avg_state_visible_array_returns_nullable_float64_values() {
        let input = binary_array(&[
            Some(encode_sum_int64(4, 10)),
            Some(encode_sum_int64(0, 99)),
            None,
        ]);

        let out = eval_avg_state_visible_array(&input, None, &DataType::Float64).unwrap();
        let arr = out.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(arr.value(0), 2.5);
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn avg_state_visible_float64_rejects_input_scale_arg() {
        let input = binary_array(&[Some(encode_sum_int64(4, 10))]);
        let input_scale = int64_array(&[Some(6)]);

        let err = eval_avg_state_visible_array(&input, Some(&input_scale), &DataType::Float64)
            .expect_err("Float64 visible output should not accept input scale");

        assert_eq!(
            err,
            "avg_state_visible input decimal scale requires Decimal128 output type"
        );
    }

    #[test]
    fn avg_state_visible_array_returns_nullable_decimal128_values() {
        let input = binary_array(&[
            Some(encode_sum_decimal128(2, 3_000_000)),
            Some(encode_sum_decimal128(0, 12345)),
            None,
        ]);
        let input_scale = int64_array(&[Some(6)]);

        let out =
            eval_avg_state_visible_array(&input, Some(&input_scale), &DataType::Decimal128(38, 12))
                .unwrap();
        let arr = out.as_any().downcast_ref::<Decimal128Array>().unwrap();

        assert_eq!(arr.value(0), 1_500_000_000_000);
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
        assert_eq!(arr.data_type(), &DataType::Decimal128(38, 12));
    }

    #[test]
    fn avg_state_visible_decimal128_requires_input_scale() {
        let input = binary_array(&[Some(encode_sum_decimal128(2, 3_000_000))]);

        let err = eval_avg_state_visible_array(&input, None, &DataType::Decimal128(38, 12))
            .expect_err("decimal visible output should require input scale");

        assert_eq!(
            err,
            "avg_state_visible Decimal128 output requires input decimal scale"
        );
    }

    #[test]
    fn avg_state_union_arrays_treat_null_inputs_as_empty_state() {
        let lhs = binary_array(&[Some(encode_sum_int64(2, 30)), None]);
        let rhs = binary_array(&[Some(encode_sum_int64(3, 45)), Some(encode_sum_int64(4, 20))]);

        let out = eval_avg_state_union_arrays(&lhs, &rhs).unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(decode_sum_int64(arr.value(0)).unwrap(), (5, 75));
        assert_eq!(decode_sum_int64(arr.value(1)).unwrap(), (4, 20));
    }
}
