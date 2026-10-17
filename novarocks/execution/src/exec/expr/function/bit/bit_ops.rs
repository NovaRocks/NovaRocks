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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Int64Array};
#[cfg(test)]
use arrow::array::{Decimal128Array, FixedSizeBinaryArray, UInt64Array};
#[cfg(test)]
use arrow::compute::cast;
use arrow::datatypes::DataType;
use novarocks_functions::{
    Selection,
    bit_numeric::{BitwiseOp, ShiftOp},
};
use novarocks_types::largeint;
#[cfg(test)]
use std::sync::Arc;

fn to_i64_array(array: &ArrayRef, fn_name: &str, arg_idx: usize) -> Result<Int64Array, String> {
    novarocks_functions::bit_array::to_i64_array(array, arg_idx)
        .map_err(|error| error.legacy_message(fn_name))
}
fn to_i128_values(
    array: &ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<Vec<Option<i128>>, String> {
    novarocks_functions::bit_array::to_i128_values(array, arg_idx)
        .map_err(|error| error.legacy_message(fn_name))
}
fn cast_largeint_output(
    values: &[Option<i128>],
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    novarocks_functions::bit_array::cast_largeint_output(values, output_type)
        .map_err(|error| error.legacy_message(fn_name))
}

fn use_largeint_path<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
) -> bool {
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

fn eval_unary_i64<F, __Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    fn_name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error>
where
    F: Fn(i64) -> i64,
{
    let array = arena.eval(args[0], chunk)?;
    let values = to_i64_array(&array, fn_name, 0)?;

    let out = novarocks_functions::bit_array::map_values_observed(
        Selection::all(values.len()),
        |_, row| Ok::<_, String>((!values.is_null(row)).then(|| values.value(row))),
        func,
        &mut |_| Ok(()),
    )?;
    Ok((novarocks_functions::bit_array::finish_i64_observed(
        out,
        arena.data_type(expr),
        &mut |_| Ok::<(), String>(()),
    )?
    .map_err(|error| error.legacy_message(fn_name)))?)
}

fn eval_unary_i128<F, __Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    fn_name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error>
where
    F: Fn(i128) -> i128,
{
    let array = arena.eval(args[0], chunk)?;
    let values = to_i128_values(&array, fn_name, 0)?;
    let out = novarocks_functions::bit_array::map_values_observed(
        Selection::all(values.len()),
        |_, row| Ok::<_, String>(values[row]),
        func,
        &mut |_| Ok(()),
    )?;
    Ok((cast_largeint_output(&out, arena.data_type(expr), fn_name))?)
}

fn eval_binary_i64<F, __Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    fn_name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error>
where
    F: Fn(i64, i64) -> i64,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i64_array(&left, fn_name, 0)?;
    let right = to_i64_array(&right, fn_name, 1)?;

    let out = novarocks_functions::bit_array::map_values_observed(
        Selection::all(chunk.len()),
        |_, row| {
            Ok::<_, String>(if left.is_null(row) || right.is_null(row) {
                None
            } else {
                Some((left.value(row), right.value(row)))
            })
        },
        |(left, right)| func(left, right),
        &mut |_| Ok(()),
    )?;
    Ok((novarocks_functions::bit_array::finish_i64_observed(
        out,
        arena.data_type(expr),
        &mut |_| Ok::<(), String>(()),
    )?
    .map_err(|error| error.legacy_message(fn_name)))?)
}

fn eval_binary_i128<F, __Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    fn_name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error>
where
    F: Fn(i128, i128) -> i128,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i128_values(&left, fn_name, 0)?;
    let right = to_i128_values(&right, fn_name, 1)?;

    let out = novarocks_functions::bit_array::map_values_observed(
        Selection::all(chunk.len()),
        |_, row| {
            Ok::<_, String>(match (left[row], right[row]) {
                (Some(left), Some(right)) => Some((left, right)),
                _ => None,
            })
        },
        |(left, right)| func(left, right),
        &mut |_| Ok(()),
    )?;
    Ok((cast_largeint_output(&out, arena.data_type(expr), fn_name))?)
}

fn eval_shift_i64<F, __Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    fn_name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error>
where
    F: Fn(i64, u32) -> i64,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i64_array(&left, fn_name, 0)?;
    let right = to_i64_array(&right, fn_name, 1)?;

    let out = novarocks_functions::bit_array::shift_values_observed(
        Selection::all(chunk.len()),
        |_, row| {
            Ok::<_, std::convert::Infallible>(if left.is_null(row) || right.is_null(row) {
                None
            } else {
                Some((left.value(row), right.value(row)))
            })
        },
        func,
        &mut |_| Ok::<_, std::convert::Infallible>(()),
    )
    .unwrap_or_else(|never| match never {});
    Ok((novarocks_functions::bit_array::finish_i64_observed(
        out,
        arena.data_type(expr),
        &mut |_| Ok::<_, std::convert::Infallible>(()),
    )
    .unwrap_or_else(|never| match never {})
    .map_err(|error| error.legacy_message(fn_name)))?)
}

fn eval_shift_i128<F, __Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    fn_name: &str,
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error>
where
    F: Fn(i128, u32) -> i128,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i128_values(&left, fn_name, 0)?;
    let right = to_i128_values(&right, fn_name, 1)?;

    let out = novarocks_functions::bit_array::shift_values_observed(
        Selection::all(chunk.len()),
        |_, row| {
            Ok::<_, std::convert::Infallible>(match (left[row], right[row]) {
                (Some(l), Some(r)) => Some((l, r)),
                _ => None,
            })
        },
        func,
        &mut |_| Ok::<_, std::convert::Infallible>(()),
    )
    .unwrap_or_else(|never| match never {});
    Ok((cast_largeint_output(&out, arena.data_type(expr), fn_name))?)
}

pub fn eval_bit_shift_left<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok(
            (eval_shift_i128("bit_shift_left", arena, expr, args, chunk, |a, b| {
                ShiftOp::Left.apply_i128(a, i64::from(b))
            }))?,
        );
    }
    Ok(
        (eval_shift_i64("bit_shift_left", arena, expr, args, chunk, |a, b| {
            ShiftOp::Left.apply_i64(a, i64::from(b))
        }))?,
    )
}

pub fn eval_bit_shift_right<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok(
            (eval_shift_i128("bit_shift_right", arena, expr, args, chunk, |a, b| {
                ShiftOp::Right.apply_i128(a, i64::from(b))
            }))?,
        );
    }
    Ok(
        (eval_shift_i64("bit_shift_right", arena, expr, args, chunk, |a, b| {
            ShiftOp::Right.apply_i64(a, i64::from(b))
        }))?,
    )
}

pub fn eval_bit_shift_right_logical<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok((eval_shift_i128(
            "bit_shift_right_logical",
            arena,
            expr,
            args,
            chunk,
            |a, b| ShiftOp::RightLogical.apply_i128(a, i64::from(b)),
        ))?);
    }
    Ok((eval_shift_i64(
        "bit_shift_right_logical",
        arena,
        expr,
        args,
        chunk,
        |a, b| ShiftOp::RightLogical.apply_i64(a, i64::from(b)),
    ))?)
}

pub fn eval_bitand<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok(
            (eval_binary_i128("bitand", arena, expr, args, chunk, |a, b| {
                BitwiseOp::And.apply_i128(a, b)
            }))?,
        );
    }
    Ok(
        (eval_binary_i64("bitand", arena, expr, args, chunk, |a, b| {
            BitwiseOp::And.apply_i64(a, b)
        }))?,
    )
}

pub fn eval_bitnot<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok((eval_unary_i128("bitnot", arena, expr, args, chunk, |a| {
            BitwiseOp::Not.apply_i128(a, 0)
        }))?);
    }
    Ok((eval_unary_i64("bitnot", arena, expr, args, chunk, |a| {
        BitwiseOp::Not.apply_i64(a, 0)
    }))?)
}

pub fn eval_bitor<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok(
            (eval_binary_i128("bitor", arena, expr, args, chunk, |a, b| {
                BitwiseOp::Or.apply_i128(a, b)
            }))?,
        );
    }
    Ok((eval_binary_i64("bitor", arena, expr, args, chunk, |a, b| {
        BitwiseOp::Or.apply_i64(a, b)
    }))?)
}

pub fn eval_bitxor<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if use_largeint_path(arena, expr, args) {
        return Ok(
            (eval_binary_i128("bitxor", arena, expr, args, chunk, |a, b| {
                BitwiseOp::Xor.apply_i128(a, b)
            }))?,
        );
    }
    Ok(
        (eval_binary_i64("bitxor", arena, expr, args, chunk, |a, b| {
            BitwiseOp::Xor.apply_i64(a, b)
        }))?,
    )
}

#[cfg(test)]
mod legacy_shift_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Int8Array, Int16Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, left: ArrayRef, right: ArrayRef) -> ArrayRef {
        let left_type = left.data_type().clone();
        let right_type = right.data_type().clone();
        assert_eq!(right_type, DataType::Int64);
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", left_type.clone(), true),
            Field::new("count", right_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), left_type.clone());
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), right_type);
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Bit(name),
                args: vec![left, right],
            },
            left_type.clone(),
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(output.data_type(), &left_type);
        output
    }
    fn narrow(ty: &DataType, values: &[Option<i64>]) -> ArrayRef {
        match ty {
            DataType::Int8 => Arc::new(Int8Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
            _ => panic!("fixture is not a signed integer carrier"),
        }
    }
    fn counts(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }
    fn assert_signed(output: &ArrayRef, expected: &[Option<i64>]) {
        let cast = cast(output, &DataType::Int64).unwrap();
        let values = cast.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), expected);
    }
    fn assert_large(output: &ArrayRef, expected: &[Option<i128>]) {
        // This legacy carrier oracle does not authenticate the logical root
        // domain. The new pure owner must independently require LARGEINT FVT.
        assert_eq!(output.data_type(), &DataType::FixedSizeBinary(16));
        let values = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!values.is_null(row));
                    assert_eq!(values.value(row), expected.to_be_bytes().as_slice());
                    assert_eq!(
                        largeint::i128_from_be_bytes(values.value(row)).unwrap(),
                        *expected
                    );
                }
                None => assert!(values.is_null(row)),
            }
        }
    }
    #[test]
    fn signed_narrow_left_computes_in_i64_before_checked_output_conversion() {
        for (ty, width) in [
            (DataType::Int8, 8),
            (DataType::Int16, 16),
            (DataType::Int32, 32),
        ] {
            let output = evaluate(
                "bit_shift_left",
                narrow(&ty, &[Some(1); 5]),
                counts(&[
                    Some(width - 2),
                    Some(width - 1),
                    Some(width),
                    Some(63),
                    Some(64),
                ]),
            );
            assert_signed(
                &output,
                &[Some(1_i64 << (width - 2)), None, None, None, Some(1)],
            );
        }
    }
    #[test]
    fn signed_narrow_logical_right_does_not_mask_before_i64_shift() {
        for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
            let output = evaluate(
                "bit_shift_right_logical",
                narrow(&ty, &[Some(-1); 6]),
                counts(&[Some(0), Some(1), Some(63), Some(64), Some(65), Some(-1)]),
            );
            assert_signed(&output, &[Some(-1), None, Some(1), Some(-1), None, Some(1)]);
        }
    }
    #[test]
    fn signed_widths_arithmetic_right_keep_sign_and_wrap_count_modulo64() {
        for (ty, min) in [
            (DataType::Int8, i8::MIN as i64),
            (DataType::Int16, i16::MIN as i64),
            (DataType::Int32, i32::MIN as i64),
            (DataType::Int64, i64::MIN),
        ] {
            let output = evaluate(
                "bit_shift_right",
                narrow(&ty, &[Some(min); 5]),
                counts(&[Some(1), Some(63), Some(64), Some(-64), Some(i64::MIN)]),
            );
            assert_signed(
                &output,
                &[Some(min / 2), Some(-1), Some(min), Some(min), Some(min)],
            );
        }
    }
    #[test]
    fn i64_negative_and_large_counts_use_u32_projection_then_modulo64() {
        let rhs = counts(&[
            Some(-1),
            Some(-64),
            Some(i64::MIN),
            Some(1_i64 << 32),
            Some((1_i64 << 32) + 1),
        ]);
        assert_signed(
            &evaluate(
                "bit_shift_left",
                narrow(&DataType::Int64, &[Some(1); 5]),
                rhs.clone(),
            ),
            &[Some(i64::MIN), Some(1), Some(1), Some(1), Some(2)],
        );
        assert_signed(
            &evaluate(
                "bit_shift_right",
                narrow(&DataType::Int64, &[Some(-8); 5]),
                rhs.clone(),
            ),
            &[Some(-1), Some(-8), Some(-8), Some(-8), Some(-4)],
        );
        assert_signed(
            &evaluate(
                "bit_shift_right_logical",
                narrow(&DataType::Int64, &[Some(-1); 5]),
                rhs,
            ),
            &[Some(1), Some(-1), Some(-1), Some(-1), Some(i64::MAX)],
        );
        assert_signed(
            &evaluate(
                "bit_shift_left",
                narrow(
                    &DataType::Int64,
                    &[Some(i64::MAX), Some(i64::MIN), Some(-1)],
                ),
                counts(&[Some(1); 3]),
            ),
            &[Some(-2), Some(0), Some(-2)],
        );
    }
    #[test]
    fn largeint_left_uses_modulo128_and_exact_big_endian_output() {
        let left = largeint::array_from_i128(&[Some(1); 7]).unwrap();
        let rhs = counts(&[
            Some(127),
            Some(128),
            Some(129),
            Some(-1),
            Some(-128),
            Some(i64::MIN),
            Some((1_i64 << 32) + 129),
        ]);
        assert_large(
            &evaluate("bit_shift_left", left, rhs),
            &[
                Some(i128::MIN),
                Some(1),
                Some(2),
                Some(i128::MIN),
                Some(1),
                Some(1),
                Some(2),
            ],
        );
        assert_large(
            &evaluate(
                "bit_shift_left",
                largeint::array_from_i128(&[Some(i128::MIN)]).unwrap(),
                counts(&[Some(1)]),
            ),
            &[Some(0)],
        );
    }
    #[test]
    fn largeint_arithmetic_and_logical_right_preserve_distinct_128bit_rules() {
        let rhs = counts(&[
            Some(127),
            Some(128),
            Some(129),
            Some(-1),
            Some(-128),
            Some(i64::MIN),
        ]);
        assert_large(
            &evaluate(
                "bit_shift_right",
                largeint::array_from_i128(&[Some(i128::MIN); 6]).unwrap(),
                rhs.clone(),
            ),
            &[
                Some(-1),
                Some(i128::MIN),
                Some(i128::MIN / 2),
                Some(-1),
                Some(i128::MIN),
                Some(i128::MIN),
            ],
        );
        assert_large(
            &evaluate(
                "bit_shift_right_logical",
                largeint::array_from_i128(&[Some(-1); 6]).unwrap(),
                rhs,
            ),
            &[
                Some(1),
                Some(-1),
                Some(i128::MAX),
                Some(1),
                Some(-1),
                Some(-1),
            ],
        );
    }
    #[test]
    fn nulls_and_independently_sliced_carriers_keep_original_row_alignment() {
        let left = Arc::new(Int8Array::from(vec![
            Some(99),
            Some(1),
            None,
            Some(-1),
            Some(2),
            Some(99),
        ])) as ArrayRef;
        let rhs = counts(&[Some(9), Some(7), Some(2), None, Some(64), Some(9)]);
        let output = evaluate("bit_shift_left", left.slice(1, 4), rhs.slice(1, 4));
        assert_signed(&output, &[None, None, None, Some(2)]);
        let left =
            largeint::array_from_i128(&[Some(99), Some(1), None, Some(-1), Some(99)]).unwrap();
        let rhs = counts(&[Some(9), Some(129), Some(1), None, Some(9)]);
        assert_large(
            &evaluate("bit_shift_left", left.slice(1, 3), rhs.slice(1, 3)),
            &[Some(2), None, None],
        );
    }
}

#[cfg(test)]
mod legacy_bitwise_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Int8Array, Int16Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate_result(name: &'static str, arrays: Vec<ArrayRef>) -> Result<ArrayRef, String> {
        let output_type = arrays[0].data_type().clone();
        let fields = arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                Field::new(format!("argument{index}"), array.data_type().clone(), true)
            })
            .collect::<Vec<_>>();
        let slots = (0..arrays.len())
            .map(|index| SlotId::new(index as u32 + 1))
            .collect::<Vec<_>>();
        let types = arrays
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let arguments = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
            .collect::<Vec<_>>();
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Bit(name),
                args: arguments,
            },
            output_type.clone(),
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(output.data_type(), &output_type);
        Ok(output)
    }
    fn evaluate(name: &'static str, arrays: Vec<ArrayRef>) -> ArrayRef {
        evaluate_result(name, arrays).unwrap()
    }
    fn signed(ty: &DataType, values: &[Option<i64>]) -> ArrayRef {
        match ty {
            DataType::Int8 => Arc::new(Int8Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
            _ => panic!("fixture requires a signed integer carrier"),
        }
    }
    fn assert_signed(output: &ArrayRef, expected: &[Option<i64>]) {
        let converted = cast(output, &DataType::Int64).unwrap();
        assert_eq!(
            converted
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
    }
    fn assert_large(output: &ArrayRef, expected: &[Option<i128>]) {
        // This exact BE16 carrier oracle does not grant a Physical Fixed16
        // source the logical LARGEINT authority required by the new owner.
        assert_eq!(output.data_type(), &DataType::FixedSizeBinary(16));
        let array = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(array.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!array.is_null(row));
                    assert_eq!(array.value(row), expected.to_be_bytes().as_slice());
                    assert_eq!(
                        largeint::i128_from_be_bytes(array.value(row)).unwrap(),
                        *expected
                    );
                }
                None => assert!(array.is_null(row)),
            }
        }
    }
    fn widths() -> [(DataType, i64, i64); 4] {
        [
            (DataType::Int8, i8::MIN as i64, i8::MAX as i64),
            (DataType::Int16, i16::MIN as i64, i16::MAX as i64),
            (DataType::Int32, i32::MIN as i64, i32::MAX as i64),
            (DataType::Int64, i64::MIN, i64::MAX),
        ]
    }
    #[test]
    fn signed_binary_operations_preserve_widened_sign_extension_for_all_widths() {
        for (ty, min, max) in widths() {
            let left = signed(
                &ty,
                &[Some(-1), Some(min), Some(max), Some(0), Some(1), Some(-2)],
            );
            let right = signed(
                &ty,
                &[Some(1), Some(-1), Some(min), Some(max), Some(-1), Some(3)],
            );
            assert_signed(
                &evaluate("bitand", vec![left.clone(), right.clone()]),
                &[Some(1), Some(min), Some(0), Some(0), Some(1), Some(2)],
            );
            assert_signed(
                &evaluate("bitor", vec![left.clone(), right.clone()]),
                &[Some(-1), Some(-1), Some(-1), Some(max), Some(-1), Some(-1)],
            );
            assert_signed(
                &evaluate("bitxor", vec![left, right]),
                &[Some(-2), Some(max), Some(-1), Some(max), Some(-2), Some(-3)],
            );
        }
    }
    #[test]
    fn signed_unary_not_preserves_source_width_after_i64_complement() {
        for (ty, min, max) in widths() {
            assert_signed(
                &evaluate(
                    "bitnot",
                    vec![signed(
                        &ty,
                        &[
                            Some(min),
                            Some(max),
                            Some(-1),
                            Some(0),
                            Some(1),
                            Some(-2),
                            None,
                        ],
                    )],
                ),
                &[
                    Some(max),
                    Some(min),
                    Some(0),
                    Some(-1),
                    Some(-2),
                    Some(1),
                    None,
                ],
            );
        }
    }
    #[test]
    fn i64_high_and_low_bit_patterns_have_independent_signed_expected_values() {
        let high = 1_i64 << 62;
        let left = signed(
            &DataType::Int64,
            &[Some(high + 5), Some(i64::MIN), Some(i64::MAX)],
        );
        let right = signed(&DataType::Int64, &[Some(-high), Some(5), Some(5)]);
        assert_signed(
            &evaluate("bitand", vec![left.clone(), right.clone()]),
            &[Some(high), Some(0), Some(5)],
        );
        assert_signed(
            &evaluate("bitor", vec![left.clone(), right.clone()]),
            &[Some(-high + 5), Some(i64::MIN + 5), Some(i64::MAX)],
        );
        assert_signed(
            &evaluate("bitxor", vec![left, right]),
            &[Some(i64::MIN + 5), Some(i64::MIN + 5), Some(i64::MAX - 5)],
        );
    }
    #[test]
    fn largeint_binary_operations_keep_upper100_bits_extrema_and_big_endian_bytes() {
        let high = 1_i128 << 100;
        let left = largeint::array_from_i128(&[
            Some(high + 5),
            Some(i128::MIN),
            Some(i128::MAX),
            Some(-1),
            Some(0),
        ])
        .unwrap();
        let right = largeint::array_from_i128(&[
            Some(high + 10),
            Some(-1),
            Some(i128::MIN),
            Some(high),
            Some(-1),
        ])
        .unwrap();
        assert_large(
            &evaluate("bitand", vec![left.clone(), right.clone()]),
            &[Some(high), Some(i128::MIN), Some(0), Some(high), Some(0)],
        );
        assert_large(
            &evaluate("bitor", vec![left.clone(), right.clone()]),
            &[Some(high + 15), Some(-1), Some(-1), Some(-1), Some(-1)],
        );
        assert_large(
            &evaluate("bitxor", vec![left, right]),
            &[
                Some(15),
                Some(i128::MAX),
                Some(-1),
                Some(-high - 1),
                Some(-1),
            ],
        );
    }
    #[test]
    fn largeint_unary_not_retains_all128_bits_and_exact_nulls() {
        let high = 1_i128 << 100;
        let values = largeint::array_from_i128(&[
            Some(i128::MIN),
            Some(i128::MAX),
            Some(high + 5),
            Some(-1),
            Some(0),
            None,
        ])
        .unwrap();
        assert_large(
            &evaluate("bitnot", vec![values]),
            &[
                Some(i128::MAX),
                Some(i128::MIN),
                Some(-high - 6),
                Some(0),
                Some(-1),
                None,
            ],
        );
    }
    #[test]
    fn binary_nulls_and_real_slices_preserve_each_argument_row_and_output_width() {
        let left = signed(
            &DataType::Int16,
            &[Some(999), Some(-1), None, Some(1), None, Some(7)],
        );
        let right = signed(
            &DataType::Int16,
            &[Some(888), Some(1), Some(1), None, None, Some(7)],
        );
        for (name, value) in [("bitand", 1), ("bitor", -1), ("bitxor", -2)] {
            assert_signed(
                &evaluate(name, vec![left.slice(1, 4), right.slice(1, 4)]),
                &[Some(value), None, None, None],
            );
        }
        let high = 1_i128 << 100;
        let left =
            largeint::array_from_i128(&[Some(99), Some(high + 5), None, Some(1), None, Some(7)])
                .unwrap();
        let right =
            largeint::array_from_i128(&[Some(99), Some(high + 10), Some(1), None, None, Some(7)])
                .unwrap();
        for (name, value) in [("bitand", high), ("bitor", high + 15), ("bitxor", 15)] {
            assert_large(
                &evaluate(name, vec![left.slice(1, 4), right.slice(1, 4)]),
                &[Some(value), None, None, None],
            );
        }
    }
    #[test]
    fn real_expression_dispatch_rejects_bitnot_binary_arity_for_every_source_profile() {
        for (ty, _, _) in widths() {
            let error = evaluate_result(
                "bitnot",
                vec![signed(&ty, &[Some(1)]), signed(&ty, &[Some(2)])],
            )
            .unwrap_err();
            assert_eq!(error, "bitnot expects 1 to 1 arguments, got 2");
        }
        let error = evaluate_result(
            "bitnot",
            vec![
                largeint::array_from_i128(&[Some(1)]).unwrap(),
                largeint::array_from_i128(&[Some(2)]).unwrap(),
            ],
        )
        .unwrap_err();
        assert_eq!(error, "bitnot expects 1 to 1 arguments, got 2");
        // A genuine unary call remains accepted; a false blanket rejection
        // of bitnot cannot make this negative-arity oracle pass.
        assert_signed(
            &evaluate("bitnot", vec![signed(&DataType::Int8, &[Some(1)])]),
            &[Some(-2)],
        );
    }
}

#[cfg(test)]
#[path = "legacy_shift_raw_contract_tests.rs"]
mod legacy_shift_raw_contract_tests;
