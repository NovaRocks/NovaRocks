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
#[cfg(test)]
use crate::exec::expr::decimal::{pow10_i128, pow10_i256};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
#[cfg(test)]
use novarocks_functions::legacy_arithmetic::{
    DecimalOp, decimal_overflow_error, eval_decimal_binop,
};
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::largeint;
use std::sync::Arc;

pub fn eval_add<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    Ok((novarocks_functions::legacy_arithmetic::eval_add_arrays(
        lhs,
        rhs,
        output_type,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    ))?)
}

pub fn eval_sub<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    Ok((novarocks_functions::legacy_arithmetic::eval_sub_arrays(
        lhs,
        rhs,
        output_type,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    ))?)
}

pub fn eval_mul<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    Ok((novarocks_functions::legacy_arithmetic::eval_mul_arrays(
        lhs,
        rhs,
        output_type,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    ))?)
}

pub fn eval_div<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).unwrap_or(&DataType::Null);
    Ok((novarocks_functions::legacy_arithmetic::eval_div_arrays(
        lhs,
        rhs,
        output_type,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    ))?)
}

pub fn eval_mod<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    Ok((novarocks_functions::legacy_arithmetic::eval_mod_arrays(
        lhs,
        rhs,
        output_type,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    ))?)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Decimal128Array, FixedSizeBinaryArray, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use novarocks_types::largeint;

    fn create_test_chunk_int(values: Vec<i64>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_float(values: Vec<f64>) -> Chunk {
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_two_decimals(
        left: Vec<Option<i128>>,
        right: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> Chunk {
        let left_arr = Arc::new(
            Decimal128Array::from(left)
                .with_precision_and_scale(precision, scale)
                .expect("left decimal array"),
        ) as ArrayRef;
        let right_arr = Arc::new(
            Decimal128Array::from(right)
                .with_precision_and_scale(precision, scale)
                .expect("right decimal array"),
        ) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Decimal128(precision, scale), true),
            Field::new("right", DataType::Decimal128(precision, scale), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left_arr, right_arr]).expect("batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    #[test]
    fn mixed_decimal_largeint_prepared_columns_preserve_extrema_and_nulls() {
        let decimal = Arc::new(
            Decimal128Array::from(vec![
                Some(1250000000000000_i128),
                Some(-1250000000000000),
                None,
                Some(1),
            ])
            .with_precision_and_scale(38, 15)
            .unwrap(),
        ) as ArrayRef;
        let integers =
            largeint::array_from_i128(&[Some(i128::MAX), Some(i128::MIN), Some(1), None]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("decimal", decimal.data_type().clone(), true),
                Field::new("integer", integers.data_type().clone(), true),
            ])),
            vec![decimal, integers],
        )
        .unwrap();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let decimal = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 15),
        );
        let integer = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::FixedSizeBinary(16),
        );
        let out = DataType::Decimal256(55, 15);
        let add = arena.push_typed(
            ExprNode::Add(decimal, integer, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let add_reverse = arena.push_typed(
            ExprNode::Add(integer, decimal, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let sub = arena.push_typed(
            ExprNode::Sub(decimal, integer, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let reverse = arena.push_typed(
            ExprNode::Sub(integer, decimal, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let unsupported = arena.push_typed(
            ExprNode::Mul(decimal, integer, DecimalOverflowPolicy::OutputNull),
            out,
        );
        let frozen = arena.into_immutable().unwrap();
        let prepared =
            ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
        let factor = pow10_i256(15).unwrap();
        let scaled = [
            i256::from_i128(i128::MAX).checked_mul(factor).unwrap(),
            i256::from_i128(i128::MIN).checked_mul(factor).unwrap(),
        ];
        let coefficient = [
            i256::from_i128(1250000000000000),
            i256::from_i128(-1250000000000000),
        ];
        for (expr, expected) in [
            (
                add,
                vec![
                    Some(scaled[0].checked_add(coefficient[0]).unwrap()),
                    Some(scaled[1].checked_add(coefficient[1]).unwrap()),
                    None,
                    None,
                ],
            ),
            (
                add_reverse,
                vec![
                    Some(scaled[0].checked_add(coefficient[0]).unwrap()),
                    Some(scaled[1].checked_add(coefficient[1]).unwrap()),
                    None,
                    None,
                ],
            ),
            (
                sub,
                vec![
                    Some(coefficient[0].checked_sub(scaled[0]).unwrap()),
                    Some(coefficient[1].checked_sub(scaled[1]).unwrap()),
                    None,
                    None,
                ],
            ),
            (
                reverse,
                vec![
                    Some(scaled[0].checked_sub(coefficient[0]).unwrap()),
                    Some(scaled[1].checked_sub(coefficient[1]).unwrap()),
                    None,
                    None,
                ],
            ),
        ] {
            let output = prepared.eval(expr, &chunk).unwrap();
            assert_eq!(output.data_type(), &DataType::Decimal256(55, 15));
            let output = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
            assert_eq!(output.iter().collect::<Vec<_>>(), expected);
        }
        assert!(
            prepared
                .eval(unsupported, &chunk)
                .unwrap_err()
                .contains("frozen add/subtract rule")
        );
    }

    #[test]
    fn mixed_decimal_largeint_scale36_fits_precision76() {
        let decimal = Arc::new(
            Decimal128Array::from(vec![Some(1_i128)])
                .with_precision_and_scale(38, 36)
                .unwrap(),
        ) as ArrayRef;
        let integer = largeint::array_from_i128(&[Some(i128::MAX)]).unwrap();
        let expected = i256::from_i128(i128::MAX)
            .checked_mul(pow10_i256(36).unwrap())
            .unwrap()
            .checked_add(i256::ONE)
            .unwrap();
        let output = eval_decimal_binop(
            &decimal,
            &integer,
            &DataType::Decimal256(76, 36),
            DecimalOp::Add,
            false,
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap()
        .unwrap();
        let output = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(output.value(0), expected);
        assert!(
            eval_decimal_binop(
                &decimal,
                &integer,
                &DataType::Decimal128(38, 36),
                DecimalOp::Add,
                false,
                DecimalOverflowPolicy::OutputNull,
            )
            .unwrap_err()
            .contains("frozen add/subtract rule")
        );
    }

    #[test]
    fn test_add_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let add = arena.push_typed(
            ExprNode::Add(lit5, lit3, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1, 2, 3]);

        let result = arena.eval(add, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.len(), 3);
        assert_eq!(result_arr.value(0), 8);
    }

    #[test]
    fn test_sub_integers() {
        let mut arena = ExprArena::default();
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let sub = arena.push_typed(
            ExprNode::Sub(lit10, lit3, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(sub, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 7);
    }

    #[test]
    fn test_mul_integers() {
        let mut arena = ExprArena::default();
        let lit6 = arena.push(ExprNode::Literal(LiteralValue::Int64(6)));
        let lit7 = arena.push(ExprNode::Literal(LiteralValue::Int64(7)));
        let mul = arena.push_typed(
            ExprNode::Mul(lit6, lit7, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(mul, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 42);
    }

    #[test]
    fn test_div_integers() {
        let mut arena = ExprArena::default();
        let lit20 = arena.push(ExprNode::Literal(LiteralValue::Int64(20)));
        let lit4 = arena.push(ExprNode::Literal(LiteralValue::Int64(4)));
        let div = arena.push_typed(
            ExprNode::Div(lit20, lit4, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(div, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 5);
    }

    #[test]
    fn test_mod_integers() {
        let mut arena = ExprArena::default();
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let rem = arena.push_typed(
            ExprNode::Mod(lit10, lit3, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(rem, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 1);
    }

    #[test]
    fn test_add_floats() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Float64(1.5)));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Float64(2.3)));
        let add = arena.push_typed(
            ExprNode::Add(lit1, lit2, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );

        let chunk = create_test_chunk_float(vec![0.0]);

        let result = arena.eval(add, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!((result_arr.value(0) - 3.8).abs() < 0.0001);
    }

    #[test]
    fn test_mixed_int_float() {
        let mut arena = ExprArena::default();
        let lit_int = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit_float = arena.push(ExprNode::Literal(LiteralValue::Float64(2.5)));
        let mul = arena.push_typed(
            ExprNode::Mul(lit_int, lit_float, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(mul, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!((result_arr.value(0) - 25.0).abs() < 0.0001);
    }

    #[test]
    fn test_add_largeint_and_bigint_returns_largeint() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::Literal(LiteralValue::LargeInt(9_223_372_036_854_775_808_i128)),
            DataType::FixedSizeBinary(16),
        );
        let rhs = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let add = arena.push_typed(
            ExprNode::Add(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::FixedSizeBinary(16),
        );

        let chunk = create_test_chunk_int(vec![1]);
        let result = arena.eval(add, &chunk).unwrap();
        let result_arr = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let parsed = largeint::i128_from_be_bytes(result_arr.value(0)).unwrap();
        assert_eq!(parsed, 9_223_372_036_854_775_809_i128);
    }

    #[test]
    fn test_mul_largeint_and_bigint_returns_largeint() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::Literal(LiteralValue::LargeInt(
                170141183460469231731687303715884105727_i128,
            )),
            DataType::FixedSizeBinary(16),
        );
        let rhs = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(0)), DataType::Int64);
        let mul = arena.push_typed(
            ExprNode::Mul(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::FixedSizeBinary(16),
        );

        let chunk = create_test_chunk_int(vec![1]);
        let result = arena.eval(mul, &chunk).unwrap();
        let result_arr = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let parsed = largeint::i128_from_be_bytes(result_arr.value(0)).unwrap();
        assert_eq!(parsed, 0);
    }

    #[test]
    fn frozen_float64_decimal_product_casts_operands_before_multiplication() {
        let lhs_type = DataType::Decimal128(30, 10);
        let rhs_type = DataType::Decimal128(18, 9);
        let lhs_coefficient = 123456789012345678901234567890_i128;
        let rhs_coefficient = 123456789123456789_i128;
        let lhs_array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(lhs_coefficient),
                Some(-lhs_coefficient),
                None,
                Some(0),
            ])
            .with_precision_and_scale(30, 10)
            .unwrap(),
        );
        let rhs_array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(rhs_coefficient),
                Some(-rhs_coefficient),
                Some(rhs_coefficient),
                Some(rhs_coefficient),
            ])
            .with_precision_and_scale(18, 9)
            .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", lhs_type.clone(), true),
            Field::new("right", rhs_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![lhs_array, rhs_array]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), lhs_type);
        let rhs = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), rhs_type);
        let lhs_f64 = arena.push_typed(
            ExprNode::Cast(lhs, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );
        let rhs_f64 = arena.push_typed(
            ExprNode::Cast(rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );
        let promoted = arena.push_typed(
            ExprNode::Mul(lhs_f64, rhs_f64, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );
        let checked = arena.push_typed(
            ExprNode::Mul(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 19),
        );
        let output = arena.eval(promoted, &chunk).unwrap();
        assert_eq!(output.data_type(), &DataType::Float64);
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
        // Independent original-SQL coefficient -> binary64 cast -> binary64 product oracle.
        assert_eq!(output.value(0).to_bits(), 0x4593_b303_f039_904e);
        assert_eq!(output.value(1).to_bits(), 0x4593_b303_f039_904e);
        assert!(output.is_null(2));
        assert!(!output.is_null(3));
        assert_eq!(output.value(3), 0.0);
        let checked = arena.eval(checked, &chunk).unwrap();
        let checked = checked.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(checked.is_null(0));
        assert!(checked.is_null(1));
        assert!(checked.is_null(2));
        assert_eq!(checked.value(3), 0);
    }

    #[test]
    fn test_decimal_div_precision_overflow_returns_null() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 18),
        );
        let rhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 18),
        );
        let div_expr = arena.push_typed(
            ExprNode::Div(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 38),
        );

        let chunk = create_test_chunk_two_decimals(
            vec![Some(-2_516_460_439_000_000_000_000_i128)],
            vec![Some(1_673_370_000_000_000_000_000_i128)],
            38,
            18,
        );
        let result = arena.eval(div_expr, &chunk).expect("decimal div");
        let result_arr = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(result_arr.is_null(0));
    }

    #[test]
    fn test_decimal_div_non_overflow_keeps_value() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 18),
        );
        let rhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 18),
        );
        let div_expr = arena.push_typed(
            ExprNode::Div(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 18),
        );

        let chunk = create_test_chunk_two_decimals(
            vec![Some(1_200_000_000_000_000_000_i128)],
            vec![Some(2_000_000_000_000_000_000_i128)],
            38,
            18,
        );
        let result = arena.eval(div_expr, &chunk).expect("decimal div");
        let result_arr = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(!result_arr.is_null(0));
    }
}

#[cfg(test)]
mod overflow_policy_prepared_tests {
    use super::*;
    use crate::exec::expr::ExprNode;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn chunk(columns: Vec<ArrayRef>) -> Chunk {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(i, array)| Field::new(format!("c{i}"), array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let ids = (1..=batch.num_columns())
            .map(|i| SlotId::new(i as u32))
            .collect::<Vec<_>>();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &ids,
        )
        .unwrap();
        Chunk::new_with_chunk_schema(batch, schema)
    }

    #[test]
    fn prepared_decimal256_policy_uses_wide_coefficients_and_precision76_boundary() {
        use DecimalOverflowPolicy::{OutputNull, ReportError};
        // Independent decimal coefficients: wide exceeds i128, and max is the
        // largest lawful DECIMAL(76,0) coefficient. No kernel pow10 is an oracle.
        let wide = i256::from_string("200000000000000000000000000000000000000").unwrap();
        let max = i256::from_string(
            "9999999999999999999999999999999999999999999999999999999999999999999999999999",
        )
        .unwrap();
        assert!(wide.to_i128().is_none());
        for (operation, right_value, right_scale, output_scale, expected) in [
            (
                DecimalOp::Add,
                1,
                0,
                0,
                "200000000000000000000000000000000000001",
            ),
            (
                DecimalOp::Sub,
                1,
                0,
                0,
                "199999999999999999999999999999999999999",
            ),
            (
                DecimalOp::Mul,
                2,
                0,
                0,
                "400000000000000000000000000000000000000",
            ),
            (
                DecimalOp::Div,
                1,
                1,
                6,
                "2000000000000000000000000000000000000000000000",
            ),
            (DecimalOp::Mod, 1, 1, 1, "0"),
        ] {
            let batch = chunk(vec![
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(wide),
                        None,
                        Some(if matches!(operation, DecimalOp::Sub) {
                            -max
                        } else {
                            max
                        }),
                    ])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal256Array::from(vec![Some(i256::from_i128(right_value)); 3])
                        .with_precision_and_scale(2, right_scale)
                        .unwrap(),
                ) as ArrayRef,
            ]);
            let mut arena = ExprArena::default();
            let left = arena.push_typed(
                ExprNode::SlotId(SlotId::new(1)),
                DataType::Decimal256(76, 0),
            );
            let right = arena.push_typed(
                ExprNode::SlotId(SlotId::new(2)),
                DataType::Decimal256(2, right_scale),
            );
            let node = |policy| match operation {
                DecimalOp::Add => ExprNode::Add(left, right, policy),
                DecimalOp::Sub => ExprNode::Sub(left, right, policy),
                DecimalOp::Mul => ExprNode::Mul(left, right, policy),
                DecimalOp::Div => ExprNode::Div(left, right, policy),
                DecimalOp::Mod => ExprNode::Mod(left, right, policy),
            };
            let output = DataType::Decimal256(76, output_scale);
            let nullable = arena.push_typed(node(OutputNull), output.clone());
            let throwing = arena.push_typed(node(ReportError), output.clone());
            let result = arena.eval(nullable, &batch).unwrap();
            assert_eq!(result.data_type(), &output);
            let result = result.as_any().downcast_ref::<Decimal256Array>().unwrap();
            assert_eq!(result.value(0).to_string(), expected);
            assert!(result.is_null(1), "input NULL remains NULL");
            assert!(
                result.is_null(2),
                "numeric overflow is NULL under OutputNull"
            );
            assert_eq!(
                arena.eval(throwing, &batch).unwrap_err(),
                decimal_overflow_error(operation)
            );
            let again = arena.eval(nullable, &batch).unwrap();
            let again = again.as_any().downcast_ref::<Decimal256Array>().unwrap();
            assert_eq!(again.value(0).to_string(), expected);
            assert_eq!(again.null_count(), 2);
        }

        // max * 2 is representable by i256 but exceeds declared precision;
        // max * 6 exceeds i256 itself. Both are checked numeric overflow.
        let batch = chunk(vec![
            Arc::new(
                Decimal256Array::from(vec![Some(max), None])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal256Array::from(vec![Some(i256::from_i128(6)); 2])
                    .with_precision_and_scale(2, 0)
                    .unwrap(),
            ) as ArrayRef,
        ]);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal256(76, 0),
        );
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Decimal256(2, 0));
        let nullable = arena.push_typed(
            ExprNode::Mul(left, right, OutputNull),
            DataType::Decimal256(76, 0),
        );
        let throwing = arena.push_typed(
            ExprNode::Mul(left, right, ReportError),
            DataType::Decimal256(76, 0),
        );
        assert_eq!(arena.eval(nullable, &batch).unwrap().null_count(), 2);
        assert_eq!(
            arena.eval(throwing, &batch).unwrap_err(),
            decimal_overflow_error(DecimalOp::Mul)
        );

        // Maximum finite input divided/modulo zero is NULL, never overflow;
        // either input's NULL also remains NULL under the reporting policy.
        let batch = chunk(vec![
            Arc::new(
                Decimal256Array::from(vec![None, Some(max), Some(i256::ZERO), Some(max)])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::ZERO),
                    Some(i256::ZERO),
                    Some(i256::ZERO),
                    None,
                ])
                .with_precision_and_scale(2, 0)
                .unwrap(),
            ) as ArrayRef,
        ]);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal256(76, 0),
        );
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Decimal256(2, 0));
        for policy in [OutputNull, ReportError] {
            for (node, scale) in [
                (ExprNode::Div(left, right, policy), 6),
                (ExprNode::Mod(left, right, policy), 0),
            ] {
                let result = arena.push_typed(node, DataType::Decimal256(76, scale));
                assert_eq!(arena.eval(result, &batch).unwrap().null_count(), 4);
            }
        }
    }

    #[test]
    fn prepared_arithmetic_nodes_keep_independent_policies_for_every_decimal_operation() {
        use DecimalOverflowPolicy::{OutputNull, ReportError};
        let max = 10_i128.pow(38) - 1;
        for (operation, overflow_left, right_value, right_scale, output_scale, first) in [
            (DecimalOp::Add, max, 1, 0, 0, 2),
            (DecimalOp::Sub, -max, 1, 0, 0, 0),
            (DecimalOp::Mul, max, 2, 0, 0, 2),
            (DecimalOp::Div, max, 1, 1, 6, 10_000_000),
            (DecimalOp::Mod, max, 1, 1, 1, 0),
        ] {
            let left = Arc::new(
                Decimal128Array::from(vec![Some(1), None, Some(overflow_left)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ) as ArrayRef;
            let right = Arc::new(
                Decimal128Array::from(vec![Some(right_value); 3])
                    .with_precision_and_scale(2, right_scale)
                    .unwrap(),
            ) as ArrayRef;
            let batch = chunk(vec![left, right]);
            let mut arena = ExprArena::default();
            let left = arena.push_typed(
                ExprNode::SlotId(SlotId::new(1)),
                DataType::Decimal128(38, 0),
            );
            let right = arena.push_typed(
                ExprNode::SlotId(SlotId::new(2)),
                DataType::Decimal128(2, right_scale),
            );
            let node = |policy| match operation {
                DecimalOp::Add => ExprNode::Add(left, right, policy),
                DecimalOp::Sub => ExprNode::Sub(left, right, policy),
                DecimalOp::Mul => ExprNode::Mul(left, right, policy),
                DecimalOp::Div => ExprNode::Div(left, right, policy),
                DecimalOp::Mod => ExprNode::Mod(left, right, policy),
            };
            let output = DataType::Decimal128(38, output_scale);
            let nullable = arena.push_typed(node(OutputNull), output.clone());
            let throwing = arena.push_typed(node(ReportError), output);
            let result = arena.eval(nullable, &batch).unwrap();
            let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(result.value(0), first);
            assert!(result.is_null(1));
            assert!(result.is_null(2));
            assert_eq!(
                arena.eval(throwing, &batch).unwrap_err(),
                decimal_overflow_error(operation)
            );
            // A failed sibling publishes no partial result and cannot mutate the NULL policy.
            assert_eq!(arena.eval(nullable, &batch).unwrap().null_count(), 2);
        }
    }

    #[test]
    fn prepared_null_and_zero_are_not_decimal_overflow_and_allow_throw_is_independent() {
        use DecimalOverflowPolicy::{OutputNull, ReportError};
        let batch = chunk(vec![
            Arc::new(
                Decimal128Array::from(vec![Some(1), None])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(0), Some(1)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ]);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 0),
        );
        let right = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 0),
        );
        for node in [
            ExprNode::Div(left, right, ReportError),
            ExprNode::Mod(left, right, ReportError),
        ] {
            let id = arena.push_typed(node, DataType::Decimal128(38, 6));
            assert_eq!(arena.eval(id, &batch).unwrap().null_count(), 2);
        }
        let overflow = chunk(vec![
            Arc::new(
                Decimal128Array::from(vec![10_i128.pow(38) - 1])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![2])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ]);
        arena.set_allow_throw_exception(true);
        let add = arena.push_typed(
            ExprNode::Add(left, right, OutputNull),
            DataType::Decimal128(38, 0),
        );
        let multiply = arena.push_typed(
            ExprNode::Mul(left, right, OutputNull),
            DataType::Decimal128(38, 0),
        );
        assert_eq!(arena.eval(add, &overflow).unwrap().null_count(), 1);
        assert!(
            arena
                .eval(multiply, &overflow)
                .unwrap_err()
                .contains("'mul'")
        );
    }
}

#[cfg(test)]
mod legacy_signed_prepared_oracle_tests {
    use super::*;
    use crate::exec::expr::ExprNode;
    use arrow::array::{Int8Array, Int16Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_functions::{
        ArithmeticRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
        PreparedArithmeticRecipe,
    };
    use novarocks_type_contract::{
        ArithmeticOperator, CompileControlError, CompilePhase, FunctionValueType,
        PureCompileControl, arithmetic_result_value_type_with_op,
    };
    use novarocks_types::SlotId;
    use std::time::Duration;

    struct OriginalControl;
    impl PureCompileControl for OriginalControl {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    impl KernelEvaluationControl for OriginalControl {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= 256);
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("signed arithmetic has no waiting operation")
        }
    }

    fn signed_array(ty: &DataType, values: &[Option<i64>]) -> ArrayRef {
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
            _ => panic!("fixture must author a signed source"),
        }
    }

    fn fixture(
        op: ArithmeticOperator,
        left: ArrayRef,
        right: ArrayRef,
        policy: DecimalOverflowPolicy,
        allow: bool,
    ) -> (ExprArena, ExprId, Chunk, PreparedArithmeticRecipe) {
        let left_type = FunctionValueType::new(left.data_type().clone(), true);
        let right_type = FunctionValueType::new(right.data_type().clone(), true);
        let result = arithmetic_result_value_type_with_op(&left_type, &right_type, op).unwrap();
        let recipe = PreparedArithmeticRecipe::try_new(
            op,
            &left_type,
            &right_type,
            &result,
            policy,
            allow,
            &OriginalControl,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", left_type.data_type().clone(), true),
            Field::new("right", right_type.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        arena.set_allow_throw_exception(allow);
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            left_type.data_type().clone(),
        );
        let right = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            right_type.data_type().clone(),
        );
        let kind = match op {
            ArithmeticOperator::Add => ExprNode::Add(left, right, policy),
            ArithmeticOperator::Subtract => ExprNode::Sub(left, right, policy),
            ArithmeticOperator::Multiply => ExprNode::Mul(left, right, policy),
            ArithmeticOperator::Divide => ExprNode::Div(left, right, policy),
            ArithmeticOperator::Modulo => ExprNode::Mod(left, right, policy),
        };
        let id = arena.push_typed(kind, result.data_type().clone());
        (arena, id, chunk, recipe)
    }

    fn prepared_row(
        recipe: &PreparedArithmeticRecipe,
        left: &ArrayRef,
        right: &ArrayRef,
        row: usize,
    ) -> ArithmeticRowResult {
        recipe
            .evaluate_row(
                EvaluatedArgument::Column(left),
                row,
                row,
                EvaluatedArgument::Column(right),
                row,
                row,
                &OriginalControl,
            )
            .unwrap()
    }

    fn assert_same_row(legacy: &ArrayRef, row: usize, prepared: ArithmeticRowResult) {
        match prepared {
            ArithmeticRowResult::Null => assert!(legacy.is_null(row)),
            ArithmeticRowResult::Signed(value) => {
                assert!(!legacy.is_null(row));
                let value_at = match legacy.data_type() {
                    DataType::Int16 => i64::from(
                        legacy
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .value(row),
                    ),
                    DataType::Int32 => i64::from(
                        legacy
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row),
                    ),
                    DataType::Int64 => legacy
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    _ => panic!("foreign signed result"),
                };
                assert_eq!(value_at, value);
            }
            ArithmeticRowResult::Float(value) => {
                assert!(!legacy.is_null(row));
                assert_eq!(
                    legacy
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_bits(),
                    value.to_bits()
                );
            }
            ArithmeticRowResult::LargeInt(_)
            | ArithmeticRowResult::Decimal128(_)
            | ArithmeticRowResult::Decimal256(_) => {
                panic!("foreign result in signed legacy oracle")
            }
            ArithmeticRowResult::RowError(error) => {
                panic!("unexpected row error: {}", error.message())
            }
        }
    }

    #[test]
    fn legacy_signed_arithmetic_oracle_matches_prepared_rows_for_all_frozen_width_pairs() {
        use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
        for left_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for right_type in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                let left = signed_array(&left_type, &[Some(7), Some(-7), None, Some(0)]);
                let right = signed_array(&right_type, &[Some(3), Some(-3), Some(7), None]);
                for op in [Add, Subtract, Multiply, Divide, Modulo] {
                    let (arena, id, chunk, recipe) = fixture(
                        op,
                        left.clone(),
                        right.clone(),
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    );
                    let legacy = arena.eval(id, &chunk).unwrap();
                    assert_eq!(
                        legacy.data_type().clone(),
                        recipe.result_type().data_type().clone()
                    );
                    for row in 0..left.len() {
                        assert_same_row(&legacy, row, prepared_row(&recipe, &left, &right, row));
                    }
                }
            }
        }
    }

    #[test]
    fn legacy_signed_fault_oracle_distinguishes_whole_batch_errors_from_prepared_row_errors() {
        use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                for (op, lhs, rhs) in [
                    (Add, i64::MAX, 1),
                    (Subtract, i64::MIN, 1),
                    (Multiply, i64::MAX, 2),
                    (Modulo, 7, 0),
                ] {
                    let left = signed_array(&DataType::Int64, &[None, Some(lhs)]);
                    let right = signed_array(&DataType::Int64, &[Some(rhs), Some(rhs)]);
                    let (arena, id, chunk, recipe) =
                        fixture(op, left.clone(), right.clone(), policy, allow);
                    // The legacy Arrow kernel fails the batch; the selected recipe
                    // retains the same diagnostic at the actual required row.
                    let legacy_error = arena.eval(id, &chunk).unwrap_err();
                    assert_eq!(
                        prepared_row(&recipe, &left, &right, 0),
                        ArithmeticRowResult::Null
                    );
                    let ArithmeticRowResult::RowError(error) =
                        prepared_row(&recipe, &left, &right, 1)
                    else {
                        panic!("required signed fault disappeared");
                    };
                    assert_eq!(error.selected_ordinal(), 1);
                    assert!(legacy_error.contains(error.message()), "{legacy_error}");
                    let masked_left = signed_array(&DataType::Int64, &[None]);
                    let masked_right = signed_array(&DataType::Int64, &[Some(rhs)]);
                    let (arena, id, chunk, recipe) =
                        fixture(op, masked_left.clone(), masked_right.clone(), policy, allow);
                    let masked = arena.eval(id, &chunk).unwrap();
                    assert_same_row(
                        &masked,
                        0,
                        prepared_row(&recipe, &masked_left, &masked_right, 0),
                    );
                }
                let left =
                    signed_array(&DataType::Int64, &[Some(7), Some(i64::MIN), Some(7), None]);
                let right = signed_array(&DataType::Int64, &[Some(0), Some(-1), Some(2), Some(0)]);
                let (arena, id, chunk, recipe) =
                    fixture(Divide, left.clone(), right.clone(), policy, allow);
                let legacy = arena.eval(id, &chunk).unwrap();
                assert_eq!(legacy.data_type(), &DataType::Float64);
                let values = legacy.as_any().downcast_ref::<Float64Array>().unwrap();
                assert!(values.is_null(0));
                assert_eq!(
                    values.value(1).to_bits(),
                    9_223_372_036_854_775_808.0_f64.to_bits()
                );
                assert_eq!(values.value(2), 3.5);
                assert!(values.is_null(3));
                for row in 0..left.len() {
                    assert_same_row(&legacy, row, prepared_row(&recipe, &left, &right, row));
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "arithmetic_decimal_oracle_tests.rs"]
mod arithmetic_decimal_oracle_tests;

#[cfg(test)]
#[path = "arithmetic_largeint_oracle_tests.rs"]
mod arithmetic_largeint_oracle_tests;
