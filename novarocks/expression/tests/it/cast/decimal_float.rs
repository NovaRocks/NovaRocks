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

//! Decimal-to-floating CAST at the real compiled expression boundary.

use super::*;
use arrow::array::Decimal128Array;
use novarocks_expression::{
    CallAuthorError, CapabilityRefusal, EvaluationInput, ExprEvaluationFailure,
    testkit::{
        PassThroughCallHost,
        numeric_fixtures::{cast_fixture_in_mode, compiled_cast},
    },
};
use novarocks_functions::{CastPrepareError, ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

fn decimals(precision: u8, scale: i8, coefficients: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(coefficients)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn batch(program: &CompiledFixture, source: ArrayRef) -> RecordBatch {
    let rows = source.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; rows])),
            Arc::new(BooleanArray::from(vec![true; rows])),
        ],
    )
    .unwrap()
}

fn assert_bits(array: &ArrayRef, target: &DataType, expected: &[Option<u64>]) {
    assert_eq!(array.data_type(), target);
    let bits: Vec<Option<u64>> = match target {
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .map(|value| value.map(|value| u64::from(value.to_bits())))
            .collect(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|value| value.map(f64::to_bits))
            .collect(),
        _ => unreachable!("only REAL and DOUBLE are tested here"),
    };
    assert_eq!(bits, expected);
}

fn compare(target: DataType, precision: u8, scale: i8, last_coefficient: i128, bits: [u64; 3]) {
    let [positive, negative, last] = bits;
    for source_nullable in [false, true] {
        let null_or_zero = (!source_nullable).then_some(0);
        let values = decimals(
            precision,
            scale,
            vec![
                Some(0),
                Some(1),
                null_or_zero,
                Some(-1),
                Some(last_coefficient),
            ],
        )
        .slice(1, 4);
        let expected = [
            Some(positive),
            null_or_zero.map(|_| 0),
            Some(negative),
            Some(last),
        ];
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let program = compiled_cast(
                FunctionValueType::new(values.data_type().clone(), source_nullable),
                FunctionValueType::new(
                    target.clone(),
                    source_nullable || mode == TrinoCastMode::Try,
                ),
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            );
            let data = batch(&program, values.clone());
            for rows in [&[0, 1, 2, 3][..], &[0, 1, 3], &[]] {
                let selected = Selection::try_sparse(4, rows).unwrap();
                let out = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &data,
                            source: None,
                        },
                        ScalarInvocationScope::new(selected, selected),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(out.selection(), selected);
                assert_bits(
                    out.values(),
                    &target,
                    &rows.iter().map(|&row| expected[row]).collect::<Vec<_>>(),
                );
                assert!(out.errors().is_empty());
            }

            let constant = compiled_cast(
                FunctionValueType::new(values.data_type().clone(), false),
                FunctionValueType::new(target.clone(), mode == TrinoCastMode::Try),
                Source::Constant,
                Wrap::Bare,
                mode,
                false,
            );
            let data = batch(&constant, decimals(precision, scale, vec![Some(0); 4]));
            let selected = Selection::try_sparse(4, &[0, 1, 3]).unwrap();
            let out = instance(&constant)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(selected, selected),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(out.selection(), selected);
            // The compiled decimal constant has coefficient 1 at the original scale.
            assert_bits(out.values(), &target, &[Some(positive); 3]);
            assert!(out.errors().is_empty());
        }
    }
}

const MAX_DECIMAL38: i128 = 10_i128.pow(38) - 1;

#[test]
fn decimal_float_actual_compiler_decimal128_float64() {
    // 2^53 + 1 is the exact midpoint between consecutive binary64 integers.
    compare(
        DataType::Float64,
        38,
        0,
        9_007_199_254_740_993,
        [0x3ff0000000000000, 0xbff0000000000000, 0x4340000000000000],
    );
    compare(
        DataType::Float64,
        1,
        1,
        9,
        [0x3fb999999999999a, 0xbfb999999999999a, 0x3feccccccccccccd],
    );
    // A scale-38 coefficient of 1 is 1e-38; the maximal coefficient rounds to 1.
    compare(
        DataType::Float64,
        38,
        38,
        MAX_DECIMAL38,
        [0x380b38fb9daa78e4, 0xb80b38fb9daa78e4, 0x3ff0000000000000],
    );
}

#[test]
fn decimal_float32_actual_compiler_decimal128_float32() {
    // 2^24 + 1 is the exact midpoint between consecutive binary32 integers.
    compare(
        DataType::Float32,
        38,
        0,
        16_777_217,
        [0x3f800000, 0xbf800000, 0x4b800000],
    );
    compare(
        DataType::Float32,
        1,
        1,
        9,
        [0x3dcccccd, 0xbdcccccd, 0x3f666666],
    );
    // Both signs of 1e-38 round to a binary32 subnormal.
    compare(
        DataType::Float32,
        38,
        38,
        MAX_DECIMAL38,
        [0x006ce3ee, 0x806ce3ee, 0x3f800000],
    );
}

#[test]
fn decimal_float_actual_compiler_rejects_retired_and_malformed_decimals() {
    let retired = DataType::Decimal256(76, 0);
    let malformed = decimals(1, 0, vec![Some(i128::MAX)]);
    for target in [DataType::Float32, DataType::Float64] {
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let result = FunctionValueType::new(target.clone(), true);
            let error = match cast_fixture_in_mode(
                FunctionValueType::new(retired.clone(), true),
                result.clone(),
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            ) {
                Ok(_) => panic!("retired Decimal256 conversion was admitted"),
                Err(error) => error,
            };
            let CallAuthorError::Capability(CapabilityRefusal::Primitive { source, .. }) = error
            else {
                panic!("expected a typed CAST refusal, got {error:?}");
            };
            assert_eq!(
                source.downcast_ref::<CastPrepareError>(),
                Some(&CastPrepareError::Illegal)
            );

            let program = compiled_cast(
                FunctionValueType::new(malformed.data_type().clone(), true),
                result,
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            );
            let data = batch(&program, malformed.clone());
            let selected = Selection::all(1);
            assert!(matches!(
                instance(&program).evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None
                    },
                    ScalarInvocationScope::new(selected, selected),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                ),
                Err(ExprEvaluationFailure::Kernel(
                    KernelFailure::InvalidProgram(_)
                ))
            ));
        }
    }
}
