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
//! Compiled CAST evaluation with fixed decimal coefficients.
//! Expected values follow exact binary 1.25 = 5/4, decimal scale and precision;
//! no old evaluator, Arrow cast or replacement conversion calculator is used.
use super::*;
use arrow::array::Decimal128Array;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::{
    PassThroughCallHost,
    numeric_fixtures::{compiled_cast, float_batch},
};
use novarocks_failure_contract::kernel_reason::{INVALID_CAST_ARGUMENT, NUMERIC_OUT_OF_RANGE};
use novarocks_functions::{DataFailureReason, ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

fn values(array: &ArrayRef) -> Vec<Option<i128>> {
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row)))
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Expected {
    Value(i128),
    SourceNull,
    Error(DataFailureReason),
}
use Expected::{Error as E, SourceNull as N, Value as V};

impl Expected {
    fn outcome(self, mode: TrinoCastMode) -> (Option<i128>, Option<DataFailureReason>) {
        match self {
            Self::Value(value) => (Some(value), None),
            Self::SourceNull => (None, None),
            Self::Error(_) if mode == TrinoCastMode::Try => (None, None),
            Self::Error(reason) => (None, Some(reason)),
        }
    }
}

#[test]
fn double_to_decimal_cast_preserves_rounding_and_selected_error_reasons() {
    let overflow = E(DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE));
    let nan = E(DataFailureReason::Domain(INVALID_CAST_ARGUMENT));
    // The first two inputs are binary64 neighbors of the decimal(1, 0)
    // boundary. The cutover is the half tie 9.5, which rounds to 10.
    let source = [
        Some(f64::from_bits(9.5_f64.to_bits() - 1)),
        Some(9.5),
        Some(1.25),
        None,
        Some(-1.25),
        Some(f64::NAN),
        Some(f64::INFINITY),
    ];
    for (p, s, fixed) in [
        (1, 0, [V(9), overflow, V(1), N, V(-1), nan, overflow]),
        (
            1,
            1,
            [overflow, overflow, overflow, N, overflow, nan, overflow],
        ),
        (2, 1, [V(95), V(95), V(13), N, V(-13), nan, overflow]),
        (
            18,
            4,
            [V(95000), V(95000), V(12500), N, V(-12500), nan, overflow],
        ),
        (20, 0, [V(9), V(10), V(1), N, V(-1), nan, overflow]),
        (
            38,
            38,
            [overflow, overflow, overflow, N, overflow, nan, overflow],
        ),
    ] {
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let target = DataType::Decimal128(p, s);
            let program = compiled_cast(
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(target.clone(), true),
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            );
            let data = float_batch(&program, &source);
            let sparse = Selection::try_sparse(source.len(), &[0, 1, 3, 5, 6]).unwrap();
            let empty = Selection::try_sparse(source.len(), &[]).unwrap();
            for selected in [sparse, Selection::all(source.len()), empty] {
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
                let mut expected = Vec::new();
                let mut expected_errors = Vec::new();
                for (ordinal, row) in selected.iter().enumerate() {
                    let (value, error) = fixed[row].outcome(mode);
                    expected.push(value);
                    if let Some(reason) = error {
                        expected_errors.push((ordinal, reason));
                    }
                }
                assert_eq!(values(out.values()), expected, "{target:?} {mode:?}");
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|error| (error.selected_ordinal(), error.reason()))
                        .collect::<Vec<_>>(),
                    expected_errors,
                    "{target:?} {mode:?}"
                );
                assert_eq!(out.values().data_type(), &target);
            }
            let constant = compiled_cast(
                FunctionValueType::new(DataType::Float64, false),
                FunctionValueType::new(target.clone(), true),
                Source::Constant,
                Wrap::Bare,
                mode,
                false,
            );
            let data = float_batch(&constant, &[Some(0.5); 7]);
            let out = instance(&constant)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(sparse, sparse),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            // Source::Constant emits DOUBLE -0.0, which has coefficient zero.
            assert_eq!(out.selection(), sparse);
            assert_eq!(out.values().data_type(), &target);
            assert_eq!(values(out.values()), vec![Some(0); 5]);
            assert!(out.errors().is_empty());
        }
    }
}
