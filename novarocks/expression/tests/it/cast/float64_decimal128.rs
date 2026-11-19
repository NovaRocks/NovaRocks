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
use arrow::array::{Decimal128Array, Float64Array};
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
const OVERFLOW: &str =
    "Expr evaluate meet error: The numeric type cast involving decimal overflows";
fn input(raw: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(raw))
}
fn values(array: &ArrayRef) -> Vec<Option<i128>> {
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row)))
        .collect()
}
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
#[derive(Clone, Copy, Debug)]
enum Expected {
    Value(i128),
    Null,
    Overflow,
}
use Expected::{Null as N, Overflow as O, Value as V};
impl Expected {
    // Only materialize the policy response to the explicit finite-overflow
    // marker. NaN/infinity and source NULL are distinct from finite overflow;
    // they remain successful NULL under every policy/allow combination.
    fn result(self, policy: DecimalOverflowPolicy) -> (Option<i128>, Option<&'static str>) {
        match self {
            Self::Value(value) => (Some(value), None),
            Self::Null => (None, None),
            Self::Overflow if policy == DecimalOverflowPolicy::ReportError => {
                (None, Some(OVERFLOW))
            }
            Self::Overflow => (None, None),
        }
    }
}
fn batch(program: &CompiledFixture, a: ArrayRef) -> RecordBatch {
    let n = a.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            a,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
#[test]
fn float64_decimal128_actual_compiler_complete_physical_profiles_and_constant_author() {
    // Exact input 1.25 has IEEE bits 0x3ff4000000000000. Listed coefficients
    // are for [1.25, source NULL, -1.25, NaN, +infinity], after the original slice.
    // The conversion rounds half ties away from zero; declared precision p
    // requires absolute coefficient < 10^p. Finite overflow follows policy.
    for (p, s, fixed) in [
        // Dividing 5/4 by 10^38 has magnitude below half, so rounds to zero.
        (1, -38, [V(0), N, V(0), N, N]),
        // +/-1.25 rounds to +/-1, within precision1's -9..=9.
        (1, 0, [V(1), N, V(-1), N, N]),
        // +/-12.5 rounds to +/-13, outside precision1.
        (1, 1, [O, N, O, N, N]),
        // (5/4)*10^4 is exactly 12500 and fits precision18.
        (18, 4, [V(12500), N, V(-12500), N, N]),
        (38, -38, [V(0), N, V(0), N, N]),
        // Magnitude 1.25*10^38 exceeds the precision38 limit 10^38. The
        // binary64 factor rounding cannot close the 25% excess; both signs O.
        (38, 38, [O, N, O, N, N]),
    ] {
        for (policy, allow) in modes() {
            let target = DataType::Decimal128(p, s);
            let program = compiled(
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(target.clone(), true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            let a = input(vec![
                Some(99.0),
                Some(1.25),
                None,
                Some(-1.25),
                Some(f64::NAN),
                Some(f64::INFINITY),
            ])
            .slice(1, 5);
            let data = batch(&program, a);
            let rows = [0, 1, 3, 4];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            for selected in [selection, Selection::all(5)] {
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
                    let (value, error) = fixed[row].result(policy);
                    expected.push(value);
                    if let Some(message) = error {
                        expected_errors.push((ordinal, message));
                    }
                }
                assert_eq!(values(out.values()), expected);
                assert_eq!(out.errors().len(), expected_errors.len());
                for (error, (ordinal, message)) in out.errors().iter().zip(expected_errors) {
                    assert_eq!(error.selected_ordinal(), ordinal);
                    assert_eq!(error.message(), message);
                }
                assert_eq!(out.values().data_type(), &target);
            }
            let empty_selection = Selection::try_sparse(5, &[]).unwrap();
            let empty = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(empty_selection, empty_selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(empty.selection(), empty_selection);
            assert_eq!(empty.values().data_type(), &target);
            assert!(empty.values().is_empty());
            assert!(empty.errors().is_empty());
            let program = compiled(
                FunctionValueType::new(DataType::Float64, false),
                FunctionValueType::new(target.clone(), true),
                Source::Constant,
                Wrap::Bare,
                policy,
                allow,
            );
            let data = batch(&program, input(vec![Some(7.0); 5]));
            let out = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            // The real parent source author emits Float64 -0.0 (bits
            // 0x8000000000000000), not the batch's 7.0. Every profile maps
            // signed zero to integer coefficient zero, and decimal has no
            // negative-zero representation. No precision overflow occurs.
            assert_eq!(out.selection(), selection);
            assert_eq!(out.values().data_type(), &target);
            assert_eq!(values(out.values()), vec![Some(0); 4]);
            assert!(out.errors().is_empty());
        }
    }
}
