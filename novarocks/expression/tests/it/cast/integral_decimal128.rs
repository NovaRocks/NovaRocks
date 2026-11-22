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

//! Compiled CAST evaluation with fixed mathematical expectations.
use super::*;
use arrow::array::{Decimal128Array, Int8Array, Int16Array, Int32Array};
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_failure_contract::kernel_reason::NUMERIC_OUT_OF_RANGE;
use novarocks_functions::{DataFailureReason, ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

const MODES: [TrinoCastMode; 2] = [TrinoCastMode::Explicit, TrinoCastMode::Try];

// Pure input construction and output inspection, never a reference conversion.
fn integral_input(dtype: &DataType, raw: Vec<Option<i64>>) -> ArrayRef {
    macro_rules! array {
        ($array:ty, $value:ty) => {
            Arc::new(<$array>::from(
                raw.into_iter()
                    .map(|v| {
                        v.map(|n| {
                            <$value>::try_from(n).expect("fixture value fits its original carrier")
                        })
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match dtype {
        DataType::Int8 => array!(Int8Array, i8),
        DataType::Int16 => array!(Int16Array, i16),
        DataType::Int32 => array!(Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::from(raw)),
        _ => panic!("fixture must supply an actual signed carrier"),
    }
}
fn values(array: &ArrayRef) -> Vec<Option<i128>> {
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    (0..array.len())
        .map(|r| (!array.is_null(r)).then(|| array.value(r)))
        .collect()
}
#[derive(Clone, Copy, Debug)]
enum Expected {
    Value(i128),
    SourceNull,
    Overflow,
}
use Expected::{Overflow as O, SourceNull as N, Value as V};
impl Expected {
    fn outcome(self, mode: TrinoCastMode) -> (Option<i128>, Option<DataFailureReason>) {
        match self {
            Self::Value(value) => (Some(value), None),
            Self::SourceNull => (None, None),
            Self::Overflow if mode == TrinoCastMode::Try => (None, None),
            Self::Overflow => (None, Some(DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE))),
        }
    }
}
fn compare_integral(dtype: DataType) {
    // All four carriers represent -100/0/100 exactly. Scale zero keeps the
    // raw integer, and precision4 admits absolute magnitudes below 10_000.
    let expected_full = [V(-100), N, V(100), V(0)];
    for mode in MODES {
        let target = DataType::Decimal128(4, 0);
        let full = integral_input(&dtype, vec![Some(-100), None, Some(100), Some(0)]);
        let program = compiled_cast(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        for (source, fixed) in [
            (full.clone(), expected_full.as_slice()),
            (full.slice(1, 3), &expected_full[1..]),
            (full.slice(0, 0), &expected_full[..0]),
        ] {
            let n = source.len();
            let b = RecordBatch::try_new(
                program.input_schema().clone(),
                vec![
                    source.clone(),
                    Arc::new(Int64Array::from(vec![42; n])),
                    Arc::new(BooleanArray::from(vec![true; n])),
                ],
            )
            .unwrap();
            for rows in [
                (0..n).collect::<Vec<_>>(),
                (0..n).filter(|r| r % 2 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(n, &rows).unwrap();
                let out = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &b,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(out.selection(), selection);
                assert_eq!(out.values().data_type(), &target);
                let mut wanted = Vec::new();
                let mut errors = Vec::new();
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    let (value, error) = fixed[row].outcome(mode);
                    wanted.push(value);
                    if let Some(reason) = error {
                        errors.push((ordinal, reason));
                    }
                }
                assert_eq!(values(out.values()), wanted);
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.reason()))
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
    }
}

#[test]
fn integral_decimal128_actual_compiler_i8_to_4_0_modes_selection_and_carrier() {
    compare_integral(DataType::Int8);
}
#[test]
fn integral_decimal128_actual_compiler_i16_to_4_0_modes_selection_and_carrier() {
    compare_integral(DataType::Int16);
}
#[test]
fn integral_decimal128_actual_compiler_i32_to_4_0_modes_selection_and_carrier() {
    compare_integral(DataType::Int32);
}
#[test]
fn integral_decimal128_actual_compiler_i64_to_4_0_modes_selection_and_carrier() {
    compare_integral(DataType::Int64);
}

mod after;

#[test]
fn decimal_to_tinyint_overflow_keeps_its_selected_row_error() {
    use crate::zero_port;
    use novarocks_expression_contract::ExprKind;
    use novarocks_type_contract::SemanticParameters;

    let input: ArrayRef = Arc::new(
        Decimal128Array::from(vec![1000])
            .with_precision_and_scale(4, 0)
            .unwrap(),
    );
    let unit = zero_port::primitive(
        [input],
        FunctionValueType::new(DataType::Int8, true),
        |children| ExprKind::Cast {
            expr: children[0],
            target: DataType::Int8,
            cast_mode: TrinoCastMode::Explicit,
        },
        &SemanticParameters::default(),
    )
    .unwrap();
    let output = zero_port::evaluate(unit);
    assert!(output.values().is_null(0));
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        output.errors()[0].reason(),
        DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE)
    );
}
