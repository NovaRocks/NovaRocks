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

//! Actual frozen package -> LocalCompiler -> Frame, using fixed mathematical expectations.
use super::*;
use arrow::array::{Decimal128Array, Int8Array, Int16Array, Int32Array};
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
const OVERFLOW: &str =
    "Expr evaluate meet error: The numeric type cast involving decimal overflows";
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
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
    // Only interpret a handwritten overflow marker's existing policy response.
    // Integral numeric CAST ignores allow-throw for OutputNull overflow.
    fn result(self, policy: DecimalOverflowPolicy) -> (Option<i128>, Option<&'static str>) {
        match self {
            Self::Value(value) => (Some(value), None),
            Self::SourceNull => (None, None),
            Self::Overflow if policy == DecimalOverflowPolicy::ReportError => {
                (None, Some(OVERFLOW))
            }
            Self::Overflow => (None, None),
        }
    }
}
fn compare_integral(dtype: DataType) {
    // All four carriers represent -100/0/100 exactly. Scale zero keeps the
    // raw integer, and precision4 admits absolute magnitudes below 10_000.
    let expected_full = [V(-100), N, V(100), V(0)];
    for (policy, allow) in modes() {
        let target = DataType::Decimal128(4, 0);
        let full = integral_input(&dtype, vec![Some(-100), None, Some(100), Some(0)]);
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        for (source, fixed) in [
            (full.clone(), expected_full.as_slice()),
            (full.slice(1, 3), &expected_full[1..]),
            (full.slice(0, 0), &expected_full[..0]),
        ] {
            let n = source.len();
            let b = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
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
                    let (value, error) = fixed[row].result(policy);
                    wanted.push(value);
                    if let Some(message) = error {
                        errors.push((ordinal, message.to_owned()));
                    }
                }
                assert_eq!(values(out.values()), wanted);
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
    }
}

#[test]
fn integral_decimal128_actual_compiler_i8_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int8);
}
#[test]
fn integral_decimal128_actual_compiler_i16_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int16);
}
#[test]
fn integral_decimal128_actual_compiler_i32_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int32);
}
#[test]
fn integral_decimal128_actual_compiler_i64_to_4_0_policy_selection_and_carrier() {
    compare_integral(DataType::Int64);
}

#[path = "cast_integral_decimal128_after_tests.rs"]
mod after;
