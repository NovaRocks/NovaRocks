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

//! Actual LocalCompiler/CompiledFixture decimal text selection and empty output.
use super::*;
use crate::EvaluationInput;
use crate::testkit::PassThroughCallHost;
use arrow::array::{Decimal128Array, Decimal256Array, StringArray};
use arrow::datatypes::i256;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

#[test]
fn decimal_text_actual_compiler_both_widths_scales_sparse_slice_and_empty() {
    for wide in [false, true] {
        for scale in [-3, 0, 2, if wide { 76 } else { 38 }] {
            let input: ArrayRef = if wide {
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::ZERO),
                        Some(i256::from_i128(5)),
                        None,
                        Some(i256::from_i128(-1)),
                    ])
                    .with_precision_and_scale(76, scale)
                    .unwrap(),
                )
            } else {
                Arc::new(
                    Decimal128Array::from(vec![Some(i128::MIN), Some(5), None, Some(-1)])
                        .with_precision_and_scale(38, scale)
                        .unwrap(),
                )
            };
            let input = input.slice(1, 3);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(input.data_type().clone(), true),
                        FunctionValueType::new(DataType::Utf8, true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let schema = program.input_schema().clone();
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            input.clone(),
                            Arc::new(Int64Array::from(vec![42; 3])),
                            Arc::new(BooleanArray::from(vec![true; 3])),
                        ],
                    )
                    .unwrap();
                    let rows = [0, 1, 2];
                    let selection = Selection::try_sparse(3, &rows).unwrap();
                    let mut evaluator = instance(&program);
                    let occurrence = cast_root_use(&program);
                    let summary = evaluator.effects[&occurrence];
                    assert_eq!(
                        summary
                            .for_use(summary.context())
                            .unwrap()
                            .may_raise_row_error,
                        !wide && scale > 0,
                    );
                    let result = evaluator
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            ScalarInvocationScope::new(selection, selection),
                            ScalarInvocationActivation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    // The frozen text owner spells nonpositive scales as the
                    // raw integer. Positive scales place exactly `scale` digits
                    // after the point; these literals are independent of its formatter.
                    let (five, minus_one) = match (wide, scale) {
                        (_, -3 | 0) => ("5", "-1"),
                        (_, 2) => ("0.05", "-0.01"),
                        (false, 38) => (
                            "0.00000000000000000000000000000000000005",
                            "-0.00000000000000000000000000000000000001",
                        ),
                        (true, 76) => (
                            "0.0000000000000000000000000000000000000000000000000000000000000000000000000005",
                            "-0.0000000000000000000000000000000000000000000000000000000000000000000000000001",
                        ),
                        _ => unreachable!(),
                    };
                    let expected = StringArray::from(vec![Some(five), None, Some(minus_one)]);
                    assert_eq!(result.values().to_data(), expected.to_data());
                    assert!(result.errors().is_empty());
                    let result = evaluator
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            {
                                let selection = Selection::try_sparse(3, &[]).unwrap();
                                ScalarInvocationScope::new(selection, selection)
                            },
                            ScalarInvocationActivation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    assert_eq!(
                        result.values().to_data(),
                        StringArray::from(Vec::<Option<&str>>::new()).to_data()
                    );
                    assert!(result.errors().is_empty());
                }
            }
        }
    }
}
