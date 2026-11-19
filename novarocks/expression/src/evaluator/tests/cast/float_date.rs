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

//! Actual LocalCompiler strict float-DATE profiles and native source/first-failure contracts.
use super::*;
use crate::EvaluationInput;
use crate::testkit::PassThroughCallHost;
use arrow::array::{Date32Array, Float32Array, Float64Array};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
// Pure authored input construction; conversion to f32 defines the source
// carrier, not the expected DATE. No old evaluator is borrowed.
fn input(wide: bool, values: Vec<Option<f64>>) -> ArrayRef {
    if wide {
        Arc::new(Float64Array::from(values))
    } else {
        Arc::new(Float32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    }
}
fn float_batch(program: &CompiledFixture, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    let schema = program.input_schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
fn source_type(wide: bool) -> DataType {
    if wide {
        DataType::Float64
    } else {
        DataType::Float32
    }
}
#[test]
fn float_date_actual_compiler_all_profiles_root_nullability_allow_policies_slice_selection_and_empty()
 {
    for wide in [false, true] {
        for source_nullable in [false, true] {
            for result_nullable in [true, false] {
                if source_nullable && !result_nullable {
                    continue;
                }
                let mut values = vec![
                    Some(f64::NAN),
                    Some(19700102.0),
                    Some(20240230.0),
                    Some(20240229.0),
                    Some(0.0),
                    Some(f64::INFINITY),
                ];
                if source_nullable {
                    values.push(None);
                }
                let padded = input(wide, [vec![Some(999999.0)], values.clone()].concat());
                let source = padded.slice(1, values.len());
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let program = compiled(
                            FunctionValueType::new(source_type(wide), source_nullable),
                            FunctionValueType::new(DataType::Date32, result_nullable),
                            Source::Column,
                            Wrap::Bare,
                            policy,
                            allow,
                        );
                        let batch = float_batch(&program, source.clone());
                        let mut evaluator = instance(&program);
                        let occurrence = cast_root_use(&program);
                        let summary = evaluator.effects[&occurrence];
                        assert!(
                            summary
                                .for_use(summary.context())
                                .unwrap()
                                .may_raise_row_error
                        );
                        let rows = [1, 2, 3, 5];
                        let selection = Selection::try_sparse(values.len(), &rows).unwrap();
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
                        // YYYYMMDD numeric grammar: 19700102 is epoch day 1;
                        // February 30 is invalid. In f64, 20240229 means leap
                        // day 19_782. f32 has ULP 2 here: ties-to-even rounds
                        // 20240229 to 20240228, which is day 19_781. Infinity
                        // is invalid under both policies and both allow flags.
                        // These are calendar/IEEE-derived literals, not results
                        // obtained from either the new or retired cast code.
                        let expected = vec![
                            Some(1),
                            None,
                            Some(if wide { 19_782 } else { 19_781 }),
                            None,
                        ];
                        let expected_errors = [
                            (
                                1,
                                format!(
                                    "CAST failed: from {:?} to Date32: invalid date literal 20240230",
                                    source_type(wide)
                                ),
                            ),
                            (
                                3,
                                format!(
                                    "CAST failed: from {:?} to Date32: invalid date literal inf",
                                    source_type(wide)
                                ),
                            ),
                        ];
                        assert_eq!(
                            result
                                .errors()
                                .iter()
                                .map(|e| e.selected_ordinal())
                                .collect::<Vec<_>>(),
                            vec![1, 3],
                        );
                        assert_eq!(result.errors().len(), expected_errors.len());
                        for (error, (ordinal, message)) in
                            result.errors().iter().zip(&expected_errors)
                        {
                            assert_eq!(error.selected_ordinal(), *ordinal);
                            assert_eq!(error.message(), message.as_str());
                        }
                        assert_eq!(
                            result.values().to_data(),
                            Date32Array::from(expected).to_data()
                        );
                        let empty = [];
                        let result = instance(&program)
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &batch,
                                    source: None,
                                },
                                {
                                    let selection =
                                        Selection::try_sparse(values.len(), &empty).unwrap();
                                    ScalarInvocationScope::new(selection, selection)
                                },
                                ScalarInvocationActivation::Activated,
                                &Control,
                                &mut PassThroughCallHost,
                            )
                            .unwrap();
                        assert_eq!(result.values().data_type(), &DataType::Date32);
                        assert_eq!(result.values().len(), 0);
                        assert!(result.errors().is_empty());
                        if source_nullable {
                            let rows = [values.len() - 1];
                            let result = instance(&program)
                                .evaluate(
                                    EvaluationInput::Batch {
                                        batch: &batch,
                                        source: None,
                                    },
                                    {
                                        let selection =
                                            Selection::try_sparse(values.len(), &rows).unwrap();
                                        ScalarInvocationScope::new(selection, selection)
                                    },
                                    ScalarInvocationActivation::Activated,
                                    &Control,
                                    &mut PassThroughCallHost,
                                )
                                .unwrap();
                            assert!(result.values().is_null(0));
                            assert!(result.errors().is_empty());
                        }
                    }
                }
            }
        }
    }
}
