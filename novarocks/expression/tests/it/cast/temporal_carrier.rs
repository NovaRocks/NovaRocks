// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::timestamp::timestamp_array;
use super::*;
use arrow::array::{Date32Array, StringArray};
use arrow::datatypes::TimeUnit;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

#[test]
fn temporal_cast_actual_compiler_sparse_date_profiles_and_constant_producers() {
    for nullable in [false, true] {
        let dates: ArrayRef = Arc::new(Date32Array::from(if nullable {
            vec![Some(0), Some(-1), None, Some(1), Some(19782)]
        } else {
            vec![Some(0), Some(-1), Some(0), Some(1), Some(19782)]
        }));
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        for target in [
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(DataType::Date32, nullable),
                        FunctionValueType::new(target.clone(), nullable),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let schema = program.input_schema().clone();
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            dates.clone(),
                            Arc::new(Int64Array::from(vec![42; 5])),
                            Arc::new(BooleanArray::from(vec![true; 5])),
                        ],
                    )
                    .unwrap();
                    let mut evaluator = instance(&program);
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
                    // Unix-day/calendar facts, independent of either cast path.
                    let expected: ArrayRef = match &target {
                        DataType::Utf8 => Arc::new(StringArray::from(vec![
                            Some("1969-12-31"),
                            if nullable { None } else { Some("1970-01-01") },
                            Some("1970-01-02"),
                            Some("2024-02-29"),
                        ])),
                        DataType::Timestamp(unit, None) => {
                            let (day, leap) = match unit {
                                TimeUnit::Second => (86_400, 1_709_164_800),
                                TimeUnit::Millisecond => (86_400_000, 1_709_164_800_000),
                                TimeUnit::Microsecond => (86_400_000_000, 1_709_164_800_000_000),
                                TimeUnit::Nanosecond => {
                                    (86_400_000_000_000, 1_709_164_800_000_000_000)
                                }
                            };
                            timestamp_array(
                                *unit,
                                vec![
                                    Some(-day),
                                    if nullable { None } else { Some(0) },
                                    Some(day),
                                    Some(leap),
                                ],
                            )
                        }
                        _ => unreachable!("fixed temporal corpus"),
                    };
                    assert_eq!(result.values().to_data(), expected.to_data());
                    assert!(result.errors().is_empty());
                    let constant = compiled(
                        FunctionValueType::new(DataType::Date32, false),
                        FunctionValueType::new(target.clone(), false),
                        Source::Constant,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let schema = constant.input_schema().clone();
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(Date32Array::from(vec![0; 5])),
                            Arc::new(Int64Array::from(vec![42; 5])),
                            Arc::new(BooleanArray::from(vec![true; 5])),
                        ],
                    )
                    .unwrap();
                    let result = instance(&constant)
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
                    // Unix day 71 is 1970-03-13, 6_134_400 seconds after epoch.
                    let expected: ArrayRef = match &target {
                        DataType::Utf8 => Arc::new(StringArray::from(vec!["1970-03-13"; 4])),
                        DataType::Timestamp(unit, None) => {
                            let value = match unit {
                                TimeUnit::Second => 6_134_400,
                                TimeUnit::Millisecond => 6_134_400_000,
                                TimeUnit::Microsecond => 6_134_400_000_000,
                                TimeUnit::Nanosecond => 6_134_400_000_000_000,
                            };
                            timestamp_array(*unit, vec![Some(value); 4])
                        }
                        _ => unreachable!("fixed temporal corpus"),
                    };
                    assert_eq!(result.values().to_data(), expected.to_data());
                }
            }
        }
    }
}
#[test]
fn temporal_cast_actual_timestamp_date_profiles_skip_unselected_error_and_keep_selected_error() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let program = compiled(
                    FunctionValueType::new(DataType::Timestamp(unit, None), true),
                    FunctionValueType::new(DataType::Date32, true),
                    Source::Column,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let schema = program.input_schema().clone();
                let source =
                    timestamp_array(unit, vec![Some(i64::MAX), Some(-1), None, Some(0), Some(1)]);
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        source,
                        Arc::new(Int64Array::from(vec![42; 5])),
                        Arc::new(BooleanArray::from(vec![true; 5])),
                    ],
                )
                .unwrap();
                let rows = [1, 2, 3, 4];
                let result = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        {
                            let selection = Selection::try_sparse(5, &rows).unwrap();
                            ScalarInvocationScope::new(selection, selection)
                        },
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(
                    result.values().to_data(),
                    Date32Array::from(vec![Some(-1), None, Some(0), Some(0)]).to_data()
                );
                assert!(result.errors().is_empty());
                if unit == TimeUnit::Second {
                    let result = instance(&program)
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            ScalarInvocationScope::new(Selection::all(5), Selection::all(5)),
                            ScalarInvocationActivation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    assert_eq!(result.errors().len(), 1);
                    assert_eq!(result.errors()[0].selected_ordinal(), 0);
                    assert_eq!(
                        result.errors()[0].message(),
                        "Cast error: Cannot convert arrow_array::types::TimestampSecondType 9223372036854775807 to datetime"
                    );
                }
            }
        }
    }
}
