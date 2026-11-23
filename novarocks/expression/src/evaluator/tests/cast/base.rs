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
use super::*;
use crate::EvaluationInput;
use crate::testkit::PassThroughCallHost;
use crate::testkit::numeric_fixtures::compiled_cast;
use arrow::array::Float32Array;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

#[test]
fn all_twenty_four_signed_cast_profiles_preserve_types_and_sparse_rows_in_both_modes() {
    for source in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let (min, max) = bound(&source);
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ] {
            for cast_mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
                let program = compiled_cast(
                    FunctionValueType::new(source.clone(), true),
                    FunctionValueType::new(target.clone(), true),
                    Source::Column,
                    Wrap::Bare,
                    cast_mode,
                    false,
                );
                let values = [Some(17), Some(min), None, Some(max), Some(-7), Some(19)];
                let batch = sliced_batch(&program, &values);
                let rows = [1, 2, 3, 4];
                let selection = Selection::try_sparse(6, &rows).unwrap();
                let mut evaluator = instance(&program);
                let output = evaluator
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
                assert_eq!(output.selection(), selection);
                assert_eq!(output.values().data_type(), &target);
                match target {
                    DataType::Float32 => {
                        assert!(output.errors().is_empty());
                        let actual = output
                            .values()
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap();
                        for (ordinal, row) in rows.into_iter().enumerate() {
                            assert_eq!(actual.is_null(ordinal), values[row].is_none());
                            if let Some(value) = values[row] {
                                assert_eq!(
                                    actual.value(ordinal).to_bits(),
                                    (value as f32).to_bits()
                                );
                            }
                        }
                    }
                    DataType::Float64 => {
                        assert!(output.errors().is_empty());
                        let actual = output
                            .values()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        for (ordinal, row) in rows.into_iter().enumerate() {
                            assert_eq!(actual.is_null(ordinal), values[row].is_none());
                            if let Some(value) = values[row] {
                                assert_eq!(
                                    actual.value(ordinal).to_bits(),
                                    (value as f64).to_bits()
                                );
                            }
                        }
                    }
                    _ => {
                        let (low, high) = bound(&target);
                        let expected_errors: Vec<_> = rows
                            .iter()
                            .enumerate()
                            .filter(|&(_, row)| {
                                cast_mode == TrinoCastMode::Explicit
                                    && values[*row]
                                        .is_some_and(|value| !(low..=high).contains(&value))
                            })
                            .map(|(ordinal, _)| ordinal)
                            .collect();
                        assert_eq!(errors(&output), expected_errors);
                        assert_eq!(
                                signed_values(&output),
                                rows.map(
                                    |row| values[row].filter(|value| (low..=high).contains(value))
                                )
                                .to_vec()
                            );
                    }
                }
                assert!(evaluator.instances.is_empty());
            }
        }
    }
}

#[test]
fn cast_literal_broadcast_empty_selection_and_real_round_rand_owners_keep_exact_effects_and_instances()
 {
    let constant = compiled_cast(
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Int16, true),
        Source::Constant,
        Wrap::Bare,
        TrinoCastMode::Explicit,
        false,
    );
    let batch = sliced_batch(&constant, &[Some(7); 5]);
    let mut evaluator = instance(&constant);
    let empty = [];
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(5, &empty).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.values().is_empty());
    assert!(evaluator.instances.is_empty());
    let rows = [0, 2, 4];
    let output = evaluator
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
    assert_eq!(signed_values(&output), vec![Some(71); 3]);
    assert!(evaluator.instances.is_empty());
    for source in [Source::Round, Source::RandCondition] {
        let program = compiled_cast(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Int8, true),
            source,
            if matches!(source, Source::RandCondition) {
                Wrap::If
            } else {
                Wrap::Bare
            },
            TrinoCastMode::Explicit,
            false,
        );
        let calls = root_unit(&program).calls();
        assert!(
            calls
                .values()
                .any(|call| call.call_contract().function_id().as_str()
                    == if matches!(source, Source::Round) {
                        "builtin.scalar/round/v1"
                    } else {
                        "builtin.scalar/rand/v1"
                    })
        );
        if matches!(source, Source::RandCondition) {
            assert!(calls.values().any(|call| {
                let effects = call.effects();
                effects.for_use(effects.context()).unwrap().value_stability
                    == novarocks_type_contract::FunctionVolatility::Volatile
            }));
        }
        let batch = sliced_batch(&program, &[Some(9), None, Some(130), Some(-7), Some(11)]);
        let mut evaluator = instance(&program);
        let empty_output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(5, &empty).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(empty_output.values().is_empty());
        assert!(evaluator.instances.is_empty());
        let selected = evaluator
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
        assert_eq!(signed_values(&selected), vec![Some(9), None, Some(11)]);
        assert_eq!(errors(&selected), [1]);
        assert_eq!(evaluator.instances.len(), 1);
    }
}
