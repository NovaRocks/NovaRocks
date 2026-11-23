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
use crate::testkit::numeric_fixtures::{compiled_cast, float_batch};
use novarocks_failure_contract::{FailureProjection, kernel_reason};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

fn edge_pair(source: &DataType, target: &DataType) -> ([f64; 4], [Option<i64>; 4]) {
    match (source, target) {
        (_, DataType::Int8) => (
            [-128.0, 127.0, -129.0, 128.0],
            [Some(-128), Some(127), None, None],
        ),
        (_, DataType::Int16) => (
            [-32768.0, 32767.0, -32769.0, 32768.0],
            [Some(-32768), Some(32767), None, None],
        ),
        (DataType::Float32, DataType::Int32) => (
            [
                f32::from_bits(0xcf000000) as f64,
                f32::from_bits(0x4effffff) as f64,
                f32::from_bits(0xcf000001) as f64,
                f32::from_bits(0x4f000000) as f64,
            ],
            [Some(i32::MIN.into()), Some(2147483520), None, None],
        ),
        (DataType::Float64, DataType::Int32) => (
            [-2147483648.0, 2147483647.0, -2147483649.0, 2147483648.0],
            [Some(i32::MIN.into()), Some(i32::MAX.into()), None, None],
        ),
        (DataType::Float32, DataType::Int64) => (
            [
                f32::from_bits(0xdf000000) as f64,
                f32::from_bits(0x5effffff) as f64,
                f32::from_bits(0xdf000001) as f64,
                f32::from_bits(0x5f000000) as f64,
            ],
            [Some(i64::MIN), Some(9223371487098961920), None, None],
        ),
        (DataType::Float64, DataType::Int64) => (
            [
                f64::from_bits(0xc3e0000000000000),
                f64::from_bits(0x43dfffffffffffff),
                f64::from_bits(0xc3e0000000000001),
                f64::from_bits(0x43e0000000000000),
            ],
            [Some(i64::MIN), Some(9223372036854774784), None, None],
        ),
        _ => panic!("one of the eight admitted profiles"),
    }
}

#[test]
fn float_signed_cast_modes_preserve_edges_and_sparse_selected_rows() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let (edges, edge_outputs) = edge_pair(&source, &target);
            let values = [
                Some(99.0),
                Some(edges[0]),
                Some(edges[1]),
                Some(edges[2]),
                Some(edges[3]),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(-0.0),
                Some(4.75),
                Some(-99.0),
            ];
            let rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            let selection = Selection::try_sparse(values.len(), &rows).unwrap();
            let expected = [
                edge_outputs[0],
                edge_outputs[1],
                None,
                None,
                None,
                None,
                None,
                None,
                Some(0),
                Some(5),
            ];
            for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
                let program = compiled_cast(
                    FunctionValueType::new(source.clone(), true),
                    FunctionValueType::new(target.clone(), true),
                    Source::Column,
                    Wrap::Bare,
                    mode,
                    false,
                );
                let occurrence = cast_root_use(&program);
                let unit = root_unit(&program);
                let recipe = unit.recipes().cast(occurrence).unwrap();
                let context = unit.control_flow().uses()[&occurrence].context;
                assert_eq!(
                    recipe
                        .own_effects(context)
                        .for_use(context)
                        .unwrap()
                        .may_raise_row_error,
                    mode == TrinoCastMode::Explicit
                );
                let batch = float_batch(&program, &values);
                let mut evaluator = instance(&program);
                let no_rows = Selection::try_sparse(values.len(), &[]).unwrap();
                let empty = evaluator
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(no_rows, no_rows),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert!(empty.values().is_empty());
                assert!(empty.errors().is_empty());
                for _ in 0..2 {
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
                    assert_eq!(signed_values(&output), expected);
                    if mode == TrinoCastMode::Explicit {
                        assert_eq!(errors(&output), [2, 3, 5, 6, 7]);
                        assert_eq!(
                            output.errors()[2].failure_fact().reason(),
                            kernel_reason::INVALID_CAST_ARGUMENT
                        );
                    } else {
                        assert!(output.errors().is_empty());
                    }
                }
                assert!(evaluator.instances.is_empty());
            }
        }
    }
}

#[test]
fn float_cast_inherits_real_round_journal_without_reinterpreting_error_null_or_falling_through() {
    for allow in [false, true] {
        let program = inherited_round_fixture_with_target(allow, DataType::Int8);
        let raw = 10_i128.pow(38) - 1;
        let batch = RecordBatch::try_new(
            program.input_schema().clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(7.9),
                    Some(12.9),
                    Some(128.0),
                    None,
                    Some(-7.9),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(raw), Some(raw), Some(10), None, Some(raw)])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let mut evaluator = instance(&program);
        assert!(evaluator.effects.values().any(|effect| {
            effect
                .for_use(effect.context())
                .unwrap()
                .may_raise_row_error
        }));
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
        assert_eq!(signed_values(&output), vec![None, None, None, None]);
        assert_eq!(errors(&output), [0, 1, 3]);
        let last = output.errors().last().unwrap();
        assert_eq!(output.errors()[0].message(), last.message());
        assert_eq!(
            output.errors()[1].failure_fact().reason(),
            kernel_reason::NUMERIC_OUT_OF_RANGE,
        );
        assert_eq!(output.selection(), selection);
    }
}

#[test]
fn float_cast_errors_remain_terminal_under_coalesce_and_is_null_with_full_child_effects() {
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        for wrap in [Wrap::Coalesce, Wrap::IsNull] {
            let program = compiled_cast(
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(DataType::Int8, true),
                Source::Column,
                wrap,
                mode,
                false,
            );
            let batch = float_batch(&program, &[Some(f64::NAN), Some(7.9), None]);
            let root = cast_root_use(&program);
            let unit = root_unit(&program);
            let occurrence = unit.control_flow().uses()[&root].arguments[0];
            assert_eq!(
                unit.recipes().cast(occurrence).unwrap().cast_mode(),
                Some(mode)
            );
            let mut evaluator = instance(&program);
            let summary = evaluator.effects[&occurrence];
            assert_eq!(
                summary
                    .for_use(summary.context())
                    .unwrap()
                    .may_raise_row_error,
                mode == TrinoCastMode::Explicit
            );
            // The COALESCE fallback is its own explicit BIGINT-to-TINYINT cast.
            let root_summary = evaluator.effects[&root];
            assert_eq!(
                root_summary
                    .for_use(root_summary.context())
                    .unwrap()
                    .may_raise_row_error,
                mode == TrinoCastMode::Explicit || matches!(wrap, Wrap::Coalesce)
            );
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                errors(&output),
                if mode == TrinoCastMode::Explicit {
                    vec![0]
                } else {
                    vec![]
                }
            );
            if matches!(wrap, Wrap::Coalesce) {
                assert_eq!(
                    signed_values(&output),
                    if mode == TrinoCastMode::Explicit {
                        vec![None, Some(8), Some(71)]
                    } else {
                        vec![Some(71), Some(8), Some(71)]
                    }
                );
            } else {
                let values = output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                assert_eq!(
                    values,
                    if mode == TrinoCastMode::Explicit {
                        vec![None, Some(false), Some(true)]
                    } else {
                        vec![Some(true), Some(false), Some(true)]
                    }
                );
            }
        }
    }
}
