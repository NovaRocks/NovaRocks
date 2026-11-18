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
use crate::testkit::numeric_fixtures::float_batch;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

fn edge_pair(source: &DataType, target: &DataType) -> ([f64; 4], [Option<i64>; 4]) {
    match (source, target) {
        (_, DataType::Int8) => (
            [-128.5, 127.9, -129.0, 128.0],
            [Some(-128), Some(127), None, None],
        ),
        (_, DataType::Int16) => (
            [-32768.5, 32767.5, -32769.0, 32768.0],
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
            [-2147483648.5, 2147483647.5, -2147483649.0, 2147483648.0],
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
fn float_signed_eight_profiles_preserve_arrow_edges_sparse_slices_null_and_both_frozen_policies() {
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
                Some(4),
            ];
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(source.clone(), true),
                        FunctionValueType::new(target.clone(), true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let occurrence = cast_root_use(&program);
                    let recipe = root_unit(&program).recipes().cast(occurrence).unwrap();
                    let context = root_unit(&program).control_flow().uses()[&occurrence].context;
                    assert_eq!(
                        recipe
                            .own_effects(context)
                            .for_use(context)
                            .unwrap()
                            .may_raise_row_error,
                        allow
                    );
                    let batch = float_batch(&program, &values);
                    let mut evaluator = instance(&program);
                    let empty = evaluator
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            {
                                let selection = Selection::try_sparse(values.len(), &[]).unwrap();
                                ScalarInvocationScope::new(selection, selection)
                            },
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
                        assert_eq!(signed_values(&output), expected.to_vec());
                        assert_eq!(
                            errors(&output),
                            if allow { vec![2, 3, 5, 6, 7] } else { vec![] }
                        );
                        for error in output.errors() {
                            // NULL input has no error. NaN and both infinities do:
                            // ALLOW is independent of DecimalOverflowPolicy.
                            assert!(
                                error
                                    .message()
                                    .starts_with("Expr evaluate meet error: CAST failed:")
                            );
                            assert!(error.message().contains("conflict with range of"));
                        }
                    }
                    assert!(evaluator.instances.is_empty());
                }
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
        assert_eq!(
            errors(&output),
            if allow { vec![0, 1, 3] } else { vec![0, 3] }
        );
        let last = output.errors().last().unwrap();
        assert_eq!(output.errors()[0].message(), last.message());
        assert!(
            !output.errors()[0]
                .message()
                .starts_with("Expr evaluate meet error: CAST failed:")
        );
        if allow {
            assert!(
                output.errors()[1]
                    .message()
                    .starts_with("Expr evaluate meet error: CAST failed:")
            );
        }
        assert_eq!(output.selection(), selection);
    }
}

#[test]
fn float_cast_errors_remain_terminal_under_coalesce_and_is_null_with_full_child_effects() {
    for allow in [false, true] {
        for wrap in [Wrap::Coalesce, Wrap::IsNull] {
            let program = compiled(
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(DataType::Int8, true),
                Source::Column,
                wrap,
                DecimalOverflowPolicy::OutputNull,
                allow,
            );
            let batch = float_batch(&program, &[Some(f64::NAN), Some(7.9), None]);
            let occurrence = cast_root_use(&program);
            let mut evaluator = instance(&program);
            let summary = evaluator.effects[&occurrence];
            assert_eq!(
                summary
                    .for_use(summary.context())
                    .unwrap()
                    .may_raise_row_error,
                allow
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
            assert_eq!(errors(&output), if allow { vec![0] } else { vec![] });
            if matches!(wrap, Wrap::Coalesce) {
                assert_eq!(
                    signed_values(&output),
                    if allow {
                        vec![None, Some(7), Some(71)]
                    } else {
                        vec![Some(71), Some(7), Some(71)]
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
                    if allow {
                        vec![None, Some(false), Some(true)]
                    } else {
                        vec![Some(true), Some(false), Some(true)]
                    }
                );
            }
        }
    }
}
