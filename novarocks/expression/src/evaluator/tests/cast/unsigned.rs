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

use super::*;
use crate::EvaluationInput;
use crate::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

fn input_array(
    program: &CompiledFixture,
    array: ArrayRef,
    flags: Option<Vec<Option<bool>>>,
) -> RecordBatch {
    let n = array.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            array,
            Arc::new(Int64Array::from(vec![Some(42); n])),
            Arc::new(BooleanArray::from(
                flags.unwrap_or_else(|| vec![Some(true); n]),
            )),
        ],
    )
    .unwrap()
}
fn context(program: &CompiledFixture) -> (ExpressionUseId, ExpressionEffectContext) {
    let u = cast_root_use(program);
    (u, root_unit(program).control_flow().uses()[&u].context)
}

#[test]
fn cast_keeps_required_add_errors_terminal_under_guards() {
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        for wrap in [Wrap::Bare, Wrap::Coalesce, Wrap::IsNull, Wrap::If] {
            let program = compiled_cast(
                FunctionValueType::new(DataType::Int64, true),
                FunctionValueType::new(DataType::Int8, true),
                Source::Add,
                wrap,
                mode,
                false,
            );
            let batch = input_array(
                &program,
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(i64::MAX),
                    None,
                    Some(-1),
                    Some(i64::MAX),
                ])),
                Some(vec![None, Some(true), Some(true), Some(true), Some(false)]),
            );
            let rows = [1, 2, 3, 4];
            let mut evaluator = instance(&program);
            let (u, c) = context(&program);
            assert!(
                evaluator.effects[&u]
                    .for_use(c)
                    .unwrap()
                    .may_raise_row_error
            );
            let result = evaluator
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
                errors(&result),
                if matches!(wrap, Wrap::If) {
                    vec![0]
                } else {
                    vec![0, 3]
                }
            );
            if matches!(wrap, Wrap::IsNull) {
                let values = result
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    [None, Some(true), Some(false), None]
                );
            } else {
                assert_eq!(
                    signed_values(&result),
                    match wrap {
                        Wrap::Bare => [None, None, Some(0), None],
                        Wrap::Coalesce => [None, Some(71), Some(0), None],
                        Wrap::If => [None, None, Some(0), Some(71)],
                        _ => unreachable!(),
                    }
                );
            }
        }
    }
}

#[test]
fn cast_keeps_round_errors_terminal_and_rounds_negative_halves_in_successful_rows() {
    for allow in [false, true] {
        let program = inherited_round_fixture_with_target(allow, DataType::Int8);
        let max = 10_i128.pow(38) - 1;
        let batch = RecordBatch::try_new(
            program.input_schema().clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(7.0),
                    Some(1.0),
                    None,
                    Some(-0.5),
                    Some(1.0),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(25), Some(max), None, Some(25), Some(max)])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
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
        assert_eq!(errors(&result), vec![0, 3]);
        assert_eq!(signed_values(&result), [None, None, Some(-1), None]);
        assert!(
            result
                .errors()
                .iter()
                .all(|e| e.message().contains("overflow"))
        );
    }
}
