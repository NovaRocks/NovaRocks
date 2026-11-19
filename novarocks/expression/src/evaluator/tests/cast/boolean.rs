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
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

fn bool_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn root_context(program: &CompiledFixture) -> (ExpressionUseId, ExpressionEffectContext) {
    let occurrence = cast_root_use(program);
    (
        occurrence,
        root_unit(program).control_flow().uses()[&occurrence].context,
    )
}

#[test]
fn actual_compiled_bool_cast_required_children_remain_terminal_through_real_control_owners() {
    for wrap in [Wrap::Bare, Wrap::Coalesce, Wrap::IsNull, Wrap::If] {
        let program = compiled(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Boolean, true),
            Source::Add,
            wrap,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let batch = input(
            &program,
            &[Some(1), Some(i64::MAX), None, Some(-1), Some(i64::MAX)],
            &[Some(42); 5],
            &[None, Some(true), Some(true), Some(true), Some(false)],
        );
        let rows = [1, 2, 3, 4];
        let mut evaluator = instance(&program);
        let (occurrence, context) = root_context(&program);
        assert!(
            evaluator.effects[&occurrence]
                .for_use(context)
                .unwrap()
                .may_raise_row_error
        );
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
        match wrap {
            Wrap::Bare => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(bool_values(&output), vec![None, None, Some(false), None]);
            }
            Wrap::Coalesce => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(
                    bool_values(&output),
                    vec![None, Some(true), Some(false), None]
                );
            }
            Wrap::IsNull => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(
                    bool_values(&output),
                    vec![None, Some(true), Some(false), None]
                );
            }
            Wrap::If => {
                assert_eq!(errors(&output), vec![0]);
                assert_eq!(
                    bool_values(&output),
                    vec![None, None, Some(false), Some(true)]
                );
            }
            _ => unreachable!(),
        }
    }
    for allow in [false, true] {
        let program = inherited_round_fixture_with_target(allow, DataType::Boolean);
        let max38 = 10_i128.pow(38) - 1;
        let batch = RecordBatch::try_new(
            program.input_schema().clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(7.0),
                    Some(f64::NAN),
                    None,
                    Some(-0.0),
                    Some(f64::INFINITY),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(25), Some(max38), None, Some(25), Some(max38)])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let rows = [1, 2, 3, 4];
        let output = instance(&program)
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
        assert_eq!(errors(&output), vec![0, 3]);
        assert_eq!(bool_values(&output), vec![None, None, Some(false), None]);
        assert!(
            output
                .errors()
                .iter()
                .all(|error| error.message().contains("overflow"))
        );
    }
}

// The signed 128-bit representation contract defines these bytes independently
// of CAST. A checked package and the actual identity controller must preserve
// both extrema, NULLs, selection order, and the nominal type.
