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
use crate::testkit::PassThroughCallHost;
use arrow::array::{UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Expected {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    F32(u32),
    F64(u64),
}
fn actual(array: &ArrayRef) -> Vec<Option<Expected>> {
    macro_rules! values {
        ($a:ty,$f:expr) => {
            array
                .as_any()
                .downcast_ref::<$a>()
                .unwrap()
                .iter()
                .map(|v| v.map($f))
                .collect()
        };
    }
    match array.data_type() {
        DataType::Boolean => values!(BooleanArray, Expected::Bool),
        DataType::Int8 => values!(Int8Array, |v| Expected::Signed(i64::from(v))),
        DataType::Int16 => values!(Int16Array, |v| Expected::Signed(i64::from(v))),
        DataType::Int32 => values!(Int32Array, |v| Expected::Signed(i64::from(v))),
        DataType::Int64 => values!(Int64Array, Expected::Signed),
        DataType::UInt8 => values!(UInt8Array, |v| Expected::Unsigned(u64::from(v))),
        DataType::UInt16 => values!(UInt16Array, |v| Expected::Unsigned(u64::from(v))),
        DataType::UInt32 => values!(UInt32Array, |v| Expected::Unsigned(u64::from(v))),
        DataType::UInt64 => values!(UInt64Array, Expected::Unsigned),
        DataType::Float32 => values!(Float32Array, |v: f32| Expected::F32(v.to_bits())),
        DataType::Float64 => values!(Float64Array, |v: f64| Expected::F64(v.to_bits())),
        _ => panic!("closed scalar result"),
    }
}
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
fn actual_compiled_unsigned_cast_required_add_and_round_errors_remain_terminal_under_guards() {
    for wrap in [Wrap::Bare, Wrap::Coalesce, Wrap::IsNull, Wrap::If] {
        let program = compiled(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::UInt64, true),
            Source::Add,
            wrap,
            DecimalOverflowPolicy::OutputNull,
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
        assert_eq!(
            actual(result.values()),
            match wrap {
                Wrap::Bare => vec![None, None, Some(Expected::Unsigned(0)), None],
                Wrap::Coalesce => vec![
                    None,
                    Some(Expected::Unsigned(71)),
                    Some(Expected::Unsigned(0)),
                    None
                ],
                Wrap::IsNull => vec![
                    None,
                    Some(Expected::Bool(true)),
                    Some(Expected::Bool(false)),
                    None
                ],
                Wrap::If => vec![
                    None,
                    None,
                    Some(Expected::Unsigned(0)),
                    Some(Expected::Unsigned(71))
                ],
                _ => unreachable!(),
            }
        );
    }
    for allow in [false, true] {
        let program = inherited_round_fixture_with_target(allow, DataType::UInt64);
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
        assert_eq!(
            actual(result.values()),
            vec![None, None, Some(Expected::Unsigned(0)), None]
        );
        assert!(
            result
                .errors()
                .iter()
                .all(|e| e.message().contains("overflow"))
        );
    }
}
