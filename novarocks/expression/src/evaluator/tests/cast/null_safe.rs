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
use arrow::array::{
    Date32Array, Float32Array, NullArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::{ControlShape, EvaluationDemand};

fn nullsafe_batch(program: &CompiledFixture, left: ArrayRef, right: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(program.input_schema().clone(), vec![left, right]).unwrap()
}
fn boolean_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn flat_arrays() -> Vec<(ArrayRef, ArrayRef)> {
    let mut arrays: Vec<(ArrayRef, ArrayRef)> = vec![];
    macro_rules! primitive {
        ($array:ty, $one:expr, $two:expr, $three:expr) => {
            arrays.push((
                Arc::new(<$array>::from(vec![
                    Some($one),
                    Some($two),
                    None,
                    None,
                    Some($one),
                ])),
                Arc::new(<$array>::from(vec![
                    Some($one),
                    Some($three),
                    Some($three),
                    None,
                    Some($one),
                ])),
            ));
        };
    }
    primitive!(BooleanArray, true, true, false);
    primitive!(Int8Array, 1, 2, 3);
    primitive!(Int16Array, 1, 2, 3);
    primitive!(Int32Array, 1, 2, 3);
    primitive!(Int64Array, 1, 2, 3);
    primitive!(Float32Array, 1.0, 2.0, 3.0);
    primitive!(Float64Array, 1.0, 2.0, 3.0);
    arrays.push((
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            None,
            Some("a"),
        ])),
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("c"),
            Some("c"),
            None,
            Some("a"),
        ])),
    ));
    primitive!(Date32Array, 1, 2, 3);
    primitive!(TimestampSecondArray, 1, 2, 3);
    primitive!(TimestampMillisecondArray, 1, 2, 3);
    primitive!(TimestampMicrosecondArray, 1, 2, 3);
    primitive!(TimestampNanosecondArray, 1, 2, 3);
    arrays.push((
        Arc::new(
            Decimal128Array::from(vec![Some(1), Some(2), None, None, Some(1)])
                .with_precision_and_scale(18, -2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(1), Some(3), Some(3), None, Some(1)])
                .with_precision_and_scale(18, -2)
                .unwrap(),
        ),
    ));
    arrays.push((Arc::new(NullArray::new(5)), Arc::new(NullArray::new(5))));
    arrays
}

#[test]
fn all_fifteen_frozen_nullsafe_profiles_keep_nonnullable_boolean_sparse_rows_and_empty_selection() {
    let arrays = flat_arrays();
    assert_eq!(arrays.len(), 15);
    for (left, right) in arrays {
        let ty = FunctionValueType::new(left.data_type().clone(), true);
        let program = nullsafe_program(ty.clone(), NullSafeRight::Column);
        let unit = &program.unit;
        let root_use = unit.root(program.root).unwrap().use_id;
        let flow = unit.control_flow();
        let invocation = &flow.uses()[&root_use];
        assert_eq!(invocation.control, ControlShape::OrderedNull);
        assert_eq!(invocation.context.demand, EvaluationDemand::Value);
        assert_eq!(invocation.arguments.len(), 2);
        let left_use = &flow.uses()[&invocation.arguments[0]];
        let right_use = &flow.uses()[&invocation.arguments[1]];
        assert_eq!(left_use.context.region, invocation.context.region);
        assert_eq!(
            flow.regions()[&right_use.context.region].parent,
            Some(invocation.context.region)
        );
        assert_eq!(left_use.context.demand, EvaluationDemand::Value);
        assert_eq!(right_use.context.demand, EvaluationDemand::Value);
        assert!(matches!(
            unit.definitions()
                .node(invocation.definition)
                .unwrap()
                .kind(),
            StaticExprKind::BoundCall { .. }
        ));
        let batch = nullsafe_batch(&program, left, right);
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let mut evaluator = instance(&program);
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
            let expected = if *ty.data_type() == DataType::Null {
                vec![Some(true); 4]
            } else {
                vec![Some(false), Some(false), Some(true), Some(true)]
            };
            assert_eq!(boolean_values(&output), expected);
            assert!(output.errors().is_empty());
            assert_eq!(output.values().null_count(), 0);
        }
        let instance_count = evaluator.instances.len();
        let empty = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(5, &[]).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(empty.values().is_empty());
        assert_eq!(evaluator.instances.len(), instance_count);
    }
}

#[test]
fn null_left_keeps_required_right_random_state_and_round_row_error() {
    let random = nullsafe_program(
        FunctionValueType::new(DataType::Float64, true),
        NullSafeRight::Random,
    );
    let calls = root_unit(&random).calls();
    assert!(calls.values().any(|call| {
        call.effects()
            .for_use(call.effects().context())
            .unwrap()
            .value_stability
            == novarocks_type_contract::FunctionVolatility::Volatile
    }));
    let batch = nullsafe_batch(
        &random,
        Arc::new(Float64Array::from(vec![None; 4])),
        Arc::new(Float64Array::from(vec![None; 4])),
    );
    let mut evaluator = instance(&random);
    let empty = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(4, &[]).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(empty.values().is_empty());
    assert_eq!(evaluator.instances.len(), 0);
    let rows = [1, 3];
    let selected = Selection::try_sparse(4, &rows).unwrap();
    for _ in 0..2 {
        assert_eq!(
            boolean_values(
                &evaluator
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(selected, selected),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost
                    )
                    .unwrap()
            ),
            vec![Some(false); 2]
        );
    }
    assert_eq!(evaluator.instances.len(), 2);
    let decimal = nullsafe_program(
        FunctionValueType::new(DataType::Decimal128(38, 0), true),
        NullSafeRight::Round,
    );
    let raw = 10_i128.pow(38) - 1;
    let array = |values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ) as ArrayRef
    };
    let batch = nullsafe_batch(
        &decimal,
        array(vec![None; 4]),
        array(vec![Some(raw), Some(10), None, Some(raw)]),
    );
    let output = instance(&decimal)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selected, selected),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(errors(&output), vec![1]);
    assert_eq!(boolean_values(&output), vec![Some(false), None]);
    // Error placeholders are terminal evidence, not successful NULL operands.
    assert!(!output.errors()[0].message().is_empty());
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
