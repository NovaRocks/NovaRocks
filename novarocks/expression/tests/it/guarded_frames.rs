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

use arrow::array::{Array, BooleanArray, Decimal128Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, PassThroughCallHost, callback_causes,
        frame_fixtures::{
            Control, FrameFixture, POLICY,
            guarded::{SEED_1_FIRST, SEED_42, Shape, batch, decimal_batch, program},
        },
    },
};
use novarocks_functions::{
    ConstantValue, FunctionResultType, KernelFailure, ScalarInvocationActivation,
    ScalarInvocationScope, Selection,
};
use novarocks_type_contract::CompilePhase;
use std::sync::Arc;

fn instance(program: &FrameFixture) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control).unwrap()
}

#[test]
fn nested_if_coalesce_maps_sparse_original_rows_and_constant_versus_column_rng() {
    let program = program(Shape::Random);
    let input = batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
        ],
        vec![Some(42), Some(1), None, Some(99), Some(42), None],
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert!(output.errors().is_empty());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        vec![SEED_1_FIRST, SEED_42[0], SEED_42[1]]
    );
    let next = batch(
        &program,
        vec![Some(true), Some(false), None],
        vec![Some(1), None, None],
    );
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &next,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        vec![SEED_1_FIRST, SEED_42[2], SEED_42[3]]
    );
}

#[test]
fn guarded_round_skips_inactive_overflow_and_remaps_required_error_without_null_fallback() {
    let program = program(Shape::Decimal);
    let max38 = 10_i128.pow(38) - 1;
    let input = decimal_batch(
        &program,
        vec![
            Some(true),
            None,
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
        ],
        vec![
            Some(max38),
            Some(max38),
            Some(max38),
            None,
            Some(25),
            Some(max38),
            Some(max38),
        ],
        vec![Some(71); 7],
    );
    let rows = [1, 3, 5, 6];
    let selection = Selection::try_sparse(7, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(values.value(0), 71);
    assert_eq!(values.value(1), 71);
    assert!(values.is_null(2));
    assert_eq!(values.value(3), 71);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 2);
    assert!(output.errors()[0].message().contains("overflow"));
    let clean = decimal_batch(&program, vec![Some(true)], vec![Some(25)], vec![Some(71)]);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &clean,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        30
    );
}

#[test]
fn every_guarded_constructor_callback_preserves_all_original_failure_categories() {
    let program = program(Shape::Random);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ =
        CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &recorder).unwrap();
    let trace = recorder.trace();
    assert!(!trace.is_empty());
    for index in 1..=trace.len() {
        for cause in callback_causes([
            "guarded-origin-invalid",
            "guarded-origin-internal",
            "guarded-origin-operational",
        ]) {
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &control), Err(actual) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
        }
    }
}

#[test]
fn every_guarded_evaluation_callback_preserves_primary_failure_and_never_replays_state() {
    let program = program(Shape::Random);
    let input = batch(
        &program,
        (0..320)
            .map(|row| {
                if row % 3 == 0 {
                    None
                } else {
                    Some(row % 2 == 0)
                }
            })
            .collect(),
        vec![Some(42); 320],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in callback_causes([
            "guarded-origin-invalid",
            "guarded-origin-internal",
            "guarded-origin-operational",
        ]) {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &after,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace().is_empty());
        }
    }
}

#[test]
fn constant_entry_reuses_actual_prepared_if_coalesce_and_declines_rand() {
    use novarocks_expression::constant_node::{
        ConstantNode, ConstantNodeOutcome, DeclineReason, evaluate_constant_node,
    };
    let program = program(Shape::Random);
    let calls = program.unit.calls();
    for (identity, cases) in [
        (
            "builtin.scalar/if/v1",
            vec![
                (Some(true), Some(17.0), Some(42.0), 17.0),
                (Some(false), Some(17.0), Some(42.0), 42.0),
                (None, Some(17.0), Some(42.0), 42.0),
            ],
        ),
        (
            "builtin.scalar/coalesce/v1",
            vec![
                (None, None, Some(42.0), 42.0),
                (None, Some(17.0), Some(42.0), 17.0),
            ],
        ),
    ] {
        let prepared = calls
            .values()
            .find(|call| call.call_contract().function_id().as_str() == identity)
            .unwrap()
            .prepared();
        let selected = prepared.call_contract().selected();
        let FunctionResultType::Scalar(result) = &selected.result_type else {
            panic!("scalar control");
        };
        for (flag, left, right, expected) in cases {
            let mut children = vec![];
            for (ordinal, ty) in selected.argument_types.iter().enumerate() {
                let novarocks_functions::FunctionArgumentType::Value(ty) = ty else {
                    panic!("value control");
                };
                let array: arrow::array::ArrayRef = if ty.data_type() == &DataType::Boolean {
                    Arc::new(BooleanArray::from(vec![flag]))
                } else {
                    let first = if identity == "builtin.scalar/if/v1" {
                        ordinal == 1
                    } else {
                        ordinal == 0
                    };
                    Arc::new(Float64Array::from(vec![if first { left } else { right }]))
                };
                children.push(
                    ConstantValue::from_scalar_array(
                        Arc::new(ty.try_to_field("source").unwrap()),
                        ty.clone(),
                        array.to_data(),
                        POLICY,
                        CompilePhase::FunctionSpecialization,
                        &Control,
                    )
                    .unwrap(),
                );
            }
            let output = evaluate_constant_node(
                ConstantNode::Call(prepared),
                &children,
                result,
                &Control,
                CompilePhase::FunctionSpecialization,
            )
            .unwrap();
            let ConstantNodeOutcome::Value(output) = output else {
                panic!("control must produce its selected value");
            };
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }
    let rand = calls
        .values()
        .find(|call| call.call_contract().function_id().as_str() == "builtin.scalar/rand/v1")
        .unwrap()
        .prepared();
    let FunctionResultType::Scalar(result) = &rand.call_contract().selected().result_type else {
        panic!("scalar rand");
    };
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Call(rand),
            &[],
            result,
            &Control,
            CompilePhase::FunctionSpecialization
        )
        .unwrap(),
        ConstantNodeOutcome::Declined(DeclineReason::EffectfulCall)
    ));
}

#[test]
fn constant_entry_strict_null_skips_creation_and_opaque_requirement_declines_before_creation() {
    use novarocks_expression::constant_node::{
        ConstantNode, ConstantNodeOutcome, DeclineReason, evaluate_constant_node,
    };
    use novarocks_functions::{
        PreparedPureKernel, PreparedScalarKernel, ScalarCallContract, ScalarKernelInstance,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct Counted {
        actual: Arc<dyn PreparedScalarKernel>,
        creates: Arc<AtomicUsize>,
        opaque: bool,
    }
    impl PreparedScalarKernel for Counted {
        fn contract(&self) -> &Arc<ScalarCallContract> {
            self.actual.contract()
        }
        fn instance_retained_upper_bound(&self) -> usize {
            self.actual.instance_retained_upper_bound()
        }
        fn instance_inline_allocation_bytes(&self) -> usize {
            if self.opaque { 1 } else { 0 }
        }
        fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            self.actual.create_instance()
        }
    }
    let program = program(Shape::Decimal);
    let actual = program
        .unit
        .calls()
        .values()
        .find(|call| call.call_contract().function_id().as_str() == "builtin.scalar/round/v1")
        .unwrap()
        .prepared();
    let PreparedPureKernel::Scalar(actual) = actual else {
        panic!("actual scalar ROUND owner")
    };
    assert_eq!(actual.instance_inline_allocation_bytes(), 0);
    assert_eq!(
        actual.contract().effects().null_behavior,
        novarocks_type_contract::FunctionNullBehavior::Strict
    );
    let FunctionResultType::Scalar(result) = &actual.contract().selected().result_type else {
        panic!("scalar result")
    };
    let make_children = |blocked| {
        actual
            .contract()
            .selected()
            .argument_types
            .iter()
            .map(|ty| {
                let novarocks_functions::FunctionArgumentType::Value(ty) = ty else {
                    panic!("value argument")
                };
                let array: arrow::array::ArrayRef = match ty.data_type() {
                    DataType::Decimal128(p, s) => Arc::new(
                        Decimal128Array::from(vec![if blocked { None } else { Some(123) }])
                            .with_precision_and_scale(*p, *s)
                            .unwrap(),
                    ),
                    DataType::Int64 => Arc::new(Int64Array::from(vec![Some(-1)])),
                    other => panic!("unexpected actual ROUND signature: {other:?}"),
                };
                ConstantValue::from_scalar_array(
                    Arc::new(ty.try_to_field("source").unwrap()),
                    ty.clone(),
                    array.to_data(),
                    POLICY,
                    CompilePhase::FunctionSpecialization,
                    &Control,
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let creates = Arc::new(AtomicUsize::new(0));
    let call = PreparedPureKernel::Scalar(Arc::new(Counted {
        actual: actual.clone(),
        creates: creates.clone(),
        opaque: false,
    }));
    let outcome = evaluate_constant_node(
        ConstantNode::Call(&call),
        &make_children(true),
        result,
        &Control,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    let ConstantNodeOutcome::Value(array) = outcome else {
        panic!("strict NULL is a successful SQL NULL")
    };
    assert!(array.is_null(0));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Call(&call),
            &make_children(false),
            result,
            &Control,
            CompilePhase::FunctionSpecialization
        )
        .unwrap(),
        ConstantNodeOutcome::Value(_)
    ));
    assert_eq!(
        creates.load(Ordering::SeqCst),
        1,
        "the sentinel observes actual admitted creation"
    );
    creates.store(0, Ordering::SeqCst);
    let opaque = PreparedPureKernel::Scalar(Arc::new(Counted {
        actual: actual.clone(),
        creates: creates.clone(),
        opaque: true,
    }));
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Call(&opaque),
            &make_children(false),
            result,
            &Control,
            CompilePhase::FunctionSpecialization
        )
        .unwrap(),
        ConstantNodeOutcome::Declined(DeclineReason::OpaqueHostRequired)
    ));
    assert_eq!(creates.load(Ordering::SeqCst), 0);
}
