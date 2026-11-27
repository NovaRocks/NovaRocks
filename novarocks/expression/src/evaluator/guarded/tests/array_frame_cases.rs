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

use crate::{
    EvaluationInput, ExprEvaluationFailure,
    evaluator::{
        CompiledExpressionInstance,
        scalar_invocation::{LeafCallObservation, observe_calls},
    },
    testkit::{
        CallbackControl, PassThroughCallHost,
        frame_fixtures::{
            Control,
            array::{
                Guard, SourceMode, input, invocation_program, original_source_program,
                tracked_instance,
            },
            guarded::{self, Shape},
        },
    },
};
use arrow::{
    array::{Array, BooleanArray, Int64Array},
    record_batch::RecordBatch,
};
use novarocks_functions::{
    KernelDiagnostic, KernelFailure, ScalarInvocationActivation as Activation,
    ScalarInvocationScope, Selection,
};
use std::{convert::Infallible, sync::Arc};

fn data(
    error: ExprEvaluationFailure<Infallible>,
    message: &str,
) -> novarocks_functions::ScalarInvocationData {
    let ExprEvaluationFailure::Data(data) = error else {
        panic!("expected original atomic Data, got {error:?}")
    };
    assert_eq!(data.message(), message);
    data
}
#[test]
fn scalar_invocation_noarguments_keeps_full_definition_n_but_invokes_no_children() {
    for count in [3, 5] {
        let program = invocation_program(count, Guard::Direct);
        let batch = input(&program, vec![Some(true); 4], false);
        let (mut instance, _tracker) = tracked_instance(&program);
        let actual = data(
            instance
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                    Activation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap_err(),
            &format!("array_struct_subfield expects 2 to 2 arguments, got {count}"),
        );
        assert_eq!(actual.contract().call().logical_argument_count(), count);
        assert_eq!(actual.contract().selected().argument_types.len(), count);
        assert!(actual.contract().value_argument_types().len() == 0);
        assert_eq!(actual.source_len(), 4);
        assert_eq!(
            actual.contract().effects().argument_control,
            novarocks_type_contract::ArgumentControl::NoArguments
        );
        assert_eq!(
            instance.instances.len(),
            1,
            "no tail kernel may be instantiated"
        );
    }
}

#[test]
fn scalar_invocation_actual_empty_if_else_is_activated_and_validation_root_is_not() {
    let program = invocation_program(2, Guard::IfElse);
    let batch = input(&program, vec![], false);
    let (mut instance, _) = tracked_instance(&program);
    data(
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        "__array_struct_subfield field-name argument is empty",
    );
    let (mut structural, _) = tracked_instance(&program);
    let out = structural
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
            Activation::ValidateOnly,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(out.values().len(), 0);
    assert!(out.errors().is_empty());
    assert!(structural.instances.is_empty());
}

#[test]
fn scalar_invocation_guarded_data_is_atomic_and_inactive_branch_never_instantiates() {
    let program = invocation_program(3, Guard::IfThen);
    let inactive = input(&program, vec![Some(false), None, Some(false)], false);
    let (mut instance, _) = tracked_instance(&program);
    let out = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &inactive,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(out.errors().is_empty());
    assert!(instance.instances.is_empty());
    assert_eq!(out.values().null_count(), 3);
    let mixed = input(&program, vec![Some(false), Some(true), None], false);
    let (mut instance, _) = tracked_instance(&program);
    let actual = data(
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &mixed,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        "array_struct_subfield expects 2 to 2 arguments, got 3",
    );
    assert_eq!(actual.batch_rows(), 3);
    assert_eq!(actual.source_len(), 1);
    assert_eq!(actual.source_row(0), Some(1));
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert!(matches!(
        instance.evaluate(EvaluationInput::Batch { batch: &mixed, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), Activation::Activated, &after, &mut PassThroughCallHost),
        Err(error) if matches!(&error, ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    assert!(after.trace().is_empty());
}

#[test]
fn scalar_invocation_entry_validation_preserves_first_cause_without_footer_or_leaf() {
    let program = invocation_program(3, Guard::Direct);
    let batch = input(&program, vec![Some(true); 3], false);
    let (mut instance, _tracker) = tracked_instance(&program);
    assert!(instance.has_invocation_data);
    let control = CallbackControl::new(KernelFailure::Cancelled, 2);
    let error = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            Activation::Activated,
            &control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    let ExprEvaluationFailure::Kernel(cause) = &error else {
        panic!("input validation must preserve its originating Kernel cause: {error:?}")
    };
    assert_eq!(
        cause,
        &KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "selection differs from actual input batch rows",
        ))
    );
    assert_eq!(control.trace(), vec![0]);
    assert!(instance.instances.is_empty());
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert!(matches!(
        instance.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), Activation::Activated, &after, &mut PassThroughCallHost),
        Err(error) if matches!(&error, ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    assert!(after.trace().is_empty());
    assert!(instance.instances.is_empty());
}

#[test]
fn original_domain_required_child_row_outside_demand_stops_before_parent_instantiation() {
    let program = original_source_program("ID", Guard::Direct, SourceMode::RequiredError);
    let original = input(&program, vec![Some(true); 6], false);
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1, 0, 1, 1, 1, 1]));
    let batch = RecordBatch::try_new(original.schema(), columns).unwrap();
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let error = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    let ExprEvaluationFailure::RequiredRow(row) = &error else {
        panic!("required child cannot be parent Data or invariant: {error:?}")
    };
    assert_eq!(row.root(), program.root);
    assert_eq!(row.batch_row(), 1);
    assert_eq!(row.error().selected_ordinal(), 0);
    assert!(row.error().message().contains("zero"));
    assert!(
        !frame
            .instances
            .contains_key(&program.unit.root(program.root).unwrap().use_id),
        "required child failure must precede parent creation/core, not merely suppress its output"
    );
}

#[test]
fn original_domain_policy_does_not_expand_old_volatile_if_inside_coalesce() {
    let program = guarded::program(Shape::Random);
    let batch = guarded::batch(&program, vec![Some(true); 3], vec![Some(42); 3]);
    let mut frame =
        CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control).unwrap();
    let demand = [0, 2];
    let (output, calls) = observe_calls(|| {
        frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                novarocks_functions::ScalarInvocationScope::new(
                    Selection::all(3),
                    Selection::try_sparse(3, &demand).unwrap(),
                ),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(
        calls,
        vec![LeafCallObservation::Rows {
            explicit_actual_domain: false,
            actual: vec![0, 2],
            demand: vec![0, 2],
        }],
        "real old RAND must evaluate only the two demanded Coalesce/IF rows"
    );
}

#[test]
fn original_domain_policy_conditional_mask_uses_actual_parent_not_outer_demand() {
    let program = original_source_program("id", Guard::IfThen, SourceMode::Computed);
    let batch = input(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ],
        false,
    );
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let (output, calls) = observe_calls(|| {
        frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                novarocks_functions::ScalarInvocationScope::new(
                    Selection::try_sparse(6, &actual).unwrap(),
                    Selection::try_sparse(6, &demand).unwrap(),
                ),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert_eq!(
        calls
            .into_iter()
            .filter(|call| matches!(
                call,
                LeafCallObservation::Rows {
                    explicit_actual_domain: true,
                    ..
                }
            ))
            .collect::<Vec<_>>(),
        vec![LeafCallObservation::Rows {
            explicit_actual_domain: true,
            actual: vec![1, 5],
            demand: vec![1, 5],
        }],
        "typed original owner needs the actual IF true-mask, including global1 outside output demand"
    );
    assert!(output.errors().is_empty());
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), demand);
    // Global row 3 takes NULL; row 5 tests the non-null projected ARRAY.
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(false)]
    );
}
