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
use crate::exec::pipeline::builder::{unpivot_program, unpivot_rows};
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::runtime::task::ExecutionFailureCause;
use arrow::array::Int64Array;

fn runtime_factory(
    tracker: &Arc<MemTracker>,
    error: &Arc<RuntimeErrorState>,
) -> Arc<dyn InputConstructionFactory> {
    try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        error.clone(),
    )
    .unwrap()
}

/// The fixture is authored against the real compiled input declaration. All
/// newly made BE Vec/primitive wrappers use the same real creating host.
fn input(
    program: &LocalProgram,
    factory: &dyn InputConstructionFactory,
    error: Arc<RuntimeErrorState>,
) -> Chunk {
    let schema = factory
        .create_compiled_chunk_schema(program.graph().nodes()[0].output_layout())
        .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    let control = RuntimeKernelControl::new(error);
    let mut columns = unpivot_vec::<ArrayRef>(3, owner.as_ref()).unwrap();
    for values in [[1, 2, 3], [10, 20, 30], [11, 21, 31]] {
        let mut owned = unpivot_vec::<i64>(3, owner.as_ref()).unwrap();
        owned.extend_from_slice(&values);
        primitive_adoption(owner.as_ref(), &control).unwrap();
        request_arc::<Int64Array>(owner.as_ref()).unwrap();
        columns.push(Arc::new(Int64Array::from(owned)) as ArrayRef);
    }
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        3,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}

fn prepared(
    program: Arc<LocalProgram>,
    factory: Arc<dyn InputConstructionFactory>,
    error: Arc<RuntimeErrorState>,
    tracker: Arc<MemTracker>,
    state: &RuntimeState,
) -> Box<dyn Operator> {
    let declaration =
        CompiledUnpivotProcessorFactory::try_new(program, ProgramNodeId::new(1), error).unwrap();
    let mut operator = declaration.create(1, 0);
    operator.set_mem_tracker(tracker);
    operator.set_construction_factory(factory);
    operator.bind_runtime_state(state).unwrap();
    operator.prepare().unwrap();
    operator
}

#[test]
fn compiled_unpivot_output_and_slot_retain_actual_creator_after_operator_exit() {
    let program = unpivot_program();
    let program_weak = Arc::downgrade(&program);
    let tracker = MemTracker::new_root("compiled Unpivot last output owner");
    let state = RuntimeState::for_local_execution(None, None, None, None, None, None, None);
    let error = state.error_state();
    let error_weak = Arc::downgrade(&error);
    let factory = runtime_factory(&tracker, &error);
    let source = input(&program, factory.as_ref(), error.clone());
    let mut operator = prepared(
        program.clone(),
        factory.clone(),
        error.clone(),
        tracker.clone(),
        &state,
    );
    crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state)
        .accept_without_output(source)
        .unwrap();
    let output = crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state)
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 2);
    let rows = unpivot_rows(std::slice::from_ref(&output));
    assert_eq!(
        rows[0],
        (
            1,
            Some(10),
            "first".to_owned(),
            vec![7, 8],
            vec![("a".to_owned(), "x".to_owned())]
        )
    );
    assert_eq!(rows[1].1, Some(20));
    let projection = output.batch_projection();
    let slot = output.chunk_schema().slots()[0].clone();
    drop(output);
    drop(operator);
    drop(factory);
    drop(program);
    drop(error);
    drop(state);
    assert!(
        program_weak.upgrade().is_none(),
        "output does not retain the LocalProgram"
    );
    assert!(
        error_weak.upgrade().is_none(),
        "output does not retain runtime cancellation control"
    );
    assert!(
        tracker.current() > 0,
        "the actual output wrapper responsibility remains"
    );
    drop(projection);
    assert!(
        tracker.current() > 0,
        "the independent last schema slot retains its actual creator"
    );
    drop(slot);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_unpivot_candidate_refusal_preserves_typed_limit_and_original_input() {
    let program = unpivot_program();
    let tracker = MemTracker::new_root("compiled Unpivot candidate refusal");
    let state = RuntimeState::for_local_execution(None, None, None, None, None, None, None);
    let error = state.error_state();
    let factory = runtime_factory(&tracker, &error);
    let source = input(&program, factory.as_ref(), error.clone());
    let source_projection = source.batch_projection();
    let mut operator = prepared(
        program.clone(),
        factory.clone(),
        error.clone(),
        tracker.clone(),
        &state,
    );
    crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state)
        .accept_without_output(source)
        .unwrap();
    // Finish accepted-input preparation and install the original expansion
    // cursor before refusing the candidate's first output construction.
    let installed = operator.drive_for_test(&state).unwrap();
    assert!(installed.output.is_none());
    assert!(matches!(
        installed.next,
        crate::exec::pipeline::step::NextStep::Runnable
    ));
    let before = tracker.current();
    tracker.install_limit_once(before + 1).unwrap();
    // Observe rollback before the caller's distinct failure cleanup. The
    // high-level TestDriver performs that cleanup as soon as drive fails.
    let failure = operator.drive_for_test(&state).unwrap_err();
    assert!(
        matches!(
            failure.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ),
        "{failure:?}"
    );
    assert_eq!(
        tracker.current(),
        before,
        "refused new construction rolls back without resetting old holds"
    );
    assert!(
        operator
            .drive_for_test(&state)
            .unwrap_err()
            .same_occurrence(&failure),
        "a failed candidate cannot resume or replay its retained original input"
    );
    operator.on_driver_failure();
    let after_cleanup = tracker.current();
    operator.on_driver_failure();
    assert_eq!(tracker.current(), after_cleanup, "cleanup is idempotent");
    let ids = source_projection
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values().as_ref(), &[1, 2, 3]);
    drop(operator);
    drop(factory);
    drop(error);
    drop(state);
    drop(program);
    assert!(
        tracker.current() > 0,
        "the caller's original projection remains paid"
    );
    drop(source_projection);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_unpivot_cancel_preserves_original_caller_cause_before_candidate_creation() {
    let program = unpivot_program();
    let tracker = MemTracker::new_root("compiled Unpivot original cancellation cause");
    let state = RuntimeState::for_local_execution(None, None, None, None, None, None, None);
    let error = state.error_state();
    let factory = runtime_factory(&tracker, &error);
    let source = input(&program, factory.as_ref(), error.clone());
    let mut operator = prepared(
        program.clone(),
        factory.clone(),
        error.clone(),
        tracker.clone(),
        &state,
    );
    use crate::exec::pipeline::{
        driver_step::{DriverStep, TurnWork},
        input::RawInput,
        step::InputResult,
    };
    let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut context = DriverStep::new(&error, &mut work, &mut value_conversions);
    let step = operator.as_mut();
    assert!(matches!(
        step.try_push(&mut context, RawInput::for_test(source)),
        InputResult::Accepted(Ok(_))
    ));
    assert!(step.drive(&mut context).unwrap().output.is_none());
    let before = tracker.current();
    let allocated_before = tracker.allocated();
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    error.set_error(original.clone());
    let observed = match step.drive(&mut context) {
        Err(failure) => failure,
        Ok(_) => panic!("stopped work must not create another Unpivot candidate"),
    };
    assert!(observed.same_occurrence(&original));
    assert_eq!(observed.site(), original.site());
    assert_eq!(error.error(), Some(original));
    assert_eq!(tracker.current(), before);
    assert_eq!(tracker.allocated(), allocated_before);
    drop(operator);
    drop(factory);
    drop(error);
    drop(state);
    drop(program);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn actual_literal_materializers_refuse_wide_offsets_before_any_expansion() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    let copies = i32::MAX as usize + 1;
    let item = Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, false));
    let error = int32_list(&item, &[7], copies).unwrap_err();
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Representation);
    let rep = fact.representation().unwrap();
    assert_eq!(rep.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(rep.required, Some(copies as u64));
    assert_eq!(rep.extent_limit, Some(i32::MAX as u64));
    assert_eq!(rep.layout.code.get(), 11);
    let entries_field = Arc::new(arrow::datatypes::Field::new(
        "entries",
        DataType::Struct(
            vec![
                arrow::datatypes::Field::new("key", DataType::Utf8, false),
                arrow::datatypes::Field::new("value", DataType::Utf8, false),
            ]
            .into(),
        ),
        false,
    ));
    let error = utf8_map(&entries_field, false, &[("k", "v")], copies).unwrap_err();
    assert_eq!(
        error
            .failure_fact()
            .representation()
            .unwrap()
            .layout
            .code
            .get(),
        12
    );
    // The row count itself fits; repeated string bytes do not. No huge Vec is made.
    let key = "k".repeat(1024);
    let copies = i32::MAX as usize / key.len() + 1;
    let error = utf8_map(&entries_field, false, &[(key.as_str(), "v")], copies).unwrap_err();
    let rep = error.failure_fact().representation().unwrap();
    assert_eq!(rep.layout.code.get(), 13);
    assert_eq!(rep.required, Some((copies * key.len()) as u64));
    assert_eq!(rep.boundary, RepresentationBoundary::SingleOutput);
    let error = int32_list(&item, &[], usize::MAX).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    // Legal small materializations still use the original Arrow body and values.
    let actual = int32_list(&item, &[7, 9], 2).unwrap();
    let actual = actual.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(actual.value_offsets(), &[0, 2, 4]);
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, 9, 7, 9]
    );
}
