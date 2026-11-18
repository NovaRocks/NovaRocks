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

//! Actual Chunk projection, source authority and operator materialization.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::fixtures::{SeedMode, input_chunk, input_project, program};
use crate::exec::operators::compiled_expression::{
    InputAdapterObserver, RuntimeKernelControl, assert_original_chunk_input,
    chunk_evaluation_input, projected_chunk_evaluation_input,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeErrorState;
use arrow::{
    array::{Array, BooleanArray, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure, InputColumnProjection,
    testkit::PassThroughCallHost,
};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    SelectedValues, Selection,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_type_contract::EvaluationDemand;
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

fn site(program: &LocalProgram) -> ProgramRootRef {
    let (_, exprs) = input_project(program);
    exprs[1]
}

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Slot must not wait")
    }
}

fn evaluator(program: &Arc<LocalProgram>) -> CompiledExpressionInstance {
    let tracker = MemTracker::new_root("input-source-custody-host");
    let host = crate::runtime::tracked_state_allocator::expression_allocation_host(tracker);
    let root = site(program);
    CompiledExpressionInstance::try_new_with_allocator(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &Control,
        Some(host),
    )
    .unwrap()
}

fn batch_input(batch: &RecordBatch) -> EvaluationInput<'_> {
    EvaluationInput::Batch {
        batch,
        source: None,
    }
}

fn chunk_input(chunk: &Chunk) -> EvaluationInput<'_> {
    chunk_evaluation_input(
        chunk,
        &RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default())),
    )
}

fn evaluate<'a>(
    instance: &mut CompiledExpressionInstance,
    input: EvaluationInput<'_>,
    selection: Selection<'a>,
) -> Result<SelectedValues<'a>, ExprEvaluationFailure<Infallible>> {
    instance.evaluate(
        input,
        ScalarInvocationScope::new(selection, selection),
        if selection.is_empty() {
            ScalarInvocationActivation::ValidateOnly
        } else {
            ScalarInvocationActivation::Activated
        },
        &Control,
        &mut PassThroughCallHost,
    )
}

#[test]
fn checked_slot_pairs_exact_column_and_original_chunk_authority() {
    let program = program(SeedMode::Input, false);
    let tracker = MemTracker::new_root("checked-slot-source");
    let mut chunk = input_chunk(&program, vec![Some(11), Some(42), None]);
    chunk.try_transfer_to(&tracker).unwrap();
    let owners = chunk.source_authority().owner_identity();
    let original_array = Arc::clone(chunk.record_batch().column(1));
    let observed = Arc::new(Mutex::new(0));
    let seen = Arc::clone(&observed);
    let observer: Arc<InputAdapterObserver> = Arc::new(move |chunk, input| {
        assert_original_chunk_input(chunk, input);
        assert!(Arc::ptr_eq(input.batch().column(1), &original_array));
        let source = chunk.source_authority();
        assert_eq!(source.owner_identity(), owners);
        assert_eq!(source.strong_counts(), (Some(1), None));
        *seen.lock().unwrap() += 1;
    });
    let mut adapter = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    adapter.observe_input_adapter(observer);
    let input = chunk_evaluation_input(&chunk, &adapter);
    let mut instance = evaluator(&program);
    let result = evaluate(&mut instance, input, Selection::all(3)).unwrap();
    assert_eq!(
        result.values().to_data(),
        chunk.record_batch().column(1).to_data()
    );
    assert_eq!(*observed.lock().unwrap(), 1);
    assert!(chunk.source_authority().strong_counts().0.unwrap() > 1);
    assert!(input.batch().columns().get(3).is_none());
}

#[test]
fn projected_input_maps_original_slots_through_actual_sparse_domains() {
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let program = program(SeedMode::Input, false);
    let full = input_chunk(&program, vec![Some(11), Some(42), None, Some(7)]);
    let declared = full.chunk_schema_ref();
    // Plain Arrow fixture metadata is outside construction qualification.
    // Production gather funds both the original map and the compact header.
    for ordinals in [vec![1], vec![0]] {
        let schema = declared
            .project_by_slots_typed(
                &ordinals
                    .iter()
                    .map(|&i| declared.slots()[i].slot_id())
                    .collect::<Vec<_>>(),
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap();
        let compact = Chunk::try_new_with_chunk_schema(
            full.record_batch().project(&ordinals).unwrap(),
            Arc::new(schema),
        )
        .unwrap();
        let projection = Arc::new(InputColumnProjection::from_owned(
            ordinals.clone(),
            ValueTypeStorageLifetime::pure(),
        ));
        let loan =
            projected_chunk_evaluation_input(&compact, &declared, &projection, &Control).unwrap();
        assert_eq!(loan.batch().num_columns(), 1);
        assert_eq!(loan.declared_schema().fields().len(), 3);
        for selected in [vec![0, 1, 2, 3], vec![1, 3], vec![]] {
            let selection = Selection::try_sparse(4, &selected).unwrap();
            let mut instance = evaluator(&program);
            let actual = evaluate(&mut instance, loan, selection);
            if ordinals == [0] {
                assert!(matches!(
                    actual.unwrap_err(),
                    ExprEvaluationFailure::Kernel(KernelFailure::InvalidProgram(_))
                ));
                continue;
            }
            let actual = actual.unwrap();
            let values = actual
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let expected = selected
                .iter()
                .map(|&row| [Some(11), Some(42), None, Some(7)][row])
                .collect::<Vec<_>>();
            assert_eq!(values.iter().collect::<Vec<_>>(), expected);
        }
    }
}

#[test]
fn projected_input_does_not_promote_equal_arrow_fields_to_a_logical_binding() {
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType, ValueTypeStorageLifetime};
    use novarocks_types::SlotId;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("json", DataType::Utf8, true),
    ]));
    let logical =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(
        Arc::clone(&schema),
        &[SlotId::new(7)],
        &[logical],
    );
    let declared = ChunkSchema::from_compiled_layout(&layout).unwrap();
    let actual = crate::exec::chunk::typed_fixture::chunk(
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec!["[]"]))],
        )
        .unwrap(),
        &[SlotId::new(7)],
        &[FunctionValueType::new(DataType::Utf8, true)],
    );
    let projection = Arc::new(InputColumnProjection::from_owned(
        vec![0],
        ValueTypeStorageLifetime::pure(),
    ));
    let actual = projected_chunk_evaluation_input(&actual, &declared, &projection, &Control);
    assert!(matches!(
        actual.err().expect("logical binding was refused"),
        KernelFailure::InvalidProgram(_)
    ));
}

#[test]
fn record_batch_without_chunk_and_chunk_without_authority_are_distinct() {
    let program = program(SeedMode::Input, false);
    let chunk = input_chunk(&program, vec![Some(42)]);
    assert!(!chunk.source_authority().is_attached());
    let bare = batch_input(chunk.record_batch());
    let adapted = chunk_input(&chunk);
    assert!(bare.source_storage().is_none());
    assert_original_chunk_input(&chunk, adapted);
    assert!(Arc::ptr_eq(
        bare.batch().column(1),
        adapted.batch().column(1),
    ));
    let results = [bare, adapted].map(|input| {
        evaluate(&mut evaluator(&program), input, Selection::all(1))
            .unwrap()
            .values()
            .to_data()
    });
    assert_eq!(results[0], results[1]);
}

#[test]
fn chunk_and_batch_entries_keep_full_sparse_empty_null_results() {
    let program = program(SeedMode::Input, false);
    let tracker = MemTracker::new_root("input-selection-source");
    let mut chunk = input_chunk(&program, vec![Some(11), Some(42), None, Some(7)]);
    chunk.try_transfer_to(&tracker).unwrap();
    let rows = [1, 2, 3];
    let empty = [];
    for selection in [
        Selection::all(4),
        Selection::try_sparse(4, &rows).unwrap(),
        Selection::try_sparse(4, &empty).unwrap(),
    ] {
        let mut old = evaluator(&program);
        let mut actual = evaluator(&program);
        let before = evaluate(&mut old, batch_input(chunk.record_batch()), selection).unwrap();
        let after = evaluate(&mut actual, chunk_input(&chunk), selection).unwrap();
        assert_eq!(before.selection(), after.selection());
        assert_eq!(before.values().to_data(), after.values().to_data());
        assert_eq!(before.errors(), after.errors());
        assert_eq!(after.values().data_type(), &DataType::Int64);
        if selection.is_empty() {
            assert_eq!(
                chunk.source_authority().strong_counts(),
                (Some(1), None),
                "ValidateOnly-empty must not capture a Slot source"
            );
        } else {
            assert!(chunk.source_authority().strong_counts().0.unwrap() > 1);
        }
    }
    // Returned values retain original finite custody. The input Chunk may
    // exit first, but its source charge survives the last real Buffer alias.
    let mut instance = evaluator(&program);
    let result = evaluate(&mut instance, chunk_input(&chunk), Selection::all(4)).unwrap();
    let bytes = tracker.current();
    let buffer = result.values().to_data().buffers()[0].clone();
    drop(chunk);
    assert_eq!(tracker.current(), bytes);
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(11), Some(42), None, Some(7)]
    );
    drop(result);
    assert_eq!(tracker.current(), bytes);
    drop(buffer);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn chunk_entry_keeps_original_validation_and_failed_instance_latch() {
    let program = program(SeedMode::Input, false);
    let chunk = input_chunk(&program, vec![Some(42)]);
    let bad = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("wrong", DataType::Int64, true),
        ])),
        vec![Arc::clone(chunk.record_batch().column(1))],
    )
    .unwrap();
    let bad_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        bad.schema().as_ref(),
        &[novarocks_types::SlotId(1)],
    )
    .unwrap();
    let bad = Chunk::try_new_with_chunk_schema(bad, bad_schema).unwrap();
    let mut old = evaluator(&program);
    let mut actual = evaluator(&program);
    let a = evaluate(&mut old, batch_input(bad.record_batch()), Selection::all(1)).unwrap_err();
    let b = evaluate(&mut actual, chunk_input(&bad), Selection::all(1)).unwrap_err();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    let a = evaluate(
        &mut old,
        batch_input(chunk.record_batch()),
        Selection::all(1),
    )
    .unwrap_err();
    let b = evaluate(&mut actual, chunk_input(&chunk), Selection::all(1)).unwrap_err();
    assert!(matches!(
        &a,
        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    ));
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

#[test]
fn project_and_writer_entries_observe_original_slot_before_complete_custody_capture() {
    let program = program(SeedMode::Input, false);
    let tracker = MemTracker::new_root("operator-source-borrow");
    for writer in [false, true] {
        let mut chunk = input_chunk(&program, vec![Some(42), None, Some(7)]);
        chunk.try_transfer_to(&tracker).unwrap();
        let bytes = tracker.current();
        let expected_owner = chunk.source_authority().owner_identity();
        let expected_array = Arc::as_ptr(chunk.record_batch().column(1)) as *const () as usize;
        let seen = Arc::new(Mutex::new(0));
        let observed = Arc::clone(&seen);
        // Project intermediates can already retain genuine holders. Check the
        // identity of the original source and the borrowed array at adaptation.
        let observer: Arc<InputAdapterObserver> = Arc::new(move |chunk, input| {
            assert_original_chunk_input(chunk, input);
            let array = input.batch().column(1);
            assert_eq!(Arc::as_ptr(array) as *const () as usize, expected_array);
            let source = chunk.source_authority();
            assert_eq!(source.owner_identity(), expected_owner);
            assert!(source.strong_counts().0.unwrap() >= 1);
            assert_eq!(source.strong_counts().1, None);
            *observed.lock().unwrap() += 1;
        });
        let output = if writer {
            let mut projector = crate::exec::operators::compiled_writer::source_loan_test_projector(
                Arc::clone(&program),
                Some(observer),
            );
            let output = projector.project(&chunk).unwrap();
            assert_eq!(tracker.current(), bytes);
            assert!(chunk.source_authority().strong_counts().0.unwrap() > 1);
            drop(chunk);
            output
        } else {
            crate::exec::operators::compiled_expression::project_chunk_with_source_observer(
                Arc::clone(&program),
                chunk,
                observer,
            )
            .unwrap()
        };
        assert!(*seen.lock().unwrap() > 0);
        assert_eq!(output.len(), 3);
        // The full Project emits RAND before its identity Slot; the writer
        // fixture projects only that Slot.
        let identity_column = if writer { 0 } else { 1 };
        assert_eq!(
            output.columns()[identity_column]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(42), None, Some(7)]
        );
        assert!(!output.source_authority().is_attached());
        assert_eq!(tracker.current(), bytes);
        // Copy/projection custody must survive both the operator and Chunk.
        // Keep an actual output alias, then only its real values Buffer: the
        // finite original source may exit only after this last alias exits.
        let alias = output.columns()[identity_column].clone();
        let last_buffer = alias.to_data().buffers()[0].clone();
        drop(output);
        assert_eq!(tracker.current(), bytes);
        drop(alias);
        assert_eq!(tracker.current(), bytes);
        std::thread::spawn(move || drop(last_buffer))
            .join()
            .unwrap();
        assert_eq!(tracker.current(), 0);
    }
    assert_eq!(tracker.allocated(), tracker.deallocated());
}

#[test]
fn input_source_provider_token_survives_actual_frame_and_last_buffer_alias() {
    let program = program(SeedMode::Input, false);
    let original = input_chunk(&program, vec![Some(11), None, Some(7)]);
    let tracker = MemTracker::new_root("frame-provider-finite-custody");
    let (chunk, bytes) =
        crate::exec::pipeline::async_writer::tests::provider_chunk_with_original_token(
            original.record_batch().clone(),
            &tracker,
        );
    let mut instance = evaluator(&program);
    let output = evaluate(&mut instance, chunk_input(&chunk), Selection::all(3)).unwrap();
    let alias = output.values().to_data().buffers()[0].clone();
    drop((chunk, original, output, instance));
    assert_eq!(tracker.current(), bytes as i64);
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
}

#[test]
fn input_source_no_authority_single_creator_lifetime_is_not_a_pure_source() {
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Creator(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Creator {}
    impl Drop for Creator {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let program = program(SeedMode::Input, false);
    for has_host in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let creator = ValueTypeStorageLifetime::held(Arc::new(Creator(drops.clone())));
        let chunk = input_chunk(&program, vec![Some(11), None, Some(7)])
            .retaining_array_type_storage_observed::<LayoutResourceError>(&creator, |_| Ok(()))
            .unwrap();
        drop(creator);
        assert!(!chunk.source_authority().is_attached());
        let mut instance = if has_host {
            evaluator(&program)
        } else {
            CompiledExpressionInstance::try_new(
                program.unit(site(&program).unit).unwrap().clone(),
                site(&program).root,
                &Control,
            )
            .unwrap()
        };
        let result = evaluate(&mut instance, chunk_input(&chunk), Selection::all(3));
        if has_host {
            let result = result.unwrap();
            let alias = result.values().to_data().buffers()[0].clone();
            drop((chunk, result, instance));
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(alias);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        } else {
            let error = result.unwrap_err();
            let ExprEvaluationFailure::Kernel(KernelFailure::InvalidProgram(message)) = &error
            else {
                panic!("missing real host must remain nominal: {error:?}")
            };
            assert_eq!(
                message.message(),
                "compiled input finite custody requires its actual allocation host"
            );
            drop((chunk, instance));
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn input_source_additional_buffer_ledger_survives_actual_frame_and_last_alias() {
    let program = program(SeedMode::Input, false);
    let tracker = MemTracker::new_root("frame-complete-additional-buffer-ledger");
    tracker.install_limit_once(64 << 20).unwrap();
    let mut input = input_chunk(&program, vec![Some(11), Some(42), Some(7)]);
    input.try_transfer_to(&tracker).unwrap();
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(), Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let construction = factory
        .begin_typed_construction(input.chunk_schema())
        .unwrap();
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> =
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone());
    let filtered = input
        .filter_ordered_scan_controlled(
            &BooleanArray::from(vec![true, false, true]),
            host,
            &Control,
            construction.as_ref(),
        )
        .unwrap()
        .unwrap();
    assert!(
        filtered.memory_lease().is_none(),
        "additional extent must not become a whole-batch lease"
    );
    let mut instance = evaluator(&program);
    let result = evaluate(&mut instance, chunk_input(&filtered), Selection::all(2)).unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(11), Some(7)]
    );
    let alias = result.values().to_data().buffers()[0].clone();
    drop((filtered, result, instance, construction, factory));
    assert!(
        tracker.current() > 0,
        "whole additional ledger must follow the real alias, not only native source authority"
    );
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
}

#[test]
fn input_source_genuinely_pure_chunk_and_record_batch_keep_no_host_ability() {
    let program = program(SeedMode::Input, false);
    let chunk = input_chunk(&program, vec![Some(11), None, Some(7)]);
    let mut requests = 0;
    let storage = chunk.source_storage_lifetime_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(), |_| {requests+=1;Ok(())}
    ).unwrap();
    assert!(
        storage.is_pure(),
        "this no-host positive fixture must actually have no finite creator hold"
    );
    assert_eq!(requests, 0);
    for input in [chunk_input(&chunk), batch_input(chunk.record_batch())] {
        let mut instance = CompiledExpressionInstance::try_new(
            program.unit(site(&program).unit).unwrap().clone(),
            site(&program).root,
            &Control,
        )
        .unwrap();
        let result = evaluate(&mut instance, input, Selection::all(3)).unwrap();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(11), None, Some(7)]
        );
    }
}

#[test]
fn identity_slot_preserves_required_binding_and_null_parent_carrier() {
    use crate::exec::chunk::ChunkSlotSchema;
    use crate::exec::operators::family_fixture::{
        FixtureControl, compile, constant_policy, package,
    };
    use crate::exec::pipeline::operator::input_construction::{
        ConstructionFailure, try_runtime_input_construction_factory,
    };
    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_physical_plan::{
        ConstantPool, ConstantPoolId, ConstantPools, ConstantReference, FragmentId, NodeId,
        ValueOrigin,
    };
    use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueTypeStorageLifetime};

    let required =
        DataType::Struct(vec![Field::new("f", DataType::FixedSizeBinary(4), false)].into());
    let source_type = FunctionValueType::new(required.clone(), true);
    // The static program has a legal exact constant declaration and values.
    // Its runtime input is supplied independently, as a reader supplies a page.
    let present: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"abcd", b"efgh"].into_iter()).unwrap());
    let DataType::Struct(fields) = &required else {
        unreachable!()
    };
    let admitted = StructArray::try_new(fields.clone(), vec![present], None).unwrap();
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("st", required, true)),
        source_type.clone(),
        admitted.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let pool_id = ConstantPoolId::new(904);
    let mut constants = ConstantPools::empty();
    constants.insert(pool_id, pool).unwrap();
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            FragmentId::new(66),
            &fixture_sources,
        );
    let source = NodeId::new(0);
    let project = NodeId::new(1);
    let input = builder
        .add_value(
            source_type.clone(),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let rows = (0..2)
        .map(|ordinal| {
            let definition = builder
                .unit(source)
                .unwrap()
                .add(
                    None,
                    source_type.clone(),
                    ExprKind::Constant(ConstantReference {
                        pool: pool_id,
                        ordinal,
                    }),
                )
                .unwrap();
            let root = builder
                .unit(source)
                .unwrap()
                .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
                .unwrap();
            Box::from([root])
        })
        .collect::<Vec<_>>();
    builder
        .add_values(source, rows.into_boxed_slice(), Box::from([input]))
        .unwrap();
    let expression = builder
        .unit(project)
        .unwrap()
        .add(None, source_type, ExprKind::Value(input))
        .unwrap();
    let expression = builder
        .unit(project)
        .unwrap()
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let output = builder
        .add_value(
            FunctionValueType::new(admitted.data_type().clone(), true),
            ValueOrigin::Expr {
                node: project,
                root: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            source,
            Box::from([(expression, output)]),
            Box::from([output]),
        )
        .unwrap();
    let program = compile(package(builder, project, constants, 1), 1);
    let layout = program.graph().nodes()[0].output_layout();
    let binding = layout.value_binding_at(0).unwrap();
    let expected = binding.data_type().clone();
    let DataType::Struct(fields) = &expected else {
        panic!("frozen source Struct")
    };
    assert!(
        !fields[0].is_nullable(),
        "the original source still promises a required child"
    );
    let actual: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(fields[0].as_ref().clone().with_nullable(true))].into(),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(b"abcd".as_slice()), None].into_iter(),
                    4,
                )
                .unwrap(),
            )],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap(),
    );
    let actual_type = actual.data_type().clone();
    assert_ne!(
        actual_type, expected,
        "the reader carrier has its own physical NULL flag"
    );
    let pure = ValueTypeStorageLifetime::pure();
    let slot = ChunkSlotSchema::try_new_typed(
        layout.slots()[0],
        Arc::new(
            layout
                .schema()
                .field(0)
                .clone()
                .with_data_type(actual_type.clone()),
        ),
        binding.clone(),
        None,
        pure.clone(),
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
    assert_eq!(schema.slots()[0].typed_binding(), binding);
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("permissive fixed source construction"),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    owner
        .observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
        )
        .unwrap();
    let mut chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        vec![actual],
        2,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap();
    let source_tracker = MemTracker::new_root("permissive fixed source payload");
    chunk.try_transfer_to(&source_tracker).unwrap();
    assert!(source_tracker.current() > 0);
    // Materialization keeps the legal source carrier for both full and sparse
    // demand; the actual Project output author then rebuilds its declared port.
    for selected in [vec![0, 1], vec![1]] {
        let selection = if selected.len() == 2 {
            Selection::all(2)
        } else {
            Selection::try_sparse(2, &selected).unwrap()
        };
        let original_array = chunk.columns()[0].clone();
        let original_owner = chunk.source_authority().owner_identity();
        let seen = Arc::new(Mutex::new(0));
        let observed = seen.clone();
        let observer: Arc<InputAdapterObserver> = Arc::new(move |chunk, input| {
            assert_original_chunk_input(chunk, input);
            assert!(Arc::ptr_eq(input.batch().column(0), &original_array));
            assert_eq!(chunk.source_authority().owner_identity(), original_owner);
            *observed.lock().unwrap() += 1;
        });
        let host_tracker = MemTracker::new_root("permissive fixed identity result");
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            {
                let ProgramNodeKind::Project { exprs, .. } =
                    program.graph().nodes()[program.graph().root().index()].kind()
                else {
                    panic!("actual identity Project")
                };
                program.unit(exprs[0].unit).unwrap().clone()
            },
            {
                let ProgramNodeKind::Project { exprs, .. } =
                    program.graph().nodes()[program.graph().root().index()].kind()
                else {
                    panic!("actual identity Project")
                };
                exprs[0].root
            },
            &Control,
            Some(crate::runtime::tracked_state_allocator::expression_allocation_host(host_tracker)),
        )
        .unwrap();
        let mut adapter = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
        adapter.observe_input_adapter(observer);
        let input = chunk_evaluation_input(&chunk, &adapter);
        let output = evaluate(&mut instance, input, selection)
            .unwrap_or_else(|failure| panic!(
                "legal NULL-parent Slot must materialize: selected={selected:?}, expected={expected:?}, actual={actual_type:?}, fact={:?}, failure={failure:?}",
                failure.failure_fact(),
            ));
        assert_eq!(*seen.lock().unwrap(), 1);
        assert_eq!(output.values().data_type(), &actual_type);
        assert!(output.errors().is_empty());
        let values = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let child = values
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (ordinal, source_row) in selected.iter().copied().enumerate() {
            assert_eq!(values.is_null(ordinal), source_row == 1);
            if source_row == 0 {
                assert_eq!(child.value(ordinal), b"abcd");
            } else {
                assert!(child.is_null(ordinal));
            }
        }
        let declared = factory
            .create_compiled_chunk_schema(program.graph().nodes()[1].output_layout())
            .unwrap();
        let project_owner = factory.begin_typed_construction(&declared).unwrap();
        let storage = chunk
            .source_storage_lifetime_observed::<ConstructionFailure>(
                project_owner.storage_lifetime(),
                |layout| project_owner.request(layout),
            )
            .unwrap();
        let mut columns =
            crate::exec::operators::project_reserve::<ArrayRef>(1, project_owner.as_ref()).unwrap();
        columns.push(output.values().clone());
        let projected = crate::exec::operators::materialize_project_output_owned(
            columns,
            selected.len(),
            declared,
            |_| Ok(binding),
            |_| Ok(crate::exec::operators::ProjectEncodingSource::Plain),
            storage,
            project_owner.as_ref(),
        )
        .expect("the actual Project author rebuilds its frozen required-child output");
        assert_eq!(projected.columns()[0].data_type(), &expected);
        assert_eq!(projected.chunk_schema().slots()[0].typed_binding(), binding);
        let projected_values = projected.columns()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let projected_child = projected_values
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (ordinal, source_row) in selected.iter().copied().enumerate() {
            assert_eq!(projected_values.is_null(ordinal), source_row == 1);
            if source_row == 0 {
                assert_eq!(projected_child.value(ordinal), b"abcd");
            }
        }
        // A required Struct child may physically remain NULL below its NULL
        // parent; the declared child field and active-value check are exact.
        assert!(!projected_values.fields()[0].is_nullable());
        drop(projected);
        drop(output);
        drop(instance);
    }
    drop(chunk);
    assert_eq!(source_tracker.current(), 0);
}

mod cast;
