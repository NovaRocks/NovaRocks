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

use std::sync::{Arc, Mutex};

use crate::runtime::task::{ExecutionFailureCause, ExecutionResult};
use arrow::array::{Array, BooleanArray, Int64Array};
use novarocks_functions::ScalarInvocationActivation;
use novarocks_local_program::ProgramNodeExpressionRole;

use super::input_source::{BorrowedInputSource, CompiledInputLoan, SlotSourceObserver};
use super::tests::{SeedMode, program};
use super::*;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::runtime::mem_tracker::MemTracker;

pub(crate) fn input_chunk(program: &LocalProgram, values: Vec<Option<i64>>) -> Chunk {
    let rows = values.len();
    let layout = program.graph().nodes()[2].output_layout();
    let batch = RecordBatch::try_new(
        Arc::clone(layout.schema()),
        vec![
            Arc::new(Int64Array::from(vec![999; rows])),
            Arc::new(Int64Array::from(values)),
            Arc::new(BooleanArray::from(vec![true; rows])),
        ],
    )
    .unwrap();
    Chunk::try_new_with_chunk_schema(batch, ChunkSchema::from_compiled_layout(layout).unwrap())
        .unwrap()
}

fn site() -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(3),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 1 },
    }
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
    CompiledExpressionInstance::try_new_with_allocator(
        Arc::clone(program),
        site(),
        &Control,
        Some(host),
    )
    .unwrap()
}

fn evaluate<'a>(
    instance: &mut CompiledExpressionInstance,
    input: CompiledInputLoan<'_>,
    selection: Selection<'a>,
) -> ExecutionResult<SelectedValues<'a>> {
    instance.evaluate_input_controlled(
        input,
        selection,
        selection,
        if selection.is_empty() {
            ScalarInvocationActivation::ValidateOnly
        } else {
            ScalarInvocationActivation::Activated
        },
        &Control,
        None,
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
    let observer: Arc<SlotSourceObserver> = Arc::new(move |column| {
        let (array, source) = column.into_parts();
        assert!(Arc::ptr_eq(array, &original_array));
        let BorrowedInputSource::Attached(source) = source else {
            panic!("original attached source is required")
        };
        assert_eq!(source.owner_identity(), owners);
        assert_eq!(source.strong_counts(), (Some(1), None));
        *seen.lock().unwrap() += 1;
    });
    let mut instance = evaluator(&program);
    instance.observe_slot_source(observer);
    let result = evaluate(
        &mut instance,
        CompiledInputLoan::from_chunk(&chunk),
        Selection::all(3),
    )
    .unwrap();
    assert_eq!(
        result.values().to_data(),
        chunk.record_batch().column(1).to_data()
    );
    assert_eq!(*observed.lock().unwrap(), 1);
    assert!(chunk.source_authority().strong_counts().0.unwrap() > 1);
    assert!(
        CompiledInputLoan::from_chunk(&chunk)
            .checked_column(3)
            .is_none()
    );
}

#[test]
fn projected_input_maps_original_slots_through_actual_sparse_domains() {
    use super::input_source::InputColumnProjection;
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
            CompiledInputLoan::from_projected_chunk(&compact, &declared, &projection, &Control)
                .unwrap();
        assert_eq!(loan.batch().num_columns(), 1);
        assert_eq!(loan.schema().fields().len(), 3);
        for selected in [vec![0, 1, 2, 3], vec![1, 3], vec![]] {
            let selection = Selection::try_sparse(4, &selected).unwrap();
            let mut instance = evaluator(&program);
            let actual = evaluate(&mut instance, loan, selection);
            if ordinals == [0] {
                assert!(matches!(
                    actual.unwrap_err().cause(),
                    ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(_))
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
    use super::input_source::InputColumnProjection;
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
    let actual = CompiledInputLoan::from_projected_chunk(&actual, &declared, &projection, &Control);
    assert!(matches!(
        actual.err().expect("logical binding was refused"),
        KernelFailure::InvalidProgram(_)
    ));
}

#[test]
fn record_batch_without_chunk_and_chunk_without_authority_are_distinct() {
    let program = program(SeedMode::Input, false);
    let chunk = input_chunk(&program, vec![Some(42)]);
    assert!(matches!(
        CompiledInputLoan::RecordBatch(chunk.record_batch())
            .checked_column(1)
            .unwrap()
            .into_parts()
            .1,
        BorrowedInputSource::NoChunkProvided
    ));
    assert!(matches!(
        CompiledInputLoan::from_chunk(&chunk)
            .checked_column(1)
            .unwrap()
            .into_parts()
            .1,
        BorrowedInputSource::NoAttachedAuthority
    ));
    let results = [
        CompiledInputLoan::RecordBatch(chunk.record_batch()),
        CompiledInputLoan::from_chunk(&chunk),
    ]
    .map(|input| {
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
        let before = old
            .evaluate_evaluation(
                chunk.record_batch(),
                selection,
                if selection.is_empty() {
                    ScalarInvocationActivation::ValidateOnly
                } else {
                    ScalarInvocationActivation::Activated
                },
                &Control,
            )
            .unwrap();
        let after = evaluate(
            &mut actual,
            CompiledInputLoan::from_chunk(&chunk),
            selection,
        )
        .unwrap();
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
    let result = evaluate(
        &mut instance,
        CompiledInputLoan::from_chunk(&chunk),
        Selection::all(4),
    )
    .unwrap();
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
    let a = old
        .evaluate_evaluation(
            bad.record_batch(),
            Selection::all(1),
            ScalarInvocationActivation::Activated,
            &Control,
        )
        .unwrap_err();
    let b = evaluate(
        &mut actual,
        CompiledInputLoan::from_chunk(&bad),
        Selection::all(1),
    )
    .unwrap_err();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    let a = old
        .evaluate_evaluation(
            chunk.record_batch(),
            Selection::all(1),
            ScalarInvocationActivation::Activated,
            &Control,
        )
        .unwrap_err();
    let b = evaluate(
        &mut actual,
        CompiledInputLoan::from_chunk(&chunk),
        Selection::all(1),
    )
    .unwrap_err();
    assert!(matches!(
        a.cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
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
        // Existing Project intermediates can retain several genuine holders.
        // Observe the original Slot identity, not an incidental clone count;
        // checked_slot_pairs_exact_column_and_original_chunk_authority above
        // separately proves the lazy first-Slot observation precedes capture.
        let observer: Arc<SlotSourceObserver> = Arc::new(move |column| {
            let (array, source) = column.into_parts();
            assert_eq!(Arc::as_ptr(array) as *const () as usize, expected_array);
            let BorrowedInputSource::Attached(source) = source else {
                panic!("production entry lost the Chunk source")
            };
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
    let output = evaluate(
        &mut instance,
        CompiledInputLoan::from_chunk(&chunk),
        Selection::all(3),
    )
    .unwrap();
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
        assert!(matches!(
            CompiledInputLoan::from_chunk(&chunk)
                .checked_column(1)
                .unwrap()
                .into_parts()
                .1,
            BorrowedInputSource::NoAttachedAuthority
        ));
        let mut instance = if has_host {
            evaluator(&program)
        } else {
            CompiledExpressionInstance::try_new(program.clone(), site(), &Control).unwrap()
        };
        let result = evaluate(
            &mut instance,
            CompiledInputLoan::from_chunk(&chunk),
            Selection::all(3),
        );
        if has_host {
            let result = result.unwrap();
            let alias = result.values().to_data().buffers()[0].clone();
            drop((chunk, result, instance));
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(alias);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        } else {
            let error = result.unwrap_err();
            let ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(message)) =
                error.cause()
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
    let result = evaluate(
        &mut instance,
        CompiledInputLoan::from_chunk(&filtered),
        Selection::all(2),
    )
    .unwrap();
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
    for input in [
        CompiledInputLoan::from_chunk(&chunk),
        CompiledInputLoan::RecordBatch(chunk.record_batch()),
    ] {
        let mut instance =
            CompiledExpressionInstance::try_new(program.clone(), site(), &Control).unwrap();
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
fn compiled_canonical_empty_result_keeps_its_own_creator_independent_of_equal_source() {
    use novarocks_type_contract::{
        FunctionValueType, ValueLayout, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Creator(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Creator {}
    impl Drop for Creator {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let data_type = DataType::Struct(
        vec![Arc::new(arrow::datatypes::Field::new(
            "canonical_child",
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let semantic = FunctionValueType::new(data_type.clone(), true);
    let source_drops = Arc::new(AtomicUsize::new(0));
    let canonical_drops = Arc::new(AtomicUsize::new(0));
    let source = FunctionValueType::try_from_value(
        semantic.value_type().clone(),
        ValueLayout::try_new(
            &data_type,
            ValueTypeStorageLifetime::held(Arc::new(Creator(source_drops.clone()))),
        )
        .unwrap(),
    )
    .unwrap();
    let canonical = FunctionValueType::try_from_value(
        semantic.value_type().clone(),
        ValueLayout::try_new(
            &data_type,
            ValueTypeStorageLifetime::held(Arc::new(Creator(canonical_drops.clone()))),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(source, canonical);
    assert!(
        !source
            .typed_binding()
            .layout()
            .shares_storage(canonical.typed_binding().layout())
    );
    let raw = arrow::array::new_empty_array(canonical.data_type());
    let missing = super::input_source::retain_result_type(raw.clone(), &canonical, None, &Control)
        .unwrap_err();
    assert_eq!(
        missing,
        KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "compiled result Arrow creator requires its actual allocation host"
        ))
    );
    let tracker = MemTracker::new_root("canonical-empty-result-custody");
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> =
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone());
    let result =
        super::input_source::retain_result_type(raw, &canonical, Some(&host), &Control).unwrap();
    assert_eq!(result.len(), 0);
    drop((source, canonical, semantic));
    assert_eq!(
        source_drops.load(Ordering::SeqCst),
        1,
        "equal source is not the canonical creator"
    );
    assert_eq!(
        canonical_drops.load(Ordering::SeqCst),
        0,
        "the empty result has actual independent canonical custody"
    );
    let last = result.clone();
    drop(result);
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 1);
    assert_eq!(tracker.current(), 0);
}

// Exercise real SQL/LocalCompiler CASE roots over the original finite input.
// Parent validity masks required children; only demanded branches read payloads.
#[test]
fn fixed_binary_case_roots_preserve_values_and_input_lifetime() {
    use crate::exec::expr::filter_conjunction_actual_sql_compiler_tests::compiler_results_with_declared_read_fields;
    use crate::exec::expr::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
    use crate::exec::expr::sql_test_source::sql_source_with_declared_fields;
    use arrow::array::{
        ArrayRef, FixedSizeBinaryArray, Int32Array, StructArray, builder::FixedSizeBinaryBuilder,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use novarocks_local_program::ProgramNodeKind;
    let nested = DataType::Struct(
        vec![Arc::new(Field::new(
            "f",
            DataType::FixedSizeBinary(4),
            false,
        ))]
        .into(),
    );
    let fields = [
        Field::new("raw", DataType::FixedSizeBinary(16), false),
        Field::new("st", nested, true),
        Field::new("n", DataType::Int32, false),
    ];
    let source = sql_source_with_declared_fields(
        "SELECT CASE WHEN n=10 THEN raw ELSE NULL END AS raw,CASE WHEN n=10 THEN st ELSE NULL END AS st,n+1 AS next_n FROM fixture",
        &fields,
    );
    let programs = compiler_results_with_declared_read_fields(
        &source,
        &fields,
        &installed_builtin_owner_catalogue(),
    );
    let mut sites = Vec::new();
    for program in programs.into_values() {
        let program = Arc::new(program.expect("real SQL fixed roots compile unchanged"));
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { input, exprs, .. } = node.kind() {
                let child = &program.graph().nodes()[input.index()];
                if exprs.len() == 3
                    && matches!(child.kind(), ProgramNodeKind::Scan { .. })
                    && child.output_layout().schema().fields().len() == 3
                {
                    assert_eq!(
                        child.output_layout().schema().field(0).data_type(),
                        &DataType::FixedSizeBinary(16)
                    );
                    assert_eq!(
                        child.output_layout().schema().field(1).data_type(),
                        fields[1].data_type()
                    );
                    for &expression in &exprs[..2] {
                        assert!(
                            matches!(
                                program
                                    .graph()
                                    .expressions()
                                    .node(expression)
                                    .unwrap()
                                    .kind(),
                                novarocks_local_program::StaticExprKind::Case { .. }
                            ),
                            "the real SQL author must preserve both whole CASE roots"
                        );
                    }
                    sites.push((program.clone(), node.local_id().unwrap(), *input));
                }
            }
        }
    }
    assert_eq!(
        sites.len(),
        1,
        "use the one genuine scan-fed Project, not another fragment's input"
    );
    let (program, node, input) = sites.pop().unwrap();
    let sparse = [1usize];
    let empty = [];
    for expression in [0u32, 1] {
        for selection in [
            Selection::all(2),
            Selection::try_sparse(2, &sparse).unwrap(),
            Selection::try_sparse(2, &empty).unwrap(),
        ] {
            let layout = program.graph().nodes()[input.index()].output_layout();
            let mut raw = FixedSizeBinaryBuilder::with_capacity(2, 16);
            raw.append_value([0x11; 16]).unwrap();
            raw.append_value([0x22; 16]).unwrap();
            let mut child = FixedSizeBinaryBuilder::with_capacity(2, 4);
            child.append_value([1, 2, 3, 4]).unwrap();
            child.append_null();
            let DataType::Struct(nested_fields) = layout.schema().field(1).data_type() else {
                panic!("actual frozen Struct")
            };
            assert!(!nested_fields[0].is_nullable());
            let st: ArrayRef = Arc::new(
                StructArray::try_new(
                    nested_fields.clone(),
                    vec![Arc::new(child.finish())],
                    Some(NullBuffer::from(vec![true, false])),
                )
                .unwrap(),
            );
            let batch = RecordBatch::try_new(
                layout.schema().clone(),
                vec![
                    Arc::new(raw.finish()),
                    st,
                    Arc::new(Int32Array::from(vec![10, 20])),
                ],
            )
            .unwrap();
            let mut chunk = Chunk::try_new_with_chunk_schema(
                batch,
                ChunkSchema::from_compiled_layout(layout).unwrap(),
            )
            .unwrap();
            let source_tracker = MemTracker::new_root("fixed-case-input-source");
            chunk.try_transfer_to(&source_tracker).unwrap();
            let source_bytes = source_tracker.current();
            assert!(source_bytes > 0);
            let actual_owner = chunk.source_authority().owner_identity();
            let actual_array = chunk.record_batch().column(expression as usize).clone();
            let condition_array = chunk.record_batch().column(2).clone();
            let seen = Arc::new(Mutex::new([0usize; 2]));
            let observed = seen.clone();
            let observer: Arc<SlotSourceObserver> = Arc::new(move |column| {
                let (array, source) = column.into_parts();
                let ordinal = if Arc::ptr_eq(array, &condition_array) {
                    0
                } else {
                    assert!(
                        Arc::ptr_eq(array, &actual_array),
                        "CASE observes its own payload or condition Slot only"
                    );
                    1
                };
                let BorrowedInputSource::Attached(source) = source else {
                    panic!("actual input source must survive")
                };
                assert_eq!(source.owner_identity(), actual_owner);
                observed.lock().unwrap()[ordinal] += 1;
            });
            let host_tracker = MemTracker::new_root("fixed-case-result-host");
            let host = crate::runtime::tracked_state_allocator::expression_allocation_host(
                host_tracker.clone(),
            );
            let mut instance = CompiledExpressionInstance::try_new_with_allocator(
                program.clone(),
                ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::ProjectOutput { expression },
                },
                &Control,
                Some(host),
            )
            .unwrap();
            instance.observe_slot_source(observer);
            let output = evaluate(
                &mut instance,
                CompiledInputLoan::from_chunk(&chunk),
                selection,
            )
            .expect("the real whole CASE must prepare and materialize with its exact declaration");
            assert!(output.errors().is_empty());
            assert_eq!(output.values().len(), selection.len());
            assert_eq!(
                *seen.lock().unwrap(),
                [
                    usize::from(!selection.is_empty()),
                    usize::from(selection.iter().any(|row| row == 0))
                ],
                "condition is read once; the THEN payload is read only for the true row"
            );
            if expression == 0 {
                let raw = output
                    .values()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                assert_eq!(raw.value_length(), 16);
                for (i, row) in selection.iter().enumerate() {
                    assert_eq!(raw.is_null(i), row == 1);
                    if row == 0 {
                        assert_eq!(raw.value(i), &[0x11; 16]);
                    }
                }
            } else {
                let st = output
                    .values()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let DataType::Struct(fields) = st.data_type() else {
                    unreachable!()
                };
                assert!(!fields[0].is_nullable());
                for (i, row) in selection.iter().enumerate() {
                    assert_eq!(st.is_null(i), row == 1);
                    if row == 0 {
                        assert_eq!(
                            st.column(0)
                                .as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()
                                .unwrap()
                                .value(i),
                            &[1, 2, 3, 4]
                        );
                    }
                }
            }
            let data = output.values().to_data();
            let last = if expression == 0 {
                data.buffers()[0].clone()
            } else {
                data.child_data()[0].buffers()[0].clone()
            };
            drop(data);
            drop(output);
            drop(instance);
            drop(chunk);
            if !selection.is_empty() {
                assert_eq!(
                    source_tracker.current(),
                    source_bytes,
                    "the output Buffer retains the exact original source"
                );
            }
            std::thread::spawn(move || drop(last)).join().unwrap();
            assert_eq!(source_tracker.current(), 0);
            assert_eq!(host_tracker.current(), 0);
        }
    }
}
