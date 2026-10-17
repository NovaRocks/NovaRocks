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

use crate::runtime::fragment::{ExecutionFailureCause, ExecutionResult};
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
    let host = crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker);
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
fn record_batch_without_chunk_and_chunk_without_authority_are_distinct() {
    let program = program(SeedMode::Input, false);
    let chunk = input_chunk(&program, vec![Some(42)]);
    assert!(matches!(
        CompiledInputLoan::RecordBatch(&chunk.record_batch())
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
        CompiledInputLoan::RecordBatch(&chunk.record_batch()),
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
                &chunk.record_batch(),
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
            &bad.record_batch(),
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
            &chunk.record_batch(),
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
    input.try_transfer_to_typed(&tracker).unwrap();
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(), Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let construction = factory
        .begin_typed_construction(input.chunk_schema())
        .unwrap();
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> =
        crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker.clone());
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
        crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker.clone());
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
