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

use arrow::array::{Array, BooleanArray, Int64Array};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationFailure};
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
    CompiledExpressionInstance::try_new(Arc::clone(program), site(), &Control).unwrap()
}

fn evaluate<'a>(
    instance: &mut CompiledExpressionInstance,
    input: CompiledInputLoan<'_>,
    selection: Selection<'a>,
) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
    instance.evaluate_input_controlled(
        input,
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
    let original_array = Arc::clone(chunk.batch.column(1));
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
    assert!(Arc::ptr_eq(result.values(), chunk.batch.column(1)));
    assert_eq!(*observed.lock().unwrap(), 1);
    assert_eq!(chunk.source_authority().strong_counts(), (Some(1), None));
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
        CompiledInputLoan::RecordBatch(&chunk.batch)
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
        CompiledInputLoan::RecordBatch(&chunk.batch),
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
                &chunk.batch,
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
        assert_eq!(chunk.source_authority().strong_counts(), (Some(1), None));
    }
    // Returned values borrow Selection only. Both the input Chunk and its
    // attached charge may exit before the result, as in the original entry.
    let mut instance = evaluator(&program);
    let result = evaluate(
        &mut instance,
        CompiledInputLoan::from_chunk(&chunk),
        Selection::all(4),
    )
    .unwrap();
    drop(chunk);
    assert_eq!(tracker.current(), 0);
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
}

#[test]
fn chunk_entry_keeps_original_validation_and_failed_instance_latch() {
    let program = program(SeedMode::Input, false);
    let chunk = input_chunk(&program, vec![Some(42)]);
    let bad = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("wrong", DataType::Int64, true),
        ])),
        vec![Arc::clone(chunk.batch.column(1))],
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
            &bad.batch,
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
            &chunk.batch,
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
        a,
        ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
    ));
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

#[test]
fn project_and_writer_entries_reach_original_checked_slot_without_capture() {
    let program = program(SeedMode::Input, false);
    let tracker = MemTracker::new_root("operator-source-borrow");
    for writer in [false, true] {
        let mut chunk = input_chunk(&program, vec![Some(42), None, Some(7)]);
        chunk.try_transfer_to(&tracker).unwrap();
        let bytes = tracker.current();
        let expected_owner = chunk.source_authority().owner_identity();
        let expected_array = Arc::as_ptr(chunk.batch.column(1)) as *const () as usize;
        let seen = Arc::new(Mutex::new(0));
        let observed = Arc::clone(&seen);
        let observer: Arc<SlotSourceObserver> = Arc::new(move |column| {
            let (array, source) = column.into_parts();
            assert_eq!(Arc::as_ptr(array) as *const () as usize, expected_array);
            let BorrowedInputSource::Attached(source) = source else {
                panic!("production entry lost the Chunk source")
            };
            assert_eq!(source.owner_identity(), expected_owner);
            assert_eq!(source.strong_counts(), (Some(1), None));
            *observed.lock().unwrap() += 1;
        });
        let output = if writer {
            let mut projector = crate::exec::operators::compiled_writer::source_loan_test_projector(
                Arc::clone(&program),
                Some(observer),
            );
            let output = projector.project(&chunk).unwrap();
            assert_eq!(tracker.current(), bytes);
            assert_eq!(chunk.source_authority().strong_counts(), (Some(1), None));
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
        assert_eq!(tracker.current(), 0);
    }
    assert_eq!(tracker.allocated(), tracker.deallocated());
}
