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

//! Actual grouped producer aliases, exact SUM phases and unpublished finish.
//! Execution owns this local affected-PR component lane; checked Values and
//! original installed owners need no services. Arithmetic/text expectations
//! are declared independently. Existing host limits and actual Drop protect
//! resource behavior, with no timing, performance gate or product controller.

use super::*;

fn populated(f: &Fixture, tracker: Arc<MemTracker>) -> CompiledGroupedTopNProcessor {
    let factory = CompiledGroupedTopNProcessorFactory::try_new(
        f.program.clone(),
        f.grouped,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut processor = factory.processor();
    processor.set_mem_tracker(tracker);
    let state = RuntimeState::default();
    // Independent Partial processors preserve each original contribution.
    for offset in 0..f.input.len() {
        for chunk in partial_chunks(f, offset, 1) {
            processor.push_chunk(&state, chunk).unwrap();
        }
    }
    assert!(!processor.has_output());
    processor
}

fn drain(processor: &mut CompiledGroupedTopNProcessor) -> Vec<Chunk> {
    let state = RuntimeState::default();
    processor.set_finishing(&state).unwrap();
    let mut output = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
        output.push(chunk);
    }
    output
}

fn text_fixture(keys: &[String], limit: usize) -> Fixture {
    let rows = keys
        .iter()
        .map(|key| {
            vec![
                LiteralValue::Utf8(key.clone().into()),
                LiteralValue::Int64(1),
            ]
        })
        .collect::<Vec<_>>();
    fixture_with_keys(
        &[FunctionValueType::new(DataType::Utf8, true)],
        &rows,
        &[(0, true, true)],
        limit,
    )
}

#[test]
fn compiled_grouped_topn_actual_key_array_clone_keeps_stock_after_all_parents_drop() {
    let f = text_fixture(&["a".into(), "é中".into()], 2);
    let tracker = MemTracker::new_root("grouped-key-array-alias");
    let mut processor = populated(&f, tracker.clone());
    let output = drain(&mut processor);
    let alias = output[1].batch.column(0).clone();
    drop(output);
    drop(processor);
    drop(f);
    assert_eq!(array_key(&alias, 0), Some(KeyCell::Text("é中".into())));
    assert!(tracker.current() > 0, "actual key custody remains live");
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_actual_key_buffer_keeps_stock_after_parent_array_drop() {
    let f = text_fixture(&["a".into(), "é中".into()], 2);
    let tracker = MemTracker::new_root("grouped-key-buffer-alias");
    let mut processor = populated(&f, tracker.clone());
    let output = drain(&mut processor);
    let array = output[1].batch.column(0).clone();
    let alias = array.to_data().buffers()[1].clone();
    drop(array);
    drop(output);
    drop(processor);
    drop(f);
    assert_eq!(alias.as_slice(), "é中".as_bytes());
    assert!(
        tracker.current() > 0,
        "derived payload retains original key custody"
    );
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

fn sum_fixture(rows: &[(i64, i64)]) -> Fixture {
    let rows = rows
        .iter()
        .map(|&(key, value)| vec![LiteralValue::Int64(key), LiteralValue::Int64(value)])
        .collect::<Vec<_>>();
    fixture_with_keys(&[int64(true)], &rows, &[(0, true, true)], 1)
}

#[test]
fn compiled_grouped_topn_sum_intermediate_remains_exact_before_final_and_eviction() {
    for (rows, expected) in [
        (
            vec![(1, i64::MAX), (1, 1), (1, -2)],
            (Some(1), 3, Some(i64::MAX - 1), Some(-2), Some(i64::MAX)),
        ),
        // Evict an exact but out-of-BIGINT-range state before Final output.
        (
            vec![(2, i64::MAX), (2, 1), (1, 5)],
            (Some(1), 1, Some(5), Some(5), Some(5)),
        ),
    ] {
        let f = sum_fixture(&rows);
        let tracker = MemTracker::new_root("grouped-exact-sum");
        let mut processor = populated(&f, tracker.clone());
        let intermediate = drain(&mut processor);
        assert_eq!(intermediate.len(), 1);
        assert_eq!(collect_final(&f, intermediate), [expected]);
        drop(processor);
        drop(f);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_grouped_topn_retained_sum_overflow_is_only_a_final_operational_failure() {
    let f = sum_fixture(&[(1, i64::MAX), (1, 1), (2, 5)]);
    let tracker = MemTracker::new_root("grouped-overflowing-sum-intermediate");
    let mut processor = populated(&f, tracker.clone());
    let intermediate = drain(&mut processor);
    assert_eq!(
        intermediate.len(),
        1,
        "Intermediate must emit the exact state"
    );
    let final_tracker = MemTracker::new_root("grouped-sum-overflow-final");
    let error = try_final_chunks_tracked(&f, intermediate, final_tracker.clone()).unwrap_err();
    let ExecutionFailureCause::Kernel(KernelFailure::Operational(message)) = error.cause() else {
        panic!("unexpected SUM failure: {error:?}")
    };
    assert_eq!(message.message(), "sum result overflows BIGINT");
    drop(error);
    drop(processor);
    drop(f);
    assert_eq!(tracker.current(), 0);
    assert_eq!(final_tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_later_finish_refusal_destroys_the_unpublished_success_prefix() {
    // A real successful two-key run supplies generous observed headroom for
    // the same pending width, first key and installed owners. This observes
    // host accounting only; it is neither a physical peak nor a byte guard.
    let control_fixture = text_fixture(&["a".into(), "z".into()], 2);
    let control_tracker = MemTracker::new_root("grouped-finish-positive-control");
    let mut control = populated(&control_fixture, control_tracker.clone());
    let before = control_tracker.current();
    let output = drain(&mut control);
    let headroom = control_tracker.peak();
    assert_eq!(output.len(), 2);
    assert_eq!(
        array_key(output[0].batch.column(0), 0),
        Some(KeyCell::Text("a".into()))
    );
    drop(output);
    drop(control);
    assert_eq!(control_tracker.current(), 0);
    assert!(headroom > before);
    let prefix_tracker = MemTracker::new_root("grouped-finish-bounded-positive-control");
    let mut prefix_control = populated(&control_fixture, prefix_tracker.clone());
    prefix_tracker
        .install_limit_once(prefix_tracker.current().checked_add(headroom).unwrap())
        .unwrap();
    let prefix_output = drain(&mut prefix_control);
    assert_eq!(prefix_output.len(), 2);
    assert_eq!(
        array_key(prefix_output[0].batch.column(0), 0),
        Some(KeyCell::Text("a".into()))
    );
    drop(prefix_output);
    drop(prefix_control);
    drop(control_fixture);
    assert_eq!(prefix_tracker.current(), 0);

    // Both consume operations are legal and complete before installing the
    // immutable host limit. The second key's public Arrow invoice alone is
    // larger than the available headroom (2 * encoded payload >= 2 * text).
    let long = "z".repeat(usize::try_from(headroom).unwrap() + 1);
    let f = text_fixture(&["a".into(), long], 2);
    let tracker = MemTracker::new_root("grouped-unpublished-finish-prefix");
    let mut processor = populated(&f, tracker.clone());
    let limit = tracker.current().checked_add(headroom).unwrap();
    tracker.install_limit_once(limit).unwrap();
    let state = RuntimeState::default();
    let first = processor.set_finishing(&state).unwrap_err();
    assert_eq!(
        first.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted)
    );
    assert!(!processor.has_output());
    assert!(processor.candidates.is_none());
    assert!(processor.pending.is_none());
    assert!(processor.instances.is_none());
    assert_eq!(
        tracker.current(),
        0,
        "real unpublished output and states dropped"
    );
    assert_eq!(processor.pull_chunk(&state).unwrap_err(), first);
    assert_eq!(processor.set_finishing(&state).unwrap_err(), first);
    processor.cancel();
    assert_eq!(processor.close().unwrap_err(), first);
}

#[test]
fn compiled_grouped_topn_second_call_merge_data_survives_state_drop_and_keeps_first_cause() {
    // Each independent Partial receives a uniform legal N. MIN_N merges N2
    // successfully; only the SECOND MAX_N call sees N2 versus N3. Original
    // producers author both Binary payloads; no malformed state is injected.
    let rows = [
        vec![
            LiteralValue::Int64(1),
            LiteralValue::LargeInt(2),
            LiteralValue::Int64(2),
            LiteralValue::Int64(2),
        ],
        vec![
            LiteralValue::Int64(1),
            LiteralValue::LargeInt(3),
            LiteralValue::Int64(2),
            LiteralValue::Int64(3),
        ],
    ];
    let f =
        fixture_with_independent_limits(&[int64(true)], &rows, &[(0, true, true)], 1, true, true);
    let tracker = MemTracker::new_root("grouped-second-call-data");
    let factory = CompiledGroupedTopNProcessorFactory::try_new(
        f.program.clone(),
        f.grouped,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let first_contract = factory.plan.calls[0].contract.clone();
    let second_contract = factory.plan.calls[1].contract.clone();
    let mut processor = factory.processor();
    processor.set_mem_tracker(tracker.clone());
    let state = RuntimeState::default();
    for chunk in partial_chunks(&f, 0, 1) {
        processor.push_chunk(&state, chunk).unwrap();
    }
    let chunks = partial_chunks(&f, 1, 1);
    assert_eq!(chunks.len(), 1);
    let first = processor
        .push_chunk(&state, chunks.into_iter().next().unwrap())
        .unwrap_err();
    let ExecutionFailureCause::InvocationData(data) = first.cause() else {
        panic!("unexpected second-call failure: {first:?}")
    };
    assert!(std::ptr::eq(
        data.aggregate_contract(),
        second_contract.as_ref()
    ));
    assert!(!std::ptr::eq(
        data.aggregate_contract(),
        first_contract.as_ref()
    ));
    assert_eq!(
        data.message(),
        "merge aggregate state: min_n/max_n limit mismatch while merging states: 2 vs 3"
    );
    assert_eq!(
        data.aggregate_call_phase(),
        novarocks_functions::AggregateKernelPhase::Intermediate
    );
    assert_eq!(
        data.aggregate_phase(),
        novarocks_functions::AggregateInvocationPhase::Merge
    );
    assert_eq!(data.batch_rows(), 1);
    assert_eq!(data.input_rows(), [0]);
    assert_eq!(data.state_indices(), [0]);
    assert_eq!(data.emission_row_capacity(), None);
    assert!(!processor.has_output());
    assert!(processor.candidates.is_none());
    assert!(processor.pending.is_none());
    assert!(processor.instances.is_none());
    assert_eq!(processor.pull_chunk(&state).unwrap_err(), first);
    assert_eq!(processor.set_finishing(&state).unwrap_err(), first);
    processor.cancel();
    assert_eq!(processor.close().unwrap_err(), first);
    drop(processor);
    drop(factory);
    drop(f);
    assert!(
        tracker.current() > 0,
        "original Data backing survives actual state Drop"
    );
    assert_eq!(
        data.message(),
        "merge aggregate state: min_n/max_n limit mismatch while merging states: 2 vs 3"
    );
    drop(first);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_actual_storage_refuses_replacement_coexistence_and_new_state() {
    // Existing generic growth coverage rejects even the final enlarged block.
    // These actual consumer containers instead get enough room for replacement
    // AFTER old-block release, but cannot admit both blocks simultaneously.
    // This is a storage-component gate through existing private owners/APIs,
    // not an injected allocation trace of a complete push or private Arrow peak.
    for storage in ["candidates", "key", "inline-state"] {
        let f = fixture(2, true, true);
        let tracker = MemTracker::new_root("grouped-owned-storage-refusal");
        let mut processor = populated(&f, tracker.clone());
        let before = tracker.current();
        let set = processor.candidates.as_mut().unwrap();
        let original_keys = set
            .entries
            .iter()
            .map(|entry| entry.key.to_vec())
            .collect::<Vec<_>>();
        let failure = match storage {
            "candidates" => {
                let capacity = set.entries.capacity();
                let len = set.entries.len();
                let old = std::alloc::Layout::array::<Candidate>(capacity)
                    .unwrap()
                    .size();
                let replacement = std::alloc::Layout::array::<Candidate>(len + capacity)
                    .unwrap()
                    .size();
                let limit =
                    before - i64::try_from(old).unwrap() + i64::try_from(replacement).unwrap();
                tracker.install_limit_once(limit).unwrap();
                assert!(set.entries.try_reserve_exact(capacity).is_err());
                assert_eq!(set.entries.capacity(), capacity);
                assert_eq!(set.entries.len(), len);
                KernelFailure::ResourceExhausted
            }
            "key" => {
                let key = &mut set.entries[0].key;
                let capacity = key.capacity();
                let len = key.len();
                let limit = before - i64::try_from(capacity).unwrap()
                    + i64::try_from(len + capacity).unwrap();
                tracker.install_limit_once(limit).unwrap();
                assert!(key.try_reserve_exact(capacity).is_err());
                assert_eq!(key.capacity(), capacity);
                assert_eq!(key.len(), len);
                KernelFailure::ResourceExhausted
            }
            "inline-state" => {
                let handle = processor.plan.calls[0].handle.clone();
                let layout = handle.state_layout().pad_to_align();
                assert!(layout.size() > 0);
                tracker
                    .install_limit_once(before + i64::try_from(layout.size()).unwrap() - 1)
                    .unwrap();
                OwnedAggregateState::try_new(handle, set.host.clone(), &processor.control)
                    .unwrap_err()
            }
            _ => unreachable!(),
        };
        assert_eq!(failure, KernelFailure::ResourceExhausted);
        assert_eq!(
            tracker.current(),
            before,
            "refused request preserves existing stock"
        );
        assert_eq!(
            set.entries
                .iter()
                .map(|entry| entry.key.to_vec())
                .collect::<Vec<_>>(),
            original_keys
        );
        let first = processor.latch::<()>(Err(failure.into())).unwrap_err();
        assert!(!processor.has_output());
        assert_eq!(
            tracker.current(),
            0,
            "real storage and typed states destroyed"
        );
        assert_eq!(
            processor.pull_chunk(&RuntimeState::default()).unwrap_err(),
            first
        );
        assert_eq!(processor.close().unwrap_err(), first);
    }
}
