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

//! Actual variable-state owners and their original emitted backing lifetimes.
//! Local checked Values/catalog inputs need no external fixture. Execution owns
//! this affected-change component lane; independent full-group i128 lists are
//! the oracle. These tests protect result/eviction/last-alias/error behavior,
//! use existing tracker/control APIs, and add no performance or product hook.

use super::*;
use arrow::array::{BinaryArray, ListArray};
use novarocks_functions::AggregateStateMemoryPolicy;

const N_ROWS: &[(Option<i64>, Option<i128>)] = &[
    (Some(9), Some(i128::MAX)),
    (Some(9), Some(-9)),
    (Some(9), Some(90)),
    (Some(8), Some(i128::MIN)),
    (Some(8), Some(80)),
    (Some(8), Some(-8)),
    (Some(3), Some(30)),
    (Some(3), Some(-3)),
    (Some(3), Some(33)),
    (Some(2), Some(i128::MAX)),
    (Some(2), Some(20)),
    (Some(2), Some(-2)),
    (Some(1), Some(i128::MIN)),
    (Some(1), Some(i128::MAX)),
    (Some(1), Some(11)),
    (None, None),
    (None, None),
    (None, None),
    (Some(2), Some(-2)),
    (Some(2), None),
    (Some(2), Some(22)),
    (Some(1), Some(i128::MIN)),
    (Some(1), None),
    (Some(1), Some(-11)),
];
fn n_fixture(limit: usize) -> Fixture {
    let rows = N_ROWS
        .iter()
        .map(|(key, value)| {
            vec![
                cell(*key),
                value.map_or(LiteralValue::Null, LiteralValue::LargeInt),
                LiteralValue::Int64(2),
            ]
        })
        .collect::<Vec<_>>();
    // Uniform real update value2 is carried in the original state. The shared
    // freezer's request constant stays None, as this owner's contract permits.
    fixture_with_calls(&[int64(true)], &rows, &[(0, true, true)], limit, true)
}
type NOutput = (Option<i64>, Vec<i128>, Vec<i128>);
fn n_oracle(limit: usize) -> Vec<NOutput> {
    let mut groups = BTreeMap::<Option<i64>, Vec<i128>>::new();
    for &(key, value) in N_ROWS {
        let values = groups.entry(key).or_default();
        if let Some(value) = value {
            values.push(value);
        }
    }
    groups
        .into_iter()
        .take(limit)
        .map(|(key, mut values)| {
            values.sort();
            let min = values.iter().copied().take(2).collect();
            let max = values[values.len().saturating_sub(2)..].to_vec();
            (key, min, max)
        })
        .collect()
}
fn n_results(chunks: &[Chunk]) -> Vec<NOutput> {
    let mut result = Vec::new();
    for chunk in chunks {
        let keys = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let read_list = |column: usize, row| {
            let lists = chunk
                .batch
                .column(column)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert!(
                !lists.is_null(row),
                "NULL-only state produces an empty list"
            );
            let DataType::List(item) = lists.data_type() else {
                unreachable!()
            };
            assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
            assert!(item.is_nullable());
            assert_eq!(
                item.metadata().get("nr_logical_type").map(String::as_str),
                Some("largeint")
            );
            let values = lists.value(row);
            let binary = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..binary.len())
                .map(|index| {
                    assert!(!binary.is_null(index));
                    i128::from_be_bytes(binary.value(index).try_into().unwrap())
                })
                .collect::<Vec<_>>()
        };
        for row in 0..chunk.len() {
            result.push((
                (!keys.is_null(row)).then(|| keys.value(row)),
                read_list(1, row),
                read_list(2, row),
            ));
        }
    }
    result.sort_by_key(|(key, _, _)| *key);
    result
}
fn populated_n_consumer(f: &Fixture, tracker: Arc<MemTracker>) -> CompiledGroupedTopNProcessor {
    let factory = CompiledGroupedTopNProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.grouped,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    for call in &factory.plan.calls {
        assert_eq!(
            call.handle.memory_policy(),
            AggregateStateMemoryPolicy::AllocationTracked
        );
    }
    let mut processor = factory.processor();
    processor.set_mem_tracker(tracker);
    let state = RuntimeState::default();
    let mut saw_heap = false;
    for offset in (0..N_ROWS.len()).step_by(3) {
        for chunk in partial_chunks(f, offset, 3) {
            processor.push_chunk(&state, chunk).unwrap();
            let entries = &processor.candidates.as_ref().unwrap().entries;
            assert!(entries.len() <= factory.plan.limit);
            saw_heap |= entries.iter().any(|candidate| {
                candidate
                    .states
                    .iter()
                    .any(|state| state.retained_heap_bytes() > 0)
            });
            assert!(!processor.has_output());
        }
    }
    assert!(saw_heap, "exercise original populated HostVec state");
    processor
}
fn drain(processor: &mut CompiledGroupedTopNProcessor) -> Vec<Chunk> {
    let state = RuntimeState::default();
    processor.set_finishing(&state).unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
        chunks.push(chunk);
    }
    chunks
}

#[test]
fn compiled_grouped_topn_largeint_n_states_evict_and_finalize_all_retained_contributions() {
    for limit in [1, 2, 3] {
        let f = n_fixture(limit);
        let tracker = MemTracker::new_root("grouped-variable-n-state");
        let mut processor = populated_n_consumer(&f, tracker.clone());
        let intermediate = drain(&mut processor);
        let final_tracker = MemTracker::new_root("final-variable-n-state");
        let output = final_chunks_tracked(&f, intermediate, final_tracker.clone());
        assert_eq!(n_results(&output), n_oracle(limit));
        drop(output);
        drop(processor);
        drop(f);
        assert_eq!(tracker.current(), 0);
        assert_eq!(final_tracker.current(), 0);
    }
}

#[test]
fn compiled_grouped_topn_actual_n_intermediate_buffer_keeps_original_stock_until_last_alias() {
    let f = n_fixture(3);
    let tracker = MemTracker::new_root("grouped-n-intermediate-alias");
    let mut processor = populated_n_consumer(&f, tracker.clone());
    let output = drain(&mut processor);
    let populated = output
        .iter()
        .find(|chunk| !chunk.batch.column(0).is_null(0))
        .unwrap();
    let states = populated
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let alias = states.to_data().buffers()[1].clone();
    drop(output);
    drop(processor);
    drop(f);
    assert!(
        tracker.current() > 0,
        "actual Intermediate payload still owns its original custody"
    );
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_actual_n_final_child_keeps_original_stock_until_last_alias() {
    let f = n_fixture(3);
    let grouped_tracker = MemTracker::new_root("grouped-before-n-final-alias");
    let mut processor = populated_n_consumer(&f, grouped_tracker.clone());
    let intermediate = drain(&mut processor);
    let final_tracker = MemTracker::new_root("n-final-child-alias");
    let output = final_chunks_tracked(&f, intermediate, final_tracker.clone());
    assert_eq!(n_results(&output), n_oracle(3));
    let lists = output
        .iter()
        .map(|chunk| {
            chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
        })
        .find(|lists| lists.values().len() > 0)
        .unwrap();
    let child = lists.values().clone();
    let alias = child.to_data().buffers()[0].clone();
    drop(child);
    drop(output);
    drop(processor);
    drop(f);
    assert_eq!(grouped_tracker.current(), 0);
    assert!(
        final_tracker.current() > 0,
        "actual Final child still owns its original custody"
    );
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(final_tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_populated_n_state_drop_cancel_and_host_refusal_release_real_owners() {
    let f = n_fixture(3);
    let tracker = MemTracker::new_root("grouped-n-drop");
    let processor = populated_n_consumer(&f, tracker.clone());
    assert!(tracker.current() > 0);
    drop(processor);
    assert_eq!(
        tracker.current(),
        0,
        "actual state Drop, without cancel flag"
    );

    let tracker = MemTracker::new_root("grouped-n-cancel");
    let mut processor = populated_n_consumer(&f, tracker.clone());
    assert!(tracker.current() > 0);
    processor.cancel();
    assert!(processor.is_finished());
    assert!(!processor.has_output());
    assert_eq!(tracker.current(), 0);
    let cancelled = processor.close().unwrap_err();
    assert_eq!(
        cancelled.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
    );
    assert_eq!(
        processor.pull_chunk(&RuntimeState::default()).unwrap_err(),
        cancelled
    );

    let tracker = MemTracker::new_root("grouped-n-host-refusal");
    let mut processor = populated_n_consumer(&f, tracker.clone());
    tracker.install_limit_once(tracker.current() + 1).unwrap();
    let next = partial_chunks(&f, 0, 3).remove(0);
    let error = processor
        .push_chunk(&RuntimeState::default(), next)
        .unwrap_err();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted)
    );
    assert_eq!(tracker.current(), 0);
    assert!(!processor.has_output());
    assert_eq!(
        processor.pull_chunk(&RuntimeState::default()).unwrap_err(),
        error
    );
    assert_eq!(processor.close().unwrap_err(), error);
}

#[test]
fn compiled_grouped_topn_full_values_pipeline_uses_real_builder_and_joins_before_return() {
    for (limit, asc, first) in [(0, true, true), (1, true, false), (2, false, true)] {
        let f = fixture(limit, asc, first);
        let tracker = MemTracker::new_root("grouped-full-values-pipeline");
        let output = super::super::family_fixture::run_with_tracker(&f.program, tracker.clone());
        assert_eq!(read_final_rows(output), oracle(limit, asc, first));
        assert_eq!(tracker.current(), 0);
    }
    let f = n_fixture(3);
    let tracker = MemTracker::new_root("grouped-full-n-values-pipeline");
    let output = super::super::family_fixture::run_with_tracker(&f.program, tracker.clone());
    assert_eq!(n_results(&output), n_oracle(3));
    drop(output);
    assert_eq!(tracker.current(), 0);
    // Values is a real DOP1 source even with this profile2; no parallel gather
    // qualification is claimed here. That separate lane uses ExchangeSource.
}
