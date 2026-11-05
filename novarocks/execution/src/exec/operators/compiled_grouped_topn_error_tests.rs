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
use crate::exec::pipeline::{shape::StepOperator, test_driver::TestDriver};

fn populated(f: &Fixture, tracker: Arc<MemTracker>) -> GroupedTopNStep {
    let factory = CompiledGroupedTopNProcessorFactory::try_new(
        f.program.clone(),
        f.grouped,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut processor = factory.processor();
    processor.set_mem_tracker(tracker);
    prepare_typed_fixture_operator(&mut processor);
    let state = RuntimeState::default();
    let mut calls = TestDriver::new(&mut processor, &state);
    // Independent Partial processors preserve each original contribution.
    for offset in 0..f.input.len() {
        for chunk in partial_chunks(f, offset, 1) {
            calls.input_without_output(chunk).unwrap();
        }
    }
    assert!(matches!(calls.next(), NextStep::NeedInput));
    drop(calls);
    processor
}

fn drain(processor: &mut GroupedTopNStep) -> Vec<Chunk> {
    let state = RuntimeState::default();
    let mut calls = TestDriver::new(processor, &state);
    calls.signal_eos().unwrap();
    calls.collect_until_finished().unwrap()
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
    let alias = output[1].record_batch().column(0).clone();
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
    let array = output[1].record_batch().column(0).clone();
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
fn compiled_grouped_topn_retained_sum_overflow_is_only_a_final_data_failure() {
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
    use novarocks_failure_contract::FailureProjection;
    assert_eq!(
        error.failure_fact(),
        novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
    );
    assert_eq!(error.detail(), "sum result overflows BIGINT");
    let ExecutionFailureCause::Kernel(KernelFailure::Source(original)) = error.cause() else {
        panic!("SUM must retain its domain-owned source: {error:?}");
    };
    assert_eq!(original.failure_fact(), error.failure_fact());
    let source = std::error::Error::source(original).expect("original SUM domain cause");
    assert_eq!(source.to_string(), "sum result overflows BIGINT");
    drop(error);
    drop(processor);
    drop(f);
    assert_eq!(tracker.current(), 0);
    assert_eq!(final_tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_steps_publish_one_candidate_per_grant_without_prefilling_outputs() {
    use crate::exec::pipeline::{
        driver_step::{DriverStep, TurnWork},
        input::RawInput,
        shape::StepOperator,
        step::{InputResult, NextStep},
    };
    let f = text_fixture(&["a".into(), "z".into()], 2);
    let tracker = MemTracker::new_root("grouped-step-candidate-output");
    let mut processor = populated(&f, tracker.clone());
    let control = RuntimeErrorState::default();
    let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 0);
    processor
        .finish(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .unwrap();
    let ready = processor
        .drive(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .unwrap();
    assert!(ready.output.is_none());
    assert!(matches!(ready.next, NextStep::Runnable));
    let before = tracker.allocated();
    let blocked = processor
        .drive(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .unwrap();
    assert!(blocked.output.is_none());
    assert!(matches!(blocked.next, NextStep::Yield));
    assert_eq!(tracker.allocated(), before);
    assert_eq!(processor.kernel_for_test().emission, Some(0));
    work.begin_turn(1).unwrap();
    let first = processor
        .drive(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .unwrap();
    assert!(matches!(first.next, NextStep::Runnable));
    let first = first.output.expect("first granted candidate");
    assert_eq!(
        array_key(first.record_batch().column(0), 0),
        Some(KeyCell::Text("a".into()))
    );
    let before = tracker.allocated();
    let blocked = processor
        .drive(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .unwrap();
    assert!(blocked.output.is_none());
    assert!(matches!(blocked.next, NextStep::Yield));
    assert_eq!(tracker.allocated(), before);
    assert_eq!(processor.kernel_for_test().emission, Some(1));
    work.begin_turn(1).unwrap();
    let last = processor
        .drive(&mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
        .unwrap();
    assert!(matches!(last.next, NextStep::Finished));
    let last = last.output.expect("last granted candidate");
    assert_eq!(
        array_key(last.record_batch().column(0), 0),
        Some(KeyCell::Text("z".into()))
    );
    assert!(
        processor
            .drive(&mut DriverStep::new(
                &control,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default()
            ))
            .unwrap()
            .output
            .is_none()
    );
    let input = f.input.clone();
    let source = input.columns()[0].clone();
    let InputResult::Rejected {
        input: returned, ..
    } = processor.try_push(
        &mut DriverStep::new(
            &control,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ),
        RawInput::for_test(input),
    )
    else {
        panic!("a completed candidate cursor must return new input");
    };
    assert!(Arc::ptr_eq(
        &source,
        &returned.original_for_test().columns()[0]
    ));
    drop(processor);
    assert!(tracker.current() > 0);
    drop(first);
    assert!(tracker.current() > 0);
    drop(last);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_grouped_topn_later_emission_refusal_keeps_published_prefix_and_discards_remaining_work()
{
    // A real successful two-key run supplies generous observed headroom for
    // the same candidate width, first key and installed owners. This observes
    // host accounting only; it is neither a physical peak nor a byte guard.
    let control_fixture = text_fixture(&["a".into(), "z".into()], 2);
    let control_tracker = MemTracker::new_root("grouped-finish-positive-control");
    let mut control = populated(&control_fixture, control_tracker.clone());
    let before = control_tracker.current();
    let output = drain(&mut control);
    let headroom = control_tracker.peak();
    assert_eq!(output.len(), 2);
    assert_eq!(
        array_key(output[0].record_batch().column(0), 0),
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
        array_key(prefix_output[0].record_batch().column(0), 0),
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
    let mut calls = TestDriver::new(&mut processor, &state);
    calls.signal_eos().unwrap();
    let published = calls
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .expect("first complete candidate");
    assert_eq!(
        array_key(published.record_batch().column(0), 0),
        Some(KeyCell::Text("a".into()))
    );
    let first = calls
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap_err();
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&first);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Resource
    );
    let resource = fact
        .resource()
        .expect("the actual host retains its numerical refusal");
    assert_eq!(resource.limit, Some(limit as u64));
    assert!(resource.requested.unwrap() > 0);
    assert!(resource.committed.unwrap() > limit as u64);
    assert!(
        calls
            .advance_to_boundary()
            .unwrap_err()
            .same_occurrence(&first)
    );
    assert!(calls.operator().kernel_for_test().candidates.is_none());
    assert!(calls.operator().kernel_for_test().emission.is_none());
    assert!(calls.operator().kernel_for_test().instances.is_none());
    assert!(
        tracker.current() > 0,
        "the published candidate still owns its actual output"
    );
    assert_eq!(
        array_key(published.record_batch().column(0), 0),
        Some(KeyCell::Text("a".into()))
    );
    drop(published);
    assert_eq!(
        tracker.current(),
        0,
        "unpublished work dropped; the last published alias exited"
    );
    assert_eq!(
        calls
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap_err(),
        first
    );
    assert_eq!(calls.signal_eos().unwrap_err(), first);
    calls.operator_mut().cancel();
    calls.operator_mut().close().unwrap();
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
    prepare_typed_fixture_operator(&mut processor);
    let state = RuntimeState::default();
    let mut calls = TestDriver::new(&mut processor, &state);
    for chunk in partial_chunks(&f, 0, 1) {
        calls.input_without_output(chunk).unwrap();
    }
    let chunks = partial_chunks(&f, 1, 1);
    assert_eq!(chunks.len(), 1);
    let first = calls
        .input_without_output(chunks.into_iter().next().unwrap())
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
    assert!(
        calls
            .advance_to_boundary()
            .unwrap_err()
            .same_occurrence(&first)
    );
    assert!(calls.operator().kernel_for_test().candidates.is_none());
    assert!(calls.operator().kernel_for_test().emission.is_none());
    assert!(calls.operator().kernel_for_test().instances.is_none());
    assert_eq!(
        calls
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap_err(),
        first
    );
    assert_eq!(calls.signal_eos().unwrap_err(), first);
    calls.operator_mut().cancel();
    calls.operator_mut().close().unwrap();
    drop(calls);
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
    assert_eq!(state.error(), Some(first.clone()));
    drop(first);
    assert!(
        tracker.current() > 0,
        "early failure observation retains the original finite Data backing"
    );
    let observed = state.error().unwrap();
    let ExecutionFailureCause::InvocationData(data) = observed.cause() else {
        panic!("the early report lost its original Data cause");
    };
    assert_eq!(
        data.message(),
        "merge aggregate state: min_n/max_n limit mismatch while merging states: 2 vs 3"
    );
    drop(observed);
    drop(state);
    assert_eq!(
        tracker.current(),
        0,
        "the last error owner releases its actual finite backing"
    );
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
        let kernel = processor.kernel_for_test_mut();
        let set = kernel.candidates.as_mut().unwrap();
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
                let refusal = set
                    .entries
                    .try_reserve_exact(capacity)
                    .expect_err("replacement must coexist with the old block");
                assert_eq!(set.entries.capacity(), capacity);
                assert_eq!(set.entries.len(), len);
                KernelFailure::from_domain(
                    novarocks_functions::allocation_failure::AllocationRefusal::new(
                        "grouped test original container replacement",
                        refusal,
                    ),
                )
            }
            "key" => {
                let key = &mut set.entries[0].key;
                let capacity = key.capacity();
                let len = key.len();
                let limit = before - i64::try_from(capacity).unwrap()
                    + i64::try_from(len + capacity).unwrap();
                tracker.install_limit_once(limit).unwrap();
                let refusal = key
                    .try_reserve_exact(capacity)
                    .expect_err("replacement must coexist with the old key");
                assert_eq!(key.capacity(), capacity);
                assert_eq!(key.len(), len);
                KernelFailure::from_domain(
                    novarocks_functions::allocation_failure::AllocationRefusal::new(
                        "grouped test original container replacement",
                        refusal,
                    ),
                )
            }
            "inline-state" => {
                let handle = kernel.plan.calls[0].handle.clone();
                let layout = handle.state_layout().pad_to_align();
                assert!(layout.size() > 0);
                tracker
                    .install_limit_once(before + i64::try_from(layout.size()).unwrap() - 1)
                    .unwrap();
                OwnedAggregateState::try_new(handle, set.host.clone(), &kernel.control).unwrap_err()
            }
            _ => unreachable!(),
        };
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&failure);
        assert_eq!(
            fact.class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        let resource = fact
            .resource()
            .expect("the actual refusal retains available fields");
        assert!(resource.requested.unwrap() > 0);
        if storage == "inline-state" {
            assert_eq!(resource.limit, Some(tracker.limit() as u64));
        } else {
            assert_eq!(
                resource.limit, None,
                "the erased allocator refusal has no quota identity"
            );
        }
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
        let first = kernel.latch::<()>(Err(failure.into())).unwrap_err();
        assert!(processor.drive_for_test(&RuntimeState::default()).is_err());
        assert_eq!(
            tracker.current(),
            0,
            "real storage and typed states destroyed"
        );
        assert_eq!(
            processor
                .drive_for_test(&RuntimeState::default())
                .map(|step| step.output)
                .unwrap_err(),
            first
        );
        processor.close().unwrap();
    }
}
