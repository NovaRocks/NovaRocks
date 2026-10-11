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

use super::*;
use crate::{CompilePhase, PureCompileControl, owned_resources::copy::copy_string};
use std::sync::Mutex;

#[test]
fn partitioned_collision_vecs_cover_cumulative_requests_for_all_small_partitions() {
    fn check_partition(parts: &[usize]) {
        let total = parts.iter().sum();
        let facts = original_partitioned_fresh_push_request_bound::<(&str, &str)>(total).unwrap();
        let mut requests = 0;
        let mut bytes = 0;
        for count in parts {
            let mut bucket = Vec::new();
            for _ in 0..*count {
                let previous = bucket.capacity();
                bucket.push(("key", "value"));
                if bucket.capacity() != previous {
                    requests += 1;
                    bytes += bucket.capacity() * 32;
                }
            }
        }
        assert!(requests <= facts.allocation_requests_upper_bound);
        assert!(bytes <= facts.request_bytes_upper_bound);
        assert_eq!(facts.request_bytes_upper_bound, 128 * total);
        assert_eq!(facts.request_alignment, 8);
    }
    fn partitions(remaining: usize, parts: &mut Vec<usize>) {
        if remaining == 0 {
            check_partition(parts);
        }
        for count in 1..=remaining {
            parts.push(count);
            partitions(remaining - count, parts);
            parts.pop();
        }
    }
    assert_eq!(std::mem::size_of::<(&str, &str)>(), 32);
    for total in 0..=12 {
        partitions(total, &mut Vec::new());
    }
    // Large collision/singleton extremes exercise later doubling extents.
    check_partition(&[4096]);
    check_partition(&[1; 4096]);
    check_partition(&[1, 3, 5, 9, 17, 33, 65, 129, 257, 513]);
}

#[test]
fn partitioned_push_bound_keeps_original_minimum_zst_and_overflow_rules() {
    assert_eq!(
        original_partitioned_fresh_push_request_bound::<u8>(1).unwrap(),
        PartitionedFreshPushFacts {
            allocation_requests_upper_bound: 1,
            request_bytes_upper_bound: 8,
            request_alignment: 1,
        }
    );
    let large = original_partitioned_fresh_push_request_bound::<[u8; 1025]>(1).unwrap();
    assert_eq!(large.request_bytes_upper_bound, 4 * 1025);
    assert_eq!(large.allocation_requests_upper_bound, 1);
    let zero = original_partitioned_fresh_push_request_bound::<()>(usize::MAX).unwrap();
    assert_eq!(zero.allocation_requests_upper_bound, 0);
    assert_eq!(zero.request_bytes_upper_bound, 0);
    assert_eq!(
        original_partitioned_fresh_push_request_bound::<(&str, &str)>(usize::MAX),
        Err(resource())
    );
}

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    failure: Option<(usize, CompileControlError)>,
}
impl Control {
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::default(),
            failure: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if let Some((failure_at, cause)) = self.failure
            && at == failure_at
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn reserve_empty<T: Copy + std::fmt::Debug + PartialEq>(
    expected_capacity: usize,
    expected_bytes: usize,
    value: T,
) {
    let mut values = Vec::new();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut capture = None;
    reserve_for_push_in::<_, ControlResourceError>(
        &mut values,
        &mut |facts| {
            capture = Some(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    let facts = capture.unwrap();
    assert_eq!(facts.next_len, 1);
    assert_eq!(facts.requested_capacity, expected_capacity);
    assert_eq!(facts.old_backing, None);
    assert_eq!(
        facts.requested_backing,
        Some(Layout::from_size_align(expected_bytes, Layout::new::<T>().align()).unwrap())
    );
    // The reserve leaf has not appended, entered a second scope or finished.
    assert!(values.is_empty());
    assert_eq!(values.capacity(), expected_capacity);
    assert_eq!(control.trace(), [0, 0, 1]);
    values.push(value);
    work.step().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], value);
    work.finish().unwrap();
    assert_eq!(control.trace(), [0, 0, 1, 1]);
}

#[test]
fn actual_byte_word_and_large_elements_match_independent_request_layouts() {
    reserve_empty(8, 8, 0xa5_u8);
    reserve_empty(4, 16, 0x1234_5678_u32);
    reserve_empty(1, 1025, [0x5a_u8; 1025]);
}

#[test]
fn irregular_capacity_and_empty_spare_preserve_contents_and_backing() {
    let mut values = Vec::<u32>::new();
    values.try_reserve_exact(3).unwrap();
    values.extend([11, 22, 33]);
    assert_eq!(values.capacity(), 3);
    let facts = push_growth(&values).unwrap();
    assert_eq!(facts.next_len, 4);
    assert_eq!(facts.requested_capacity, 6);
    assert_eq!(facts.old_backing, Some(Layout::array::<u32>(3).unwrap()));
    assert_eq!(
        facts.requested_backing,
        Some(Layout::array::<u32>(6).unwrap())
    );
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    reserve_for_push_in::<_, ControlResourceError>(
        &mut values,
        &mut |captured| {
            assert_eq!(*captured, facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(values, [11, 22, 33]);
    assert_eq!(values.capacity(), 6);
    values.clear();
    let empty = push_growth(&values).unwrap();
    assert_eq!(empty.next_len, 1);
    assert_eq!(empty.requested_capacity, 6);
    assert_eq!(empty.old_backing, Some(Layout::array::<u32>(6).unwrap()));
    assert_eq!(empty.requested_backing, None);
    let trace = control.trace();
    reserve_for_push_in::<_, ControlResourceError>(
        &mut values,
        &mut |captured| {
            assert_eq!(*captured, empty);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(control.trace(), trace);
}

#[test]
fn actual_zero_sized_and_spare_pushes_request_nothing_and_emit_no_leaf_callbacks() {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut zst = Vec::<()>::new();
    let mut captures = 0;
    for expected_len in 1..=3 {
        reserve_for_push_in::<_, ControlResourceError>(
            &mut zst,
            &mut |facts| {
                captures += 1;
                assert_eq!(facts.next_len, expected_len);
                assert_eq!(facts.requested_capacity, usize::MAX);
                assert_eq!(facts.old_backing, None);
                assert_eq!(facts.requested_backing, None);
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        zst.push(());
        work.step().unwrap();
    }
    assert_eq!(captures, 3);
    assert_eq!(control.trace(), [0]);
    assert_eq!(zst.len(), 3);
    work.finish().unwrap();
    assert_eq!(control.trace(), [0, 3]);

    let mut spare = Vec::with_capacity(5);
    spare.extend([1_u8, 2, 3, 4]);
    let facts = push_growth(&spare).unwrap();
    assert_eq!(facts.next_len, 5);
    assert_eq!(facts.old_backing, Some(Layout::array::<u8>(5).unwrap()));
    assert_eq!(facts.requested_backing, None);
}

#[test]
fn private_max_geometry_refuses_numeric_overflow_without_huge_vectors() {
    let refused = Err(ControlResourceError::Control(
        CompileControlError::ResourceExhausted,
    ));
    assert_eq!(push_geometry::<()>(usize::MAX, usize::MAX), refused);
    assert_eq!(push_geometry::<u8>(usize::MAX, usize::MAX), refused);
    // The old layouts are individually representable; only their next
    // original doubled requests overflow the Layout/isize domain.
    assert!(Layout::array::<u8>(isize::MAX as usize).is_ok());
    assert_eq!(
        push_geometry::<u8>(isize::MAX as usize, isize::MAX as usize),
        refused
    );
    let words = (isize::MAX as usize) / 2;
    assert!(Layout::array::<u16>(words).is_ok());
    assert_eq!(push_geometry::<u16>(words, words), refused);
    assert!(matches!(
        push_geometry::<u32>(1, 0),
        Err(ControlResourceError::SourceModel(_))
    ));
    assert!(matches!(
        push_geometry::<()>(0, 0),
        Err(ControlResourceError::SourceModel(_))
    ));
}

fn append_in_scope(control: &Control) -> Result<Vec<u32>, ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut values = Vec::new();
    for value in [7, 9, 11] {
        reserve_for_push_in::<_, ControlResourceError>(
            &mut values,
            &mut |facts| {
                if let Some(layout) = facts.requested_backing {
                    assert_eq!(layout.size(), 16);
                }
                Ok(())
            },
            &mut work,
        )?;
        values.push(value);
        work.step()?;
    }
    work.finish()?;
    Ok(values)
}

#[test]
fn every_actual_small_callback_preserves_three_primary_causes_and_caller_tail() {
    let baseline = Control::default();
    assert_eq!(append_in_scope(&baseline).unwrap(), [7, 9, 11]);
    let trace = baseline.trace();
    assert_eq!(trace, [0, 0, 1, 3]);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(at, cause);
            assert_eq!(append_in_scope(&control), Err(cause.into()));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn real_copy_pending_255_parent_one_under_wins_before_later_observation() {
    for cause in CAUSES {
        // Real shared byte copying creates the caller's pending work; this
        // is not a Kahn graph fixture or synthetic meter preloading.
        let control = Control::refusing(3, cause);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let source = "x".repeat(255);
        let copied = copy_string::<ControlResourceError>(&source, &mut work).unwrap();
        assert_eq!(copied.as_bytes(), [b'x'; 255]);
        assert_ne!(copied.as_ptr(), source.as_ptr());
        assert_eq!(control.trace(), [0, 0, 1]);
        let mut values = Vec::<u32>::new();
        let outcome = reserve_for_push_in::<_, ControlResourceError>(
            &mut values,
            &mut |facts| {
                assert_eq!(facts.requested_capacity, 4);
                assert_eq!(facts.requested_backing.unwrap().size(), 16);
                let parent_limit = 15;
                if facts.requested_backing.unwrap().size() > parent_limit {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                Ok(())
            },
            &mut work,
        );
        assert_eq!(outcome, Err(CompileControlError::ResourceExhausted.into()));
        assert_eq!(control.trace(), [0, 0, 1]);
        assert_eq!(values.capacity(), 0);
    }
}

#[test]
fn hand_cumulative_requests_exact_and_one_under_preserve_actual_prefix_and_hook_errors() {
    for limit in [112, 111] {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut values = Vec::<u32>::new();
        let mut requests = Vec::new();
        let mut total = 0;
        for value in 0..9 {
            let outcome = reserve_for_push_in::<_, ControlResourceError>(
                &mut values,
                &mut |facts| {
                    if let Some(layout) = facts.requested_backing {
                        requests.push(layout.size());
                        total += layout.size();
                    }
                    if total > limit {
                        return Err(CompileControlError::ResourceExhausted.into());
                    }
                    Ok(())
                },
                &mut work,
            );
            if value == 8 && limit == 111 {
                assert_eq!(outcome, Err(CompileControlError::ResourceExhausted.into()));
                assert_eq!(values, [0, 1, 2, 3, 4, 5, 6, 7]);
                assert_eq!(values.capacity(), 8);
            } else {
                outcome.unwrap();
                values.push(value);
                work.step().unwrap();
            }
        }
        // Independent hand invoice: three full new buffers, not deltas.
        assert_eq!(requests, [16, 32, 64]);
        assert_eq!(total, 112);
        if limit == 112 {
            assert_eq!(values, [0, 1, 2, 3, 4, 5, 6, 7, 8]);
            assert_eq!(values.capacity(), 16);
            assert_eq!(values.len(), 9);
        }
    }

    let control = Control::refusing(1, CompileControlError::Cancelled);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut values = Vec::<u32>::new();
    let ordinary = ControlResourceError::SourceModel("actual caller source refusal");
    assert_eq!(
        reserve_for_push_in::<_, ControlResourceError>(
            &mut values,
            &mut |_| Err(ordinary),
            &mut work,
        ),
        Err(ordinary)
    );
    assert_eq!(control.trace(), [0]);
    assert_eq!(values.capacity(), 0);
}

#[test]
fn boxing_equal_capacity_reuses_backing_and_spare_trim_preserves_full_content() {
    for capacity in [3, 6] {
        let mut source = Vec::<u32>::new();
        source.try_reserve_exact(capacity).unwrap();
        source.extend([31, 17, 99]);
        assert_eq!(source.capacity(), capacity);
        let pointer = source.as_ptr();
        let old = Layout::from_size_align(capacity * 4, 4).unwrap();
        let result = Layout::from_size_align(12, 4).unwrap();
        let expected = VecBoxFacts {
            old_backing: Some(old),
            requested_backing: (capacity == 6).then_some(result),
            result_backing: Some(result),
        };
        assert_eq!(box_facts(&source).unwrap(), expected);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut captured = None;
        let output = boxed_slice_in::<_, ControlResourceError>(
            source,
            &mut |facts| {
                captured = Some(*facts);
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(captured, Some(expected));
        assert_eq!(*output, [31, 17, 99]);
        if capacity == 3 {
            // This branch performs no shrink operation, so ownership moves
            // without a new allocation or element copy.
            assert_eq!(output.as_ptr(), pointer);
        }
        // Spare trimming may legitimately move the allocation.
        assert_eq!(output.into_vec().capacity(), 3);
        assert_eq!(control.trace(), [0, 0, 1]);
        work.finish().unwrap();
        assert_eq!(control.trace(), [0, 0, 1, 0]);
    }
}

#[test]
fn boxing_empty_free_and_zero_sized_max_have_no_zero_byte_request() {
    for capacity in [0, 7] {
        let mut source = Vec::<u8>::new();
        source.try_reserve_exact(capacity).unwrap();
        let expected = VecBoxFacts {
            old_backing: (capacity == 7).then_some(Layout::array::<u8>(7).unwrap()),
            requested_backing: None,
            result_backing: None,
        };
        assert_eq!(box_facts(&source).unwrap(), expected);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let output = boxed_slice_in::<_, ControlResourceError>(
            source,
            &mut |facts| {
                assert_eq!(*facts, expected);
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert!(output.is_empty());
        assert_eq!(output.into_vec().capacity(), 0);
        assert_eq!(control.trace(), [0, 0, 1]);
    }
    let no_backing = VecBoxFacts {
        old_backing: None,
        requested_backing: None,
        result_backing: None,
    };
    // A pure boundary oracle: boxing does not append, so a legal ZST lenMAX
    // must not inherit the push author's checked len+1 rejection.
    assert_eq!(box_geometry::<()>(usize::MAX, usize::MAX), Ok(no_backing));
    for source in [Vec::<()>::new(), vec![(); 3]] {
        let len = source.len();
        assert_eq!(box_facts(&source), Ok(no_backing));
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let output = boxed_slice_in::<_, ControlResourceError>(
            source,
            &mut |facts| {
                assert_eq!(*facts, no_backing);
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(output.len(), len);
        assert_eq!(control.trace(), [0, 0, 1]);
    }
}

#[test]
fn boxing_private_geometry_preserves_source_errors_and_typed_layout_overflow() {
    assert_eq!(
        box_geometry::<u16>(0, isize::MAX as usize),
        Err(CompileControlError::ResourceExhausted.into())
    );
    assert!(matches!(
        box_geometry::<u32>(2, 1),
        Err(ControlResourceError::SourceModel(_))
    ));
    assert!(matches!(
        box_geometry::<()>(0, 0),
        Err(ControlResourceError::SourceModel(_))
    ));
}

fn boxing_in_scope(spare: bool, control: &Control) -> Result<Box<[u32]>, ControlResourceError> {
    let mut source = Vec::new();
    source.try_reserve_exact(if spare { 6 } else { 3 }).unwrap();
    source.extend([31, 17, 99]);
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let output = boxed_slice_in::<_, ControlResourceError>(
        source,
        &mut |facts| {
            assert_eq!(facts.result_backing.unwrap().size(), 12);
            assert_eq!(facts.requested_backing.is_some(), spare);
            Ok(())
        },
        &mut work,
    )?;
    work.finish()?;
    Ok(output)
}

#[test]
fn boxing_each_actual_callback_preserves_three_causes_and_original_caller_scope() {
    for spare in [false, true] {
        let baseline = Control::default();
        assert_eq!(*boxing_in_scope(spare, &baseline).unwrap(), [31, 17, 99]);
        let trace = baseline.trace();
        assert_eq!(trace, [0, 0, 1, 0]);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control::refusing(at, cause);
                assert_eq!(boxing_in_scope(spare, &control), Err(cause.into()));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn boxing_known_trim_request_one_under_precedes_real_pending_copy_observation() {
    for limit in [12, 11] {
        for cause in CAUSES {
            let control = if limit == 11 {
                Control::refusing(3, cause)
            } else {
                Control::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let spelling = "x".repeat(255);
            let copied = copy_string::<ControlResourceError>(&spelling, &mut work).unwrap();
            assert_eq!(copied.as_bytes(), [b'x'; 255]);
            assert_ne!(copied.as_ptr(), spelling.as_ptr());
            assert_eq!(control.trace(), [0, 0, 1]);
            let mut source = Vec::new();
            source.try_reserve_exact(6).unwrap();
            source.extend([31_u32, 17, 99]);
            let mut captured = false;
            let result = boxed_slice_in::<_, ControlResourceError>(
                source,
                &mut |facts| {
                    captured = true;
                    assert_eq!(facts.old_backing.unwrap().size(), 24);
                    assert_eq!(facts.result_backing.unwrap().size(), 12);
                    let requested = facts.requested_backing.unwrap().size();
                    assert_eq!(requested, 12);
                    if requested > limit {
                        return Err(CompileControlError::ResourceExhausted.into());
                    }
                    Ok(())
                },
                &mut work,
            );
            assert!(captured);
            if limit == 11 {
                assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
                assert_eq!(control.trace(), [0, 0, 1]);
            } else {
                assert_eq!(*result.unwrap(), [31, 17, 99]);
                assert_eq!(control.trace(), [0, 0, 1, 255, 1]);
            }
        }
    }
}

struct DropValue {
    value: u32,
    drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for DropValue {
    fn drop(&mut self) {
        self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn boxing_closed_elements_drop_once_on_parent_control_errors_and_final_owner_drop() {
    use std::sync::{Arc, atomic::Ordering};
    // This closed fixture proves destructor occurrences, not arbitrary
    // element Drop cost, allocator work or a cleanup admission grant.
    for failure in [None, Some(1), Some(2)] {
        for cause in CAUSES {
            let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut source = Vec::with_capacity(4);
            for value in [31, 17] {
                source.push(DropValue {
                    value,
                    drops: drops.clone(),
                });
            }
            let control = failure.map_or_else(Control::default, |at| Control::refusing(at, cause));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let result =
                boxed_slice_in::<_, ControlResourceError>(source, &mut |_| Ok(()), &mut work);
            if let Some(at) = failure {
                assert!(
                    matches!(result, Err(ControlResourceError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), [0, 0, 1][..=at]);
                assert_eq!(drops.load(Ordering::SeqCst), 2);
            } else {
                let output = result.unwrap();
                assert_eq!(output[0].value, 31);
                assert_eq!(output[1].value, 17);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(output);
                assert_eq!(drops.load(Ordering::SeqCst), 2);
            }
        }
    }

    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = vec![DropValue {
        value: 7,
        drops: drops.clone(),
    }];
    let control = Control::refusing(1, CompileControlError::Cancelled);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let ordinary = ControlResourceError::SourceModel("actual boxing caller source refusal");
    let result =
        boxed_slice_in::<_, ControlResourceError>(source, &mut |_| Err(ordinary), &mut work);
    assert!(matches!(result, Err(error) if error == ordinary));
    assert_eq!(control.trace(), [0]);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
