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
use crate::{
    exec::{
        hash_table::allocation::AggregateAllocator,
        pipeline::operator::{
            InputConstructionOwner, input_construction::try_runtime_input_construction_factory,
        },
    },
    runtime::{mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host},
};
use arrow::{
    array::{Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray},
    datatypes::{DataType, Schema},
};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(super) struct Control(AtomicBool);
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        if self.0.load(Ordering::SeqCst) {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("local join adapter must not wait")
    }
}
pub(super) fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("join K adapter fixture");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    )
    .unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}
pub(super) fn ports<'a>(
    owner: &'a dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
    control: &'a dyn KernelEvaluationControl,
) -> BulkWork<'a> {
    BulkWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker.clone()),
        control,
    }
}
fn build(
    pages: &[Vec<ArrayRef>],
    bindings: &[FunctionValueType],
    safe: &[bool],
    purpose: JoinHashMapBuildPurpose,
    work: &BulkWork<'_>,
) -> JoinHashMap {
    let mut capture = JoinKeyBuild::new(bindings, safe, purpose, work).unwrap();
    let mut context = ValueConversionContext::default();
    for arrays in pages {
        capture
            .push(arrays, bindings, arrays[0].len(), &mut context, work)
            .unwrap();
    }
    let result = capture.finish(work).unwrap();
    context.clear();
    result
}
fn pairs(
    map: &JoinHashMap,
    arrays: &[ArrayRef],
    bindings: &[FunctionValueType],
    work: &BulkWork<'_>,
    page: usize,
) -> (Vec<(u32, u32)>, Vec<usize>) {
    let groups = map
        .lookup_group_ids_arrays(
            arrays,
            bindings,
            arrays[0].len(),
            &mut ValueConversionContext::default(),
            work,
        )
        .unwrap();
    let mut cursor = JoinPairCursor::new(groups, work.owner.storage_lifetime());
    let mut output = Vec::new();
    let mut lengths = Vec::new();
    while !cursor.is_complete() {
        let selection = cursor
            .next_page(map, NonZeroUsize::new(page).unwrap(), work.control)
            .unwrap();
        lengths.push(selection.len());
        output.extend(selection.probe.into_iter().zip(selection.build));
    }
    (output, lengths)
}

#[test]
fn join_k_duplicate_pages_preserve_original_build_rows_and_finite_pair_cursor() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Int32, true);
    let map = build(
        &[
            vec![Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(1),
                None,
            ]))],
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(4), Some(2)]))],
        ],
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    let probe: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![
        Some(1),
        None,
        Some(3),
        Some(2),
    ]))];
    let (actual, lengths) = pairs(&map, &probe, std::slice::from_ref(&binding), &work, 2);
    assert_eq!(actual, vec![(0, 4), (0, 2), (0, 0), (3, 6), (3, 1)]);
    assert_eq!(lengths, vec![2, 2, 1]);
    let (mask, stats) = map
        .search_membership_arrays(
            &probe,
            &[binding],
            4,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert_eq!(mask.as_slice(), &[true, false, false, true]);
    assert_eq!((stats.lookup_hit_rows, stats.lookup_miss_rows), (2, 2));
}

#[test]
fn join_k_dense_integer_uses_original_direct_and_presence_never_builds_row_topology() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Int64, true);
    let input: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(
        (0..2048).map(|i| Some(i % 32 - 16)).collect::<Vec<_>>(),
    ))];
    let rows = build(
        std::slice::from_ref(&input),
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    let set = build(
        &[input],
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::PresenceOnly,
        &work,
    );
    assert!(rows.view.is_direct_for_test());
    assert!(set.view.is_direct_for_test());
    assert!(set.groups.is_none());
    let probe: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
        Some(-16),
        Some(15),
        Some(16),
        None,
    ]))];
    let (mask, stats) = set
        .search_membership_arrays(
            &probe,
            std::slice::from_ref(&binding),
            4,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert_eq!(mask.as_slice(), &[true, true, false, false]);
    assert_eq!((stats.lookup_hit_rows, stats.lookup_miss_rows), (2, 2));
    assert!(
        set.lookup_group_ids_arrays(
            &probe,
            &[binding],
            4,
            &mut ValueConversionContext::default(),
            &work
        )
        .is_err()
    );
    let mut cursor = JoinPairCursor::new(vec![], owner.storage_lifetime());
    let error = cursor
        .next_page(&set, NonZeroUsize::new(1).unwrap(), work.control)
        .unwrap_err();
    assert_eq!(
        error.failure_fact(),
        JoinCursorFailure::PresenceOnly.failure_fact()
    );
}

#[test]
fn join_k_sparse_full_i64_domain_has_no_private_range_cap_and_preserves_null_safe_class() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Int64, true);
    let map = build(
        &[vec![Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            Some(i64::MIN),
        ]))]],
        std::slice::from_ref(&binding),
        &[true],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    assert!(!map.view.is_direct_for_test());
    let probe: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
        None,
        Some(i64::MIN),
        Some(0),
        Some(i64::MAX),
    ]))];
    assert_eq!(
        pairs(&map, &probe, &[binding], &work, 1).0,
        vec![(0, 1), (1, 3), (1, 0), (3, 2)]
    );
}

#[test]
fn join_k_string_offsets_views_and_large_offsets_share_signed_domain_without_re_evaluation() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Utf8, true);
    let mut capture = JoinKeyBuild::new(
        std::slice::from_ref(&binding),
        &[true],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    )
    .unwrap();
    let mut context = ValueConversionContext::default();
    let first: ArrayRef = Arc::new(StringArray::from(vec![
        Some("long-prefix-key-123456789"),
        Some("b"),
        None,
    ]));
    let weak = Arc::downgrade(&first);
    capture
        .push(&[first], &[binding], 3, &mut context, &work)
        .unwrap();
    capture
        .push(
            &[Arc::new(StringViewArray::from(vec![
                Some("b"),
                Some("long-prefix-key-123456789"),
            ]))],
            &[FunctionValueType::new(DataType::Utf8View, true)],
            2,
            &mut context,
            &work,
        )
        .unwrap();
    let map = capture.finish(&work).unwrap();
    context.clear();
    assert!(weak.upgrade().is_none());
    let probe: Vec<ArrayRef> = vec![Arc::new(LargeStringArray::from(vec![
        Some("b"),
        None,
        Some("long-prefix-key-123456789"),
        Some("long-prefix-key-123456780"),
    ]))];
    assert_eq!(
        pairs(
            &map,
            &probe,
            &[FunctionValueType::new(DataType::LargeUtf8, true)],
            &work,
            2
        )
        .0,
        vec![(0, 3), (0, 1), (1, 2), (2, 4), (2, 0)]
    );
}

#[test]
fn join_k_composite_mixed_null_policy_keeps_physical_rows() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let bindings = vec![
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let map = build(
        &[vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(1), None, Some(1)])),
            Arc::new(StringArray::from(vec![None, Some("x"), None, None])),
        ]],
        &bindings,
        &[false, true],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    let probe: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1), None, Some(1)])),
        Arc::new(StringArray::from(vec![None, None, Some("x")])),
    ];
    assert_eq!(
        pairs(&map, &probe, &bindings, &work, 2).0,
        vec![(0, 3), (0, 0), (2, 1)]
    );
}

#[test]
fn join_k_empty_input_probe_and_all_unmatchable_keep_original_shape_contract() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Int32, true);
    let empty = build(
        &[],
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    assert!(empty.is_empty());
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![Some(1), None]))];
    assert!(
        pairs(&empty, &arrays, std::slice::from_ref(&binding), &work, 2)
            .0
            .is_empty()
    );
    let nulls = build(
        &[vec![Arc::new(Int32Array::from(vec![None, None]))]],
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    assert!(!nulls.is_empty());
    assert!(
        pairs(&nulls, &arrays, std::slice::from_ref(&binding), &work, 1)
            .0
            .is_empty()
    );
    let empty_probe: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(Vec::<Option<i32>>::new()))];
    let (mask, stats) = nulls
        .search_membership_arrays(
            &empty_probe,
            std::slice::from_ref(&binding),
            0,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert!(mask.as_slice().is_empty());
    assert_eq!((stats.lookup_hit_rows, stats.lookup_miss_rows), (0, 0));
    assert!(
        nulls
            .lookup_group_ids_arrays(
                &arrays,
                &[binding],
                3,
                &mut ValueConversionContext::default(),
                &work
            )
            .is_err()
    );
}

#[test]
fn join_k_mutating_shape_failure_latches_and_readonly_stop_preserves_index() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Int32, true);
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![Some(7)]))];
    let mut capture = JoinKeyBuild::new(
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    )
    .unwrap();
    let first = capture
        .push(
            &arrays,
            std::slice::from_ref(&binding),
            2,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap_err();
    let again = capture
        .push(
            &arrays,
            std::slice::from_ref(&binding),
            1,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap_err();
    assert_eq!(first.failure_fact(), again.failure_fact());
    assert!(capture.finish(&work).is_err());
    let map = build(
        std::slice::from_ref(&arrays),
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    control.0.store(true, Ordering::SeqCst);
    let error = map
        .lookup_group_ids_arrays(
            &arrays,
            std::slice::from_ref(&binding),
            1,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap_err();
    assert_eq!(
        error.failure_fact(),
        ExecutionFailure::from(KernelFailure::DeadlineExceeded).failure_fact()
    );
    control.0.store(false, Ordering::SeqCst);
    assert_eq!(pairs(&map, &arrays, &[binding], &work, 1).0, vec![(0, 0)]);
}

#[test]
fn join_k_true_tracker_refusal_refunds_all_owned_backing_on_exit() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let binding = FunctionValueType::new(DataType::Int32, true);
    let baseline = tracker.current();
    // This is the real creator guard, not another allocation authority.
    let creator_hold = owner.storage_lifetime();
    {
        let work = ports(owner.as_ref(), &tracker, &control);
        let mut capture = JoinKeyBuild::new(
            std::slice::from_ref(&binding),
            &[false],
            JoinHashMapBuildPurpose::RowMatches,
            &work,
        )
        .unwrap();
        capture
            .push(
                &[Arc::new(Int32Array::from((0..1024).collect::<Vec<_>>()))],
                &[binding],
                1024,
                &mut ValueConversionContext::default(),
                &work,
            )
            .unwrap();
        tracker.install_limit_once(tracker.current() + 1).unwrap();
        let error = match capture.finish(&work) {
            Ok(_) => panic!("real original tracker must refuse construction growth"),
            Err(error) => error,
        };
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    }
    // Owner requests are accumulated by this scope's original CreatorGuard.
    // They remain charged until its last storage alias actually exits.
    assert!(tracker.current() > baseline);
    drop(owner);
    assert!(tracker.current() > 0);
    drop(creator_hold);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn join_k_dictionary_bottom_null_summary_and_probe_nulls_use_original_prepared_facts() {
    use arrow::{array::DictionaryArray, datatypes::Int32Type};
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Utf8, true);
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None, Some(2)]),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        )
        .unwrap(),
    );
    let mut capture = JoinKeyBuild::new(
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    )
    .unwrap();
    let mut facts = Vec::new();
    capture
        .push_observed(
            &[array],
            std::slice::from_ref(&binding),
            4,
            &mut ValueConversionContext::default(),
            &work,
            &mut |row, has_null, participates| {
                facts.push((row, has_null, participates));
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        facts,
        vec![
            (0, false, true),
            (1, true, false),
            (2, true, false),
            (3, false, true)
        ]
    );
    let map = capture.finish(&work).unwrap();
    let probe: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), Some(0), None, Some(2)]),
            Arc::new(StringArray::from(vec![Some("b"), None, Some("a")])),
        )
        .unwrap(),
    );
    let input = map
        .input(
            &[probe],
            &[binding],
            4,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert_eq!(
        (0..4)
            .map(|i| input.row_has_null_observed(i, &|| Ok(())).unwrap())
            .collect::<Vec<_>>(),
        vec![true, false, true, false]
    );
    let prepared =
        PreparedKeyBatch::from_captured(map.spec(), &input, work.allocator.clone(), &|| Ok(()))
            .unwrap();
    assert_eq!(
        map.membership(&prepared, &work).unwrap().0.as_slice(),
        &[false, true, false, true]
    );
}

#[test]
fn join_k_nested_keys_keep_child_nulls_and_root_null_class_distinct() {
    use arrow::array::{Int32Builder, ListBuilder};
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let make = || {
        let mut values = ListBuilder::new(Int32Builder::new());
        values.values().append_value(1);
        values.values().append_null();
        values.append(true);
        values.values().append_value(2);
        values.append(true);
        values.append(false);
        values.values().append_value(1);
        values.values().append_null();
        values.append(true);
        Arc::new(values.finish()) as ArrayRef
    };
    let input = make();
    let binding = FunctionValueType::new(input.data_type().clone(), true);
    let map = build(
        &[vec![input]],
        std::slice::from_ref(&binding),
        &[true],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    );
    assert_eq!(
        pairs(&map, &[make()], &[binding], &work, 2).0,
        vec![(0, 3), (0, 0), (1, 1), (2, 2), (3, 3), (3, 0)]
    );
}

#[test]
fn join_k_required_null_keeps_original_typed_rejection_and_latched_cause() {
    let (owner, tracker) = fixture();
    let control = Control::default();
    let work = ports(owner.as_ref(), &tracker, &control);
    let binding = FunctionValueType::new(DataType::Int32, false);
    let mut capture = JoinKeyBuild::new(
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &work,
    )
    .unwrap();
    let error = capture
        .push(
            &[Arc::new(Int32Array::from(vec![None]))],
            std::slice::from_ref(&binding),
            1,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap_err();
    assert_eq!(
        error.failure_fact(),
        novarocks_value_arrays::ValueError::Invariant(
            novarocks_value_arrays::ValueViolation::RequiredNull
        )
        .failure_fact()
    );
    let again = capture
        .push(
            &[Arc::new(Int32Array::from(vec![Some(1)]))],
            &[binding],
            1,
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap_err();
    assert_eq!(error.failure_fact(), again.failure_fact());
    assert!(capture.finish(&work).is_err());
}

#[test]
fn join_k_captured_source_tag_exits_before_published_destination_index() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::ValueTypeStorageHold;
    use std::{alloc::Layout, sync::atomic::AtomicUsize};
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Tagged<'a> {
        original: &'a dyn InputConstructionOwner,
        storage: ValueTypeStorageLifetime,
    }
    impl InputConstructionOwner for Tagged<'_> {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.storage.clone()
        }
        fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
            self.original.request(layout)
        }
        fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
            self.original.observe_opaque_upper_bound(bytes)
        }
        fn observe(
            &self,
            n: usize,
            path: &[crate::exec::chunk::ArrayPathStep],
            step: crate::exec::chunk::ArrayNormalizationStep<'_>,
        ) -> Result<(), ConstructionFailure> {
            self.original.observe(n, path, step)
        }
    }
    let (owner, tracker) = fixture();
    let control = Control::default();
    let source_drops = Arc::new(AtomicUsize::new(0));
    let destination_drops = Arc::new(AtomicUsize::new(0));
    let tagged = |drops: &Arc<AtomicUsize>| Tagged {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    let destination = tagged(&destination_drops);
    let source = tagged(&source_drops);
    let destination_work = ports(&destination, &tracker, &control);
    let source_work = ports(&source, &tracker, &control);
    let binding = FunctionValueType::new(DataType::Utf8, true);
    let mut capture = JoinKeyBuild::new(
        std::slice::from_ref(&binding),
        &[false],
        JoinHashMapBuildPurpose::RowMatches,
        &destination_work,
    )
    .unwrap();
    capture
        .push(
            &[Arc::new(StringArray::from(vec![
                Some("a long source value requiring owned destination bytes"),
                None,
            ]))],
            std::slice::from_ref(&binding),
            2,
            &mut ValueConversionContext::default(),
            &source_work,
        )
        .unwrap();
    drop(source_work);
    drop(source);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    let map = capture.finish(&destination_work).unwrap();
    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        pairs(
            &map,
            &[Arc::new(StringArray::from(vec![
                "a long source value requiring owned destination bytes"
            ]))],
            &[binding],
            &destination_work,
            1
        )
        .0,
        vec![(0, 0)]
    );
    drop(destination_work);
    drop(destination);
    assert_eq!(destination_drops.load(Ordering::SeqCst), 0);
    drop(map);
    assert_eq!(destination_drops.load(Ordering::SeqCst), 1);
}
