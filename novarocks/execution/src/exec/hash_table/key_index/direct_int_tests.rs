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
use crate::runtime::{
    mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host,
};
use arrow::{
    array::{Array, Int8Array, Int16Array, Int64Array},
    datatypes::DataType,
};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelFailure;
use novarocks_value_arrays::{
    ValueError, decode::ValueConversionContext, reachability::ReachableRows,
};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("direct operations never wait")
    }
}
fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("direct original resource owner");
    let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}
fn work<'a>(
    owner: &'a dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
    control: &'a dyn KernelEvaluationControl,
) -> DirectWork<'a> {
    DirectWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker.clone()),
        control,
    }
}
fn spec(ty: &FunctionValueType, nulls: NullPolicy, work: &DirectWork<'_>) -> KeySpec {
    KeySpec::new(std::slice::from_ref(ty), &[nulls], work.owner, &|| Ok(())).unwrap()
}
fn input(
    spec: &KeySpec,
    array: ArrayRef,
    binding: &FunctionValueType,
    rows: &[usize],
    work: &DirectWork<'_>,
) -> KeyInput {
    let readable = ReachableRows::all(array.len())
        .materialize_observed(
            &mut |l| work.owner.request(l).map_err(ExecutionFailure::from),
            &mut |_| Ok(()),
        )
        .unwrap();
    KeyInput::new_for_spec(
        spec,
        &[array],
        std::slice::from_ref(binding),
        &readable,
        rows,
        &mut ValueConversionContext::default(),
        work.owner,
        &|| Ok(()),
    )
    .unwrap()
}
fn geometry(
    spec: &KeySpec,
    min_max: Option<(i64, i64)>,
    purpose: IndexPurpose,
    workers: usize,
) -> DirectGeometry {
    DirectGeometry::for_selected(
        spec,
        IntegerDomain { min_max },
        purpose,
        NonZeroUsize::new(workers).unwrap(),
    )
    .unwrap()
}
fn error<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(e) => e,
        Ok(_) => panic!("the original operation must refuse"),
    }
}
fn assert_invariant(cause: &ExecutionFailure, violation: ValueViolation) {
    assert_eq!(
        cause.failure_fact(),
        ValueError::Invariant(violation).failure_fact()
    );
}
fn initialize(store: &Arc<Storage>, ports: &DirectWork<'_>) {
    std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..store.geometry().shard_count())
            .map(|shard| {
                let store = Arc::clone(store);
                scope.spawn(move || {
                    let mut cursor = InitCursor::new(Arc::clone(&store), shard, ports).unwrap();
                    let zero_quota_done = cursor.advance(0, ports).unwrap();
                    if store.geometry().words != 0 {
                        assert!(!zero_quota_done);
                        assert!(store.shards[shard].get().is_none());
                    }
                    while !cursor.advance(7, ports).unwrap() {}
                })
            })
            .collect();
        for job in jobs {
            job.join().unwrap();
        }
    });
}
fn mark(store: &Arc<Storage>, keys: &KeyInput, ports: &DirectWork<'_>) {
    let split = keys.len() / 2;
    std::thread::scope(|scope| {
        let jobs: Vec<_> = [0..split, split..keys.len()]
            .into_iter()
            .map(|range| {
                let store = Arc::clone(store);
                scope.spawn(move || {
                    let mut cursor = MarkCursor::new(store, keys, range).unwrap();
                    while !cursor.advance(2, ports).unwrap() {}
                })
            })
            .collect();
        for job in jobs {
            job.join().unwrap();
        }
    });
}
fn fill_ids(store: &Arc<Storage>, ports: &DirectWork<'_>) -> usize {
    // Real joins delimit the global Init/Mark/Count barriers in this local test.
    // They are not a fake production TaskGraph or Ready receipt.
    let mut counts = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..store.geometry().shard_count())
            .map(|shard| {
                let store = Arc::clone(store);
                scope.spawn(move || {
                    let mut cursor = CountCursor::new(store, shard, ports).unwrap();
                    while !cursor.advance(1, ports).unwrap() {}
                    cursor.finish().unwrap()
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    counts.sort_by_key(CountedShard::shard);
    let mut base = 0;
    let mut prefix_cursors = Vec::new();
    for count in counts {
        let next = base + count.total();
        prefix_cursors.push(PrefixCursor::new(count, base).unwrap());
        base = next;
    }
    let prefixes = std::thread::scope(|scope| {
        let jobs: Vec<_> = prefix_cursors
            .into_iter()
            .map(|mut cursor| {
                scope.spawn(move || {
                    while !cursor.advance(1, ports).unwrap() {}
                    cursor.finish().unwrap()
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    std::thread::scope(|scope| {
        let jobs: Vec<_> = prefixes
            .into_iter()
            .map(|prefix| {
                scope.spawn(move || {
                    let mut cursor = FillCursor::new(prefix, ports).unwrap();
                    while !cursor.advance(2, ports).unwrap() {}
                })
            })
            .collect();
        for job in jobs {
            job.join().unwrap();
        }
    });
    store.finish_null(base, ports).unwrap();
    base
}

#[test]
fn multiple_workers_preserve_signed_slot_order_null_class_duplicates_and_exact_representatives() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let declared = FunctionValueType::new(DataType::Int16, false);
    let actual = FunctionValueType::new(DataType::Int16, true);
    let spec = spec(&declared, NullPolicy::Equal, &ports);
    let keys = input(
        &spec,
        Arc::new(Int16Array::from(vec![
            None,
            Some(-70),
            Some(130),
            Some(2),
            Some(-70),
            Some(0),
        ])),
        &actual,
        &[4, 0, 3, 1, 4, 2, 5, 0],
        &ports,
    );
    let store = Storage::new(
        geometry(&spec, Some((-70, 130)), IndexPurpose::KeyIds, 3),
        &spec,
        Some(true),
        &ports,
    )
    .unwrap();
    assert!(store.binding().nullable());
    initialize(&store, &ports);
    mark(&store, &keys, &ports);
    assert_eq!(fill_ids(&store, &ports), 4);
    let mut assignment =
        AssignCursor::new(Arc::clone(&store), &keys, 0..keys.len(), &ports).unwrap();
    while !assignment.advance(2, &ports).unwrap() {}
    let assignment = assignment.finish().unwrap();
    assert_eq!(assignment.start, 0);
    assert_eq!(
        assignment.matches(),
        &[
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(4)),
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(3)),
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(4))
        ]
    );
    let mut representatives = Vec::new();
    for shard in 0..store.geometry().shard_count() {
        for array in store.representatives(shard).unwrap().arrays() {
            assert_eq!(array.data_type(), declared.data_type());
            let values = array.as_any().downcast_ref::<Int16Array>().unwrap();
            assert_eq!(values.null_count(), 0);
            representatives.extend(values.values().iter().copied());
        }
    }
    assert_eq!(representatives, [-70, 0, 2, 130]);
    let null = store.null_array().unwrap().unwrap();
    assert_eq!(null.data_type(), declared.data_type());
    assert_eq!(null.null_count(), 1);
    assert_eq!(
        store.lookup_value_after_fill(Some(-71)).unwrap(),
        KeyMatch::Miss
    );
    assert_eq!(
        store.lookup_value_after_fill(Some(131)).unwrap(),
        KeyMatch::Miss
    );
}

#[test]
fn presence_only_has_no_slot_ids_representative_columns_or_assignment_and_keeps_signed_null_declaration()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let declared = FunctionValueType::new(DataType::Int8, false);
    let actual = FunctionValueType::new(DataType::Int8, true);
    let spec = spec(&declared, NullPolicy::Equal, &ports);
    // No actual NULL is needed to preserve the signed nullable source promise.
    let keys = input(
        &spec,
        Arc::new(Int8Array::from(vec![Some(-2), Some(4), Some(-2)])),
        &actual,
        &[2, 1, 0, 2],
        &ports,
    );
    let layout = geometry(&spec, Some((-2, 4)), IndexPurpose::PresenceOnly, 8);
    let costs = layout.costs(usize::MAX, true).unwrap();
    assert_eq!(
        (
            costs.slot_id_bytes,
            costs.count_prefix_peak_bytes,
            costs.representative_payload_estimate,
            costs.representative_pages_upper,
            costs.null_constructor_peak_upper
        ),
        (0, 0, 0, 0, 0)
    );
    let store = Storage::new(layout, &spec, Some(true), &ports).unwrap();
    assert!(store.binding().nullable());
    initialize(&store, &ports);
    mark(&store, &keys, &ports);
    assert!(store.contains_value_after_mark(Some(-2)).unwrap());
    assert!(store.contains_value_after_mark(Some(4)).unwrap());
    assert!(!store.contains_value_after_mark(Some(0)).unwrap());
    assert!(!store.contains_value_after_mark(None).unwrap());
    for shard in 0..layout.shard_count() {
        let shard = store.shard(shard).unwrap();
        assert!(shard.pages.iter().all(|page| page.ids.is_none()));
        assert!(shard.representatives.get().is_none());
    }
    assert!(store.null_representative.get().is_none());
    assert_invariant(
        &error(AssignCursor::new(
            Arc::clone(&store),
            &keys,
            0..keys.len(),
            &ports,
        )),
        ValueViolation::Layout,
    );
}

#[test]
fn all_null_has_no_fake_minimum_or_nonnull_representative_and_unmatchable_null_stays_outside_ids() {
    for policy in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let ty = FunctionValueType::new(DataType::Int64, true);
        let spec = spec(&ty, policy, &ports);
        let keys = input(
            &spec,
            Arc::new(Int64Array::from(vec![None, None])),
            &ty,
            &[1, 0, 1],
            &ports,
        );
        let layout = geometry(&spec, None, IndexPurpose::KeyIds, 4);
        assert_eq!(layout.slots(), 0);
        let store = Storage::new(layout, &spec, Some(true), &ports).unwrap();
        initialize(&store, &ports);
        mark(&store, &keys, &ports);
        assert_eq!(fill_ids(&store, &ports), 0);
        assert!(store.representatives(0).unwrap().arrays().is_empty());
        assert_eq!(
            store.lookup_value_after_fill(None).unwrap(),
            if policy == NullPolicy::Equal {
                KeyMatch::Found(KeyId(0))
            } else {
                KeyMatch::Unmatchable
            }
        );
        assert_eq!(
            store.null_array().unwrap().is_some(),
            policy == NullPolicy::Equal
        );
    }
}

#[cfg(target_pointer_width = "64")]
#[test]
fn geometry_is_pure_without_arbitrary_span_limit_and_rejects_only_its_unrepresentable_index_extent()
{
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int64, false);
    let spec = spec(&ty, NullPolicy::Equal, &ports);
    let large = geometry(
        &spec,
        Some((-(1i64 << 39), (1i64 << 39) - 1)),
        IndexPurpose::PresenceOnly,
        4,
    );
    assert_eq!(large.slots(), 1usize << 40);
    assert!(large.costs(1, false).unwrap().bitmap_bytes > 0);
    assert!(
        DirectGeometry::for_selected(
            &spec,
            IntegerDomain {
                min_max: Some((i64::MIN, i64::MAX))
            },
            IndexPurpose::KeyIds,
            NonZeroUsize::new(4).unwrap()
        )
        .is_none()
    );
    // No Storage or cursor is created: geometry/cost arithmetic allocates and
    // initializes no bitmap, ID slot, source array or representative payload.
}

struct RefusingOwner<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
impl InputConstructionOwner for RefusingOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request(&self, _: Layout) -> Result<(), ConstructionFailure> {
        Err(self.1.clone().into())
    }
    fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
        self.0.observe_opaque_upper_bound(n)
    }
    fn observe(
        &self,
        occurrence: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.0.observe(occurrence, path, step)
    }
}
#[test]
fn initialization_refusal_is_original_and_cannot_turn_into_a_different_strategy_or_successful_count()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int64, false);
    let spec = spec(&ty, NullPolicy::Equal, &ports);
    let store = Storage::new(
        geometry(&spec, Some((0, 300)), IndexPurpose::KeyIds, 2),
        &spec,
        Some(false),
        &ports,
    )
    .unwrap();
    let mut init = InitCursor::new(Arc::clone(&store), 0, &ports).unwrap();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let refused = work(&refusing, &tracker, &Control);
    assert_eq!(error(init.advance(1, &refused)), cause);
    assert_eq!(error(init.advance(1, &ports)), cause);
    assert_eq!(
        error(CountCursor::new(Arc::clone(&store), 0, &ports)),
        cause
    );
    assert_eq!(error(store.check_failure()), cause);
}
struct StopAfterMarked {
    store: Arc<Storage>,
}
impl KernelEvaluationControl for StopAfterMarked {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        if self.store.bits(0).unwrap_or(0) & 1 != 0 {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("direct operations never wait")
    }
}
#[test]
fn control_after_real_atomic_mark_keeps_original_failure_and_prevents_later_success() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int64, false);
    let spec = spec(&ty, NullPolicy::Equal, &ports);
    let keys = input(
        &spec,
        Arc::new(Int64Array::from(vec![0, 1])),
        &ty,
        &[0, 1],
        &ports,
    );
    let store = Storage::new(
        geometry(&spec, Some((0, 1)), IndexPurpose::KeyIds, 1),
        &spec,
        Some(false),
        &ports,
    )
    .unwrap();
    initialize(&store, &ports);
    let stop = StopAfterMarked {
        store: Arc::clone(&store),
    };
    let stopped = work(owner.as_ref(), &tracker, &stop);
    let mut mark = MarkCursor::new(Arc::clone(&store), &keys, 0..keys.len()).unwrap();
    let cause = ExecutionFailure::from(KernelFailure::DeadlineExceeded);
    assert_eq!(error(mark.advance(2, &stopped)), cause);
    assert_eq!(store.bits(0).unwrap() & 1, 1);
    assert_eq!(error(mark.advance(2, &ports)), cause);
    assert_eq!(
        error(CountCursor::new(Arc::clone(&store), 0, &ports)),
        cause
    );
}

#[test]
fn missing_initialization_and_premature_null_publication_are_typed_failures_not_phase_success() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int64, true);
    let spec = spec(&ty, NullPolicy::Equal, &ports);
    let layout = geometry(&spec, Some((0, 1)), IndexPurpose::KeyIds, 1);
    let uninitialized = Storage::new(layout, &spec, Some(true), &ports).unwrap();
    assert_invariant(
        &error(CountCursor::new(Arc::clone(&uninitialized), 0, &ports)),
        ValueViolation::RowTransaction,
    );
    assert!(uninitialized.shards[0].get().is_none());
    let initialized = Storage::new(layout, &spec, Some(true), &ports).unwrap();
    initialize(&initialized, &ports);
    assert_invariant(
        &error(initialized.finish_null(0, &ports)),
        ValueViolation::RowTransaction,
    );
    assert!(initialized.null_representative.get().is_none());
}

struct TaggedHold {
    _origin: ValueTypeStorageLifetime,
    _tag: Arc<()>,
}
impl novarocks_type_contract::ValueTypeStorageHold for TaggedHold {}
struct TaggedOwner<'a> {
    original: &'a dyn InputConstructionOwner,
    storage: ValueTypeStorageLifetime,
}
impl InputConstructionOwner for TaggedOwner<'_> {
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
        occurrence: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.original.observe(occurrence, path, step)
    }
}
#[test]
fn representative_buffer_keeps_actual_earlier_vec_creator_across_later_adoption_owner() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int64, false);
    let spec = spec(&ty, NullPolicy::Equal, &ports);
    let keys = input(
        &spec,
        Arc::new(Int64Array::from(vec![0, 1])),
        &ty,
        &[0, 1],
        &ports,
    );
    let store = Storage::new(
        geometry(&spec, Some((0, 1)), IndexPurpose::KeyIds, 1),
        &spec,
        Some(false),
        &ports,
    )
    .unwrap();
    initialize(&store, &ports);
    mark(&store, &keys, &ports);
    let mut count = CountCursor::new(Arc::clone(&store), 0, &ports).unwrap();
    while !count.advance(1, &ports).unwrap() {}
    let mut prefix = PrefixCursor::new(count.finish().unwrap(), 0).unwrap();
    while !prefix.advance(1, &ports).unwrap() {}
    // The representative page directory is owned by the original final writer.
    let mut fill = FillCursor::new(prefix.finish().unwrap(), &ports).unwrap();
    let alpha_tracker;
    let weak;
    {
        let (alpha_owner, actual_tracker) = fixture();
        alpha_tracker = actual_tracker;
        let tag = Arc::new(());
        weak = Arc::downgrade(&tag);
        alpha_owner
            .request(
                novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                    TaggedHold,
                >())
                .unwrap(),
            )
            .unwrap();
        let storage = ValueTypeStorageLifetime::held(Arc::new(TaggedHold {
            _origin: alpha_owner.storage_lifetime(),
            _tag: tag,
        }));
        let tagged = TaggedOwner {
            original: alpha_owner.as_ref(),
            storage,
        };
        let alpha = work(&tagged, &alpha_tracker, &Control);
        // One row allocates and partially fills the native Vec under alpha.
        // No per-page payload allocation will be replaced by the later writer.
        assert!(!fill.advance(1, &alpha).unwrap());
    }
    assert!(weak.upgrade().is_some());
    assert!(alpha_tracker.current() > 0);
    while !fill.advance(2, &ports).unwrap() {}
    store.finish_null(2, &ports).unwrap();
    let array = store.representatives(0).unwrap().arrays()[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(array.values().as_ref(), &[0, 1]);
    let escaped = array.values().inner().clone();
    drop(fill);
    drop(store);
    assert!(weak.upgrade().is_some());
    assert!(alpha_tracker.current() > 0);
    drop(escaped);
    assert!(weak.upgrade().is_none());
    assert_eq!(alpha_tracker.current(), 0);
}
