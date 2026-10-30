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

use super::super::{
    KeyMatch, KeySpec, LocalHashPolicy, NullPolicy,
    bulk::CompleteInput,
    capture::{BulkDomain, BulkInput, CaptureWork, CapturedPageId, EvaluatedPage},
    hash_layout::HashLayout,
    local_hash::BYTE_HASH_INITS,
    route::{MergeAccumulator, RouteAdvance, RoutePlan, RouteWork, RouteWorkspace},
    sizing::SizingDomain,
};
use super::*;
use crate::{
    exec::pipeline::operator::ConstructionFailure,
    runtime::{mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host},
};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::DataType,
};
use novarocks_functions::KernelFailure;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::ValueError;
use novarocks_value_arrays::{decode::ValueConversionContext, reachability::ReachableRows};
use std::{alloc::Layout, cell::Cell, num::NonZeroUsize, sync::Arc};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("partition construction never waits")
    }
}
struct Stop;
impl KernelEvaluationControl for Stop {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Err(KernelFailure::DeadlineExceeded)
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("partition construction never waits")
    }
}
fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("hash partition construction test");
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let declaration = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}
fn work<'a>(
    owner: &'a dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
    control: &'a dyn KernelEvaluationControl,
) -> BuildWork<'a> {
    BuildWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        control,
    }
}
fn route_work<'a>(
    owner: &'a dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> RouteWork<'a> {
    RouteWork {
        owner,
        host: expression_allocation_host(tracker.clone()),
        control: &Control,
    }
}
fn error<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("original operation must refuse"),
    }
}
fn assert_invariant(cause: &ExecutionFailure, expected: ValueViolation) {
    let crate::runtime::fragment::ExecutionFailureCause::Domain(original) = cause.cause() else {
        panic!("the original typed value error must remain recoverable");
    };
    assert!(matches!(original.downcast_ref::<ValueError>(),
        Some(ValueError::Invariant(actual)) if *actual == expected));
}

fn captured(
    nulls: NullPolicy,
    purpose: IndexPurpose,
    forced: Option<u64>,
    large: bool,
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> (CompleteInput, Vec<CapturedPageId>) {
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(std::slice::from_ref(&ty), &[nulls], owner, &|| Ok(())).unwrap();
    let domain = match forced {
        Some(hash) => BulkDomain::new_forced_hash_for_test(&spec, owner, &Control, hash).unwrap(),
        None => BulkDomain::new(&spec, owner, &Control).unwrap(),
    };
    let ports = CaptureWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker.clone()),
        control: &Control,
    };
    let mut builder = domain.begin_capture(&ports).unwrap();
    // The final physical row of each source is unreachable by occurrences.
    let first: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("b"),
        Some("a"),
        Some("unselected first"),
    ]));
    let second: ArrayRef = Arc::new(StringArray::from(vec![
        Some("c"),
        Some("b"),
        None,
        Some("unselected second"),
    ]));
    let first_rows = if large {
        (0..1100).map(|i| [3, 1, 0, 2, 3][i % 5]).collect()
    } else {
        vec![3, 1, 0, 2, 3]
    };
    let mut ids = Vec::new();
    for (array, rows) in [(first, first_rows), (second, vec![2, 1, 0, 0, 2])] {
        let readable = ReachableRows::all(array.len())
            .materialize_observed(
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| Ok(()),
            )
            .unwrap();
        ids.push(
            builder
                .push_evaluated(
                    EvaluatedPage {
                        arrays: &[array],
                        bindings: std::slice::from_ref(&ty),
                        readable: &readable,
                        occurrences: &rows,
                    },
                    &mut ValueConversionContext::default(),
                    &ports,
                )
                .unwrap(),
        );
    }
    let sealed = builder.seal(&ports).unwrap();
    let input = BulkInput::new(&domain, vec![sealed], &ports).unwrap();
    (
        CompleteInput::from_captures(input, purpose, &ports).unwrap(),
        ids,
    )
}
fn route(
    input: &CompleteInput,
    ids: &[CapturedPageId],
    bits: u32,
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> (HashLayout, Vec<MergedPartition>) {
    let ports = route_work(owner, tracker);
    let layout = HashLayout::for_bulk(bits, input.stats().exact().indexable_rows());
    let domains = (0..layout.partitions())
        .map(|partition| {
            SizingDomain::new(
                input.stats().domain().spec(),
                input.stats().domain().policy(),
                SizingDomainKind::Partition(partition),
                owner,
            )
            .unwrap()
        })
        .collect();
    let plan = RoutePlan::new(input, layout, domains, &ports).unwrap();
    let mut merges = (0..layout.partitions())
        .map(|partition| MergeAccumulator::new(Arc::clone(&plan), partition, &ports).unwrap())
        .collect::<Vec<_>>();
    let mut workspace =
        RouteWorkspace::new(Arc::clone(&plan), NonZeroUsize::new(1).unwrap(), &ports).unwrap();
    for id in ids {
        let count = input.runs().page(*id).unwrap().len();
        let mut cursor = workspace.start(*id, 0..count, &ports).unwrap();
        loop {
            match cursor.advance(7, &ports).unwrap() {
                RouteAdvance::Progress => {}
                RouteAdvance::Yielded(partial) => {
                    let partition = partial.partition();
                    merges[partition].merge(partial, &ports).unwrap();
                }
                RouteAdvance::Completed => break,
            }
        }
        workspace = cursor.finish(&ports).unwrap().0;
    }
    (
        layout,
        merges
            .into_iter()
            .map(|merge| merge.finish(&ports).unwrap())
            .collect(),
    )
}
fn index(
    input: &CompleteInput,
    layout: HashLayout,
    purpose: IndexPurpose,
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> KeyIndex {
    // No initial reservation is requested here. This is not an exact D=0
    // fact: normal original directory growth is exercised below. Production
    // HashInit owns its qualified estimate and reservation.
    KeyIndex::new_for_bulk_hash(
        input.stats().domain().spec().clone(),
        purpose,
        input.stats().domain().policy().clone(),
        layout,
        tracker.clone(),
        owner,
    )
    .unwrap()
}

#[test]
fn forced_zero_hash_uses_original_equality_and_dense_ids_across_pages_nulls_duplicates_and_step_boundaries()
 {
    for nulls in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        for quota in [1, 17, usize::MAX] {
            let (owner, tracker) = fixture();
            let ports = work(owner.as_ref(), &tracker, &Control);
            let (input, ids) = captured(
                nulls,
                IndexPurpose::KeyIds,
                Some(0),
                true,
                owner.as_ref(),
                &tracker,
            );
            let (layout, partitions) = route(&input, &ids, 2, owner.as_ref(), &tracker);
            BYTE_HASH_INITS.with(|count| count.set(0));
            for merged in partitions {
                let partition = merged.partition();
                let eligible = merged.eligible();
                if partition == 0 {
                    assert!(merged.pages().len() > 1);
                }
                let mut cursor = PartitionBuildCursor::new(
                    index(
                        &input,
                        layout,
                        IndexPurpose::KeyIds,
                        owner.as_ref(),
                        &tracker,
                    ),
                    merged,
                    IndexPurpose::KeyIds,
                    &ports,
                )
                .unwrap();
                assert_eq!(cursor.consumed().unwrap(), 0);
                assert_eq!(cursor.advance(0, &ports).unwrap(), eligible == 0);
                loop {
                    let before = cursor.consumed().unwrap();
                    let done = cursor.advance(quota, &ports).unwrap();
                    let after = cursor.consumed().unwrap();
                    assert!(after >= before && after - before <= quota);
                    if done {
                        break;
                    }
                    assert!(after > before);
                }
                assert_eq!(cursor.consumed().unwrap(), eligible);
                let built = cursor.finish(&ports).unwrap();
                if partition != 0 {
                    assert_eq!(built.len(), 0);
                    continue;
                }
                assert_eq!(built.len(), if nulls == NullPolicy::Equal { 4 } else { 3 });
                for (ordinal, id) in ids.iter().enumerate() {
                    let source = input.runs().page(*id).unwrap();
                    let prepared = PreparedKeyBatch::from_captured(
                        built.spec(),
                        source.input(),
                        ports.allocator.clone(),
                        &|| Ok(()),
                    )
                    .unwrap();
                    let probe = built
                        .probe_batch(&prepared, source.hashes(), owner.as_ref(), &Control)
                        .unwrap();
                    let expected = (0..source.len())
                        .map(|occurrence| {
                            let physical = source.input().row(occurrence).unwrap();
                            match (ordinal, physical, nulls) {
                                (0, 0 | 3, _) => KeyMatch::Found(super::super::KeyId(0)),
                                (0, 1, NullPolicy::Equal) | (1, 2, NullPolicy::Equal) => {
                                    KeyMatch::Found(super::super::KeyId(1))
                                }
                                (0, 1, NullPolicy::Unmatchable)
                                | (1, 2, NullPolicy::Unmatchable) => KeyMatch::Unmatchable,
                                (0, 2, NullPolicy::Equal) | (1, 1, NullPolicy::Equal) => {
                                    KeyMatch::Found(super::super::KeyId(2))
                                }
                                (0, 2, NullPolicy::Unmatchable)
                                | (1, 1, NullPolicy::Unmatchable) => {
                                    KeyMatch::Found(super::super::KeyId(1))
                                }
                                (1, 0, NullPolicy::Equal) => {
                                    KeyMatch::Found(super::super::KeyId(3))
                                }
                                (1, 0, NullPolicy::Unmatchable) => {
                                    KeyMatch::Found(super::super::KeyId(2))
                                }
                                _ => panic!("unselected physical row must not enter construction"),
                            }
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(probe.matches(), expected);
                }
            }
            assert_eq!(BYTE_HASH_INITS.with(Cell::get), 0);
        }
    }
}

#[test]
fn real_hash_partitions_keep_selected_class_membership_without_rehashing_and_presence_never_exposes_ids()
 {
    for purpose in [IndexPurpose::KeyIds, IndexPurpose::PresenceOnly] {
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let (input, ids) = captured(
            NullPolicy::Unmatchable,
            purpose,
            None,
            false,
            owner.as_ref(),
            &tracker,
        );
        let (layout, partitions) = route(&input, &ids, 2, owner.as_ref(), &tracker);
        BYTE_HASH_INITS.with(|count| count.set(0));
        let mut distinct = 0;
        for merged in partitions {
            let partition = merged.partition();
            let mut cursor = PartitionBuildCursor::new(
                index(&input, layout, purpose, owner.as_ref(), &tracker),
                merged,
                purpose,
                &ports,
            )
            .unwrap();
            while !cursor.advance(2, &ports).unwrap() {}
            let built = cursor.finish(&ports).unwrap();
            distinct += built.len();
            for id in &ids {
                let source = input.runs().page(*id).unwrap();
                let prepared = PreparedKeyBatch::from_captured(
                    built.spec(),
                    source.input(),
                    ports.allocator.clone(),
                    &|| Ok(()),
                )
                .unwrap();
                let present = built
                    .contains_batch(&prepared, source.hashes(), owner.as_ref(), &Control)
                    .unwrap();
                for occurrence in 0..source.len() {
                    let participates = source.hashes().participates(occurrence).unwrap();
                    assert_eq!(present.participates()[occurrence] != 0, participates);
                    assert_eq!(
                        present.present()[occurrence] != 0,
                        participates
                            && layout.route(source.hashes().hash(occurrence).unwrap()) == partition
                    );
                }
                if purpose == IndexPurpose::PresenceOnly {
                    assert!(
                        built
                            .probe_batch(&prepared, source.hashes(), owner.as_ref(), &Control)
                            .is_err()
                    );
                    assert!(built.read_keys(&[], owner.as_ref(), &Control).is_err());
                }
            }
        }
        assert_eq!(distinct, 3);
        assert_eq!(BYTE_HASH_INITS.with(Cell::get), 0);
    }
}

struct RefusingOwner<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
impl InputConstructionOwner for RefusingOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request(&self, _: Layout) -> Result<(), ConstructionFailure> {
        Err(self.1.clone().into())
    }
    fn observe_opaque_upper_bound(&self, upper: usize) -> Result<(), ConstructionFailure> {
        self.0.observe_opaque_upper_bound(upper)
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
fn original_control_or_growth_refusal_after_partial_build_destroys_state_and_preserves_the_first_cause()
 {
    for purpose in [IndexPurpose::KeyIds, IndexPurpose::PresenceOnly] {
        for stop in [false, true] {
            let (owner, tracker) = fixture();
            let ports = work(owner.as_ref(), &tracker, &Control);
            let (input, ids) = captured(
                NullPolicy::Equal,
                purpose,
                Some(0),
                false,
                owner.as_ref(),
                &tracker,
            );
            let (layout, mut partitions) = route(&input, &ids, 0, owner.as_ref(), &tracker);
            let mut cursor = PartitionBuildCursor::new(
                index(&input, layout, purpose, owner.as_ref(), &tracker),
                partitions.remove(0),
                purpose,
                &ports,
            )
            .unwrap();
            assert!(!cursor.advance(1, &ports).unwrap());
            assert_eq!(cursor.consumed().unwrap(), 1);
            let cause = if stop {
                ExecutionFailure::from(KernelFailure::DeadlineExceeded)
            } else {
                ExecutionFailure::from(ConstructionFailure::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                ))
            };
            let refusing = RefusingOwner(owner.as_ref(), cause.clone());
            let bad = if stop {
                work(owner.as_ref(), &tracker, &Stop)
            } else {
                work(&refusing, &tracker, &Control)
            };
            assert_eq!(error(cursor.advance(1, &bad)), cause);
            assert!(cursor.state.is_none());
            assert_eq!(error(cursor.consumed()), cause);
            assert_eq!(error(cursor.advance(usize::MAX, &ports)), cause);
            assert_eq!(error(cursor.finish(&ports)), cause);
        }
    }
}

#[test]
fn incomplete_build_and_foreign_header_cannot_release_an_index_and_original_prefailed_index_wins() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (input, ids) = captured(
        NullPolicy::Equal,
        IndexPurpose::KeyIds,
        Some(0),
        false,
        owner.as_ref(),
        &tracker,
    );
    let (layout, mut partitions) = route(&input, &ids, 0, owner.as_ref(), &tracker);
    let cursor = PartitionBuildCursor::new(
        index(
            &input,
            layout,
            IndexPurpose::KeyIds,
            owner.as_ref(),
            &tracker,
        ),
        partitions.remove(0),
        IndexPurpose::KeyIds,
        &ports,
    )
    .unwrap();
    assert_invariant(
        &error(cursor.finish(&ports)),
        ValueViolation::RowTransaction,
    );

    for foreign in [0, 1, 2] {
        let (layout, mut partitions) = route(&input, &ids, 0, owner.as_ref(), &tracker);
        let original = input.stats().domain();
        let spec = if foreign == 0 {
            KeySpec::new(
                &[FunctionValueType::new(DataType::Utf8, true)],
                &[NullPolicy::Equal],
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap()
        } else {
            original.spec().clone()
        };
        let policy = if foreign <= 1 {
            LocalHashPolicy::new(&spec, owner.as_ref()).unwrap()
        } else {
            original.policy().clone()
        };
        let purpose = if foreign == 2 {
            IndexPurpose::PresenceOnly
        } else {
            IndexPurpose::KeyIds
        };
        let built = KeyIndex::new_for_bulk_hash(
            spec,
            purpose,
            policy,
            layout,
            tracker.clone(),
            owner.as_ref(),
        )
        .unwrap();
        assert_invariant(
            &error(PartitionBuildCursor::new(
                built,
                partitions.remove(0),
                IndexPurpose::KeyIds,
                &ports,
            )),
            ValueViolation::Layout,
        );
    }
    let (layout, mut partitions) = route(&input, &ids, 0, owner.as_ref(), &tracker);
    let mut built = index(
        &input,
        layout,
        IndexPurpose::KeyIds,
        owner.as_ref(),
        &tracker,
    );
    let source = input.runs().page(ids[0]).unwrap();
    let prepared = PreparedKeyBatch::from_captured(
        built.spec(),
        source.input(),
        ports.allocator.clone(),
        &|| Ok(()),
    )
    .unwrap();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    assert_eq!(
        error(built.intern_selected(&prepared, source.hashes(), &[0], &refusing, &Control)),
        cause
    );
    let stopped = work(owner.as_ref(), &tracker, &Stop);
    assert_eq!(
        error(PartitionBuildCursor::new(
            built,
            partitions.remove(0),
            IndexPurpose::KeyIds,
            &stopped
        )),
        cause
    );
}

#[test]
fn completed_advance_retires_source_page_headers_before_finish_and_keeps_owned_keys_readable() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (input, ids) = captured(
        NullPolicy::Equal,
        IndexPurpose::KeyIds,
        Some(0),
        true,
        owner.as_ref(),
        &tracker,
    );
    let source = Arc::downgrade(&input.runs().page(ids[0]).unwrap().input().arrays()[0]);
    let (layout, mut partitions) = route(&input, &ids, 0, owner.as_ref(), &tracker);
    let mut cursor = PartitionBuildCursor::new(
        index(
            &input,
            layout,
            IndexPurpose::KeyIds,
            owner.as_ref(),
            &tracker,
        ),
        partitions.remove(0),
        IndexPurpose::KeyIds,
        &ports,
    )
    .unwrap();
    drop(partitions);
    drop(input);
    assert!(source.upgrade().is_some());
    assert!(!cursor.advance(1, &ports).unwrap());
    assert!(source.upgrade().is_some());
    while !cursor.advance(17, &ports).unwrap() {}
    // No final cursor finish is needed to release the last captured input
    // ArrayRef header. This does not assert source Buffer aliases or refunds.
    assert!(source.upgrade().is_none());
    let built = cursor.finish(&ports).unwrap();
    let output = built
        .export_keys(
            &[
                super::super::KeyId(3),
                super::super::KeyId(1),
                super::super::KeyId(0),
                super::super::KeyId(2),
            ],
            &[FunctionValueType::new(DataType::Utf8, true)],
            owner.as_ref(),
            &Control,
        )
        .unwrap();
    assert_eq!(
        output.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("c"), None, Some("a"), Some("b")],
    );
}
