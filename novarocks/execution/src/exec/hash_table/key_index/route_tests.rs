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
    IndexPurpose, KeySpec, NullPolicy,
    bulk::CompleteInput,
    capture::{BulkInput, CaptureWork, EvaluatedPage},
    local_hash::BYTE_HASH_INITS,
};
use super::*;
use crate::{
    exec::hash_table::allocation::AggregateAllocator,
    runtime::{mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host},
};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::DataType,
};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelFailure;
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{
    ValueError, decode::ValueConversionContext, reachability::ReachableRows,
};
use std::{alloc::Layout, cell::Cell, collections::BTreeSet};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("routing never waits")
    }
}
fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("route original owner");
    let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
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
) -> RouteWork<'a> {
    RouteWork {
        owner,
        host: expression_allocation_host(tracker.clone()),
        control,
    }
}
fn complete(
    pages: Vec<(ArrayRef, Vec<usize>)>,
    nulls: NullPolicy,
    forced: Option<u64>,
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> (CompleteInput, Vec<CapturedPageId>) {
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(std::slice::from_ref(&ty), &[nulls], owner, &|| Ok(())).unwrap();
    // Test-only dependency agreed by main: the entire new domain shares this
    // override before any claim, and capture applies it before stats/HLL.
    let domain = match forced {
        Some(hash) => BulkDomain::new_forced_hash_for_test(&spec, owner, &Control, hash).unwrap(),
        None => BulkDomain::new(&spec, owner, &Control).unwrap(),
    };
    let capture = CaptureWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker.clone()),
        control: &Control,
    };
    let mut builder = domain.begin_capture(&capture).unwrap();
    let mut ids = Vec::new();
    for (array, rows) in pages {
        let readable = ReachableRows::all(array.len())
            .materialize_observed(
                &mut |l| owner.request(l).map_err(ExecutionFailure::from),
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
                    &capture,
                )
                .unwrap(),
        );
    }
    let sealed = builder.seal(&capture).unwrap();
    let input = BulkInput::new(&domain, vec![sealed], &capture).unwrap();
    (
        CompleteInput::from_captures(input, IndexPurpose::KeyIds, &capture).unwrap(),
        ids,
    )
}
fn plan(input: &CompleteInput, bits: u32, ports: &RouteWork<'_>) -> Arc<RoutePlan> {
    let layout = HashLayout::for_bulk(bits, input.stats().exact().indexable_rows());
    let domains = (0..layout.partitions())
        .map(|partition| {
            SizingDomain::new(
                input.stats().domain().spec(),
                input.stats().domain().policy(),
                SizingDomainKind::Partition(partition),
                ports.owner,
            )
            .unwrap()
        })
        .collect();
    RoutePlan::new(input, layout, domains, ports).unwrap()
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
fn take_partial(step: RouteAdvance) -> RoutePartial {
    match step {
        RouteAdvance::Yielded(partial) => partial,
        _ => panic!("this admitted row completes the small reference page"),
    }
}

#[test]
fn zero_hash_and_null_policy_keep_original_occurrences_merge_before_route_done_and_never_rehash_bytes()
 {
    for nulls in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            Some("a"),
        ]));
        let original = Arc::clone(&array);
        let (input, ids) = complete(
            vec![(array, vec![3, 1, 0, 2, 3, 1, 2, 0])],
            nulls,
            Some(0),
            owner.as_ref(),
            &tracker,
        );
        let plan = plan(&input, 2, &ports);
        let page = ids[0];
        let mut merges = (0..plan.partitions())
            .map(|p| MergeAccumulator::new(Arc::clone(&plan), p, &ports).unwrap())
            .collect::<Vec<_>>();
        BYTE_HASH_INITS.with(|n| n.set(0));
        let workspace = RouteWorkspace::with_page_target_for_test(
            Arc::clone(&plan),
            NonZeroUsize::new(1).unwrap(),
            2,
            &ports,
        )
        .unwrap();
        let mut cursor = workspace.start(page, 0..8, &ports).unwrap();
        let mut merged_early = false;
        loop {
            let step = cursor.advance(1, &ports).unwrap();
            assert!(cursor.active_partials() <= 1);
            match step {
                RouteAdvance::Progress => {}
                RouteAdvance::Yielded(partial) => {
                    assert_eq!(partial.partition(), 0);
                    assert!(partial.references().len() <= 2);
                    assert_eq!(partial.references().page_id(), page);
                    assert!(Arc::ptr_eq(
                        &partial.references().source_page().unwrap().input().arrays()[0],
                        &original
                    ));
                    merges[0].merge(partial, &ports).unwrap();
                    if cursor.position() < 8 {
                        assert!(merges[0].eligible().unwrap() > 0);
                        merged_early = true;
                    }
                }
                RouteAdvance::Completed => break,
            }
        }
        assert!(merged_early);
        let (workspace, range) = cursor.finish(&ports).unwrap();
        assert_eq!(range.page(), page);
        assert_eq!(range.range(), 0..8);
        let expected = if nulls == NullPolicy::Equal {
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        } else {
            vec![0, 2, 3, 4, 6, 7]
        };
        assert_eq!(range.eligible(), expected.len());
        for (partition, merge) in merges.into_iter().enumerate() {
            let output = merge.finish(&ports).unwrap();
            assert_eq!(output.partition(), partition);
            assert_eq!(
                output.domain().kind(),
                SizingDomainKind::Partition(partition)
            );
            if partition == 0 {
                assert_eq!(output.eligible(), expected.len());
                assert_eq!(output.ndv(), RoutedNdv::ObservedHll(1));
                let refs = output
                    .pages()
                    .iter()
                    .flat_map(|p| p.occurrences().iter().copied())
                    .collect::<Vec<_>>();
                assert_eq!(refs, expected);
                for refs in output.pages() {
                    for i in 0..refs.len() {
                        let reference = refs.reference(i).unwrap();
                        assert_eq!(reference.page, page);
                        assert_eq!(
                            plan.sources().row(reference).unwrap(),
                            [3, 1, 0, 2, 3, 1, 2, 0][reference.occurrence]
                        );
                    }
                }
            } else {
                assert_eq!(output.eligible(), 0);
                assert_eq!(output.ndv(), RoutedNdv::ExactEmptyMergedSet);
                assert!(output.pages().is_empty());
            }
        }
        assert_eq!(BYTE_HASH_INITS.with(Cell::get), 0);
        drop(workspace);
    }
}

#[test]
fn real_saved_hashes_route_every_eligible_reference_once_and_size_each_partition_from_its_own_values()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let strings = (0..64)
        .map(|i| {
            if i % 9 == 0 {
                None
            } else {
                Some(format!("key-{}", i % 7))
            }
        })
        .collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(StringArray::from(strings));
    let mut occurrences = (0..64).collect::<Vec<_>>();
    occurrences.extend([3, 3, 10]);
    let (input, ids) = complete(
        vec![(array, occurrences)],
        NullPolicy::Unmatchable,
        None,
        owner.as_ref(),
        &tracker,
    );
    let plan = plan(&input, 2, &ports);
    let source = plan.sources().page(ids[0]).unwrap();
    let mut expected = vec![Vec::new(); plan.partitions()];
    let mut members = vec![BTreeSet::new(); plan.partitions()];
    for occurrence in 0..source.len() {
        if source.hashes().participates(occurrence).unwrap() {
            let hash = source.hashes().hash(occurrence).unwrap();
            let p = plan.layout.route(hash);
            expected[p].push(occurrence);
            members[p].insert(hash);
        }
    }
    assert_eq!(expected.iter().map(Vec::len).sum::<usize>(), 59);
    let workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&plan),
        NonZeroUsize::new(1).unwrap(),
        3,
        &ports,
    )
    .unwrap();
    let mut cursor = workspace.start(ids[0], 0..source.len(), &ports).unwrap();
    let mut accumulators = (0..plan.partitions())
        .map(|p| MergeAccumulator::new(Arc::clone(&plan), p, &ports).unwrap())
        .collect::<Vec<_>>();
    BYTE_HASH_INITS.with(|n| n.set(0));
    loop {
        match cursor.advance(2, &ports).unwrap() {
            RouteAdvance::Progress => {}
            RouteAdvance::Yielded(partial) => {
                assert!(partial.references().len() <= 3);
                let p = partial.partition();
                for occurrence in partial.references().occurrences() {
                    assert_eq!(
                        plan.layout
                            .route(source.hashes().hash(*occurrence).unwrap()),
                        p
                    );
                }
                accumulators[p].merge(partial, &ports).unwrap();
            }
            RouteAdvance::Completed => break,
        };
        assert!(cursor.active_partials() <= 1);
    }
    let (_, range) = cursor.finish(&ports).unwrap();
    assert_eq!(range.eligible(), 59);
    for (p, merge) in accumulators.into_iter().enumerate() {
        let output = merge.finish(&ports).unwrap();
        let refs = output
            .pages()
            .iter()
            .flat_map(|page| page.occurrences().iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(refs, expected[p]);
        assert_eq!(output.eligible(), expected[p].len());
        assert_eq!(
            output.ndv(),
            if members[p].is_empty() {
                RoutedNdv::ExactEmptyMergedSet
            } else {
                RoutedNdv::ObservedHll(members[p].len() as u64)
            }
        );
    }
    assert_eq!(BYTE_HASH_INITS.with(Cell::get), 0);
}

#[test]
fn drained_workspace_reuses_one_slot_directory_across_source_pages_without_changing_page_coordinates()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let a: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["b"]));
    let (input, ids) = complete(
        vec![(a, vec![0, 0, 0]), (b, vec![0, 0])],
        NullPolicy::Equal,
        Some(0),
        owner.as_ref(),
        &tracker,
    );
    let plan = plan(&input, 1, &ports);
    let mut workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&plan),
        NonZeroUsize::new(1).unwrap(),
        2,
        &ports,
    )
    .unwrap();
    let mut merge = MergeAccumulator::new(Arc::clone(&plan), 0, &ports).unwrap();
    for (page, count) in ids.iter().copied().zip([3, 2]) {
        let mut cursor = workspace.start(page, 0..count, &ports).unwrap();
        loop {
            match cursor.advance(1, &ports).unwrap() {
                RouteAdvance::Progress => {}
                RouteAdvance::Yielded(partial) => {
                    assert_eq!(partial.references().page_id(), page);
                    merge.merge(partial, &ports).unwrap();
                }
                RouteAdvance::Completed => break,
            }
        }
        let (next, range) = cursor.finish(&ports).unwrap();
        assert_eq!(range.eligible(), count);
        workspace = next;
    }
    let output = merge.finish(&ports).unwrap();
    assert_eq!(output.eligible(), 5);
    assert!(output.pages().iter().any(|p| p.page_id() == ids[0]));
    assert!(output.pages().iter().any(|p| p.page_id() == ids[1]));
    for page in output.pages() {
        assert!(
            page.occurrences()
                .iter()
                .all(|i| *i < if page.page_id() == ids[0] { 3 } else { 2 })
        );
    }
    drop(workspace);
}

#[test]
fn partial_payload_and_reference_pages_keep_distinct_actual_creators_until_real_merge_and_reference_exit()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    let (input, ids) = complete(
        vec![(array, vec![0, 1])],
        NullPolicy::Equal,
        Some(0),
        owner.as_ref(),
        &tracker,
    );
    let plan = plan(&input, 1, &ports);
    let workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&plan),
        NonZeroUsize::new(1).unwrap(),
        1,
        &ports,
    )
    .unwrap();
    let mut cursor = workspace.start(ids[0], 0..2, &ports).unwrap();
    let alpha_tracker;
    let a;
    {
        let (alpha, actual) = fixture();
        alpha_tracker = actual;
        let work_a = work(alpha.as_ref(), &alpha_tracker, &Control);
        a = take_partial(cursor.advance(1, &work_a).unwrap());
    }
    let beta_tracker;
    let b;
    {
        let (beta, actual) = fixture();
        beta_tracker = actual;
        let work_b = work(beta.as_ref(), &beta_tracker, &Control);
        b = take_partial(cursor.advance(1, &work_b).unwrap());
    }
    assert!(alpha_tracker.current() > 0);
    assert!(beta_tracker.current() > 0);
    assert!(matches!(
        cursor.advance(1, &ports).unwrap(),
        RouteAdvance::Completed
    ));
    let (workspace, _) = cursor.finish(&ports).unwrap();
    let mut merge = MergeAccumulator::new(Arc::clone(&plan), 0, &ports).unwrap();
    merge.merge(a, &ports).unwrap();
    merge.merge(b, &ports).unwrap();
    let output = merge.finish(&ports).unwrap();
    assert_eq!(output.eligible(), 2);
    assert!(alpha_tracker.current() > 0);
    assert!(beta_tracker.current() > 0);
    drop(workspace);
    drop(output);
    assert_eq!(alpha_tracker.current(), 0);
    assert_eq!(beta_tracker.current(), 0);
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
fn merge_growth_refusal_preserves_original_cause_destroys_failed_state_and_blocks_route_success() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    let (input, ids) = complete(
        vec![(array, vec![0, 1])],
        NullPolicy::Equal,
        Some(0),
        owner.as_ref(),
        &tracker,
    );
    let plan = plan(&input, 1, &ports);
    let workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&plan),
        NonZeroUsize::new(1).unwrap(),
        1,
        &ports,
    )
    .unwrap();
    let mut cursor = workspace.start(ids[0], 0..2, &ports).unwrap();
    let partial = take_partial(cursor.advance(1, &ports).unwrap());
    let mut merge = MergeAccumulator::new(Arc::clone(&plan), 0, &ports).unwrap();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let refused = work(&refusing, &tracker, &Control);
    assert_eq!(error(merge.merge(partial, &refused)), cause);
    assert!(merge.sketch.is_none());
    assert!(merge.pages.is_empty());
    assert_eq!(error(merge.eligible()), cause);
    assert_eq!(error(cursor.advance(1, &ports)), cause);
    assert_eq!(error(merge.finish(&ports)), cause);
}
#[test]
fn foreign_plan_and_incomplete_range_cannot_become_successful_merge_or_range_completion() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    let (input, ids) = complete(
        vec![(array, vec![0, 1])],
        NullPolicy::Equal,
        Some(0),
        owner.as_ref(),
        &tracker,
    );
    let a = plan(&input, 1, &ports);
    let b = plan(&input, 1, &ports);
    let workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&b),
        NonZeroUsize::new(1).unwrap(),
        1,
        &ports,
    )
    .unwrap();
    let mut cursor = workspace.start(ids[0], 0..2, &ports).unwrap();
    let partial = take_partial(cursor.advance(1, &ports).unwrap());
    let mut wrong = MergeAccumulator::new(Arc::clone(&a), 0, &ports).unwrap();
    assert_invariant(&error(wrong.merge(partial, &ports)), ValueViolation::Layout);
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let array: ArrayRef = Arc::new(StringArray::from(vec!["z"]));
    let (input, ids) = complete(
        vec![(array, vec![0, 0])],
        NullPolicy::Equal,
        Some(0),
        owner.as_ref(),
        &tracker,
    );
    let plan = plan(&input, 1, &ports);
    let workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&plan),
        NonZeroUsize::new(1).unwrap(),
        3,
        &ports,
    )
    .unwrap();
    let mut cursor = workspace.start(ids[0], 0..2, &ports).unwrap();
    assert!(matches!(
        cursor.advance(1, &ports).unwrap(),
        RouteAdvance::Progress
    ));
    assert_invariant(
        &error(cursor.finish(&ports)),
        ValueViolation::RowTransaction,
    );
    assert_invariant(&error(plan.check()), ValueViolation::RowTransaction);
}

#[test]
fn route_reference_growth_refusal_destroys_active_sketches_without_consuming_the_current_occurrence()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    let (input, ids) = complete(
        vec![(array, vec![0, 1])],
        NullPolicy::Equal,
        Some(0),
        owner.as_ref(),
        &tracker,
    );
    let plan = plan(&input, 1, &ports);
    let workspace = RouteWorkspace::with_page_target_for_test(
        Arc::clone(&plan),
        NonZeroUsize::new(1).unwrap(),
        2,
        &ports,
    )
    .unwrap();
    let mut cursor = workspace.start(ids[0], 0..2, &ports).unwrap();
    let merge = MergeAccumulator::new(Arc::clone(&plan), 0, &ports).unwrap();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let refused = work(&refusing, &tracker, &Control);
    assert_eq!(error(cursor.advance(1, &refused)), cause);
    assert_eq!(cursor.position(), 0);
    assert_eq!(cursor.active_partials(), 0);
    assert_eq!(error(cursor.advance(1, &ports)), cause);
    assert_eq!(error(cursor.finish(&ports)), cause);
    assert_eq!(error(merge.finish(&ports)), cause);
}
