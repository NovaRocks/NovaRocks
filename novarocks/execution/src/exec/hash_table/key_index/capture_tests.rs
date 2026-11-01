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
    array::{Array, DictionaryArray, Int32Array, StringArray},
    datatypes::{DataType, Int32Type},
};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelFailure;
use novarocks_value_arrays::{ValueError, reachability::ReachableRows};
use std::{
    alloc::Layout,
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("capture does not wait")
    }
}
fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("capture original construction");
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(), Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
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
) -> CaptureWork<'a> {
    CaptureWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker.clone()),
        control,
    }
}
fn domain(
    ty: &FunctionValueType,
    nulls: super::super::NullPolicy,
    work: &CaptureWork<'_>,
) -> BulkDomain {
    let spec = KeySpec::new(std::slice::from_ref(ty), &[nulls], work.owner, &|| Ok(())).unwrap();
    BulkDomain::new(&spec, work.owner, work.control).unwrap()
}
fn readable(rows: usize, owner: &dyn InputConstructionOwner) -> SelectionSet {
    ReachableRows::all(rows)
        .materialize_observed(
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| Ok(()),
        )
        .unwrap()
}
fn error<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("the original operation must refuse"),
    }
}
fn assert_invariant(cause: &ExecutionFailure, violation: ValueViolation) {
    assert_eq!(
        cause.failure_fact(),
        ValueError::Invariant(violation).failure_fact()
    );
}
fn push_int(
    builder: &mut CaptureBuilder,
    values: Vec<Option<i32>>,
    occurrences: &[usize],
    ty: &FunctionValueType,
    work: &CaptureWork<'_>,
) -> CapturedPageId {
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    let readable = readable(array.len(), work.owner);
    builder
        .push_evaluated(
            EvaluatedPage {
                arrays: &[array],
                bindings: std::slice::from_ref(ty),
                readable: &readable,
                occurrences,
            },
            &mut ValueConversionContext::default(),
            work,
        )
        .unwrap()
}

#[test]
fn captured_pages_preserve_occurrences_exact_null_minmax_and_shared_whole_hll() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domain = domain(&ty, super::super::NullPolicy::Unmatchable, &ports);
    let mut first = domain.begin_capture(&ports).unwrap();
    let first_id = push_int(
        &mut first,
        vec![Some(9), None, Some(9), Some(-2)],
        &[3, 0, 3, 1],
        &ty,
        &ports,
    );
    let mut second = domain.begin_capture(&ports).unwrap();
    let second_id = push_int(
        &mut second,
        vec![Some(5), Some(5), Some(8)],
        &[2, 0, 2, 1],
        &ty,
        &ports,
    );
    assert_ne!(first_id.run, second_id.run);
    assert_eq!(first_id.page, 0);
    assert_eq!(first_id.run, first.run_id());
    assert_eq!(first_id.run.index(), 0);
    assert_eq!(second_id.run.index(), 1);
    let first = first.seal(&ports).unwrap();
    let second = second.seal(&ports).unwrap();
    let pages = first.pages();
    assert_eq!((pages.len(), pages.occurrences()), (1, 4));
    let page = pages.page(0).unwrap();
    assert_eq!(page.id(), first_id);
    assert_eq!(page.len(), 4);
    assert_eq!(
        (0..4)
            .map(|i| page.input().row(i).unwrap())
            .collect::<Vec<_>>(),
        [3, 0, 3, 1]
    );
    assert_eq!(
        page.occurrence(2).unwrap(),
        BulkOccurrence {
            page: first_id,
            occurrence: 2
        }
    );
    assert_eq!(
        page.hashes().hash(0).unwrap(),
        page.hashes().hash(2).unwrap()
    );
    assert!(!page.hashes().participates(3).unwrap());
    assert_invariant(&error(page.occurrence(4)), ValueViolation::Bounds);
    let mut input = BulkInput::new(&domain, vec![second, first], &ports).unwrap();
    assert_eq!(input.len(), 2);
    assert!(input.domain().spec().same_instance(domain.spec()));
    assert_eq!(
        input
            .capture(first_id.run.index())
            .unwrap()
            .pages()
            .page(0)
            .unwrap()
            .id(),
        first_id
    );
    let mut stats = BuildRangeStats::empty_for_spec(domain.spec());
    let mut sketch =
        SizingSketch::new(domain.sizing_domain(), ports.host.clone(), &Control).unwrap();
    let mut run_ids = Vec::new();
    while let Some(capture) = input.pop_capture() {
        let (pages, counts, payload) = capture.into_parts();
        run_ids.push(pages.run_id());
        stats.merge_completed_range(counts, &|| Ok(())).unwrap();
        sketch.merge_payload(payload, &Control).unwrap();
    }
    assert_eq!(run_ids, [second_id.run, first_id.run]);
    assert_eq!(
        (
            stats.input_rows(),
            stats.indexable_rows(),
            stats.null_rows()
        ),
        (8, 7, 1)
    );
    assert_eq!(stats.integer().unwrap().min_max, Some((-2, 9)));
    assert_eq!(sketch.estimate_observed(&Control).unwrap(), 4);
}

#[test]
fn one_byte_hash_batch_per_capture_page_uses_only_selected_distinct_dictionary_bottoms() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let domain = domain(&ty, super::super::NullPolicy::Unmatchable, &ports);
    let mut builder = domain.begin_capture(&ports).unwrap();
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), Some(0), Some(1), None, Some(2)]),
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
        )
        .unwrap(),
    );
    let readable = readable(array.len(), owner.as_ref());
    super::super::local_hash::BYTE_HASH_INITS.with(|count| count.set(0));
    builder
        .push_evaluated(
            EvaluatedPage {
                arrays: &[array],
                bindings: std::slice::from_ref(&ty),
                readable: &readable,
                occurrences: &[2, 0, 1, 2, 4, 3],
            },
            &mut ValueConversionContext::default(),
            &ports,
        )
        .unwrap();
    assert_eq!(super::super::local_hash::BYTE_HASH_INITS.with(Cell::get), 2);
    let capture = builder.seal(&ports).unwrap();
    assert_eq!(
        (
            capture.counts().input_rows(),
            capture.counts().indexable_rows(),
            capture.counts().null_rows()
        ),
        (6, 4, 2)
    );
    let input = BulkInput::new(&domain, vec![capture], &ports).unwrap();
    let page = input.capture(0).unwrap().pages().page(0).unwrap();
    for _ in 0..8 {
        let prepared = PreparedKeyBatch::from_captured(
            domain.spec(),
            page.input(),
            ports.allocator.clone(),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(prepared.len(), 6);
        page.hashes()
            .validate(page.input(), domain.policy())
            .unwrap();
        assert_eq!(
            page.hashes().hash(0).unwrap(),
            page.hashes().hash(1).unwrap()
        );
        assert_eq!(
            page.hashes().hash(0).unwrap(),
            page.hashes().hash(3).unwrap()
        );
    }
    assert_eq!(super::super::local_hash::BYTE_HASH_INITS.with(Cell::get), 2);
}

#[test]
fn empty_run_seals_once_and_numeric_run_replay_or_missing_claim_cannot_certify_completion() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domain0 = domain(&ty, super::super::NullPolicy::Equal, &ports);
    assert_eq!(BulkInput::new(&domain0, vec![], &ports).unwrap().len(), 0);
    let domain1 = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let mut builder = domain1.begin_capture(&ports).unwrap();
    let capture = builder.seal(&ports).unwrap();
    assert_eq!(
        (capture.pages().len(), capture.counts().input_rows()),
        (0, 0)
    );
    let input = BulkInput::new(&domain1, vec![capture], &ports).unwrap();
    assert_eq!(input.len(), 1);
    domain1.check_failure().unwrap();

    let replay_domain = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let mut replay_builder = replay_domain.begin_capture(&ports).unwrap();
    let receipt = replay_builder.seal(&ports).unwrap();
    let replay_cause = error(replay_builder.seal(&ports));
    assert_invariant(&replay_cause, ValueViolation::RowTransaction);
    assert_eq!(
        error(BulkInput::new(&replay_domain, vec![receipt], &ports)),
        replay_cause
    );
    assert_eq!(error(replay_domain.check_failure()), replay_cause);

    let closed_domain = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let closed_input = BulkInput::new(&closed_domain, vec![], &ports).unwrap();
    let closed_cause = error(closed_domain.begin_capture(&ports));
    assert_invariant(&closed_cause, ValueViolation::RowTransaction);
    assert_eq!(error(closed_domain.check_failure()), closed_cause);
    // Existing immutable data is not rolled back; a future graph final must
    // inspect check_failure before first publishing its own Ready state.
    assert_eq!(closed_input.len(), 0);

    let domain2 = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let mut a = domain2.begin_capture(&ports).unwrap();
    let mut b = domain2.begin_capture(&ports).unwrap();
    let a = a.seal(&ports).unwrap();
    let mut b = b.seal(&ports).unwrap();
    // Only this private child test can forge a duplicate run field. Production
    // callers cannot clone either a claim or a seal, or supply an arbitrary id.
    b.directory.run = a.directory.run;
    assert_invariant(
        &error(BulkInput::new(&domain2, vec![a, b], &ports)),
        ValueViolation::RowTransaction,
    );

    let domain3 = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let mut a = domain3.begin_capture(&ports).unwrap();
    let _unfinished = domain3.begin_capture(&ports).unwrap();
    let a = a.seal(&ports).unwrap();
    assert_invariant(
        &error(BulkInput::new(&domain3, vec![a], &ports)),
        ValueViolation::RowTransaction,
    );
}

#[test]
fn semantically_equal_but_independent_bulk_domain_is_foreign_to_assembly_and_hll_merge() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[super::super::NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let domain_a = BulkDomain::new(&spec, owner.as_ref(), &Control).unwrap();
    let domain_b = BulkDomain::new(&spec, owner.as_ref(), &Control).unwrap();
    let mut a = domain_a.begin_capture(&ports).unwrap();
    let mut b = domain_b.begin_capture(&ports).unwrap();
    let _a = a.seal(&ports).unwrap();
    let b = b.seal(&ports).unwrap();
    assert_invariant(
        &error(BulkInput::new(&domain_a, vec![b], &ports)),
        ValueViolation::Layout,
    );
    let mut sketch =
        SizingSketch::new(domain_a.sizing_domain(), ports.host.clone(), &Control).unwrap();
    let mut b = domain_b.begin_capture(&ports).unwrap();
    let (_, _, payload) = b.seal(&ports).unwrap().into_parts();
    assert_invariant(
        &error(sketch.merge_payload(payload, &Control)),
        ValueViolation::Layout,
    );
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
fn original_capture_growth_failure_latches_and_cannot_be_bypassed_by_sealing_other_runs() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domain = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let mut builder = domain.begin_capture(&ports).unwrap();
    push_int(&mut builder, vec![Some(1)], &[0, 0], &ty, &ports);
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let refused_ports = work(&refusing, &tracker, &Control);
    let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(2)]));
    let rows = readable(1, owner.as_ref());
    assert_eq!(
        error(builder.push_evaluated(
            EvaluatedPage {
                arrays: &[array],
                bindings: std::slice::from_ref(&ty),
                readable: &rows,
                occurrences: &[0],
            },
            &mut ValueConversionContext::default(),
            &refused_ports
        )),
        cause
    );
    assert_eq!(error(builder.seal(&ports)), cause);
    assert_eq!(error(domain.begin_capture(&ports)), cause);
    assert_eq!(error(BulkInput::new(&domain, vec![], &ports)), cause);
}

#[derive(Default)]
struct CountingControl(AtomicUsize);
impl KernelEvaluationControl for CountingControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("capture does not wait")
    }
}
#[test]
fn assembling_one_sealed_run_does_not_inspect_its_page_or_row_count() {
    let (owner, tracker) = fixture();
    let control = CountingControl::default();
    let ports = work(owner.as_ref(), &tracker, &control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let mut assembly_observations = Vec::new();
    for page_count in [1, 32] {
        let domain = domain(&ty, super::super::NullPolicy::Equal, &ports);
        let mut builder = domain.begin_capture(&ports).unwrap();
        for _ in 0..page_count {
            // Identical NDV leaves the HLL serialization shape unchanged;
            // only captured page/occurrence counts differ between the runs.
            push_int(&mut builder, vec![Some(7)], &[0, 0, 0], &ty, &ports);
        }
        control.0.store(0, Ordering::SeqCst);
        let capture = builder.seal(&ports).unwrap();
        let seal_observations = control.0.load(Ordering::SeqCst);
        control.0.store(0, Ordering::SeqCst);
        let input = BulkInput::new(&domain, vec![capture], &ports).unwrap();
        assembly_observations.push((seal_observations, control.0.load(Ordering::SeqCst)));
        assert_eq!(input.capture(0).unwrap().pages().len(), page_count as usize);
    }
    assert!(assembly_observations[0].0 > 0);
    assert!(assembly_observations[0].1 > 0);
    assert_eq!(assembly_observations[0], assembly_observations[1]);
}

struct StoppedControl;
impl KernelEvaluationControl for StoppedControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Err(KernelFailure::DeadlineExceeded)
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("capture does not wait")
    }
}
#[test]
fn refused_seal_keeps_original_control_cause_and_cannot_return_a_partial_receipt() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domain = domain(&ty, super::super::NullPolicy::Equal, &ports);
    let mut builder = domain.begin_capture(&ports).unwrap();
    push_int(&mut builder, vec![Some(4)], &[0], &ty, &ports);
    let stopped_ports = work(owner.as_ref(), &tracker, &StoppedControl);
    let cause = ExecutionFailure::from(KernelFailure::DeadlineExceeded);
    assert_eq!(error(builder.seal(&stopped_ports)), cause);
    assert_eq!(error(builder.seal(&ports)), cause);
    assert_eq!(error(BulkInput::new(&domain, vec![], &ports)), cause);
}
