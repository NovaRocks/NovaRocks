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

//! Complete capture facts use hand-written classes and row coordinates. These
//! owner tests require no cluster and do not certify later task completion.

use super::super::{
    bulk::{CompleteInput, NdvMethod},
    capture::{BulkDomain, BulkInput, CaptureBuilder, CaptureWork, CapturedPageId, EvaluatedPage},
};
use super::*;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "strategy_tests.rs"]
mod strategy_tests;

fn work<'a>(
    owner: &'a dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
    control: &'a dyn KernelEvaluationControl,
) -> CaptureWork<'a> {
    CaptureWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        control,
    }
}
fn setup(ports: &CaptureWork<'_>, nulls: NullPolicy) -> BulkDomain {
    let spec = KeySpec::new(
        &[FunctionValueType::new(DataType::Int64, false)],
        &[nulls],
        ports.owner,
        &|| Ok(()),
    )
    .unwrap();
    BulkDomain::new(&spec, ports.owner, ports.control).unwrap()
}
fn page(
    builder: &mut CaptureBuilder,
    values: Vec<Option<i64>>,
    nullable: bool,
    occurrences: &[usize],
    ports: &CaptureWork<'_>,
) -> CapturedPageId {
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    builder
        .push_evaluated(
            EvaluatedPage {
                arrays: std::slice::from_ref(&array),
                bindings: &[FunctionValueType::new(DataType::Int64, nullable)],
                readable: &readable(array.len()),
                occurrences,
            },
            &mut ValueConversionContext::default(),
            ports,
        )
        .unwrap()
}
fn error<T>(value: ExecutionResult<T>) -> ExecutionFailure {
    value.err().expect("the requested operation must fail")
}

#[test]
fn complete_input_merges_whole_counts_ndv_and_original_run_page_occurrence_coordinates() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    let domain = setup(&ports, NullPolicy::Equal);
    let mut a = domain.begin_capture(&ports).unwrap();
    let a_id = page(
        &mut a,
        vec![Some(4), Some(-2), Some(4)],
        false,
        &[2, 0, 1, 2],
        &ports,
    );
    let mut b = domain.begin_capture(&ports).unwrap();
    let b_id = page(
        &mut b,
        vec![Some(4), None, Some(9)],
        true,
        &[1, 2, 0, 1],
        &ports,
    );
    let input = BulkInput::new(
        &domain,
        vec![b.seal(&ports).unwrap(), a.seal(&ports).unwrap()],
        &ports,
    )
    .unwrap();
    let complete = CompleteInput::from_captures(input, IndexPurpose::KeyIds, &ports).unwrap();
    let stats = complete.stats();
    stats.check().unwrap();
    assert_eq!(stats.purpose(), IndexPurpose::KeyIds);
    assert!(stats.domain().spec().same_instance(domain.spec()));
    assert_eq!(
        (
            stats.exact().input_rows(),
            stats.exact().indexable_rows(),
            stats.exact().null_rows()
        ),
        (8, 8, 2)
    );
    assert_eq!(stats.exact().integer().unwrap().min_max, Some((-2, 9)));
    assert_eq!(stats.exact().integer_root_nullable_union(), Some(true));
    assert_eq!(stats.hint().method(), NdvMethod::WholeInputHll);
    assert_eq!(stats.hint().estimate(), 4); // {-2, 4, 9, NULL}
    assert_eq!(stats.hint().initial_reservation(), 5);
    assert_eq!(complete.runs().len(), 2);
    let reference = complete.runs().page(a_id).unwrap().occurrence(0).unwrap();
    assert_eq!(complete.runs().row(reference).unwrap(), 2);
    let reference = complete.runs().page(b_id).unwrap().occurrence(3).unwrap();
    assert_eq!(complete.runs().row(reference).unwrap(), 1);

    let foreign = setup(&ports, NullPolicy::Equal);
    assert_value_failure(
        &error(stats.hint().validate(&foreign, stats.exact())),
        ValueViolation::Layout,
    );
}

#[test]
fn complete_empty_participating_domain_uses_exact_absence_and_preserves_signed_empty_declarations()
{
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    for kind in 0..3 {
        let domain = setup(&ports, NullPolicy::Unmatchable);
        let mut captures = Vec::new();
        if kind != 0 {
            let mut builder = domain.begin_capture(&ports).unwrap();
            if kind == 1 {
                page(
                    &mut builder,
                    vec![None, None],
                    true,
                    &[1, 0, 1, 0, 1, 0],
                    &ports,
                );
            } else {
                // No NULL values and no occurrences; the actual signed input
                // is still nullable. Neither counts nor spec can infer this.
                page(&mut builder, vec![], true, &[], &ports);
            }
            captures.push(builder.seal(&ports).unwrap());
        }
        let input = BulkInput::new(&domain, captures, &ports).unwrap();
        let complete =
            CompleteInput::from_captures(input, IndexPurpose::PresenceOnly, &ports).unwrap();
        assert_eq!(complete.stats().purpose(), IndexPurpose::PresenceOnly);
        assert_eq!(
            complete.stats().hint().method(),
            NdvMethod::EmptyIndexableDomain
        );
        assert_eq!(complete.stats().hint().estimate(), 0);
        assert_eq!(complete.stats().hint().initial_reservation(), 0);
        assert_eq!(complete.stats().exact().indexable_rows(), 0);
        assert_eq!(
            complete.stats().exact().input_rows(),
            if kind == 1 { 6 } else { 0 }
        );
        assert_eq!(complete.stats().exact().integer().unwrap().min_max, None);
        assert_eq!(
            complete.stats().exact().integer_root_nullable_union(),
            if kind == 0 { None } else { Some(true) }
        );
    }
}

#[test]
fn partially_drained_capture_set_cannot_be_promoted_to_complete_input() {
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    let domain = setup(&ports, NullPolicy::Equal);
    let mut builder = domain.begin_capture(&ports).unwrap();
    page(&mut builder, vec![Some(7)], false, &[0], &ports);
    let mut input = BulkInput::new(&domain, vec![builder.seal(&ports).unwrap()], &ports).unwrap();
    assert_eq!(input.sealed_runs(), 1);
    drop(input.pop_capture().unwrap());
    assert_eq!(input.len(), 0);
    let cause = error(CompleteInput::from_captures(
        input,
        IndexPurpose::KeyIds,
        &ports,
    ));
    assert_value_failure(&cause, ValueViolation::RowTransaction);
    assert!(error(domain.check_failure()).same_occurrence(&cause));
}

struct Tag(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Tag {}
impl Drop for Tag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn tagged<'a>(
    owner: &'a dyn InputConstructionOwner,
    drops: &Arc<AtomicUsize>,
) -> TaggedCreator<'a> {
    TaggedCreator {
        original: owner,
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    }
}
#[test]
fn escaped_completed_directory_keeps_capture_and_directory_creators_until_last_actual_alias() {
    let (owner, tracker) = owner_with_tracker();
    let source_drops = Arc::new(AtomicUsize::new(0));
    let directory_drops = Arc::new(AtomicUsize::new(0));
    let domain = setup(
        &work(owner.as_ref(), &tracker, &CopyControl),
        NullPolicy::Equal,
    );
    let source_owner = tagged(owner.as_ref(), &source_drops);
    let source_ports = work(&source_owner, &tracker, &CopyControl);
    let mut builder = domain.begin_capture(&source_ports).unwrap();
    let id = page(&mut builder, vec![Some(-7)], false, &[0, 0], &source_ports);
    let input = BulkInput::new(
        &domain,
        vec![builder.seal(&source_ports).unwrap()],
        &source_ports,
    )
    .unwrap();
    let directory_owner = tagged(owner.as_ref(), &directory_drops);
    let complete = CompleteInput::from_captures(
        input,
        IndexPurpose::KeyIds,
        &work(&directory_owner, &tracker, &CopyControl),
    )
    .unwrap();
    let escaped = complete.runs().clone();
    drop(complete);
    drop(builder);
    drop(source_owner);
    drop(directory_owner);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(directory_drops.load(Ordering::SeqCst), 0);
    let array = escaped.page(id).unwrap().input().arrays()[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(array.value(0), -7);
    drop(escaped);
    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(directory_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn complete_input_original_growth_and_control_refusals_latch_without_publishing() {
    struct Stop;
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Err(KernelFailure::Cancelled)
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            unreachable!()
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ports = work(owner.as_ref(), &tracker, &CopyControl);
    for stop in [false, true] {
        let domain = setup(&ports, NullPolicy::Equal);
        let mut builder = domain.begin_capture(&ports).unwrap();
        page(&mut builder, vec![Some(1)], false, &[0], &ports);
        let input = BulkInput::new(&domain, vec![builder.seal(&ports).unwrap()], &ports).unwrap();
        let cause: ExecutionFailure = if stop {
            KernelFailure::Cancelled.into()
        } else {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
            .into()
        };
        let refusing = RefusingOwner(owner.as_ref(), cause.clone());
        let stopped = work(owner.as_ref(), &tracker, &Stop);
        let refused = work(&refusing, &tracker, &CopyControl);
        assert_eq!(
            error(CompleteInput::from_captures(
                input,
                IndexPurpose::KeyIds,
                if stop { &stopped } else { &refused }
            )),
            cause
        );
        assert_eq!(error(domain.check_failure()), cause);
    }
}

#[derive(Default)]
struct ControlCount(AtomicUsize);
impl KernelEvaluationControl for ControlCount {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        unreachable!()
    }
}
#[test]
fn complete_input_coordinator_work_depends_on_runs_and_sketches_without_rescanning_pages() {
    let (owner, tracker) = owner_with_tracker();
    let control = ControlCount::default();
    let ports = work(owner.as_ref(), &tracker, &control);
    let mut observations = Vec::new();
    for pages in [1, 64] {
        let domain = setup(&ports, NullPolicy::Equal);
        let mut builder = domain.begin_capture(&ports).unwrap();
        for _ in 0..pages {
            page(&mut builder, vec![Some(5)], false, &[0, 0, 0], &ports);
        }
        let input = BulkInput::new(&domain, vec![builder.seal(&ports).unwrap()], &ports).unwrap();
        control.0.store(0, Ordering::SeqCst);
        let complete = CompleteInput::from_captures(input, IndexPurpose::KeyIds, &ports).unwrap();
        observations.push(control.0.load(Ordering::SeqCst));
        assert_eq!(complete.stats().hint().estimate(), 1);
        assert_eq!(complete.stats().exact().input_rows(), 3 * pages);
        assert_eq!(
            complete.stats().exact().integer_root_nullable_union(),
            Some(false)
        );
    }
    assert!(observations[0] > 0);
    assert_eq!(observations[0], observations[1]);
}
