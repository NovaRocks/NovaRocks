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

//! Exact local phase/custody properties use ordinary isolated owner tests.
//! Hand-written sorted integer classes and physical occurrence coordinates are
//! the oracle, not the cursor implementation. No cluster or timing gate is used.

use super::super::{
    KeyId, KeyMatch, KeySpec, NullPolicy,
    capture::{BulkInput, CaptureWork, EvaluatedPage},
};
use super::*;
use crate::{
    exec::{
        hash_table::allocation::AggregateAllocator, pipeline::operator::InputConstructionOwner,
    },
    runtime::{mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host},
};
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{
    ValueError, decode::ValueConversionContext, reachability::ReachableRows,
};
use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, Ordering},
};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("local data tasks never wait")
    }
}
fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("bulk task original owner");
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
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
type ExpectedPages = Vec<(CapturedPageId, Vec<(usize, Option<i64>)>)>;
fn input(purpose: IndexPurpose, ports: &DirectWork<'_>) -> (CompleteInput, ExpectedPages) {
    let spec = KeySpec::new(
        &[FunctionValueType::new(DataType::Int64, false)],
        &[NullPolicy::Equal],
        ports.owner,
        &|| Ok(()),
    )
    .unwrap();
    let domain = BulkDomain::new(&spec, ports.owner, ports.control).unwrap();
    let capture = CaptureWork {
        owner: ports.owner,
        allocator: ports.allocator.clone(),
        host: Arc::clone(&ports.host),
        control: ports.control,
    };
    let mut captures = Vec::new();
    let mut expected = Vec::new();
    for run in 0..2 {
        let mut writer = domain.begin_capture(&capture).unwrap();
        for page in 0..2 {
            let part = run * 2 + page;
            let values: Vec<_> = (0..600)
                .map(|i| {
                    if i % 97 == 0 {
                        None
                    } else {
                        Some(((part * 600 + i) % 1024) as i64 - 512)
                    }
                })
                .collect();
            let mut rows: Vec<_> = (0..600).rev().collect();
            rows.extend([13, 13, 0]);
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let readable = ReachableRows::all(array.len())
                .materialize_observed(
                    &mut |layout| ports.owner.request(layout).map_err(ExecutionFailure::from),
                    &mut |_| Ok(()),
                )
                .unwrap();
            let id = writer
                .push_evaluated(
                    EvaluatedPage {
                        arrays: &[array],
                        bindings: &[FunctionValueType::new(DataType::Int64, true)],
                        readable: &readable,
                        occurrences: &rows,
                    },
                    &mut ValueConversionContext::default(),
                    &capture,
                )
                .unwrap();
            expected.push((id, rows.into_iter().map(|row| (row, values[row])).collect()));
        }
        captures.push(writer.seal(&capture).unwrap());
    }
    let complete = CompleteInput::from_captures(
        BulkInput::new(&domain, captures, &capture).unwrap(),
        purpose,
        &capture,
    )
    .unwrap();
    (complete, expected)
}
fn graph(purpose: IndexPurpose, ports: &DirectWork<'_>) -> (BulkTaskGraph, ExpectedPages) {
    let (input, expected) = input(purpose, ports);
    (
        BulkTaskGraph::begin(
            input,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            ports,
        )
        .unwrap(),
        expected,
    )
}
fn task(graph: &BulkTaskGraph, ports: &DirectWork<'_>) -> Task {
    match graph.claim(ports).unwrap() {
        Claim::Task(task) => task,
        _ => panic!("a real task is ready"),
    }
}
fn finish(mut task: Task, ports: &DirectWork<'_>) -> Completion {
    while !task.advance(17, ports).unwrap() {}
    task.finish().unwrap()
}
fn error<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    result.err().expect("the original operation must refuse")
}
fn assert_invariant(cause: &ExecutionFailure) {
    assert_eq!(
        cause.failure_fact(),
        ValueError::Invariant(ValueViolation::RowTransaction).failure_fact()
    );
}
fn claim_phase(graph: &BulkTaskGraph, phase: Phase, ports: &DirectWork<'_>) -> Task {
    loop {
        let task = match graph.claim(ports).unwrap() {
            Claim::Task(task) => task,
            _ => panic!("all prior tasks completed, a successor must progress"),
        };
        if task.phase() == phase {
            return task;
        }
        let mut completed = finish(task, ports);
        graph.complete(&mut completed, ports).unwrap();
    }
}

#[test]
fn direct_graph_uses_multiple_real_workers_and_preserves_all_physical_occurrences_and_slot_order_ids()
 {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (graph, expected) = graph(IndexPurpose::KeyIds, &ports);
    let mut workers_by_phase = BTreeMap::<String, BTreeSet<usize>>::new();
    loop {
        let mut tasks = Vec::new();
        for _ in 0..2 {
            match graph.claim(&ports).unwrap() {
                Claim::Task(task) => tasks.push(task),
                Claim::Pending | Claim::Finished => break,
            }
        }
        if tasks.is_empty() {
            break;
        }
        assert!(graph.take_ready(&ports).unwrap().is_none());
        assert!(matches!(graph.claim(&ports).unwrap(), Claim::Pending));
        let phase = tasks[0].phase();
        assert!(tasks.iter().all(|task| task.phase() == phase));
        let mut completed = std::thread::scope(|scope| {
            let joins: Vec<_> = tasks
                .into_iter()
                .enumerate()
                .map(|(worker, task)| {
                    scope.spawn(move || {
                        let (owner, tracker) = fixture();
                        let ports = work(owner.as_ref(), &tracker, &Control);
                        (worker, finish(task, &ports), tracker)
                    })
                })
                .collect();
            joins
                .into_iter()
                .map(|join| join.join().unwrap())
                .collect::<Vec<_>>()
        });
        // Completion order does not become ID order or a phase shortcut.
        completed.reverse();
        for (worker, mut complete, _) in completed {
            workers_by_phase
                .entry(format!("{phase:?}"))
                .or_default()
                .insert(worker);
            graph.complete(&mut complete, &ports).unwrap();
        }
    }
    let ready = graph
        .take_ready(&ports)
        .unwrap()
        .expect("every phase barrier is closed");
    assert!(graph.take_ready(&ports).unwrap().is_none());
    for phase in [
        Phase::Init,
        Phase::Mark,
        Phase::Count,
        Phase::Prefix,
        Phase::Fill,
        Phase::Assign,
    ] {
        assert_eq!(
            workers_by_phase[&format!("{phase:?}")].len(),
            2,
            "{phase:?}"
        );
    }
    let values: BTreeSet<_> = expected
        .iter()
        .flat_map(|(_, rows)| rows.iter().filter_map(|(_, value)| *value))
        .collect();
    let ids: BTreeMap<_, _> = values
        .iter()
        .enumerate()
        .map(|(id, value)| (*value, KeyId(id)))
        .collect();
    let requested = [
        KeyId(values.len()),
        KeyId(0),
        KeyId(values.len() - 1),
        KeyId(0),
    ]
    .map(|id| BulkKey { partition: 0, id });
    let read = ready
        .read_keys(&requested, owner.as_ref(), &Control)
        .unwrap();
    assert_eq!(read.len(), requested.len());
    drop(read);
    let exported = ready
        .export_keys(
            &requested,
            &[FunctionValueType::new(DataType::Int64, true)],
            owner.as_ref(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![
            None,
            values.first().copied(),
            values.last().copied(),
            values.first().copied()
        ])
    );
    drop(exported);
    assert_eq!(
        ready.lookup_integer(None).unwrap(),
        KeyMatch::Found(KeyId(values.len()))
    );
    assert_eq!(ready.lookup_integer(Some(10_000)).unwrap(), KeyMatch::Miss);
    let mut seen = BTreeSet::new();
    for chunk in ready.assignments() {
        let (_, rows) = expected.iter().find(|(id, _)| *id == chunk.page()).unwrap();
        for (ordinal, found) in chunk.matches().iter().enumerate() {
            let occurrence = chunk.start() + ordinal;
            assert!(seen.insert((chunk.page().run.index(), chunk.page().page, occurrence)));
            assert_eq!(chunk.row(ordinal).unwrap(), rows[occurrence].0);
            let expected = rows[occurrence]
                .1
                .map_or(KeyId(values.len()), |value| ids[&value]);
            assert_eq!(
                *found,
                BulkMatch::Found(BulkKey {
                    partition: 0,
                    id: expected
                })
            );
        }
    }
    assert_eq!(
        seen.len(),
        expected.iter().map(|(_, rows)| rows.len()).sum::<usize>()
    );
    for value in values {
        assert_eq!(
            ready.lookup_integer(Some(value)).unwrap(),
            KeyMatch::Found(ids[&value])
        );
    }
    drop(ready);
    drop(graph);
    drop(ports);
    drop(owner);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn mark_cannot_begin_until_every_init_claim_is_completed_even_when_a_cursor_is_done() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (graph, _) = graph(IndexPurpose::KeyIds, &ports);
    let first = task(&graph, &ports);
    let second = task(&graph, &ports);
    let mut first = finish(first, &ports);
    assert_eq!(graph.phase().unwrap(), Phase::Init);
    assert!(graph.take_ready(&ports).unwrap().is_none());
    graph.complete(&mut first, &ports).unwrap();
    assert!(matches!(graph.claim(&ports).unwrap(), Claim::Pending));
    let mut second = finish(second, &ports);
    assert!(matches!(graph.claim(&ports).unwrap(), Claim::Pending));
    graph.complete(&mut second, &ports).unwrap();
    let mark = task(&graph, &ports);
    assert_eq!(mark.phase(), Phase::Mark);
}

#[test]
fn presence_graph_publishes_after_mark_without_ranking_representatives_or_assignment() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (graph, _) = graph(IndexPurpose::PresenceOnly, &ports);
    loop {
        match graph.claim(&ports).unwrap() {
            Claim::Task(task) => {
                assert!(matches!(task.phase(), Phase::Init | Phase::Mark));
                let mut completed = finish(task, &ports);
                graph.complete(&mut completed, &ports).unwrap();
            }
            Claim::Finished => break,
            Claim::Pending => panic!("serial caller has no inflight work"),
        }
    }
    let ready = graph.take_ready(&ports).unwrap().unwrap();
    assert!(ready.contains_integer(None).unwrap());
    assert!(ready.contains_integer(Some(0)).unwrap());
    assert!(!ready.contains_integer(Some(100_000)).unwrap());
    assert_eq!(ready.assignments().count(), 0);
    assert_eq!(
        error(ready.read_keys(&[], owner.as_ref(), &Control)).failure_fact(),
        ValueError::Invariant(ValueViolation::Layout).failure_fact()
    );
    assert_eq!(
        error(ready.lookup_integer(Some(0))).failure_fact(),
        ValueError::Invariant(ValueViolation::Layout).failure_fact()
    );
}

#[test]
fn lost_task_or_lost_completed_output_blocks_publication_and_releases_actual_sources_on_exit() {
    for completed in [false, true] {
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let (graph, _) = graph(IndexPurpose::KeyIds, &ports);
        let mut task = task(&graph, &ports);
        assert!(!task.advance(0, &ports).unwrap());
        if completed {
            drop(finish(task, &ports));
        } else {
            task.advance(1, &ports).unwrap();
            drop(task);
        }
        assert_invariant(&error(graph.take_ready(&ports)));
        assert_invariant(&error(graph.claim(&ports)));
        drop(graph);
        drop(ports);
        drop(owner);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn repeated_and_foreign_completion_are_rejected_without_a_successful_barrier() {
    for foreign in [false, true] {
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let (original, _) = graph(IndexPurpose::KeyIds, &ports);
        let (other, _) = graph(IndexPurpose::KeyIds, &ports);
        let mut completed = finish(task(&original, &ports), &ports);
        if foreign {
            let cause = error(other.complete(&mut completed, &ports));
            assert_invariant(&cause);
            assert_eq!(error(original.take_ready(&ports)), cause);
            assert_eq!(error(other.take_ready(&ports)), cause);
        } else {
            original.complete(&mut completed, &ports).unwrap();
            let cause = error(original.complete(&mut completed, &ports));
            assert_invariant(&cause);
            assert_eq!(error(original.take_ready(&ports)), cause);
        }
    }
}

#[test]
fn unfinished_finish_returns_the_same_first_cause_as_the_graph_not_a_new_drop_failure() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (graph, _) = graph(IndexPurpose::KeyIds, &ports);
    let cause = error(task(&graph, &ports).finish());
    assert_invariant(&cause);
    assert_eq!(error(graph.take_ready(&ports)), cause);
}

struct Stopped(AtomicBool);
impl KernelEvaluationControl for Stopped {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        if self.0.load(Ordering::Acquire) {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("data tasks never wait")
    }
}
#[test]
fn original_control_failure_in_every_direct_phase_prevents_success_and_cannot_be_retried() {
    for target in [
        Phase::Init,
        Phase::Mark,
        Phase::Count,
        Phase::Prefix,
        Phase::Fill,
        Phase::Null,
        Phase::Assign,
    ] {
        for after_progress in [false, true] {
            let (owner, tracker) = fixture();
            let ports = work(owner.as_ref(), &tracker, &Control);
            let (graph, _) = graph(IndexPurpose::KeyIds, &ports);
            let mut task = claim_phase(&graph, target, &ports);
            assert_eq!(task.phase(), target);
            let stopped = Stopped(AtomicBool::new(false));
            let controlled = work(owner.as_ref(), &tracker, &stopped);
            if after_progress {
                // The small NULL task may already be completed; failing the
                // following complete still must stop publication.
                if task.advance(1, &controlled).unwrap() {
                    let mut complete = task.finish().unwrap();
                    stopped.0.store(true, Ordering::Release);
                    let cause = error(graph.complete(&mut complete, &controlled));
                    assert_eq!(error(graph.take_ready(&ports)), cause);
                    continue;
                }
            }
            stopped.0.store(true, Ordering::Release);
            let cause = error(task.advance(1, &controlled));
            assert_eq!(
                cause,
                ExecutionFailure::from(KernelFailure::DeadlineExceeded)
            );
            assert_eq!(error(graph.claim(&ports)), cause);
            assert_eq!(error(graph.take_ready(&ports)), cause);
        }
    }
}

struct Refusing<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
impl InputConstructionOwner for Refusing<'_> {
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
fn a_claimed_constructor_refusal_preserves_original_failure_before_abandonment() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (graph, _) = graph(IndexPurpose::KeyIds, &ports);
    let cause = invariant(ValueViolation::Bounds);
    let refusing = Refusing(owner.as_ref(), cause.clone());
    let refused = work(&refusing, &tracker, &Control);
    assert_eq!(error(graph.claim(&refused)), cause);
    assert_eq!(error(graph.take_ready(&ports)), cause);
}

#[test]
fn lazy_input_ranges_observe_original_control_while_skipping_empty_runs() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let capture = CaptureWork {
        owner: ports.owner,
        allocator: ports.allocator.clone(),
        host: Arc::clone(&ports.host),
        control: ports.control,
    };
    let spec = KeySpec::new(
        &[FunctionValueType::new(DataType::Int64, false)],
        &[NullPolicy::Equal],
        ports.owner,
        &|| Ok(()),
    )
    .unwrap();
    let domain = BulkDomain::new(&spec, ports.owner, ports.control).unwrap();
    let mut captures = Vec::new();
    for _ in 0..513 {
        captures.push(
            domain
                .begin_capture(&capture)
                .unwrap()
                .seal(&capture)
                .unwrap(),
        );
    }
    let input = CompleteInput::from_captures(
        BulkInput::new(&domain, captures, &capture).unwrap(),
        IndexPurpose::KeyIds,
        &capture,
    )
    .unwrap();
    let checks = std::cell::Cell::new(0);
    let cause = invariant(ValueViolation::Bounds);
    let mut ranges = InputRanges::new();
    assert_eq!(
        error(ranges.next(input.runs(), &|| {
            checks.set(checks.get() + 1);
            if checks.get() == 2 {
                Err(cause.clone())
            } else {
                Ok(())
            }
        })),
        cause
    );
    assert_eq!(checks.get(), 2);
    assert_eq!(ranges.run, 256);
}

#[path = "bulk_tasks_hash_tests.rs"]
mod hash_tests;
