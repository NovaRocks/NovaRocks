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

//! Common graph obligations not covered by low-level cursor tests: eager
//! merge progress, exact whole-route closure, independent physical occurrence
//! and value oracles, real worker turns and immutable publication boundaries.
use super::super::super::{bulk_read::BulkKey, local_hash::BYTE_HASH_INITS};
use super::*;
use arrow::array::StringArray;

type BytePages = Vec<(CapturedPageId, Vec<(usize, Option<String>)>)>;
fn bytes_input(
    purpose: IndexPurpose,
    nulls: NullPolicy,
    zero_hash: bool,
    pages: usize,
    page_rows: usize,
    all_null: bool,
    ports: &DirectWork<'_>,
) -> (CompleteInput, BytePages) {
    let spec = KeySpec::new(
        &[FunctionValueType::new(DataType::Utf8, true)],
        &[nulls],
        ports.owner,
        &|| Ok(()),
    )
    .unwrap();
    let domain = if zero_hash {
        BulkDomain::new_forced_hash_for_test(&spec, ports.owner, ports.control, 0).unwrap()
    } else {
        BulkDomain::new(&spec, ports.owner, ports.control).unwrap()
    };
    let capture = CaptureWork {
        owner: ports.owner,
        allocator: ports.allocator.clone(),
        host: Arc::clone(&ports.host),
        control: ports.control,
    };
    let mut writer = domain.begin_capture(&capture).unwrap();
    let mut expected = Vec::new();
    for p in 0..pages {
        let values: Vec<Option<String>> = (0..page_rows)
            .map(|i| {
                if all_null || (p * page_rows + i).is_multiple_of(13) {
                    None
                } else {
                    Some(format!("key-{:02}-é中", (p * page_rows + i) % 67))
                }
            })
            .collect();
        let array: ArrayRef = Arc::new(StringArray::from(
            values.iter().map(Option::as_deref).collect::<Vec<_>>(),
        ));
        let readable = ReachableRows::all(array.len())
            .materialize_observed(
                &mut |layout| ports.owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| Ok(()),
            )
            .unwrap();
        let mut rows: Vec<usize> = (0..page_rows).rev().collect();
        if page_rows > 3 {
            rows.extend([3, 3, 0]);
        }
        let id = writer
            .push_evaluated(
                EvaluatedPage {
                    arrays: &[array],
                    bindings: &[FunctionValueType::new(DataType::Utf8, true)],
                    readable: &readable,
                    occurrences: &rows,
                },
                &mut ValueConversionContext::default(),
                &capture,
            )
            .unwrap();
        expected.push((
            id,
            rows.into_iter()
                .map(|row| (row, values[row].clone()))
                .collect(),
        ));
    }
    let captured = writer.seal(&capture).unwrap();
    let complete = CompleteInput::from_captures(
        BulkInput::new(&domain, vec![captured], &capture).unwrap(),
        purpose,
        &capture,
    )
    .unwrap();
    (complete, expected)
}

#[expect(
    clippy::large_enum_variant,
    reason = "Exercises the original inline linear task/completion transition without a new carrier allocation."
)]
enum Step {
    Retained(Task),
    Complete(Completion),
}
/// At most two real workers advance at once. Retained route data may coexist
/// with merge leases, but do not create extra CPU positions or a second graph.
fn drive(
    graph: &BulkTaskGraph,
    ports: &DirectWork<'_>,
    stop: Option<Phase>,
) -> (
    Option<BulkReady>,
    BTreeMap<String, BTreeSet<usize>>,
    Option<Task>,
) {
    let mut pending = Vec::new();
    let mut workers = BTreeMap::<String, BTreeSet<usize>>::new();
    for _ in 0..20_000 {
        if pending.is_empty() && stop == Some(graph.phase().unwrap()) {
            return (None, workers, None);
        }
        while let Claim::Task(task) = graph.claim(ports).unwrap() {
            if stop == Some(task.phase()) {
                assert!(pending.is_empty());
                return (None, workers, Some(task));
            }
            pending.push(task);
        }
        if pending.is_empty() {
            let ready = graph
                .take_ready(ports)
                .unwrap()
                .expect("all selected work drained");
            return (Some(ready), workers, None);
        }
        assert!(graph.take_ready(ports).unwrap().is_none());
        let mut next = Vec::new();
        let mut inputs = pending.into_iter();
        loop {
            let group: Vec<Task> = inputs.by_ref().take(2).collect();
            if group.is_empty() {
                break;
            }
            let mut results = std::thread::scope(|scope| {
                let joins: Vec<_> =
                    group
                        .into_iter()
                        .enumerate()
                        .map(|(worker, mut task)| {
                            scope.spawn(move || {
                        let phase = task.phase();
                        let (owner, tracker) = fixture();
                        let ports = work(owner.as_ref(), &tracker, &Control);
                        let before = BYTE_HASH_INITS.with(|n| n.get());
                        let done = task.advance(17, &ports).unwrap();
                        assert_eq!(BYTE_HASH_INITS.with(|n| n.get()), before,
                            "captured bytes must never be rehashed by a build/assignment turn");
                        let result = if done { Step::Complete(task.finish().unwrap()) }
                            else { Step::Retained(task) };
                        (worker, phase, result)
                    })
                        })
                        .collect();
                joins
                    .into_iter()
                    .map(|j| j.join().unwrap())
                    .collect::<Vec<_>>()
            });
            results.reverse();
            for (worker, phase, step) in results {
                workers
                    .entry(format!("{phase:?}"))
                    .or_default()
                    .insert(worker);
                match step {
                    Step::Retained(task) => next.push(task),
                    Step::Complete(mut complete) => graph.complete(&mut complete, ports).unwrap(),
                }
            }
        }
        pending = next;
    }
    panic!("the dependency-aware test schedule must make progress");
}

#[test]
fn hash_graph_parallel_tasks_close_all_physical_rows_and_compact_fragmented_representatives() {
    for zero_hash in [false, true] {
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let (input, expected) = bytes_input(
            IndexPurpose::KeyIds,
            NullPolicy::Equal,
            zero_hash,
            80,
            72,
            false,
            &ports,
        );
        let source_array = Arc::downgrade(
            &input
                .runs()
                .run(0)
                .unwrap()
                .page(0)
                .unwrap()
                .input()
                .arrays()[0],
        );
        let graph = BulkTaskGraph::begin(
            input,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            &ports,
        )
        .unwrap();
        assert_eq!(graph.phase().unwrap(), Phase::HashRoute);
        let (ready, workers, _) = drive(&graph, &ports, None);
        let ready = ready.unwrap();
        for phase in [
            Phase::HashRoute,
            Phase::HashInit,
            Phase::HashBuild,
            Phase::Assign,
        ] {
            assert_eq!(workers[&format!("{phase:?}")].len(), 2, "{phase:?}");
        }
        let mut seen = BTreeSet::new();
        let mut by_value = BTreeMap::<Option<String>, BulkKey>::new();
        let mut by_id = std::collections::HashMap::<BulkKey, Option<String>>::new();
        for chunk in ready.assignments() {
            let (_, rows) = expected.iter().find(|(id, _)| *id == chunk.page()).unwrap();
            for (i, found) in chunk.matches().iter().enumerate() {
                let occurrence = chunk.start() + i;
                assert!(seen.insert((chunk.page().run.index(), chunk.page().page, occurrence)));
                assert_eq!(chunk.row(i).unwrap(), rows[occurrence].0);
                let value = rows[occurrence].1.clone();
                let BulkMatch::Found(key) = found else {
                    panic!("null-safe inputs all participate")
                };
                if zero_hash {
                    assert_eq!(key.partition, 0);
                }
                if let Some(old) = by_value.insert(value.clone(), *key) {
                    assert_eq!(old, *key);
                }
                if let Some(old) = by_id.insert(*key, value.clone()) {
                    assert_eq!(old, value);
                }
            }
        }
        assert_eq!(
            seen.len(),
            expected.iter().map(|(_, r)| r.len()).sum::<usize>()
        );
        assert_eq!(by_value.len(), 68);
        let keys: Vec<_> = by_value.values().copied().collect();
        let read = ready.read_keys(&keys, owner.as_ref(), &Control).unwrap();
        assert!(
            read.columns()[0].pages().len() <= 2,
            "80 tiny source pages must become class-sized representative pages"
        );
        drop(read);
        let result = ready
            .export_keys(
                &keys,
                &[FunctionValueType::new(DataType::Utf8, true)],
                owner.as_ref(),
                expression_allocation_host(tracker.clone()),
                &Control,
            )
            .unwrap();
        let values = result.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (i, expected) in by_value.keys().enumerate() {
            assert_eq!(values.iter().nth(i).unwrap(), expected.as_deref());
        }
        drop(result);
        let (index, assignments) = ready.into_parts();
        assert_eq!(
            assignments
                .iter()
                .map(|chunk| chunk.matches().len())
                .sum::<usize>(),
            seen.len()
        );
        drop(graph);
        assert!(
            source_array.upgrade().is_some(),
            "real assignment source custody remains"
        );
        drop(assignments);
        assert!(
            source_array.upgrade().is_none(),
            "consumed assignment no longer pins original input pages"
        );
        // Independent query custody remains usable after actual input exit.
        let result = index
            .export_keys(
                &keys,
                &[FunctionValueType::new(DataType::Utf8, true)],
                owner.as_ref(),
                expression_allocation_host(tracker.clone()),
                &Control,
            )
            .unwrap();
        let values = result.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            by_value.keys().map(Option::as_deref).collect::<Vec<_>>()
        );
        drop(result);
        drop(index);
        drop(ports);
        drop(owner);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn queued_merge_after_enumeration_eof_is_pending_work_not_a_broken_barrier() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (input, _) = bytes_input(
        IndexPurpose::KeyIds,
        NullPolicy::Equal,
        true,
        2,
        80,
        false,
        &ports,
    );
    let graph = BulkTaskGraph::begin(
        input,
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        &ports,
    )
    .unwrap();
    let mut first = task(&graph, &ports);
    let mut second = task(&graph, &ports);
    assert!(!first.advance(256, &ports).unwrap());
    assert!(!second.advance(256, &ports).unwrap());
    // Both final partials are queued for p0. Keep its first merge busy while
    // final route cursors exit and enumeration discovers the actual EOF.
    let merge = task(&graph, &ports);
    assert!(matches!(
        merge.body.as_ref(),
        Some(Body::Hash(Some(hash::HashBody::Merge { .. })))
    ));
    for mut route in [first, second] {
        assert!(route.advance(256, &ports).unwrap());
        let mut complete = route.finish().unwrap();
        graph.complete(&mut complete, &ports).unwrap();
    }
    assert!(matches!(graph.claim(&ports).unwrap(), Claim::Pending));
    assert!(graph.0.state().unwrap().exhausted);
    let mut complete = finish(merge, &ports);
    graph.complete(&mut complete, &ports).unwrap();
    assert_eq!(graph.phase().unwrap(), Phase::HashRoute);
    assert!(graph.take_ready(&ports).unwrap().is_none());
    let remaining = task(&graph, &ports);
    assert!(matches!(
        remaining.body.as_ref(),
        Some(Body::Hash(Some(hash::HashBody::Merge { .. })))
    ));
    let mut complete = finish(remaining, &ports);
    graph.complete(&mut complete, &ports).unwrap();
    assert_eq!(graph.phase().unwrap(), Phase::HashInit);
    assert!(drive(&graph, &ports, None).0.is_some());
}

#[test]
fn full_frontier_stops_route_data_without_stopping_eager_merge_progress() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (input, _) = bytes_input(
        IndexPurpose::KeyIds,
        NullPolicy::Equal,
        true,
        128,
        80,
        false,
        &ports,
    );
    let graph = BulkTaskGraph::begin(
        input,
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        &ports,
    )
    .unwrap();
    let mut routes = [task(&graph, &ports), task(&graph, &ports)];
    for route in &mut routes {
        assert!(!route.advance(256, &ports).unwrap());
    }
    assert_eq!(
        graph
            .0
            .state()
            .unwrap()
            .hash
            .as_ref()
            .unwrap()
            .frontier_counts(),
        (0, 2, 0)
    );
    for _ in 0..5 {
        for route in &mut routes {
            assert!(!route.advance(256, &ports).unwrap());
        }
    }
    assert_eq!(
        graph
            .0
            .state()
            .unwrap()
            .hash
            .as_ref()
            .unwrap()
            .frontier_counts(),
        (0, 2, 0)
    );
    let merge = task(&graph, &ports);
    assert_eq!(
        graph
            .0
            .state()
            .unwrap()
            .hash
            .as_ref()
            .unwrap()
            .frontier_counts(),
        (0, 1, 1)
    );
    let mut complete = finish(merge, &ports);
    graph.complete(&mut complete, &ports).unwrap();
    assert_eq!(
        graph
            .0
            .state()
            .unwrap()
            .hash
            .as_ref()
            .unwrap()
            .frontier_counts(),
        (1, 1, 0)
    );
    for mut route in routes {
        assert!(route.advance(256, &ports).unwrap());
        let mut complete = route.finish().unwrap();
        graph.complete(&mut complete, &ports).unwrap();
    }
    // Both original cursors have exited, so a workspace is available. Hold
    // p0's next merge while another cursor fills the last frontier position.
    let held_merge = task(&graph, &ports);
    assert!(matches!(
        held_merge.body.as_ref(),
        Some(Body::Hash(Some(hash::HashBody::Merge { .. })))
    ));
    let mut next_route = task(&graph, &ports);
    assert!(!next_route.advance(256, &ports).unwrap());
    assert_eq!(
        graph
            .0
            .state()
            .unwrap()
            .hash
            .as_ref()
            .unwrap()
            .frontier_counts(),
        (0, 1, 1)
    );
    // No new range may be claimed while queued+merging fill the frontier,
    // even though the other original workspace is now free.
    assert!(matches!(graph.claim(&ports).unwrap(), Claim::Pending));
    let mut complete = finish(held_merge, &ports);
    graph.complete(&mut complete, &ports).unwrap();
    assert!(next_route.advance(256, &ports).unwrap());
    let mut complete = next_route.finish().unwrap();
    graph.complete(&mut complete, &ports).unwrap();
    assert!(drive(&graph, &ports, None).0.is_some());
}

#[test]
fn exact_empty_build_keeps_all_null_unmatchable_occurrences_and_presence_has_no_assignment() {
    for (pages, page_rows) in [(3, 37), (0, 0), (1, 0)] {
        for purpose in [IndexPurpose::KeyIds, IndexPurpose::PresenceOnly] {
            let (owner, tracker) = fixture();
            let ports = work(owner.as_ref(), &tracker, &Control);
            let (input, expected) = bytes_input(
                purpose,
                NullPolicy::Unmatchable,
                false,
                pages,
                page_rows,
                true,
                &ports,
            );
            let graph = BulkTaskGraph::begin(
                input,
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                &ports,
            )
            .unwrap();
            assert_eq!(
                graph.phase().unwrap(),
                if purpose == IndexPurpose::KeyIds {
                    Phase::Assign
                } else {
                    Phase::Ready
                }
            );
            let ready = drive(&graph, &ports, None).0.unwrap();
            if purpose == IndexPurpose::KeyIds {
                let mut seen = BTreeSet::new();
                for chunk in ready.assignments() {
                    let (_, rows) = expected.iter().find(|(id, _)| *id == chunk.page()).unwrap();
                    for (i, found) in chunk.matches().iter().enumerate() {
                        assert_eq!(*found, BulkMatch::Unmatchable);
                        assert_eq!(chunk.row(i).unwrap(), rows[chunk.start() + i].0);
                        assert!(seen.insert((chunk.page().page, chunk.start() + i)));
                    }
                }
                assert_eq!(
                    seen.len(),
                    expected.iter().map(|(_, rows)| rows.len()).sum::<usize>()
                );
            } else {
                assert_eq!(ready.assignments().count(), 0);
            }
        }
    }
}

#[test]
fn hash_presence_retains_exact_membership_without_public_ids_or_occurrence_assignments() {
    let (owner, tracker) = fixture();
    let ports = work(owner.as_ref(), &tracker, &Control);
    let (input, _) = bytes_input(
        IndexPurpose::PresenceOnly,
        NullPolicy::Unmatchable,
        false,
        8,
        80,
        false,
        &ports,
    );
    let runs = input.runs().clone();
    let graph = BulkTaskGraph::begin(
        input,
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        &ports,
    )
    .unwrap();
    let ready = drive(&graph, &ports, None).0.unwrap();
    assert_eq!(ready.assignments().count(), 0);
    for run in 0..runs.len() {
        for p in 0..runs.run(run).unwrap().len() {
            let page = runs.run(run).unwrap().page(p).unwrap();
            let prepared = PreparedKeyBatch::from_captured(
                ready.view().spec(),
                page.input(),
                ports.allocator.clone(),
                &|| Ok(()),
            )
            .unwrap();
            let contains = ready
                .view()
                .contains(&prepared, Some(page.hashes()), owner.as_ref(), &Control)
                .unwrap();
            assert_eq!(contains.present(), contains.participates());
            assert!(
                ready
                    .view()
                    .probe_range(
                        &prepared,
                        Some(page.hashes()),
                        0..0,
                        owner.as_ref(),
                        &Control
                    )
                    .is_err()
            );
        }
    }
    assert!(ready.read_keys(&[], owner.as_ref(), &Control).is_err());
}

#[test]
fn hash_phases_never_publish_after_original_control_failure_or_lost_completion() {
    for phase in [
        Phase::HashRoute,
        Phase::HashInit,
        Phase::HashBuild,
        Phase::HashMaterialize,
        Phase::HashPublish,
        Phase::HashRetire,
        Phase::Assign,
    ] {
        for after_work in [false, true] {
            let (owner, tracker) = fixture();
            let ports = work(owner.as_ref(), &tracker, &Control);
            let (input, _) = bytes_input(
                IndexPurpose::KeyIds,
                NullPolicy::Equal,
                true,
                2,
                20,
                false,
                &ports,
            );
            let graph = BulkTaskGraph::begin(
                input,
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                &ports,
            )
            .unwrap();
            let (ready, _, held) = drive(&graph, &ports, Some(phase));
            assert!(ready.is_none(), "{phase:?}");
            let mut original = held.unwrap_or_else(|| task(&graph, &ports));
            let stop = Stopped(AtomicBool::new(true));
            let stopped = work(owner.as_ref(), &tracker, &stop);
            let cause = if after_work {
                // Run a real finite turn, then refuse completion. A completed
                // cursor still cannot substitute for the common phase barrier.
                let mut done = finish(original, &ports);
                error(graph.complete(&mut done, &stopped))
            } else {
                error(original.advance(1, &stopped))
            };
            assert_eq!(error(graph.take_ready(&ports)), cause, "{phase:?}");
        }
        let (owner, tracker) = fixture();
        let ports = work(owner.as_ref(), &tracker, &Control);
        let (input, _) = bytes_input(
            IndexPurpose::KeyIds,
            NullPolicy::Equal,
            true,
            2,
            20,
            false,
            &ports,
        );
        let graph = BulkTaskGraph::begin(
            input,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            &ports,
        )
        .unwrap();
        let (ready, _, held) = drive(&graph, &ports, Some(phase));
        assert!(ready.is_none(), "{phase:?}");
        drop(held.unwrap_or_else(|| task(&graph, &ports)));
        assert!(graph.take_ready(&ports).is_err());
    }
}
