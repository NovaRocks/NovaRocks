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
    build_capture::{EvaluatedJoinKeyPage, EvaluatedKeyProjection, InputRunWriter},
    build_pages::{
        B3RunId,
        tests::{ordinary, source, work},
    },
    join_completion::tests::{Control, fixture},
};
use super::*;
use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::DataType,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::decode::ValueConversionContext;
use std::{collections::BTreeSet, num::NonZeroU64};

fn build_frame(tails: usize, bits: bool, work: &BulkWork<'_>) -> Arc<JoinBuildFrame> {
    let ty = FunctionValueType::new(DataType::Int32, true);
    let domains = ordinary(
        &ty,
        crate::exec::hash_table::key_index::NullPolicy::Unmatchable,
        IndexPurpose::KeyIds,
        work,
    );
    let participants = JoinParticipants {
        inputs: 2,
        finalizers: NonZeroUsize::new(3).unwrap(),
        probes: 2,
        tails,
    };
    let handoff = participants
        .handoff_producers()
        .unwrap()
        .map(|count| LocalExchanger::new_handoff(count, 2, 4));
    JoinBuildFrame::new(
        domains,
        FinalizeOptions {
            participants,
            handoff,
            live_slots: NonZeroUsize::MIN,
            match_bits: bits,
        },
        work,
    )
    .unwrap()
}
fn input(frame: &JoinBuildFrame, slot: usize, values: Vec<Option<i32>>, work: &BulkWork<'_>) {
    let ty = FunctionValueType::new(DataType::Int32, true);
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    let mut writer = InputRunWriter::new(
        &frame.domains,
        B3RunId::from_input_slot(slot, 2).unwrap(),
        true,
        work,
    )
    .unwrap();
    writer
        .push_evaluated(
            &source(array.len()),
            EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
                arrays: &[array],
                bindings: &[ty],
            }),
            &mut ValueConversionContext::default(),
            work,
        )
        .unwrap();
    frame
        .inputs
        .normal_input(slot, writer.seal(work).unwrap())
        .unwrap();
}
fn turn() -> TurnWork {
    TurnWork::new(NonZeroU64::MIN, 9)
}

#[test]
fn exact_e_receipts_gate_real_k_csr_and_one_bitmap_before_publication() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let frame = build_frame(0, true, &work);
    let values: Vec<_> = (0..2301)
        .map(|i| if i % 17 == 0 { None } else { Some(i % 101) })
        .collect();
    input(&frame, 0, values.clone(), &work);
    let mut workers: Vec<_> = (0..3).map(|_| FinalizeWorker::new()).collect();
    for worker in &mut workers {
        assert_eq!(
            worker.step(&frame, &mut turn(), &work).unwrap(),
            FinalizeStep::Blocked
        );
    }
    assert!(frame.completion.published().unwrap().is_none());
    // Empty E still owns a real sealed receipt; absence is never empty EOS.
    input(&frame, 1, vec![], &work);
    let mut observed = [false; 3];
    let mut done = false;
    for _ in 0..1_000_000 {
        for (id, worker) in workers.iter_mut().enumerate() {
            let step = worker.step(&frame, &mut turn(), &work).unwrap();
            observed[id] |= worker.projections.iter().any(Option::is_some);
            if step == FinalizeStep::Finished {
                done = true;
                break;
            }
        }
        if done {
            break;
        }
    }
    assert!(done);
    assert!(observed.iter().all(|seen| *seen));
    let published = frame.completion.published().unwrap().unwrap();
    assert_eq!(published.rows, values.len());
    assert_eq!(published.artifact.rows.len(), values.len());
    let BuildLookup::Ordinary(index) = &published.artifact.lookup else {
        panic!("original ordinary projection");
    };
    let groups = index.groups.as_ref().unwrap();
    let actual = (0..groups.row_count())
        .map(|position| groups.row(position).unwrap())
        .collect::<BTreeSet<_>>();
    let expected = values
        .iter()
        .enumerate()
        .filter_map(|(row, value)| value.map(|_| row))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    let bitmap = published.artifact.matches.as_ref().unwrap();
    assert_eq!(bitmap.word_count(), values.len().div_ceil(64));
    assert!(!bitmap.is_marked(values.len() - 1).unwrap());
    bitmap.mark(values.len() - 1).unwrap();
    assert!(bitmap.is_marked(values.len() - 1).unwrap());
    drop(workers);
    drop(frame);
    assert_eq!(published.artifact.rows.len(), values.len());
}

#[test]
fn payload_only_e_pages_publish_without_k_graphs_or_assignment_arrays() {
    for bits in [false, true] {
        let (owner, tracker) = fixture();
        let work = work(owner.as_ref(), &tracker, &Control);
        let frame = JoinBuildFrame::new(
            ProjectionDomains::PayloadOnly,
            FinalizeOptions {
                participants: JoinParticipants {
                    inputs: 3,
                    finalizers: NonZeroUsize::new(3).unwrap(),
                    probes: 2,
                    tails: 0,
                },
                handoff: None,
                live_slots: NonZeroUsize::MIN,
                match_bits: bits,
            },
            &work,
        )
        .unwrap();
        let mut workers: Vec<_> = (0..3).map(|_| FinalizeWorker::new()).collect();
        for (slot, pages) in [(2, vec![2, 0, 2]), (0, vec![3, 0, 4]), (1, vec![])] {
            let mut writer = InputRunWriter::new(
                &frame.domains,
                B3RunId::from_input_slot(slot, 3).unwrap(),
                true,
                &work,
            )
            .unwrap();
            for rows in pages {
                let chunk = source(rows);
                let page = writer.push_payload(&chunk, &work).unwrap();
                assert!(page.values().is_none());
                assert!(page.value_nulls().is_none());
                assert!(page.projection_id(ProjectionKind::Ordinary).is_none());
                assert!(page.projection_id(ProjectionKind::Full).is_none());
            }
            frame
                .inputs
                .normal_input(slot, writer.seal(&work).unwrap())
                .unwrap();
            if slot != 1 {
                for worker in &mut workers {
                    assert_eq!(
                        worker.step(&frame, &mut turn(), &work).unwrap(),
                        FinalizeStep::Blocked
                    );
                }
                assert!(frame.completion.published().unwrap().is_none());
            }
        }
        for _ in 0..1000 {
            for worker in &mut workers {
                worker.step(&frame, &mut turn(), &work).unwrap();
                assert!(worker.projections.iter().all(Option::is_none));
            }
            if frame.completion.published().unwrap().is_some() {
                break;
            }
        }
        let published = frame
            .completion
            .published()
            .unwrap()
            .expect("real payload publication");
        assert_eq!(published.rows, 11);
        assert!(matches!(
            published.artifact.lookup,
            BuildLookup::PayloadOnly
        ));
        assert_eq!(published.artifact.matches.is_some(), bits);
        let rows = Arc::clone(&published.artifact.rows);
        drop(workers);
        drop(frame);
        let actual = (0..rows.len())
            .map(|flat| {
                let id = rows.row(flat).unwrap();
                let address = rows.locate(id).unwrap();
                assert_eq!(rows.row_id(address).unwrap(), id);
                let page = rows.page(address.run(), address.page()).unwrap();
                let payload = page.payload().unwrap();
                payload
                    .column(0)
                    .unwrap()
                    .array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(address.row_in_page())
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![0, 1, 2, 0, 1, 2, 3, 0, 1, 0, 1]);
    }
}

#[test]
fn normal_demand_withdrawal_settles_every_workers_original_partial_storage() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let frame = build_frame(0, true, &work);
    input(&frame, 0, (0..1800).map(Some).collect(), &work);
    input(&frame, 1, vec![Some(7); 1300], &work);
    let mut workers: Vec<_> = (0..3).map(|_| FinalizeWorker::new()).collect();
    for _ in 0..8 {
        for worker in &mut workers {
            worker.step(&frame, &mut turn(), &work).unwrap();
        }
    }
    assert!(workers.iter().all(|worker| worker.active.is_some()));
    assert!(frame.completion.published().unwrap().is_none());
    frame.completion.withdraw(0).unwrap();
    frame.completion.withdraw(1).unwrap();
    for worker in &mut workers {
        assert_eq!(
            worker.step(&frame, &mut turn(), &work).unwrap(),
            FinalizeStep::Retired
        );
        assert!(worker.active.is_none());
        assert!(worker.bitmap.is_none());
    }
    drop(workers);
    assert!(frame.completion.published().unwrap().is_none());
    frame.completion.check().unwrap();
}

#[test]
fn original_handoff_all_closed_event_wakes_frame_without_partial_close_withdrawal() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let domains = ordinary(
        &FunctionValueType::new(DataType::Int32, true),
        crate::exec::hash_table::key_index::NullPolicy::Unmatchable,
        IndexPurpose::KeyIds,
        &work,
    );
    let handoff = LocalExchanger::new_handoff(4, 2, 4);
    let frame = JoinBuildFrame::new(
        domains,
        FinalizeOptions {
            participants: JoinParticipants {
                inputs: 2,
                finalizers: NonZeroUsize::new(3).unwrap(),
                probes: 2,
                tails: 2,
            },
            handoff: Some(Arc::clone(&handoff)),
            live_slots: NonZeroUsize::MIN,
            match_bits: true,
        },
        &work,
    )
    .unwrap();
    assert!(
        frame.activation.get().is_none(),
        "prepare must remain dormant"
    );
    frame.activate(&work).unwrap();
    let changed = frame.completion.observable();
    let before = changed.generation();
    handoff.close_consumer(0);
    assert_eq!(changed.generation(), before);
    assert_eq!(
        frame.completion.remaining_demand().unwrap(),
        RemainingDemand::Needed
    );
    let before = changed.generation();
    handoff.close_consumer(1);
    assert_ne!(changed.generation(), before);
    assert_eq!(
        frame.completion.remaining_demand().unwrap(),
        RemainingDemand::Released
    );
    let mut worker = FinalizeWorker::new();
    assert_eq!(
        worker.step(&frame, &mut turn(), &work).unwrap(),
        FinalizeStep::Retired
    );
    assert!(frame.completion.published().unwrap().is_none());
}

#[test]
fn naa_two_original_projections_make_progress_without_cross_domain_coordinate_guesses() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int32, true),
    ];
    let domains = super::super::build_pages::tests::naa(
        &types,
        &[
            crate::exec::hash_table::key_index::NullPolicy::Equal,
            crate::exec::hash_table::key_index::NullPolicy::Unmatchable,
        ],
        1..2,
        &[0],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let frame = JoinBuildFrame::new(
        domains,
        FinalizeOptions {
            participants: JoinParticipants {
                inputs: 2,
                finalizers: NonZeroUsize::new(3).unwrap(),
                probes: 2,
                tails: 0,
            },
            handoff: None,
            live_slots: NonZeroUsize::MIN,
            match_bits: false,
        },
        &work,
    )
    .unwrap();
    let ProjectionDomains::Naa {
        group: Some(group),
        full,
        ..
    } = &frame.domains
    else {
        unreachable!()
    };
    struct PauseAfterScope<'a> {
        group: &'a crate::exec::hash_table::key_index::BulkDomain,
        full: &'a crate::exec::hash_table::key_index::BulkDomain,
        claimed: &'a std::sync::Barrier,
        resume: &'a std::sync::Barrier,
        paused: std::sync::atomic::AtomicBool,
    }
    impl novarocks_functions::KernelEvaluationControl for PauseAfterScope<'_> {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            if self.group.capture_claims_for_test().unwrap() == 1
                && self.full.capture_claims_for_test().unwrap() == 0
                && !self.paused.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                self.claimed.wait();
                self.resume.wait();
            }
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("local capture never waits for I/O");
        }
    }
    let claimed = std::sync::Barrier::new(2);
    let resume = std::sync::Barrier::new(2);
    let pause = PauseAfterScope {
        group,
        full,
        claimed: &claimed,
        resume: &resume,
        paused: std::sync::atomic::AtomicBool::new(false),
    };
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            let work = super::super::build_pages::tests::work(owner.as_ref(), &tracker, &pause);
            InputRunWriter::new(
                &frame.domains,
                B3RunId::from_input_slot(0, 2).unwrap(),
                false,
                &work,
            )
            .unwrap()
        });
        let second = scope.spawn(|| {
            claimed.wait();
            let writer = InputRunWriter::new(
                &frame.domains,
                B3RunId::from_input_slot(1, 2).unwrap(),
                false,
                &work,
            )
            .unwrap();
            resume.wait();
            writer
        });
        (first.join().unwrap(), second.join().unwrap())
    });
    let mut writers = [first, second];
    // Push in an order different from E-slot order; each domain mints its own
    // real run identity. The page directory must preserve those actual facts.
    for slot in [1, 0, 1, 0] {
        let scope: ArrayRef = Arc::new(Int32Array::from(
            (0..701)
                .map(|i| if i % 13 == 0 { None } else { Some(i % 43) })
                .collect::<Vec<_>>(),
        ));
        let values: ArrayRef = Arc::new(Int32Array::from(
            (0..701)
                .map(|i| if i % 11 == 0 { None } else { Some(i % 59) })
                .collect::<Vec<_>>(),
        ));
        let page = writers[slot]
            .push_evaluated(
                &source(701),
                EvaluatedJoinKeyPage::Naa {
                    scope: EvaluatedKeyProjection {
                        arrays: &[scope],
                        bindings: &types[..1],
                    },
                    values: EvaluatedKeyProjection {
                        arrays: &[values],
                        bindings: &types[1..],
                    },
                },
                &mut ValueConversionContext::default(),
                &work,
            )
            .unwrap();
        assert_ne!(
            page.projection_id(ProjectionKind::Group)
                .unwrap()
                .run_index(),
            page.projection_id(ProjectionKind::Full)
                .unwrap()
                .run_index()
        );
    }
    for (slot, writer) in writers.iter_mut().enumerate() {
        frame
            .inputs
            .normal_input(slot, writer.seal(&work).unwrap())
            .unwrap();
    }
    let mut workers: Vec<_> = (0..3).map(|_| FinalizeWorker::new()).collect();
    let mut done = false;
    for _ in 0..1_000_000 {
        for worker in &mut workers {
            if worker.step(&frame, &mut turn(), &work).unwrap() == FinalizeStep::Finished {
                done = true;
                break;
            }
        }
        if done {
            break;
        }
    }
    assert!(done);
    let published = frame.completion.published().unwrap().unwrap();
    assert_eq!(published.rows, 2804);
    let BuildLookup::Naa {
        group: Some(group),
        full,
        ..
    } = &published.artifact.lookup
    else {
        panic!("two authored NAA projections");
    };
    assert_eq!(group.groups.as_ref().unwrap().row_count(), 2804);
    let nulls = group
        .ordinary_nulls
        .as_ref()
        .expect("original group NULL selection");
    let actual = (0..nulls.row_count())
        .map(|position| nulls.row(position).unwrap())
        .collect::<BTreeSet<_>>();
    let expected = (0..2804)
        .filter(|row| row % 701 % 11 == 0)
        .collect::<BTreeSet<_>>();
    assert_eq!(nulls.row_count(), 256);
    assert_eq!(actual, expected);
    // Query actual K coordinates, then compare each CSR class to an
    // independent physical-row oracle. This includes empty NULL classes and
    // the null-safe scope bucket; full/group K run IDs remain unrelated.
    let scopes = std::iter::once(None)
        .chain((0..43).map(Some))
        .collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(scopes.clone()))];
    let readable = novarocks_value_arrays::reachability::ReachableRows::all(scopes.len())
        .materialize_observed(
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| Ok::<_, ExecutionFailure>(()),
        )
        .unwrap();
    let occurrences = (0..scopes.len()).collect::<Vec<_>>();
    let input = crate::exec::hash_table::key_index::KeyInput::new_for_spec(
        group.view.spec(),
        &arrays,
        &types[..1],
        &readable,
        &occurrences,
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let input = crate::exec::hash_table::key_index::PreparedKeyBatch::from_captured(
        group.view.spec(),
        &input,
        work.allocator.clone(),
        &|| Ok(()),
    )
    .unwrap();
    let lookup = group
        .view
        .probe(&input, None, owner.as_ref(), &Control)
        .unwrap();
    let mut empty_null_classes = 0;
    for (scope, found) in scopes.iter().zip(lookup.matches()) {
        let crate::exec::hash_table::key_index::BulkMatch::Found(key) = found else {
            panic!("every actual scope exists");
        };
        let expected = (0..2804)
            .filter(|row| {
                let i = row % 701;
                (if i % 13 == 0 {
                    None
                } else {
                    Some(i as i32 % 43)
                }) == *scope
            })
            .collect::<BTreeSet<_>>();
        let all = group.groups.as_ref().unwrap();
        let actual = all
            .group(*key)
            .unwrap()
            .map(|position| all.row(position).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        let expected = expected
            .into_iter()
            .filter(|row| row % 701 % 11 == 0)
            .collect::<BTreeSet<_>>();
        let actual = nulls
            .group(*key)
            .unwrap()
            .map(|position| nulls.row(position).unwrap())
            .collect::<BTreeSet<_>>();
        empty_null_classes += usize::from(expected.is_empty());
        assert_eq!(actual, expected);
    }
    assert!(empty_null_classes > 0);
    assert!(full.groups.is_none());
    assert_eq!(full.view.purpose(), IndexPurpose::PresenceOnly);
    assert!(published.artifact.matches.is_none());
}

#[test]
fn published_finalizers_release_settled_work_after_the_last_q_normal_completion() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let frame = build_frame(0, true, &work);
    input(&frame, 0, vec![Some(1), None, Some(1)], &work);
    input(&frame, 1, vec![], &work);
    let mut workers = [
        FinalizeWorker::new(),
        FinalizeWorker::new(),
        FinalizeWorker::new(),
    ];
    for _ in 0..100_000 {
        for worker in &mut workers {
            worker.step(&frame, &mut turn(), &work).unwrap();
        }
        if frame.completion.published().unwrap().is_some() {
            break;
        }
    }
    assert!(frame.completion.published().unwrap().is_some());
    frame.completion.normal_after_output(0).unwrap();
    frame.completion.normal_after_output(1).unwrap();
    assert_eq!(
        frame.completion.remaining_demand().unwrap(),
        RemainingDemand::Released
    );
    for worker in &mut workers {
        assert!(worker.finish_exit(&frame).unwrap());
        assert!(worker.active.is_none());
        assert!(worker.projections.iter().all(Option::is_none));
    }
    frame.completion.check().unwrap();
    assert!(frame.completion.published().unwrap().is_some());
}
#[test]
fn publication_losing_all_demand_closes_original_projection_owners_before_retired() {
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
    use std::sync::atomic::{AtomicBool, Ordering};
    struct CloseAtPublication<'a> {
        original: &'a dyn InputConstructionOwner,
        frame: Arc<JoinBuildFrame>,
        closed: AtomicBool,
    }
    impl InputConstructionOwner for CloseAtPublication<'_> {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.original.storage_lifetime()
        }
        fn request(&self, layout: std::alloc::Layout) -> Result<(), ConstructionFailure> {
            self.original.request(layout)?;
            let publishing = matches!(&*self.frame.state.lock().unwrap(), State::Publishing(_));
            if publishing && !self.closed.swap(true, Ordering::SeqCst) {
                self.frame.completion.withdraw(0).unwrap();
                self.frame.completion.withdraw(1).unwrap();
            }
            Ok(())
        }
        fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
            self.original.observe_opaque_upper_bound(n)
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
    let initial = work(owner.as_ref(), &tracker, &Control);
    let frame = build_frame(0, true, &initial);
    input(&frame, 0, vec![Some(1), None, Some(2)], &initial);
    input(&frame, 1, vec![Some(1)], &initial);
    let closing = CloseAtPublication {
        original: owner.as_ref(),
        frame: frame.clone(),
        closed: AtomicBool::new(false),
    };
    let work = work(&closing, &tracker, &Control);
    let mut workers = [
        FinalizeWorker::new(),
        FinalizeWorker::new(),
        FinalizeWorker::new(),
    ];
    let mut retired = false;
    for _ in 0..100_000 {
        for worker in &mut workers {
            let step = worker.step(&frame, &mut turn(), &work).unwrap();
            retired |= step == FinalizeStep::Retired;
        }
        if retired {
            break;
        }
    }
    assert!(closing.closed.load(Ordering::SeqCst));
    assert!(retired);
    assert!(matches!(&*frame.state().unwrap(), State::Retired));
    assert!(frame.completion.published().unwrap().is_none());
    for worker in &mut workers {
        assert!(worker.finish_exit(&frame).unwrap());
        assert!(worker.active.is_none());
    }
    frame.completion.check().unwrap();
}
