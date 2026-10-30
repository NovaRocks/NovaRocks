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
    build_collection::{BeginClaim, BuildCollection},
    build_pages::{
        B3RunId, CapturesForGraphs,
        tests::{NoArtifact, ordinary, source, work},
    },
    csr_slots::SlotsProgress,
    join_completion::tests::{Control, fixture},
    join_completion::{JoinCompletion, JoinParticipants, funded_shared},
};
use super::*;
use crate::exec::hash_table::key_index::{
    BulkClaim, BulkTask, BulkTaskAdvance, BulkTaskGraph, IndexPurpose, NullPolicy,
};
use arrow::{
    array::{ArrayRef, Int32Array, StringArray},
    datatypes::DataType,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::decode::ValueConversionContext;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn captured_graph(
    pages: &[Vec<Option<i32>>],
    strings: bool,
    work: &BulkWork<'_>,
) -> (BulkTaskGraph, Arc<BuildRows>) {
    let ty = FunctionValueType::new(
        if strings {
            DataType::Utf8
        } else {
            DataType::Int32
        },
        true,
    );
    let domains = ordinary(&ty, NullPolicy::Unmatchable, IndexPurpose::KeyIds, work);
    let mut receipts = Vec::new();
    for (slot, values) in pages.iter().enumerate() {
        let mut writer = InputRunWriter::new(
            &domains,
            B3RunId::from_input_slot(slot, pages.len()).unwrap(),
            true,
            work,
        )
        .unwrap();
        let array: ArrayRef = if strings {
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|v| v.map(|v| format!("key-{v}")))
                    .collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(Int32Array::from(values.clone()))
        };
        writer
            .push_evaluated(
                &source(values.len()),
                EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
                    arrays: &[array],
                    bindings: std::slice::from_ref(&ty),
                }),
                &mut ValueConversionContext::default(),
                work,
            )
            .unwrap();
        receipts.push(writer.seal(work).unwrap());
    }
    let completion: Arc<JoinCompletion<NoArtifact>> = JoinCompletion::new(
        JoinParticipants {
            inputs: receipts.len(),
            finalizers: NonZeroUsize::new(3).unwrap(),
            probes: 1,
            tails: 0,
        },
        None,
        funded_shared(work.owner, Observable::new()).unwrap(),
        work.owner,
        work.control,
    )
    .unwrap();
    let collection = BuildCollection::new(completion, work.owner, work.control).unwrap();
    for receipt in receipts {
        collection
            .normal_input(receipt.run().index(), receipt)
            .unwrap();
    }
    let BeginClaim::Claimed { inputs, mut lease } =
        collection.claim_begin(work.owner, work.control).unwrap()
    else {
        panic!("original input coverage");
    };
    let (rows, captures) = BuildRows::from_inputs(inputs, &domains, work).unwrap();
    let CapturesForGraphs::Ordinary { keys } = captures else {
        unreachable!()
    };
    let super::super::build_state::ProjectionDomains::Ordinary { keys: domain, .. } = &domains
    else {
        unreachable!()
    };
    let graph = BulkTaskGraph::begin(
        keys.into_complete(domain, IndexPurpose::KeyIds, work)
            .unwrap(),
        NonZeroUsize::new(3).unwrap(),
        NonZeroUsize::MIN,
        work,
    )
    .unwrap();
    collection.complete_begin(&mut lease).unwrap();
    (graph, rows)
}
fn captured(
    pages: &[Vec<Option<i32>>],
    strings: bool,
    work: &BulkWork<'_>,
) -> (Arc<IndexView>, BulkAssignments, Arc<BuildRows>) {
    let (graph, rows) = captured_graph(pages, strings, work);
    let mut tasks: Vec<BulkTask> = Vec::new();
    for _ in 0..1_000_000 {
        match graph.claim(work).unwrap() {
            BulkClaim::Task(task) => tasks.push(task),
            BulkClaim::Finished => {
                assert!(tasks.is_empty());
                let (view, assignments) = graph.take_ready(work).unwrap().unwrap().into_parts();
                return (view, assignments, rows);
            }
            BulkClaim::Pending => {}
        }
        let mut index = 0;
        while index < tasks.len() {
            if tasks[index].advance(13, work).unwrap() == BulkTaskAdvance::Done {
                let mut complete = tasks.swap_remove(index).finish().unwrap();
                graph.complete(&mut complete, work).unwrap();
            } else {
                index += 1;
            }
        }
    }
    panic!("original K graph did not converge");
}

fn builder(
    pages: &[Vec<Option<i32>>],
    strings: bool,
    workers: usize,
    work: &BulkWork<'_>,
) -> (Arc<CsrBuilder>, Arc<IndexView>) {
    let (view, assignments, rows) = captured(pages, strings, work);
    let builder = CsrBuilder::new(
        &view,
        assignments,
        rows,
        ProjectionKind::Ordinary,
        NonZeroUsize::new(workers).unwrap(),
        Arc::new(Observable::new()),
        work,
    )
    .unwrap();
    (builder, view)
}
fn drive(
    builder: &Arc<CsrBuilder>,
    workers: usize,
    work: &BulkWork<'_>,
) -> (BuildCsr, Vec<(usize, CsrStage, Range<usize>)>) {
    let mut active: Vec<Option<CsrLease>> = (0..workers).map(|_| None).collect();
    let mut init: Vec<Option<SlotsLease>> = (0..workers).map(|_| None).collect();
    let mut claims = Vec::new();
    for _ in 0..1_000_000 {
        for worker in 0..workers {
            if active[worker].is_none() && init[worker].is_none() {
                match builder.claim(work).unwrap() {
                    CsrClaim::Initialize(lease) => init[worker] = Some(lease),
                    CsrClaim::Work(lease) => {
                        claims.push((worker, lease.stage(), lease.range()));
                        active[worker] = Some(lease);
                    }
                    CsrClaim::Ready => {
                        assert!(active.iter().all(Option::is_none));
                        assert!(init.iter().all(Option::is_none));
                        let ready = builder.take_ready().unwrap().unwrap();
                        assert!(builder.take_ready().unwrap().is_none());
                        return (ready, claims);
                    }
                    CsrClaim::Pending | CsrClaim::Progressed => {}
                    CsrClaim::Retired => panic!("original demand remains"),
                }
            }
            if let Some(lease) = &mut init[worker] {
                assert_eq!(
                    lease.initialize(0, work.control).unwrap(),
                    SlotsProgress::Yielded
                );
                match lease.initialize(7, work.control).unwrap() {
                    SlotsProgress::Initialized => {
                        lease.complete().unwrap();
                        init[worker] = None;
                    }
                    SlotsProgress::Progressed => {}
                    _ => panic!("positive original turn budget"),
                }
            }
            if let Some(lease) = &mut active[worker] {
                assert_eq!(lease.advance(0, work).unwrap(), CsrProgress::Yielded);
                match lease.advance(5, work).unwrap() {
                    CsrProgress::Done => {
                        lease.complete().unwrap();
                        active[worker] = None;
                    }
                    CsrProgress::Progressed => {}
                    _ => panic!("positive original turn budget"),
                }
            }
        }
    }
    panic!("CSR did not converge");
}

#[test]
fn counted_groups_preserve_every_physical_duplicate_across_runs_and_k_layouts() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let pages = vec![
        (0..900)
            .map(|i| if i % 19 == 0 { None } else { Some(i % 137) })
            .collect(),
        vec![],
        (0..711)
            .map(|i| if i % 17 == 0 { None } else { Some(i % 137) })
            .collect(),
    ];
    for strings in [false, true] {
        let (builder, _) = builder(&pages, strings, 3, &work);
        let mut expected = BTreeMap::<(usize, usize), BTreeSet<usize>>::new();
        for chunk in builder.source.assignments.iter() {
            for (ordinal, value) in chunk.matches().iter().enumerate() {
                if let BulkMatch::Found(key) = value {
                    let address = builder
                        .source
                        .rows
                        .address_for_assignment(
                            ProjectionKind::Ordinary,
                            chunk.page(),
                            chunk.start() + ordinal,
                        )
                        .unwrap();
                    expected
                        .entry((key.partition(), key.id().index()))
                        .or_default()
                        .insert(builder.source.rows.row_id(address).unwrap().index());
                }
            }
        }
        let (csr, claims) = drive(&builder, 3, &work);
        assert_eq!(
            csr.row_count(),
            pages.iter().flatten().filter(|v| v.is_some()).count()
        );
        let mut all = BTreeSet::new();
        for chunk in builder.source.assignments.iter() {
            for value in chunk.matches() {
                if let BulkMatch::Found(key) = value {
                    let group = csr.group(*key).unwrap();
                    let actual = group.map(|i| csr.row(i).unwrap()).collect::<BTreeSet<_>>();
                    assert_eq!(actual, expected[&(key.partition(), key.id().index())]);
                    all.extend(actual);
                }
            }
        }
        assert_eq!(all.len(), csr.row_count());
        for stage in [CsrStage::Count, CsrStage::Prefix, CsrStage::Scatter] {
            let workers = claims
                .iter()
                .filter(|(_, s, _)| *s == stage)
                .map(|(w, _, _)| *w)
                .collect::<BTreeSet<_>>();
            assert!(
                workers.len() > 1,
                "{stage:?} must be claimed by multiple actual workers: {claims:?}"
            );
        }
    }
}

#[test]
fn empty_and_all_unmatchable_inputs_publish_only_after_real_barriers() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    for pages in [vec![vec![], vec![]], vec![vec![None; 700], vec![None; 20]]] {
        let (builder, _) = builder(&pages, false, 4, &work);
        assert!(builder.take_ready().unwrap().is_none());
        let (csr, _) = drive(&builder, 4, &work);
        assert_eq!(csr.row_count(), 0);
        assert!(csr.row(0).is_err());
    }
}

#[test]
fn unfinished_assignment_range_cannot_publish_and_withdrawal_is_distinct() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    for withdraw in [false, true] {
        let (builder, _) = builder(&[vec![Some(2); 700]], false, 3, &work);
        let mut range = loop {
            match builder.claim(&work).unwrap() {
                CsrClaim::Initialize(mut lease) => {
                    while lease.initialize(11, work.control).unwrap() != SlotsProgress::Initialized
                    {
                    }
                    lease.complete().unwrap();
                }
                CsrClaim::Work(lease) => break lease,
                CsrClaim::Pending | CsrClaim::Progressed => {}
                _ => panic!("real count range required"),
            }
        };
        assert!(range.complete().is_err());
        assert!(builder.take_ready().is_err());
        drop(range);
        if withdraw {
            let (builder, _) = self::builder(&[vec![Some(2); 700]], false, 3, &work);
            let mut range = loop {
                match builder.claim(&work).unwrap() {
                    CsrClaim::Initialize(mut lease) => {
                        while lease.initialize(11, work.control).unwrap()
                            != SlotsProgress::Initialized
                        {}
                        lease.complete().unwrap();
                    }
                    CsrClaim::Work(lease) => break lease,
                    CsrClaim::Pending | CsrClaim::Progressed => {}
                    _ => panic!("real count range required"),
                }
            };
            builder.retire().unwrap();
            assert_eq!(range.advance(9, &work).unwrap(), CsrProgress::Retired);
            range.retire().unwrap();
            drop(range);
            assert!(matches!(builder.claim(&work).unwrap(), CsrClaim::Retired));
            assert!(builder.take_ready().unwrap().is_none());
        }
    }
}

#[test]
fn concurrent_scatter_of_one_large_duplicate_class_has_exact_unique_physical_rows() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let pages = vec![vec![Some(17); 1800], vec![], vec![Some(17); 2201]];
    let (builder, _) = builder(&pages, true, 4, &work);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let builder = Arc::clone(&builder);
            scope.spawn(move || {
                // Each actual worker initializes through its own finite creator.
                let (owner, tracker) = fixture();
                let work = self::work(owner.as_ref(), &tracker, &Control);
                for _ in 0..1_000_000 {
                    match builder.claim(&work).unwrap() {
                        CsrClaim::Initialize(mut lease) => {
                            while lease.initialize(17, work.control).unwrap()
                                != SlotsProgress::Initialized
                            {
                                std::thread::yield_now();
                            }
                            lease.complete().unwrap();
                        }
                        CsrClaim::Work(mut lease) => {
                            while lease.advance(7, &work).unwrap() != CsrProgress::Done {
                                std::thread::yield_now();
                            }
                            lease.complete().unwrap();
                        }
                        CsrClaim::Pending => std::thread::yield_now(),
                        CsrClaim::Progressed => {}
                        CsrClaim::Ready => return,
                        CsrClaim::Retired => panic!("original demand remains"),
                    }
                }
                panic!("concurrent original CSR ranges did not converge");
            });
        }
    });
    let csr = builder.take_ready().unwrap().unwrap();
    assert_eq!(csr.row_count(), 4001);
    let actual = (0..csr.row_count())
        .map(|i| csr.row(i).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, (0..4001).collect());
}
