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
    build_csr::tests::{captured_graph, captured_graph_for},
    build_pages::tests::work,
    join_completion::tests::{Control, fixture},
};
use super::*;
use std::{collections::BTreeSet, num::NonZeroU64};

#[test]
fn one_projection_composes_real_k_ready_with_csr_before_unique_publication() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let pages = vec![
        (0..1800)
            .map(|i| if i % 11 == 0 { None } else { Some(i % 151) })
            .collect(),
        vec![],
        (0..1700).map(|i| Some(i % 151)).collect(),
    ];
    for strings in [false, true] {
        let (graph, rows) = captured_graph(&pages, strings, &work);
        let build = ProjectionBuild::from_graph(
            graph,
            rows,
            ProjectionKind::Ordinary,
            NonZeroUsize::new(3).unwrap(),
            Arc::new(Observable::new()),
            &work,
        )
        .unwrap();
        let mut workers: Vec<_> = (0..3).map(|_| ProjectionWorker::new(&work)).collect();
        let mut seen_csr = false;
        let mut all_finished = false;
        for _ in 0..1_000_000 {
            let mut finished = 0;
            for worker in &mut workers {
                let mut turn = TurnWork::new(NonZeroU64::MIN, 11);
                let step = worker.step(&build, &mut turn, &work).unwrap();
                seen_csr |= matches!(&*build.phase().unwrap(), Phase::Csr { .. });
                if step == ProjectionStep::Finished {
                    finished += 1;
                } else if !matches!(&*build.phase().unwrap(), Phase::Ready(_)) {
                    assert!(
                        build.take_ready().unwrap().is_none(),
                        "no fake ready during K/CSR work"
                    );
                }
            }
            if finished == 3 {
                all_finished = true;
                break;
            }
        }
        assert!(seen_csr);
        assert!(all_finished);
        let ready = build.take_ready().unwrap().unwrap();
        assert!(build.take_ready().unwrap().is_none());
        drop(workers);
        drop(build);
        let groups = ready.groups.unwrap();
        let expected = pages
            .iter()
            .flatten()
            .enumerate()
            .filter_map(|(row, value)| value.map(|_| row))
            .collect::<BTreeSet<_>>();
        let actual = (0..groups.row_count())
            .map(|position| groups.row(position).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(ready.view.purpose(), IndexPurpose::KeyIds);
    }
}

#[test]
fn presence_only_uses_k_presence_ready_without_fabricated_csr() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    for pages in [
        vec![vec![]],
        vec![vec![None; 10]],
        vec![vec![Some(2); 1000]],
    ] {
        let (graph, rows) = captured_graph_for(&pages, true, IndexPurpose::PresenceOnly, &work);
        let build = ProjectionBuild::from_graph(
            graph,
            rows,
            ProjectionKind::Ordinary,
            NonZeroUsize::new(3).unwrap(),
            Arc::new(Observable::new()),
            &work,
        )
        .unwrap();
        let mut workers: Vec<_> = (0..3).map(|_| ProjectionWorker::new(&work)).collect();
        let mut finished = false;
        for _ in 0..1_000_000 {
            for worker in &mut workers {
                let mut turn = TurnWork::new(NonZeroU64::MIN, 11);
                if worker.step(&build, &mut turn, &work).unwrap() == ProjectionStep::Finished {
                    finished = true;
                    break;
                }
                assert!(!matches!(&*build.phase().unwrap(), Phase::Csr { .. }));
            }
            if finished {
                break;
            }
        }
        assert!(finished);
        let ready = build.take_ready().unwrap().unwrap();
        assert!(ready.groups.is_none());
        assert_eq!(ready.view.purpose(), IndexPurpose::PresenceOnly);
    }
}

#[test]
fn normal_global_withdrawal_retires_real_csr_cursors_without_ready_or_failure() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let (graph, rows) = captured_graph(&[vec![Some(4); 1500]], false, &work);
    let build = ProjectionBuild::from_graph(
        graph,
        rows,
        ProjectionKind::Ordinary,
        NonZeroUsize::new(3).unwrap(),
        Arc::new(Observable::new()),
        &work,
    )
    .unwrap();
    let mut workers: Vec<_> = (0..3).map(|_| ProjectionWorker::new(&work)).collect();
    let mut pending = false;
    for _ in 0..1_000_000 {
        for worker in &mut workers {
            let mut turn = TurnWork::new(NonZeroU64::MIN, 3);
            worker.step(&build, &mut turn, &work).unwrap();
            pending |= worker.csr.is_some();
        }
        if pending {
            break;
        }
    }
    assert!(pending);
    build.retire().unwrap();
    for worker in &mut workers {
        let mut turn = TurnWork::new(NonZeroU64::MIN, 3);
        assert_eq!(
            worker.step(&build, &mut turn, &work).unwrap(),
            ProjectionStep::Retired
        );
        assert!(worker.csr.is_none());
    }
    drop(workers);
    assert!(build.take_ready().unwrap().is_none());
    assert!(matches!(&*build.phase().unwrap(), Phase::Retired));
}

#[test]
fn withdrawing_retained_k_work_preserves_control_and_any_earlier_failure() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    for failed_before_withdrawal in [false, true] {
        let pages = vec![(0..4001).map(|i| Some(i % 51)).collect()];
        let (graph, rows) = captured_graph(&pages, false, &work);
        let build = ProjectionBuild::from_graph(
            graph,
            rows,
            ProjectionKind::Ordinary,
            NonZeroUsize::new(3).unwrap(),
            Arc::new(Observable::new()),
            &work,
        )
        .unwrap();
        let graph = match &*build.phase().unwrap() {
            Phase::Keys(graph) => Arc::clone(graph),
            _ => panic!("the original K graph is installed"),
        };
        let mut worker = ProjectionWorker::new(&work);
        let mut turn = TurnWork::new(NonZeroU64::MIN, 2);
        worker.step(&build, &mut turn, &work).unwrap();
        assert!(worker.keys.retained() > 0);
        let original: ExecutionFailure = KernelFailure::DeadlineExceeded.into();
        if failed_before_withdrawal {
            assert_eq!(graph.fail(original.clone()), original);
            assert_eq!(build.retire().unwrap_err(), original);
        } else {
            build.retire().unwrap();
            worker.retire_owned(&build).unwrap();
        }
        drop(worker);
        let actual = graph
            .take_ready(&work)
            .err()
            .expect("K did not publish Ready");
        if failed_before_withdrawal {
            assert_eq!(actual, original);
        } else {
            assert!(matches!(
                actual.cause(),
                ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
            ));
            assert!(build.take_ready().unwrap().is_none());
        }
    }
}
