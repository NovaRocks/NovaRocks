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

use super::super::stage_completion::tests::{Control, error, fixture, test_contract};
use super::*;
use crate::exec::{
    chunk::{ArrayNormalizationStep, ArrayPathStep},
    operators::hashjoin::{build_fixture_work as work, captured_graph},
    pipeline::operator::{ConstructionFailure, InputConstructionOwner},
};
use novarocks_type_contract::CompileControlError;
use std::{
    alloc::Layout,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[test]
fn real_turn_budget_keeps_large_direct_and_hash_graphs_cooperative() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let pages = vec![
        (0..4500).map(Some).collect(),
        (4500..9000).map(Some).collect(),
    ];
    for strings in [false, true] {
        let (graph, _) = captured_graph(&pages, strings, &work);
        let changed = Observable::new();
        let mut workers: Vec<_> = (0..3)
            .map(|_| KeyBuildWorker::new(&work, test_contract))
            .collect();
        let mut claimed = [false; 3];
        let mut finished = false;
        for _ in 0..1_000_000 {
            for (id, worker) in workers.iter_mut().enumerate() {
                let mut turn = TurnWork::new(NonZeroU64::MIN, 5);
                let step = worker.step(&graph, &changed, &mut turn, &work).unwrap();
                claimed[id] |= worker.retained() > 0;
                match step {
                    KeyWorkerStep::Finished => {
                        finished = true;
                        break;
                    }
                    KeyWorkerStep::Blocked => {
                        // The actual source must register then recheck this
                        // original generation/predicate before waiting.
                        assert!(turn.remaining() < 5);
                    }
                    KeyWorkerStep::Progressed | KeyWorkerStep::Yielded => {}
                }
            }
            if finished {
                break;
            }
        }
        assert!(finished);
        assert!(workers.iter().all(|worker| worker.retained() == 0));
        assert!(claimed.iter().all(|claimed| *claimed));
        if !strings {
            for phase in ["Init", "Mark", "Count", "Prefix", "Fill", "Assign"] {
                let workers = workers
                    .iter()
                    .enumerate()
                    .filter(|(_, worker)| {
                        worker.claim_phases().iter().any(|claimed| claimed == phase)
                    })
                    .map(|(id, _)| id)
                    .collect::<std::collections::BTreeSet<_>>();
                assert!(
                    workers.len() > 1,
                    "actual direct {phase} task claims: {workers:?}"
                );
            }
        }
        let (view, assignments) = graph.take_ready(&work).unwrap().unwrap().into_parts();
        assert_eq!(
            assignments
                .iter()
                .map(|chunk| chunk.matches().len())
                .sum::<usize>(),
            9000
        );
        assert!(view.partition_count() >= 1);
        assert!(graph.take_ready(&work).unwrap().is_none());
    }
}

#[test]
fn all_workers_can_hold_blocked_routes_without_starving_original_eager_merge_claims() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let pages = vec![
        (0..3000).map(Some).collect(),
        (3000..6000).map(Some).collect(),
    ];
    let (graph, _) = captured_graph(&pages, true, &work);
    let changed = Arc::new(Observable::new());
    let wakes = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&wakes);
    let _subscription = changed.subscribe(Arc::new(move || {
        observed.fetch_add(1, Ordering::Relaxed);
    }));
    let mut workers: Vec<_> = (0..3)
        .map(|_| KeyBuildWorker::new(&work, test_contract))
        .collect();
    // First freeze the difficult state using the actual private K leases:
    // W original routes occupy W workers; no merge has been claimed yet.
    for worker in &mut workers {
        let BulkClaim::Step(task) = graph.claim(&work).unwrap() else {
            panic!("actual route lease required");
        };
        worker.retained.push(Retained {
            task,
            blocked: false,
        });
        worker.observed = changed.generation();
    }
    for worker in &mut workers {
        let retained = &mut worker.retained[0];
        let mut blocked = false;
        for _ in 0..100_000 {
            match retained.task.advance(1, &work).unwrap() {
                StepAdvance::Blocked => {
                    blocked = true;
                    break;
                }
                StepAdvance::Progressed => {}
                StepAdvance::Done => {
                    panic!("frontier stress must block before this route completes")
                }
            }
        }
        assert!(blocked);
        retained.blocked = true;
    }
    assert!(graph.take_ready(&work).unwrap().is_none());
    let mut retained_merge = false;
    let mut finished = false;
    for _ in 0..1_000_000 {
        for worker in &mut workers {
            let mut turn = TurnWork::new(NonZeroU64::MIN, 7);
            let step = worker.step(&graph, &changed, &mut turn, &work).unwrap();
            retained_merge |= worker.retained() > 1;
            if step == KeyWorkerStep::Finished {
                finished = true;
                break;
            }
        }
        if finished {
            break;
        }
    }
    assert!(finished);
    assert!(
        retained_merge,
        "eager merge must coexist with an original retained route"
    );
    assert!(wakes.load(Ordering::Relaxed) > 0);
    let (_, assignments) = graph.take_ready(&work).unwrap().unwrap().into_parts();
    assert_eq!(
        assignments
            .iter()
            .map(|chunk| chunk.matches().len())
            .sum::<usize>(),
        6000
    );
}

#[test]
fn zero_budget_does_not_claim_and_original_failure_precedes_cursor_drop() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let (graph, _) = captured_graph(&[vec![Some(3); 1024]], true, &work);
    let changed = Observable::new();
    let mut worker = KeyBuildWorker::new(&work, test_contract);
    let mut empty = TurnWork::new(NonZeroU64::MIN, 0);
    assert_eq!(
        worker.step(&graph, &changed, &mut empty, &work).unwrap(),
        KeyWorkerStep::Yielded
    );
    assert_eq!(worker.retained(), 0);
    let mut turn = TurnWork::new(NonZeroU64::MIN, 2);
    worker.step(&graph, &changed, &mut turn, &work).unwrap();
    assert_eq!(worker.retained(), 1);
    let cause: ExecutionFailure = novarocks_functions::KernelFailure::DeadlineExceeded.into();
    assert_eq!(graph.fail(cause.clone()), cause);
    drop(worker);
    assert_eq!(graph.fail(test_contract("later cursor drop")), cause);
    match graph.take_ready(&work) {
        Err(actual) => assert_eq!(actual, cause),
        Ok(_) => panic!("failed original graph cannot publish Ready"),
    }
}

struct RefuseRetainedBacking<'a> {
    original: &'a dyn InputConstructionOwner,
    backing: Layout,
    refused: AtomicUsize,
}

impl InputConstructionOwner for RefuseRetainedBacking<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.original.storage_lifetime()
    }

    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        if layout == self.backing {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(ConstructionFailure::Control(
                CompileControlError::ResourceExhausted,
            ));
        }
        self.original.request(layout)
    }

    fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        self.original.observe_opaque_upper_bound(bytes)
    }

    fn observe(
        &self,
        occurrence: usize,
        path: &[ArrayPathStep],
        step: ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.original.observe(occurrence, path, step)
    }
}

#[test]
fn retained_backing_refusal_precedes_drop_of_the_new_claim() {
    let (owner, tracker) = fixture();
    let normal = work(owner.as_ref(), &tracker, &Control);
    let (graph, _) = captured_graph(&[vec![Some(3); 1024]], true, &normal);
    let changed = Observable::new();
    let mut worker = KeyBuildWorker::new(&normal, test_contract);
    let growth =
        novarocks_type_contract::owned_resources::vec::push_growth(&worker.retained).unwrap();
    let denying = RefuseRetainedBacking {
        original: owner.as_ref(),
        backing: growth.requested_backing.unwrap(),
        refused: AtomicUsize::new(0),
    };
    let refused = work(&denying, &tracker, &Control);
    let mut turn = TurnWork::new(NonZeroU64::MIN, 2);
    let cause = ExecutionFailure::from(ConstructionFailure::Control(
        CompileControlError::ResourceExhausted,
    ));

    assert_eq!(
        error(worker.step(&graph, &changed, &mut turn, &refused)),
        cause
    );
    assert_eq!(denying.refused.load(Ordering::Relaxed), 1);
    assert_eq!(worker.claim_phases().len(), 1);
    assert_eq!(worker.retained(), 0);
    assert_eq!(error(graph.claim(&normal)), cause);
    assert_eq!(error(graph.take_ready(&normal)), cause);
}
