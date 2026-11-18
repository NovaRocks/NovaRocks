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

//! Original lane and fanout properties at the passive dispatch owner.
//! Grants are test inputs; settling a generation is not physical Source exit.

use super::*;
use novarocks_execution_contract::{ReleaseParticipation, TaskIdentity};
use novarocks_types::{StageId, TaskId};

fn original_create(
    index: u32,
    backend: BackendProcessId,
    stage: u32,
    bytes: usize,
) -> OperationIntent {
    let original = crate::test_support::remote(1).identity();
    let mut intent = operation(u64::from(index));
    let OperationIntent::CreateTask {
        identity,
        queued_bytes,
        ..
    } = &mut intent
    else {
        unreachable!()
    };
    *identity = TaskIdentity::new(
        original.query_execution_id(),
        StageId::new(stage).unwrap(),
        TaskId::new(index).unwrap(),
        backend,
    );
    *queued_bytes = bytes;
    intent
}

fn assert_original_bounds(
    dispatcher: &OperationDispatcher,
    budget: DispatchBudget,
    limits: TransportBudget,
) {
    assert!(dispatcher.queued_items() <= limits.max_backend_queued_operations());
    assert!(dispatcher.queued_bytes() <= limits.max_backend_queued_bytes());
    let backends: Vec<_> = dispatcher.backends().collect();
    for backend in &backends {
        let queues = &dispatcher.backends[backend];
        assert!(queues.queued_items <= limits.max_query_backend_queued_operations());
        assert!(queues.queued_bytes <= limits.max_query_backend_queued_bytes());
        for lane in LANES {
            assert!(dispatcher.lane_in_flight(*backend, lane) <= budget.permits_for(lane));
        }
    }
    assert!(dispatcher.in_flight_operations() <= budget.total_permits() * backends.len());
}

fn accept_available_rotation(
    dispatcher: &mut OperationDispatcher,
    budget: DispatchBudget,
    limits: TransportBudget,
    seen: &mut BTreeSet<TaskOperationId>,
) -> Vec<DispatchBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = dispatcher.take_batch() {
        assert!(batches.len() < budget.total_permits() * dispatcher.backends().len());
        for operation in batch.operations.iter() {
            assert!(
                seen.insert(operation.operation_id()),
                "an unanswered original operation is sent once"
            );
        }
        dispatcher.accept(&batch).unwrap();
        assert_original_bounds(dispatcher, budget, limits);
        batches.push(batch);
    }
    batches
}

fn settle_original_rotation(dispatcher: &mut OperationDispatcher, batches: Vec<DispatchBatch>) {
    for batch in batches {
        for (operation, grant) in batch.operations.iter().zip(batch.queued_inputs.iter()) {
            assert!(
                dispatcher
                    .settle_generation(operation.operation_id(), grant.generation())
                    .unwrap()
            );
        }
    }
    assert_eq!(dispatcher.in_flight_operations(), 0);
}

#[test]
fn original_sixty_six_create_burst_preserves_lifecycle_in_the_same_rotation() {
    let budget = DispatchBudget::DEFAULT;
    let limits = limits();
    let mut dispatcher = OperationDispatcher::new(budget, limits);
    let backend = crate::test_support::backend(1);
    for index in 1..=66 {
        dispatcher.register_task(backend).unwrap();
        dispatcher
            .enqueue_reserved(
                original_create(index, backend, 1, 256),
                at(0),
                grant(u64::from(index)),
            )
            .unwrap();
    }
    let mut ids = OperationIds::new(IdSeed::new(10_000, 1));
    let lifecycle = OperationIntent::ReleaseParticipation(ReleaseParticipation::new(
        ids.issue(),
        crate::test_support::remote(1).participation(),
    ));
    let lifecycle_id = lifecycle.operation_id();
    dispatcher
        .enqueue_reserved(lifecycle, at(0), grant(10_000))
        .unwrap();
    let mut seen = BTreeSet::new();
    let first = accept_available_rotation(&mut dispatcher, budget, limits, &mut seen);
    assert!(first.iter().any(|batch| batch.lane == DispatchLane::Create));
    assert!(first.iter().any(|batch| {
        batch.lane == DispatchLane::Lifecycle
            && batch
                .operations
                .iter()
                .any(|operation| operation.operation_id() == lifecycle_id)
    }));
    assert!(
        dispatcher.queued_items() > 0,
        "the Create burst remains larger than one rotation"
    );
    settle_original_rotation(&mut dispatcher, first);
    for _ in 0..66 {
        if dispatcher.queued_items() == 0 {
            break;
        }
        let batches = accept_available_rotation(&mut dispatcher, budget, limits, &mut seen);
        assert!(!batches.is_empty());
        settle_original_rotation(&mut dispatcher, batches);
    }
    assert_eq!(seen.len(), 67);
    assert_eq!(dispatcher.queued_items(), 0);
    assert_eq!(dispatcher.queued_bytes(), 0);
}

#[test]
fn original_ten_hundred_thousand_leaf_fanout_keeps_four_backend_bounds_and_parallel_rounds() {
    for leaves in [10_u32, 100, 1000] {
        let budget = DispatchBudget::DEFAULT;
        let limits = limits();
        let mut dispatcher = OperationDispatcher::new(budget, limits);
        // Original shape: N leaf tasks, four middle tasks, and one Root.
        for index in 1..=leaves + 5 {
            let (backend, stage) = if index <= leaves {
                (
                    crate::test_support::backend(u64::from((index - 1) % 4 + 1)),
                    1,
                )
            } else if index <= leaves + 4 {
                (crate::test_support::backend(u64::from(index - leaves)), 2)
            } else {
                (crate::test_support::backend(1), 3)
            };
            dispatcher.register_task(backend).unwrap();
            dispatcher
                .enqueue_reserved(
                    original_create(index, backend, stage, 256),
                    at(0),
                    grant(u64::from(index)),
                )
                .unwrap();
            assert_original_bounds(&dispatcher, budget, limits);
        }
        assert_eq!(dispatcher.backends().len(), 4);
        let mut seen = BTreeSet::new();
        let mut rounds = 0;
        while dispatcher.queued_items() != 0 {
            rounds += 1;
            assert!(rounds <= usize::try_from(leaves).unwrap() + 32);
            let batches = accept_available_rotation(&mut dispatcher, budget, limits, &mut seen);
            assert!(!batches.is_empty());
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.lane == DispatchLane::Create)
            );
            settle_original_rotation(&mut dispatcher, batches);
        }
        assert_eq!(seen.len(), usize::try_from(leaves + 5).unwrap());
        let busiest = usize::try_from(leaves).unwrap().div_ceil(4) + 1;
        let expected = busiest.div_ceil(budget.create_permits());
        assert!(rounds <= expected + 1);
        assert!(busiest <= budget.create_permits() || rounds > 1);
        assert_eq!(dispatcher.queued_bytes(), 0);
    }
}

#[test]
fn original_sixty_six_large_quotes_wait_at_the_target_bound_and_drain_without_duplicate_send() {
    let budget = DispatchBudget::DEFAULT;
    let bytes = 15 * 1024 * 1024;
    // These are passive quote limits for this component, not an I/O grant or
    // a claim that the fixture allocates sixty-six Native request bodies.
    let limits = TransportBudget {
        ordinary_request_max_bytes: bytes,
        ordinary_queued_max_bytes: bytes,
        max_query_backend_queued_bytes: bytes * 2,
        max_backend_queued_bytes: bytes * 4,
        ..limits()
    };
    let mut dispatcher = OperationDispatcher::new(budget, limits);
    let backend = crate::test_support::backend(1);
    let mut pending = (1..=66)
        .map(|index| original_create(index, backend, 1, bytes))
        .peekable();
    let mut seen = BTreeSet::new();
    let mut target_full = 0;
    let mut admitted = BTreeSet::new();
    for _ in 0..256 {
        while let Some(operation) = pending.peek() {
            match dispatcher.local_capacity(operation.queue_request()) {
                LocalQueueCapacity::Fits => {
                    let operation = pending.next().unwrap();
                    let id = operation.operation_id();
                    assert!(admitted.insert(id));
                    let input = grant(u64::try_from(admitted.len()).unwrap());
                    dispatcher.register_task(backend).unwrap();
                    dispatcher
                        .enqueue_reserved(operation, at(0), input)
                        .unwrap();
                    assert_original_bounds(&dispatcher, budget, limits);
                }
                LocalQueueCapacity::TargetFull => {
                    target_full += 1;
                    break;
                }
                LocalQueueCapacity::AttemptFull => {
                    panic!("the smaller original target bound must refuse first")
                }
            }
        }
        if dispatcher.queued_items() == 0 {
            break;
        }
        let batches = accept_available_rotation(&mut dispatcher, budget, limits, &mut seen);
        assert!(!batches.is_empty());
        settle_original_rotation(&mut dispatcher, batches);
    }
    assert!(
        target_full > 0,
        "the original substantial quote actually reaches target backpressure"
    );
    assert!(
        pending.next().is_none(),
        "all sixty-six original quotes eventually enter their own queue"
    );
    assert_eq!(admitted.len(), 66);
    assert_eq!(seen, admitted);
    assert_eq!(dispatcher.queued_items(), 0);
    assert_eq!(dispatcher.queued_bytes(), 0);
    assert_eq!(
        CapacityBound::BackendBytes { limit: 1 }.as_operation_outcome(),
        novarocks_execution_contract::OperationOutcome::ResourceExhausted
    );
}
