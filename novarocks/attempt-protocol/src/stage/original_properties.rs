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

//! Frozen multicast membership and each producer's original edge-open domain.
//! Payload refs and queue grants here are passive component inputs, not WC proof.

use super::*;
use crate::{
    effect::TaskDomainIntent,
    ids::IdSeed,
    input::{
        AckPayload, OperationReply, OperationReplyResult, QueuedInputGrantRef, SendGeneration,
        UpdateAdmission,
    },
    start::{FrozenCreation, FrozenExchangeEdge, FrozenTask, FrozenTaskGraph, PayloadRef},
};
use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};
use novarocks_execution_contract::{
    CreateTaskReceipt, DomainProgression, EdgeOpenVersion, ExchangeDeliveryRequirement,
    OperationOutcome, ParticipationRef, TaskDomainReceipt, TaskStatus, UpdateTaskReceipt,
    descriptor::FragmentNodeId, operation::OperationResult,
};
use novarocks_types::{FrontendProcessId, UniqueId};
use std::num::NonZeroU64;

struct Multicast {
    graph: TaskGraph,
    creation: BTreeMap<TaskId, FrozenCreation>,
    leaves: Vec<TaskIdentity>,
    middle: Vec<TaskIdentity>,
}

fn multicast(width: u32) -> Multicast {
    let execution = crate::test_support::remote(1)
        .identity()
        .query_execution_id();
    let frontend =
        FrontendProcessId::try_from_bytes(crate::test_support::backend(700).to_bytes()).unwrap();
    let mut tasks = BTreeMap::new();
    let mut stages = BTreeMap::new();
    let mut creation = BTreeMap::new();
    let mut add = |stage: u32, index: u32, backend: u64| {
        let stage = StageId::new(stage).unwrap();
        let task = TaskId::new(index).unwrap();
        let identity = TaskIdentity::new(
            execution,
            stage,
            task,
            crate::test_support::backend(backend),
        );
        let frozen_creation = FrozenCreation {
            resource: PayloadRef(NonZeroU64::new(u64::from(index)).unwrap()),
            queued_bytes: 512,
        };
        tasks.insert(
            task,
            FrozenTask {
                identity,
                participation: ParticipationRef::new(
                    execution,
                    frontend,
                    identity.backend_process_id(),
                ),
                task_key: UniqueId::new(i64::from(index), 1),
                fragment_id: stage.get(),
                split_plan_nodes: Box::new([]),
                creation: frozen_creation,
            },
        );
        creation.insert(task, frozen_creation);
        identity
    };
    let leaves: Vec<_> = (0..width)
        .map(|n| add(1, n + 1, u64::from(n + 1)))
        .collect();
    let middle: Vec<_> = (0..width)
        .map(|n| add(2, width + n + 1, u64::from(n + 1)))
        .collect();
    let root = add(3, width * 2 + 1, 1);
    stages.insert(StageId::new(1).unwrap(), leaves.clone().into_boxed_slice());
    stages.insert(StageId::new(2).unwrap(), middle.clone().into_boxed_slice());
    stages.insert(root.stage_id(), Box::from([root]));
    let edge = |id, producer_stage, consumer_stage, producers: Vec<_>, destinations: Vec<_>| {
        let edge = ExchangeEdgeId::new(id).unwrap();
        (
            edge,
            FrozenExchangeEdge {
                edge,
                producer_stage: StageId::new(producer_stage).unwrap(),
                consumer_stage: StageId::new(consumer_stage).unwrap(),
                destination_node: FragmentNodeId::new(i32::try_from(id).unwrap()),
                delivery: ExchangeDeliveryRequirement::DemandDriven,
                producers: producers.into_boxed_slice(),
                destinations: destinations.into_boxed_slice(),
            },
        )
    };
    let graph = TaskGraph::from_frozen(FrozenTaskGraph {
        root,
        tasks,
        stages,
        edges: BTreeMap::from([
            edge(1, 2, 3, middle.clone(), vec![root]),
            edge(2, 1, 2, leaves.clone(), middle.clone()),
            edge(3, 1, 3, leaves.clone(), vec![root]),
        ]),
    })
    .unwrap();
    Multicast {
        graph,
        creation,
        leaves,
        middle,
    }
}

#[test]
fn original_mixed_multicast_destination_does_not_close_a_sibling_edge() {
    let fixture = multicast(2);
    assert_eq!(fixture.graph.tasks().len(), 5);
    let mixed = fixture
        .graph
        .edges()
        .find(|edge| edge.edge_id() == ExchangeEdgeId::new(2).unwrap())
        .unwrap();
    assert_eq!(mixed.producers(), fixture.leaves.as_slice());
    assert_eq!(mixed.destinations(), fixture.middle.as_slice());
    let sibling = fixture
        .graph
        .edges()
        .find(|edge| edge.edge_id() == ExchangeEdgeId::new(3).unwrap())
        .unwrap();
    assert_eq!(sibling.producers(), mixed.producers());
    assert_eq!(sibling.destinations(), [fixture.graph.root_identity()]);
    let [needed, closed] = fixture.middle.as_slice() else {
        unreachable!()
    };
    let mut tracker = EdgeOpenTracker::from_graph(&fixture.graph);
    assert!(tracker.note_created(*needed).is_empty());
    let resolution = tracker.note_normally_closed(*closed);
    assert_eq!(resolution.closed().len(), 1);
    assert_eq!(resolution.closed()[0].edge_id(), mixed.edge_id());
    assert_eq!(resolution.closed()[0].destination(), *closed);
    assert_eq!(resolution.opened().len(), 1);
    assert_eq!(resolution.opened()[0].edge_id(), mixed.edge_id());
    assert_eq!(resolution.opened()[0].normally_closed(), [*closed]);
    let opened = tracker.note_created(fixture.graph.root_identity());
    let sibling_opened: Vec<_> = opened
        .iter()
        .filter(|decision| decision.edge_id() == sibling.edge_id())
        .collect();
    assert_eq!(sibling_opened.len(), 1);
    assert!(sibling_opened[0].normally_closed().is_empty());
}

fn accepted(operation: &OperationIntent, payload: AckPayload) -> OperationReply {
    OperationReply {
        operation_id: operation.operation_id(),
        kind: operation.kind(),
        result: OperationReplyResult::Worker(OperationResult::Verdict {
            outcome: OperationOutcome::Accepted,
            detail: None,
            acknowledgement: Some(payload),
        }),
    }
}

#[test]
fn original_multicast_producer_versions_each_exact_edge_and_finishes_created() {
    let fixture = multicast(3);
    assert_eq!(fixture.graph.tasks().len(), 7);
    let mut ids = OperationIds::new(IdSeed::new(1000, 1));
    let mut remote = BTreeMap::new();
    for seed in fixture.graph.tasks() {
        let outbound: Vec<_> = fixture
            .graph
            .edges()
            .filter(|edge| edge.producers().contains(&seed.identity()))
            .collect();
        let mut task = RemoteTask::new(
            seed,
            fixture.creation[&seed.task_id()],
            outbound.iter().map(|edge| edge.edge_id()).collect(),
            &mut ids,
            outbound
                .iter()
                .flat_map(|edge| {
                    edge.destinations()
                        .iter()
                        .map(|destination| (edge.edge_id(), *destination))
                })
                .collect(),
        )
        .unwrap();
        let create = task.release_create(MonotonicInstant::ORIGIN).unwrap();
        task.on_create_ack(
            &accepted(
                &create,
                AckPayload::Create(Box::new(CreateTaskReceipt::new(
                    seed.identity(),
                    Vec::new(),
                    TaskStatus::created(seed.identity()).with_installed(),
                ))),
            ),
            MonotonicInstant::ORIGIN,
            true,
            None,
        )
        .unwrap();
        assert_eq!(task.state(), RemoteTaskState::Created);
        remote.insert(seed.identity(), task);
    }
    let mut tracker = EdgeOpenTracker::from_graph(&fixture.graph);
    let mut opens = BTreeMap::<TaskIdentity, Vec<(EdgeOpenVersion, Vec<ExchangeEdgeId>)>>::new();
    let mut grant_sequence = 1;
    // Each exact Created observation feeds the original edge decision author.
    for destination in fixture.graph.tasks().map(|task| task.identity()) {
        for decision in tracker.note_created(destination) {
            let edge = fixture
                .graph
                .edges()
                .find(|edge| edge.edge_id() == decision.edge_id())
                .unwrap();
            for producer in edge.producers() {
                let task = remote.get_mut(producer).unwrap();
                let update = task.prepare_edge_open(decision.edge_id()).unwrap();
                let sequence = NonZeroU64::new(grant_sequence).unwrap();
                grant_sequence += 1;
                let grant = QueuedInputGrantRef::new(
                    sequence,
                    SendGeneration::new(AttemptActionId::new(sequence)),
                    64,
                );
                assert_eq!(
                    task.enqueue_update(ids.issue(), update, grant).unwrap(),
                    UpdateAdmission::Queued
                );
                let (intent, returned_grant) = task.next_update_intent().unwrap().unwrap();
                assert_eq!(returned_grant, Some(grant));
                let OperationIntent::UpdateTask {
                    identity, domains, ..
                } = &intent
                else {
                    unreachable!()
                };
                assert_eq!(identity, producer);
                let [TaskDomainIntent::OpenExchangeEdges { version, edges }] = domains.as_ref()
                else {
                    unreachable!()
                };
                assert_eq!(edges.as_slice(), [decision.edge_id()]);
                opens
                    .entry(*producer)
                    .or_default()
                    .push((*version, edges.clone()));
                task.on_update_ack(&accepted(
                    &intent,
                    AckPayload::Update(UpdateTaskReceipt::new(
                        *producer,
                        vec![TaskDomainReceipt::OpenExchangeEdges {
                            accepted_version: *version,
                            opened: edges.clone(),
                            progression: DomainProgression::Apply,
                        }],
                    )),
                ))
                .unwrap();
                assert!(task.next_update_intent().unwrap().is_none());
            }
        }
    }
    for leaf in &fixture.leaves {
        let decisions = &opens[leaf];
        assert_eq!(decisions.len(), 2);
        assert_eq!(
            decisions
                .iter()
                .map(|(version, _)| *version)
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        assert_eq!(
            decisions
                .iter()
                .flat_map(|(_, edges)| edges.iter().copied())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                ExchangeEdgeId::new(2).unwrap(),
                ExchangeEdgeId::new(3).unwrap()
            ])
        );
    }
    for task in remote.values() {
        assert_eq!(task.state(), RemoteTaskState::Created);
        assert!(!task.create_pending());
    }
}
