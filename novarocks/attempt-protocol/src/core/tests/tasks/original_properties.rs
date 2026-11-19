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

//! Original graph and exact Quiesce-fence assertions at their AP owner.
//! No physical transport, financial credit, or actual-exit claim is made.

use super::*;
use crate::execution::ack_phase::emit_quiesce;
use novarocks_execution_contract::{EdgeOpenVersion, OperationKind, QuiesceParticipationReceipt};
use std::sync::Arc;

#[test]
fn original_edge_open_waits_for_its_own_producer_create_receipt() {
    let mut trace = DrainTrace::with_dispatched_creates();
    let producer = trace.producer;
    let root = trace.core.execution.graph().root_identity();
    assert_eq!(trace.creates.len(), 4);
    for destination in std::iter::once(root).chain(trace.middle.clone()) {
        trace.acknowledge_create(TaskStatus::created(destination).with_installed());
    }
    trace.pump();
    let task = trace.core.execution.task(producer.task_id()).unwrap();
    assert_eq!(task.state(), crate::remote_task::RemoteTaskState::Creating);
    assert_eq!(task.pending_updates(), 1);
    assert_eq!(trace.producer_opens, 0);
    assert_eq!(trace.core.execution.dispatcher().queued_items(), 0);

    // The original Native Create acknowledgement changes this producer,
    // while its already-recorded edge-open decision and identity remain.
    trace.acknowledge_create(TaskStatus::created(producer).with_installed());
    trace.run_round(None);
    let action = trace.action();
    let (queued, replay) = trace
        .core
        .execution
        .advance_update_send(producer, action)
        .unwrap();
    assert!(queued);
    assert!(replay.is_none());
    let send = trace.action();
    let BatchDispatch::Ready(AttemptEffect {
        body: EffectBody::SendBatch { batch, .. },
        ..
    }) = trace
        .core
        .execution
        .advance_send_batch(&mut trace.core.dispatch, send, &mut trace.transport, |_| {
            Ok(())
        })
        .unwrap()
    else {
        panic!("the original producer update must cross its normal send gate");
    };
    let updates: Vec<&OperationIntent> = batch
        .operations
        .iter()
        .filter(|operation| matches!(operation, OperationIntent::UpdateTask { .. }))
        .collect();
    assert_eq!(updates.len(), 1);
    let OperationIntent::UpdateTask {
        identity, domains, ..
    } = updates[0]
    else {
        panic!("the exact pending edge-open Update must be sent");
    };
    assert_eq!(*identity, producer);
    assert_eq!(domains.len(), 1);
    let TaskDomainIntent::OpenExchangeEdges { version, edges } = &domains[0] else {
        panic!("the original domain is edge-open");
    };
    assert_eq!(*version, EdgeOpenVersion::FIRST);
    assert_eq!(edges.as_slice(), [ExchangeEdgeId::new(2).unwrap()]);
}

#[test]
fn original_excluding_quiesce_refuses_the_same_late_accepted_create() {
    let mut trace = DrainTrace::with_dispatched_creates();
    let excluded = trace.middle[0];
    let peer = trace.middle[1];
    let participation = trace
        .core
        .execution
        .graph()
        .task(excluded.task_id())
        .unwrap()
        .participation();
    let receipt = Arc::new(QuiesceParticipationReceipt::new(
        participation,
        1,
        Vec::new(),
        ParticipationState::Quiescing,
    ));
    let original_fence = emit_quiesce(&mut trace.core.execution, receipt.clone());
    assert_eq!(
        original_fence.reply.kind(),
        OperationKind::QuiesceParticipation
    );
    let (_, applied) = trace.core.execution.apply_dispatched_reply(&original_fence);
    applied.unwrap();
    assert!(matches!(
        trace.core.execution.advance_quiesce(&mut trace.transport),
        Some(Ok(()))
    ));
    assert!(
        trace
            .core
            .execution
            .task(excluded.task_id())
            .unwrap()
            .fenced_out()
    );
    assert!(
        !trace
            .core
            .execution
            .task(peer.task_id())
            .unwrap()
            .fenced_out()
    );

    let (original_create, generation) = &trace.creates[&excluded];
    let reply = worker_accepted(
        original_create,
        *generation,
        Some(AckPayload::Create(Box::new(CreateTaskReceipt::new(
            excluded,
            Vec::new(),
            TaskStatus::created(excluded),
        )))),
    );
    assert_eq!(reply.reply.operation_id(), original_create.operation_id());
    let (_, result) = trace.core.execution.apply_dispatched_reply(&reply);
    assert!(
        matches!(result, Err(TaskExecutionError::DomainReceipt(message))
        if message == format!("accepted Create contradicts the exact Quiesce fence for task {excluded}"))
    );
    let task = trace.core.execution.task(excluded.task_id()).unwrap();
    assert!(task.fenced_out());
    assert_eq!(task.state(), crate::remote_task::RemoteTaskState::Terminal);
    assert!(!task.create_ownership_proven());
    assert!(!task.create_pending());
    assert_eq!(receipt.participation(), participation);
}

// Keep the original two leaf producers and one middle destination. This only
// freezes neutral graph metadata; all requests use the existing trace's real
// Core, Native prefix, round tail, queue admission, and exact reply authors.
fn original_two_producer_trace() -> (DrainTrace, TaskIdentity) {
    let mut start = start();
    let graph = &mut start.plan.graph;
    let root = graph.root;
    let original_root_participation = graph.tasks[&root.task_id()].participation;
    let middle = graph.tasks[&TaskId::new(2).unwrap()].identity;
    let middle_participation = graph.tasks[&middle.task_id()].participation;
    let producer = graph.tasks[&TaskId::new(3).unwrap()].identity;
    let producer_participation = graph.tasks[&producer.task_id()].participation;
    let root = TaskIdentity::new(
        root.query_execution_id(),
        root.stage_id(),
        root.task_id(),
        producer.backend_process_id(),
    );
    graph.root = root;
    let root_task = graph.tasks.get_mut(&root.task_id()).unwrap();
    root_task.identity = root;
    root_task.participation = producer_participation;
    let peer = TaskIdentity::new(
        producer.query_execution_id(),
        producer.stage_id(),
        TaskId::new(4).unwrap(),
        middle.backend_process_id(),
    );
    let mut peer_task = graph.tasks[&producer.task_id()].clone();
    peer_task.identity = peer;
    peer_task.participation = middle_participation;
    peer_task.task_key = UniqueId::new(4, 1);
    peer_task.creation.resource = PayloadRef(id(4));
    graph.tasks.insert(peer.task_id(), peer_task);
    graph.stages.insert(root.stage_id(), Box::from([root]));
    graph
        .stages
        .insert(producer.stage_id(), Box::from([producer, peer]));
    let root_edge = graph
        .edges
        .get_mut(&ExchangeEdgeId::new(1).unwrap())
        .unwrap();
    root_edge.delivery = ExchangeDeliveryRequirement::DemandDriven;
    root_edge.destinations = Box::from([root]);
    let edge = graph
        .edges
        .get_mut(&ExchangeEdgeId::new(2).unwrap())
        .unwrap();
    edge.delivery = ExchangeDeliveryRequirement::DemandDriven;
    edge.producers = Box::from([producer, peer]);
    edge.destinations = Box::from([middle]);
    start
        .plan
        .establish_material
        .remove(&original_root_participation);
    start
        .plan
        .admission_epochs
        .remove(&original_root_participation.backend_process_id());
    start
        .plan
        .preparation_positions
        .remove(&original_root_participation.backend_process_id());
    let tasks = graph.tasks.values().map(|task| task.identity).collect();
    let mut trace = DrainTrace {
        core: Core::new(start).unwrap(),
        producer,
        middle: vec![middle],
        transport: ExchangeTurn {
            blocked_create: None,
            blocked_update: None,
            refused: 0,
        },
        next: 100,
        replies: Vec::new(),
        openings: BTreeMap::new(),
        creates: BTreeMap::new(),
        closes: BTreeMap::new(),
        close_sends: 0,
        producer_opens: 0,
        cancels: Vec::new(),
        quiesces: Vec::new(),
        split_notices: Vec::new(),
        split_sends: Vec::new(),
    };
    trace.dispatch_creates(tasks);
    (trace, peer)
}

#[test]
fn original_two_producers_keep_their_own_create_gate_and_exact_edge_update() {
    let (mut trace, peer) = original_two_producer_trace();
    let producer = trace.producer;
    let middle = trace.middle[0];
    assert_eq!(trace.creates.len(), 4);
    let edge = trace
        .core
        .execution
        .graph()
        .edges()
        .find(|edge| edge.edge_id() == ExchangeEdgeId::new(2).unwrap())
        .unwrap();
    assert_eq!(edge.producers(), &[producer, peer]);
    assert_eq!(edge.destinations(), &[middle]);
    trace.acknowledge_create(TaskStatus::created(middle).with_installed());
    trace.pump();
    for original in [producer, peer] {
        let task = trace.core.execution.task(original.task_id()).unwrap();
        assert_eq!(task.state(), crate::remote_task::RemoteTaskState::Creating);
        assert_eq!(task.pending_updates(), 1);
    }
    assert_eq!(trace.producer_opens, 0);
    assert_eq!(trace.core.execution.dispatcher().queued_items(), 0);

    trace.acknowledge_create(TaskStatus::created(producer).with_installed());
    trace.run_round(None);
    for original in [producer, peer] {
        let action = trace.action();
        let (queued, replay) = trace
            .core
            .execution
            .advance_update_send(original, action)
            .unwrap();
        assert_eq!(queued, original == producer);
        assert!(replay.is_none());
    }
    let send = trace.action();
    let BatchDispatch::Ready(AttemptEffect {
        body: EffectBody::SendBatch { batch, .. },
        ..
    }) = trace
        .core
        .execution
        .advance_send_batch(&mut trace.core.dispatch, send, &mut trace.transport, |_| {
            Ok(())
        })
        .unwrap()
    else {
        panic!("only the acknowledged producer may send its original Update");
    };
    let updates: Vec<&OperationIntent> = batch
        .operations
        .iter()
        .filter(|operation| matches!(operation, OperationIntent::UpdateTask { .. }))
        .collect();
    assert_eq!(updates.len(), 1);
    let OperationIntent::UpdateTask {
        identity, domains, ..
    } = updates[0]
    else {
        unreachable!()
    };
    assert_eq!(*identity, producer);
    assert!(
        matches!(domains.as_ref(), [TaskDomainIntent::OpenExchangeEdges { version, edges }] if *version == EdgeOpenVersion::FIRST && edges.as_slice() == [ExchangeEdgeId::new(2).unwrap()])
    );
    let held = trace.core.execution.task(peer.task_id()).unwrap();
    assert_eq!(held.state(), crate::remote_task::RemoteTaskState::Creating);
    assert_eq!(held.pending_updates(), 1);
}

#[test]
fn original_unsent_create_ack_is_rejected_before_any_receipt_is_adopted() {
    let mut execution = crate::test_support::execution_with_strict_input();
    let operation = crate::ids::OperationIds::new(crate::ids::IdSeed::new(1900, 1)).issue();
    let error = execution
        .acknowledge(&OperationReply {
            operation_id: operation,
            kind: OperationKind::CreateTask,
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: None,
            }),
        })
        .unwrap_err();
    assert_eq!(error, TaskExecutionError::UnknownOperation);
    assert_eq!(execution.accepted_protocol_revision(), 0);
    assert_eq!(execution.dispatcher().in_flight_operations(), 0);
}
