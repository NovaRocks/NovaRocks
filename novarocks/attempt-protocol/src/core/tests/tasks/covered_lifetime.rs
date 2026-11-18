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

//! Original ACK gates at the pure AP owner, without physical-exit claims.

use super::*;
use novarocks_execution_contract::TaskFailurePhase;

/// Use the same round-tail fixture, stopping before any Establish receipt.
/// All admissions, sends, replies and subscription requests are original AP
/// values; no owner flag or subscription-controller state is assigned here.
fn before_establish_receipts() -> (DrainTrace, Vec<(ParticipationRef, OperationObservation)>) {
    let core = Core::new(start()).unwrap();
    let identities: Vec<_> = core
        .execution
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect();
    let mut trace = DrainTrace {
        core,
        producer: identities[2],
        middle: vec![identities[1]],
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
    let participants: Vec<_> = trace
        .core
        .execution
        .graph()
        .participations()
        .copied()
        .collect();
    assert!(participants.len() > 1);
    let mut receipts = Vec::new();
    // Complete the original Acquire batches before retaining Establish sends.
    // The existing admission helper uses sequence + 100 for its send effect;
    // it must not reuse an effect that still owns an emitted Establish batch.
    for (index, &participation) in participants.iter().enumerate() {
        let request = admission_intent(&mut trace.core.execution, participation);
        let receipt = dispatch_admission(
            &mut trace.core.execution,
            request,
            index as u64 + 1,
            OperationOutcome::Accepted,
        );
        trace
            .core
            .execution
            .apply_dispatched_reply(&receipt)
            .1
            .unwrap();
    }
    assert_eq!(trace.core.execution.dispatcher().in_flight_operations(), 0);
    for participation in participants {
        let action = trace.action();
        let grant = QueuedInputGrantRef::new(id(trace.next), SendGeneration::new(action), 64);
        let establish = queue_establish(&mut trace.core.execution, participation, grant);
        let OperationIntent::EstablishParticipation {
            initial_lease_sequence,
            initial_lease_valid_for,
            ..
        } = &establish
        else {
            panic!("the original participation's Establish")
        };
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
            panic!("the original Establish send")
        };
        assert_eq!(batch.queued_inputs.as_ref(), [grant]);
        assert_eq!(batch.operations.len(), 1);
        assert_eq!(batch.operations[0].operation_id(), establish.operation_id());
        trace.core.execution.observe_send_started(send);
        trace
            .core
            .execution
            .establish_send_started(participation, establish.operation_id())
            .unwrap();
        let active = ParticipationReceipt::new(participation, ParticipationState::Active)
            .with_lease(LeaseReceipt::new(
                *initial_lease_sequence,
                *initial_lease_valid_for,
                Duration::from_secs(9),
            ));
        receipts.push((
            participation,
            worker_accepted(
                &establish,
                grant.generation(),
                Some(AckPayload::Participation(active)),
            ),
        ));
    }
    assert_eq!(
        trace.core.execution.dispatcher().in_flight_operations(),
        receipts.len()
    );
    (trace, receipts)
}

#[test]
fn each_covered_subscription_opens_only_after_its_own_establish_ack() {
    let (mut trace, receipts) = before_establish_receipts();
    trace.run_round(None);
    assert!(
        trace.openings.is_empty(),
        "sending Establish cannot open any subscription"
    );
    for (part, _) in &receipts {
        assert!(
            !trace
                .core
                .execution
                .owner(*part)
                .unwrap()
                .establish_acknowledged()
        );
    }

    let (first, receipt) = &receipts[0];
    trace.replies.push(receipt.clone());
    trace.run_round(None);
    assert_eq!(trace.openings.keys().copied().collect::<Vec<_>>(), [*first]);
    for (part, _) in &receipts[1..] {
        assert!(
            !trace
                .core
                .execution
                .owner(*part)
                .unwrap()
                .establish_acknowledged()
        );
        assert!(!trace.openings.contains_key(part));
    }
    trace
        .replies
        .extend(receipts[1..].iter().map(|(_, receipt)| receipt.clone()));
    trace.run_round(None);
    assert_eq!(
        trace.openings.keys().copied().collect::<Vec<_>>(),
        receipts.iter().map(|(part, _)| *part).collect::<Vec<_>>(),
    );
    assert!(trace.creates.is_empty());
}

#[test]
fn establish_and_create_start_gates_observe_receipts_not_sends() {
    let (mut trace, receipts) = before_establish_receipts();
    let tasks: Vec<_> = trace
        .core
        .execution
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect();
    let assert_unowned = |trace: &DrainTrace| {
        for task in &tasks {
            assert!(
                !trace
                    .core
                    .execution
                    .task(task.task_id())
                    .unwrap()
                    .create_ownership_proven()
            );
        }
    };
    assert_unowned(&trace);
    assert!(!receipts.iter().all(|(part, _)| {
        trace
            .core
            .execution
            .owner(*part)
            .unwrap()
            .establish_acknowledged()
    }));

    trace.replies.push(receipts[0].1.clone());
    trace.run_round(None);
    assert!(
        trace
            .core
            .execution
            .owner(receipts[0].0)
            .unwrap()
            .establish_acknowledged()
    );
    assert!(!receipts.iter().all(|(part, _)| {
        trace
            .core
            .execution
            .owner(*part)
            .unwrap()
            .establish_acknowledged()
    }));
    assert_unowned(&trace);
    trace
        .replies
        .extend(receipts[1..].iter().map(|(_, receipt)| receipt.clone()));
    trace.run_round(None);
    assert!(receipts.iter().all(|(part, _)| {
        trace
            .core
            .execution
            .owner(*part)
            .unwrap()
            .establish_acknowledged()
    }));
    assert_unowned(&trace);

    for task in &tasks {
        let action = trace.action();
        let Some(AttemptEffect {
            body:
                EffectBody::AdmitQueuedInput {
                    operation,
                    generation,
                    ..
                },
            ..
        }) = trace
            .core
            .execution
            .begin_create_admission(*task, action)
            .unwrap()
        else {
            panic!("the exact original Create candidate")
        };
        let grant = QueuedInputGrantRef::new(id(trace.next), generation, 64);
        assert!(matches!(
            trace
                .core
                .execution
                .observe_create_grant(operation.operation_id(), grant)
                .unwrap(),
            CreateGrantProgress::Queued,
        ));
    }
    trace.run_round(None);
    assert_eq!(trace.creates.len(), tasks.len());
    assert_unowned(&trace);
    trace.acknowledge_create(TaskStatus::created(tasks[0]));
    trace.run_round(None);
    assert!(
        trace
            .core
            .execution
            .task(tasks[0].task_id())
            .unwrap()
            .create_ownership_proven()
    );
    for task in &tasks[1..] {
        assert!(
            !trace
                .core
                .execution
                .task(task.task_id())
                .unwrap()
                .create_ownership_proven()
        );
    }
    for task in &tasks[1..] {
        trace.acknowledge_create(TaskStatus::created(*task));
    }
    trace.run_round(None);
    assert!(tasks.iter().all(|task| {
        trace
            .core
            .execution
            .task(task.task_id())
            .unwrap()
            .create_ownership_proven()
    }));
}

#[test]
fn preparation_failure_in_create_ack_blocks_the_original_queued_creates_before_stream_repeat() {
    // The same queued original Creates are dispatchable when this receipt is
    // healthy. Capacity refusal therefore cannot explain the negative case.
    let mut healthy = DrainTrace::with_chain(ExchangeDeliveryRequirement::MustDrain);
    assert_eq!(healthy.creates.len(), 1);
    assert_eq!(healthy.core.execution.dispatcher().queued_items(), 2);
    healthy.transport.blocked_create = None;
    healthy.acknowledge_create(TaskStatus::created(healthy.middle[0]));
    healthy.run_round(None);
    assert_eq!(healthy.creates.len(), 3);
    assert!(healthy.core.execution.failure_cause().is_none());

    for state in [TaskState::Failing, TaskState::Failed] {
        let mut trace = DrainTrace::with_chain(ExchangeDeliveryRequirement::MustDrain);
        let original = trace.middle[0];
        assert_eq!(
            trace.creates.keys().copied().collect::<Vec<_>>(),
            [original]
        );
        assert_eq!(trace.core.execution.dispatcher().queued_items(), 2);
        let failure = TerminationDetail::Failed(TaskFailure::new_in_phase(
            FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(
                    NonZeroU16::new(u16::MAX).unwrap(),
                    NonZeroU16::new(3).unwrap(),
                ),
            ),
            SafeDetail::truncating("preparation failed"),
            TaskFailurePhase::Preparation,
        ));
        trace.transport.blocked_create = None;
        trace.acknowledge_create(
            TaskStatus::try_new_with_installed(
                original,
                TaskStatusVersion::FIRST,
                state,
                Some(failure.clone()),
                TaskOutputFacts::default(),
                false,
            )
            .unwrap(),
        );
        trace.run_round(None);
        assert_eq!(trace.core.execution.failure_cause(), Some(&failure));
        let selected = trace.core.execution.selected_task_failure().unwrap();
        assert_eq!(selected.identity(), original);
        assert_eq!(selected.detail(), &failure);
        assert_eq!(
            trace.creates.keys().copied().collect::<Vec<_>>(),
            [original]
        );
        let tasks: Vec<_> = trace
            .core
            .execution
            .graph()
            .tasks()
            .map(|task| task.identity())
            .collect();
        for task in tasks {
            let action = trace.action();
            assert!(
                trace
                    .core
                    .execution
                    .begin_create_admission(task, action)
                    .unwrap()
                    .is_none()
            );
        }
        trace.run_round(None);
        assert_eq!(
            trace.creates.keys().copied().collect::<Vec<_>>(),
            [original]
        );
    }
}
