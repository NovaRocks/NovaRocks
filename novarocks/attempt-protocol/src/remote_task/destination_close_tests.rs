// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::input::{LocalDispatchResult, OperationReplyResult};
use novarocks_execution_contract::DomainVersion;
use novarocks_types::{StageId, TaskId};
use std::{num::NonZeroU64, sync::Arc};
struct TestIds {
    operations: OperationIds,
    grant: u64,
}
fn identity(task: u32, backend: novarocks_types::BackendProcessId) -> TaskIdentity {
    TaskIdentity::new(
        crate::test_support::remote(1)
            .identity()
            .query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(task).unwrap(),
        backend,
    )
}
fn producer_with_two_destinations() -> (
    RemoteTask,
    ExchangeEdgeId,
    TaskIdentity,
    TaskIdentity,
    TestIds,
) {
    let mut producer = crate::test_support::remote(1);
    let backend = producer.identity.backend_process_id();
    let target = identity(2, backend);
    let sibling = identity(3, backend);
    let edge = ExchangeEdgeId::new(1).unwrap();
    producer.domains = TaskDomainIntentTracker::new([], [edge])
        .with_exchange_destinations([(edge, target), (edge, sibling)]);
    producer.creation = CreationReplay::Settled;
    producer.state = RemoteTaskState::Created;
    producer.create_ownership_proven = true;
    producer.status = Some(TaskStatus::created(producer.identity).with_installed());
    (
        producer,
        edge,
        target,
        sibling,
        TestIds {
            operations: OperationIds::new(crate::ids::IdSeed::new(1000, 100)),
            grant: 1,
        },
    )
}
fn enqueue(
    producer: &mut RemoteTask,
    ids: &mut TestIds,
    update: TaskDomainUpdate,
) -> Result<UpdateAdmission, TaskExecutionError> {
    let original = QueuedInputGrantRef::new(
        NonZeroU64::new(ids.grant).unwrap(),
        crate::input::SendGeneration::new(novarocks_coordination_contract::AttemptActionId::new(
            NonZeroU64::new(ids.grant).unwrap(),
        )),
    );
    ids.grant += 1;
    producer.enqueue_update(ids.operations.issue(), update, original)
}
fn worker_receipt(
    operation_id: TaskOperationId,
    kind: OperationKind,
    outcome: OperationOutcome,
    payload: AckPayload,
) -> OperationAcknowledgement {
    OperationAcknowledgement {
        operation_id,
        kind,
        result: OperationReplyResult::Worker(
            novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome,
                detail: None,
                acknowledgement: Some(payload),
            },
        ),
    }
}
fn transport_unknown(
    operation_id: TaskOperationId,
    kind: OperationKind,
) -> OperationAcknowledgement {
    OperationAcknowledgement {
        operation_id,
        kind,
        result: OperationReplyResult::Local {
            result: LocalDispatchResult::TransportUnknown,
            failure: None,
            protocol_failure: None,
            detail: None,
        },
    }
}
#[test]
fn rejected_create_snapshot_is_not_renewed_by_late_receipts_or_owned_tasks() {
    for (owned, established, expiry) in [
        (false, true, Some(Duration::from_secs(9))),
        (true, true, Some(Duration::from_secs(9))),
        (false, false, Some(Duration::from_secs(9))),
        (false, true, None),
        (false, true, Some(Duration::ZERO)),
    ] {
        let (mut task, _, _, _, _ids) = producer_with_two_destinations();
        let operation = task.create_envelope.operation_id();
        task.create_in_flight = Some(operation);
        task.create_ownership_proven = owned;
        let ack = worker_receipt(
            operation,
            OperationKind::CreateTask,
            OperationOutcome::IdentityMismatch,
            AckPayload::None,
        );
        // Preserve the old false-parent plus valid Applied-lease negative.
        // Unknown submitted parents require their distinct owner source.
        let old = expiry
            .map(MonotonicInstant::from_origin)
            .map(CreateRefusalObservationHorizon::AppliedLease);
        task.on_create_ack(&ack, MonotonicInstant::ORIGIN, established, old)
            .unwrap();
        let before =
            task.rejected_create_observation_remaining(operation, MonotonicInstant::ORIGIN);
        assert_eq!(
            before,
            (!owned && established && expiry == Some(Duration::from_secs(9)))
                .then_some(Duration::from_secs(9))
        );
        // A later effective lease cannot be used by a duplicate Create
        // receipt: the original in-flight operation was already consumed.
        assert!(
            task.on_create_ack(
                &ack,
                MonotonicInstant::ORIGIN,
                true,
                Some(CreateRefusalObservationHorizon::AppliedLease(
                    MonotonicInstant::from_origin(Duration::from_secs(90))
                ))
            )
            .is_err()
        );
        assert_eq!(
            task.rejected_create_observation_remaining(operation, MonotonicInstant::ORIGIN),
            before
        );
    }
}

#[test]
fn destination_close_rejects_nonmembers_and_versions_each_exact_member() {
    let (mut producer, edge, target, sibling, mut ids) = producer_with_two_destinations();
    let outsider = identity(4, target.backend_process_id());
    let invalid = producer.prepare_destination_close(edge, outsider).unwrap();
    assert!(matches!(
        enqueue(&mut producer, &mut ids, invalid),
        Err(TaskExecutionError::DomainRegression {
            conflict: DomainConflict::UnknownMember,
            ..
        })
    ));

    let first = producer.prepare_destination_close(edge, target).unwrap();
    assert!(matches!(
        first,
        TaskDomainUpdate::CloseExchangeDestination {
            version: DomainVersion::FIRST,
            destination,
            ..
        } if destination == target
    ));
    assert!(matches!(
        enqueue(&mut producer, &mut ids, first),
        Ok(UpdateAdmission::Queued)
    ));
    let second = producer.prepare_destination_close(edge, sibling).unwrap();
    assert!(matches!(
        second,
        TaskDomainUpdate::CloseExchangeDestination { version, destination, .. }
            if version.get() == 2 && destination == sibling
    ));
    assert!(matches!(
        enqueue(&mut producer, &mut ids, second),
        Ok(UpdateAdmission::Queued)
    ));
}

#[test]
fn owner_failed_create_stops_replay_without_synthesizing_terminal_status() {
    let (mut producer, _, _, _, _ids) = producer_with_two_destinations();
    producer.state = RemoteTaskState::Creating;
    producer.status = None;
    producer.create_ownership_proven = false;
    let operation_id = producer.create_envelope.operation_id();
    producer.create_in_flight = Some(operation_id);
    assert_eq!(
        producer
            .on_create_ack(
                &worker_receipt(
                    operation_id,
                    OperationKind::CreateTask,
                    OperationOutcome::OwnerFailed,
                    AckPayload::None
                ),
                MonotonicInstant::ORIGIN,
                true,
                None,
            )
            .unwrap(),
        CreateSettlement::FailedClosed(OperationOutcome::OwnerFailed)
    );
    assert!(!producer.is_terminal());
    assert!(producer.terminal_report().is_none());
    assert!(!producer.create_pending());
    assert!(
        producer
            .create_candidate(MonotonicInstant::ORIGIN)
            .unwrap()
            .is_none()
    );
    assert!(producer.awaiting_terminal_status);
}

#[test]
fn owner_failed_update_and_cancel_preserve_the_last_real_status() {
    let (mut producer, edge, target, _, mut ids) = producer_with_two_destinations();
    let before = producer.status.clone();
    let close = producer.prepare_destination_close(edge, target).unwrap();
    enqueue(&mut producer, &mut ids, close).unwrap();
    let (intent, _) = producer.next_update_intent().unwrap().unwrap();
    assert_eq!(
        producer
            .on_update_ack(&worker_receipt(
                intent.operation_id(),
                OperationKind::UpdateTask,
                OperationOutcome::OwnerFailed,
                AckPayload::None,
            ))
            .unwrap(),
        UpdateSettlement::FailedClosed(OperationOutcome::OwnerFailed)
    );
    assert!(!producer.is_terminal());
    assert!(producer.terminal_report().is_none());
    assert_eq!(producer.status, before);
    assert!(producer.next_update_intent().unwrap().is_none());

    let (mut producer, _, _, _, mut ids) = producer_with_two_destinations();
    let before = producer.status.clone();
    let cancel = producer
        .cancel_intent(CancelReason::UpstreamNoLongerNeeded, &mut ids.operations)
        .unwrap();
    assert!(matches!(
        producer.on_cancel_ack(&worker_receipt(
            cancel.operation_id(),
            OperationKind::CancelTask,
            OperationOutcome::OwnerFailed,
            AckPayload::None,
        )),
        Err(TaskExecutionError::OperationFailed { .. })
    ));
    assert!(!producer.is_terminal());
    assert!(producer.terminal_report().is_none());
    assert_eq!(producer.status, before);
}

#[test]
fn destination_close_transport_unknown_replays_the_same_passive_update() {
    let (mut producer, edge, target, _, mut ids) = producer_with_two_destinations();
    let close = producer.prepare_destination_close(edge, target).unwrap();
    enqueue(&mut producer, &mut ids, close).unwrap();
    let (first, first_permit) = producer.next_update_intent().unwrap().unwrap();
    assert!(first_permit.is_some());
    let OperationIntent::UpdateTask {
        envelope: first_envelope,
        domains: first_domains,
        ..
    } = first
    else {
        panic!("destination close is a task update");
    };
    assert_eq!(
        producer
            .on_update_ack(&transport_unknown(
                first_envelope.operation_id(),
                OperationKind::UpdateTask,
            ))
            .unwrap(),
        UpdateSettlement::RetryExactRequest
    );
    let (replay, replay_permit) = producer.next_update_intent().unwrap().unwrap();
    assert!(replay_permit.is_none());
    let OperationIntent::UpdateTask {
        envelope: replay_envelope,
        domains: replay_domains,
        ..
    } = replay
    else {
        panic!("replay remains a task update");
    };
    // The pure carrier preserves its original immutable domain allocation.
    // Actual Native request custody is verified by its own executor tests.
    assert!(Arc::ptr_eq(&first_domains, &replay_domains));
    assert_eq!(
        first_envelope.operation_id(),
        replay_envelope.operation_id()
    );
}

#[test]
fn normal_stand_down_discards_unknown_ordinary_update_before_exact_close() {
    let (mut producer, edge, target, _, mut ids) = producer_with_two_destinations();
    let open = producer.prepare_edge_open(edge).unwrap();
    enqueue(&mut producer, &mut ids, open).unwrap();
    let (ordinary, _) = producer.next_update_intent().unwrap().unwrap();
    producer.begin_normal_stand_down();
    let close = producer.prepare_destination_close(edge, target).unwrap();
    enqueue(&mut producer, &mut ids, close).unwrap();
    assert_eq!(
        producer
            .on_update_ack(&transport_unknown(
                ordinary.operation_id(),
                OperationKind::UpdateTask,
            ))
            .unwrap(),
        UpdateSettlement::DiscardedAfterNormalStandDown
    );
    let (next, _) = producer.next_update_intent().unwrap().unwrap();
    let OperationIntent::UpdateTask { domains, .. } = next else {
        panic!("the exact close follows the obsolete ordinary update");
    };
    assert!(matches!(
        domains.as_ref(),
        [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == target
    ));
}

#[test]
fn destination_close_reservation_matches_frozen_control_class() {
    let (mut producer, edge, target, _, mut ids) = producer_with_two_destinations();
    let close = producer.prepare_destination_close(edge, target).unwrap();
    let preview =
        TaskOperationQueueRequest::task_update(producer.identity().backend_process_id(), &close);
    assert!(preview.requires_control_progress());
    assert_eq!(preview.lane(), crate::config::DispatchLane::Update);
    enqueue(&mut producer, &mut ids, close).unwrap();
    let (frozen, _) = producer.next_update_intent().unwrap().unwrap();
    assert_eq!(preview, frozen.queue_request());
    assert!(
        producer.next_update_intent().unwrap().is_none(),
        "control reserve does not bypass the Task's single in-flight Update"
    );

    let (mut ordinary_producer, edge, _, _, mut ordinary_ids) = producer_with_two_destinations();
    let open = ordinary_producer.prepare_edge_open(edge).unwrap();
    let ordinary_preview = TaskOperationQueueRequest::task_update(
        ordinary_producer.identity().backend_process_id(),
        &open,
    );
    assert!(!ordinary_preview.requires_control_progress());
    enqueue(&mut ordinary_producer, &mut ordinary_ids, open).unwrap();
    let (ordinary_frozen, _) = ordinary_producer.next_update_intent().unwrap().unwrap();
    assert_eq!(ordinary_preview, ordinary_frozen.queue_request());
}
