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

//! Original Establish and Release properties at their participation owner.

use super::*;
use crate::input::{AckPayload, OperationReply, OperationReplyResult};
use novarocks_execution_contract::{AdmissionTicketId, ParticipationAdmissionTicketReceipt};

#[test]
fn release_is_withheld_while_the_third_create_remains_unresolved() {
    let seed = owner();
    let mut owner = ParticipationOwner::new(
        seed.participation,
        3,
        NativeCompatibilityId::new([0x71; 32]),
        AdmissionEpochCapability::try_from_bytes([0x72; 16]).unwrap(),
        RequestedLeaseDurations::DEFAULT,
    );
    let mut ids = OperationIds::new(crate::ids::IdSeed::new(101, 1));
    let now = MonotonicInstant::ORIGIN;
    owner.install_quiesce(Arc::new(QuiesceParticipationReceipt::new(
        owner.participation,
        1,
        Vec::new(),
        ParticipationState::Quiescing,
    )));
    for _ in 0..3 {
        owner.note_task_drained();
        owner.note_output_released();
    }
    for _ in 0..2 {
        owner.note_create_owned();
    }
    assert!(owner.locally_drained());
    assert!(!owner.creates_closed());
    assert!(owner.release_intent(now, &mut ids).is_none());
    owner.note_create_owned();
    let Some(OperationIntent::ReleaseParticipation(release)) = owner.release_intent(now, &mut ids)
    else {
        panic!("the final Create ownership proof opens the original Release");
    };
    assert_eq!(release.participation(), owner.participation);
}

#[test]
fn original_initial_establish_rejection_keeps_its_participation_operation_and_outcome() {
    let mut owner = owner();
    let mut ids = OperationIds::new(crate::ids::IdSeed::new(101, 1));
    let now = MonotonicInstant::ORIGIN;
    let OperationIntent::AcquireParticipationAdmissionTicket(request) =
        owner.admission_intent(now, &mut ids).unwrap().unwrap()
    else {
        panic!("the original owner requests admission");
    };
    let participation = request.participation();
    let ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
        participation,
        request.valid_for(),
    );
    owner
        .on_admission_ack(
            &OperationReply {
                operation_id: request.envelope().operation_id(),
                kind: OperationKind::AcquireParticipationAdmissionTicket,
                result: OperationReplyResult::Worker(
                    novarocks_execution_contract::operation::OperationResult::Verdict {
                        outcome: OperationOutcome::Accepted,
                        detail: None,
                        acknowledgement: Some(AckPayload::AdmissionTicket(ticket)),
                    },
                ),
            },
            now,
        )
        .unwrap();
    let establish = owner
        .establish_intent(
            ParticipationEstablishFacts {
                material: crate::start::PayloadRef(std::num::NonZeroU64::new(1).unwrap()),
                queued_bytes: 256,
            },
            now,
            &mut ids,
        )
        .unwrap()
        .unwrap();
    let error = owner
        .on_participation_ack(&OperationReply {
            operation_id: establish.operation_id(),
            kind: OperationKind::UpdateParticipation,
            result: OperationReplyResult::Worker(
                novarocks_execution_contract::operation::OperationResult::Verdict {
                    outcome: OperationOutcome::InvalidStateOrRequest,
                    detail: None,
                    acknowledgement: None,
                },
            ),
        })
        .unwrap_err();
    assert!(
        matches!(error, TaskExecutionError::PreReadyEstablishRejected {
        participation: actual, operation_id, rejection,
    } if actual == participation && operation_id == establish.operation_id()
        && rejection.outcome() == OperationOutcome::InvalidStateOrRequest)
    );
    assert!(owner.is_released());
}
