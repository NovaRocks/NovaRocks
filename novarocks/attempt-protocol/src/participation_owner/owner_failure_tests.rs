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
use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};

fn owner() -> ParticipationOwner {
    let participation = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(7101, 7102), AttemptId::new(1).unwrap()).unwrap(),
        novarocks_types::identity::FrontendProcessId::try_from_bytes(
            crate::test_support::backend(700).to_bytes(),
        )
        .unwrap(),
        crate::test_support::backend(7103),
    );
    ParticipationOwner::new(
        participation,
        1,
        NativeCompatibilityId::new([0x71; 32]),
        AdmissionEpochCapability::try_from_bytes([0x72; 16]).unwrap(),
        RequestedLeaseDurations::DEFAULT,
    )
}

fn failed(id: TaskOperationId, kind: OperationKind) -> OperationAcknowledgement {
    OperationAcknowledgement {
        operation_id: id,
        kind,
        result: crate::input::OperationReplyResult::Worker(
            novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome: OperationOutcome::OwnerFailed,
                detail: None,
                acknowledgement: None,
            },
        ),
    }
}

fn assert_owner_failure(error: TaskExecutionError) {
    assert!(matches!(
        error,
        TaskExecutionError::OperationFailed {
            rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome: OperationOutcome::OwnerFailed,
                ..
            },
            ..
        }
    ));
}

#[test]
fn failed_admission_stands_down_without_claiming_remote_resources_released() {
    let mut owner = owner();
    let mut ids = OperationIds::new(crate::ids::IdSeed::new(1, 1));
    let intent = owner
        .admission_intent(MonotonicInstant::ORIGIN, &mut ids)
        .unwrap()
        .unwrap();
    assert_owner_failure(
        owner
            .on_admission_ack(
                &failed(intent.operation_id(), intent.kind()),
                MonotonicInstant::ORIGIN,
            )
            .unwrap_err(),
    );
    assert!(!owner.is_released());
    assert_eq!(owner.state(), ParticipationState::Absent);
    assert!(
        owner
            .admission_intent(MonotonicInstant::ORIGIN, &mut ids)
            .unwrap()
            .is_none()
    );
}

#[test]
fn failed_renewal_keeps_the_last_published_participation_state() {
    let mut owner = owner();
    let mut ids = OperationIds::new(crate::ids::IdSeed::new(1, 1));
    owner.state = ParticipationState::Active;
    let request = RenewParticipationLease::new(
        ids.issue(),
        owner.participation,
        LeaseSequence::INITIAL,
        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    );
    let id = request.envelope().operation_id();
    owner.renewal = Some(InFlightRenewal {
        request,
        sequence: LeaseSequence::INITIAL,
        first_sent_at: MonotonicInstant::ORIGIN,
        awaiting_outcome: true,
    });
    assert_owner_failure(
        owner
            .on_participation_ack(&failed(id, OperationKind::UpdateParticipation))
            .unwrap_err(),
    );
    assert_eq!(owner.state(), ParticipationState::Active);
    assert!(!owner.is_released());
    assert!(owner.renewal.is_none());
}

#[test]
fn failed_quiesce_and_release_do_not_publish_participation_convergence() {
    let mut owner = owner();
    let mut ids = OperationIds::new(crate::ids::IdSeed::new(1, 1));
    owner.state = ParticipationState::Active;
    let quiesce = owner.quiesce_intent(&mut ids).unwrap();
    assert_owner_failure(
        owner
            .take_quiesce_ack(&failed(quiesce.operation_id(), quiesce.kind()))
            .unwrap_err(),
    );
    assert_eq!(owner.state(), ParticipationState::Active);
    assert!(!owner.is_released());
    assert!(!owner.quiesce_acknowledged());

    owner.state = ParticipationState::Quiescing;
    let request = ReleaseParticipation::new(ids.issue(), owner.participation);
    owner.release = Some(request);
    owner.release_in_flight = true;
    assert_owner_failure(
        owner
            .on_release_ack(
                &failed(
                    request.envelope().operation_id(),
                    OperationKind::ReleaseParticipation,
                ),
                MonotonicInstant::ORIGIN,
            )
            .unwrap_err(),
    );
    assert_eq!(owner.state(), ParticipationState::Quiescing);
    assert!(!owner.is_released());
}

#[test]
fn a_failed_owner_does_not_revoke_an_existing_release_receipt() {
    let mut owner = owner();
    let mut ids = OperationIds::new(crate::ids::IdSeed::new(1, 1));
    owner.state = ParticipationState::TerminalRetained;
    owner.released = true;
    let request = ReleaseParticipation::new(ids.issue(), owner.participation);
    owner.release = Some(request);
    owner.release_in_flight = true;
    assert_owner_failure(
        owner
            .on_release_ack(
                &failed(
                    request.envelope().operation_id(),
                    OperationKind::ReleaseParticipation,
                ),
                MonotonicInstant::ORIGIN,
            )
            .unwrap_err(),
    );
    assert!(owner.is_released());
    assert_eq!(owner.state(), ParticipationState::TerminalRetained);
}
