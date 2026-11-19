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

use super::*;
use novarocks_coordination_contract::{
    AttemptFailureClass, IdSeed, LogicalExecutionActorId, OperationIds,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    LeaseValidFor, ParticipationAdmissionTicketReceipt,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId,
};

fn now(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn operation(seed: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(seed, 100)).issue()
}
fn execution(attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(61, 62), AttemptId::new(attempt).unwrap()).unwrap()
}
fn identity() -> QualificationIdentity {
    QualificationIdentity {
        actor: LogicalExecutionActorId::from_raw(17),
        failed: execution(1),
        replacement: execution(2),
        generation: 9,
    }
}
fn participation(seed: u128, attempt: u64) -> ParticipationRef {
    ParticipationRef::new(
        execution(attempt),
        FrontendProcessId::try_from_bytes(operation(999).to_bytes()).unwrap(),
        BackendProcessId::try_from_bytes(operation(seed).to_bytes()).unwrap(),
    )
}
fn admission(seed: u128, valid_seconds: u64) -> QualifiedAdmission {
    let participation = participation(seed, 2);
    let valid_for = LeaseValidFor::new(Duration::from_secs(valid_seconds)).unwrap();
    QualifiedAdmission {
        request: AcquireParticipationAdmissionTicket::new(
            operation(seed + 100),
            participation,
            valid_for,
            NativeCompatibilityId::new([3; 32]),
            AdmissionEpochCapability::try_from_bytes([4; 16]).unwrap(),
        ),
        receipt: ParticipationAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes(operation(seed + 200).to_bytes()).unwrap(),
            participation,
            valid_for,
        ),
    }
}
fn request(participations: Vec<ParticipationRef>) -> QualificationRequest {
    QualificationRequest {
        identity: identity(),
        operation_id: operation(1),
        failed_participations: vec![participation(1, 1)].into(),
        replacement_participations: participations.into(),
        issued_at: now(0),
        expires_at: now(100),
    }
}
fn facts(admissions: Vec<QualifiedAdmission>) -> QualifiedReplacementFacts {
    QualifiedReplacementFacts {
        identity: identity(),
        operation_id: operation(1),
        topology_revision: 7,
        admissions: admissions.into_boxed_slice(),
    }
}
fn prerequisites() -> ReplacementPrerequisites {
    ReplacementPrerequisites {
        reachable_participations_closed: true,
        attempt_isolation_proven: true,
        stale_usage_accounted: true,
        new_capacity_admitted: true,
    }
}
fn pending(admissions: Vec<QualifiedAdmission>) -> QualificationState {
    let mut state = QualificationState::new(request(
        admissions
            .iter()
            .map(|a| a.request.participation())
            .collect(),
    ))
    .unwrap();
    state
        .record_dispatched(identity(), operation(1), now(20))
        .unwrap();
    state
        .settle(
            identity(),
            operation(1),
            QualificationSettlement::Qualified(facts(admissions)),
            now(21),
        )
        .unwrap();
    state
}
fn validate(
    state: &mut QualificationState,
    at: MonotonicInstant,
) -> Result<(), ReplacementQualificationFailure> {
    loop {
        match state.validate_next(at)? {
            QualificationProgress::Pending => {}
            QualificationProgress::Qualified => return Ok(()),
            QualificationProgress::Failed => {
                panic!("qualified facts cannot become a failure without a failure result")
            }
        }
    }
}

#[test]
fn qualification_outcome_cannot_claim_backend_loss_or_accepted_cancellation() {
    let source = ReplacementQualificationFailure::OutcomeUnknown;
    let error = source.error();
    let input = error.execution_failure().unwrap();
    assert_eq!(input.fact(), source.failure_fact());
    assert_eq!(input.detail(), &source.safe_detail());
    assert_eq!(input.fact().class(), FailureClass::Dependency);
    assert_eq!(input.fact().reason().code(), 97);
    assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
    let source = ReplacementQualificationFailure::EffectOwnerClosed;
    assert_eq!(
        source
            .error()
            .execution_failure()
            .unwrap()
            .fact()
            .evidence(),
        novarocks_execution_contract::failure::FailureEvidence::StopObservation
    );
    assert_eq!(
        source.error().execution_failure().unwrap().attempt_class(),
        AttemptFailureClass::ExecutionFailure
    );
}

#[test]
fn qualification_failures_keep_original_reason_codes_and_safe_details() {
    for (failure, class, code, detail) in [
        (
            ReplacementQualificationFailure::Rejected,
            FailureClass::Control,
            94,
            "attempt replacement qualification was refused",
        ),
        (
            ReplacementQualificationFailure::InvalidReservation,
            FailureClass::Invariant,
            95,
            "attempt replacement qualification returned an invalid reservation",
        ),
        (
            ReplacementQualificationFailure::Expired,
            FailureClass::Control,
            96,
            "attempt replacement qualification expired",
        ),
        (
            ReplacementQualificationFailure::OutcomeUnknown,
            FailureClass::Dependency,
            97,
            "attempt replacement qualification outcome is unknown",
        ),
        (
            ReplacementQualificationFailure::MismatchedReceipt,
            FailureClass::Invariant,
            100,
            "attempt replacement receipt does not match its exact operation and identity",
        ),
    ] {
        assert_eq!(failure.failure_fact().class(), class);
        assert_eq!(failure.failure_fact().reason().code(), code);
        assert_eq!(failure.safe_detail().as_str(), detail);
    }
    let fact = FailureFact::new(
        FailureClass::Resource,
        FailureReason::new(domain::CONNECTOR, NonZeroU16::new(23).unwrap()),
    );
    assert_eq!(
        ReplacementQualificationFailure::DomainRefused(fact).failure_fact(),
        fact
    );
}

#[test]
fn ticket_expiry_uses_actual_dispatch_without_renewing_the_request_horizon() {
    let mut state = pending(vec![admission(2, 10)]);
    validate(&mut state, now(22)).unwrap();
    assert_eq!(state.dispatched_at(), Some(now(20)));
    assert_eq!(state.deadline(), now(30));
    assert_eq!(
        state
            .begin_activation(now(30), prerequisites())
            .unwrap_err(),
        ReplacementQualificationFailure::Expired
    );

    let mut request = request(vec![participation(2, 2)]);
    request.expires_at = now(25);
    let mut state = QualificationState::new(request).unwrap();
    state
        .record_dispatched(identity(), operation(1), now(20))
        .unwrap();
    state
        .settle(
            identity(),
            operation(1),
            QualificationSettlement::Qualified(facts(vec![admission(2, 40)])),
            now(21),
        )
        .unwrap();
    validate(&mut state, now(22)).unwrap();
    assert_eq!(state.deadline(), now(25));
}

#[test]
fn missing_or_duplicate_dispatch_and_foreign_receipt_do_not_gain_qualification() {
    let mut state = QualificationState::new(request(vec![])).unwrap();
    assert_eq!(
        state.settle(
            identity(),
            operation(1),
            QualificationSettlement::OutcomeUnknown,
            now(1)
        ),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
    state
        .record_dispatched(identity(), operation(1), now(2))
        .unwrap();
    assert_eq!(
        state.record_dispatched(identity(), operation(1), now(3)),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
    assert_eq!(
        state.settle(
            identity(),
            operation(2),
            QualificationSettlement::OutcomeUnknown,
            now(4)
        ),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
    let foreign = QualificationIdentity {
        actor: LogicalExecutionActorId::from_raw(18),
        ..identity()
    };
    assert_eq!(
        state.settle(
            foreign,
            operation(1),
            QualificationSettlement::OutcomeUnknown,
            now(4)
        ),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
    assert_eq!(state.phase(), QualificationPhase::AwaitSettlement);
}

#[test]
fn replacement_effect_receipt_is_pending_bound_and_first_wins() {
    let mut state = pending(vec![admission(2, 30)]);
    assert_eq!(
        state.settle(
            identity(),
            operation(1),
            QualificationSettlement::OutcomeUnknown,
            now(22)
        ),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
    validate(&mut state, now(22)).unwrap();
    state.begin_activation(now(23), prerequisites()).unwrap();
    state
        .activation_completed(identity(), operation(1), now(24))
        .unwrap();
    assert_eq!(
        state.activation_completed(identity(), operation(1), now(24)),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
}

#[test]
fn fact_validation_is_resumable_with_one_participant_per_call() {
    let mut state = pending(vec![admission(2, 30), admission(3, 40)]);
    for (expected_cursor, admission_cursor) in [(1, 0), (2, 0), (2, 1), (2, 2)] {
        assert_eq!(
            state.validate_next(now(22)).unwrap(),
            QualificationProgress::Pending
        );
        assert_eq!(
            (state.expected_cursor, state.admission_cursor),
            (expected_cursor, admission_cursor)
        );
    }
    assert_eq!(
        state.validate_next(now(22)).unwrap(),
        QualificationProgress::Qualified
    );
    assert_eq!(state.deadline(), now(50));
}

#[test]
fn exact_admission_set_and_nonzero_topology_are_required() {
    let mut state = pending(vec![admission(2, 30), admission(2, 30)]);
    assert_eq!(
        validate(&mut state, now(22)),
        Err(ReplacementQualificationFailure::InvalidReservation)
    );
    let mut state = QualificationState::new(request(vec![participation(2, 2)])).unwrap();
    state
        .record_dispatched(identity(), operation(1), now(20))
        .unwrap();
    let mut invalid = facts(vec![admission(2, 30)]);
    invalid.topology_revision = 0;
    assert_eq!(
        state.settle(
            identity(),
            operation(1),
            QualificationSettlement::Qualified(invalid),
            now(21)
        ),
        Err(ReplacementQualificationFailure::InvalidReservation)
    );

    let mut state = QualificationState::new(request(vec![participation(2, 2)])).unwrap();
    state
        .record_dispatched(identity(), operation(1), now(20))
        .unwrap();
    state
        .settle(
            identity(),
            operation(1),
            QualificationSettlement::Qualified(facts(vec![admission(3, 30)])),
            now(21),
        )
        .unwrap();
    assert_eq!(
        validate(&mut state, now(22)),
        Err(ReplacementQualificationFailure::InvalidReservation)
    );
}

#[test]
fn admission_receipt_cannot_change_original_participation_or_validity() {
    let mut invalid = admission(2, 30);
    invalid.receipt = admission(3, 30).receipt;
    assert_eq!(
        validate_admission(execution(2), &invalid),
        Err(ReplacementQualificationFailure::InvalidReservation)
    );
    let mut invalid = admission(2, 30);
    invalid.receipt = admission(2, 31).receipt;
    assert_eq!(
        validate_admission(execution(2), &invalid),
        Err(ReplacementQualificationFailure::InvalidReservation)
    );
    assert_eq!(
        validate_admission(execution(1), &admission(2, 30)),
        Err(ReplacementQualificationFailure::InvalidReservation)
    );
}

#[test]
fn stale_usage_and_running_obligation_prerequisites_cannot_be_skipped() {
    let mut state = pending(vec![admission(2, 30)]);
    validate(&mut state, now(22)).unwrap();
    assert_eq!(
        state
            .begin_activation(
                now(23),
                ReplacementPrerequisites {
                    stale_usage_accounted: false,
                    ..prerequisites()
                }
            )
            .unwrap_err(),
        ReplacementQualificationFailure::InvalidReservation
    );
    assert_eq!(state.phase(), QualificationPhase::Qualified);
    state.begin_activation(now(23), prerequisites()).unwrap();
    assert_eq!(
        state.activation_completed(identity(), operation(2), now(24)),
        Err(ReplacementQualificationFailure::MismatchedReceipt)
    );
    state
        .activation_completed(identity(), operation(1), now(24))
        .unwrap();
}

#[test]
fn late_activation_cannot_outlive_the_original_ticket_deadline() {
    let mut state = pending(vec![admission(2, 10)]);
    validate(&mut state, now(22)).unwrap();
    state.begin_activation(now(29), prerequisites()).unwrap();
    assert_eq!(
        state.activation_completed(identity(), operation(1), now(30)),
        Err(ReplacementQualificationFailure::Expired)
    );
}

#[test]
fn typed_owner_refusal_keeps_the_original_domain_input() {
    let mut state = QualificationState::new(request(vec![])).unwrap();
    state
        .record_dispatched(identity(), operation(1), now(20))
        .unwrap();
    let original = ReplacementQualificationFailure::DomainRefused(FailureFact::new(
        FailureClass::Data,
        FailureReason::new(domain::CONNECTOR, NonZeroU16::new(12).unwrap()),
    ))
    .error();
    assert_eq!(
        state
            .settle(
                identity(),
                operation(1),
                QualificationSettlement::Rejected(original.clone()),
                now(21)
            )
            .unwrap(),
        QualificationProgress::Failed
    );
    assert_eq!(state.failure(), Some(&original));
    assert_eq!(
        state
            .begin_activation(now(22), prerequisites())
            .unwrap_err(),
        ReplacementQualificationFailure::InvalidReservation
    );
}

#[test]
fn preparation_window_is_opened_once_and_does_not_renew() {
    let mut window = PreparationWindow::default();
    assert_eq!(window.open_once(now(10), Duration::from_secs(10)), now(20));
    assert_eq!(window.open_once(now(19), Duration::from_secs(100)), now(20));
    assert!(!window.expired(now(19)));
    assert!(window.expired(now(20)));
}

#[test]
fn never_dispatched_refusal_keeps_its_original_input_without_fabricating_handoff() {
    let mut state = QualificationState::new(request(vec![])).unwrap();
    let original = ReplacementQualificationFailure::Rejected.error();
    assert_eq!(
        state
            .settle(
                identity(),
                operation(1),
                QualificationSettlement::Rejected(original.clone()),
                now(1)
            )
            .unwrap(),
        QualificationProgress::Failed
    );
    assert_eq!(state.dispatched_at(), None);
    assert_eq!(state.failure(), Some(&original));
}
