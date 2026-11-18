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
//! Pure sequences protect authorization/replay and conservative remote facts.
//! Existing actor, transport and destructor mechanics are covered by their owners.

use super::*;
use novarocks_coordination_contract::{
    IdSeed, OperationIds, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialDomainId,
    CredentialEpoch, CredentialUpdate, EstablishParticipation,
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::{sync::Arc, time::Duration};

fn time(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn uuid_bytes(tag: u8) -> [u8; 16] {
    let mut bytes = [tag; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn operation(tag: u64) -> TaskOperationId {
    OperationIds::new(IdSeed::new(u128::from(tag), 1234)).issue()
}
fn execution() -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(43, 1), AttemptId::new(1).unwrap()).unwrap()
}
fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(7, execution(), 1)
}
fn participation(tag: u8) -> ParticipationRef {
    ParticipationRef::new(
        execution(),
        FrontendProcessId::try_from_bytes(uuid_bytes(1)).unwrap(),
        BackendProcessId::try_from_bytes(uuid_bytes(tag)).unwrap(),
    )
}
fn ledger(
    participations: &[ParticipationRef],
    admission_limit: usize,
    establish_limit: usize,
) -> EstablishIssueLedger {
    EstablishIssueLedger::new(
        activation(),
        participations.iter().copied().collect(),
        NonZeroUsize::new(admission_limit).unwrap(),
        NonZeroUsize::new(establish_limit).unwrap(),
    )
    .unwrap()
}
fn request(participation: ParticipationRef, tag: u8) -> AcquireParticipationAdmissionTicket {
    AcquireParticipationAdmissionTicket::new(
        operation(u64::from(tag)),
        participation,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([tag; 32]),
        AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
    )
}
fn ticket(participation: ParticipationRef, tag: u8) -> ParticipationAdmissionTicketReceipt {
    ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
        participation,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
    )
}

#[derive(Debug)]
struct Metadata(ContentFingerprint);
impl CodecOwnedContent for Metadata {
    fn fingerprint(&self) -> ContentFingerprint {
        self.0
    }
    fn encoded_len(&self) -> usize {
        1
    }
}
struct Secret;
impl ConfidentialContent for Secret {
    fn encoded_len(&self) -> usize {
        1
    }
    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        other.encoded_len() == 1
    }
}
fn establish(
    participation: ParticipationRef,
    ticket: ParticipationAdmissionTicketReceipt,
    tag: u8,
) -> AttemptAuthorization {
    // Use the existing request author to freeze semantic identity; no payload or
    // credential object enters the ledger, and no new semantic hash is invented.
    let metadata = |offset| {
        Arc::new(Metadata(ContentFingerprint::from_bytes([tag + offset; 16])))
            as Arc<dyn CodecOwnedContent>
    };
    let original = EstablishParticipation::new(
        operation(u64::from(tag) + 100),
        participation,
        ticket.ticket_id(),
        metadata(0),
        metadata(1),
        metadata(2),
        CredentialUpdate::new(
            CredentialDomainId::new(u64::from(tag)),
            CredentialEpoch::new(1).unwrap(),
            Arc::new(Secret),
        ),
        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    );
    AttemptAuthorization::Establish {
        participation,
        operation_id: original.envelope().operation_id(),
        ticket_id: original.admission_ticket_id(),
        semantics: original.semantic_identity(),
        native_compatibility_id: NativeCompatibilityId::new([tag; 32]),
    }
}
fn begin(
    ledger: &mut EstablishIssueLedger,
    request: AcquireParticipationAdmissionTicket,
    now: MonotonicInstant,
) -> AttemptAuthorizationRef {
    match ledger
        .authorize(now, AttemptAuthorization::BeginAdmission(request))
        .unwrap()
    {
        AttemptAuthorizationDecision::AdmissionStarted(reference) => reference,
        _ => panic!("admission start must return its exact reference"),
    }
}
fn admission_settlement(
    ledger: &mut EstablishIssueLedger,
    reference: AttemptAuthorizationRef,
    request: AcquireParticipationAdmissionTicket,
    outcome: Option<OperationOutcome>,
    receipt: Option<ParticipationAdmissionTicketReceipt>,
    prior_unknown: bool,
    now: MonotonicInstant,
) -> Result<AttemptAuthorizationDecision, EstablishIssueError> {
    ledger.authorize(
        now,
        AttemptAuthorization::SettleAdmission {
            issue: reference,
            participation: request.participation(),
            operation_id: request.envelope().operation_id(),
            outcome,
            receipt,
            prior_transport_unknown: prior_unknown,
        },
    )
}
fn grant(
    ledger: &mut EstablishIssueLedger,
    p: ParticipationRef,
    tag: u8,
) -> ParticipationAdmissionTicketReceipt {
    let req = request(p, tag);
    let receipt = ticket(p, tag);
    let reference = begin(ledger, req, time(1));
    assert!(matches!(
        admission_settlement(
            ledger,
            reference,
            req,
            Some(OperationOutcome::Accepted),
            Some(receipt),
            false,
            time(2)
        ),
        Ok(AttemptAuthorizationDecision::AdmissionGranted)
    ));
    receipt
}
fn issue(
    ledger: &mut EstablishIssueLedger,
    p: ParticipationRef,
    receipt: ParticipationAdmissionTicketReceipt,
    tag: u8,
) -> (AttemptAuthorizationRef, TaskOperationId) {
    let req = establish(p, receipt, tag);
    let AttemptAuthorization::Establish { operation_id, .. } = req else {
        unreachable!()
    };
    let AttemptAuthorizationDecision::EstablishAuthorized(reference) =
        ledger.authorize(time(2), req).unwrap()
    else {
        panic!("Establish must return its issued identity");
    };
    (reference, operation_id)
}
fn held_issue(
    establish_limit: usize,
) -> (
    EstablishIssueLedger,
    ParticipationRef,
    AttemptAuthorizationRef,
    TaskOperationId,
) {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, establish_limit);
    let receipt = grant(&mut ledger, p, 1);
    let (reference, op) = issue(&mut ledger, p, receipt, 1);
    (ledger, p, reference, op)
}
fn owned_issue(
    establish_limit: usize,
) -> (
    EstablishIssueLedger,
    ParticipationRef,
    AttemptAuthorizationRef,
    TaskOperationId,
) {
    let (mut ledger, p, reference, op) = held_issue(establish_limit);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    (ledger, p, reference, op)
}
fn reauthorize(
    ledger: &mut EstablishIssueLedger,
    p: ParticipationRef,
    op: TaskOperationId,
    now: MonotonicInstant,
) -> Result<AttemptAuthorizationRef, EstablishIssueError> {
    match ledger.authorize(
        now,
        AttemptAuthorization::ReauthorizeEstablish {
            participation: p,
            operation_id: op,
        },
    )? {
        AttemptAuthorizationDecision::EstablishAuthorized(reference) => Ok(reference),
        _ => panic!("Establish replay must return its issued identity"),
    }
}
fn refusal(p: ParticipationRef, op: TaskOperationId, kind: OperationKind) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("exact process identity refused"),
        ExecutionFailureSite::Operation {
            participation: p,
            operation_id: op,
            kind,
        },
    )
}

#[test]
fn pre_worker_admission_refusal_closes_replay_without_a_grant() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(1));
    assert!(matches!(
        admission_settlement(&mut ledger, reference, req, None, None, false, time(2)),
        Err(EstablishIssueError::AdmissionRejected)
    ));
    assert_eq!(
        ledger.authorize(time(3), AttemptAuthorization::BeginAdmission(req)),
        Err(EstablishIssueError::AdmissionReplayClosed)
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::NoGrant)
    );
    assert_eq!(
        ledger.ensure_success_ready(),
        Err(EstablishIssueError::AdmissionRejected)
    );
}

#[test]
fn owner_failed_admission_closes_replay_but_keeps_abort_responsibility() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(1));
    assert!(matches!(
        admission_settlement(
            &mut ledger,
            reference,
            req,
            Some(OperationOutcome::OwnerFailed),
            None,
            false,
            time(2)
        ),
        Err(EstablishIssueError::AdmissionRejected)
    ));
    assert_eq!(
        ledger.authorize(time(3), AttemptAuthorization::BeginAdmission(req)),
        Err(EstablishIssueError::AdmissionReplayClosed)
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn admission_refusal_after_unknown_keeps_abort_responsibility() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(1));
    assert!(matches!(
        admission_settlement(&mut ledger, reference, req, None, None, true, time(2)),
        Err(EstablishIssueError::AdmissionRejected)
    ));
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn original_worker_receipt_settles_unknown_without_reauthorization() {
    let (mut ledger, p, reference, op) = owned_issue(2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    ledger.close_authority();
    assert_eq!(
        ledger.settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted)
        ),
        Ok(true)
    );
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
    assert_eq!(
        ledger.snapshot(p).unwrap().state,
        EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
    );
}

#[test]
fn admission_rejection_without_establish_is_required_until_exact_result_withdrawal() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(1));
    assert!(matches!(
        admission_settlement(
            &mut ledger,
            reference,
            req,
            Some(OperationOutcome::ParticipationConflict),
            None,
            false,
            time(2)
        ),
        Err(EstablishIssueError::AdmissionRejected)
    ));
    assert_eq!(
        ledger.ensure_success_ready(),
        Err(EstablishIssueError::AdmissionRejected)
    );
    assert_eq!(
        ledger.record_result_withdrawal(participation(3)),
        Err(EstablishIssueError::UnknownParticipation)
    );
    assert_eq!(
        ledger.ensure_success_ready(),
        Err(EstablishIssueError::AdmissionRejected)
    );
    ledger.record_result_withdrawal(p).unwrap();
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
    assert!(ledger.required_participations().contains(&p));
}

#[test]
fn pending_participation_cannot_hide_a_later_required_admission_or_establish_refusal() {
    let pending = participation(2);
    let failed = participation(3);
    for establish_rejection in [false, true] {
        let mut ledger = ledger(&[pending, failed], 3, 2);
        let expected = if establish_rejection {
            let receipt = grant(&mut ledger, failed, 1);
            let (reference, op) = issue(&mut ledger, failed, receipt, 1);
            ledger
                .settle_establish(
                    reference,
                    failed,
                    op,
                    AttemptIssueSettlement::TransportOwned,
                )
                .unwrap();
            ledger
                .settle_establish(
                    reference,
                    failed,
                    op,
                    AttemptIssueSettlement::Worker(OperationOutcome::IdentityMismatch),
                )
                .unwrap();
            EstablishIssueError::EstablishRejected
        } else {
            let req = request(failed, 1);
            let reference = begin(&mut ledger, req, time(1));
            assert!(
                admission_settlement(&mut ledger, reference, req, None, None, false, time(2))
                    .is_err()
            );
            EstablishIssueError::AdmissionRejected
        };
        assert_eq!(ledger.ensure_success_ready(), Err(expected));
        ledger.record_result_withdrawal(failed).unwrap();
        assert_eq!(
            ledger.ensure_success_ready(),
            Err(EstablishIssueError::EstablishNotSettled)
        );
    }
}

#[test]
fn withdrawn_result_participation_still_validates_its_original_worker_receipts() {
    let (mut ledger, p, reference, op) = owned_issue(2);
    ledger.record_result_withdrawal(p).unwrap();
    ledger
        .settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    assert_eq!(
        ledger.settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::IdentityMismatch)
        ),
        Err(EstablishIssueError::ConflictingWorkerSettlement)
    );
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn pre_worker_establish_refusal_retains_ticket_abort_responsibility() {
    let (mut ledger, p, reference, op) = owned_issue(2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::PreWorkerRejected)
        .unwrap();
    assert_eq!(
        ledger.ensure_success_ready(),
        Err(EstablishIssueError::EstablishRejected)
    );
    assert!(ledger.has_establish_rejection());
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn identity_is_typed_and_rejects_a_foreign_activation() {
    let p = participation(2);
    let foreign = AttemptActivationIdentity::from_parts(
        7,
        QueryExecutionId::new(QueryId::new(43, 2), AttemptId::new(1).unwrap()).unwrap(),
        1,
    );
    assert!(matches!(
        EstablishIssueLedger::new(
            foreign,
            BTreeSet::from([p]),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap()
        ),
        Err(EstablishIssueError::ActivationExecutionMismatch)
    ));
    let (ledger, p, _, op) = held_issue(2);
    let identity = ledger.snapshot(p).unwrap().identity.unwrap();
    assert_eq!(identity.activation, activation());
    assert_eq!(identity.participation, p);
    assert_eq!(identity.operation_id, op);
    assert_eq!(identity.ticket_id, ticket(p, 1).ticket_id());
    let AttemptAuthorization::Establish {
        semantics,
        native_compatibility_id,
        ..
    } = establish(p, ticket(p, 1), 1)
    else {
        unreachable!()
    };
    assert_eq!(identity.semantics, semantics);
    assert_eq!(identity.native_compatibility_id, native_compatibility_id);
}

#[test]
fn ticket_receipt_and_expiry_are_retained_exactly() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let receipt = grant(&mut ledger, p, 1);
    let snapshot = ledger.snapshot(p).unwrap();
    assert_eq!(snapshot.ticket, receipt);
    assert_eq!(snapshot.conservative_expiry, time(11));
    assert_eq!(snapshot.state, EstablishIssueState::TicketHeld);
    assert_eq!(
        ledger.stand_down_fact(participation(3)),
        Err(EstablishIssueError::UnknownParticipation)
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn admission_replay_retains_the_first_issue_time_and_operation() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let first = begin(&mut ledger, req, time(1));
    let again = begin(&mut ledger, req, time(8));
    assert_eq!(first, again);
    let IssuedAuthorization::Admission(original) = ledger.authorizations[&first] else {
        unreachable!()
    };
    assert_eq!(original.first_sent_at(), time(1));
    let changed = AcquireParticipationAdmissionTicket::new(
        req.envelope().operation_id(),
        p,
        req.valid_for(),
        NativeCompatibilityId::new([2; 32]),
        req.admission_epoch_capability(),
    );
    assert_eq!(
        ledger.authorize(time(8), AttemptAuthorization::BeginAdmission(changed)),
        Err(EstablishIssueError::ConflictingAdmissionIssue)
    );
    admission_settlement(
        &mut ledger,
        first,
        req,
        Some(OperationOutcome::Accepted),
        Some(ticket(p, 1)),
        false,
        time(8),
    )
    .unwrap();
    assert_eq!(ledger.snapshot(p).unwrap().conservative_expiry, time(11));
}

#[test]
fn expired_ticket_can_be_replaced_by_one_bounded_new_admission_operation() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 2, 5);
    let old = request(p, 1);
    let old_ref = begin(&mut ledger, old, time(1));
    admission_settlement(
        &mut ledger,
        old_ref,
        old,
        Some(OperationOutcome::Accepted),
        Some(ticket(p, 1)),
        false,
        time(2),
    )
    .unwrap();
    let second = request(p, 2);
    let second_ref = begin(&mut ledger, second, time(11));
    assert!(matches!(
        admission_settlement(
            &mut ledger,
            old_ref,
            old,
            Some(OperationOutcome::Idempotent),
            Some(ticket(p, 1)),
            false,
            time(12)
        ),
        Ok(AttemptAuthorizationDecision::AdmissionRetryable)
    ));
    assert!(ledger.snapshot(p).is_none());
    admission_settlement(
        &mut ledger,
        second_ref,
        second,
        Some(OperationOutcome::OperationTimedOut),
        None,
        false,
        time(12),
    )
    .unwrap();
    assert_eq!(
        ledger.authorize(
            time(13),
            AttemptAuthorization::BeginAdmission(request(p, 3))
        ),
        Err(EstablishIssueError::AdmissionIssueBudgetExhausted)
    );
    assert_eq!(
        ledger.authorize(time(13), AttemptAuthorization::BeginAdmission(old)),
        Err(EstablishIssueError::AdmissionReplayClosed)
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn admission_settlement_must_name_the_exact_operation_and_ticket_shape() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(1));
    let other = request(p, 2);
    assert!(matches!(
        admission_settlement(
            &mut ledger,
            reference,
            other,
            Some(OperationOutcome::Accepted),
            Some(ticket(p, 1)),
            false,
            time(2)
        ),
        Err(EstablishIssueError::ConflictingAdmissionIssue)
    ));
    assert!(matches!(
        admission_settlement(
            &mut ledger,
            reference,
            req,
            Some(OperationOutcome::ResourceExhausted),
            Some(ticket(p, 1)),
            false,
            time(2)
        ),
        Err(EstablishIssueError::InvalidAdmissionSettlement)
    ));
    let wrong = ParticipationAdmissionTicketReceipt::new(
        ticket(p, 1).ticket_id(),
        p,
        LeaseValidFor::new(Duration::from_secs(9)).unwrap(),
    );
    assert!(matches!(
        admission_settlement(
            &mut ledger,
            reference,
            req,
            Some(OperationOutcome::Accepted),
            Some(wrong),
            false,
            time(2)
        ),
        Err(EstablishIssueError::InvalidAdmissionSettlement)
    ));
    assert!(ledger.snapshot(p).is_none());
    assert_eq!(
        ledger.ensure_success_ready(),
        Err(EstablishIssueError::EstablishNotSettled)
    );
}

#[test]
fn definitely_unsent_allows_exact_resign_with_a_distinct_reference() {
    let (mut ledger, p, first, op) = held_issue(2);
    let identity = ledger.snapshot(p).unwrap().identity;
    ledger
        .settle_establish(first, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    let second = reauthorize(&mut ledger, p, op, time(3)).unwrap();
    assert_ne!(first, second);
    assert_eq!(ledger.snapshot(p).unwrap().identity, identity);
    assert_eq!(ledger.snapshot(p).unwrap().latest_authorization.0, 2);
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn closing_authority_does_not_invent_an_unsent_settlement() {
    let (mut ledger, p, reference, op) = held_issue(2);
    ledger.close_authority();
    assert_eq!(
        ledger.snapshot(p).unwrap().state,
        EstablishIssueState::IssueAuthorized
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    assert_eq!(
        ledger.snapshot(p).unwrap().state,
        EstablishIssueState::DefinitelyUnsent
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn explicit_transport_backpressure_is_definitely_unsent() {
    let (mut ledger, p, reference, op) = owned_issue(2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    assert_eq!(
        ledger.snapshot(p).unwrap().state,
        EstablishIssueState::DefinitelyUnsent
    );
    assert!(reauthorize(&mut ledger, p, op, time(3)).is_ok());
}

#[test]
fn transport_unknown_exact_replay_survives_expiry() {
    let (mut ledger, p, first, op) = owned_issue(2);
    ledger
        .settle_establish(first, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    let second = reauthorize(&mut ledger, p, op, time(50)).unwrap();
    assert_ne!(first, second);
    assert_eq!(
        ledger.snapshot(p).unwrap().identity.unwrap().operation_id,
        op
    );
}

#[test]
fn exact_replay_cannot_exceed_the_participation_authorization_budget() {
    let (mut ledger, p, first, op) = owned_issue(2);
    ledger
        .settle_establish(first, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    let second = reauthorize(&mut ledger, p, op, time(3)).unwrap();
    ledger
        .settle_establish(second, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    assert_eq!(
        reauthorize(&mut ledger, p, op, time(4)),
        Err(EstablishIssueError::AuthorizationBudgetExhausted)
    );
    let error = EstablishIssueError::AuthorizationBudgetExhausted.failure();
    assert_eq!(error.input().fact().class(), FailureClass::Resource);
}

#[test]
fn worker_settlement_is_terminal_and_idempotent() {
    let (mut ledger, p, reference, op) = owned_issue(2);
    assert_eq!(
        ledger.settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted)
        ),
        Ok(true)
    );
    assert_eq!(
        ledger.settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Idempotent)
        ),
        Ok(false)
    );
    assert_eq!(
        ledger.settle_establish(reference, p, op, AttemptIssueSettlement::TransportUnknown),
        Ok(false)
    );
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
    assert_eq!(
        ledger.settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::ParticipationConflict)
        ),
        Err(EstablishIssueError::ConflictingWorkerSettlement)
    );
}

#[test]
fn late_worker_settlement_wins_over_a_reissue_but_old_carrier_events_do_not() {
    let (mut ledger, p, first, op) = owned_issue(3);
    ledger
        .settle_establish(first, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    let second = reauthorize(&mut ledger, p, op, time(3)).unwrap();
    ledger
        .settle_establish(second, p, op, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    for old in [
        AttemptIssueSettlement::TransportOwned,
        AttemptIssueSettlement::TransportUnknown,
        AttemptIssueSettlement::DefinitelyUnsent,
        AttemptIssueSettlement::PreWorkerRejected,
    ] {
        assert_eq!(ledger.settle_establish(first, p, op, old), Ok(false));
        assert_eq!(
            ledger.snapshot(p).unwrap().state,
            EstablishIssueState::TransportOwned
        );
    }
    assert_eq!(
        ledger.settle_establish(
            first,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted)
        ),
        Ok(true)
    );
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
    assert_eq!(
        ledger.settle_establish(
            second,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Idempotent)
        ),
        Ok(false)
    );
}

#[test]
fn withdrawn_replay_preserves_prior_unknown_after_ticket_expiry_and_revocation() {
    let (mut ledger, p, first, op) = owned_issue(3);
    ledger
        .settle_establish(first, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    let second = reauthorize(&mut ledger, p, op, time(40)).unwrap();
    ledger
        .settle_establish(second, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    ledger.close_authority();
    let snapshot = ledger.snapshot(p).unwrap();
    assert_eq!(snapshot.state, EstablishIssueState::TransportUnknown);
    assert_eq!(
        snapshot.latest_authorization.1,
        EstablishIssueState::DefinitelyUnsent
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
    ledger
        .settle_establish(
            first,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
}

#[test]
fn definitely_unsent_replay_still_obeys_ticket_expiry() {
    let (mut ledger, p, reference, op) = held_issue(2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    let error = reauthorize(&mut ledger, p, op, time(11)).unwrap_err();
    assert_eq!(error, EstablishIssueError::TicketExpired);
    assert_eq!(
        error.failure().input().fact().class(),
        FailureClass::Control
    );
}

#[test]
fn conservative_ticket_expiry_uses_the_first_admission_send_time() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(3));
    admission_settlement(
        &mut ledger,
        reference,
        req,
        Some(OperationOutcome::Accepted),
        Some(ticket(p, 1)),
        false,
        time(8),
    )
    .unwrap();
    assert_eq!(ledger.snapshot(p).unwrap().conservative_expiry, time(13));
}

#[test]
fn required_participation_never_issued_is_not_ready() {
    let p = participation(2);
    let ledger = ledger(&[p], 3, 2);
    assert_eq!(
        ledger.ensure_success_ready(),
        Err(EstablishIssueError::EstablishNotSettled)
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::NoGrant)
    );
}

#[test]
fn closed_authority_rejects_new_authorization_and_accepts_original_settlements() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let req = request(p, 1);
    let reference = begin(&mut ledger, req, time(1));
    ledger.close_authority();
    ledger.close_authority();
    assert!(!ledger.authority_open());
    assert_eq!(
        ledger.authorize(time(2), AttemptAuthorization::BeginAdmission(req)),
        Err(EstablishIssueError::IssueAuthorityRevoked)
    );
    admission_settlement(
        &mut ledger,
        reference,
        req,
        Some(OperationOutcome::Accepted),
        Some(ticket(p, 1)),
        false,
        time(2),
    )
    .unwrap();
    assert_eq!(
        ledger.authorize(time(2), establish(p, ticket(p, 1), 1)),
        Err(EstablishIssueError::IssueAuthorityRevoked)
    );
    let (mut ledger, p, reference, op) = owned_issue(2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    ledger.close_authority();
    assert_eq!(
        reauthorize(&mut ledger, p, op, time(3)),
        Err(EstablishIssueError::IssueAuthorityRevoked)
    );
    ledger
        .settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
}

#[test]
fn admission_and_establish_references_share_one_exact_sequence() {
    let (mut ledger, p, establish_ref, op) = held_issue(2);
    let admission_ref = ledger.admissions[&p].authorization;
    assert_ne!(admission_ref, establish_ref);
    assert!(admission_ref.get() < establish_ref.get());
    assert_eq!(
        ledger.settle_establish(admission_ref, p, op, AttemptIssueSettlement::TransportOwned),
        Err(EstablishIssueError::IdentityMismatch)
    );
    let future = AttemptAuthorizationRef::new(NonZeroU64::new(establish_ref.get() + 1).unwrap());
    assert_eq!(
        ledger.settle_establish(future, p, op, AttemptIssueSettlement::TransportOwned),
        Err(EstablishIssueError::FutureAuthorization)
    );
}

#[test]
fn authorized_issue_immediately_requires_abort_before_transport_handoff() {
    let (ledger, p, _, _) = held_issue(2);
    assert_eq!(
        ledger.snapshot(p).unwrap().state,
        EstablishIssueState::IssueAuthorized
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn inherited_admission_keeps_the_qualification_dispatch_time() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    assert_eq!(
        ledger.import_admission(request(p, 1), ticket(p, 1), time(3), time(12)),
        Ok(AdmissionIssueDisposition::Granted)
    );
    assert_eq!(ledger.snapshot(p).unwrap().conservative_expiry, time(13));
    assert_eq!(
        ledger.authorize(time(13), establish(p, ticket(p, 1), 1)),
        Err(EstablishIssueError::TicketExpired)
    );
}

#[test]
fn expired_inherited_admission_does_not_start_a_new_lifetime() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    assert_eq!(
        ledger.import_admission(request(p, 1), ticket(p, 1), time(3), time(13)),
        Ok(AdmissionIssueDisposition::Retryable)
    );
    assert!(ledger.snapshot(p).is_none());
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
}

#[test]
fn admission_and_establish_have_independent_budgets() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 2, 1);
    let first_req = request(p, 1);
    let first = begin(&mut ledger, first_req, time(1));
    admission_settlement(
        &mut ledger,
        first,
        first_req,
        Some(OperationOutcome::OperationTimedOut),
        None,
        false,
        time(2),
    )
    .unwrap();
    let second_req = request(p, 2);
    let second = begin(&mut ledger, second_req, time(2));
    admission_settlement(
        &mut ledger,
        second,
        second_req,
        Some(OperationOutcome::Accepted),
        Some(ticket(p, 2)),
        false,
        time(2),
    )
    .unwrap();
    let (reference, op) = issue(&mut ledger, p, ticket(p, 2), 2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    assert_eq!(
        reauthorize(&mut ledger, p, op, time(4)),
        Err(EstablishIssueError::AuthorizationBudgetExhausted)
    );
    assert_eq!(ledger.authorization_limit().get(), 1);
}

#[test]
fn establish_replay_cannot_substitute_another_operation() {
    let (mut ledger, p, reference, op) = held_issue(2);
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::DefinitelyUnsent)
        .unwrap();
    assert_eq!(
        reauthorize(&mut ledger, p, operation(999), time(3)),
        Err(EstablishIssueError::IdentityMismatch)
    );
    assert_eq!(
        ledger.snapshot(p).unwrap().identity.unwrap().operation_id,
        op
    );
}

#[test]
fn create_refusal_requires_an_observed_parent_and_rechecks_late_parent_settlement() {
    let (mut ledger, p, reference, op) = held_issue(2);
    let create = refusal(p, operation(700), OperationKind::CreateTask);
    assert!(!ledger.correlates(&create));
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    assert!(ledger.correlates(&create));
    ledger
        .settle_establish(reference, p, op, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    assert!(ledger.correlates(&create));
    ledger
        .settle_establish(
            reference,
            p,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::ParticipationConflict),
        )
        .unwrap();
    assert!(!ledger.correlates(&create));
}

#[test]
fn update_refusal_matches_the_exact_establish_operation() {
    let (ledger, p, _, op) = held_issue(2);
    assert!(ledger.correlates(&refusal(p, op, OperationKind::UpdateParticipation)));
    assert!(!ledger.correlates(&refusal(
        p,
        operation(800),
        OperationKind::UpdateParticipation
    )));
    let unrelated = ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("unrelated operation"),
        ExecutionFailureSite::Operation {
            participation: p,
            operation_id: op,
            kind: OperationKind::AbortParticipation,
        },
    );
    assert!(!ledger.correlates(&unrelated));
}

#[test]
fn one_settlement_failure_does_not_prevent_the_next_exact_settlement() {
    let first = participation(2);
    let second = participation(3);
    let mut ledger = ledger(&[first, second], 3, 2);
    let first_ticket = grant(&mut ledger, first, 1);
    let second_ticket = grant(&mut ledger, second, 2);
    let (first_ref, first_op) = issue(&mut ledger, first, first_ticket, 1);
    let (second_ref, second_op) = issue(&mut ledger, second, second_ticket, 2);
    ledger
        .settle_establish(
            first_ref,
            first,
            first_op,
            AttemptIssueSettlement::TransportOwned,
        )
        .unwrap();
    ledger
        .settle_establish(
            second_ref,
            second,
            second_op,
            AttemptIssueSettlement::TransportOwned,
        )
        .unwrap();
    ledger
        .settle_establish(
            first_ref,
            first,
            first_op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    assert_eq!(
        ledger.settle_establish(
            first_ref,
            first,
            first_op,
            AttemptIssueSettlement::Worker(OperationOutcome::IdentityMismatch)
        ),
        Err(EstablishIssueError::ConflictingWorkerSettlement)
    );
    ledger.close_authority();
    assert_eq!(
        ledger.settle_establish(
            second_ref,
            second,
            second_op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted)
        ),
        Ok(true)
    );
    assert_eq!(ledger.ensure_success_ready(), Ok(()));
}

#[test]
fn authorized_transport_handoff_can_arrive_after_authority_closes() {
    let (mut ledger, p, reference, op) = held_issue(2);
    ledger.close_authority();
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
    assert_eq!(
        ledger.settle_establish(reference, p, op, AttemptIssueSettlement::TransportOwned),
        Ok(true)
    );
    assert_eq!(
        ledger.settle_establish(reference, p, op, AttemptIssueSettlement::TransportUnknown),
        Ok(true)
    );
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
    assert_eq!(
        reauthorize(&mut ledger, p, op, time(3)),
        Err(EstablishIssueError::IssueAuthorityRevoked)
    );
}

#[test]
fn create_refusal_can_correlate_with_applied_or_exact_identity_refused_parent() {
    for outcome in [
        OperationOutcome::Accepted,
        OperationOutcome::IdentityMismatch,
    ] {
        let (mut ledger, p, reference, op) = owned_issue(2);
        ledger
            .settle_establish(reference, p, op, AttemptIssueSettlement::Worker(outcome))
            .unwrap();
        assert!(ledger.correlates(&refusal(p, operation(701), OperationKind::CreateTask)));
    }
}

#[test]
fn admission_ticket_still_active_keeps_retired_capacity_responsibility() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    let first_req = request(p, 1);
    let first = begin(&mut ledger, first_req, time(1));
    admission_settlement(
        &mut ledger,
        first,
        first_req,
        Some(OperationOutcome::AdmissionTicketStillActive),
        None,
        false,
        time(2),
    )
    .unwrap();
    let second_req = request(p, 2);
    let second = begin(&mut ledger, second_req, time(3));
    assert!(matches!(
        admission_settlement(&mut ledger, second, second_req, None, None, false, time(4)),
        Err(EstablishIssueError::AdmissionRejected)
    ));
    assert_eq!(
        ledger.stand_down_fact(p),
        Ok(EstablishStandDownFact::AbortRequired)
    );
    assert_eq!(ledger.remote_admissions.get(&p), Some(&1));
}

#[test]
fn establish_authorization_requires_its_original_admission_ticket() {
    let p = participation(2);
    let mut ledger = ledger(&[p], 3, 2);
    grant(&mut ledger, p, 1);
    assert_eq!(
        ledger.authorize(time(2), establish(p, ticket(p, 2), 1)),
        Err(EstablishIssueError::TicketDoesNotAuthorizeRequest)
    );
    assert_eq!(
        ledger.snapshot(p).unwrap().state,
        EstablishIssueState::TicketHeld
    );
    assert!(ledger.snapshot(p).unwrap().identity.is_none());
}
