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

//! Process control actor: low-frequency directory, ticket and retention policy.
//! Task creation/preparation capacity never traverses this mailbox.
//! Pure Root request routing only. The admission stub records exact policy
//! calls; these tests prove no Work, I/O, source exit or Native publication.

use novarocks_control_runtime_core::identity::{
    Generation, InputId, ReservationId, ScopeId, ScopedWorkRef, WorkId,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionTicketId, ContentFingerprint, CredentialDomainId,
    CredentialEpoch, LeaseValidFor, OperationOutcome, ParticipationRef, ParticipationState,
    TaskOperationId,
};
use novarocks_types::{
    NativeCompatibilityId,
    identity::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId},
};
use novarocks_worker_protocol::{
    admission::AdmissionTicketConfig,
    control::{
        ControlConfig, ControlCore, ControlEffect, ControlInput, ControlReplyBody, ControlStep,
        EstablishSemanticFacts,
    },
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestReplyTag},
    lease::MonotonicInstant,
    participation::{
        DomainRefusal, EstablishReplayRequest, ParticipationEstablishment,
        ParticipationEstablishmentDecision, RequestResult,
    },
};
use std::time::Duration;

// One explicit model process owns these requests. The fixture's non-reused
// input issuance ordinal also names its original R Work; this is plain protocol
// metadata and cannot create runtime Work, capacity or an InputAuthor proof.
fn model_request_author(token: InputId) -> ScopedWorkRef {
    ScopedWorkRef {
        scope: ScopeId::new(1).expect("the model process Scope"),
        work: WorkId::new(token.get()).expect("the original model request ordinal"),
    }
}

fn at(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn bytes(value: u8) -> [u8; 16] {
    let mut bytes = [value; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn operation(value: u8) -> TaskOperationId {
    TaskOperationId::try_from_bytes(bytes(value)).unwrap()
}
fn ticket(value: u8) -> AdmissionTicketId {
    AdmissionTicketId::try_from_bytes(bytes(value)).unwrap()
}
fn participation(value: u64) -> ParticipationRef {
    ParticipationRef::new(
        QueryExecutionId::new(
            QueryId::new(
                i64::try_from(7000 + value).unwrap(),
                i64::try_from(8000 + value).unwrap(),
            ),
            AttemptId::new(1).unwrap(),
        )
        .unwrap(),
        FrontendProcessId::try_from_bytes(bytes(1)).unwrap(),
        BackendProcessId::try_from_bytes(bytes(2)).unwrap(),
    )
}
fn request(token: u64, operation: TaskOperationId, deadline: u64) -> RequestReplyTag {
    RequestReplyTag {
        author: model_request_author(InputId::new(token).unwrap()),
        token: InputId::new(token).unwrap(),
        operation,
        deadline: at(deadline),
    }
}
fn semantics() -> EstablishSemanticFacts {
    EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([1; 16]),
        shared_filter: ContentFingerprint::from_bytes([2; 16]),
        query_options: ContentFingerprint::from_bytes([3; 16]),
        credential_domain_id: CredentialDomainId::new(1),
        credential_epoch: CredentialEpoch::new(1).unwrap(),
        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    }
}
#[derive(Default)]
struct AdmissionStub {
    calls: Vec<AdmissionNeed>,
    next: u64,
}
impl AdmissionPort for AdmissionStub {
    fn try_reserve(&mut self, need: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.calls.push(need);
        self.next += 1;
        Ok(ReservationId::new(self.next).unwrap())
    }
}
struct Fixture {
    root: ControlCore,
    capacity: AdmissionStub,
    original: RequestReplyTag,
    ticket: AdmissionTicketId,
}
impl Fixture {
    fn new(max_pending: usize, original_deadline: u64) -> Self {
        let root = ControlCore::new(
            ControlConfig {
                max_pending_requests: max_pending,
                step_budget: ControlCore::MAX_STEP_WORK,
            },
            AdmissionTicketConfig::DEFAULT,
            [1; 8],
            at(0),
        )
        .unwrap();
        let mut f = Self {
            root,
            capacity: AdmissionStub::default(),
            original: request(2, operation(2), original_deadline),
            ticket: ticket(4),
        };
        let acquiring = request(1, operation(1), 100);
        let acquire = AcquireParticipationAdmissionTicket::new(
            acquiring.operation,
            participation(1),
            LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
            NativeCompatibilityId::new([3; 32]),
            f.root.admission().capability(),
        );
        let granted = f
            .root
            .handle_request(
                at(0),
                acquiring,
                ControlInput::Acquire {
                    request: acquire,
                    ticket: f.ticket,
                },
                &mut f.capacity,
            )
            .unwrap();
        assert_eq!(granted.requests.len(), 1);
        assert_eq!(granted.requests[0].request, acquiring);
        assert!(
            matches!(&granted.requests[0].body, ControlReplyBody::Admission(Ok(receipt)) if receipt.ticket_id() == f.ticket)
        );
        let input = f.establish(f.original, f.ticket, PayloadId(1), semantics());
        let fresh = f
            .root
            .handle_request(at(1), f.original, input, &mut f.capacity)
            .unwrap();
        assert_eq!(
            fresh.effects,
            [ControlEffect::Materialize {
                participation: participation(1),
                establishment: ParticipationEstablishment {
                    operation: f.original.operation,
                    ticket: f.ticket,
                    payload: PayloadId(1),
                    semantics: semantics(),
                    elected_at: at(1),
                },
                reservation: ReservationId::new(1).unwrap(),
                generation: Generation::new(1).unwrap(),
            }]
        );
        assert!(fresh.requests.is_empty());
        assert_eq!(
            f.root.route(participation(1)),
            ParticipationState::Establishing
        );
        f.assert_original_admissions();
        f
    }
    fn establish(
        &self,
        request: RequestReplyTag,
        ticket: AdmissionTicketId,
        payload: PayloadId,
        semantics: EstablishSemanticFacts,
    ) -> ControlInput {
        ControlInput::Establish {
            request: request.operation,
            participation: participation(1),
            ticket,
            payload,
            semantics,
            generation: Generation::new(1).unwrap(),
        }
    }
    fn event(&mut self, now: u64, input: ControlInput) -> ControlStep {
        let step = self
            .root
            .handle(at(now), input, &mut self.capacity)
            .unwrap();
        assert!(step.charged_work() <= self.root.max_step_work());
        step
    }
    fn route(
        &mut self,
        now: u64,
        request: RequestReplyTag,
        ticket: AdmissionTicketId,
        payload: PayloadId,
        semantics: EstablishSemanticFacts,
    ) -> ControlStep {
        let input = self.establish(request, ticket, payload, semantics);
        let step = self
            .root
            .handle_request(at(now), request, input, &mut self.capacity)
            .unwrap();
        assert_eq!(
            step.effects,
            [ControlEffect::RouteEstablish {
                participation: participation(1),
                request: EstablishReplayRequest {
                    request,
                    ticket,
                    payload,
                    semantics
                }
            }]
        );
        assert!(step.requests.is_empty());
        assert!(step.charged_work() <= self.root.max_step_work());
        self.assert_original_admissions();
        step
    }
    fn decide_original(
        &mut self,
        now: u64,
        state: ParticipationState,
        outcome: OperationOutcome,
    ) -> ControlStep {
        self.event(
            now,
            ControlInput::EstablishmentDecided {
                participation: participation(1),
                decision: ParticipationEstablishmentDecision {
                    state,
                    outcome: Ok(outcome),
                    acknowledgement: Some(novarocks_worker_protocol::participation::ParticipationAcknowledgement {
                        outcome,
                        receipt: std::sync::Arc::new(novarocks_execution_contract::ParticipationReceipt::new(
                            participation(1), state,
                        ).with_lease(novarocks_worker_protocol::lease::InstalledLease::install_initial(
                            semantics().initial_lease_valid_for, novarocks_worker_protocol::lease::LeaseBounds::DEFAULT, at(0),
                        ).receipt())),
                        termination_cause: None,
                    }),
                    decided_at: at(now),
                },
            },
        )
    }
    fn complete_route(
        &mut self,
        now: u64,
        request: RequestReplyTag,
        decided_at: u64,
        result: Result<OperationOutcome, DomainRefusal>,
    ) -> ControlStep {
        self.event(
            now,
            ControlInput::RoutedEstablishFinished {
                participation: participation(1),
                request,
                result,
                acknowledgement: None,
                decided_at: at(decided_at),
            },
        )
    }
    fn assert_original_admissions(&self) {
        assert_eq!(
            self.capacity.calls,
            [AdmissionNeed::Participation {
                participation: participation(1),
                generation: Generation::new(1).unwrap()
            }]
        );
    }
}
fn assert_verdict(step: &ControlStep, expected: RequestReplyTag, outcome: RequestResult) {
    assert_eq!(step.requests.len(), 1);
    assert_eq!(step.requests[0].request, expected);
    assert_eq!(step.requests[0].result, outcome);
    assert_eq!(step.requests[0].body, ControlReplyBody::Verdict(outcome));
}

fn assert_establishment_acknowledgement(
    step: &ControlStep,
    expected: RequestReplyTag,
    outcome: OperationOutcome,
    state: ParticipationState,
) {
    assert_eq!(step.requests.len(), 1);
    assert_eq!(step.requests[0].request, expected);
    assert_eq!(step.requests[0].result, RequestResult::Outcome(outcome));
    let ControlReplyBody::Participation(ack) = &step.requests[0].body else {
        panic!("the original entity decision keeps its complete Participation acknowledgement");
    };
    assert_eq!(ack.outcome, outcome);
    assert_eq!(ack.receipt.participation(), participation(1));
    assert_eq!(ack.receipt.state(), state);
    assert_eq!(
        ack.receipt.lease(),
        Some(novarocks_execution_contract::LeaseReceipt::new(
            novarocks_execution_contract::LeaseSequence::INITIAL,
            semantics().initial_lease_valid_for,
            Duration::from_secs(30),
        ))
    );
    assert!(
        ack.receipt.domains().is_empty(),
        "this fixture's original M authors no domain journal"
    );
    assert_eq!(ack.termination_cause, None);
}

#[test]
fn same_operation_routes_each_existing_participation_request_without_joining_the_original_materialization()
 {
    let mut f = Fixture::new(4, 8);
    let second = request(3, f.original.operation, 6);
    let changed = EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([9; 16]),
        ..semantics()
    };
    // Even incompatible candidate facts go to the actual Participation, not the
    // Root's original operation body or a second participation admission.
    f.route(2, second, ticket(8), PayloadId(9), changed);
    let first = f.decide_original(4, ParticipationState::Active, OperationOutcome::Accepted);
    assert_establishment_acknowledgement(
        &first,
        f.original,
        OperationOutcome::Accepted,
        ParticipationState::Active,
    );
    // The read was decided before its own deadline; delivery after that
    // deadline must not substitute the Root's current time or first body.
    let replay = f.complete_route(9, second, 5, Ok(OperationOutcome::ParticipationConflict));
    assert_verdict(
        &replay,
        second,
        RequestResult::Outcome(OperationOutcome::ParticipationConflict),
    );
    let third = request(4, f.original.operation, 20);
    f.route(10, third, f.ticket, PayloadId(1), semantics());
    let old = f.complete_route(10, second, 5, Ok(OperationOutcome::ParticipationConflict));
    assert!(old.requests.is_empty() && old.effects.is_empty());
    let current = f.complete_route(11, third, 10, Ok(OperationOutcome::Idempotent));
    assert_verdict(
        &current,
        third,
        RequestResult::Outcome(OperationOutcome::Idempotent),
    );
    assert_eq!(f.root.route(participation(1)), ParticipationState::Active);
    f.assert_original_admissions();
}

#[test]
fn wrong_route_correlation_or_decision_time_cannot_consume_the_original_pending_token() {
    for wrong in 0..7 {
        let mut f = Fixture::new(4, 100);
        let second = request(3, f.original.operation, 20);
        f.route(2, second, f.ticket, PayloadId(1), semantics());
        let mut candidate = second;
        let mut target = participation(1);
        let mut decided_at = at(3);
        let mut result = Ok(OperationOutcome::ParticipationConflict);
        match wrong {
            0 => target = participation(2),
            1 => candidate.operation = operation(9),
            2 => candidate.deadline = at(21),
            3 => candidate = f.original, // M's original R was not routed.
            4 => decided_at = at(6),     // Future relative to delivery at 5.
            5 => decided_at = at(1),     // Before this exact route's issue at 2.
            _ => result = Ok(OperationOutcome::Accepted),
        }
        let refused = f
            .root
            .handle(
                at(5),
                ControlInput::RoutedEstablishFinished {
                    participation: target,
                    request: candidate,
                    result,
                    acknowledgement: None,
                    decided_at,
                },
                &mut f.capacity,
            )
            .is_err();
        assert!(refused, "malformed route fact {wrong}");
        f.assert_original_admissions();
        assert_eq!(
            f.root.route(participation(1)),
            ParticipationState::Establishing
        );
        let valid = f.complete_route(6, second, 3, Ok(OperationOutcome::ParticipationConflict));
        assert_verdict(
            &valid,
            second,
            RequestResult::Outcome(OperationOutcome::ParticipationConflict),
        );
        let original = f.decide_original(7, ParticipationState::Active, OperationOutcome::Accepted);
        assert_establishment_acknowledgement(
            &original,
            f.original,
            OperationOutcome::Accepted,
            ParticipationState::Active,
        );
    }
}

#[test]
fn exact_deadline_cancels_only_the_original_route_and_late_returns_do_not_settle_a_new_rpc() {
    let mut f = Fixture::new(4, 100);
    let second = request(3, f.original.operation, 5);
    f.route(2, second, f.ticket, PayloadId(1), semantics());
    let expired = f.event(5, ControlInput::Tick);
    assert_verdict(&expired, second, RequestResult::TimeoutWhileAccepted);
    assert!(
        expired
            .effects
            .contains(&ControlEffect::CancelEstablishRoute {
                participation: participation(1),
                request: second
            })
    );
    assert_eq!(
        f.root.route(participation(1)),
        ParticipationState::Establishing
    );
    let third = request(4, f.original.operation, 10);
    f.route(6, third, f.ticket, PayloadId(1), semantics());
    let late = f.complete_route(6, second, 4, Ok(OperationOutcome::ParticipationConflict));
    assert!(late.requests.is_empty() && late.effects.is_empty());
    let current = f.complete_route(7, third, 7, Ok(OperationOutcome::ParticipationConflict));
    assert_verdict(
        &current,
        third,
        RequestResult::Outcome(OperationOutcome::ParticipationConflict),
    );
    let original = f.decide_original(8, ParticipationState::Active, OperationOutcome::Accepted);
    assert_establishment_acknowledgement(
        &original,
        f.original,
        OperationOutcome::Accepted,
        ParticipationState::Active,
    );
    f.assert_original_admissions();
}

#[test]
fn a_participation_decision_at_the_exact_rpc_deadline_is_timeout_even_if_delivered_later() {
    let mut f = Fixture::new(4, 100);
    let second = request(3, f.original.operation, 5);
    f.route(2, second, f.ticket, PayloadId(1), semantics());
    let result = f.complete_route(6, second, 5, Ok(OperationOutcome::ParticipationConflict));
    assert_verdict(&result, second, RequestResult::TimeoutWhileAccepted);
    assert!(!result.effects.iter().any(|effect| matches!(
        effect,
        ControlEffect::Materialize { .. } | ControlEffect::RouteEstablish { .. }
    )));
    f.assert_original_admissions();
}

#[test]
fn existing_closing_and_retained_participations_still_choose_their_own_replay_verdict() {
    for state in [
        ParticipationState::Quiescing,
        ParticipationState::Aborting,
        ParticipationState::Releasing,
        ParticipationState::TerminalRetained,
    ] {
        let mut f = Fixture::new(4, 100);
        let original = f.decide_original(2, state, OperationOutcome::ParticipationTerminalReceipt);
        assert_establishment_acknowledgement(
            &original,
            f.original,
            OperationOutcome::ParticipationTerminalReceipt,
            state,
        );
        let second = request(3, f.original.operation, 10);
        f.route(3, second, f.ticket, PayloadId(1), semantics());
        assert_eq!(f.root.route(participation(1)), state);
        let result = f.complete_route(
            4,
            second,
            3,
            Ok(OperationOutcome::ParticipationTerminalReceipt),
        );
        assert_verdict(
            &result,
            second,
            RequestResult::Outcome(OperationOutcome::ParticipationTerminalReceipt),
        );
        assert_eq!(f.root.route(participation(1)), state);
        f.assert_original_admissions();
    }
}

#[test]
fn permanent_fence_and_original_participation_expiry_win_until_actual_route_removal() {
    let mut f = Fixture::new(1, 100);
    f.event(
        2,
        ControlInput::FenceFrontend {
            frontend: participation(1).frontend_process_id(),
        },
    );
    let probe = request(3, f.original.operation, 2);
    let input = f.establish(probe, f.ticket, PayloadId(1), semantics());
    let fenced = f
        .root
        .handle_request(at(2), probe, input, &mut f.capacity)
        .unwrap();
    assert_verdict(
        &fenced,
        probe,
        RequestResult::Outcome(OperationOutcome::Gone),
    );
    assert!(fenced.effects.is_empty());
    f.assert_original_admissions();

    let mut f = Fixture::new(2, 100);
    f.decide_original(
        2,
        ParticipationState::TerminalRetained,
        OperationOutcome::ParticipationTerminalReceipt,
    );
    // Terminal M cannot prove execution exit. These explicit original facts
    // model P06 retirement and ParticipationCore's H, not an actual Native exit.
    f.event(
        2,
        ControlInput::ExecutionRetired {
            participation: participation(1),
            retirement:
                novarocks_worker_protocol::retention::ParticipationRetirementIdentity::Protocol {
                    close_epoch: Generation::new(3).unwrap(),
                },
        },
    );
    let removing = f.event(
        122,
        ControlInput::ParticipationExpired {
            participation: participation(1),
            retirement:
                novarocks_worker_protocol::retention::ParticipationRetirementIdentity::Protocol {
                    close_epoch: Generation::new(3).unwrap(),
                },
        },
    );
    assert!(removing.effects.contains(&ControlEffect::RemoveHost {
        participation: participation(1)
    }));
    // The original expired Participation still owns Gone until its actual route is
    // removed. Its fact takes priority even at this new request's deadline.
    let probe = request(3, f.original.operation, 122);
    let input = f.establish(probe, f.ticket, PayloadId(1), semantics());
    let gone = f
        .root
        .handle_request(at(122), probe, input, &mut f.capacity)
        .unwrap();
    assert_verdict(&gone, probe, RequestResult::Outcome(OperationOutcome::Gone));
    assert!(gone.effects.is_empty());
    // These are pure protocol inputs, not claims of an actual Native removal.
    f.event(
        123,
        ControlInput::HostRemoved {
            participation: participation(1),
        },
    );
    assert_eq!(f.root.route(participation(1)), ParticipationState::Absent);
    // No second Root-owned Gone timer survives the original request horizon.
    // An expired new request sees its own deadline without issuing admission.
    let probe = request(4, f.original.operation, 124);
    let input = f.establish(probe, f.ticket, PayloadId(1), semantics());
    let absent = f
        .root
        .handle_request(at(124), probe, input, &mut f.capacity)
        .unwrap();
    assert_verdict(
        &absent,
        probe,
        RequestResult::Outcome(OperationOutcome::OperationTimedOut),
    );
    assert!(absent.effects.is_empty());
    f.assert_original_admissions();
}

#[test]
fn routed_waiters_share_the_original_r_inventory_and_overflow_is_owner_failure() {
    for acquire_overflow in [false, true] {
        let mut f = Fixture::new(2, 100);
        let second = request(3, f.original.operation, 20);
        f.route(2, second, f.ticket, PayloadId(1), semantics());
        let excess = if acquire_overflow {
            request(4, operation(8), 20)
        } else {
            request(4, f.original.operation, 20)
        };
        let input = if acquire_overflow {
            ControlInput::Acquire {
                request: AcquireParticipationAdmissionTicket::new(
                    excess.operation,
                    participation(2),
                    LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    f.root.admission().capability(),
                ),
                ticket: ticket(8),
            }
        } else {
            f.establish(excess, f.ticket, PayloadId(1), semantics())
        };
        let failure = f
            .root
            .handle_request(at(3), excess, input, &mut f.capacity)
            .unwrap_err();
        assert_eq!(
            failure,
            novarocks_worker_protocol::participation::CoreError::IllegalFact
        );
        f.assert_original_admissions();
        // The invalid owner is not reused as a healthy request manager.
    }
}

#[test]
fn a_routed_domain_failure_keeps_its_exact_token_body_and_original_decision_time() {
    use novarocks_execution_contract::{
        OperationDomainFailure, SafeDetail,
        failure::{FailureClass, FailureFact, FailureReason, domain},
        operation::DomainFailureOutcome,
    };
    let mut f = Fixture::new(4, 100);
    let second = request(3, f.original.operation, 5);
    f.route(2, second, f.ticket, PayloadId(1), semantics());
    let refusal = DomainRefusal {
        outcome: DomainFailureOutcome::InvalidStateOrRequest,
        failure: OperationDomainFailure::new(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(91).unwrap()),
            ),
            SafeDetail::truncating("the original Participation decided this exact probe"),
        ),
    };
    let result = f.complete_route(6, second, 4, Err(refusal.clone()));
    assert_eq!(result.requests.len(), 1);
    assert_eq!(result.requests[0].request, second);
    assert_eq!(
        result.requests[0].result,
        RequestResult::Outcome(OperationOutcome::InvalidStateOrRequest)
    );
    assert_eq!(result.requests[0].body, ControlReplyBody::Domain(refusal));
    let original = f.decide_original(7, ParticipationState::Active, OperationOutcome::Accepted);
    assert_establishment_acknowledgement(
        &original,
        f.original,
        OperationOutcome::Accepted,
        ParticipationState::Active,
    );
    f.assert_original_admissions();
}
