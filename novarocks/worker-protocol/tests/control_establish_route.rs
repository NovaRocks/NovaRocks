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

use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionTicketId, ContentFingerprint, CredentialEpoch,
    CredentialLeaseId, LeaseValidFor, OperationOutcome, QueryContextRef, QueryContextState,
    TaskOperationId,
};
use novarocks_types::{
    NativeCompatibilityId,
    identity::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId},
};
use novarocks_worker_protocol::{
    admission::AdmissionTicketConfig,
    context::{
        ContextEstablishment, ContextEstablishmentDecision, DomainRefusal, EstablishReplayRequest,
        RequestResult,
    },
    control::{
        ControlConfig, ControlCore, ControlEffect, ControlInput, ControlReplyBody, ControlStep,
        EstablishSemanticFacts,
    },
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestContext},
    lease::MonotonicInstant,
};
use std::time::Duration;

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
fn context(value: u64) -> QueryContextRef {
    QueryContextRef::new(
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
fn request(token: u64, operation: TaskOperationId, deadline: u64) -> RequestContext {
    RequestContext {
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
        credential_lease: CredentialLeaseId::new(1),
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
    original: RequestContext,
    ticket: AdmissionTicketId,
}
impl Fixture {
    fn new(max_pending: usize, original_deadline: u64) -> Self {
        let root = ControlCore::new(
            ControlConfig {
                max_contexts: 4,
                max_waiters: 4,
                max_pending_requests: max_pending,
                step_budget: ControlCore::MAX_STEP_WORK,
                retained_contexts: 4,
                gone_fences: 4,
                retention: Duration::from_secs(10),
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
        let acquire = AcquireQueryContextAdmissionTicket::new(
            acquiring.operation,
            context(1),
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
                context: context(1),
                establishment: ContextEstablishment {
                    operation: f.original.operation,
                    ticket: f.ticket,
                    payload: PayloadId(1),
                    semantics: semantics(),
                    elected_at: at(1),
                },
                reservation: ReservationId::new(2).unwrap(),
                generation: Generation::new(1).unwrap(),
            }]
        );
        assert!(fresh.requests.is_empty());
        assert_eq!(f.root.route(context(1)), QueryContextState::Establishing);
        f.assert_original_admissions();
        f
    }
    fn establish(
        &self,
        request: RequestContext,
        ticket: AdmissionTicketId,
        payload: PayloadId,
        semantics: EstablishSemanticFacts,
    ) -> ControlInput {
        ControlInput::Establish {
            request: request.operation,
            context: context(1),
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
        request: RequestContext,
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
                context: context(1),
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
        state: QueryContextState,
        outcome: OperationOutcome,
    ) -> ControlStep {
        self.event(
            now,
            ControlInput::EstablishmentDecided {
                context: context(1),
                decision: ContextEstablishmentDecision {
                    state,
                    outcome: Ok(outcome),
                    acknowledgement: Some(novarocks_worker_protocol::context::ContextAcknowledgement {
                        outcome,
                        receipt: std::sync::Arc::new(novarocks_execution_contract::QueryContextReceipt::new(
                            context(1), state,
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
        request: RequestContext,
        decided_at: u64,
        result: Result<OperationOutcome, DomainRefusal>,
    ) -> ControlStep {
        self.event(
            now,
            ControlInput::RoutedEstablishFinished {
                context: context(1),
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
            [
                AdmissionNeed::Ticket {
                    context: context(1)
                },
                AdmissionNeed::Context {
                    context: context(1),
                    generation: Generation::new(1).unwrap()
                }
            ]
        );
    }
}
fn assert_verdict(step: &ControlStep, expected: RequestContext, outcome: RequestResult) {
    assert_eq!(step.requests.len(), 1);
    assert_eq!(step.requests[0].request, expected);
    assert_eq!(step.requests[0].result, outcome);
    assert_eq!(step.requests[0].body, ControlReplyBody::Verdict(outcome));
}

#[test]
fn same_operation_routes_each_existing_context_request_without_joining_the_original_materialization()
 {
    let mut f = Fixture::new(4, 8);
    let second = request(3, f.original.operation, 6);
    let changed = EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([9; 16]),
        ..semantics()
    };
    // Even incompatible candidate facts go to the actual Context, not the
    // Root's original operation body or a second context admission.
    f.route(2, second, ticket(8), PayloadId(9), changed);
    let first = f.decide_original(4, QueryContextState::Active, OperationOutcome::Accepted);
    assert_verdict(
        &first,
        f.original,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    // The read was decided before its own deadline; delivery after that
    // deadline must not substitute the Root's current time or first body.
    let replay = f.complete_route(9, second, 5, Ok(OperationOutcome::ContextConflict));
    assert_verdict(
        &replay,
        second,
        RequestResult::Outcome(OperationOutcome::ContextConflict),
    );
    let third = request(4, f.original.operation, 20);
    f.route(10, third, f.ticket, PayloadId(1), semantics());
    let old = f.complete_route(10, second, 5, Ok(OperationOutcome::ContextConflict));
    assert!(old.requests.is_empty() && old.effects.is_empty());
    let current = f.complete_route(11, third, 10, Ok(OperationOutcome::Idempotent));
    assert_verdict(
        &current,
        third,
        RequestResult::Outcome(OperationOutcome::Idempotent),
    );
    assert_eq!(f.root.route(context(1)), QueryContextState::Active);
    f.assert_original_admissions();
}

#[test]
fn wrong_route_correlation_or_decision_time_cannot_consume_the_original_pending_token() {
    for wrong in 0..7 {
        let mut f = Fixture::new(4, 100);
        let second = request(3, f.original.operation, 20);
        f.route(2, second, f.ticket, PayloadId(1), semantics());
        let mut candidate = second;
        let mut target = context(1);
        let mut decided_at = at(3);
        let mut result = Ok(OperationOutcome::ContextConflict);
        match wrong {
            0 => target = context(2),
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
                    context: target,
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
        assert_eq!(f.root.route(context(1)), QueryContextState::Establishing);
        let valid = f.complete_route(6, second, 3, Ok(OperationOutcome::ContextConflict));
        assert_verdict(
            &valid,
            second,
            RequestResult::Outcome(OperationOutcome::ContextConflict),
        );
        let original = f.decide_original(7, QueryContextState::Active, OperationOutcome::Accepted);
        assert_verdict(
            &original,
            f.original,
            RequestResult::Outcome(OperationOutcome::Accepted),
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
                context: context(1),
                request: second
            })
    );
    assert_eq!(f.root.route(context(1)), QueryContextState::Establishing);
    let third = request(4, f.original.operation, 10);
    f.route(6, third, f.ticket, PayloadId(1), semantics());
    let late = f.complete_route(6, second, 4, Ok(OperationOutcome::ContextConflict));
    assert!(late.requests.is_empty() && late.effects.is_empty());
    let current = f.complete_route(7, third, 7, Ok(OperationOutcome::ContextConflict));
    assert_verdict(
        &current,
        third,
        RequestResult::Outcome(OperationOutcome::ContextConflict),
    );
    let original = f.decide_original(8, QueryContextState::Active, OperationOutcome::Accepted);
    assert_verdict(
        &original,
        f.original,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    f.assert_original_admissions();
}

#[test]
fn a_context_decision_at_the_exact_rpc_deadline_is_timeout_even_if_delivered_later() {
    let mut f = Fixture::new(4, 100);
    let second = request(3, f.original.operation, 5);
    f.route(2, second, f.ticket, PayloadId(1), semantics());
    let result = f.complete_route(6, second, 5, Ok(OperationOutcome::ContextConflict));
    assert_verdict(&result, second, RequestResult::TimeoutWhileAccepted);
    assert!(!result.effects.iter().any(|effect| matches!(
        effect,
        ControlEffect::Materialize { .. } | ControlEffect::RouteEstablish { .. }
    )));
    f.assert_original_admissions();
}

#[test]
fn existing_closing_and_retained_contexts_still_choose_their_own_replay_verdict() {
    for state in [
        QueryContextState::Quiescing,
        QueryContextState::Aborting,
        QueryContextState::Releasing,
        QueryContextState::TerminalRetained,
    ] {
        let mut f = Fixture::new(4, 100);
        let original = f.decide_original(2, state, OperationOutcome::ContextTerminalReceipt);
        assert_verdict(
            &original,
            f.original,
            RequestResult::Outcome(OperationOutcome::ContextTerminalReceipt),
        );
        let second = request(3, f.original.operation, 10);
        f.route(3, second, f.ticket, PayloadId(1), semantics());
        assert_eq!(f.root.route(context(1)), state);
        let result = f.complete_route(4, second, 3, Ok(OperationOutcome::ContextTerminalReceipt));
        assert_verdict(
            &result,
            second,
            RequestResult::Outcome(OperationOutcome::ContextTerminalReceipt),
        );
        assert_eq!(f.root.route(context(1)), state);
        f.assert_original_admissions();
    }
}

#[test]
fn permanent_frontend_fence_and_gone_take_priority_without_reentering_admission() {
    let mut f = Fixture::new(1, 100);
    f.event(
        2,
        ControlInput::FenceFrontend {
            frontend: context(1).frontend_process_id(),
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
        QueryContextState::TerminalRetained,
        OperationOutcome::ContextTerminalReceipt,
    );
    let removing = f.event(12, ControlInput::Tick);
    assert!(removing.effects.contains(&ControlEffect::RemoveHost {
        context: context(1)
    }));
    // These are pure protocol inputs, not claims of an actual Native removal.
    f.event(
        13,
        ControlInput::HostRemoved {
            context: context(1),
        },
    );
    assert_eq!(f.root.route(context(1)), QueryContextState::Gone);
    let probe = request(3, f.original.operation, 14);
    let input = f.establish(probe, f.ticket, PayloadId(1), semantics());
    let gone = f
        .root
        .handle_request(at(14), probe, input, &mut f.capacity)
        .unwrap();
    assert_verdict(&gone, probe, RequestResult::Outcome(OperationOutcome::Gone));
    assert!(gone.effects.is_empty());
    f.assert_original_admissions();
}

#[test]
fn routed_waiters_share_the_same_pending_limit_with_original_requests_and_acquire() {
    let mut f = Fixture::new(2, 100);
    let second = request(3, f.original.operation, 20);
    f.route(2, second, f.ticket, PayloadId(1), semantics());
    let denied = request(4, f.original.operation, 20);
    let input = f.establish(denied, f.ticket, PayloadId(1), semantics());
    let full = f
        .root
        .handle_request(at(3), denied, input, &mut f.capacity)
        .unwrap();
    assert_verdict(
        &full,
        denied,
        RequestResult::Outcome(OperationOutcome::ResourceExhausted),
    );
    assert!(full.effects.is_empty());
    let acquiring = request(5, operation(8), 20);
    let acquire = AcquireQueryContextAdmissionTicket::new(
        acquiring.operation,
        context(2),
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        f.root.admission().capability(),
    );
    let full = f
        .root
        .handle_request(
            at(3),
            acquiring,
            ControlInput::Acquire {
                request: acquire,
                ticket: ticket(8),
            },
            &mut f.capacity,
        )
        .unwrap();
    assert_verdict(
        &full,
        acquiring,
        RequestResult::Outcome(OperationOutcome::ResourceExhausted),
    );
    f.assert_original_admissions();
    let original = f.decide_original(4, QueryContextState::Active, OperationOutcome::Accepted);
    assert_verdict(
        &original,
        f.original,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    let third = request(6, f.original.operation, 20);
    f.route(5, third, f.ticket, PayloadId(1), semantics());
    let replay = f.complete_route(6, second, 5, Ok(OperationOutcome::ContextConflict));
    assert_verdict(
        &replay,
        second,
        RequestResult::Outcome(OperationOutcome::ContextConflict),
    );
    let replay = f.complete_route(7, third, 6, Ok(OperationOutcome::Idempotent));
    assert_verdict(
        &replay,
        third,
        RequestResult::Outcome(OperationOutcome::Idempotent),
    );
    f.assert_original_admissions();
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
            SafeDetail::truncating("the original Context decided this exact probe"),
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
    let original = f.decide_original(7, QueryContextState::Active, OperationOutcome::Accepted);
    assert_verdict(
        &original,
        f.original,
        RequestResult::Outcome(OperationOutcome::Accepted),
    );
    f.assert_original_admissions();
}
