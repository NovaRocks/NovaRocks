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

//! Exercise the real Core directory lifecycle and its sparse member index.
//! Admission is a pure fixture; no host exit or runtime fee is inferred here.

use super::{
    ControlConfig, ControlCore, ControlEffect, ControlInput, ControlReplyBody, ControlStep,
};
use crate::{
    admission::AdmissionTicketConfig,
    control::EstablishSemanticFacts,
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestReplyTag},
    lease::MonotonicInstant,
    participation::ParticipationEstablishmentDecision,
};
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
fn bytes(value: u64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    bytes[6] = 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}
fn participation(frontend: u64, query: u64) -> ParticipationRef {
    ParticipationRef::new(
        QueryExecutionId::new(
            QueryId::new(
                i64::try_from(7000 + query).unwrap(),
                i64::try_from(8000 + query).unwrap(),
            ),
            AttemptId::new(1).unwrap(),
        )
        .unwrap(),
        FrontendProcessId::try_from_bytes(bytes(frontend)).unwrap(),
        BackendProcessId::try_from_bytes(bytes(2)).unwrap(),
    )
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
    calls: usize,
    next: u64,
}
impl AdmissionPort for AdmissionStub {
    fn try_reserve(&mut self, _: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.calls += 1;
        self.next += 1;
        Ok(ReservationId::new(self.next).unwrap())
    }
}
struct Fixture {
    core: ControlCore,
    capacity: AdmissionStub,
    now: u64,
    next: u64,
}
impl Fixture {
    fn new() -> Self {
        Self {
            core: ControlCore::new(
                participation(1, 1).backend_process_id(),
                ControlConfig {
                    max_pending_requests: 2,
                    step_budget: ControlCore::MAX_STEP_WORK,
                },
                AdmissionTicketConfig::DEFAULT,
                [1; 8],
                at(0),
            )
            .unwrap(),
            capacity: AdmissionStub::default(),
            now: 0,
            next: 1,
        }
    }
    fn request(&mut self) -> RequestReplyTag {
        let id = self.next;
        self.next += 1;
        RequestReplyTag {
            author: model_request_author(InputId::new(id).unwrap()),
            token: InputId::new(id).unwrap(),
            operation: TaskOperationId::try_from_bytes(bytes(id)).unwrap(),
            deadline: at(self.now + 100),
        }
    }
    fn event(&mut self, input: ControlInput) -> ControlStep {
        let step = self
            .core
            .handle(at(self.now), input, &mut self.capacity)
            .unwrap();
        assert!(step.charged_work() <= self.core.max_step_work());
        step
    }
    fn acquire(&mut self, participation: ParticipationRef) -> (AdmissionTicketId, ControlStep) {
        let request = self.request();
        let ticket = AdmissionTicketId::try_from_bytes(bytes(request.token.get())).unwrap();
        let body = AcquireParticipationAdmissionTicket::new(
            request.operation,
            participation,
            LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
            NativeCompatibilityId::new([3; 32]),
            self.core.admission().capability(),
        );
        let step = self
            .core
            .handle_request(
                at(self.now),
                request,
                ControlInput::Acquire {
                    request: body,
                    ticket,
                },
                &mut self.capacity,
            )
            .unwrap();
        assert!(step.charged_work() <= self.core.max_step_work());
        assert_eq!(step.requests.len(), 1);
        assert_eq!(step.requests[0].request, request);
        (ticket, step)
    }
    fn activate(&mut self, participation: ParticipationRef) {
        let (ticket, granted) = self.acquire(participation);
        assert!(matches!(
            &granted.requests[0].body,
            ControlReplyBody::Admission(Ok(receipt)) if receipt.ticket_id() == ticket
        ));
        let request = self.request();
        let step = self
            .core
            .handle_request(
                at(self.now),
                request,
                ControlInput::Establish {
                    request: request.operation,
                    participation,
                    ticket,
                    payload: PayloadId(request.token.get()),
                    semantics: semantics(),
                    generation: Generation::new(1).unwrap(),
                },
                &mut self.capacity,
            )
            .unwrap();
        assert!(step.effects.iter().any(|effect| matches!(
            effect, ControlEffect::Materialize { participation: original, .. } if *original == participation
        )));
        assert_eq!(
            self.core.route(participation),
            ParticipationState::Establishing
        );
        let decided = self.event(ControlInput::EstablishmentDecided {
            participation,
            decision: ParticipationEstablishmentDecision {
                state: ParticipationState::Active,
                outcome: Ok(OperationOutcome::Accepted),
                acknowledgement: Some(crate::participation::ParticipationAcknowledgement {
                    outcome: OperationOutcome::Accepted,
                    receipt: std::sync::Arc::new(
                        novarocks_execution_contract::ParticipationReceipt::new(
                            participation,
                            ParticipationState::Active,
                        )
                        .with_lease(
                            crate::lease::InstalledLease::install_initial(
                                semantics().initial_lease_valid_for,
                                crate::lease::LeaseBounds::DEFAULT,
                                at(self.now),
                            )
                            .receipt(),
                        ),
                    ),
                    termination_cause: None,
                }),
                decided_at: at(self.now),
            },
        });
        assert_eq!(decided.requests.len(), 1);
        assert_eq!(decided.requests[0].request, request);
        assert_eq!(self.core.route(participation), ParticipationState::Active);
    }
    fn remove(&mut self, participation: ParticipationRef) {
        let close_epoch = Generation::new(3).unwrap();
        let retired = self.event(ControlInput::ExecutionRetired {
            participation,
            retirement: crate::retention::ParticipationRetirementIdentity::Protocol { close_epoch },
        });
        assert_eq!(
            retired
                .effects
                .iter()
                .filter(|effect| matches!(effect, ControlEffect::RetireReservation(_)))
                .count(),
            1
        );
        assert_eq!(
            self.core.route(participation),
            ParticipationState::TerminalRetained
        );
        // The explicit fact models the original Participation timer; Root does not
        // manufacture H from a second TTL. No Native exit is claimed here.
        self.now += 120;
        let due = self.event(ControlInput::ParticipationExpired {
            participation,
            retirement: crate::retention::ParticipationRetirementIdentity::Protocol { close_epoch },
        });
        assert!(due.effects.iter().any(|effect| matches!(
            effect, ControlEffect::RemoveHost { participation: original } if *original == participation
        )));
        let removed = self.event(ControlInput::HostRemoved { participation });
        assert!(
            !removed
                .effects
                .iter()
                .any(|effect| matches!(effect, ControlEffect::RetireReservation(_)))
        );
        assert_eq!(self.core.route(participation), ParticipationState::Absent);
    }
}

#[test]
fn sequential_frontend_churn_reclaims_actual_member_rows_after_participation_expiry() {
    let mut f = Fixture::new();
    for query in 1..=64 {
        let participation = participation(10 + query, query);
        f.activate(participation);
        assert_eq!(f.core.frontend_members.len(), 1);
        assert_eq!(f.core.directory.len(), 1);
        f.remove(participation);
        assert!(f.core.frontend_members.is_empty());
        assert!(f.core.directory.is_empty());
        assert!(f.core.owners.is_empty());
        assert!(f.core.fenced.is_empty());
    }
    assert_eq!(f.capacity.calls, 64);
}

#[test]
fn removing_one_member_preserves_its_live_sibling_and_last_removal_keeps_the_permanent_fence() {
    let mut f = Fixture::new();
    let first = participation(3, 1);
    let second = participation(3, 2);
    f.activate(first);
    f.activate(second);
    assert_eq!(f.core.frontend_members.len(), 1);
    assert_eq!(
        f.core.frontend_members[&first.frontend_process_id()].len(),
        2
    );
    f.remove(first);
    assert_eq!(f.core.frontend_members.len(), 1);
    let remaining = &f.core.frontend_members[&first.frontend_process_id()];
    assert_eq!(remaining.len(), 1);
    assert!(remaining.contains(&second));
    assert!(!remaining.contains(&first));
    f.event(ControlInput::FenceFrontend {
        frontend: first.frontend_process_id(),
    });
    let mut effects = Vec::new();
    for _ in 0..16 {
        if !f.core.has_internal_work() {
            break;
        }
        effects.extend(f.event(ControlInput::NextInternal).effects);
    }
    assert!(!f.core.has_internal_work());
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(effect,
                ControlEffect::AbortParticipation { participation, .. } if *participation == second
            ))
            .count(),
        1
    );
    assert_eq!(f.core.route(second), ParticipationState::Aborting);
    f.remove(second);
    assert!(f.core.frontend_members.is_empty());
    assert!(f.core.fenced.contains(&first.frontend_process_id()));
    let calls = f.capacity.calls;
    let (_, refused) = f.acquire(participation(3, 3));
    assert!(matches!(
        &refused.requests[0].body,
        ControlReplyBody::Admission(Err(OperationOutcome::Gone))
    ));
    assert_eq!(f.capacity.calls, calls);
    assert!(f.core.frontend_members.is_empty());
}
