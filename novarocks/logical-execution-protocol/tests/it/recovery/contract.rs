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

//! Public-core sequences use two real participation identities and complete
//! admission request/receipt metadata. No Native owner or I/O is simulated.
use super::super::support::*;
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, AttemptAuthorizationRef, AttemptIssueSettlement,
    ExecutionFailure, ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
    logical_port::{AttemptAuthorization, AttemptAuthorizationFailure},
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialDomainId,
    CredentialEpoch, CredentialUpdate, EstablishParticipation, LeaseValidFor, OperationOutcome,
    ParticipationAdmissionTicketReceipt, ParticipationRef, SafeDetail, TaskOperationId,
    failure::FailureProjection,
};
use novarocks_types::{
    BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId, StageId, TaskId,
};
use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc, time::Duration};
pub(crate) fn at(ms: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_millis(ms))
}
pub(crate) fn operation(tag: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(tag, 400)).issue()
}
pub(crate) fn participant(attempt: u64, backend: u128) -> ParticipationRef {
    ParticipationRef::new(
        activation(attempt).execution(),
        FrontendProcessId::try_from_bytes(operation(900).to_bytes()).unwrap(),
        if backend == 1 {
            root(attempt).backend_process_id()
        } else {
            BackendProcessId::try_from_bytes(operation(1000 + backend).to_bytes()).unwrap()
        },
    )
}
pub(crate) fn roster(attempt: u64) -> Vec<ParticipationRef> {
    vec![participant(attempt, 1), participant(attempt, 2)]
}
pub(crate) fn admission(p: ParticipationRef, tag: u8, valid_ms: u64) -> QualifiedAdmission {
    let valid_for = LeaseValidFor::new(Duration::from_millis(valid_ms)).unwrap();
    QualifiedAdmission {
        request: AcquireParticipationAdmissionTicket::new(
            operation(2000 + u128::from(tag)),
            p,
            valid_for,
            NativeCompatibilityId::new([tag; 32]),
            AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
        ),
        receipt: ParticipationAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
            p,
            valid_for,
        ),
    }
}
pub(crate) fn backend_failure(attempt: u64, text: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        SafeDetail::truncating(text),
        ExecutionFailureSite::Backend {
            process: root(attempt).backend_process_id(),
        },
    ))
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
pub(crate) fn establish_request(
    p: ParticipationRef,
    receipt: ParticipationAdmissionTicketReceipt,
    tag: u8,
) -> AttemptAuthorization {
    let content = |offset| {
        Arc::new(Metadata(ContentFingerprint::from_bytes([tag + offset; 16])))
            as Arc<dyn CodecOwnedContent>
    };
    let original = EstablishParticipation::new(
        operation(3000 + u128::from(tag)),
        p,
        receipt.ticket_id(),
        content(0),
        content(1),
        content(2),
        CredentialUpdate::new(
            CredentialDomainId::new(u64::from(tag)),
            CredentialEpoch::new(1).unwrap(),
            Arc::new(Secret),
        ),
        LeaseValidFor::new(Duration::from_secs(1)).unwrap(),
    );
    AttemptAuthorization::Establish {
        participation: p,
        operation_id: original.envelope().operation_id(),
        ticket_id: original.admission_ticket_id(),
        semantics: original.semantic_identity(),
        native_compatibility_id: NativeCompatibilityId::new([tag; 32]),
    }
}
#[derive(Clone, Copy)]
pub(crate) struct Issued {
    pub activation: AttemptActivationIdentity,
    pub participation: ParticipationRef,
    pub authorization: AttemptAuthorizationRef,
    pub operation: TaskOperationId,
}
#[derive(Clone)]
pub(crate) struct Dispatch {
    pub action: LogicalActionId,
    pub identity: QualificationIdentity,
    pub operation: TaskOperationId,
    pub expires_at: MonotonicInstant,
    pub failed: Arc<[ParticipationRef]>,
    pub replacement: Arc<[ParticipationRef]>,
}
pub(crate) struct RecoveryFixture {
    pub f: Fixture,
    next: u64,
    roots: BTreeMap<QueryExecutionId, TaskIdentity>,
}
impl RecoveryFixture {
    pub(crate) fn new(output: LogicalOutputMode) -> Self {
        Self::with_policy(
            output,
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
        )
    }
    pub(crate) fn with_policy(
        output: LogicalOutputMode,
        recovery: RecoveryMode,
        effect: ExecutionEffect,
    ) -> Self {
        let mut original = start(output);
        original.policy.recovery = recovery;
        original.policy.effect = effect;
        original.budgets.replacement_reservation_valid_for = Some(Duration::from_millis(50));
        original.first_attempt.participations = roster(1).into_boxed_slice();
        let mut result = Self::unstarted(original);
        result.f.begin();
        result
    }
    pub(crate) fn unstarted(original: LogicalStart) -> Self {
        Self {
            f: Fixture::with_start(original),
            next: 1000,
            roots: BTreeMap::from([(activation(1).execution(), root(1))]),
        }
    }
    pub(crate) fn send_port(&mut self, a: AttemptActivationIdentity, body: AttemptToLogicalBody) {
        self.next += 1;
        self.f
            .send(LogicalInput::AttemptPort(Box::new(AttemptToLogical {
                activation: a,
                action: AttemptActionId::new(NonZeroU64::new(self.next).unwrap()),
                body,
            })));
    }
    pub(crate) fn authorize(
        &mut self,
        a: AttemptActivationIdentity,
        request: AttemptAuthorization,
    ) -> Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure> {
        self.next += 1;
        let token = AttemptRequestToken::new(NonZeroU64::new(self.next).unwrap());
        self.send_port(
            a,
            AttemptToLogicalBody::AuthorizationRequest { token, request },
        );
        let effect = self.f.take(|b| matches!(b, LogicalEffectBody::AttemptPort(p) if p.activation == a && matches!(p.body, LogicalToAttemptBody::AuthorizationReply { token: held, .. } if held == token)));
        let LogicalEffectBody::AttemptPort(p) = effect.body else {
            unreachable!()
        };
        let LogicalToAttemptBody::AuthorizationReply { decision, .. } = p.body else {
            unreachable!()
        };
        decision
    }
    pub(crate) fn grant(
        &mut self,
        a: AttemptActivationIdentity,
        p: ParticipationRef,
        tag: u8,
    ) -> QualifiedAdmission {
        let original = admission(p, tag, 1000);
        let AttemptAuthorizationDecision::AdmissionStarted(issue) = self
            .authorize(a, AttemptAuthorization::BeginAdmission(original.request))
            .unwrap()
        else {
            panic!("exact original admission reference required");
        };
        assert!(matches!(
            self.authorize(
                a,
                AttemptAuthorization::SettleAdmission {
                    issue,
                    participation: p,
                    operation_id: original.request.envelope().operation_id(),
                    outcome: Some(OperationOutcome::Accepted),
                    receipt: Some(original.receipt),
                    prior_transport_unknown: false
                }
            ),
            Ok(AttemptAuthorizationDecision::AdmissionGranted)
        ));
        original
    }
    pub(crate) fn issue(
        &mut self,
        a: AttemptActivationIdentity,
        p: ParticipationRef,
        grant: QualifiedAdmission,
        tag: u8,
    ) -> Issued {
        let request = establish_request(p, grant.receipt, tag);
        let AttemptAuthorization::Establish { operation_id, .. } = request else {
            unreachable!()
        };
        let AttemptAuthorizationDecision::EstablishAuthorized(authorization) =
            self.authorize(a, request).unwrap()
        else {
            panic!("original Establish authorization required");
        };
        Issued {
            activation: a,
            participation: p,
            authorization,
            operation: operation_id,
        }
    }
    pub(crate) fn settle(
        &mut self,
        issued: Issued,
        settlement: AttemptIssueSettlement,
    ) -> Result<(), ExecutionFailure> {
        self.next += 1;
        let token = AttemptRequestToken::new(NonZeroU64::new(self.next).unwrap());
        self.send_port(
            issued.activation,
            AttemptToLogicalBody::EstablishSettlement {
                token,
                authorization: issued.authorization,
                participation: issued.participation,
                operation_id: issued.operation,
                settlement,
            },
        );
        let effect = self.f.take(|b| matches!(b, LogicalEffectBody::AttemptPort(p) if p.activation == issued.activation && matches!(p.body, LogicalToAttemptBody::EstablishSettlementReply { token: held, .. } if held == token)));
        let LogicalEffectBody::AttemptPort(p) = effect.body else {
            unreachable!()
        };
        let LogicalToAttemptBody::EstablishSettlementReply { result, .. } = p.body else {
            unreachable!()
        };
        result
    }
    pub(crate) fn establish_initial(&mut self, settlement: AttemptIssueSettlement) -> Vec<Issued> {
        roster(1)
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                let tag = 10 + i as u8;
                let grant = self.grant(activation(1), p, tag);
                let issued = self.issue(activation(1), p, grant, tag);
                if matches!(
                    settlement,
                    AttemptIssueSettlement::TransportUnknown | AttemptIssueSettlement::Worker(_)
                ) {
                    self.settle(issued, AttemptIssueSettlement::TransportOwned)
                        .unwrap();
                }
                self.settle(issued, settlement).unwrap();
                issued
            })
            .collect()
    }
    pub(crate) fn fail(&mut self, a: AttemptActivationIdentity, failure: ExecutionFailure) {
        self.send_port(
            a,
            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(failure)),
        );
    }
    pub(crate) fn prepare(
        &mut self,
        replacement: Vec<ParticipationRef>,
    ) -> (Dispatch, SuccessorTopology) {
        let effect = self.f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            )
        });
        let LogicalEffectBody::Native(NativeEffect::PrepareSuccessor {
            replacement: execution,
            topology,
            deadline,
            ..
        }) = effect.body
        else {
            unreachable!()
        };
        assert!(
            replacement
                .iter()
                .all(|p| p.query_execution_id() == execution)
        );
        self.f.ack(
            effect.action,
            ActionResult::Native(NativeFact::SuccessorPrepared {
                execution,
                participations: Arc::from(replacement),
            }),
        );
        let effect = self.f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::DispatchQualification { .. })
            )
        });
        let LogicalEffectBody::Native(NativeEffect::DispatchQualification {
            identity,
            operation_id,
            failed_participations,
            replacement_participations,
            expires_at,
            ..
        }) = effect.body
        else {
            unreachable!()
        };
        assert_eq!(expires_at, deadline);
        (
            Dispatch {
                action: effect.action,
                identity,
                operation: operation_id,
                expires_at,
                failed: failed_participations,
                replacement: replacement_participations,
            },
            topology,
        )
    }
    pub(crate) fn qualify(
        &mut self,
        d: &Dispatch,
        admissions: Vec<QualifiedAdmission>,
        dispatched_at: MonotonicInstant,
    ) {
        self.f.ack(
            d.action,
            ActionResult::Native(NativeFact::QualificationSettled {
                identity: d.identity,
                operation_id: d.operation,
                dispatched_at: Some(dispatched_at),
                settlement: QualificationSettlement::Qualified(QualifiedReplacementFacts {
                    identity: d.identity,
                    operation_id: d.operation,
                    topology_revision: 7,
                    admissions: admissions.into_boxed_slice(),
                }),
            }),
        );
    }
    pub(crate) fn activate(
        &mut self,
        d: &Dispatch,
    ) -> (
        AttemptActivationIdentity,
        Vec<novarocks_coordination_contract::AttemptInheritedAdmission>,
    ) {
        let effect = self.f.take(|b| matches!(b, LogicalEffectBody::Native(NativeEffect::ActivateReservation { identity, .. }) if *identity == d.identity));
        self.f.ack(
            effect.action,
            ActionResult::Native(NativeFact::ReservationActivated {
                identity: d.identity,
                operation_id: d.operation,
            }),
        );
        let a = AttemptActivationIdentity::from_parts(
            d.identity.actor.get(),
            d.identity.replacement,
            d.identity.generation,
        );
        let register = self.f.take(|b| matches!(b, LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { activation, .. }) if *activation == a));
        self.f.ack(
            register.action,
            ActionResult::Governance(GovernanceFact::ObligationRegistered {
                activation: a,
                kind: ObligationKind::RunningWork,
            }),
        );
        let effect = self.f.take(|b| matches!(b, LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation, .. }) if *activation == a));
        let LogicalEffectBody::Native(NativeEffect::ActivateAttempt {
            inherited_admissions,
            ..
        }) = effect.body
        else {
            unreachable!()
        };
        let original_root = TaskIdentity::new(
            a.execution(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            d.replacement[0].backend_process_id(),
        );
        self.roots.insert(a.execution(), original_root);
        self.f.ack(
            effect.action,
            ActionResult::Native(NativeFact::AttemptActivated {
                activation: a,
                root: self.f.declared_read_root(original_root),
            }),
        );
        (a, inherited_admissions)
    }
    pub(crate) fn relay(&mut self, a: AttemptActivationIdentity, request: RelayRequest) {
        self.next += 1;
        self.f.send(LogicalInput::Relay {
            source: LogicalSourceId::new(NonZeroU64::new(a.generation()).unwrap()),
            token: LogicalRequestToken::new(NonZeroU64::new(self.next).unwrap()),
            activation: a,
            request,
        });
    }
    pub(crate) fn succeed_empty_stream(&mut self, a: AttemptActivationIdentity) {
        let root = *self.roots.get(&a.execution()).unwrap();
        let status = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(6).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        self.send_port(
            a,
            AttemptToLogicalBody::RootStatus(Box::new(
                novarocks_coordination_contract::AttemptRootProjection {
                    status,
                    failure: novarocks_coordination_contract::AttemptRootFailure::None,
                },
            )),
        );
        self.relay(
            a,
            RelayRequest::OpenDelivery {
                root,
                carrier: carrier(false),
            },
        );
        if let Some(index) = self.f.effects.iter().position(|e| {
            matches!(
                e.body,
                LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
            )
        }) {
            let schema = self.f.effects.remove(index);
            self.f.ack(
                schema.action,
                ActionResult::Consumer(ConsumerFact::SchemaWritten),
            );
        }
        self.relay(
            a,
            RelayRequest::EndConsumed {
                root,
                end: end(0, 0),
                accepted_packets: 0,
                delivered_through: None,
            },
        );
        let seal=self.f.take(|b|matches!(b,LogicalEffectBody::AttemptPort(p)if p.activation==a&&matches!(p.body,LogicalToAttemptBody::SealRequest{..})));
        let LogicalEffectBody::AttemptPort(p) = seal.body else {
            unreachable!()
        };
        let LogicalToAttemptBody::SealRequest { token } = p.body else {
            unreachable!()
        };
        self.send_port(
            a,
            AttemptToLogicalBody::SealResult {
                token,
                result: Ok(Arc::from([])),
            },
        );
        let slot=self.f.take(|b|matches!(b,LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot{activation})if *activation==a));
        assert_eq!(self.f.core.facts().conclusion(), None);
        self.f.ack(
            slot.action,
            ActionResult::Consumer(ConsumerFact::EndSlotReady),
        );
        assert_eq!(
            self.f.core.facts().conclusion(),
            Some(LogicalConclusion::Succeeded)
        );
        assert!(self.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd{activation})if activation==a)));
    }
}
