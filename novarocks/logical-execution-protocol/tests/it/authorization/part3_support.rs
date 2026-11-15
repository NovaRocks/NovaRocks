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

//! Public-contract fixtures for the two part-three behavior axes. No legacy
//! reducer, permit, host owner or private core state is manufactured here.
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, AttemptAuthorizationRef, AttemptIssueSettlement,
    ExecutionFailure,
    logical_port::{AttemptAuthorization, AttemptAuthorizationFailure},
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialDomainId,
    CredentialEpoch, CredentialUpdate, EstablishParticipation, LeaseValidFor, OperationOutcome,
    ParticipationAdmissionTicketReceipt, ParticipationRef, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;
use std::{num::NonZeroU64, sync::Arc, time::Duration};

pub(crate) fn operation(tag: u64) -> TaskOperationId {
    OperationIds::new(IdSeed::new(u128::from(tag) + 1000, 100)).issue()
}
pub(crate) fn roster(count: u8) -> Vec<ParticipationRef> {
    (1..=count)
        .map(|i| super::participation(u128::from(i)))
        .collect()
}
pub(crate) fn with_roster(count: u8) -> (Fixture, Vec<ParticipationRef>) {
    let participants = roster(count);
    let mut initial = start(LogicalOutputMode::CompletionOnly);
    initial.first_attempt.participations = participants.clone().into_boxed_slice();
    let mut f = Fixture::with_start(initial);
    f.begin();
    (f, participants)
}
pub(crate) fn request(
    p: ParticipationRef,
    tag: u8,
    valid_for: Duration,
) -> AcquireParticipationAdmissionTicket {
    AcquireParticipationAdmissionTicket::new(
        operation(u64::from(tag)),
        p,
        LeaseValidFor::new(valid_for).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        AdmissionEpochCapability::try_from_bytes([4; 16]).unwrap(),
    )
}
pub(crate) fn authorize(
    f: &mut Fixture,
    request: AttemptAuthorization,
) -> Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure> {
    let token = AttemptRequestToken::new(NonZeroU64::new(10_000 + f.steps as u64).unwrap());
    f.port(AttemptToLogicalBody::AuthorizationRequest { token, request });
    let reply = f.take(|body| matches!(body, LogicalEffectBody::AttemptPort(p) if matches!(&p.body, LogicalToAttemptBody::AuthorizationReply { token: found, .. } if *found == token)));
    let LogicalEffectBody::AttemptPort(reply) = reply.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::AuthorizationReply {
        token: found,
        decision,
    } = reply.body
    else {
        unreachable!()
    };
    assert_eq!(found, token);
    assert_eq!(reply.activation, activation(1));
    decision
}
pub(crate) fn begin(
    f: &mut Fixture,
    request: AcquireParticipationAdmissionTicket,
) -> AttemptAuthorizationRef {
    let AttemptAuthorizationDecision::AdmissionStarted(reference) =
        authorize(f, AttemptAuthorization::BeginAdmission(request)).unwrap()
    else {
        panic!("expected original admission identity")
    };
    reference
}
pub(crate) fn grant(
    f: &mut Fixture,
    p: ParticipationRef,
    tag: u8,
    valid_for: Duration,
) -> ParticipationAdmissionTicketReceipt {
    let request = request(p, tag, valid_for);
    let issue = begin(f, request);
    let receipt = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
        p,
        request.valid_for(),
    );
    assert_eq!(
        authorize(
            f,
            AttemptAuthorization::SettleAdmission {
                issue,
                participation: p,
                operation_id: request.envelope().operation_id(),
                outcome: Some(OperationOutcome::Accepted),
                receipt: Some(receipt),
                prior_transport_unknown: false
            }
        )
        .unwrap(),
        AttemptAuthorizationDecision::AdmissionGranted
    );
    receipt
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
pub(crate) fn issue(
    f: &mut Fixture,
    p: ParticipationRef,
    receipt: ParticipationAdmissionTicketReceipt,
    tag: u8,
) -> (AttemptAuthorizationRef, TaskOperationId) {
    let metadata = |offset| {
        Arc::new(Metadata(ContentFingerprint::from_bytes([tag + offset; 16])))
            as Arc<dyn CodecOwnedContent>
    };
    let original = EstablishParticipation::new(
        operation(100 + u64::from(tag)),
        p,
        receipt.ticket_id(),
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
    let op = original.envelope().operation_id();
    let AttemptAuthorizationDecision::EstablishAuthorized(reference) = authorize(
        f,
        AttemptAuthorization::Establish {
            participation: p,
            operation_id: op,
            ticket_id: receipt.ticket_id(),
            semantics: original.semantic_identity(),
            native_compatibility_id: NativeCompatibilityId::new([3; 32]),
        },
    )
    .unwrap() else {
        panic!("expected original Establish identity")
    };
    (reference, op)
}
pub(crate) fn settle(
    f: &mut Fixture,
    reference: AttemptAuthorizationRef,
    p: ParticipationRef,
    operation_id: TaskOperationId,
    settlement: AttemptIssueSettlement,
) -> Result<(), ExecutionFailure> {
    let token = AttemptRequestToken::new(NonZeroU64::new(100_000 + f.steps as u64).unwrap());
    f.port(AttemptToLogicalBody::EstablishSettlement {
        token,
        authorization: reference,
        participation: p,
        operation_id,
        settlement,
    });
    let reply = f.take(|body| matches!(body,LogicalEffectBody::AttemptPort(p) if matches!(&p.body,LogicalToAttemptBody::EstablishSettlementReply{token:found,..} if *found==token)));
    let LogicalEffectBody::AttemptPort(reply) = reply.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::EstablishSettlementReply {
        authorization,
        result,
        ..
    } = reply.body
    else {
        unreachable!()
    };
    assert_eq!(authorization, reference);
    result
}
pub(crate) fn no_finish(f: &Fixture) {
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(
            GovernanceEffect::ResolveObligation { .. } | GovernanceEffect::CompleteWorkOwner
        ) | LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
    )));
    assert!(!f.core.facts().protocol_join_ready());
}
pub(crate) fn ack_housekeeping(f: &mut Fixture) {
    while let Some(index) = f.effects.iter().position(|e| {
        matches!(
            e.body,
            LogicalEffectBody::Consumer(
                ConsumerEffect::PublishFailure(_) | ConsumerEffect::PublishCompletion
            ) | LogicalEffectBody::Native(
                NativeEffect::RecordTerminalCut { .. } | NativeEffect::RetirePreparation
            )
        )
    }) {
        let effect = f.effects.remove(index);
        let result = match effect.body {
            LogicalEffectBody::Consumer(_) => ActionResult::Consumer(ConsumerFact::Applied),
            _ => ActionResult::Native(NativeFact::Applied),
        };
        f.ack(effect.action, result);
    }
}
