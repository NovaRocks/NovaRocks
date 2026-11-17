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

use super::support::*;
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, logical_port::AttemptAuthorization,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    LeaseValidFor, OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationRef,
};
use novarocks_types::{BackendProcessId, FrontendProcessId, NativeCompatibilityId};
use std::{num::NonZeroU64, time::Duration};
fn participation(index: u128) -> ParticipationRef {
    ParticipationRef::new(
        activation(1).execution(),
        FrontendProcessId::try_from_bytes(
            OperationIds::new(IdSeed::new(900, 100)).issue().to_bytes(),
        )
        .unwrap(),
        BackendProcessId::try_from_bytes(
            OperationIds::new(IdSeed::new(1000 + index, 100))
                .issue()
                .to_bytes(),
        )
        .unwrap(),
    )
}
#[test]
fn closing_authority_rejects_new_issues_but_settles_the_original_admission() {
    let p = participation(1);
    let mut s = start(LogicalOutputMode::CompletionOnly);
    s.first_attempt.participations = vec![p].into_boxed_slice();
    let mut f = Fixture::with_start(s);
    f.begin();
    let request = AcquireParticipationAdmissionTicket::new(
        OperationIds::new(IdSeed::new(300, 100)).issue(),
        p,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        AdmissionEpochCapability::try_from_bytes([4; 16]).unwrap(),
    );
    let token = AttemptRequestToken::new(NonZeroU64::new(1).unwrap());
    f.port(AttemptToLogicalBody::AuthorizationRequest {
        token,
        request: AttemptAuthorization::BeginAdmission(request),
    });
    let reply=f.take(|b|matches!(b,LogicalEffectBody::AttemptPort(p) if matches!(p.body,LogicalToAttemptBody::AuthorizationReply{..})));
    let LogicalEffectBody::AttemptPort(reply) = reply.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::AuthorizationReply {
        decision: Ok(AttemptAuthorizationDecision::AdmissionStarted(issue)),
        ..
    } = reply.body
    else {
        panic!("original admission reference expected")
    };
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::Abort{..}))));
    let receipt = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([5; 16]).unwrap(),
        p,
        request.valid_for(),
    );
    let token = AttemptRequestToken::new(NonZeroU64::new(2).unwrap());
    f.port(AttemptToLogicalBody::AuthorizationRequest {
        token,
        request: AttemptAuthorization::SettleAdmission {
            issue,
            participation: p,
            operation_id: request.envelope().operation_id(),
            outcome: Some(OperationOutcome::Accepted),
            receipt: Some(receipt),
            prior_transport_unknown: false,
        },
    });
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::AuthorizationReply{token:t,decision:Ok(AttemptAuthorizationDecision::AdmissionGranted)}if t==token))));
    let token = AttemptRequestToken::new(NonZeroU64::new(3).unwrap());
    f.port(AttemptToLogicalBody::AuthorizationRequest {
        token,
        request: AttemptAuthorization::BeginAdmission(request),
    });
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(&p.body,LogicalToAttemptBody::AuthorizationReply{token:t,decision:Err(_)}if *t==token))));
    assert!(
        !f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .all_participations_closed
    );
}
#[test]
fn a_large_uncontacted_roster_is_walked_under_budget_and_idle_does_not_self_wake() {
    let mut s = start(LogicalOutputMode::CompletionOnly);
    s.first_attempt.participations = (1..=257)
        .map(participation)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let mut f = Fixture::with_start(s);
    f.begin();
    f.budget = 2;
    let before = f.steps;
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    assert!(f.steps - before >= 257);
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .all_participations_closed
    );
    assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::Abort{..}))));
    assert_eq!(f.core.next_deadline(), None);
}

fn contacted() -> (Fixture, ParticipationRef) {
    let p = participation(1);
    let mut s = start(LogicalOutputMode::CompletionOnly);
    s.first_attempt.participations = vec![p].into_boxed_slice();
    let mut f = Fixture::with_start(s);
    f.begin();
    let request = AcquireParticipationAdmissionTicket::new(
        OperationIds::new(IdSeed::new(301, 100)).issue(),
        p,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        AdmissionEpochCapability::try_from_bytes([4; 16]).unwrap(),
    );
    f.port(AttemptToLogicalBody::AuthorizationRequest {
        token: AttemptRequestToken::new(NonZeroU64::new(99).unwrap()),
        request: AttemptAuthorization::BeginAdmission(request),
    });
    (f, p)
}
#[test]
fn registry_close_shortens_the_existing_remote_bound_without_claiming_local_exit() {
    let (mut f, _) = contacted();
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    let first = f
        .core
        .facts()
        .attempts()
        .next()
        .unwrap()
        .remote_deadline
        .unwrap();
    f.now = MonotonicInstant::from_origin(Duration::from_millis(20));
    assert!(f.now < first);
    f.send(LogicalInput::RegistryClose(RegistryClose::Shutdown));
    let attempt = f.core.facts().attempts().next().unwrap();
    assert_eq!(attempt.remote_deadline, Some(f.now));
    assert!(attempt.remote_tracking_ended);
    assert!(!attempt.all_participations_closed);
    assert!(!attempt.cleanup_complete);
    assert!(
        f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking { .. })
        )),
        "effects: {:#?}; next: {:?}",
        f.effects,
        f.core.next_deadline()
    );
}
#[test]
fn registry_close_before_the_first_cut_does_not_open_a_new_cleanup_window() {
    let (mut f, _) = contacted();
    f.send(LogicalInput::RegistryClose(RegistryClose::Shutdown));
    f.now = MonotonicInstant::from_origin(Duration::from_millis(20));
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::ServerShutdown,
    ));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().remote_deadline,
        Some(MonotonicInstant::ORIGIN)
    );
}
#[test]
fn buffered_positive_closure_is_not_overwritten_by_end_of_tracking() {
    let (mut f, p) = contacted();
    f.send_once(
        LogicalInput::Cancelled(
            novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
        ),
        2,
    );
    f.send_once(
        LogicalInput::RegistryParticipation {
            activation: activation(1),
            participation: p,
            convergence: RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced,
        },
        0,
    );
    f.now = MonotonicInstant::from_origin(Duration::from_millis(100));
    f.send_once(
        LogicalInput::RegistryParticipation {
            activation: activation(1),
            participation: p,
            convergence: RegistryParticipationConvergence::RemoteTrackingEnded,
        },
        0,
    );
    f.send(LogicalInput::Timer);
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .all_participations_closed
    );
    assert_eq!(f.core.facts().participation(activation(1),p).unwrap().closure,novarocks_logical_execution_protocol::facts::ParticipationClosureState::WorkerStoppedAndParticipationFenced);
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
    )));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking { .. })
    )));
}

#[path = "authorization/part3.rs"]
mod part3;

#[path = "authorization/part3_support.rs"]
pub(crate) mod part3_support;

#[test]
fn manifest_member_admission_resource_exhausted_is_retryable() {
    let p = participation(1);
    let mut original = start(LogicalOutputMode::CompletionOnly);
    original.first_attempt.participations = vec![p].into_boxed_slice();
    original.budgets.establish_per_participation = std::num::NonZeroUsize::new(3).unwrap();
    original.budgets.abort_per_participation = std::num::NonZeroUsize::new(5).unwrap();
    let mut f = Fixture::with_start(original);
    f.begin();
    let request = AcquireParticipationAdmissionTicket::new(
        OperationIds::new(IdSeed::new(3301, 100)).issue(),
        p,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        AdmissionEpochCapability::try_from_bytes([4; 16]).unwrap(),
    );
    let token = AttemptRequestToken::new(NonZeroU64::new(1).unwrap());
    f.port(AttemptToLogicalBody::AuthorizationRequest {
        token,
        request: AttemptAuthorization::BeginAdmission(request),
    });
    let issued = f.take(|b| {
        matches!(b, LogicalEffectBody::AttemptPort(p)
        if matches!(&p.body, LogicalToAttemptBody::AuthorizationReply { token: found, .. }
            if *found == token))
    });
    let LogicalEffectBody::AttemptPort(issued) = issued.body else {
        unreachable!()
    };
    assert_eq!(issued.activation, activation(1));
    let LogicalToAttemptBody::AuthorizationReply {
        decision: Ok(AttemptAuthorizationDecision::AdmissionStarted(issue)),
        ..
    } = issued.body
    else {
        panic!("the same logical activation must authorize its manifest member");
    };
    let settled = AttemptRequestToken::new(NonZeroU64::new(2).unwrap());
    f.port(AttemptToLogicalBody::AuthorizationRequest {
        token: settled,
        request: AttemptAuthorization::SettleAdmission {
            issue,
            participation: p,
            operation_id: request.envelope().operation_id(),
            outcome: Some(OperationOutcome::ResourceExhausted),
            receipt: None,
            prior_transport_unknown: false,
        },
    });
    let reply = f.take(|b| {
        matches!(b, LogicalEffectBody::AttemptPort(p)
        if matches!(&p.body, LogicalToAttemptBody::AuthorizationReply { token: found, .. }
            if *found == settled))
    });
    let LogicalEffectBody::AttemptPort(reply) = reply.body else {
        unreachable!()
    };
    assert_eq!(reply.activation, activation(1));
    assert!(
        matches!(reply.body, LogicalToAttemptBody::AuthorizationReply {
        token, decision: Ok(AttemptAuthorizationDecision::AdmissionRetryable),
    } if token == settled)
    );
    assert_eq!(f.core.facts().conclusion(), None);
}
