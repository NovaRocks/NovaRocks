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

//! Exact residual assertions from the historical non-P13 component inventory.
//! Stateful cases exercise the public Core; no old reducer or permit is built.
use crate::authorization::part3_support::{
    authorize, begin, grant, issue, request, roster, settle, with_roster,
};
use crate::recovery::contract::{
    RecoveryFixture, admission, at, backend_failure, roster as recovery_roster,
};
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, AttemptFailureClass, AttemptIssueSettlement, ExecutionFailure,
    ExecutionFailureSite,
    logical_port::{AbortIssueSettlement, AttemptAuthorization, AttemptAuthorizationFailure},
};
use novarocks_execution_contract::{
    OperationOutcome, ParticipationRef,
    failure::{FailureClass, FailureEvidence, FailureReason, domain},
};
use novarocks_logical_execution_protocol::{
    CoreFault,
    cancel::{LogicalCancellationReason, cancellation_decision},
    facts::{AbortParticipationIssueState, CleanupPath, ParticipationClosureState},
};
use std::{
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    time::Duration,
};

fn rejected(
    result: Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure>,
) -> ExecutionFailure {
    match result {
        Err(AttemptAuthorizationFailure::Rejected(error)) => error,
        other => panic!("expected the original ordinary authorization refusal: {other:?}"),
    }
}
fn has_abort(f: &Fixture, p: ParticipationRef) -> bool {
    f.effects.iter().any(|e| matches!(&e.body, LogicalEffectBody::AttemptPort(m) if matches!(&m.body, LogicalToAttemptBody::Abort { request, .. } if request.participation() == p)))
}
fn internal_decoded_nonempty() -> (RecoveryFixture, InternalEndReceiptId) {
    let mut r = RecoveryFixture::new(LogicalOutputMode::InternalResultStream);
    r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(3).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(false),
    )
    .unwrap();
    r.send_port(
        activation(1),
        AttemptToLogicalBody::RootStatus(Box::new(
            novarocks_coordination_contract::AttemptRootProjection {
                status,
                failure: novarocks_coordination_contract::AttemptRootFailure::None,
            },
        )),
    );
    r.f.internal(InternalRequest::RegisterSource {
        root: root(1),
        plan: PLAN,
        supervised_exit: true,
    });
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(true),
        },
    );
    assert!(r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Relay(RelayEffect::Reply {
            response: RelayResponse::DeliveryGranted,
            ..
        })
    )));
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
    )));
    // Honest Internal delivery has no client visibility request per packet.
    // This nonempty summary is the original relay fact, not a physical payload.
    r.relay(
        activation(1),
        RelayRequest::EndConsumed {
            root: root(1),
            end: end(1, 27),
            accepted_packets: 1,
            delivered_through: Some(0),
        },
    );
    let receipt = InternalEndReceiptId(NonZeroU64::new(17).unwrap());
    let token = r.f.internal(InternalRequest::EndDecoded {
        receipt,
        end: end(1, 27),
    });
    assert!(r.f.effects.iter().any(|e| matches!(&e.body, LogicalEffectBody::Consumer(ConsumerEffect::InternalReply { token: actual, response: InternalResponse::Applied, .. }) if *actual == token)));
    assert!(!r.f.core.facts().output_visible());
    assert!(r.f.core.facts().ready_cut().is_none());
    assert_eq!(r.f.core.facts().conclusion(), None);
    (r, receipt)
}

#[test]
fn internal_nonempty_decoded_end_can_recover_before_ready_but_never_after_its_cut() {
    let (mut r, _) = internal_decoded_nonempty();
    let first = backend_failure(1, "original failure after nonempty Internal EndDecoded");
    r.fail(activation(1), first.clone());
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(!r.f.core.facts().output_visible());
    assert!(r.f.core.facts().ready_cut().is_none());
    let (dispatch, _) = r.prepare(recovery_roster(2));
    let grants = dispatch
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
        .collect();
    r.f.now = at(10);
    r.qualify(&dispatch, grants, at(5));
    let (b, inherited) = r.activate(&dispatch);
    assert_eq!(inherited.len(), 2);
    assert_eq!(r.f.core.facts().current_activation(), Some(b));
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap()
            .accepted_failure,
        Some(first.input())
    );
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(!r.f.core.facts().output_visible());

    let (mut accepted, receipt) = internal_decoded_nonempty();
    accepted.f.finished_root();
    accepted.f.internal(InternalRequest::AcceptReady {
        root: root(1),
        plan: PLAN,
        receipt,
        end: end(1, 27),
    });
    accepted.f.seal();
    assert_eq!(
        accepted.f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert!(accepted.f.core.facts().output_visible());
    let cut = accepted.f.core.facts().ready_cut().unwrap();
    assert_eq!(cut.end, end(1, 27));
    accepted.fail(
        activation(1),
        backend_failure(1, "late original failure after accepted Internal Ready"),
    );
    assert_eq!(
        accepted.f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(accepted.f.core.facts().ready_cut(), Some(cut));
    assert!(!accepted.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
    )));
}

#[test]
fn owner_failed_admission_rejects_a_fresh_operation_without_losing_abort_responsibility() {
    let (mut f, participants) = with_roster(1);
    let p = participants[0];
    let req = request(p, 1, Duration::from_secs(1));
    let original = begin(&mut f, req);
    let owner_failed = rejected(authorize(
        &mut f,
        AttemptAuthorization::SettleAdmission {
            issue: original,
            participation: p,
            operation_id: req.envelope().operation_id(),
            outcome: Some(OperationOutcome::OwnerFailed),
            receipt: None,
            prior_transport_unknown: false,
        },
    ));
    let replay = rejected(authorize(&mut f, AttemptAuthorization::BeginAdmission(req)));
    assert_eq!(replay.input().fact().reason().code(), 79);
    assert_eq!(
        replay.input().fact().evidence(),
        FailureEvidence::StopObservation
    );
    let fresh = request(p, 2, Duration::from_secs(1));
    assert_ne!(
        req.envelope().operation_id(),
        fresh.envelope().operation_id()
    );
    let conflicting = rejected(authorize(
        &mut f,
        AttemptAuthorization::BeginAdmission(fresh),
    ));
    assert_eq!(conflicting.input().fact().class(), FailureClass::Invariant);
    assert_eq!(
        conflicting.input().fact().reason(),
        FailureReason::new(domain::QUERY, NonZeroU16::new(74).unwrap())
    );
    assert_eq!(
        conflicting.input().site(),
        ExecutionFailureSite::Coordinator
    );
    assert_eq!(
        conflicting.input().detail().as_str(),
        "participation admission was issued with conflicting immutable facts"
    );
    assert_eq!(f.core.facts().conclusion(), None);
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(owner_failed.error()));
    assert!(
        !f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .all_participations_closed
    );
    assert!(has_abort(&f, p));
    assert!(!f.core.facts().protocol_join_ready());
}

#[test]
fn original_unknown_worker_outcome_matrix_survives_authority_close_and_preserves_success_gate() {
    for outcome in [
        OperationOutcome::Accepted,
        OperationOutcome::ParticipationConflict,
    ] {
        for close_first in [false, true] {
            let (mut f, participants) = with_roster(1);
            let p = participants[0];
            let ticket = grant(&mut f, p, 1, Duration::from_secs(1));
            let (original, operation_id) = issue(&mut f, p, ticket, 1);
            settle(
                &mut f,
                original,
                p,
                operation_id,
                AttemptIssueSettlement::TransportOwned,
            )
            .unwrap();
            settle(
                &mut f,
                original,
                p,
                operation_id,
                AttemptIssueSettlement::TransportUnknown,
            )
            .unwrap();
            if close_first {
                f.send(LogicalInput::Cancelled(
                    LogicalCancellationReason::Requested,
                ));
                assert!(!f.core.facts().attempts().next().unwrap().authority_open);
                let denied = rejected(authorize(
                    &mut f,
                    AttemptAuthorization::ReauthorizeEstablish {
                        participation: p,
                        operation_id,
                    },
                ));
                assert_eq!(denied.input().fact().reason().code(), 79);
            }
            // A late original Worker fact needs no new authorization reference.
            settle(
                &mut f,
                original,
                p,
                operation_id,
                AttemptIssueSettlement::Worker(outcome),
            )
            .unwrap();
            f.port(AttemptToLogicalBody::AttemptTerminal(
                AttemptTerminal::Completed,
            ));
            if close_first {
                assert_eq!(
                    f.core.facts().conclusion(),
                    Some(LogicalConclusion::Cancelled)
                );
                assert_eq!(
                    f.core.facts().failure(),
                    Some(&cancellation_decision(LogicalCancellationReason::Requested).failure)
                );
                assert!(has_abort(&f, p));
            } else if outcome == OperationOutcome::Accepted {
                assert_eq!(
                    f.core.facts().conclusion(),
                    Some(LogicalConclusion::Succeeded)
                );
                assert!(f.core.facts().failure().is_none());
            } else {
                assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
                let input = f
                    .core
                    .facts()
                    .failure()
                    .unwrap()
                    .execution_failure()
                    .unwrap();
                assert_eq!(input.fact().reason().code(), 83);
                assert_eq!(
                    input.detail().as_str(),
                    "Worker rejected a required Establish operation"
                );
            }
            assert!(
                !f.core
                    .facts()
                    .attempts()
                    .next()
                    .unwrap()
                    .all_participations_closed
            );
            assert!(!f.core.facts().protocol_join_ready());
        }
    }
}

#[test]
fn actual_establish_budget_refusal_has_no_invented_measurement_metadata() {
    let (mut f, participants) = with_roster(1);
    let p = participants[0];
    let ticket = grant(&mut f, p, 1, Duration::from_secs(1));
    let (mut reference, operation_id) = issue(&mut f, p, ticket, 1);
    for replay in [false, true] {
        if replay {
            let AttemptAuthorizationDecision::EstablishAuthorized(next) = authorize(
                &mut f,
                AttemptAuthorization::ReauthorizeEstablish {
                    participation: p,
                    operation_id,
                },
            )
            .unwrap() else {
                panic!("one exact replay expected")
            };
            reference = next;
        }
        settle(
            &mut f,
            reference,
            p,
            operation_id,
            AttemptIssueSettlement::TransportOwned,
        )
        .unwrap();
        settle(
            &mut f,
            reference,
            p,
            operation_id,
            AttemptIssueSettlement::TransportUnknown,
        )
        .unwrap();
    }
    let Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(error)) = authorize(
        &mut f,
        AttemptAuthorization::ReauthorizeEstablish {
            participation: p,
            operation_id,
        },
    ) else {
        panic!("dedicated original budget verdict expected")
    };
    assert_eq!(error.input().fact().class(), FailureClass::Resource);
    assert_eq!(error.input().fact().resource(), None);
    assert_eq!(error.input().fact().representation(), None);
    assert_eq!(
        error.input().attempt_class(),
        AttemptFailureClass::ResourceGovernance
    );
    assert_eq!(error.input().site(), ExecutionFailureSite::Coordinator);
    assert_eq!(f.core.facts().conclusion(), None);
}

#[test]
fn definitely_unsent_expiry_keeps_the_single_original_control_error_projection() {
    let (mut f, participants) = with_roster(1);
    let p = participants[0];
    let ticket = grant(&mut f, p, 1, Duration::from_millis(10));
    let (reference, operation_id) = issue(&mut f, p, ticket, 1);
    settle(
        &mut f,
        reference,
        p,
        operation_id,
        AttemptIssueSettlement::DefinitelyUnsent,
    )
    .unwrap();
    f.now = at(10);
    let expired = rejected(authorize(
        &mut f,
        AttemptAuthorization::ReauthorizeEstablish {
            participation: p,
            operation_id,
        },
    ));
    let original = expired.input();
    assert_eq!(original.fact().class(), FailureClass::Control);
    assert_eq!(original.fact().evidence(), FailureEvidence::Originating);
    assert_eq!(
        original.fact().reason(),
        FailureReason::new(domain::QUERY, NonZeroU16::new(82).unwrap())
    );
    assert_eq!(original.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        original.attempt_class(),
        AttemptFailureClass::ExecutionFailure
    );
    assert_eq!(
        original.detail().as_str(),
        "admission ticket expired before Establish issue authorization"
    );
    assert_eq!(expired.error().execution_failure(), Some(original));
    assert_eq!(f.core.facts().conclusion(), None);
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(expired.clone()),
    ));
    assert_eq!(f.core.facts().failure(), Some(expired.error()));
    assert!(has_abort(&f, p));
}

#[test]
fn exact_process_replacement_closes_the_original_authorization_exhausted_abort_without_fake_exit() {
    let participants = roster(1);
    let p = participants[0];
    let mut initial = start(LogicalOutputMode::CompletionOnly);
    initial.first_attempt.participations = participants.into_boxed_slice();
    initial.budgets.abort_per_participation = NonZeroUsize::MIN;
    let mut f = Fixture::with_start(initial);
    f.begin();
    grant(&mut f, p, 1, Duration::from_secs(1));
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    let abort = f.take(|body| matches!(body, LogicalEffectBody::AttemptPort(m) if matches!(&m.body, LogicalToAttemptBody::Abort { request, .. } if request.participation() == p)));
    let LogicalEffectBody::AttemptPort(message) = abort.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::Abort { action, request } = message.body else {
        unreachable!()
    };
    f.port(AttemptToLogicalBody::AbortSettlement {
        abort_action: action,
        participation: p,
        operation_id: request.envelope().operation_id(),
        settlement: AbortIssueSettlement::TransportUnknown,
    });
    f.now = at(50);
    f.send(LogicalInput::Timer);
    let exhausted = f.core.facts().participation(activation(1), p).unwrap();
    assert_eq!(
        exhausted.closure,
        ParticipationClosureState::AbortIssueExhausted
    );
    assert_eq!(
        exhausted.abort_issue_state,
        Some(AbortParticipationIssueState::AuthorizationExhausted)
    );
    assert_eq!(exhausted.abort_authorizations, 1);
    assert_eq!(
        exhausted.abort_operation,
        Some(request.envelope().operation_id())
    );
    assert!(!has_abort(&f, p));
    #[derive(Default)]
    struct Capture(Vec<LogicalEffect>);
    impl EffectSink for Capture {
        fn emit(&mut self, effect: LogicalEffect) {
            self.0.push(effect);
        }
    }
    let mut out = Capture::default();
    assert_eq!(
        f.core.handle(
            f.now,
            LogicalInput::RegistryParticipation {
                activation: activation(1),
                participation: roster(2)[1],
                convergence: RegistryParticipationConvergence::WorkerProcessReplaced
            },
            &mut Budget(32),
            &mut out
        ),
        Err(CoreFault::ForeignExecution)
    );
    assert!(out.0.is_empty());
    assert_eq!(
        f.core.facts().participation(activation(1), p),
        Some(exhausted)
    );
    f.send(LogicalInput::RegistryParticipation {
        activation: activation(1),
        participation: p,
        convergence: RegistryParticipationConvergence::WorkerProcessReplaced,
    });
    let closed = f.core.facts().participation(activation(1), p).unwrap();
    assert_eq!(
        closed.closure,
        ParticipationClosureState::WorkerProcessReplaced
    );
    assert_eq!(closed.abort_operation, exhausted.abort_operation);
    assert_eq!(closed.abort_authorizations, 1);
    let attempt = f.core.facts().attempts().next().unwrap();
    assert!(attempt.all_participations_closed);
    assert_eq!(attempt.cleanup_path, Some(CleanupPath::ResolveThenFinish));
    assert!(!attempt.cleanup_complete);
    assert!(!f.core.facts().protocol_join_ready());
    assert!(f.effects.iter().any(|e| matches!(e.body, LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { activation: a, .. }) if a == activation(1))));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking { .. })
            | LogicalEffectBody::Native(NativeEffect::AbandonResources { .. })
    )));
}
