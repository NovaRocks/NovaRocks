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

use super::part3_support::*;
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, AttemptIssueSettlement,
    logical_port::{AttemptAuthorization, AttemptAuthorizationFailure},
};
use novarocks_execution_contract::OperationOutcome;
use novarocks_failure_contract::FailureClass;
use novarocks_logical_execution_protocol::facts::ParticipationClosureState;
use std::time::Duration;

#[test]
fn unknown_establish_replay_keeps_exact_operation_after_ticket_expiry_and_settles_idempotently() {
    let (mut f, p) = with_roster(1);
    let p = p[0];
    let ticket = grant(&mut f, p, 1, Duration::from_millis(10));
    let (original, op) = issue(&mut f, p, ticket, 1);
    settle(
        &mut f,
        original,
        p,
        op,
        AttemptIssueSettlement::TransportOwned,
    )
    .unwrap();
    settle(
        &mut f,
        original,
        p,
        op,
        AttemptIssueSettlement::TransportUnknown,
    )
    .unwrap();
    f.now = MonotonicInstant::from_origin(Duration::from_millis(20));
    let AttemptAuthorizationDecision::EstablishAuthorized(replay) = authorize(
        &mut f,
        AttemptAuthorization::ReauthorizeEstablish {
            participation: p,
            operation_id: op,
        },
    )
    .unwrap() else {
        panic!("expected replay")
    };
    assert_ne!(original, replay);
    settle(
        &mut f,
        replay,
        p,
        op,
        AttemptIssueSettlement::TransportOwned,
    )
    .unwrap();
    assert!(
        settle(
            &mut f,
            replay,
            p,
            operation(999),
            AttemptIssueSettlement::Worker(OperationOutcome::Idempotent)
        )
        .is_err()
    );
    settle(
        &mut f,
        replay,
        p,
        op,
        AttemptIssueSettlement::Worker(OperationOutcome::Idempotent),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    // A late old transport verdict does not retract the accepted Worker fact.
    settle(
        &mut f,
        original,
        p,
        op,
        AttemptIssueSettlement::TransportUnknown,
    )
    .unwrap();
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p) if matches!(p.body,LogicalToAttemptBody::Abort{..}|LogicalToAttemptBody::Stop{..}))));
}

#[test]
fn unknown_establish_budget_exhaustion_keeps_the_dedicated_typed_verdict_and_abort_debt() {
    let (mut f, participants) = with_roster(1);
    let p = participants[0];
    let ticket = grant(&mut f, p, 1, Duration::from_secs(1));
    let (first, op) = issue(&mut f, p, ticket, 1);
    settle(&mut f, first, p, op, AttemptIssueSettlement::TransportOwned).unwrap();
    settle(
        &mut f,
        first,
        p,
        op,
        AttemptIssueSettlement::TransportUnknown,
    )
    .unwrap();
    let AttemptAuthorizationDecision::EstablishAuthorized(second) = authorize(
        &mut f,
        AttemptAuthorization::ReauthorizeEstablish {
            participation: p,
            operation_id: op,
        },
    )
    .unwrap() else {
        panic!("expected replay")
    };
    settle(
        &mut f,
        second,
        p,
        op,
        AttemptIssueSettlement::TransportOwned,
    )
    .unwrap();
    settle(
        &mut f,
        second,
        p,
        op,
        AttemptIssueSettlement::TransportUnknown,
    )
    .unwrap();
    let failure = match authorize(
        &mut f,
        AttemptAuthorization::ReauthorizeEstablish {
            participation: p,
            operation_id: op,
        },
    ) {
        Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(original)) => original,
        other => panic!("expected exact Establish budget verdict: {other:?}"),
    };
    assert_eq!(failure.input().fact().class(), FailureClass::Resource);
    let expected = failure.error().clone();
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(failure),
    ));
    assert_eq!(f.core.facts().failure(), Some(&expected));
    assert!(
        !f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .all_participations_closed
    );
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p) if matches!(p.body,LogicalToAttemptBody::Abort{..}))));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
    )));
}

#[test]
fn pre_worker_establish_refusal_keeps_granted_participation_in_terminal_abort_set() {
    let (mut f, participants) = with_roster(1);
    let p = participants[0];
    let ticket = grant(&mut f, p, 1, Duration::from_secs(1));
    let (reference, op) = issue(&mut f, p, ticket, 1);
    settle(
        &mut f,
        reference,
        p,
        op,
        AttemptIssueSettlement::TransportOwned,
    )
    .unwrap();
    settle(
        &mut f,
        reference,
        p,
        op,
        AttemptIssueSettlement::PreWorkerRejected,
    )
    .unwrap();
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m) if matches!(&m.body,LogicalToAttemptBody::Abort{request,..} if request.participation()==p))));
    no_finish(&f);
}

#[test]
fn admission_refusal_after_unknown_or_owner_failure_retains_abort_debt() {
    for (outcome, prior_unknown) in [(None, true), (Some(OperationOutcome::OwnerFailed), false)] {
        let (mut f, participants) = with_roster(1);
        let p = participants[0];
        let request = request(p, 1, Duration::from_secs(1));
        let issue = begin(&mut f, request);
        assert!(
            authorize(
                &mut f,
                AttemptAuthorization::SettleAdmission {
                    issue,
                    participation: p,
                    operation_id: request.envelope().operation_id(),
                    outcome,
                    receipt: None,
                    prior_transport_unknown: prior_unknown
                }
            )
            .is_err()
        );
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m) if matches!(&m.body,LogicalToAttemptBody::Abort{request,..} if request.participation()==p))));
        no_finish(&f);
    }
}

#[test]
fn g9b3_1089_terminal_settlement_error_does_not_consume_independent_later_verdicts() {
    for reverse in [false, true] {
        let (mut f, participants) = with_roster(2);
        let mut held = Vec::new();
        for (i, p) in participants.iter().copied().enumerate() {
            let tag = i as u8 + 1;
            let ticket = grant(&mut f, p, tag, Duration::from_secs(1));
            let (reference, op) = issue(&mut f, p, ticket, tag);
            settle(
                &mut f,
                reference,
                p,
                op,
                AttemptIssueSettlement::TransportOwned,
            )
            .unwrap();
            held.push((p, reference, op));
        }
        if reverse {
            held.reverse();
        }
        let (first, reference, op) = held[0];
        settle(
            &mut f,
            reference,
            first,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
        f.send(LogicalInput::Cancelled(
            novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
        ));
        let failure = settle(
            &mut f,
            reference,
            first,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::IdentityMismatch),
        )
        .unwrap_err();
        assert!(!failure.input().detail().as_str().is_empty());
        let (second, reference, op) = held[1];
        settle(
            &mut f,
            reference,
            second,
            op,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::Cancelled)
        );
        assert!(f.core.facts().supervision_failure().is_none());
        no_finish(&f);
        for p in participants {
            f.send(LogicalInput::RegistryParticipation {
                activation: activation(1),
                participation: p,
                convergence: RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced,
            });
        }
        assert!(f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
        )));
        assert!(!f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking { .. })
        )));
    }
}

#[test]
fn terminal_ack_drain_refuses_unissued_admission_but_settles_the_original_issued_ticket() {
    let (mut f, participants) = with_roster(2);
    let original = request(participants[0], 1, Duration::from_secs(1));
    let issue = begin(&mut f, original);
    let queued = request(participants[1], 2, Duration::from_secs(1));
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    assert!(authorize(&mut f, AttemptAuthorization::BeginAdmission(queued)).is_err());
    let receipt = novarocks_execution_contract::ParticipationAdmissionTicketReceipt::new(
        novarocks_execution_contract::AdmissionTicketId::try_from_bytes([1; 16]).unwrap(),
        participants[0],
        original.valid_for(),
    );
    assert_eq!(
        authorize(
            &mut f,
            AttemptAuthorization::SettleAdmission {
                issue,
                participation: participants[0],
                operation_id: original.envelope().operation_id(),
                outcome: Some(OperationOutcome::Accepted),
                receipt: Some(receipt),
                prior_transport_unknown: false
            }
        )
        .unwrap(),
        AttemptAuthorizationDecision::AdmissionGranted
    );
    assert_eq!(
        f.core
            .facts()
            .participation(activation(1), participants[1])
            .unwrap()
            .closure,
        ParticipationClosureState::NoRemoteResponsibility
    );
    assert_ne!(
        f.core
            .facts()
            .participation(activation(1), participants[0])
            .unwrap()
            .closure,
        ParticipationClosureState::NoRemoteResponsibility
    );
    no_finish(&f);
}

#[test]
fn nonempty_completion_requires_each_exact_establish_worker_settlement() {
    for complete in [false, true] {
        let (mut f, participants) = with_roster(2);
        for (i, p) in participants.iter().copied().enumerate() {
            let tag = i as u8 + 1;
            let ticket = grant(&mut f, p, tag, Duration::from_secs(1));
            let (reference, op) = issue(&mut f, p, ticket, tag);
            settle(
                &mut f,
                reference,
                p,
                op,
                AttemptIssueSettlement::TransportOwned,
            )
            .unwrap();
            if i == 0 || complete {
                settle(
                    &mut f,
                    reference,
                    p,
                    op,
                    AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
                )
                .unwrap();
            }
        }
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
        assert_eq!(
            f.core.facts().conclusion(),
            Some(if complete {
                LogicalConclusion::Succeeded
            } else {
                LogicalConclusion::Failed
            })
        );
        assert_eq!(
            f.effects.iter().any(|e| matches!(
                e.body,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
            )),
            complete
        );
        if complete {
            assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m) if matches!(m.body,LogicalToAttemptBody::Abort{..}|LogicalToAttemptBody::Stop{..}))));
        }
    }
}

#[test]
fn nonempty_completion_refuses_never_issued_and_explicitly_refused_required_admission() {
    for refusal in [false, true] {
        let (mut f, participants) = with_roster(1);
        let p = participants[0];
        if refusal {
            let request = request(p, 1, Duration::from_secs(1));
            let issue = begin(&mut f, request);
            assert!(
                authorize(
                    &mut f,
                    AttemptAuthorization::SettleAdmission {
                        issue,
                        participation: p,
                        operation_id: request.envelope().operation_id(),
                        outcome: None,
                        receipt: None,
                        prior_transport_unknown: false
                    }
                )
                .is_err()
            );
        }
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert!(!f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )));
        assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m) if matches!(m.body,LogicalToAttemptBody::Abort{..}))));
        assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
    }
}

fn observation_query(
    f: &mut Fixture,
    refusal: novarocks_coordination_contract::ExecutionFailure,
) -> (AttemptRequestToken, bool) {
    let token =
        AttemptRequestToken::new(std::num::NonZeroU64::new(1_000_000 + f.steps as u64).unwrap());
    f.port(AttemptToLogicalBody::PreReadyReplacementQuery { token, refusal });
    let reply=f.take(|body|matches!(body,LogicalEffectBody::AttemptPort(message)if matches!(message.body,LogicalToAttemptBody::PreReadyReplacementReply{token:found,..}if found==token)));
    let LogicalEffectBody::AttemptPort(reply) = reply.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::PreReadyReplacementReply {
        token: found,
        may_observe,
    } = reply.body
    else {
        unreachable!()
    };
    assert_eq!(found, token);
    (token, may_observe)
}
fn observation_fixture(
    mode: RecoveryMode,
    observed: bool,
) -> (
    Fixture,
    novarocks_execution_contract::ParticipationRef,
    Option<(
        novarocks_coordination_contract::AttemptAuthorizationRef,
        novarocks_execution_contract::TaskOperationId,
    )>,
    novarocks_coordination_contract::ExecutionFailure,
) {
    use novarocks_coordination_contract::{
        ExecutionFailure, ExecutionFailureInput, ExecutionFailureSite,
        TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
    };
    use novarocks_execution_contract::{OperationKind, SafeDetail};
    use novarocks_failure_contract::FailureFact;
    let p = roster(1)[0];
    let mut initial = start(LogicalOutputMode::CompletionOnly);
    initial.policy.recovery = mode;
    initial.budgets.replacement_reservation_valid_for = Some(Duration::from_millis(50));
    initial.first_attempt.participations = vec![p].into_boxed_slice();
    let mut f = Fixture::with_start(initial);
    f.begin();
    let ticket = grant(&mut f, p, 1, Duration::from_secs(1));
    // An authoritative reply itself crosses the conservative send boundary.
    // The negative case genuinely has no parent Establish authorization.
    let issued = if observed {
        let (reference, op) = issue(&mut f, p, ticket, 1);
        settle(
            &mut f,
            reference,
            p,
            op,
            AttemptIssueSettlement::TransportUnknown,
        )
        .unwrap();
        Some((reference, op))
    } else {
        None
    };
    let input = ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("original exact CreateTask process refusal"),
        ExecutionFailureSite::Operation {
            participation: p,
            operation_id: operation(7000),
            kind: OperationKind::CreateTask,
        },
    );
    (f, p, issued, ExecutionFailure::from_input(input))
}

#[test]
fn pre_ready_query_requires_current_recovery_policy_and_observed_exact_parent() {
    for (mode, observed, expected) in [
        (RecoveryMode::RestartAttemptBeforeVisibility, true, true),
        (RecoveryMode::RestartAttemptBeforeVisibility, false, false),
        (RecoveryMode::NoRecovery, true, false),
    ] {
        let (mut f, _, _, refusal) = observation_fixture(mode, observed);
        let original = refusal.input().clone();
        let (_, allowed) = observation_query(&mut f, refusal.clone());
        assert_eq!(allowed, expected);
        assert_eq!(refusal.input(), &original);
        assert_eq!(
            refusal.class(),
            novarocks_coordination_contract::AttemptFailureClass::ContractViolation
        );
        f.send(LogicalInput::Timer);
        assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m)if matches!(m.body,LogicalToAttemptBody::PreReadyReplacementChanged{..}))),"registering or rereading a window is not a window change");
        assert_eq!(f.core.facts().conclusion(), None);
    }
}

#[test]
fn late_parent_refusal_closes_only_the_original_pre_ready_watch_and_requery_refuses() {
    let (mut f, p, issued, refusal) =
        observation_fixture(RecoveryMode::RestartAttemptBeforeVisibility, true);
    let (reference, op) = issued.expect("same original parent authorization");
    let (token, allowed) = observation_query(&mut f, refusal.clone());
    assert!(allowed);
    settle(
        &mut f,
        reference,
        p,
        op,
        AttemptIssueSettlement::Worker(OperationOutcome::CompatibilityMismatch),
    )
    .unwrap();
    let changes: Vec<_> = f
        .effects
        .iter()
        .filter_map(|e| match &e.body {
            LogicalEffectBody::AttemptPort(m) => match m.body {
                LogicalToAttemptBody::PreReadyReplacementChanged { token } => {
                    Some((m.activation, token))
                }
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(changes, [(activation(1), token)]);
    let (_, allowed) = observation_query(&mut f, refusal);
    assert!(!allowed);
    f.send(LogicalInput::Timer);
    assert_eq!(f.effects.iter().filter(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m)if matches!(m.body,LogicalToAttemptBody::PreReadyReplacementChanged{..}))).count(),1);
    assert_eq!(f.core.facts().conclusion(), None);
}

#[test]
fn cancellation_closes_pre_ready_permission_without_replacing_the_accepted_original_attempt_failure()
 {
    use novarocks_coordination_contract::PreReadyProcessReplacementWitness;
    use novarocks_logical_execution_protocol::cancel::{
        LogicalCancellationReason, cancellation_decision,
    };
    let (mut f, p, _, mut refusal) =
        observation_fixture(RecoveryMode::RestartAttemptBeforeVisibility, true);
    let original = refusal.input().clone();
    let witness = PreReadyProcessReplacementWitness::try_new(
        &original,
        p,
        operation(7000),
        roster(2)[1].backend_process_id(),
    )
    .unwrap();
    refusal.attach_pre_ready_replacement(witness).unwrap();
    let (token, allowed) = observation_query(&mut f, refusal.clone());
    assert!(allowed);
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(refusal.clone()),
    ));
    assert_eq!(f.core.facts().conclusion(), None);
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    assert_eq!(
        f.core.facts().failure(),
        Some(&cancellation_decision(LogicalCancellationReason::Requested).failure)
    );
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m)if matches!(m.body,LogicalToAttemptBody::PreReadyReplacementChanged{token:found}if found==token))));
    assert!(!observation_query(&mut f, refusal).1);
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
}

#[test]
fn original_authorization_reply_already_accepts_real_worker_and_transport_settlements() {
    // The real AP has no rich guard or TransportOwned callback. Each fresh
    // reply must already be conservatively charged before any later fact.
    for settlement in [
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        AttemptIssueSettlement::TransportUnknown,
        AttemptIssueSettlement::PreWorkerRejected,
        AttemptIssueSettlement::DefinitelyUnsent,
    ] {
        let (mut f, roster) = with_roster(1);
        let p = roster[0];
        let ticket = grant(&mut f, p, 1, Duration::from_secs(10));
        let (authorization, op) = issue(&mut f, p, ticket, 1);
        settle(&mut f, authorization, p, op, settlement).unwrap();
        no_finish(&f);
    }
}

#[test]
fn original_authorization_reply_survives_revocation_without_another_handoff_event() {
    let (mut f, roster) = with_roster(1);
    let p = roster[0];
    let ticket = grant(&mut f, p, 1, Duration::from_secs(10));
    let (authorization, op) = issue(&mut f, p, ticket, 1);
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    // No synthetic TransportOwned message intervenes. Revocation rejects new
    // permission but preserves this issued operation and its real late reply.
    settle(
        &mut f,
        authorization,
        p,
        op,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    )
    .unwrap();
    assert!(
        authorize(
            &mut f,
            AttemptAuthorization::ReauthorizeEstablish {
                participation: p,
                operation_id: op
            }
        )
        .is_err()
    );
    no_finish(&f);
}
