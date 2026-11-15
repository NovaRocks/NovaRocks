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

use crate::authorization::part3_support::*;
use crate::support::*;
use novarocks_coordination_contract::logical_port::{AbortIssueSettlement, AbortWorkerClosure};
use novarocks_execution_contract::{AbortParticipation, OperationKind, OperationOutcome};
use novarocks_logical_execution_protocol::facts::{
    AbortParticipationIssueState, CleanupPath, ParticipationClosureState,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

fn aborts(f: &mut Fixture) -> Vec<(AttemptActionId, AbortParticipation)> {
    let mut result = Vec::new();
    while let Some(index)=f.effects.iter().position(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m)if matches!(m.body,LogicalToAttemptBody::Abort{..}))) {
        let effect=f.effects.remove(index);let LogicalEffectBody::AttemptPort(message)=effect.body else{unreachable!()};
        assert_eq!(message.activation,activation(1));
        let LogicalToAttemptBody::Abort{action,request}=message.body else{unreachable!()};result.push((action,request));
    }
    result
}
fn abort_result(
    f: &mut Fixture,
    action: AttemptActionId,
    request: AbortParticipation,
    settlement: AbortIssueSettlement,
) {
    f.port(AttemptToLogicalBody::AbortSettlement {
        abort_action: action,
        participation: request.participation(),
        operation_id: request.envelope().operation_id(),
        settlement,
    });
}
fn contacted_two() -> (Fixture, Vec<novarocks_execution_contract::ParticipationRef>) {
    let (mut f, participants) = with_roster(2);
    for (i, p) in participants.iter().copied().enumerate() {
        grant(&mut f, p, i as u8 + 1, Duration::from_secs(1));
    }
    (f, participants)
}

#[test]
fn g9b3_1076_terminal_cleanup_keeps_abort_issuance_and_exact_bounded_replay() {
    let (mut f, participants) = contacted_two();
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    let original = aborts(&mut f);
    assert_eq!(original.len(), participants.len());
    let mut expected_ids = OperationIds::new(IdSeed::new(7, 100));
    let expected = [expected_ids.issue(), expected_ids.issue()];
    let mut by_participation = BTreeMap::new();
    for (action, request) in original {
        assert_eq!(request.envelope().kind(), OperationKind::AbortParticipation);
        assert!(expected.contains(&request.envelope().operation_id()));
        assert!(
            by_participation
                .insert(request.participation(), (action, request))
                .is_none()
        );
        abort_result(
            &mut f,
            action,
            request,
            AbortIssueSettlement::TransportUnknown,
        );
    }
    assert_eq!(by_participation.len(), 2);
    f.now = MonotonicInstant::from_origin(Duration::from_millis(20));
    let (action, request) = *by_participation.values().next().unwrap();
    abort_result(
        &mut f,
        action,
        request,
        AbortIssueSettlement::TransportUnknown,
    );
    assert!(aborts(&mut f).is_empty());
    f.now = MonotonicInstant::from_origin(Duration::from_millis(49));
    f.send(LogicalInput::Timer);
    assert!(aborts(&mut f).is_empty());
    f.now = MonotonicInstant::from_origin(Duration::from_millis(50));
    f.send(LogicalInput::Timer);
    let replay = aborts(&mut f);
    assert_eq!(replay.len(), 2);
    for (action, request) in replay {
        let (old, original) = by_participation[&request.participation()];
        assert_ne!(action, old);
        assert_eq!(request.envelope(), original.envelope());
        assert_eq!(request.cause(), original.cause());
        assert_eq!(
            f.core
                .facts()
                .participation(activation(1), request.participation())
                .unwrap()
                .abort_authorizations,
            2
        );
    }
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::ActivateAttempt { .. }
                | NativeEffect::RunAttempt { .. }
                | NativeEffect::PrepareSuccessor { .. }
                | NativeEffect::DispatchQualification { .. }
        ) | LogicalEffectBody::Relay(RelayEffect::Start { .. })
    )));
    no_finish(&f);
}

#[test]
fn g9b3_1089_bad_abort_settlement_does_not_hide_independent_later_participation() {
    let (mut f, _) = contacted_two();
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    let held = aborts(&mut f);
    assert_eq!(held.len(), 2);
    let (first, request) = held[0];
    f.port(AttemptToLogicalBody::AbortSettlement {
        abort_action: first,
        participation: request.participation(),
        operation_id: operation(999),
        settlement: AbortIssueSettlement::FailedClosed,
    });
    assert_eq!(
        f.core
            .facts()
            .participation(activation(1), request.participation())
            .unwrap()
            .abort_issue_state,
        Some(AbortParticipationIssueState::AwaitingSettlement)
    );
    let (second, request2) = held[1];
    abort_result(
        &mut f,
        second,
        request2,
        AbortIssueSettlement::Worker {
            outcome: OperationOutcome::Accepted,
            closure: AbortWorkerClosure::TerminalRetained,
        },
    );
    assert_eq!(
        f.core
            .facts()
            .participation(activation(1), request2.participation())
            .unwrap()
            .abort_issue_state,
        Some(AbortParticipationIssueState::WorkerSettled)
    );
    abort_result(
        &mut f,
        first,
        request,
        AbortIssueSettlement::Worker {
            outcome: OperationOutcome::Accepted,
            closure: AbortWorkerClosure::Aborting,
        },
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    assert!(f.core.facts().supervision_failure().is_none());
    no_finish(&f);
    for (_, request) in held {
        f.send(LogicalInput::RegistryParticipation {
            activation: activation(1),
            participation: request.participation(),
            convergence: RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced,
        });
    }
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
    )));
}

#[test]
fn g9b3_1071_unissued_nonempty_roster_never_fabricates_original_local_exit() {
    let participants = roster(3);
    let mut initial = start(LogicalOutputMode::CompletionOnly);
    initial.first_attempt.participations = participants.clone().into_boxed_slice();
    let mut f = Fixture::with_start(initial);
    // Only registration was emitted. Cancellation discards activation before
    // any attempt host exists, but the initial dormant preparation still owns
    // actual local resources and needs its original cleanup/exit facts.
    f.send_once(LogicalInput::Started, 1);
    let register = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { .. })
        )
    });
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    f.ack(
        register.action,
        ActionResult::Governance(GovernanceFact::ObligationRegistered {
            activation: activation(1),
            kind: ObligationKind::RunningWork,
        }),
    );
    assert!(!f.core.facts().attempts().next().unwrap().activated);
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .all_participations_closed
    );
    for p in participants {
        assert_eq!(
            f.core
                .facts()
                .participation(activation(1), p)
                .unwrap()
                .closure,
            ParticipationClosureState::NoRemoteResponsibility
        );
    }
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::ActivateAttempt { .. } | NativeEffect::RunAttempt { .. }
        )
    )));
    assert!(aborts(&mut f).is_empty());
    let resolve = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
        )
    });
    f.ack(
        resolve.action,
        ActionResult::Governance(GovernanceFact::ObligationResolved {
            activation: activation(1),
            kind: ObligationKind::RunningWork,
        }),
    );
    let resources = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
        )
    });
    assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
    f.result(
        resources.action,
        ActionResult::Native(NativeFact::DormantConverged {
            execution: activation(1).execution(),
        }),
    );
    assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
    assert!(!f.core.facts().protocol_join_ready());
    f.exit(resources.action);
    assert!(f.core.facts().attempts().next().unwrap().cleanup_complete);
}

#[test]
fn unknown_remote_tracking_selects_abandon_and_never_fabricates_local_exit_in_either_action_order()
{
    for exit_first in [false, true] {
        let (mut f, participants) = contacted_two();
        f.send(LogicalInput::Cancelled(
            novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
        ));
        for (action, request) in aborts(&mut f) {
            abort_result(
                &mut f,
                action,
                request,
                AbortIssueSettlement::Worker {
                    outcome: OperationOutcome::Accepted,
                    closure: AbortWorkerClosure::TerminalRetained,
                },
            );
        }
        f.send(LogicalInput::RegistryParticipation {
            activation: activation(1),
            participation: participants[0],
            convergence: RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced,
        });
        no_finish(&f);
        f.now = MonotonicInstant::from_origin(Duration::from_millis(100));
        f.send(LogicalInput::Timer);
        let attempt = f.core.facts().attempts().next().unwrap();
        assert!(attempt.remote_tracking_ended);
        assert!(!attempt.all_participations_closed);
        assert!(!attempt.cleanup_complete);
        assert_eq!(
            attempt.cleanup_path,
            Some(CleanupPath::EndTrackingThenAbandon)
        );
        let ending = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking { .. })
            )
        });
        if exit_first {
            f.exit(ending.action);
        }
        f.result(
            ending.action,
            ActionResult::Governance(GovernanceFact::RemoteTrackingEnded {
                activation: activation(1),
                kind: ObligationKind::RunningWork,
            }),
        );
        if !exit_first {
            assert!(!f.effects.iter().any(|e| matches!(
                e.body,
                LogicalEffectBody::Native(NativeEffect::AbandonResources { .. })
            )));
            f.exit(ending.action);
        }
        let resources = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::AbandonResources { .. })
            )
        });
        // Positive evidence arriving later refines the physical fact, but it
        // cannot replay or rewrite the already selected original cleanup path.
        f.send(LogicalInput::RegistryParticipation {
            activation: activation(1),
            participation: participants[1],
            convergence: RegistryParticipationConvergence::WorkerProcessReplaced,
        });
        assert!(
            f.core
                .facts()
                .attempts()
                .next()
                .unwrap()
                .all_participations_closed
        );
        assert_eq!(
            f.core.facts().attempts().next().unwrap().cleanup_path,
            Some(CleanupPath::EndTrackingThenAbandon)
        );
        if exit_first {
            f.exit(resources.action);
        }
        f.result(resources.action, ActionResult::Native(NativeFact::Applied));
        f.port(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: None,
            lifecycle_runtime_filters: None,
        });
        if !exit_first {
            assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
            f.exit(resources.action);
        }
        assert!(f.core.facts().attempts().next().unwrap().cleanup_complete);
        assert!(!f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
                | LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
        )));
        assert!(
            !f.core.facts().protocol_join_ready(),
            "the original Native run action is still held"
        );
        ack_housekeeping(&mut f);
    }
}

#[test]
fn original_activation_is_checked_before_unknown_registry_participation_without_mutation() {
    let (mut f, participants) = contacted_two();
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    let original = f
        .core
        .facts()
        .participation(activation(1), participants[0])
        .unwrap();
    let unknown = roster(3)[2];
    let wrong_actor = AttemptActivationIdentity::from_parts(
        999,
        activation(1).execution(),
        activation(1).generation(),
    );
    struct Sink(Vec<LogicalEffect>);
    impl EffectSink for Sink {
        fn emit(&mut self, effect: LogicalEffect) {
            self.0.push(effect);
        }
    }
    for (activation, expected) in [
        (
            wrong_actor,
            novarocks_logical_execution_protocol::CoreFault::ForeignActivation,
        ),
        (
            activation(1),
            novarocks_logical_execution_protocol::CoreFault::ForeignExecution,
        ),
    ] {
        let mut sink = Sink(Vec::new());
        assert_eq!(
            f.core.handle(
                f.now,
                LogicalInput::RegistryParticipation {
                    activation,
                    participation: unknown,
                    convergence: RegistryParticipationConvergence::WorkerProcessReplaced
                },
                &mut Budget(32),
                &mut sink
            ),
            Err(expected)
        );
        assert!(sink.0.is_empty());
        assert_eq!(
            f.core
                .facts()
                .participation(crate::support::activation(1), participants[0]),
            Some(original)
        );
    }
}

#[test]
fn nonempty_convergence_closure_rejects_missing_duplicate_and_foreign_members() {
    use novarocks_coordination_contract::AttemptParticipationConvergence as C;
    let participants = roster(2);
    let foreign = roster(3)[2];
    for members in [
        vec![],
        vec![participants[0]],
        vec![participants[0], participants[0]],
        vec![participants[0], foreign],
    ] {
        let (mut f, _) = with_roster(2);
        f.send(LogicalInput::Cancelled(
            novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
        ));
        let rows = members
            .into_iter()
            .map(|p| (p, C::NeverEstablished))
            .collect();
        struct Sink;
        impl EffectSink for Sink {
            fn emit(&mut self, _: LogicalEffect) {}
        }
        let mut result = f.core.handle(
            f.now,
            LogicalInput::AttemptPort(Box::new(AttemptToLogical {
                activation: activation(1),
                action: AttemptActionId::new(std::num::NonZeroU64::new(9000).unwrap()),
                body: AttemptToLogicalBody::ConvergenceProgress {
                    local_exited: false,
                    closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(rows))),
                    lifecycle_runtime_filters: None,
                },
            })),
            &mut Budget(256),
            &mut Sink,
        );
        for _ in 0..16 {
            if result.is_err() {
                break;
            }
            result = f
                .core
                .handle(f.now, LogicalInput::Timer, &mut Budget(256), &mut Sink);
        }
        assert_eq!(
            result,
            Err(novarocks_logical_execution_protocol::CoreFault::UnexpectedInput)
        );
        assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
        assert!(!f.core.facts().protocol_join_ready());
    }
}

#[test]
fn late_abort_facts_for_residual_a_cannot_revoke_or_settle_running_successor_b() {
    use crate::recovery::contract::{
        RecoveryFixture, admission, at, backend_failure, roster as recovery_roster,
    };
    use novarocks_coordination_contract::AttemptIssueSettlement;
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    r.establish_initial(AttemptIssueSettlement::TransportOwned);
    let original = backend_failure(1, "original predecessor failure");
    r.fail(activation(1), original.clone());
    let (dispatch, _) = r.prepare(recovery_roster(2));
    r.f.now = at(10);
    let grants: Vec<_> = dispatch
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
        .collect();
    r.qualify(&dispatch, grants.clone(), at(5));
    let (b, _) = r.activate(&dispatch);
    let original_aborts = aborts(&mut r.f);
    assert_eq!(original_aborts.len(), 2);
    let (first, request) = original_aborts[0];
    r.send_port(
        activation(1),
        AttemptToLogicalBody::AbortSettlement {
            abort_action: first,
            participation: request.participation(),
            operation_id: request.envelope().operation_id(),
            settlement: AbortIssueSettlement::FailedClosed,
        },
    );
    // A real late Worker reply may refine A's FailedClosed uncertainty. It
    // remains A's fact, even though B is now the only current authority.
    r.send_port(
        activation(1),
        AttemptToLogicalBody::AbortSettlement {
            abort_action: first,
            participation: request.participation(),
            operation_id: request.envelope().operation_id(),
            settlement: AbortIssueSettlement::Worker {
                outcome: OperationOutcome::Accepted,
                closure: AbortWorkerClosure::TerminalRetained,
            },
        },
    );
    let (second, request2) = original_aborts[1];
    r.send_port(
        activation(1),
        AttemptToLogicalBody::AbortSettlement {
            abort_action: second,
            participation: request2.participation(),
            operation_id: request2.envelope().operation_id(),
            settlement: AbortIssueSettlement::Worker {
                outcome: OperationOutcome::Gone,
                closure: AbortWorkerClosure::Gone,
            },
        },
    );
    assert_eq!(r.f.core.facts().current_activation(), Some(b));
    assert_eq!(r.f.core.facts().conclusion(), None);
    let predecessor =
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap();
    assert_eq!(predecessor.accepted_failure, Some(original.input()));
    assert!(!predecessor.all_participations_closed);
    let successor =
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == b)
            .unwrap();
    assert!(successor.authority_open);
    assert!(successor.activated);
    assert!(successor.accepted_failure.is_none());
    assert!(!successor.cleanup_complete);
    let issued = r.issue(b, grants[0].request.participation(), grants[0], 30);
    r.settle(issued, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    r.settle(
        issued,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    )
    .unwrap();
    assert_eq!(r.f.core.facts().current_activation(), Some(b));
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(!r.f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(m)if m.activation==b&&matches!(m.body,LogicalToAttemptBody::Abort{..}|LogicalToAttemptBody::Stop{..}))));
    assert!(!r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::PrepareSuccessor{failed,..})if failed==b.execution())));
}

#[test]
fn frontend_drain_stop_carries_the_same_core_authored_typed_cancellation() {
    use novarocks_logical_execution_protocol::cancel::{
        LogicalCancellationReason, cancellation_decision,
    };
    let (mut f, _) = with_roster(1);
    let expected = cancellation_decision(LogicalCancellationReason::FrontendDrainDeadlineExceeded);
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::FrontendDrainDeadlineExceeded,
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(&expected.failure));
    {
        let stops: Vec<_> = f
            .effects
            .iter()
            .filter_map(|effect| match &effect.body {
                LogicalEffectBody::AttemptPort(message) => match &message.body {
                    LogicalToAttemptBody::Stop { reason, failure } => {
                        Some((message.activation, *reason, failure))
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(stops.len(), 1);
        assert_eq!(stops[0].0, activation(1));
        assert_eq!(
            stops[0].1,
            novarocks_coordination_contract::AttemptStopReason::DrainDeadline
        );
        assert_eq!(
            stops[0].2.input(),
            expected.failure.execution_failure().unwrap()
        );
    }
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    assert_eq!(f.core.facts().failure(), Some(&expected.failure));
    assert_eq!(f.effects.iter().filter(|effect|matches!(&effect.body,LogicalEffectBody::AttemptPort(message)if matches!(message.body,LogicalToAttemptBody::Stop{..}))).count(),1);
}

#[path = "part3/remaining.rs"]
mod remaining;
