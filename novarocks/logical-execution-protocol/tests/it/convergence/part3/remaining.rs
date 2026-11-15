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

use super::*;
use crate::recovery::contract::{
    RecoveryFixture, admission, at, backend_failure, roster as recovery_roster,
};
use novarocks_coordination_contract::{
    AttemptIssueSettlement, ExecutionFailureInput, ExecutionFailureSite, QueryExecutionError,
    QueryFailureReason,
};
use novarocks_execution_contract::{
    ParticipationRef, SafeDetail,
    failure::{FailureClass, FailureProjection, FailureReason, domain},
};
use novarocks_logical_execution_protocol::facts::AttemptFacts;
use std::num::{NonZeroU16, NonZeroU64};

fn attempt(f: &Fixture, activation: AttemptActivationIdentity) -> AttemptFacts<'_> {
    f.core
        .facts()
        .attempts()
        .find(|a| a.activation == activation)
        .unwrap()
}
fn port(f: &mut Fixture, activation: AttemptActivationIdentity, body: AttemptToLogicalBody) {
    f.send(LogicalInput::AttemptPort(Box::new(AttemptToLogical {
        activation,
        action: AttemptActionId::new(NonZeroU64::new(80_000 + f.steps as u64).unwrap()),
        body,
    })));
}
fn no_stop_or_abort(f: &Fixture, activation: AttemptActivationIdentity) {
    assert!(!f.effects.iter().any(|e| matches!(&e.body, LogicalEffectBody::AttemptPort(p) if p.activation == activation && matches!(p.body, LogicalToAttemptBody::Stop { .. } | LogicalToAttemptBody::Abort { .. }))));
}
fn no_resolve_or_finish(f: &Fixture, activation: AttemptActivationIdentity) {
    assert!(!f.effects.iter().any(|e| matches!(e.body,
        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { activation: a, .. })
        | LogicalEffectBody::Native(NativeEffect::FinishResources { activation: a }) if a == activation)));
}
fn take_aborts(
    f: &mut Fixture,
    activation: AttemptActivationIdentity,
) -> Vec<(AttemptActionId, AbortParticipation)> {
    let mut held = Vec::new();
    while let Some(index) = f.effects.iter().position(|e| matches!(&e.body, LogicalEffectBody::AttemptPort(p) if p.activation == activation && matches!(p.body, LogicalToAttemptBody::Abort { .. }))) {
        let LogicalEffectBody::AttemptPort(p) = f.effects.remove(index).body else { unreachable!() };
        let LogicalToAttemptBody::Abort { action, request } = p.body else { unreachable!() };
        held.push((action, request));
    }
    held
}
fn worker_ack(
    f: &mut Fixture,
    activation: AttemptActivationIdentity,
    held: &[(AttemptActionId, AbortParticipation)],
    closure: AbortWorkerClosure,
) {
    for &(action, request) in held {
        port(
            f,
            activation,
            AttemptToLogicalBody::AbortSettlement {
                abort_action: action,
                participation: request.participation(),
                operation_id: request.envelope().operation_id(),
                settlement: AbortIssueSettlement::Worker {
                    outcome: if closure == AbortWorkerClosure::Gone {
                        OperationOutcome::Gone
                    } else {
                        OperationOutcome::Accepted
                    },
                    closure,
                },
            },
        );
        assert_eq!(
            f.core
                .facts()
                .participation(activation, request.participation())
                .unwrap()
                .abort_issue_state,
            Some(AbortParticipationIssueState::WorkerSettled)
        );
    }
}
fn positive(
    f: &mut Fixture,
    activation: AttemptActivationIdentity,
    participants: &[ParticipationRef],
) {
    for (i, &participation) in participants.iter().enumerate() {
        f.send(LogicalInput::RegistryParticipation {
            activation,
            participation,
            convergence: if i == 0 {
                RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced
            } else {
                RegistryParticipationConvergence::WorkerProcessReplaced
            },
        });
    }
    assert!(attempt(f, activation).all_participations_closed);
    // This inherited name means tracking is complete: either positive proof
    // for every member or finite observation ended with Unknown responsibility.
    assert!(attempt(f, activation).remote_tracking_ended);
    for (i, &participation) in participants.iter().enumerate() {
        assert_eq!(
            f.core
                .facts()
                .participation(activation, participation)
                .unwrap()
                .closure,
            if i == 0 {
                ParticipationClosureState::WorkerStoppedAndParticipationFenced
            } else {
                ParticipationClosureState::WorkerProcessReplaced
            }
        );
    }
    assert!(!attempt(f, activation).cleanup_complete);
    assert!(!f.effects.iter().any(|effect| matches!(effect.body,
        LogicalEffectBody::Governance(GovernanceEffect::EndRemoteTracking { activation: a, .. })
        | LogicalEffectBody::Native(NativeEffect::AbandonResources { activation: a }) if a == activation)));
    if f.core.facts().supervision_failure().is_none() {
        assert_eq!(
            attempt(f, activation).cleanup_path,
            Some(CleanupPath::ResolveThenFinish)
        );
    }
}
fn local_exit(f: &mut Fixture, activation: AttemptActivationIdentity) {
    port(
        f,
        activation,
        AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: None,
            lifecycle_runtime_filters: None,
        },
    );
}
fn finish_positive(
    f: &mut Fixture,
    activation: AttemptActivationIdentity,
    local_already_exited: bool,
) {
    assert_eq!(
        attempt(f, activation).cleanup_path,
        Some(CleanupPath::ResolveThenFinish)
    );
    let resolve = f.take(|b| matches!(b, LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { activation: a, kind: ObligationKind::RunningWork }) if *a == activation));
    f.result(
        resolve.action,
        ActionResult::Governance(GovernanceFact::ObligationResolved {
            activation,
            kind: ObligationKind::RunningWork,
        }),
    );
    assert!(!f.effects.iter().any(|e| matches!(e.body, LogicalEffectBody::Native(NativeEffect::FinishResources { activation: a }) if a == activation)));
    f.exit(resolve.action);
    let resources = f.take(|b| matches!(b, LogicalEffectBody::Native(NativeEffect::FinishResources { activation: a }) if *a == activation));
    f.result(resources.action, ActionResult::Native(NativeFact::Applied));
    assert!(!attempt(f, activation).cleanup_complete);
    f.exit(resources.action);
    if !local_already_exited {
        assert!(
            !attempt(f, activation).cleanup_complete,
            "positive proof and actual resource-action exit cannot forge the original attempt's local exit"
        );
        local_exit(f, activation);
    }
    assert!(attempt(f, activation).cleanup_complete);
    assert!(
        !f.core.facts().protocol_join_ready(),
        "the original Native Run action is still held"
    );
}
fn established_completion() -> (Fixture, Vec<ParticipationRef>) {
    let (mut f, participants) = with_roster(2);
    for (i, p) in participants.iter().copied().enumerate() {
        let ticket = grant(&mut f, p, i as u8 + 1, Duration::from_secs(1));
        let (reference, operation) = issue(&mut f, p, ticket, i as u8 + 1);
        settle(
            &mut f,
            reference,
            p,
            operation,
            AttemptIssueSettlement::TransportOwned,
        )
        .unwrap();
        settle(
            &mut f,
            reference,
            p,
            operation,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    }
    (f, participants)
}
fn running_successor() -> (
    RecoveryFixture,
    AttemptActivationIdentity,
    novarocks_coordination_contract::ExecutionFailure,
) {
    let mut r = RecoveryFixture::new(LogicalOutputMode::CompletionOnly);
    r.establish_initial(AttemptIssueSettlement::TransportOwned);
    let original = backend_failure(
        1,
        "original predecessor failure retained during independent cleanup",
    );
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
    for (i, grant) in grants.iter().copied().enumerate() {
        let issued = r.issue(b, grant.request.participation(), grant, 30 + i as u8);
        r.settle(issued, AttemptIssueSettlement::TransportOwned)
            .unwrap();
        r.settle(
            issued,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    }
    assert_eq!(r.f.core.facts().current_activation(), Some(b));
    assert_eq!(r.f.core.facts().conclusion(), None);
    (r, b, original)
}

#[test]
fn nonempty_normal_success_never_stops_or_aborts_and_waits_positive_and_original_local_exit() {
    let (mut f, participants) = established_completion();
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.core.facts().failure().is_none());
    no_stop_or_abort(&f, activation(1));
    assert!(f.effects.iter().any(|e| matches!(&e.body, LogicalEffectBody::AttemptPort(p) if p.activation == activation(1) && matches!(p.body, LogicalToAttemptBody::BeginConvergence { mode: novarocks_coordination_contract::AttemptConvergenceMode::Normal, .. }))));
    assert!(!attempt(&f, activation(1)).all_participations_closed);
    no_resolve_or_finish(&f, activation(1));
    f.send(LogicalInput::RegistryParticipation {
        activation: activation(1),
        participation: participants[0],
        convergence: RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced,
    });
    assert!(!attempt(&f, activation(1)).all_participations_closed);
    no_resolve_or_finish(&f, activation(1));
    f.send(LogicalInput::RegistryParticipation {
        activation: activation(1),
        participation: participants[1],
        convergence: RegistryParticipationConvergence::WorkerProcessReplaced,
    });
    assert!(attempt(&f, activation(1)).all_participations_closed);
    assert!(!attempt(&f, activation(1)).cleanup_complete);
    finish_positive(&mut f, activation(1), false);
    no_stop_or_abort(&f, activation(1));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.core.facts().failure().is_none());
    assert!(f.core.facts().supervision_failure().is_none());
}

#[test]
fn successful_successor_keeps_predecessor_abort_ack_residual_until_positive_and_local_exit() {
    let (mut r, b, original) = running_successor();
    let a = activation(1);
    let held = take_aborts(&mut r.f, a);
    assert_eq!(held.len(), 2);
    worker_ack(&mut r.f, a, &held, AbortWorkerClosure::Aborting);
    r.send_port(
        b,
        AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(r.f.core.facts().failure().is_none());
    assert_eq!(attempt(&r.f, a).accepted_failure, Some(original.input()));
    assert!(!attempt(&r.f, a).all_participations_closed);
    assert!(!attempt(&r.f, a).cleanup_complete);
    no_resolve_or_finish(&r.f, a);
    no_stop_or_abort(&r.f, b);
    // B can finish while A has only a Worker admission-closure receipt.
    positive(&mut r.f, b, &recovery_roster(2));
    finish_positive(&mut r.f, b, false);
    assert!(!attempt(&r.f, a).all_participations_closed);
    assert!(!attempt(&r.f, a).cleanup_complete);
    no_resolve_or_finish(&r.f, a);
    positive(&mut r.f, a, &recovery_roster(1));
    finish_positive(&mut r.f, a, false);
    assert_eq!(attempt(&r.f, a).accepted_failure, Some(original.input()));
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(r.f.core.facts().failure().is_none());
    no_stop_or_abort(&r.f, b);
}

fn initial_worker_closure_still_requires_residual_proofs(
    closure: AbortWorkerClosure,
    expected: ParticipationClosureState,
) {
    for local_first in [false, true] {
        let (mut f, participants) = established_completion();
        let original = backend_failure(1, "original initial attempt failure");
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Failed(original.clone()),
        ));
        let held = take_aborts(&mut f, activation(1));
        assert_eq!(held.len(), participants.len());
        worker_ack(&mut f, activation(1), &held, closure);
        for &p in &participants {
            assert_eq!(
                f.core
                    .facts()
                    .participation(activation(1), p)
                    .unwrap()
                    .closure,
                expected
            );
        }
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert_eq!(f.core.facts().failure(), Some(original.error()));
        assert!(!attempt(&f, activation(1)).all_participations_closed);
        assert!(!attempt(&f, activation(1)).cleanup_complete);
        no_resolve_or_finish(&f, activation(1));
        if local_first {
            local_exit(&mut f, activation(1));
            assert!(!attempt(&f, activation(1)).all_participations_closed);
            assert!(!attempt(&f, activation(1)).cleanup_complete);
            no_resolve_or_finish(&f, activation(1));
        }
        positive(&mut f, activation(1), &participants);
        finish_positive(&mut f, activation(1), local_first);
        assert_eq!(
            attempt(&f, activation(1)).accepted_failure,
            Some(original.input())
        );
        assert_eq!(f.core.facts().failure(), Some(original.error()));
    }
}
#[test]
fn initial_terminal_retained_worker_receipt_preserves_residual_until_positive_and_actual_exit() {
    initial_worker_closure_still_requires_residual_proofs(
        AbortWorkerClosure::TerminalRetained,
        ParticipationClosureState::TerminalRetained,
    );
}
#[test]
fn initial_gone_worker_receipt_preserves_residual_until_positive_and_actual_exit() {
    initial_worker_closure_still_requires_residual_proofs(
        AbortWorkerClosure::Gone,
        ParticipationClosureState::Gone,
    );
}

#[test]
fn g9b3_1089_predecessor_typed_abort_error_does_not_swallow_successor_cleanup() {
    let (mut r, b, original) = running_successor();
    let a = activation(1);
    let held = take_aborts(&mut r.f, a);
    assert_eq!(held.len(), 2);
    let (action, request) = held[0];
    // The same issued action first settled Unknown. A conflicting Unsent
    // verdict returns the original WrongState error, not a Worker-stop proof.
    r.send_port(
        a,
        AttemptToLogicalBody::AbortSettlement {
            abort_action: action,
            participation: request.participation(),
            operation_id: request.envelope().operation_id(),
            settlement: AbortIssueSettlement::TransportUnknown,
        },
    );
    assert_eq!(r.f.core.facts().conclusion(), None);
    r.send_port(
        a,
        AttemptToLogicalBody::AbortSettlement {
            abort_action: action,
            participation: request.participation(),
            operation_id: request.envelope().operation_id(),
            settlement: AbortIssueSettlement::DefinitelyUnsent,
        },
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Failed)
    );
    let error = r.f.core.facts().failure().unwrap().clone();
    let input = error.execution_failure().unwrap();
    assert_eq!(input.fact().class(), FailureClass::Control);
    assert_eq!(
        input.fact().reason(),
        FailureReason::new(domain::QUERY, NonZeroU16::new(85).unwrap())
    );
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        input.detail().as_str(),
        "logical execution cleanup protocol failed"
    );
    assert!(r.f.core.facts().supervision_failure().is_none());
    assert_eq!(
        r.f.core
            .facts()
            .participation(a, request.participation())
            .unwrap()
            .abort_issue_state,
        Some(AbortParticipationIssueState::TransportUnknown)
    );
    assert_eq!(attempt(&r.f, a).accepted_failure, Some(original.input()));
    assert!(!attempt(&r.f, a).all_participations_closed);
    no_resolve_or_finish(&r.f, a);
    let b_aborts = take_aborts(&mut r.f, b);
    assert_eq!(
        b_aborts.len(),
        2,
        "the earlier predecessor error cannot swallow B's cleanup turn"
    );
    worker_ack(&mut r.f, b, &b_aborts, AbortWorkerClosure::TerminalRetained);
    assert!(!attempt(&r.f, b).all_participations_closed);
    no_resolve_or_finish(&r.f, b);
    positive(&mut r.f, b, &recovery_roster(2));
    finish_positive(&mut r.f, b, false);
    assert!(!attempt(&r.f, a).cleanup_complete);
    assert!(!attempt(&r.f, a).all_participations_closed);
    no_resolve_or_finish(&r.f, a);
    assert_eq!(r.f.core.facts().failure(), Some(&error));
    assert_eq!(attempt(&r.f, a).accepted_failure, Some(original.input()));
    assert!(r.f.core.facts().supervision_failure().is_none());
}

#[test]
fn original_required_governance_failure_is_scope_fatal_and_cannot_fake_either_attempt_exit() {
    let (mut r, b, original) = running_successor();
    let a = activation(1);
    r.send_port(
        b,
        AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed),
    );
    positive(&mut r.f, a, &recovery_roster(1));
    let resolve = r.f.take(|body| matches!(body, LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { activation, .. }) if *activation == a));
    let raw = QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating("original required obligation settlement failed"),
        ExecutionFailureSite::Coordinator,
    ));
    r.f.result(
        resolve.action,
        ActionResult::Governance(GovernanceFact::Failed(raw.clone())),
    );
    r.f.exit(resolve.action);
    assert_eq!(r.f.core.facts().supervision_failure(), Some(&raw));
    assert_eq!(attempt(&r.f, a).cleanup_failure, Some(&raw));
    assert!(
        r.f.effects.iter().any(
            |e| matches!(&e.body, LogicalEffectBody::SupervisionFailed(error) if error == &raw)
        )
    );
    positive(&mut r.f, b, &recovery_roster(2));
    local_exit(&mut r.f, a);
    local_exit(&mut r.f, b);
    r.f.send(LogicalInput::Timer);
    // Inputs may retain positive observations, but the broken obligation does
    // not authorize any new cleanup/retirement or manufacture an action exit.
    assert!(!attempt(&r.f, a).cleanup_complete);
    assert!(!attempt(&r.f, b).cleanup_complete);
    no_resolve_or_finish(&r.f, b);
    assert!(!r.f.core.facts().protocol_join_ready());
    assert_eq!(attempt(&r.f, a).accepted_failure, Some(original.input()));
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(r.f.core.facts().failure().is_none());
    assert_eq!(r.f.core.facts().supervision_failure(), Some(&raw));
}
