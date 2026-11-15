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

use super::super::support::*;
use super::contract::*;
use novarocks_coordination_contract::{
    AttemptIssueSettlement, ExecutionFailure, ExecutionFailureInput, ExecutionFailureSite,
    PreReadyProcessReplacementWitness, QueryExecutionError, QueryFailureReason,
    TASK_PROTOCOL_IDENTITY_MISMATCH_REASON, logical_port::AttemptAuthorizationFailure,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, SafeDetail,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use novarocks_logical_execution_protocol::replacement::ReplacementQualificationFailure;
use std::{num::NonZeroU16, sync::Arc};

fn successor(
    r: &mut RecoveryFixture,
    attempt: u64,
) -> (AttemptActivationIdentity, Vec<QualifiedAdmission>) {
    let (dispatch, topology) = r.prepare(roster(attempt));
    assert!(matches!(topology, SuccessorTopology::LiveSnapshot));
    assert_eq!(dispatch.failed.as_ref(), roster(attempt - 1));
    assert_eq!(dispatch.replacement.as_ref(), roster(attempt));
    let grants = dispatch
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
        .collect::<Vec<_>>();
    r.f.now = at(10);
    r.qualify(&dispatch, grants.clone(), at(5));
    let (activation, inherited) = r.activate(&dispatch);
    assert_eq!(inherited.len(), 2);
    for original in &grants {
        let retained = inherited
            .iter()
            .find(|g| g.participation == original.request.participation())
            .unwrap();
        assert_eq!(retained.receipt, original.receipt);
    }
    (activation, grants)
}
fn establish_successor(
    r: &mut RecoveryFixture,
    a: AttemptActivationIdentity,
    grants: &[QualifiedAdmission],
) {
    for (i, grant) in grants.iter().enumerate() {
        let issued = r.issue(a, grant.request.participation(), *grant, 30 + i as u8);
        r.settle(issued, AttemptIssueSettlement::TransportOwned)
            .unwrap();
        r.settle(
            issued,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    }
}
fn first_unknown() -> (RecoveryFixture, Vec<Issued>, ExecutionFailure) {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    let old = r.establish_initial(AttemptIssueSettlement::TransportUnknown);
    let failure = backend_failure(1, "original first attempt backend failure");
    r.fail(activation(1), failure.clone());
    (r, old, failure)
}
fn assert_old_failure(r: &RecoveryFixture, original: &ExecutionFailure) {
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap()
            .accepted_failure,
        Some(original.input())
    );
}
#[test]
fn two_real_recoveries_reuse_first_preparation_deadline_and_never_activate_expired_third_attempt() {
    let (mut r, _, first) = first_unknown();
    let (a2, grants) = successor(&mut r, 2);
    establish_successor(&mut r, a2, &grants);
    r.f.now = at(20);
    let second = backend_failure(2, "second attempt original failure");
    r.fail(a2, second.clone());
    let pending=r.f.take(|b|matches!(b,LogicalEffectBody::Native(NativeEffect::PrepareSuccessor{replacement,..})if *replacement==activation(3).execution()));
    let LogicalEffectBody::Native(NativeEffect::PrepareSuccessor {
        failed,
        replacement,
        deadline,
        topology,
    }) = pending.body
    else {
        unreachable!()
    };
    assert_eq!(failed, a2.execution());
    assert_eq!(replacement, activation(3).execution());
    assert!(matches!(topology, SuccessorTopology::LiveSnapshot));
    assert_eq!(
        deadline,
        at(50),
        "attempt two cannot lend a fresh preparation window"
    );
    r.f.now = at(49);
    r.f.send(LogicalInput::Timer);
    assert_eq!(r.f.core.facts().conclusion(), None);
    r.f.now = at(50);
    r.f.send(LogicalInput::Timer);
    let expected = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::RecoveryPreparationBudgetExceeded,
        "Native replacement preparation exceeded its recovery budget",
    );
    assert_eq!(r.f.core.facts().failure(), Some(&expected));
    assert_eq!(r.f.core.facts().attempts_started(), 2);
    assert!(!r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ActivateAttempt{activation,..}|NativeEffect::RunAttempt{activation})if activation.execution()==replacement)));
    assert_old_failure(&r, &first);
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == a2)
            .unwrap()
            .accepted_failure,
        Some(second.input())
    );
}
#[test]
fn nonempty_qualification_imports_exact_worker_tickets_before_successor_establish() {
    let (mut r, _, first) = first_unknown();
    let (a, grants) = successor(&mut r, 2);
    assert_eq!(r.f.core.facts().attempts_started(), 2);
    assert_eq!(r.f.core.facts().current_activation(), Some(a));
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|f| f.activation == a)
            .unwrap()
            .participations,
        roster(2)
    );
    establish_successor(&mut r, a, &grants);
    r.succeed_empty_stream(a);
    assert_old_failure(&r, &first);
    let old =
        r.f.core
            .facts()
            .attempts()
            .find(|f| f.activation == activation(1))
            .unwrap();
    assert!(!old.all_participations_closed);
    assert!(!old.cleanup_complete);
    assert!(
        r.f.core
            .facts()
            .attempts()
            .find(|f| f.activation == a)
            .unwrap()
            .accepted_failure
            .is_none()
    );
}
#[test]
fn imported_admission_expires_from_actual_qualification_dispatch_not_activation_time() {
    let (mut r, _, first) = first_unknown();
    let (d, _) = r.prepare(roster(2));
    let grants = d
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 20))
        .collect::<Vec<_>>();
    r.f.now = at(8);
    r.qualify(&d, grants.clone(), at(5));
    r.f.now = at(12);
    let (a, inherited) = r.activate(&d);
    assert_eq!(inherited.len(), 2);
    r.f.now = at(26);
    let rejection = r
        .authorize(
            a,
            establish_request(grants[0].request.participation(), grants[0].receipt, 30),
        )
        .unwrap_err();
    let AttemptAuthorizationFailure::Rejected(error) = rejection else {
        panic!("expired ticket is a typed refusal")
    };
    assert_eq!(
        error.input().fact().reason(),
        FailureReason::new(domain::QUERY, NonZeroU16::new(82).unwrap())
    );
    assert_eq!(error.input().fact().class(), FailureClass::Control);
    assert_eq!(
        error.input().detail().as_str(),
        "admission ticket expired before Establish issue authorization"
    );
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert_old_failure(&r, &first);
}
#[test]
fn malformed_nonempty_qualification_cannot_activate_or_erase_old_attempt_cause() {
    for malformed in 0..5 {
        let (mut r, _, first) = first_unknown();
        let (d, _) = r.prepare(roster(2));
        let mut grants = d
            .replacement
            .iter()
            .enumerate()
            .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
            .collect::<Vec<_>>();
        match malformed {
            0 => {
                grants.pop();
            }
            1 => grants[1] = grants[0],
            2 => grants[0].receipt = admission(participant(1, 1), 77, 1000).receipt,
            3 => grants[0].receipt = admission(grants[0].request.participation(), 30, 999).receipt,
            _ => {}
        }
        r.f.budget = 1;
        r.f.now = at(10);
        r.f.ack(
            d.action,
            ActionResult::Native(NativeFact::QualificationSettled {
                identity: d.identity,
                operation_id: d.operation,
                dispatched_at: Some(at(5)),
                settlement: QualificationSettlement::Qualified(QualifiedReplacementFacts {
                    identity: d.identity,
                    operation_id: d.operation,
                    topology_revision: if malformed == 4 { 0 } else { 7 },
                    admissions: grants.into_boxed_slice(),
                }),
            }),
        );
        let expected = ReplacementQualificationFailure::InvalidReservation.error();
        assert_eq!(
            r.f.core.facts().failure(),
            Some(&expected),
            "malformed complete grant shape {malformed}"
        );
        assert_eq!(r.f.core.facts().attempts_started(), 1);
        assert_old_failure(&r, &first);
        assert!(!r.f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Native(
                NativeEffect::ActivateReservation { .. } | NativeEffect::ActivateAttempt { .. }
            )
        )));
    }
}
#[test]
fn preparation_and_qualification_refusals_become_logical_cause_without_rewriting_failed_attempt() {
    for qualification in [false, true] {
        let (mut r, _, first) = first_unknown();
        let original = QueryExecutionError::from_execution(ExecutionFailureInput::new(
            QueryFailureReason::PreparationProtocol.failure_fact(),
            SafeDetail::truncating(if qualification {
                "original qualification owner refusal"
            } else {
                "original successor preparation refusal"
            }),
            ExecutionFailureSite::Coordinator,
        ));
        if qualification {
            let (d, _) = r.prepare(roster(2));
            r.f.ack(
                d.action,
                ActionResult::Native(NativeFact::QualificationSettled {
                    identity: d.identity,
                    operation_id: d.operation,
                    dispatched_at: None,
                    settlement: QualificationSettlement::Rejected(original.clone()),
                }),
            );
        } else {
            let prepare = r.f.take(|b| {
                matches!(
                    b,
                    LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
                )
            });
            r.f.ack(
                prepare.action,
                ActionResult::Native(NativeFact::Failed {
                    execution: activation(2).execution(),
                    failure: FailureReport {
                        error: original.clone(),
                        original_attempt_failure: None,
                        pre_ready_replacement: None,
                    },
                }),
            );
        }
        assert_eq!(r.f.core.facts().failure(), Some(&original));
        assert_old_failure(&r, &first);
        assert_eq!(r.f.core.facts().attempts_started(), 1);
    }
}
#[test]
fn late_old_establish_and_positive_convergence_cannot_mutate_the_running_successor() {
    let (mut r, old, first) = first_unknown();
    let (a, grants) = successor(&mut r, 2);
    establish_successor(&mut r, a, &grants);
    for issued in old {
        r.settle(
            issued,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    }
    let closure=AttemptConvergenceClosure::Evidence(roster(1).into_iter().map(|p|(p,novarocks_coordination_contract::AttemptParticipationConvergence::WorkerStoppedAndParticipationFenced)).collect::<Vec<_>>());
    r.send_port(
        activation(1),
        AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(closure)),
            lifecycle_runtime_filters: None,
        },
    );
    assert_eq!(r.f.core.facts().current_activation(), Some(a));
    assert_eq!(r.f.core.facts().conclusion(), None);
    let successor =
        r.f.core
            .facts()
            .attempts()
            .find(|f| f.activation == a)
            .unwrap();
    assert!(successor.authority_open);
    assert!(successor.accepted_failure.is_none());
    assert!(!successor.all_participations_closed);
    assert_old_failure(&r, &first);
}
#[test]
fn successor_terminal_failure_and_prior_success_keep_distinct_original_attempt_causes() {
    let (mut r, _, first) = first_unknown();
    let (a, grants) = successor(&mut r, 2);
    establish_successor(&mut r, a, &grants);
    let later = ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating("successor original terminal error"),
        ExecutionFailureSite::Coordinator,
    ));
    r.fail(a, later.clone());
    assert_eq!(r.f.core.facts().failure(), Some(later.error()));
    assert_old_failure(&r, &first);
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|f| f.activation == a)
            .unwrap()
            .accepted_failure,
        Some(later.input())
    );
}
#[test]
fn accepted_scope_cancellation_wins_over_late_initial_activation_failure() {
    let mut f = Fixture::with_start(start(LogicalOutputMode::CompletionOnly));
    f.send(LogicalInput::Started);
    let activate = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
        )
    });
    f.send(LogicalInput::Cancelled(novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::FrontendDrainDeadlineExceeded));
    let original = f.core.facts().failure().unwrap().clone();
    let late = backend_failure(1, "late activation failure");
    f.ack(
        activate.action,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure: FailureReport::from_attempt(&late),
        }),
    );
    assert_eq!(f.core.facts().failure(), Some(&original));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
    )));
}
#[test]
fn native_backend_failure_without_any_root_status_can_prepare_live_snapshot() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    assert!(
        r.f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .root_projection
            .is_none()
    );
    let original = backend_failure(1, "native original connection failure before root status");
    let run = r.f.run.take().unwrap();
    r.f.result(
        run,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure: FailureReport::from_attempt(&original),
        }),
    );
    let prepare = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    assert!(
        matches!(prepare.body,LogicalEffectBody::Native(NativeEffect::PrepareSuccessor{failed,topology:SuccessorTopology::LiveSnapshot,..})if failed==activation(1).execution())
    );
    assert_old_failure(&r, &original);
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(
        !r.f.effects
            .iter()
            .any(|e| matches!(e.body, LogicalEffectBody::Relay(RelayEffect::Start { .. })))
    );
}
fn witnessed(r: &mut RecoveryFixture, observed: bool) -> (Issued, ExecutionFailure) {
    let p = participant(1, 1);
    let grant = r.grant(activation(1), p, 10);
    let issued = r.issue(activation(1), p, grant, 10);
    if observed {
        r.settle(issued, AttemptIssueSettlement::TransportOwned)
            .unwrap();
        r.settle(issued, AttemptIssueSettlement::TransportUnknown)
            .unwrap();
    }
    let op = operation(6000);
    let input = ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("original CreateTask identity refusal"),
        ExecutionFailureSite::Operation {
            participation: p,
            operation_id: op,
            kind: OperationKind::CreateTask,
        },
    );
    let witness = PreReadyProcessReplacementWitness::try_new(
        &input,
        p,
        op,
        participant(2, 3).backend_process_id(),
    )
    .unwrap();
    let mut original = ExecutionFailure::from_input(input);
    original.attach_pre_ready_replacement(witness).unwrap();
    (issued, original)
}
#[test]
fn witnessed_identity_refusal_uses_exclude_process_and_preserves_original_failure_class() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    let (_, original) = witnessed(&mut r, true);
    assert_eq!(
        original.class(),
        novarocks_coordination_contract::AttemptFailureClass::ContractViolation
    );
    r.fail(activation(1), original.clone());
    let replacement = vec![participant(2, 3), participant(2, 4)];
    let (d, topology) = r.prepare(replacement.clone());
    assert!(
        matches!(topology,SuccessorTopology::ExcludeProcess(process)if process==participant(1,1).backend_process_id())
    );
    assert_eq!(d.replacement.as_ref(), replacement);
    assert_eq!(d.expires_at, at(50));
    assert_old_failure(&r, &original);
}
#[test]
fn witness_needs_observed_parent_and_cannot_override_recovery_policy_or_external_effect() {
    for (observed, mode, effect) in [
        (
            false,
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
        ),
        (true, RecoveryMode::NoRecovery, ExecutionEffect::None),
    ] {
        let mut r = RecoveryFixture::with_policy(LogicalOutputMode::ResultStream, mode, effect);
        let (_, original) = witnessed(&mut r, observed);
        r.fail(activation(1), original.clone());
        assert_eq!(r.f.core.facts().failure(), Some(original.error()));
        assert!(!r.f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )));
    }
}
#[test]
fn external_effect_restart_is_refused_before_a_core_or_native_side_effect_exists() {
    let mut original = start(LogicalOutputMode::InternalResultStream);
    original.first_attempt.participations = roster(1).into_boxed_slice();
    original.policy.effect = ExecutionEffect::External;
    original.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    original.budgets.replacement_reservation_valid_for = Some(std::time::Duration::from_millis(50));
    assert_eq!(original.validate(), Err(StartRefusal::ExternalRecovery));
}
#[test]
fn visible_nonempty_attempt_failure_never_prepares_a_successor() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
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
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
    );
    let schema = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    r.f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::SchemaWritten),
    );
    // The original queue-position precondition is represented by this public
    // request; this test does not fabricate a physical payload or receipt.
    r.relay(activation(1), RelayRequest::RequestVisibility);
    assert!(r.f.core.facts().output_visible());
    let original = backend_failure(1, "original visible attempt connection failure");
    r.fail(activation(1), original.clone());
    assert_eq!(r.f.core.facts().failure(), Some(original.error()));
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
    )));
    assert_old_failure(&r, &original);
}
#[test]
fn late_parent_refusal_during_preparation_revokes_witness_before_dispatch() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    let (issued, original) = witnessed(&mut r, true);
    r.fail(activation(1), original.clone());
    let prepare = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    r.settle(
        issued,
        AttemptIssueSettlement::Worker(OperationOutcome::CompatibilityMismatch),
    )
    .unwrap();
    r.f.ack(
        prepare.action,
        ActionResult::Native(NativeFact::SuccessorPrepared {
            execution: activation(2).execution(),
            participations: Arc::from(vec![participant(2, 3), participant(2, 4)]),
        }),
    );
    assert_eq!(r.f.core.facts().failure(), Some(original.error()));
    assert_old_failure(&r, &original);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::DispatchQualification { .. } | NativeEffect::ActivateAttempt { .. }
        )
    )));
    assert!(r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ConvergeDormant{execution,..})if execution==activation(2).execution())));
}
#[test]
fn qualification_ticket_expiry_cannot_activate_with_a_renewed_lifetime() {
    let (mut r, _, first) = first_unknown();
    let (d, _) = r.prepare(roster(2));
    let grants = d
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 20))
        .collect::<Vec<_>>();
    r.f.now = at(10);
    r.qualify(&d, grants, at(5));
    let activate = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::ActivateReservation { .. })
        )
    });
    r.f.now = at(25);
    r.f.ack(
        activate.action,
        ActionResult::Native(NativeFact::ReservationActivated {
            identity: d.identity,
            operation_id: d.operation,
        }),
    );
    assert_eq!(
        r.f.core.facts().failure(),
        Some(&ReplacementQualificationFailure::Expired.error())
    );
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert_old_failure(&r, &first);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
    )));
}
#[test]
fn accepted_cancellation_during_issued_preparation_closes_late_dormant_without_qualification() {
    use novarocks_logical_execution_protocol::cancel::{
        LogicalCancellationReason, cancellation_decision,
    };
    for reason in [
        LogicalCancellationReason::Requested,
        LogicalCancellationReason::DeadlineExceeded,
    ] {
        let (mut r, _, first) = first_unknown();
        let prepare = r.f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            )
        });
        r.f.send(LogicalInput::Cancelled(reason));
        assert_eq!(
            r.f.core.facts().failure(),
            Some(&cancellation_decision(reason).failure)
        );
        assert_old_failure(&r, &first);
        assert!(!r.f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Native(
                NativeEffect::DispatchQualification { .. } | NativeEffect::ActivateAttempt { .. }
            )
        )));
        r.f.ack(
            prepare.action,
            ActionResult::Native(NativeFact::SuccessorPrepared {
                execution: activation(2).execution(),
                participations: Arc::from(roster(2)),
            }),
        );
        assert!(r.f.effects.iter().any(|e| matches!(e.body, LogicalEffectBody::Native(NativeEffect::ConvergeDormant { execution, .. }) if execution == activation(2).execution())));
        assert!(!r.f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Native(
                NativeEffect::DispatchQualification { .. } | NativeEffect::ActivateAttempt { .. }
            )
        )));
        assert_eq!(r.f.core.facts().attempts_started(), 1);
        assert_old_failure(&r, &first);
    }
}
#[test]
fn qualification_unknown_is_its_own_failure_and_cannot_claim_backend_loss() {
    let (mut r, _, first) = first_unknown();
    let (d, _) = r.prepare(roster(2));
    r.f.now = at(10);
    r.f.ack(
        d.action,
        ActionResult::Native(NativeFact::QualificationSettled {
            identity: d.identity,
            operation_id: d.operation,
            dispatched_at: Some(at(5)),
            settlement: QualificationSettlement::OutcomeUnknown,
        }),
    );
    let expected = ReplacementQualificationFailure::OutcomeUnknown.error();
    assert_eq!(r.f.core.facts().failure(), Some(&expected));
    assert_ne!(
        expected.execution_failure().unwrap().fact(),
        QueryFailureReason::BackendLost.failure_fact()
    );
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert_old_failure(&r, &first);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::ActivateReservation { .. }
                | NativeEffect::ActivateAttempt { .. }
                | NativeEffect::PrepareSuccessor { .. }
        )
    )));
}

fn published_result_errors(r: &RecoveryFixture) -> Vec<QueryExecutionError> {
    r.f.effects
        .iter()
        .filter_map(|effect| match &effect.body {
            LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error)) => {
                Some(error.clone())
            }
            _ => None,
        })
        .collect()
}
// Baseline actor::recovered_attempt_failure_does_not_escape_its_activation
// keeps A's client input for rejected qualification, while logical setup and
// per-attempt diagnostic causes remain separate values (spec section 5.9).
fn assert_client_setup_failure_keeps_original_input(qualification: bool) {
    let (mut r, _, first) = first_unknown();
    assert!(
        published_result_errors(&r).is_empty(),
        "the pending replacement has not published any client failure"
    );
    let later = if qualification {
        ReplacementQualificationFailure::Rejected.error()
    } else {
        novarocks_logical_execution_protocol::failure::query_failure(
            QueryFailureReason::PreparationProtocol,
            "original replacement prepare error",
        )
    };
    if qualification {
        let (d, _) = r.prepare(roster(2));
        r.f.ack(
            d.action,
            ActionResult::Native(NativeFact::QualificationSettled {
                identity: d.identity,
                operation_id: d.operation,
                dispatched_at: None,
                settlement: QualificationSettlement::Rejected(later.clone()),
            }),
        );
    } else {
        let prepare = r.f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            )
        });
        r.f.ack(
            prepare.action,
            ActionResult::Native(NativeFact::Failed {
                execution: activation(2).execution(),
                failure: FailureReport {
                    error: later.clone(),
                    original_attempt_failure: None,
                    pre_ready_replacement: None,
                },
            }),
        );
    }
    assert_eq!(r.f.core.facts().failure(), Some(&later));
    assert_old_failure(&r, &first);
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert_eq!(
        published_result_errors(&r),
        vec![first.error().clone()],
        "a client result with no activated successor retains the original failed-attempt input"
    );
}
#[test]
fn replacement_prepare_failure_keeps_client_input_separate_from_logical_setup_error() {
    assert_client_setup_failure_keeps_original_input(false);
}
#[test]
fn qualification_rejection_keeps_client_input_separate_from_logical_qualification_error() {
    assert_client_setup_failure_keeps_original_input(true);
}
#[test]
fn internal_replacement_setup_failure_publishes_the_actual_setup_error() {
    for qualification in [false, true] {
        let mut r = RecoveryFixture::new(LogicalOutputMode::InternalResultStream);
        r.establish_initial(AttemptIssueSettlement::TransportUnknown);
        let first = backend_failure(1, "original internal attempt backend failure");
        r.fail(activation(1), first.clone());
        let later = ReplacementQualificationFailure::Rejected.error();
        if qualification {
            let (d, _) = r.prepare(roster(2));
            r.f.ack(
                d.action,
                ActionResult::Native(NativeFact::QualificationSettled {
                    identity: d.identity,
                    operation_id: d.operation,
                    dispatched_at: None,
                    settlement: QualificationSettlement::Rejected(later.clone()),
                }),
            );
        } else {
            let prepare = r.f.take(|b| {
                matches!(
                    b,
                    LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
                )
            });
            r.f.ack(
                prepare.action,
                ActionResult::Native(NativeFact::Failed {
                    execution: activation(2).execution(),
                    failure: FailureReport {
                        error: later.clone(),
                        original_attempt_failure: None,
                        pre_ready_replacement: None,
                    },
                }),
            );
        }
        assert_eq!(r.f.core.facts().failure(), Some(&later));
        assert_old_failure(&r, &first);
        assert_eq!(published_result_errors(&r), vec![later]);
    }
}
#[test]
fn accepted_scope_cancellation_during_recovery_overrides_client_fallback_exactly_once() {
    use novarocks_logical_execution_protocol::cancel::{
        LogicalCancellationReason, cancellation_decision,
    };
    for reason in [
        LogicalCancellationReason::Requested,
        LogicalCancellationReason::DeadlineExceeded,
        LogicalCancellationReason::FrontendDrainDeadlineExceeded,
    ] {
        for qualification in [false, true] {
            let (mut r, _, first) = first_unknown();
            let pending = if qualification {
                let (d, _) = r.prepare(roster(2));
                (d.action, Some(d))
            } else {
                let prepare = r.f.take(|b| {
                    matches!(
                        b,
                        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
                    )
                });
                (prepare.action, None)
            };
            r.f.send(LogicalInput::Cancelled(reason));
            let accepted = cancellation_decision(reason).failure;
            assert_eq!(r.f.core.facts().failure(), Some(&accepted));
            assert_old_failure(&r, &first);
            assert_eq!(published_result_errors(&r), vec![accepted.clone()]);
            let late = ReplacementQualificationFailure::Rejected.error();
            if let Some(d) = pending.1 {
                r.f.ack(
                    pending.0,
                    ActionResult::Native(NativeFact::QualificationSettled {
                        identity: d.identity,
                        operation_id: d.operation,
                        dispatched_at: None,
                        settlement: QualificationSettlement::Rejected(late),
                    }),
                );
            } else {
                r.f.ack(
                    pending.0,
                    ActionResult::Native(NativeFact::Failed {
                        execution: activation(2).execution(),
                        failure: FailureReport {
                            error: late,
                            original_attempt_failure: None,
                            pre_ready_replacement: None,
                        },
                    }),
                );
            }
            assert_eq!(r.f.core.facts().failure(), Some(&accepted));
            assert_eq!(published_result_errors(&r), vec![accepted]);
            assert_old_failure(&r, &first);
        }
    }
}
#[test]
fn activated_successor_owns_client_failure_and_does_not_inherit_predecessor_fallback() {
    let (mut r, _, first) = first_unknown();
    let (a, _) = successor(&mut r, 2);
    let own = ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating("successor own producer error"),
        ExecutionFailureSite::Coordinator,
    ));
    r.fail(a, own.clone());
    assert_eq!(r.f.core.facts().failure(), Some(own.error()));
    assert_eq!(published_result_errors(&r), vec![own.error().clone()]);
    assert_old_failure(&r, &first);
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|attempt| attempt.activation == a)
            .unwrap()
            .accepted_failure,
        Some(own.input())
    );
}
#[test]
fn successful_successor_never_publishes_the_predecessor_client_failure() {
    let (mut r, _, first) = first_unknown();
    let (a, grants) = successor(&mut r, 2);
    establish_successor(&mut r, a, &grants);
    assert!(published_result_errors(&r).is_empty());
    r.succeed_empty_stream(a);
    r.fail(
        activation(1),
        backend_failure(1, "late predecessor terminal observation"),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(r.f.core.facts().failure().is_none());
    assert!(published_result_errors(&r).is_empty());
    assert_old_failure(&r, &first);
    assert!(
        r.f.core
            .facts()
            .attempts()
            .find(|attempt| attempt.activation == a)
            .unwrap()
            .accepted_failure
            .is_none()
    );
}
#[test]
fn successor_instantiation_failure_already_owns_the_client_result_after_logical_activation() {
    let (mut r, _, first) = first_unknown();
    let (d, _) = r.prepare(roster(2));
    let grants = d
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
        .collect();
    r.f.now = at(10);
    r.qualify(&d, grants, at(5));
    let reservation = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::ActivateReservation { .. })
        )
    });
    r.f.ack(
        reservation.action,
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
    assert_eq!(r.f.core.facts().current_activation(), Some(a));
    let register = r.f.take(|b| matches!(b, LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { activation, .. }) if *activation == a));
    r.f.ack(
        register.action,
        ActionResult::Governance(GovernanceFact::ObligationRegistered {
            activation: a,
            kind: ObligationKind::RunningWork,
        }),
    );
    let activation = r.f.take(|b| matches!(b, LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation, .. }) if *activation == a));
    let own = ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating("successor original activation failure"),
        ExecutionFailureSite::Coordinator,
    ));
    r.f.ack(
        activation.action,
        ActionResult::Native(NativeFact::Failed {
            execution: a.execution(),
            failure: FailureReport::from_attempt(&own),
        }),
    );
    assert_eq!(r.f.core.facts().failure(), Some(own.error()));
    assert_eq!(published_result_errors(&r), vec![own.error().clone()]);
    assert_old_failure(&r, &first);
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|attempt| attempt.activation == a)
            .unwrap()
            .accepted_failure,
        Some(own.input())
    );
}
#[test]
fn explicit_schema_producer_failure_during_recovery_is_not_masked_by_attempt_fallback() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
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
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
    );
    let schema = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    let first = backend_failure(1, "original failed attempt before schema settled");
    r.fail(activation(1), first.clone());
    let (_, _) = r.prepare(roster(2));
    let explicit = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::ResultProtocol,
        "original explicit schema producer error",
    );
    r.f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::Failed(explicit.clone())),
    );
    assert_eq!(r.f.core.facts().failure(), Some(&explicit));
    assert_eq!(published_result_errors(&r), vec![explicit]);
    assert_old_failure(&r, &first);
}

fn settle_replacement_setup_error(
    r: &mut RecoveryFixture,
    qualification: bool,
    error: QueryExecutionError,
) {
    if qualification {
        let (d, _) = r.prepare(roster(2));
        r.f.ack(
            d.action,
            ActionResult::Native(NativeFact::QualificationSettled {
                identity: d.identity,
                operation_id: d.operation,
                dispatched_at: None,
                settlement: QualificationSettlement::Rejected(error),
            }),
        );
    } else {
        let prepare = r.f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            )
        });
        r.f.ack(
            prepare.action,
            ActionResult::Native(NativeFact::Failed {
                execution: activation(2).execution(),
                failure: FailureReport {
                    error,
                    original_attempt_failure: None,
                    pre_ready_replacement: None,
                },
            }),
        );
    }
}

#[test]
fn original_setup_scope_cancellation_waits_for_observer_to_select_exact_reason_and_stop() {
    use novarocks_coordination_contract::AttemptFailureClass;
    use novarocks_execution_contract::failure::FailureEvidence;
    use novarocks_logical_execution_protocol::cancel::{
        LogicalCancellationReason, cancellation_decision,
    };
    for (reason, observer, expected_class, detail) in [
        (
            QueryFailureReason::Cancelled,
            LogicalCancellationReason::Requested,
            AttemptFailureClass::Cancelled,
            "Native replacement preparation was cancelled",
        ),
        (
            QueryFailureReason::DeadlineExceeded,
            LogicalCancellationReason::DeadlineExceeded,
            AttemptFailureClass::DeadlineExceeded,
            "Native replacement qualification was cancelled by original deadline",
        ),
        (
            QueryFailureReason::DeadlineExceeded,
            LogicalCancellationReason::FrontendDrainDeadlineExceeded,
            AttemptFailureClass::DeadlineExceeded,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
        ),
    ] {
        for qualification in [false, true] {
            let (mut r, _, first) = first_unknown();
            let original = ExecutionFailureInput::new(
                reason.failure_fact(),
                SafeDetail::truncating(detail),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(original.fact().evidence(), FailureEvidence::Originating);
            assert_eq!(original.fact().reason(), reason.failure_fact().reason());
            assert_eq!(original.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(original.attempt_class(), expected_class);
            let error = QueryExecutionError::from_execution(original.clone());
            assert!(published_result_errors(&r).is_empty());
            // The Native original cancellation wait settles first. The scope
            // observer has not delivered its separate cancellation input yet.
            assert_eq!(error.execution_failure(), Some(&original));
            settle_replacement_setup_error(&mut r, qualification, error);
            // The closed fact proves a scope cause, but cannot recover which
            // original latch reason selected the required AP Stop action.
            assert_eq!(r.f.core.facts().conclusion(), None);
            assert!(r.f.core.facts().failure().is_none());
            assert!(published_result_errors(&r).is_empty());
            assert!(!r.f.effects.iter().any(|effect| matches!(effect.body, LogicalEffectBody::AttemptPort(ref message) if matches!(message.body, LogicalToAttemptBody::Stop { .. }))));
            assert!(!r.f.effects.iter().any(|effect| matches!(
                effect.body,
                LogicalEffectBody::Native(
                    NativeEffect::PrepareSuccessor { .. }
                        | NativeEffect::DispatchQualification { .. }
                        | NativeEffect::ActivateReservation { .. }
                        | NativeEffect::ActivateAttempt { .. }
                )
            )));
            assert_old_failure(&r, &first);
            assert_eq!(r.f.core.facts().attempts_started(), 1);
            let accepted = cancellation_decision(observer);
            r.f.send(LogicalInput::Cancelled(observer));
            assert_eq!(r.f.core.facts().conclusion(), Some(accepted.conclusion));
            assert_eq!(r.f.core.facts().failure(), Some(&accepted.failure));
            assert_eq!(published_result_errors(&r), vec![accepted.failure.clone()]);
            let stops =
                r.f.effects
                    .iter()
                    .filter_map(|effect| match &effect.body {
                        LogicalEffectBody::AttemptPort(message) => match &message.body {
                            LogicalToAttemptBody::Stop { reason, failure } => {
                                Some((message.activation, *reason, failure.error().clone()))
                            }
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>();
            assert_eq!(
                stops,
                vec![(activation(1), accepted.stop, accepted.failure.clone())]
            );
            r.f.send(LogicalInput::Cancelled(
                LogicalCancellationReason::ClientDisconnected,
            ));
            assert_eq!(r.f.core.facts().failure(), Some(&accepted.failure));
            assert_eq!(published_result_errors(&r), vec![accepted.failure]);
            assert_old_failure(&r, &first);
        }
    }
}

#[test]
fn task_control_derived_stop_and_recovery_budget_are_not_original_scope_cancellation() {
    use novarocks_coordination_contract::AttemptFailureClass;
    use novarocks_execution_contract::failure::FailureEvidence;
    let cases = [
        ExecutionFailureInput::new(
            QueryFailureReason::TaskCancelled.failure_fact(),
            SafeDetail::truncating("original Task control cancellation"),
            ExecutionFailureSite::TaskControl { identity: root(1) },
        ),
        ExecutionFailureInput::new(
            QueryFailureReason::Cancelled.failure_fact(),
            SafeDetail::truncating("Query reason at a Task control site"),
            ExecutionFailureSite::TaskControl { identity: root(1) },
        ),
        ExecutionFailureInput::new(
            FailureFact::stop_observation(QueryFailureReason::Cancelled.failure_fact().reason()),
            SafeDetail::truncating("derived stop with the same Query reason"),
            ExecutionFailureSite::Coordinator,
        ),
        ExecutionFailureInput::new(
            QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact(),
            SafeDetail::truncating("original replacement preparation budget expired"),
            ExecutionFailureSite::Coordinator,
        ),
    ];
    assert_eq!(cases[0].fact().evidence(), FailureEvidence::Originating);
    assert_eq!(
        cases[0].site(),
        ExecutionFailureSite::TaskControl { identity: root(1) }
    );
    assert_eq!(
        cases[1].fact().reason(),
        QueryFailureReason::Cancelled.failure_fact().reason()
    );
    assert_eq!(cases[2].fact().evidence(), FailureEvidence::StopObservation);
    assert_eq!(
        cases[2].fact().reason(),
        QueryFailureReason::Cancelled.failure_fact().reason()
    );
    assert_eq!(
        cases[3].fact(),
        QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact()
    );
    for original in cases {
        assert!(!matches!(
            original.attempt_class(),
            AttemptFailureClass::Cancelled | AttemptFailureClass::DeadlineExceeded
        ));
        for qualification in [false, true] {
            let (mut r, _, first) = first_unknown();
            let setup = QueryExecutionError::from_execution(original.clone());
            settle_replacement_setup_error(&mut r, qualification, setup.clone());
            assert_eq!(r.f.core.facts().failure(), Some(&setup));
            assert_eq!(
                r.f.core.facts().failure().unwrap().execution_failure(),
                Some(&original)
            );
            assert_old_failure(&r, &first);
            assert_eq!(
                published_result_errors(&r),
                vec![first.error().clone()],
                "a Task or derived stop or recovery budget is a setup cause, not scope cancellation authority"
            );
            assert_eq!(r.f.core.facts().attempts_started(), 1);
            assert!(!r.f.effects.iter().any(|effect| matches!(
                effect.body,
                LogicalEffectBody::Native(
                    NativeEffect::ActivateReservation { .. } | NativeEffect::ActivateAttempt { .. }
                )
            )));
        }
    }
}

fn recovery_with_running_obligation_held() -> (RecoveryFixture, LogicalActionId) {
    let mut original = start(LogicalOutputMode::ResultStream);
    original.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    original.budgets.replacement_reservation_valid_for = Some(std::time::Duration::from_millis(50));
    original.first_attempt.participations = roster(1).into_boxed_slice();
    let mut r = RecoveryFixture::unstarted(original);
    r.f.send(LogicalInput::Started);
    let registration = r.f.take(|b| matches!(b, LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { activation: a, kind: ObligationKind::RunningWork }) if *a == activation(1)));
    let expose = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ExposeOutput { .. })
        )
    });
    r.f.ack(
        expose.action,
        ActionResult::Consumer(ConsumerFact::OutputExposed),
    );
    let activate = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
        )
    });
    r.f.ack(
        activate.action,
        ActionResult::Native(NativeFact::AttemptActivated {
            activation: activation(1),
            root: Some(root(1)),
        }),
    );
    let run = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
        )
    });
    r.f.run = Some(run.action);
    (r, registration.action)
}
fn qualify_nonempty(r: &mut RecoveryFixture, d: &Dispatch) -> Vec<QualifiedAdmission> {
    let grants = d
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
        .collect::<Vec<_>>();
    r.f.now = at(10);
    r.qualify(d, grants.clone(), at(5));
    grants
}
#[test]
fn qualified_nonempty_successor_waits_for_original_running_work_registration() {
    let (mut r, registration) = recovery_with_running_obligation_held();
    r.establish_initial(AttemptIssueSettlement::TransportUnknown);
    let first = backend_failure(
        1,
        "original backend failure while registration is outstanding",
    );
    r.fail(activation(1), first.clone());
    let (d, _) = r.prepare(roster(2));
    let grants = qualify_nonempty(&mut r, &d);
    assert_eq!(grants.len(), 2);
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::ActivateReservation { .. } | NativeEffect::ActivateAttempt { .. }
        )
    )));
    assert_old_failure(&r, &first);
    r.f.send(LogicalInput::Timer);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::ActivateReservation { .. })
    )));
    r.f.ack(
        registration,
        ActionResult::Governance(GovernanceFact::ObligationRegistered {
            activation: activation(1),
            kind: ObligationKind::RunningWork,
        }),
    );
    assert_eq!(r.f.effects.iter().filter(|e| matches!(e.body, LogicalEffectBody::Native(NativeEffect::ActivateReservation { identity, .. }) if identity == d.identity)).count(), 1);
    let (a, inherited) = r.activate(&d);
    assert_eq!(inherited.len(), 2);
    assert_eq!(r.f.core.facts().current_activation(), Some(a));
    assert_old_failure(&r, &first);
}
#[test]
fn authorized_establish_already_requires_exact_abort_before_any_transport_observation() {
    use novarocks_logical_execution_protocol::facts::{
        AbortParticipationIssueState, ParticipationClosureState,
    };
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    let issued = roster(1)
        .into_iter()
        .enumerate()
        .map(|(i, p)| {
            let grant = r.grant(activation(1), p, 10 + i as u8);
            r.issue(activation(1), p, grant, 10 + i as u8)
        })
        .collect::<Vec<_>>();
    let first = backend_failure(
        1,
        "original failure before any authorized transport handoff",
    );
    r.fail(activation(1), first.clone());
    let (d, _) = r.prepare(roster(2));
    let before=issued.iter().map(|i| {
        let fact=r.f.core.facts().participation(activation(1),i.participation).unwrap();
        assert_eq!(fact.closure,ParticipationClosureState::AbortPending);
        assert_eq!(fact.abort_authorizations,1);
        assert_eq!(fact.abort_issue_state,Some(AbortParticipationIssueState::AwaitingSettlement));
        assert!(fact.abort_operation.is_some());
        assert!(r.f.effects.iter().any(|e| matches!(&e.body,LogicalEffectBody::AttemptPort(message) if message.activation==activation(1)&&matches!(&message.body,LogicalToAttemptBody::Abort{action,request} if request.participation()==i.participation && Some(request.envelope().operation_id())==fact.abort_operation && Some(*action)==fact.current_abort_action))));
        fact
    }).collect::<Vec<_>>();
    qualify_nonempty(&mut r, &d);
    let (a, _) = r.activate(&d);
    for (i, original) in issued.into_iter().zip(before) {
        r.settle(i, AttemptIssueSettlement::TransportOwned).unwrap();
        r.settle(i, AttemptIssueSettlement::TransportUnknown)
            .unwrap();
        let after =
            r.f.core
                .facts()
                .participation(activation(1), i.participation)
                .unwrap();
        assert_eq!(after.abort_operation, original.abort_operation);
        assert_eq!(after.abort_authorizations, 1);
        assert_eq!(
            after.abort_issue_state,
            Some(AbortParticipationIssueState::AwaitingSettlement)
        );
        assert_eq!(after.current_abort_action, original.current_abort_action);
        assert_eq!(after.closure, ParticipationClosureState::AbortPending);
    }
    assert_eq!(r.f.core.facts().current_activation(), Some(a));
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert_old_failure(&r, &first);
}
#[test]
fn late_old_worker_refusals_do_not_gate_nonempty_successor_establish_or_success() {
    for outcome in [
        OperationOutcome::CompatibilityMismatch,
        OperationOutcome::IdentityMismatch,
        OperationOutcome::OwnerFailed,
    ] {
        let (mut r, old, first) = first_unknown();
        let (a, grants) = successor(&mut r, 2);
        for issued in old {
            r.settle(issued, AttemptIssueSettlement::Worker(outcome))
                .unwrap();
        }
        establish_successor(&mut r, a, &grants);
        assert_eq!(r.f.core.facts().conclusion(), None);
        assert!(
            r.f.core
                .facts()
                .attempts()
                .find(|attempt| attempt.activation == a)
                .unwrap()
                .accepted_failure
                .is_none()
        );
        assert!(published_result_errors(&r).is_empty());
        r.succeed_empty_stream(a);
        assert_eq!(
            r.f.core.facts().conclusion(),
            Some(LogicalConclusion::Succeeded)
        );
        assert!(
            !r.f.core
                .facts()
                .attempts()
                .find(|attempt| attempt.activation == activation(1))
                .unwrap()
                .all_participations_closed
        );
        assert!(published_result_errors(&r).is_empty());
        assert_old_failure(&r, &first);
    }
}
#[test]
fn public_recovery_refusal_matrix_keeps_original_failure_and_starts_no_successor() {
    for case in 0..8 {
        let mut original = start(LogicalOutputMode::ResultStream);
        original.policy.recovery = if case == 0 {
            RecoveryMode::NoRecovery
        } else {
            RecoveryMode::RestartAttemptBeforeVisibility
        };
        original.budgets.replacement_reservation_valid_for =
            Some(std::time::Duration::from_millis(50));
        original.first_attempt.participations = roster(1).into_boxed_slice();
        if case == 1 {
            original.budgets.max_attempts = std::num::NonZeroU32::new(1).unwrap();
        }
        let mut r = RecoveryFixture::unstarted(original);
        r.f.begin();
        r.establish_initial(AttemptIssueSettlement::TransportUnknown);
        let input = if case < 2 {
            backend_failure(1, "original BackendLost refused by closed policy")
        } else {
            let class = [
                FailureClass::Unsupported,
                FailureClass::Invariant,
                FailureClass::Resource,
                FailureClass::Data,
                FailureClass::Unexpected,
                FailureClass::Control,
            ][case - 2];
            ExecutionFailure::from_input(ExecutionFailureInput::new(
                FailureFact::new(
                    class,
                    FailureReason::new(domain::QUERY, NonZeroU16::new(610 + case as u16).unwrap()),
                ),
                SafeDetail::truncating("original nonrecoverable producer fact"),
                ExecutionFailureSite::Coordinator,
            ))
        };
        r.fail(activation(1), input.clone());
        assert_eq!(
            r.f.core.facts().conclusion(),
            Some(LogicalConclusion::Failed),
            "case {case}"
        );
        assert_eq!(
            r.f.core.facts().failure(),
            Some(input.error()),
            "case {case}"
        );
        assert_eq!(r.f.core.facts().attempts_started(), 1);
        assert_eq!(published_result_errors(&r), vec![input.error().clone()]);
        assert!(!r.f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Native(
                NativeEffect::PrepareSuccessor { .. }
                    | NativeEffect::DispatchQualification { .. }
                    | NativeEffect::ActivateAttempt { .. }
            )
        )));
        assert_old_failure(&r, &input);
    }
}
#[test]
fn cancellation_of_qualified_or_issued_activation_abandons_once_without_releasing_old_residual() {
    use novarocks_logical_execution_protocol::cancel::{
        LogicalCancellationReason, cancellation_decision,
    };
    for activation_issued in [false, true] {
        for reason in [
            LogicalCancellationReason::Requested,
            LogicalCancellationReason::DeadlineExceeded,
        ] {
            let (mut r, pending_registration) = if activation_issued {
                (RecoveryFixture::new(LogicalOutputMode::ResultStream), None)
            } else {
                let (r, a) = recovery_with_running_obligation_held();
                (r, Some(a))
            };
            r.establish_initial(AttemptIssueSettlement::TransportUnknown);
            let first = backend_failure(
                1,
                "old attempt remains uncertain while qualified successor is cancelled",
            );
            r.fail(activation(1), first.clone());
            let (d, _) = r.prepare(roster(2));
            qualify_nonempty(&mut r, &d);
            let reservation = if activation_issued {
                Some(r.f.take(|b| {
                    matches!(
                        b,
                        LogicalEffectBody::Native(NativeEffect::ActivateReservation { .. })
                    )
                }))
            } else {
                None
            };
            r.f.send(LogicalInput::Cancelled(reason));
            let decision = cancellation_decision(reason);
            assert_eq!(r.f.core.facts().conclusion(), Some(decision.conclusion));
            assert_eq!(r.f.core.facts().failure(), Some(&decision.failure));
            assert_eq!(published_result_errors(&r), vec![decision.failure.clone()]);
            if let Some(effect) = reservation {
                r.f.ack(
                    effect.action,
                    ActionResult::Native(NativeFact::ReservationActivated {
                        identity: d.identity,
                        operation_id: d.operation,
                    }),
                );
            }
            if let Some(action) = pending_registration {
                r.f.ack(
                    action,
                    ActionResult::Governance(GovernanceFact::ObligationRegistered {
                        activation: activation(1),
                        kind: ObligationKind::RunningWork,
                    }),
                );
            }
            r.f.send(LogicalInput::Timer);
            assert!(!r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ActivateAttempt{activation:a,..}|NativeEffect::RunAttempt{activation:a})if a.execution()==activation(2).execution())));
            assert_eq!(r.f.effects.iter().filter(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::AbandonReservation{identity})if identity==d.identity)).count(),1);
            assert_eq!(r.f.effects.iter().filter(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ConvergeDormant{execution,..})if execution==activation(2).execution())).count(),1);
            let abandon=r.f.take(|b|matches!(b,LogicalEffectBody::Native(NativeEffect::AbandonReservation{identity})if *identity==d.identity));
            r.f.ack(abandon.action, ActionResult::Native(NativeFact::Applied));
            let dormant = r.f.take(|b| {
                matches!(
                    b,
                    LogicalEffectBody::Native(NativeEffect::ConvergeDormant { .. })
                )
            });
            r.f.ack(
                dormant.action,
                ActionResult::Native(NativeFact::DormantConverged {
                    execution: activation(2).execution(),
                }),
            );
            r.f.send(LogicalInput::Timer);
            assert_eq!(r.f.core.facts().attempts_started(), 1);
            assert!(
                !r.f.core
                    .facts()
                    .attempts()
                    .find(|attempt| attempt.activation == activation(1))
                    .unwrap()
                    .all_participations_closed
            );
            assert_eq!(published_result_errors(&r), vec![decision.failure]);
            assert_old_failure(&r, &first);
            assert!(!r.f.effects.iter().any(|e| matches!(
                e.body,
                LogicalEffectBody::Native(
                    NativeEffect::AbandonReservation { .. } | NativeEffect::ConvergeDormant { .. }
                )
            )));
        }
    }
}
#[test]
fn every_qualification_header_axis_is_a_typed_business_refusal_for_the_original_owner() {
    use novarocks_coordination_contract::LogicalExecutionActorId;
    for inner_header in [false, true] {
        for axis in 0..5 {
            let (mut r, _, first) = first_unknown();
            let (d, _) = r.prepare(roster(2));
            let mut identity = d.identity;
            let mut operation_id = d.operation;
            match axis {
                0 => identity.actor = LogicalExecutionActorId::from_raw(identity.actor.get() + 1),
                1 => identity.failed = activation(3).execution(),
                2 => identity.replacement = activation(4).execution(),
                3 => identity.generation += 1,
                4 => operation_id = operation(7000),
                _ => unreachable!(),
            }
            let grants = d
                .replacement
                .iter()
                .enumerate()
                .map(|(i, p)| admission(*p, 30 + i as u8, 1000))
                .collect::<Vec<_>>();
            let facts = QualifiedReplacementFacts {
                identity,
                operation_id,
                topology_revision: 7,
                admissions: grants.into_boxed_slice(),
            };
            r.f.now = at(10);
            r.f.ack(
                d.action,
                ActionResult::Native(NativeFact::QualificationSettled {
                    identity: if inner_header { d.identity } else { identity },
                    operation_id: if inner_header {
                        d.operation
                    } else {
                        operation_id
                    },
                    dispatched_at: Some(at(5)),
                    settlement: QualificationSettlement::Qualified(facts),
                }),
            );
            let expected = ReplacementQualificationFailure::MismatchedReceipt.error();
            assert_eq!(
                r.f.core.facts().failure(),
                Some(&expected),
                "inner={inner_header} axis={axis}"
            );
            assert_eq!(
                expected.execution_failure().unwrap().fact().class(),
                FailureClass::Invariant
            );
            assert_eq!(
                expected.execution_failure().unwrap().fact().reason().code(),
                100
            );
            assert_old_failure(&r, &first);
            assert_eq!(published_result_errors(&r), vec![first.error().clone()]);
            assert!(!r.f.effects.iter().any(|e| matches!(
                e.body,
                LogicalEffectBody::Native(
                    NativeEffect::ActivateReservation { .. } | NativeEffect::ActivateAttempt { .. }
                )
            )));
            assert!(r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::AbandonReservation{identity})if identity==d.identity)));
            assert!(r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ConvergeDormant{execution,..})if execution==d.identity.replacement)));
            assert!(!r.f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::AbandonReservation{identity})if identity!=d.identity)));
        }
    }
}
#[test]
fn qualification_domain_refusal_preserves_its_original_fact_and_separate_client_input() {
    let (mut r, _, first) = first_unknown();
    let (d, _) = r.prepare(roster(2));
    let domain_fact = QueryFailureReason::TransportDeadline.failure_fact();
    let expected = ReplacementQualificationFailure::DomainRefused(domain_fact).error();
    r.f.ack(
        d.action,
        ActionResult::Native(NativeFact::QualificationSettled {
            identity: d.identity,
            operation_id: d.operation,
            dispatched_at: None,
            settlement: QualificationSettlement::Rejected(expected.clone()),
        }),
    );
    assert_eq!(r.f.core.facts().failure(), Some(&expected));
    assert_eq!(
        r.f.core
            .facts()
            .failure()
            .unwrap()
            .execution_failure()
            .unwrap()
            .fact(),
        domain_fact
    );
    assert_eq!(published_result_errors(&r), vec![first.error().clone()]);
    assert_old_failure(&r, &first);
    assert_eq!(r.f.core.facts().attempts_started(), 1);
}
#[test]
fn duplicate_successor_roster_fails_before_qualification_without_overwriting_old_cause() {
    let (mut r, _, first) = first_unknown();
    let prepare = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    let duplicate = participant(2, 1);
    r.f.ack(
        prepare.action,
        ActionResult::Native(NativeFact::SuccessorPrepared {
            execution: activation(2).execution(),
            participations: Arc::from([duplicate, duplicate]),
        }),
    );
    let input =
        r.f.core
            .facts()
            .failure()
            .unwrap()
            .execution_failure()
            .unwrap();
    assert_eq!(input.fact().class(), FailureClass::Invariant);
    assert_eq!(input.fact().reason().code(), 72);
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        input.detail().as_str(),
        "Establish event does not match its authorized request"
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Failed)
    );
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert_old_failure(&r, &first);
    assert_eq!(published_result_errors(&r), vec![first.error().clone()]);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::DispatchQualification { .. }
                | NativeEffect::ActivateReservation { .. }
                | NativeEffect::ActivateAttempt { .. }
        )
    )));
    assert_eq!(r.f.effects.iter().filter(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ConvergeDormant{execution,..})if execution==activation(2).execution())).count(),1);
}
#[test]
fn foreign_prepared_successor_cannot_replace_the_cores_seeded_identity() {
    struct RemainingBudget(usize);
    impl StepBudget for RemainingBudget {
        fn charge(&mut self, amount: usize) -> bool {
            if amount > self.0 {
                false
            } else {
                self.0 -= amount;
                true
            }
        }
    }
    #[derive(Default)]
    struct CapturedEffects(Vec<LogicalEffect>);
    impl EffectSink for CapturedEffects {
        fn emit(&mut self, effect: LogicalEffect) {
            self.0.push(effect);
        }
    }
    let (mut r, _, first) = first_unknown();
    let prepare=r.f.take(|b|matches!(b,LogicalEffectBody::Native(NativeEffect::PrepareSuccessor{replacement,..})if *replacement==activation(2).execution()));
    let mut effects = CapturedEffects::default();
    let result = r.f.core.handle(
        r.f.now,
        LogicalInput::ActionResult {
            action: prepare.action,
            result: ActionResult::Native(NativeFact::SuccessorPrepared {
                execution: activation(3).execution(),
                participations: Arc::from(roster(3)),
            }),
        },
        &mut RemainingBudget(32),
        &mut effects,
    );
    assert_eq!(
        result,
        Err(novarocks_logical_execution_protocol::CoreFault::ForeignExecution)
    );
    assert!(effects.0.is_empty());
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert_old_failure(&r, &first);
}

fn one_bounded_turn(r: &mut RecoveryFixture, input: LogicalInput, units: usize) {
    struct Limited(usize);
    impl StepBudget for Limited {
        fn charge(&mut self, n: usize) -> bool {
            if self.0 < n {
                false
            } else {
                self.0 -= n;
                true
            }
        }
    }
    #[derive(Default)]
    struct Collected(Vec<LogicalEffect>);
    impl EffectSink for Collected {
        fn emit(&mut self, effect: LogicalEffect) {
            self.0.push(effect);
        }
    }
    let mut out = Collected::default();
    r.f.core
        .handle(r.f.now, input, &mut Limited(units), &mut out)
        .unwrap();
    assert!(out.0.len() <= units);
    r.f.effects.extend(out.0);
    r.f.core.check_invariants().unwrap();
}
fn mandatory_failure() -> QueryExecutionError {
    novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::PreparationProtocol,
        "original mandatory supervision operation failed",
    )
}
#[test]
fn mandatory_output_failure_discards_queued_activation_and_refuses_new_business_permissions() {
    use novarocks_coordination_contract::{
        AttemptAuthorizationDecision, logical_port::AttemptAuthorization,
    };
    let mut original = start(LogicalOutputMode::ResultStream);
    original.first_attempt.participations = roster(1).into_boxed_slice();
    let mut r = RecoveryFixture::unstarted(original);
    // Registration and mandatory output handoff consume the entire turn;
    // initial activation remains an unissued intent in the original outbox.
    one_bounded_turn(&mut r, LogicalInput::Started, 2);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
    )));
    let expose = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ExposeOutput { .. })
        )
    });
    let error = mandatory_failure();
    r.f.ack(
        expose.action,
        ActionResult::Consumer(ConsumerFact::Failed(error.clone())),
    );
    assert_eq!(r.f.core.facts().supervision_failure(), Some(&error));
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.core.facts().failure().is_none());
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::ActivateAttempt { .. } | NativeEffect::RunAttempt { .. }
        )
    )));
    assert!(!r.f.core.facts().attempts().next().unwrap().authority_open);
    let grant = admission(participant(1, 1), 10, 1000);
    let Err(AttemptAuthorizationFailure::Rejected(refusal)) = r.authorize(
        activation(1),
        AttemptAuthorization::BeginAdmission(grant.request),
    ) else {
        panic!("supervision failure must close new admission authority");
    };
    assert_eq!(refusal.input().fact().reason().code(), 79);
    assert_eq!(
        refusal.input().detail().as_str(),
        "Establish issue authority was revoked before transport ownership"
    );
    for request in [
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
        RelayRequest::RequestVisibility,
        RelayRequest::EndConsumed {
            root: root(1),
            end: end(0, 0),
            accepted_packets: 0,
            delivered_through: None,
        },
    ] {
        r.relay(activation(1), request);
    }
    assert_eq!(r.f.effects.iter().filter(|e|matches!(&e.body,LogicalEffectBody::Relay(RelayEffect::Reply{response:RelayResponse::Rejected(original),..})if original==&error)).count(),3);
    let register = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { .. })
        )
    });
    r.f.ack(
        register.action,
        ActionResult::Governance(GovernanceFact::ObligationRegistered {
            activation: activation(1),
            kind: ObligationKind::RunningWork,
        }),
    );
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.core.facts().failure().is_none());
    assert_eq!(r.f.core.facts().supervision_failure(), Some(&error));
    assert_eq!(r.f.core.next_deadline(), None);
    assert!(!r.f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::AuthorizationReply{decision:Ok(AttemptAuthorizationDecision::AdmissionStarted(_)|AttemptAuthorizationDecision::EstablishAuthorized(_)),..}))));
}
#[test]
fn supervision_failure_rejects_queued_permissions_but_keeps_original_settlements_and_exits() {
    use novarocks_coordination_contract::{
        AttemptAuthorizationDecision,
        logical_port::{AttemptAuthorization, AttemptToLogical},
    };
    let (mut r, registration) = recovery_with_running_obligation_held();
    let original_admission = admission(participant(1, 1), 10, 1000);
    let AttemptAuthorizationDecision::AdmissionStarted(original_ref) = r
        .authorize(
            activation(1),
            AttemptAuthorization::BeginAdmission(original_admission.request),
        )
        .unwrap()
    else {
        panic!("original reference required");
    };
    let held = r.grant(activation(1), participant(1, 2), 11);
    let original_establish = r.issue(activation(1), participant(1, 2), held, 11);
    r.settle(original_establish, AttemptIssueSettlement::TransportOwned)
        .unwrap();
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
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
    );
    let schema = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    r.f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::SchemaWritten),
    );
    let token = AttemptRequestToken::new(std::num::NonZeroU64::new(880).unwrap());
    one_bounded_turn(
        &mut r,
        LogicalInput::AttemptPort(Box::new(AttemptToLogical {
            activation: activation(1),
            action: AttemptActionId::new(std::num::NonZeroU64::new(881).unwrap()),
            body: AttemptToLogicalBody::AuthorizationRequest {
                token,
                request: AttemptAuthorization::BeginAdmission(original_admission.request),
            },
        })),
        0,
    );
    let visibility_token = LogicalRequestToken::new(std::num::NonZeroU64::new(882).unwrap());
    one_bounded_turn(
        &mut r,
        LogicalInput::Relay {
            source: LogicalSourceId::new(std::num::NonZeroU64::new(883).unwrap()),
            token: visibility_token,
            activation: activation(1),
            request: RelayRequest::RequestVisibility,
        },
        0,
    );
    let error = mandatory_failure();
    r.f.ack(
        registration,
        ActionResult::Governance(GovernanceFact::Failed(error.clone())),
    );
    assert!(r.f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(message)if matches!(&message.body,LogicalToAttemptBody::AuthorizationReply{token:exact,decision:Err(AttemptAuthorizationFailure::Rejected(refusal))}if *exact==token&&refusal.input().fact().reason().code()==79))));
    assert!(r.f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::Relay(RelayEffect::Reply{token:exact,response:RelayResponse::Rejected(original),..})if *exact==visibility_token&&original==&error)));
    assert!(!r.f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::Relay(RelayEffect::Reply{token:exact,response:RelayResponse::VisibilityGranted,..})if *exact==visibility_token)));
    assert!(matches!(
        r.authorize(
            activation(1),
            AttemptAuthorization::SettleAdmission {
                issue: original_ref,
                participation: participant(1, 1),
                operation_id: original_admission.request.envelope().operation_id(),
                outcome: Some(OperationOutcome::Accepted),
                receipt: Some(original_admission.receipt),
                prior_transport_unknown: false
            }
        ),
        Ok(AttemptAuthorizationDecision::AdmissionGranted)
    ));
    r.settle(
        original_establish,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    )
    .unwrap();
    let run = r.f.run.take().unwrap();
    r.f.ack(
        run,
        ActionResult::Native(NativeFact::RunCompleted {
            activation: activation(1),
        }),
    );
    assert_eq!(r.f.core.facts().supervision_failure(), Some(&error));
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.core.facts().failure().is_none());
    assert!(!r.f.core.facts().attempts().next().unwrap().authority_open);
    assert_eq!(r.f.effects.iter().filter(|e|matches!(&e.body,LogicalEffectBody::SupervisionFailed(original)if original==&error)).count(),1);
}
#[test]
fn supervision_failure_after_committed_success_keeps_the_original_queued_end_obligation() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(6).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
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
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
    );
    let schema = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    r.f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::SchemaWritten),
    );
    r.relay(
        activation(1),
        RelayRequest::EndConsumed {
            root: root(1),
            end: end(0, 0),
            accepted_packets: 0,
            delivered_through: None,
        },
    );
    let seal=r.f.take(|b|matches!(b,LogicalEffectBody::AttemptPort(message)if matches!(message.body,LogicalToAttemptBody::SealRequest{..})));
    let LogicalEffectBody::AttemptPort(message) = seal.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::SealRequest { token } = message.body else {
        unreachable!()
    };
    r.send_port(
        activation(1),
        AttemptToLogicalBody::SealResult {
            token,
            result: Ok(Arc::from([])),
        },
    );
    let slot = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    one_bounded_turn(
        &mut r,
        LogicalInput::ActionResult {
            action: slot.action,
            result: ActionResult::Consumer(ConsumerFact::EndSlotReady),
        },
        1,
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
    let record = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RecordStreamingSuccess { .. })
        )
    });
    let error = mandatory_failure();
    r.f.ack(
        record.action,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure: FailureReport {
                error: error.clone(),
                original_attempt_failure: None,
                pre_ready_replacement: None,
            },
        }),
    );
    let enqueue=r.f.take(|b|matches!(b,LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd{activation:a})if *a==activation(1)));
    r.f.ack(
        enqueue.action,
        ActionResult::Consumer(ConsumerFact::EndSettled),
    );
    r.f.exit(slot.action);
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(r.f.core.facts().failure().is_none());
    assert_eq!(r.f.core.facts().supervision_failure(), Some(&error));
    assert!(published_result_errors(&r).is_empty());
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
}
