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
use novarocks_coordination_contract::{ExecutionFailure, QueryFailureReason};
use novarocks_execution_contract::failure::FailureProjection;
fn failure() -> ExecutionFailure {
    ExecutionFailure::from_input(novarocks_coordination_contract::ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        novarocks_execution_contract::SafeDetail::truncating("original failed backend"),
        novarocks_coordination_contract::ExecutionFailureSite::Backend {
            process: root(1).backend_process_id(),
        },
    ))
}
fn recovering() -> Fixture {
    let mut start = start(LogicalOutputMode::ResultStream);
    start.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    start.budgets.replacement_reservation_valid_for = Some(std::time::Duration::from_millis(50));
    let mut f = Fixture::with_start(start);
    f.begin();
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(failure()),
    ));
    f
}
fn prepared(
    f: &mut Fixture,
) -> (
    QualificationIdentity,
    novarocks_execution_contract::TaskOperationId,
) {
    let prepare = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    f.ack(
        prepare.action,
        ActionResult::Native(NativeFact::SuccessorPrepared {
            execution: activation(2).execution(),
            participations: std::sync::Arc::from([]),
        }),
    );
    let dispatch = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::DispatchQualification { .. })
        )
    });
    let LogicalEffectBody::Native(NativeEffect::DispatchQualification {
        identity,
        operation_id,
        ..
    }) = dispatch.body
    else {
        unreachable!()
    };
    f.now = MonotonicInstant::from_origin(std::time::Duration::from_millis(10));
    f.ack(
        dispatch.action,
        ActionResult::Native(NativeFact::QualificationSettled {
            identity,
            operation_id,
            dispatched_at: Some(f.now),
            settlement: QualificationSettlement::Qualified(QualifiedReplacementFacts {
                identity,
                operation_id,
                topology_revision: 7,
                admissions: Box::new([]),
            }),
        }),
    );
    (identity, operation_id)
}
#[test]
fn the_first_failure_opens_exactly_one_preparation_window() {
    let mut f = recovering();
    let prepare = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    let LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { deadline, .. }) = prepare.body
    else {
        unreachable!()
    };
    assert_eq!(
        deadline,
        MonotonicInstant::from_origin(std::time::Duration::from_millis(50))
    );
    f.now = MonotonicInstant::from_origin(std::time::Duration::from_millis(20));
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(failure()),
    ));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
    )));
    f.now = deadline;
    f.send(LogicalInput::Timer);
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
}
#[test]
fn original_native_run_failure_is_an_independent_recovery_veto() {
    let mut f = recovering();
    let run = f.run.take().unwrap();
    let later = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::NativeFuturePanicked,
        "independent Native run failure",
    );
    f.result(
        run,
        ActionResult::Native(NativeFact::NativeRunFailed {
            activation: activation(1),
            failure: FailureReport {
                error: later,
                original_attempt_failure: None,
                pre_ready_replacement: None,
            },
        }),
    );
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(failure().error()));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(failure().input())
    );
}

#[test]
fn qualified_activation_keeps_the_core_generation_seeded_operation_and_old_failure() {
    let mut f = recovering();
    let (identity, operation_id) = prepared(&mut f);
    assert_eq!(identity.generation, activation(2).generation());
    assert_eq!(operation_id, OperationIds::new(IdSeed::new(7, 100)).issue());
    let activate = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::ActivateReservation { .. })
        )
    });
    f.ack(
        activate.action,
        ActionResult::Native(NativeFact::ReservationActivated {
            identity,
            operation_id,
        }),
    );
    assert_eq!(f.core.facts().current_activation(), Some(activation(2)));
    assert_eq!(f.core.facts().attempts_started(), 2);
    assert_eq!(
        f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap()
            .accepted_failure,
        Some(failure().input())
    );
    let register=f.take(|b|matches!(b,LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation{activation:a,..})if *a==activation(2)));
    f.ack(
        register.action,
        ActionResult::Governance(GovernanceFact::ObligationRegistered {
            activation: activation(2),
            kind: ObligationKind::RunningWork,
        }),
    );
    let next=f.take(|b|matches!(b,LogicalEffectBody::Native(NativeEffect::ActivateAttempt{activation:a,..})if *a==activation(2)));
    f.ack(
        next.action,
        ActionResult::Native(NativeFact::AttemptActivated {
            activation: activation(2),
            root: Some(root(2)),
        }),
    );
    let later = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::PreparationProtocol,
        "successor own terminal failure",
    );
    let later = ExecutionFailure::from_input(later.execution_failure().unwrap().clone());
    f.send(LogicalInput::AttemptPort(Box::new(AttemptToLogical {
        activation: activation(2),
        action: AttemptActionId::new(std::num::NonZeroU64::new(99).unwrap()),
        body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(later.clone())),
    })));
    assert_eq!(f.core.facts().failure(), Some(later.error()));
    assert_eq!(
        f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap()
            .accepted_failure,
        Some(failure().input())
    );
}
#[test]
fn ordinary_native_terminal_failure_is_not_the_independent_run_panic_veto() {
    let mut start = start(LogicalOutputMode::ResultStream);
    start.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    start.budgets.replacement_reservation_valid_for = Some(std::time::Duration::from_millis(50));
    let mut f = Fixture::with_start(start);
    f.begin();
    let run = f.run.take().unwrap();
    f.result(
        run,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure: FailureReport::from_attempt(&failure()),
        }),
    );
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
    )));
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .native_run_failure
            .is_none()
    );
}

#[test]
fn native_run_veto_covers_qualification_after_old_authority_was_revoked() {
    let mut f = recovering();
    let _ = prepared(&mut f);
    assert_eq!(f.core.facts().current_activation(), None);
    let run = f.run.take().unwrap();
    let error = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::NativeFuturePanicked,
        "original run panic during qualification",
    );
    f.result(
        run,
        ActionResult::Native(NativeFact::NativeRunFailed {
            activation: activation(1),
            failure: FailureReport {
                error,
                original_attempt_failure: None,
                pre_ready_replacement: None,
            },
        }),
    );
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().attempts_started(), 1);
    assert_eq!(f.core.facts().failure(), Some(failure().error()));
}
#[test]
fn honest_handoff_at_original_expiry_is_a_business_expiry_and_abandons_resources() {
    let mut f = recovering();
    let prepare = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    f.ack(
        prepare.action,
        ActionResult::Native(NativeFact::SuccessorPrepared {
            execution: activation(2).execution(),
            participations: std::sync::Arc::from([]),
        }),
    );
    let dispatch = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::DispatchQualification { .. })
        )
    });
    let LogicalEffectBody::Native(NativeEffect::DispatchQualification {
        identity,
        operation_id,
        expires_at,
        ..
    }) = dispatch.body
    else {
        unreachable!()
    };
    f.now = expires_at;
    f.result(
        dispatch.action,
        ActionResult::Native(NativeFact::QualificationSettled {
            identity,
            operation_id,
            dispatched_at: Some(expires_at),
            settlement: QualificationSettlement::OutcomeUnknown,
        }),
    );
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert!(f.core.facts().supervision_failure().is_none());
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::AbandonReservation { .. })
    )));
}

#[test]
fn cancellation_before_preparation_intent_handoff_creates_no_dormant_work() {
    let mut start = start(LogicalOutputMode::ResultStream);
    start.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    start.budgets.replacement_reservation_valid_for = Some(std::time::Duration::from_millis(50));
    let mut f = Fixture::with_start(start);
    f.begin();
    f.send_once(
        LogicalInput::AttemptPort(Box::new(AttemptToLogical {
            activation: activation(1),
            action: AttemptActionId::new(std::num::NonZeroU64::new(101).unwrap()),
            body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(failure())),
        })),
        0,
    );
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::PrepareSuccessor { .. }
                | NativeEffect::DispatchQualification { .. }
                | NativeEffect::ConvergeDormant { .. }
        )
    )));
}

#[test]
fn cancellation_discards_unissued_qualification_but_converges_the_real_prepared_owner() {
    let mut f = recovering();
    let prepare = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
        )
    });
    f.send_once(
        LogicalInput::ActionResult {
            action: prepare.action,
            result: ActionResult::Native(NativeFact::SuccessorPrepared {
                execution: activation(2).execution(),
                participations: std::sync::Arc::from([]),
            }),
        },
        1,
    );
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    f.exit(prepare.action);
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(
            NativeEffect::DispatchQualification { .. }
                | NativeEffect::CancelQualification { .. }
                | NativeEffect::AbandonReservation { .. }
        )
    )));
    assert!(f.effects.iter().any(|e|matches!(e.body,LogicalEffectBody::Native(NativeEffect::ConvergeDormant{execution,..})if execution==activation(2).execution())));
}
