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
mod seal;
mod withdrawal;
fn stream_at_end_slot() -> Fixture {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    f.finished_root();
    f.open(false);
    f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(0, 0),
        accepted_packets: 0,
        delivered_through: None,
    });
    assert_eq!(f.core.facts().conclusion(), None);
    f.seal();
    f
}
fn pending(f: &mut Fixture) {
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(6).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        novarocks_coordination_contract::AttemptRootProjection {
            status,
            failure: novarocks_coordination_contract::AttemptRootFailure::DerivedPending,
        },
    )));
}
#[test]
fn no_relay_read_precedes_the_original_installed_root_observation() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    assert!(
        !f.effects
            .iter()
            .any(|e| matches!(e.body, LogicalEffectBody::Relay(RelayEffect::Start { .. })))
    );
    f.finished_root();
    assert!(
        f.effects
            .iter()
            .any(|e| matches!(e.body, LogicalEffectBody::Relay(RelayEffect::Start { .. })))
    );
}
#[test]
fn first_visibility_waits_the_original_schema_receipt_and_reserved_position() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    f.finished_root();
    f.relay(RelayRequest::OpenDelivery {
        root: root(1),
        carrier: carrier(false),
    });
    let schema = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    let token = f.relay(RelayRequest::RequestVisibility);
    assert!(!f.core.facts().output_visible());
    assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::Relay(RelayEffect::Reply{token:t,response:RelayResponse::VisibilityGranted,..})if *t==token)));
    f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::SchemaWritten),
    );
    assert!(f.core.facts().output_visible());
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::Relay(RelayEffect::Reply{token:t,response:RelayResponse::VisibilityGranted,..})if *t==token)));
}
#[test]
fn original_end_and_success_seal_precede_slot_then_success_precedes_enqueue() {
    let mut f = stream_at_end_slot();
    assert_eq!(f.core.facts().conclusion(), None);
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    f.result(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::ClientDisconnected,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
}
#[test]
fn forged_end_cannot_reauthor_accepted_or_delivered_frontier() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    f.finished_root();
    f.open(false);
    f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(2, 8),
        accepted_packets: 1,
        delivered_through: Some(0),
    });
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
}
#[test]
fn pending_authority_and_late_slot_readiness_are_a_legal_revocation_race() {
    let mut f = stream_at_end_slot();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    pending(&mut f);
    f.result(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
}
#[test]
fn a_closed_consumer_waits_the_scope_first_winner_before_any_success() {
    let mut f = stream_at_end_slot();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    f.send(LogicalInput::ConsumerClosed {
        source: LogicalSourceId::new(std::num::NonZeroU64::new(3).unwrap()),
    });
    f.result(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(f.core.facts().conclusion(), None);
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::DeadlineExceeded,
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(
        f.core.facts().failure().unwrap().kind(),
        novarocks_coordination_contract::QueryExecutionErrorKind::DeadlineExceeded
    );
}
#[test]
fn stop_observation_never_replaces_original_authoritative_failure() {
    use novarocks_coordination_contract::{
        ExecutionFailureInput, ExecutionFailureSite, QueryExecutionError, QueryFailureReason,
        SelectedTaskTermination,
    };
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TerminationDetail,
        failure::{FailureFact, FailureProjection, FailureReason, domain},
    };
    let mut f = stream_at_end_slot();
    pending(&mut f);
    let source = f.take(|b| matches!(b, LogicalEffectBody::Relay(RelayEffect::Start { .. })));
    let observed = ExecutionFailureInput::new(
        FailureFact::stop_observation(FailureReason::new(
            domain::QUERY,
            std::num::NonZeroU16::new(58).unwrap(),
        )),
        SafeDetail::truncating("root task already reached a non-success terminal state"),
        ExecutionFailureSite::Coordinator,
    );
    let report = FailureReport {
        error: QueryExecutionError::from_execution(observed.clone()),
        original_attempt_failure: Some(Box::new(observed)),
        pre_ready_replacement: None,
    };
    f.result(
        source.action,
        ActionResult::Relay(RelayFact::Failed {
            activation: activation(1),
            failure: report.clone(),
            all_deliveries_settled: true,
        }),
    );
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .accepted_failure
            .is_none()
    );
    let producer = TaskIdentity::new(
        activation(1).execution(),
        novarocks_types::StageId::new(2).unwrap(),
        novarocks_types::TaskId::new(1).unwrap(),
        root(1).backend_process_id(),
    );
    let selected = SelectedTaskTermination::new(
        producer,
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original producer failure"),
        )),
    );
    let original = ExecutionFailureInput::from_selected_task(&selected).unwrap();
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(6).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        novarocks_coordination_contract::AttemptRootProjection {
            status,
            failure: novarocks_coordination_contract::AttemptRootFailure::Authoritative(selected),
        },
    )));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
    f.relay(RelayRequest::DeliveryFailed(report));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
}

fn selected_original_failure() -> novarocks_coordination_contract::SelectedTaskTermination {
    use novarocks_coordination_contract::{QueryFailureReason, SelectedTaskTermination};
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
    };
    SelectedTaskTermination::new(
        root(1),
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original authoritative Task cause before End handoff"),
        )),
    )
}

fn finish_with_original_failure(
    f: &mut Fixture,
) -> novarocks_coordination_contract::ExecutionFailureInput {
    use novarocks_coordination_contract::{
        AttemptRootFailure, AttemptRootProjection, ExecutionFailureInput,
    };
    let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
        TaskIdentity::new(
            activation(1).execution(),
            novarocks_types::StageId::new(2).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            root(1).backend_process_id(),
        ),
        selected_original_failure().detail().clone(),
    );
    let original = ExecutionFailureInput::from_selected_task(&selected).unwrap();
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(6).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status,
            failure: AttemptRootFailure::Authoritative(selected),
        },
    )));
    original
}

#[test]
fn root_withdrawal_waits_for_original_authoritative_terminal() {
    // #1154: withdrawing the read source asks the original control owner.
    // Neither relay stop nor the control-delivered receipt authors a failure.
    use novarocks_coordination_contract::{
        AttemptRootFailure, AttemptRootProjection, ExecutionFailureInput, QueryExecutionError,
    };
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    let running = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(3).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(false),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status: running,
            failure: AttemptRootFailure::None,
        },
    )));
    f.open(false);
    f.relay(RelayRequest::RootWithdrawn { root: root(1) });
    let demand = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::AttemptPort(p)
                if matches!(p.body, LogicalToAttemptBody::TerminalControlDemand { .. })
        )
    });
    let LogicalEffectBody::AttemptPort(demand) = demand.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::TerminalControlDemand {
        token,
        root: demanded,
    } = demand.body
    else {
        unreachable!()
    };
    assert_eq!(demanded, root(1));
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(f.core.facts().failure().is_none());
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .accepted_failure
            .is_none()
    );
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Relay(RelayEffect::Stop { activation: a }) if a == activation(1)
    )));
    f.relay(RelayRequest::Stopped {
        all_deliveries_settled: true,
    });
    f.port(AttemptToLogicalBody::TerminalControlResult {
        token,
        result: Ok(()),
    });
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(f.core.facts().failure().is_none());
    let selected = selected_original_failure();
    let original = ExecutionFailureInput::from_selected_task(&selected).unwrap();
    let failing = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(4).unwrap(),
        TaskState::Failing,
        Some(selected.detail().clone()),
        TaskOutputFacts::new(false),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status: failing,
            failure: AttemptRootFailure::DerivedPending,
        },
    )));
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(f.core.facts().failure().is_none());
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(5).unwrap(),
        TaskState::Failed,
        Some(selected.detail().clone()),
        TaskOutputFacts::new(false),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status,
            failure: AttemptRootFailure::Authoritative(selected),
        },
    )));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(
        f.core.facts().failure(),
        Some(&QueryExecutionError::from_execution(original.clone()))
    );
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(
            ConsumerEffect::ReserveEndSlot { .. } | ConsumerEffect::EnqueueEnd { .. }
        )
    )));
}

#[test]
fn authoritative_failure_after_root_facts_beats_late_end_slot_readiness() {
    let mut f = stream_at_end_slot();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    let original = finish_with_original_failure(&mut f);
    f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(
        f.core.facts().failure().unwrap().execution_failure(),
        Some(&original)
    );
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
}

#[test]
fn committed_end_success_cannot_be_revoked_by_late_original_task_failure() {
    let mut f = stream_at_end_slot();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    finish_with_original_failure(&mut f);
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.core.facts().failure().is_none());
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .accepted_failure
            .is_none()
    );
    assert_eq!(
        f.effects
            .iter()
            .filter(|e| matches!(
                e.body,
                LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
            ))
            .count(),
        1
    );
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(_))
    )));
}

#[test]
fn end_writer_failure_remains_supervision_health_after_committed_success() {
    let mut f = stream_at_end_slot();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    let enqueue = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
        )
    });
    let error = novarocks_coordination_contract::QueryExecutionError::from_execution(
        novarocks_coordination_contract::ExecutionFailureInput::from_selected_task(
            &selected_original_failure(),
        )
        .unwrap(),
    );
    f.result(
        enqueue.action,
        ActionResult::Consumer(ConsumerFact::Failed(error.clone())),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.core.facts().failure().is_none());
    assert_eq!(f.core.facts().supervision_failure(), Some(&error));
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::SupervisionFailed(original) if original == &error
    )));
    assert!(!f.core.facts().protocol_join_ready());
}

#[test]
fn original_schema_failure_closes_recovery_and_rejects_pending_visibility() {
    use novarocks_coordination_contract::{ExecutionFailureInput, QueryExecutionError};
    let mut s = start(LogicalOutputMode::ResultStream);
    s.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    s.budgets.replacement_reservation_valid_for = Some(std::time::Duration::from_millis(50));
    let mut f = Fixture::with_start(s);
    f.begin();
    f.finished_root();
    f.relay(RelayRequest::OpenDelivery {
        root: root(1),
        carrier: carrier(false),
    });
    let schema = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    let request = f.relay(RelayRequest::RequestVisibility);
    let input = ExecutionFailureInput::from_selected_task(&selected_original_failure()).unwrap();
    let error = QueryExecutionError::from_execution(input.clone());
    f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::Failed(error.clone())),
    );
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(
        f.core.facts().failure().unwrap().execution_failure(),
        Some(&input)
    );
    assert!(!f.core.facts().output_visible());
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Relay(RelayEffect::Reply {
            token,
            response: RelayResponse::Rejected(original),
            ..
        }) if *token == request && original == &error
    )));
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(
            novarocks_coordination_contract::ExecutionFailure::from_input(input),
        ),
    ));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            | LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
}

#[test]
fn coalesced_root_versions_may_skip_live_states_but_old_observations_do_not_regress() {
    use novarocks_coordination_contract::{AttemptRootFailure, AttemptRootProjection};
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    let running = |version| {
        TaskStatus::try_new(
            root(1),
            TaskStatusVersion::new(version).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap()
    };
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status: running(3),
            failure: AttemptRootFailure::None,
        },
    )));
    f.finished_root();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status: running(4),
            failure: AttemptRootFailure::None,
        },
    )));
    let accepted = f
        .core
        .facts()
        .attempts()
        .next()
        .unwrap()
        .root_projection
        .unwrap();
    assert_eq!(
        accepted.status.version(),
        TaskStatusVersion::new(6).unwrap()
    );
    assert_eq!(accepted.status.state(), TaskState::Finished);
    assert_eq!(f.core.facts().conclusion(), None);
    f.open(false);
    f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(0, 0),
        accepted_packets: 0,
        delivered_through: None,
    });
    f.seal();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
}

#[test]
fn final_ack_frontier_mismatches_fail_before_end_position_or_visibility() {
    use novarocks_coordination_contract::ExecutionFailureSite;
    use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
    for (packets, accepted_packets, delivered_through) in
        [(1, 0, None), (1, 1, None), (2, 2, Some(0)), (2, 1, Some(0))]
    {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.begin();
        f.finished_root();
        f.open(false);
        f.relay(RelayRequest::EndConsumed {
            root: root(1),
            end: end(packets, 8),
            accepted_packets,
            delivered_through,
        });
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        let input = f
            .core
            .facts()
            .failure()
            .unwrap()
            .execution_failure()
            .unwrap();
        assert_eq!(input.fact().class(), FailureClass::Invariant);
        assert_eq!(
            input.fact().reason(),
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(99).unwrap())
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert!(!f.core.facts().output_visible());
        assert!(!f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Consumer(
                ConsumerEffect::ReserveEndSlot { .. } | ConsumerEffect::EnqueueEnd { .. }
            )
        )));
    }
}

fn original_native_run_panic() -> FailureReport {
    FailureReport {
        error: novarocks_logical_execution_protocol::failure::query_failure(
            novarocks_coordination_contract::QueryFailureReason::NativeFuturePanicked,
            "original Native run poll panic",
        ),
        original_attempt_failure: None,
        pre_ready_replacement: None,
    }
}

#[test]
fn committed_success_before_end_reply_survives_residual_native_run_failure() {
    let mut f = stream_at_end_slot();
    let slot = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    let enqueue = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
        )
    });
    let failure = original_native_run_panic();
    let run = f.run.take().unwrap();
    f.result(
        run,
        ActionResult::Native(NativeFact::NativeRunFailed {
            activation: activation(1),
            failure: failure.clone(),
        }),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.core.facts().failure().is_none());
    let attempt = f.core.facts().attempts().next().unwrap();
    assert_eq!(attempt.native_run_failure, Some(&failure));
    assert!(attempt.accepted_failure.is_none());
    // The original End result has not been consumed, and no actual Native
    // action-exit fact was supplied. Neither can reauthor the accepted result.
    assert!(!f.core.facts().protocol_join_ready());
    f.result(
        enqueue.action,
        ActionResult::Consumer(ConsumerFact::EndSettled),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(f.core.facts().failure().is_none());
}

#[test]
fn native_run_failure_before_success_seal_prevents_end_authorization() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    f.finished_root();
    f.open(false);
    f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(0, 0),
        accepted_packets: 0,
        delivered_through: None,
    });
    let seal = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::AttemptPort(p)
                if matches!(p.body, LogicalToAttemptBody::SealRequest { .. })
        )
    });
    let LogicalEffectBody::AttemptPort(seal) = seal.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::SealRequest { token } = seal.body else {
        unreachable!()
    };
    assert_eq!(f.core.facts().conclusion(), None);
    let failure = original_native_run_panic();
    let run = f.run.take().unwrap();
    f.result(
        run,
        ActionResult::Native(NativeFact::NativeRunFailed {
            activation: activation(1),
            failure: failure.clone(),
        }),
    );
    f.port(AttemptToLogicalBody::SealResult {
        token,
        result: Ok(std::sync::Arc::from([])),
    });
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(&failure.error));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().native_run_failure,
        Some(&failure)
    );
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(
            ConsumerEffect::ReserveEndSlot { .. } | ConsumerEffect::EnqueueEnd { .. }
        )
    )));
}
