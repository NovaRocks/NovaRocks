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
fn completed() -> Fixture {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    f.begin();
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    let completion = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )
    });
    f.ack(
        completion.action,
        ActionResult::Consumer(ConsumerFact::Applied),
    );
    let cut = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RecordTerminalCut { .. })
        )
    });
    f.ack(cut.action, ActionResult::Native(NativeFact::Applied));
    f
}
fn resolve(f: &mut Fixture) -> LogicalActionId {
    f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
        )
    })
    .action
}
fn resolved(f: &mut Fixture, action: LogicalActionId) {
    f.result(
        action,
        ActionResult::Governance(GovernanceFact::ObligationResolved {
            activation: activation(1),
            kind: ObligationKind::RunningWork,
        }),
    );
}
#[test]
fn normal_completion_uses_the_task_boundary_and_emits_no_abort_or_stop() {
    let mut f = completed();
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::Stop{..}|LogicalToAttemptBody::Abort{..}))));
    assert!(f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::BeginConvergence{mode:novarocks_coordination_contract::AttemptConvergenceMode::Normal,..}))));
    f.send(LogicalInput::Timer);
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
}

#[test]
fn unused_preparation_retires_while_the_original_active_cleanup_is_still_held() {
    let mut f = completed();
    let preparation = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RetirePreparation)
        )
    });
    f.ack(
        preparation.action,
        ActionResult::Native(NativeFact::Applied),
    );
    assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
    assert!(!f.core.facts().protocol_join_ready());
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(
            GovernanceEffect::ReleaseExecutionStage | GovernanceEffect::CompleteWorkOwner
        )
    )));
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
    )));
}
#[test]
fn a_governance_result_does_not_substitute_for_actual_action_exit() {
    let mut f = completed();
    let action = resolve(&mut f);
    resolved(&mut f, action);
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
    )));
    f.exit(action);
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
    )));
}
#[test]
fn resources_and_native_attempt_local_exit_remain_independent_until_retirement() {
    let mut f = completed();
    let action = resolve(&mut f);
    resolved(&mut f, action);
    f.exit(action);
    let resources = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
        )
    });
    f.result(resources.action, ActionResult::Native(NativeFact::Applied));
    f.port(AttemptToLogicalBody::ConvergenceProgress {
        local_exited: true,
        closure: Some(std::sync::Arc::new(AttemptConvergenceClosure::Evidence(
            Vec::new(),
        ))),
        lifecycle_runtime_filters: None,
    });
    assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
    f.exit(resources.action);
    assert!(f.core.facts().attempts().next().unwrap().cleanup_complete);
    assert!(!f.core.facts().protocol_join_ready());
    let run = f.run.take().unwrap();
    f.ack(
        run,
        ActionResult::Native(NativeFact::RunCompleted {
            activation: activation(1),
        }),
    );
    let prep = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RetirePreparation)
        )
    });
    f.ack(prep.action, ActionResult::Native(NativeFact::Applied));
    let stage = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Governance(GovernanceEffect::ReleaseExecutionStage)
        )
    });
    f.ack(
        stage.action,
        ActionResult::Governance(GovernanceFact::StageReleased),
    );
    let owner = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Governance(GovernanceEffect::CompleteWorkOwner)
        )
    });
    f.ack(
        owner.action,
        ActionResult::Governance(GovernanceFact::WorkOwnerCompleted),
    );
    assert!(f.core.facts().protocol_join_ready());
    assert!(!f.core.facts().retirable());
    let source = LogicalSourceId::new(std::num::NonZeroU64::new(4).unwrap());
    let token = LogicalRequestToken::new(std::num::NonZeroU64::new(9).unwrap());
    f.send(LogicalInput::RegistryJoinReadiness {
        source,
        token,
        close_ingress: false,
    });
    assert!(!f.core.facts().retirable());
    f.send(LogicalInput::RegistryClose(RegistryClose::Retire));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::RequestShutdownCancellation)
    )));
    f.send(LogicalInput::RegistryJoinReadiness {
        source,
        token: LogicalRequestToken::new(std::num::NonZeroU64::new(10).unwrap()),
        close_ingress: true,
    });
    assert!(f.core.facts().retirable());
    assert!(
        f.effects
            .iter()
            .any(|e| matches!(e.body, LogicalEffectBody::Retirable))
    );
    assert_eq!(f.core.next_deadline(), None);
}
#[test]
fn completion_cannot_overtake_the_scope_first_winner_after_consumer_close() {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    f.begin();
    f.send(LogicalInput::ConsumerClosed {
        source: LogicalSourceId::new(std::num::NonZeroU64::new(3).unwrap()),
    });
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(f.core.facts().conclusion(), None);
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::ServerShutdown,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
    )));
}

#[test]
fn failed_resource_settlement_after_success_is_supervision_failure_without_false_exit() {
    let mut f = completed();
    let action = resolve(&mut f);
    resolved(&mut f, action);
    f.exit(action);
    let resources = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::FinishResources { .. })
        )
    });
    let error = novarocks_logical_execution_protocol::failure::query_failure(
        novarocks_coordination_contract::QueryFailureReason::NativeFuturePanicked,
        "original resources failed to exit",
    );
    f.ack(
        resources.action,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure: FailureReport {
                error: error.clone(),
                original_attempt_failure: None,
                pre_ready_replacement: None,
            },
        }),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert_eq!(f.core.facts().supervision_failure(), Some(&error));
    assert!(!f.core.facts().attempts().next().unwrap().cleanup_complete);
    assert!(!f.core.facts().protocol_join_ready());
    assert!(
        f.effects
            .iter()
            .any(|e| matches!(&e.body,LogicalEffectBody::SupervisionFailed(e)if e==&error))
    );
    assert_eq!(f.core.next_deadline(), None);
}
#[test]
fn failed_obligation_settlement_retains_the_exact_error_and_never_completes_owner() {
    let mut f = completed();
    let action = resolve(&mut f);
    let error = novarocks_logical_execution_protocol::failure::query_failure(
        novarocks_coordination_contract::QueryFailureReason::PreparationProtocol,
        "original obligation settlement rejected",
    );
    f.ack(
        action,
        ActionResult::Governance(GovernanceFact::Failed(error.clone())),
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert_eq!(f.core.facts().supervision_failure(), Some(&error));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::CompleteWorkOwner)
    )));
    assert_eq!(f.core.next_deadline(), None);
}
#[test]
fn cancellation_discards_a_queued_relay_before_any_relay_work_was_issued() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(4).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    f.send_once(
        LogicalInput::AttemptPort(Box::new(AttemptToLogical {
            activation: activation(1),
            action: AttemptActionId::new(std::num::NonZeroU64::new(100).unwrap()),
            body: AttemptToLogicalBody::RootStatus(Box::new(
                novarocks_coordination_contract::AttemptRootProjection {
                    status,
                    failure: novarocks_coordination_contract::AttemptRootFailure::None,
                },
            )),
        })),
        1,
    );
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::Requested,
    ));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Relay(RelayEffect::Start { .. } | RelayEffect::Stop { .. })
    )));
    assert!(!f.core.facts().attempts().next().unwrap().relay_started);
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { .. })
    )));
}

#[test]
fn failed_original_output_handoff_reports_supervision_instead_of_waiting_forever() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.send(LogicalInput::Started);
    let output = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ExposeOutput { .. })
        )
    });
    let error = novarocks_logical_execution_protocol::failure::query_failure(
        novarocks_coordination_contract::QueryFailureReason::PreparationProtocol,
        "original output handoff failed",
    );
    f.ack(
        output.action,
        ActionResult::Consumer(ConsumerFact::Failed(error.clone())),
    );
    assert_eq!(f.core.facts().supervision_failure(), Some(&error));
    assert!(!f.core.facts().protocol_join_ready());
    assert!(
        f.effects
            .iter()
            .any(|e| matches!(&e.body,LogicalEffectBody::SupervisionFailed(e)if e==&error))
    );
    assert_eq!(f.core.next_deadline(), None);
}
