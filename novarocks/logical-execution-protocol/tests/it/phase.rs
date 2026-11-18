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
mod terminal;
use novarocks_coordination_contract::{
    ExecutionFailure, ExecutionFailureInput, QueryExecutionError, QueryFailureReason,
    SelectedTaskTermination,
};
use novarocks_execution_contract::{
    SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
};
use novarocks_logical_execution_protocol::{
    CoreFault, cancel::LogicalCancellationReason, facts::LogicalPhase,
};

fn original_failure(detail: &str) -> ExecutionFailure {
    let selected = SelectedTaskTermination::new(
        root(1),
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating(detail),
        )),
    );
    ExecutionFailure::from_input(ExecutionFailureInput::from_selected_task(&selected).unwrap())
}

fn published_failures(f: &Fixture) -> Vec<&QueryExecutionError> {
    f.effects
        .iter()
        .filter_map(|e| match &e.body {
            LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error)) => Some(error),
            _ => None,
        })
        .collect()
}

#[test]
fn exact_initial_activation_enters_running_once_and_completion_has_one_publication() {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    assert_eq!(
        f.core.facts().phase(),
        LogicalPhase::Instantiating(activation(1))
    );
    f.begin();
    assert_eq!(f.core.facts().phase(), LogicalPhase::Running(activation(1)));
    assert_eq!(f.core.facts().attempts_started(), 1);
    assert_eq!(f.core.facts().current_activation(), Some(activation(1)));
    assert!(f.core.facts().attempts().next().unwrap().activated);
    for _ in 0..2 {
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    }
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert_eq!(
        f.effects
            .iter()
            .filter(|e| matches!(
                e.body,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
            ))
            .count(),
        1
    );
    assert_eq!(f.core.facts().attempts_started(), 1);
}

#[test]
fn initial_task_failure_before_activation_return_keeps_original_input() {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    f.send(LogicalInput::Started);
    let activation_job = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
        )
    });
    let original = original_failure("original Task failed during initial activation");
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(original.clone()),
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(original.error()));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(original.input())
    );
    f.ack(
        activation_job.action,
        ActionResult::Native(NativeFact::AttemptActivated {
            activation: activation(1),
            root: Some(root(1)),
        }),
    );
    assert_eq!(f.core.facts().failure(), Some(original.error()));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
    )));
    assert_eq!(published_failures(&f), vec![original.error()]);
}

#[test]
fn running_task_failure_has_one_original_client_cause_despite_late_commands() {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    f.begin();
    let original = original_failure("first authoritative Running Task failure");
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(original.clone()),
    ));
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(original_failure(
            "late unrelated failure must remain residual",
        )),
    ));
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(original.error()));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(original.input())
    );
    assert_eq!(published_failures(&f), vec![original.error()]);
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
    )));
}

#[test]
fn accepted_cancellation_preserves_its_exact_input_through_late_initial_commands() {
    use LogicalCancellationReason as R;
    for reason in [
        R::DeadlineExceeded,
        R::FrontendDrainDeadlineExceeded,
        R::ServerShutdown,
        R::OwnerDropped,
        R::Requested,
        R::ClientCancelRequested,
        R::ClientDisconnected,
        R::ExplicitKill,
        R::ExplicitKillConnection,
    ] {
        let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
        f.send(LogicalInput::Started);
        let activation_job = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
            )
        });
        let (conclusion, failure_reason, detail) = match reason {
            R::DeadlineExceeded => (
                LogicalConclusion::Failed,
                QueryFailureReason::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            R::FrontendDrainDeadlineExceeded => (
                LogicalConclusion::Failed,
                QueryFailureReason::DeadlineExceeded,
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
            ),
            _ => (
                LogicalConclusion::Cancelled,
                QueryFailureReason::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
        };
        let expected =
            novarocks_logical_execution_protocol::failure::query_failure(failure_reason, detail);
        f.send(LogicalInput::Cancelled(reason));
        f.ack(
            activation_job.action,
            ActionResult::Native(NativeFact::AttemptActivated {
                activation: activation(1),
                root: Some(root(1)),
            }),
        );
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Failed(original_failure(
                "late initial failure cannot replace original scope input",
            )),
        ));
        f.port(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
        f.send(LogicalInput::Cancelled(R::ClientDisconnected));
        assert_eq!(f.core.facts().conclusion(), Some(conclusion), "{reason:?}");
        assert_eq!(f.core.facts().failure(), Some(&expected), "{reason:?}");
        assert!(
            f.core
                .facts()
                .attempts()
                .next()
                .unwrap()
                .accepted_failure
                .is_none()
        );
        assert_eq!(published_failures(&f), vec![&expected]);
        assert!(!f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
                | LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )));
    }
}

#[test]
fn never_established_initial_cancellation_cannot_mint_remote_abort() {
    // #1071: before any activation handoff, cancellation closes only the
    // original logical responsibilities. It cannot invent a remote grant.
    let frontend = novarocks_types::FrontendProcessId::try_from_bytes(
        OperationIds::new(IdSeed::new(1100, 100)).issue().to_bytes(),
    )
    .unwrap();
    let participation = novarocks_execution_contract::ParticipationRef::new(
        activation(1).execution(),
        frontend,
        root(1).backend_process_id(),
    );
    let mut s = start(LogicalOutputMode::CompletionOnly);
    s.first_attempt.participations = vec![participation].into_boxed_slice();
    let mut f = Fixture::with_start(s);
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::DeadlineExceeded,
    ));
    f.send(LogicalInput::Started);
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    let attempt = f.core.facts().attempts().next().unwrap();
    assert!(!attempt.activated);
    assert!(!attempt.authority_open);
    assert!(attempt.all_participations_closed);
    assert!(!attempt.cleanup_complete);
    assert!(!f.core.facts().protocol_join_ready());
    assert_eq!(
        f.core.facts().participation(activation(1), participation).unwrap().closure,
        novarocks_logical_execution_protocol::facts::ParticipationClosureState::NoRemoteResponsibility,
    );
    assert!(!f.effects.iter().any(|e| {
        matches!(
            e.body,
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
        ) || matches!(&e.body, LogicalEffectBody::AttemptPort(p)
                if matches!(p.body, LogicalToAttemptBody::Abort { .. }))
    }));
}

struct NoEffects;
impl EffectSink for NoEffects {
    fn emit(&mut self, _effect: LogicalEffect) {
        panic!("foreign attempt input must not emit a logical effect")
    }
}

#[test]
fn foreign_actor_or_generation_failure_cannot_install_a_local_canonical_cause() {
    for foreign in [
        AttemptActivationIdentity::from_parts(
            activation(1).actor().get() + 1,
            activation(1).execution(),
            activation(1).generation(),
        ),
        AttemptActivationIdentity::from_parts(
            activation(1).actor().get(),
            activation(1).execution(),
            activation(1).generation() + 1,
        ),
    ] {
        let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
        f.begin();
        let result = f.core.handle(
            f.now,
            LogicalInput::AttemptPort(Box::new(AttemptToLogical {
                activation: foreign,
                action: AttemptActionId::new(std::num::NonZeroU64::MIN),
                body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(
                    original_failure("foreign author cannot set local cause"),
                )),
            })),
            &mut Budget(32),
            &mut NoEffects,
        );
        assert_eq!(result, Err(CoreFault::ForeignActivation));
        assert_eq!(f.core.facts().phase(), LogicalPhase::Running(activation(1)));
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
        f.core.check_invariants().unwrap();
    }
}

#[test]
fn effectful_client_stream_configuration_is_rejected_before_installation() {
    let mut s = start(LogicalOutputMode::ResultStream);
    s.policy.effect = ExecutionEffect::External;
    assert_eq!(s.validate(), Err(StartRefusal::InvalidLifecyclePolicy));
    s.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    assert_eq!(s.validate(), Err(StartRefusal::ExternalRecovery));
}

#[test]
fn completion_only_delivery_request_cannot_open_a_stream_or_delay_task_completion() {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    f.begin();
    let token = f.relay(RelayRequest::OpenDelivery {
        root: root(1),
        carrier: carrier(false),
    });
    assert!(!f.core.facts().output_visible());
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Relay(RelayEffect::Reply {
            token: t,
            response: RelayResponse::Rejected(_),
            ..
        }) if *t == token
    )));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
    )));
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
}

#[test]
fn required_initial_rows_root_missing_fences_before_native_run_or_relay_start() {
    for output in [
        LogicalOutputMode::ResultStream,
        LogicalOutputMode::InternalResultStream,
    ] {
        let mut f = Fixture::new(output);
        f.send(LogicalInput::Started);
        let register = f.take(|body| {
            matches!(
                body,
                LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { .. })
            )
        });
        f.ack(
            register.action,
            ActionResult::Governance(GovernanceFact::ObligationRegistered {
                activation: activation(1),
                kind: ObligationKind::RunningWork,
            }),
        );
        if let Some(index) = f.effects.iter().position(|effect| {
            matches!(
                effect.body,
                LogicalEffectBody::Consumer(ConsumerEffect::ExposeOutput { .. })
            )
        }) {
            let expose = f.effects.remove(index);
            f.ack(
                expose.action,
                ActionResult::Consumer(ConsumerFact::OutputExposed),
            );
        }
        let issued = f.take(|body| {
            matches!(body,
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation: a, .. })
                if *a == activation(1))
        });
        assert_eq!(f.core.facts().current_activation(), Some(activation(1)));
        assert_eq!(f.core.facts().attempts_started(), 1);
        f.result(
            issued.action,
            ActionResult::Native(NativeFact::AttemptActivated {
                activation: activation(1),
                root: None,
            }),
        );
        assert_missing_rows_preserves_typed_failure_and_closing(
            &mut f,
            activation(1),
            issued.action,
        );
        assert_ne!(f.core.facts().phase(), LogicalPhase::Running(activation(1)));
        assert!(!f.effects.iter().any(|effect| matches!(effect.body,
            LogicalEffectBody::Native(NativeEffect::RunAttempt { activation: a })
            | LogicalEffectBody::Relay(RelayEffect::Start { activation: a })
                if a == activation(1))));
        // This sequence supplies only passive public Core inputs. Actual
        // Native return/F/Source exit remains the physical owner's obligation.
    }
}

pub(super) fn assert_missing_rows_preserves_typed_failure_and_closing(
    f: &mut Fixture,
    activation: AttemptActivationIdentity,
    original_activate: LogicalActionId,
) {
    use novarocks_coordination_contract::{ExecutionFailureSite, QueryExecutionErrorKind};
    let expected = QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::ResultProtocol.failure_fact(),
        SafeDetail::truncating("Native activated a row execution without its root result runtime"),
        ExecutionFailureSite::Coordinator,
    ));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(&expected));
    assert_eq!(
        f.core.facts().failure().unwrap().kind(),
        QueryExecutionErrorKind::Failed
    );
    let facts = f
        .core
        .facts()
        .attempts()
        .find(|a| a.activation == activation)
        .unwrap();
    assert!(
        facts.activated,
        "the original successful activation/return remains real"
    );
    assert!(!facts.relay_started && !facts.cleanup_complete);
    assert!(f.effects.iter().any(|effect| matches!(&effect.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error)) if error == &expected)));
    assert!(!f.effects.iter().any(|effect| matches!(effect.body,
        LogicalEffectBody::Native(NativeEffect::RunAttempt { activation: a })
        | LogicalEffectBody::Relay(RelayEffect::Start { activation: a }) if a == activation)));
    // The absent read runtime cannot manufacture a close operation or a
    // native exit. The existing terminal cut still belongs to this activation.
    assert!(!f.effects.iter().any(|effect| matches!(effect.body,
        LogicalEffectBody::Native(NativeEffect::SealRootRead { activation: a, .. }) if a == activation)));
    let cut = f.take(|body| matches!(body,
        LogicalEffectBody::Native(NativeEffect::RecordTerminalCut { activation: a, .. }) if *a == activation));
    f.ack(cut.action, ActionResult::Native(NativeFact::Applied));
    assert!(f.effects.iter().any(|effect| matches!(&effect.body,
        LogicalEffectBody::AttemptPort(message) if message.activation == activation && matches!(message.body,
            LogicalToAttemptBody::BeginConvergence { mode: novarocks_coordination_contract::AttemptConvergenceMode::Cleanup, .. }))));
    assert!(!f.effects.iter().any(|effect| matches!(effect.body,
        LogicalEffectBody::Native(NativeEffect::FinishResources { activation: a }) if a == activation)));
    f.exit(original_activate);
    // The existing public fixtures have an explicitly empty frozen roster.
    // This supplied AP fact does not claim an actual Native/AP Scope exited.
    f.send(LogicalInput::AttemptPort(Box::new(AttemptToLogical {
        activation,
        action: AttemptActionId::new(std::num::NonZeroU64::new(9_036).unwrap()),
        body: AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(std::sync::Arc::new(
                novarocks_coordination_contract::AttemptConvergenceClosure::Evidence(Vec::new()),
            )),
            lifecycle_runtime_filters: None,
        },
    })));
    let resolve = f.take(|body| matches!(body,
        LogicalEffectBody::Governance(GovernanceEffect::ResolveObligation { activation: a, kind: ObligationKind::RunningWork }) if *a == activation));
    f.result(
        resolve.action,
        ActionResult::Governance(GovernanceFact::ObligationResolved {
            activation,
            kind: ObligationKind::RunningWork,
        }),
    );
    assert!(!f.effects.iter().any(|effect| matches!(effect.body,
        LogicalEffectBody::Native(NativeEffect::FinishResources { activation: a }) if a == activation)));
    f.exit(resolve.action);
    assert!(f.effects.iter().any(|effect| matches!(effect.body,
        LogicalEffectBody::Native(NativeEffect::FinishResources { activation: a }) if a == activation)));
    assert!(
        !f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation)
            .unwrap()
            .cleanup_complete
    );
    assert_eq!(f.core.facts().failure(), Some(&expected));
}
