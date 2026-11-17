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
mod replacement;
use novarocks_coordination_contract::{QueryExecutionError, QueryFailureReason};
fn original_error() -> QueryExecutionError {
    novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::InternalConsumerPanicked,
        "original consumer verdict",
    )
}
fn decoded() -> Fixture {
    let mut f = Fixture::new(LogicalOutputMode::InternalResultStream);
    f.begin();
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(3).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(false),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        novarocks_coordination_contract::AttemptRootProjection {
            status,
            failure: novarocks_coordination_contract::AttemptRootFailure::None,
        },
    )));
    f.internal(InternalRequest::RegisterSource {
        root: root(1),
        plan: PLAN,
        supervised_exit: true,
    });
    f.open(true);
    f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(0, 27),
        accepted_packets: 0,
        delivered_through: None,
    });
    let receipt = InternalEndReceiptId(std::num::NonZeroU64::new(1).unwrap());
    f.internal(InternalRequest::EndDecoded {
        receipt,
        end: end(0, 27),
    });
    f
}
fn proposal(f: &mut Fixture) -> LogicalRequestToken {
    let receipt = InternalEndReceiptId(std::num::NonZeroU64::MIN);
    f.internal(InternalRequest::AcceptReady {
        root: root(1),
        plan: PLAN,
        receipt,
        end: end(0, 27),
    })
}
fn pending() -> Fixture {
    let mut f = decoded();
    proposal(&mut f);
    f
}
fn accepted() -> Fixture {
    let mut f = pending();
    f.finished_root();
    f.seal();
    f
}
fn record_cut(f: &mut Fixture) {
    let record = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RecordComputationReady { .. })
        )
    });
    f.ack(record.action, ActionResult::Native(NativeFact::Applied));
}
#[test]
fn decoded_internal_end_waits_original_finished_seal_and_common_protocol_gates() {
    let mut f = pending();
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishReady { .. })
    )));
    f.finished_root();
    assert_eq!(f.core.facts().conclusion(), None);
    f.seal();
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(f.core.facts().ready_cut().unwrap().end, end(0, 27));
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishReady { .. })
    )));
}
#[test]
fn source_drop_waits_the_reserved_actual_consumer_verdict() {
    let mut f = pending();
    f.internal(InternalRequest::SourceClosed);
    f.finished_root();
    f.seal();
    assert_eq!(f.core.facts().conclusion(), None);
    f.internal(InternalRequest::ExitObserved {
        outcome: Some(Err(original_error())),
    });
    assert_eq!(f.core.facts().failure(), Some(&original_error()));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
}
#[test]
fn accepted_ready_is_not_rewritten_by_late_scope_deadline_or_consumer_exit() {
    let mut f = accepted();
    let cut = f.core.facts().ready_cut().unwrap();
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::DeadlineExceeded,
    ));
    f.internal(InternalRequest::ExitObserved {
        outcome: Some(Err(original_error())),
    });
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(f.core.facts().ready_cut(), Some(cut));
    assert!(!f.effects.iter().any(|e|matches!(&e.body,LogicalEffectBody::AttemptPort(p)if matches!(p.body,LogicalToAttemptBody::Stop{..}|LogicalToAttemptBody::Abort{..}))));
}
#[test]
fn pending_ready_is_rejected_with_original_cancellation_without_cut() {
    let mut f = pending();
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::ServerShutdown,
    ));
    f.finished_root();
    assert!(f.core.facts().ready_cut().is_none());
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            response: InternalResponse::Rejected(_),
            ..
        })
    )));
}
#[test]
fn required_ready_custody_failure_preserves_cut_and_requests_supervised_isolation() {
    let mut f = accepted();
    let cut = f.core.facts().ready_cut().unwrap();
    let record = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Native(NativeEffect::RecordComputationReady { .. })
        )
    });
    let error = original_error();
    let failure = FailureReport {
        error: error.clone(),
        original_attempt_failure: None,
        pre_ready_replacement: None,
    };
    f.result(
        record.action,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure,
        }),
    );
    assert_eq!(f.core.facts().ready_cut(), Some(cut));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(f.core.facts().supervision_failure(), Some(&error));
    assert!(f.effects.iter().any(
        |e| matches!(&e.body,LogicalEffectBody::SupervisionFailed(original)if *original==error)
    ));
    assert_eq!(f.core.next_deadline(), None);
}
#[test]
fn diagnostic_publication_failure_cannot_become_a_silent_inflight_wait() {
    let mut f = accepted();
    record_cut(&mut f);
    let diagnostic = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishDiagnostics { .. })
        )
    });
    f.result(
        diagnostic.action,
        ActionResult::Consumer(ConsumerFact::Failed(original_error())),
    );
    assert_eq!(
        f.core.facts().supervision_failure(),
        Some(&original_error())
    );
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(f.core.next_deadline(), None);
}
#[test]
fn convergence_publication_is_independent_and_failure_is_an_explicit_supervision_fact() {
    let mut f = accepted();
    record_cut(&mut f);
    f.now = MonotonicInstant::from_origin(std::time::Duration::from_millis(90));
    f.send(LogicalInput::Timer);
    let convergence = f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishConvergence { .. })
        )
    });
    let LogicalEffectBody::Consumer(ConsumerEffect::PublishConvergence {
        local_resources_exited,
        ..
    }) = &convergence.body
    else {
        unreachable!()
    };
    assert!(!local_resources_exited);
    f.result(
        convergence.action,
        ActionResult::Consumer(ConsumerFact::Failed(original_error())),
    );
    assert_eq!(
        f.core.facts().supervision_failure(),
        Some(&original_error())
    );
    assert_eq!(f.core.next_deadline(), None);
}

fn original_task_failure() -> novarocks_coordination_contract::ExecutionFailure {
    use novarocks_coordination_contract::{
        ExecutionFailure, ExecutionFailureInput, SelectedTaskTermination,
    };
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
    };
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
            SafeDetail::truncating("original frozen internal Task failure"),
        )),
    );
    ExecutionFailure::from_input(ExecutionFailureInput::from_selected_task(&selected).unwrap())
}

fn assert_no_ready(f: &Fixture) {
    assert!(f.core.facts().ready_cut().is_none());
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishReady { .. })
            | LogicalEffectBody::Native(NativeEffect::RecordComputationReady { .. })
    )));
}

fn seal_reply_without_driving(f: &mut Fixture) {
    f.finished_root();
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
    f.send_once(
        LogicalInput::AttemptPort(Box::new(AttemptToLogical {
            activation: activation(1),
            action: AttemptActionId::new(std::num::NonZeroU64::new(900).unwrap()),
            body: AttemptToLogicalBody::SealResult {
                token,
                result: Ok(std::sync::Arc::from([])),
            },
        })),
        0,
    );
    assert_no_ready(f);
}

#[test]
fn queued_authoritative_root_failure_prevents_the_ready_cut_and_keeps_exact_task_input() {
    use novarocks_coordination_contract::{
        AttemptRootFailure, AttemptRootProjection, SelectedTaskTermination,
    };
    use novarocks_execution_contract::{TaskFailure, TerminationDetail};
    let mut f = decoded();
    let request = proposal(&mut f);
    seal_reply_without_driving(&mut f);
    let original = original_task_failure();
    let selected = SelectedTaskTermination::new(
        match original.input().site() {
            novarocks_coordination_contract::ExecutionFailureSite::Task { identity, .. } => {
                identity
            }
            _ => panic!("original producer Task site required"),
        },
        TerminationDetail::Failed(TaskFailure::new(
            original.input().fact(),
            original.input().detail().clone(),
        )),
    );
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
    assert_no_ready(&f);
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(original.error()));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(original.input())
    );
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            token,
            response: InternalResponse::Rejected(error),
            ..
        }) if *token == request && error.execution_failure() == Some(original.input())
    )));
}

#[test]
fn original_internal_end_failure_rejects_the_pending_proof_without_losing_task_identity() {
    let mut f = decoded();
    let request = proposal(&mut f);
    let original = original_task_failure();
    f.internal(InternalRequest::Failed(FailureReport::from_attempt(
        &original,
    )));
    f.finished_root();
    assert_no_ready(&f);
    assert_eq!(f.core.facts().failure(), Some(original.error()));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(original.input())
    );
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            token,
            response: InternalResponse::Rejected(error),
            ..
        }) if *token == request && error.execution_failure() == Some(original.input())
    )));
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error))
            if error.execution_failure() == Some(original.input())
    )));
    assert!(!f.core.facts().original_internal_consumer_exited());
}

#[test]
fn registry_close_queued_after_seal_waits_for_original_scope_verdict_without_ready() {
    let mut f = decoded();
    let request = proposal(&mut f);
    seal_reply_without_driving(&mut f);
    f.send(LogicalInput::RegistryClose(RegistryClose::Shutdown));
    assert_no_ready(&f);
    assert_eq!(f.core.facts().conclusion(), None);
    assert!(f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Governance(GovernanceEffect::RequestShutdownCancellation)
    )));
    f.send(LogicalInput::Cancelled(
        novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::FrontendDrainDeadlineExceeded,
    ));
    let expected = novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::DeadlineExceeded,
        "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
    );
    assert_no_ready(&f);
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(&expected));
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            token,
            response: InternalResponse::Rejected(error),
            ..
        }) if *token == request && error == &expected
    )));
}

#[test]
fn governed_cancellation_rejects_pending_end_with_exact_reason_site_and_detail() {
    use novarocks_logical_execution_protocol::cancel::LogicalCancellationReason as R;
    for (reason, expected_reason, detail) in [
        (
            R::DeadlineExceeded,
            QueryFailureReason::DeadlineExceeded,
            "logical execution deadline expired before success EOF",
        ),
        (
            R::FrontendDrainDeadlineExceeded,
            QueryFailureReason::DeadlineExceeded,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
        ),
        (
            R::ClientCancelRequested,
            QueryFailureReason::Cancelled,
            "logical execution was cancelled before success EOF",
        ),
    ] {
        let mut f = decoded();
        let request = proposal(&mut f);
        let expected =
            novarocks_logical_execution_protocol::failure::query_failure(expected_reason, detail);
        f.send(LogicalInput::Cancelled(reason));
        assert_no_ready(&f);
        assert_eq!(f.core.facts().failure(), Some(&expected));
        assert!(f.effects.iter().any(|e| matches!(
            &e.body,
            LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
                token,
                response: InternalResponse::Rejected(error),
                ..
            }) if *token == request && error == &expected
        )));
        assert!(!f.core.facts().original_internal_consumer_exited());
    }
}

#[test]
fn conflicting_original_end_receipt_fails_closed_and_cannot_be_repaired_by_ready() {
    let mut f = decoded();
    f.internal(InternalRequest::EndDecoded {
        receipt: InternalEndReceiptId(std::num::NonZeroU64::new(2).unwrap()),
        end: end(0, 27),
    });
    let original = f.core.facts().failure().unwrap().clone();
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    let request = proposal(&mut f);
    f.finished_root();
    assert_no_ready(&f);
    assert_eq!(f.core.facts().failure(), Some(&original));
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            token,
            response: InternalResponse::Rejected(error),
            ..
        }) if *token == request && error == &original
    )));
}

#[test]
fn foreign_source_failure_cannot_conclude_or_release_original_pending_consumer() {
    let mut f = pending();
    let original = original_task_failure();
    let request = LogicalRequestToken::new(std::num::NonZeroU64::new(700).unwrap());
    f.send(LogicalInput::Internal {
        source: LogicalSourceId::new(std::num::NonZeroU64::new(3).unwrap()),
        token: request,
        activation: activation(1),
        request: InternalRequest::Failed(FailureReport::from_attempt(&original)),
    });
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
    assert!(!f.core.facts().original_internal_consumer_exited());
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            token,
            response: InternalResponse::Rejected(_),
            ..
        }) if *token == request
    )));
    f.finished_root();
    f.seal();
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
}

#[test]
fn ready_handoff_retains_the_exact_full_cut_before_observation_or_actual_exit() {
    let mut f = pending();
    f.now = MonotonicInstant::from_origin(std::time::Duration::from_millis(7));
    f.finished_root();
    f.seal();
    let cut = f.core.facts().ready_cut().unwrap();
    assert_eq!(cut.activation, activation(1));
    assert_eq!(cut.root, root(1));
    assert_eq!(cut.end, end(0, 27));
    assert_eq!(cut.cut, f.now);
    assert_eq!(
        cut.diagnostic_deadline,
        MonotonicInstant::from_origin(std::time::Duration::from_millis(27))
    );
    assert_eq!(
        cut.convergence_deadline,
        MonotonicInstant::from_origin(std::time::Duration::from_millis(97))
    );
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Native(NativeEffect::RecordComputationReady { cut: original })
            if *original == cut
    )));
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishReady { cut: original, .. })
            if *original == cut
    )));
    assert!(!has_begin_convergence(&f));
    assert!(!has_read_seal(&f));
    let relay=f.take(|body| matches!(body,LogicalEffectBody::Relay(RelayEffect::Start {activation:a}) if *a==activation(1)));
    f.result(
        relay.action,
        ActionResult::Relay(RelayFact::EndRelayed {
            activation: activation(1),
            root: root(1),
            end: end(0, 27),
        }),
    );
    let seal = f.take(|body| {
        matches!(
            body,
            LogicalEffectBody::Native(NativeEffect::SealRootRead { .. })
        )
    });
    let LogicalEffectBody::Native(NativeEffect::SealRootRead {
        activation: a,
        sealed,
    }) = &seal.body
    else {
        unreachable!()
    };
    assert_eq!(*a, activation(1));
    assert_eq!(sealed.task(), root(1));
    assert_eq!(f.core.facts().ready_cut(), Some(cut));
    assert!(!has_begin_convergence(&f));
    // Physical action exit is independent from its successful callback result.
    f.exit(seal.action);
    assert!(!has_begin_convergence(&f));
    f.result(seal.action, ActionResult::Native(NativeFact::Applied));
    assert!(f.effects.iter().any(|e| matches!(
        &e.body,
        LogicalEffectBody::AttemptPort(p)
            if matches!(&p.body, LogicalToAttemptBody::BeginConvergence {
                computation_ready: Some(original),
                remote_deadline,
                ..
            } if *original == cut && *remote_deadline == cut.convergence_deadline)
    )));
    assert!(!f.core.facts().original_internal_consumer_exited());
    assert!(!f.core.facts().protocol_join_ready());
}

fn has_begin_convergence(f: &Fixture) -> bool {
    f.effects.iter().any(|effect|matches!(&effect.body,LogicalEffectBody::AttemptPort(message) if matches!(message.body,LogicalToAttemptBody::BeginConvergence {..})))
}
fn has_read_seal(f: &Fixture) -> bool {
    f.effects.iter().any(|effect| {
        matches!(
            &effect.body,
            LogicalEffectBody::Native(NativeEffect::SealRootRead { .. })
        )
    })
}
#[test]
fn original_stopped_relay_is_not_a_normal_end_or_ack_handoff() {
    let mut f = accepted();
    let cut = f.core.facts().ready_cut().unwrap();
    let relay = f.take(|body| matches!(body, LogicalEffectBody::Relay(RelayEffect::Start { .. })));
    f.ack(
        relay.action,
        ActionResult::Relay(RelayFact::Stopped {
            activation: activation(1),
            all_deliveries_settled: true,
        }),
    );
    assert_eq!(f.core.facts().ready_cut(), Some(cut));
    assert!(!has_read_seal(&f));
    assert!(!has_begin_convergence(&f));
    assert!(!f.core.facts().protocol_join_ready());
}
#[test]
fn original_end_handoff_never_manufactures_missing_root_finished() {
    let mut f = pending();
    let relay = f.take(|body| matches!(body, LogicalEffectBody::Relay(RelayEffect::Start { .. })));
    f.ack(
        relay.action,
        ActionResult::Relay(RelayFact::EndRelayed {
            activation: activation(1),
            root: root(1),
            end: end(0, 27),
        }),
    );
    assert!(f.core.facts().ready_cut().is_none());
    assert!(!has_read_seal(&f));
    assert!(!has_begin_convergence(&f));
    f.finished_root();
    assert!(!has_read_seal(&f));
    f.seal();
    assert!(f.core.facts().ready_cut().is_some());
    assert!(has_read_seal(&f));
    assert!(!has_begin_convergence(&f));
}
#[test]
fn original_read_close_refusal_preserves_ready_and_is_not_actual_exit() {
    let mut f = accepted();
    let cut = f.core.facts().ready_cut().unwrap();
    let relay = f.take(|body| matches!(body, LogicalEffectBody::Relay(RelayEffect::Start { .. })));
    f.ack(
        relay.action,
        ActionResult::Relay(RelayFact::EndRelayed {
            activation: activation(1),
            root: root(1),
            end: end(0, 27),
        }),
    );
    let seal = f.take(|body| {
        matches!(
            body,
            LogicalEffectBody::Native(NativeEffect::SealRootRead { .. })
        )
    });
    let error = original_error();
    f.result(
        seal.action,
        ActionResult::Native(NativeFact::Failed {
            execution: activation(1).execution(),
            failure: FailureReport {
                error: error.clone(),
                original_attempt_failure: None,
                pre_ready_replacement: None,
            },
        }),
    );
    assert_eq!(f.core.facts().ready_cut(), Some(cut));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert!(f.effects.iter().any(|effect|matches!(&effect.body,LogicalEffectBody::SupervisionFailed(original) if *original==error)));
    assert!(!has_begin_convergence(&f));
    assert!(!f.core.facts().protocol_join_ready());
}
