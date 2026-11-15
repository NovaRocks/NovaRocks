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
use novarocks_coordination_contract::{QueryExecutionError, QueryFailureReason};
fn original_error() -> QueryExecutionError {
    novarocks_logical_execution_protocol::failure::query_failure(
        QueryFailureReason::InternalConsumerPanicked,
        "original consumer verdict",
    )
}
fn pending() -> Fixture {
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
    f.internal(InternalRequest::AcceptReady {
        root: root(1),
        plan: PLAN,
        receipt,
        end: end(0, 27),
    });
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
