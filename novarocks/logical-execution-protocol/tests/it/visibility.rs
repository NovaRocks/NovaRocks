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
