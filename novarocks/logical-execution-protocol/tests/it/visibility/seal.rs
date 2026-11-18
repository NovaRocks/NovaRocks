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

use crate::support::*;
use novarocks_coordination_contract::{
    AttemptRootFailure, AttemptRootProjection, ExecutionFailure, ExecutionFailureInput,
    ExecutionFailureSite, QueryFailureReason, SelectedTaskTermination,
};
use novarocks_execution_contract::{
    CancelReason, SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
};

fn running_end() -> (Fixture, AttemptRequestToken) {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
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
        AttemptRootProjection {
            status,
            failure: AttemptRootFailure::None,
        },
    )));
    f.open(false);
    f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(0, 0),
        accepted_packets: 0,
        delivered_through: None,
    });
    let effect = f.take(|b| {
        matches!(
            b, LogicalEffectBody::AttemptPort(p)
                if matches!(p.body, LogicalToAttemptBody::SealRequest { .. })
        )
    });
    let LogicalEffectBody::AttemptPort(p) = effect.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::SealRequest { token } = p.body else {
        unreachable!()
    };
    (f, token)
}
fn no_success(f: &Fixture) {
    assert!(!matches!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    ));
    assert!(!f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(
            ConsumerEffect::ReserveEndSlot { .. } | ConsumerEffect::EnqueueEnd { .. }
        )
    )));
}
fn original_ap_seal_refusal() -> ExecutionFailure {
    // AP core/turn publishes protocol_failure(Schedule(refusal.message())).
    // AP port_projection maps that exact source to this passive value; LP
    // consumes it without decoding the schedule error or inventing a cause.
    ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating("frontend task protocol could not continue"),
        ExecutionFailureSite::Coordinator,
    ))
}

#[test]
fn finished_native_completed_and_local_end_cannot_replace_original_success_seal() {
    let (mut f, token) = running_end();
    f.finished_root();
    let run = f.run.take().unwrap();
    f.ack(
        run,
        ActionResult::Native(NativeFact::RunCompleted {
            activation: activation(1),
        }),
    );
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ));
    assert_eq!(f.core.facts().conclusion(), None);
    no_success(&f);
    assert!(!f.core.facts().protocol_join_ready());
    let original = original_ap_seal_refusal();
    f.port(AttemptToLogicalBody::SealResult {
        token,
        result: Err(original.clone()),
    });
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(f.core.facts().failure(), Some(original.error()));
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(original.input())
    );
    no_success(&f);
}

#[test]
fn cancelled_root_and_derived_failure_latch_wait_the_original_seal_refusal() {
    for cancelled in [true, false] {
        let (mut f, token) = running_end();
        if cancelled {
            for (version, state) in [(4, TaskState::Canceling), (5, TaskState::Canceled)] {
                let status = TaskStatus::try_new(
                    root(1),
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    Some(TerminationDetail::Canceled(
                        CancelReason::UpstreamNoLongerNeeded,
                    )),
                    TaskOutputFacts::new(false),
                )
                .unwrap();
                f.port(AttemptToLogicalBody::RootStatus(Box::new(
                    AttemptRootProjection {
                        status,
                        failure: AttemptRootFailure::None,
                    },
                )));
            }
        } else {
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
                    failure: AttemptRootFailure::DerivedPending,
                },
            )));
        }
        assert_eq!(f.core.facts().conclusion(), None);
        assert!(f.core.facts().failure().is_none());
        no_success(&f);
        let original = original_ap_seal_refusal();
        f.port(AttemptToLogicalBody::SealResult {
            token,
            result: Err(original.clone()),
        });
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert_eq!(f.core.facts().failure(), Some(original.error()));
        no_success(&f);
        assert!(
            !f.effects.iter().any(|e| matches!(
                &e.body, LogicalEffectBody::AttemptPort(p)
                    if matches!(p.body, LogicalToAttemptBody::SealRequest { .. })
            )),
            "a refused original seal never silently retries"
        );
    }
}

#[test]
fn authoritative_attempt_failure_latched_before_seal_refusal_keeps_original_task_cause() {
    let (mut f, token) = running_end();
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
            SafeDetail::truncating("original producer failure before seal reply"),
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
        AttemptRootProjection {
            status,
            failure: AttemptRootFailure::Authoritative(selected),
        },
    )));
    f.port(AttemptToLogicalBody::SealResult {
        token,
        result: Err(original_ap_seal_refusal()),
    });
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    assert_eq!(
        f.core.facts().failure().unwrap().execution_failure(),
        Some(&original)
    );
    assert_eq!(
        f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&original)
    );
    no_success(&f);
}

#[test]
fn foreign_root_observation_cannot_advance_the_registered_root_or_request_success_seal() {
    let mut f = Fixture::new(LogicalOutputMode::ResultStream);
    f.begin();
    let foreign = TaskIdentity::new(
        activation(1).execution(),
        root(1).stage_id(),
        root(1).task_id(),
        novarocks_types::BackendProcessId::try_from_bytes(
            OperationIds::new(IdSeed::new(9000, 10)).issue().to_bytes(),
        )
        .unwrap(),
    );
    let status = TaskStatus::try_new(
        foreign,
        TaskStatusVersion::new(6).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status,
            failure: AttemptRootFailure::None,
        },
    )));
    assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
    let original = f.core.facts().failure().unwrap().clone();
    assert_eq!(
        original.execution_failure().unwrap().fact().reason().code(),
        99
    );
    assert!(
        f.core
            .facts()
            .attempts()
            .next()
            .unwrap()
            .root_projection
            .is_none()
    );
    f.relay(RelayRequest::OpenDelivery {
        root: root(1),
        carrier: carrier(false),
    });
    let request = f.relay(RelayRequest::EndConsumed {
        root: root(1),
        end: end(0, 0),
        accepted_packets: 0,
        delivered_through: None,
    });
    assert!(f.effects.iter().any(|e| matches!(
        &e.body, LogicalEffectBody::Relay(RelayEffect::Reply {
            token, response: RelayResponse::Rejected(error), ..
        }) if *token == request && error == &original
    )));
    assert!(!f.effects.iter().any(|e| matches!(
        &e.body, LogicalEffectBody::AttemptPort(p)
            if matches!(p.body, LogicalToAttemptBody::SealRequest { .. })
    )));
    no_success(&f);
}
