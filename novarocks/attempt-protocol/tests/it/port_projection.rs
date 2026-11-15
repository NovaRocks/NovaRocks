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

//! Passive projection contracts. Inputs are explicit immutable values; no host work runs.

use novarocks_attempt_protocol::{
    error::TaskExecutionError,
    failure::{AttemptFailure, HostFailureRef},
    port_projection::{
        FailureProjectionError, operation_failure_fact, operation_failure_input, project_failure,
        replacement_refusal,
    },
};
use novarocks_coordination_contract::{
    ExecutionFailureInput, ExecutionFailureSite, IdSeed, OperationIds, QueryFailureReason,
    TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, ParticipationRef, SafeDetail, TaskIdentity, TaskOperationId,
    failure::{FailureClass, FailureProjection},
    operation::OperationResult,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use std::num::NonZeroU64;

fn operation(seed: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(seed, 100)).issue()
}
fn backend(seed: u128) -> BackendProcessId {
    BackendProcessId::try_from_bytes(operation(seed).to_bytes()).unwrap()
}
fn participation() -> ParticipationRef {
    ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(operation(700).to_bytes()).unwrap(),
        backend(1),
    )
}

#[test]
fn replacement_keeps_the_original_establish_or_create_refusal() {
    let original = participation();
    let operation_id = operation(100);
    let replacement = backend(2);
    let rejection = OperationResult::Verdict {
        outcome: OperationOutcome::IdentityMismatch,
        detail: Some(SafeDetail::truncating("original worker refusal")),
        acknowledgement: None,
    };
    let task = TaskIdentity::new(
        original.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        original.backend_process_id(),
    );
    for (kind, refused) in [
        (
            OperationKind::UpdateParticipation,
            TaskExecutionError::PreReadyEstablishRejected {
                participation: original,
                operation_id,
                rejection: rejection.clone(),
            },
        ),
        (
            OperationKind::CreateTask,
            TaskExecutionError::OperationFailed {
                task: Some(task),
                participation: original,
                operation_id,
                kind: OperationKind::CreateTask,
                rejection: rejection.clone(),
            },
        ),
    ] {
        let refusal = AttemptFailure::TaskProtocol(Box::new(refused));
        let projected = replacement_refusal(&refusal).unwrap();
        let selected = |evidence_operation| AttemptFailure::PreReadyReplacement {
            refusal: Box::new(refusal.clone()),
            participation: original,
            operation_id: evidence_operation,
            kind,
            replacement,
        };
        let actual = project_failure(&selected(operation_id), &mut |_| None).unwrap();
        assert_eq!(actual.input(), &projected);
        assert_eq!(actual.input().detail().as_str(), "original worker refusal");
        assert_eq!(
            actual.input().fact().reason(),
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON
        );
        assert_eq!(
            actual
                .pre_ready_replacement()
                .unwrap()
                .replacement_process(),
            replacement
        );
        let wrong = project_failure(&selected(operation(101)), &mut |_| None).unwrap();
        assert_eq!(wrong.input(), &projected);
        assert!(wrong.pre_ready_replacement().is_none());
    }
}

#[test]
fn attempt_owner_failure_and_worker_owner_failure_keep_distinct_original_facts() {
    let attempt = project_failure(&AttemptFailure::OwnerFailed, &mut |_| None).unwrap();
    assert_eq!(attempt.input().fact().class(), FailureClass::Unexpected);
    assert_eq!(
        attempt.input().fact(),
        QueryFailureReason::NativeFuturePanicked.failure_fact()
    );
    assert_eq!(
        attempt.input().detail().as_str(),
        "Native attempt Core failed"
    );
    let worker = operation_failure_input(
        participation(),
        operation(100),
        OperationKind::ReleaseParticipation,
        &OperationResult::Verdict {
            outcome: OperationOutcome::OwnerFailed,
            detail: None,
            acknowledgement: None,
        },
    );
    assert_eq!(worker.fact().class(), FailureClass::Invariant);
    assert_ne!(worker.fact(), attempt.input().fact());
    assert_eq!(
        worker.detail().as_str(),
        "ReleaseParticipation returned OwnerFailed"
    );
    assert_eq!(
        worker.site(),
        ExecutionFailureSite::Operation {
            participation: participation(),
            operation_id: operation(100),
            kind: OperationKind::ReleaseParticipation
        }
    );
}

#[test]
fn host_projection_requires_the_exact_original_reference_without_reclassification() {
    use novarocks_attempt_protocol::failure::{
        AttemptFailureSite, HostFailureKind, HostProjectedFailure,
    };
    use novarocks_coordination_contract::AttemptFailureClass;
    let reference = HostFailureRef(NonZeroU64::new(7).unwrap());
    let original = ExecutionFailureInput::new(
        QueryFailureReason::TransportCapacity.failure_fact(),
        SafeDetail::truncating("original host resource failure"),
        ExecutionFailureSite::Coordinator,
    );
    let failure = AttemptFailure::Host(HostProjectedFailure {
        original: reference,
        kind: HostFailureKind::Workload,
        class: AttemptFailureClass::ResourceGovernance,
        site: AttemptFailureSite::Attempt,
    });
    assert_eq!(
        project_failure(&failure, &mut |_| None),
        Err(FailureProjectionError::MissingOriginalHostFailure(
            reference
        ))
    );
    let projected = project_failure(&failure, &mut |requested| {
        (requested == reference).then(|| original.clone())
    })
    .unwrap();
    assert_eq!(projected.input(), &original);
}

#[test]
fn wait_verdict_in_a_terminal_error_branch_remains_an_invariant() {
    for outcome in [
        OperationOutcome::NotReady,
        OperationOutcome::PreparationBusy,
        OperationOutcome::ReleaseNotReady,
        OperationOutcome::AdmissionTicketStillActive,
    ] {
        let fact = operation_failure_fact(outcome);
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert!(fact.resource().is_none());
    }
}
