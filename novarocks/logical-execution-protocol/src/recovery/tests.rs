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

use super::*;
use novarocks_coordination_contract::{
    ExecutionFailure, ExecutionFailureInput, ExecutionFailureSite, IdSeed, OperationIds,
    QueryFailureReason, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    OperationKind, ParticipationRef, SafeDetail,
    failure::{FailureClass, FailureFact, FailureProjection},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};

fn recoverable<'a>() -> RecoveryInput<'a> {
    RecoveryInput {
        mode: RecoveryMode::RestartAttemptBeforeVisibility,
        effect: ExecutionEffect::None,
        failure: AttemptFailureClass::RecoverableInfrastructure,
        pre_ready_replacement: None,
        output_visible: false,
        attempts_started: 1,
        max_attempts: 2,
        deadline_reached: false,
    }
}

#[test]
fn effect_free_failure_can_enter_replacement_before_visibility() {
    assert_eq!(
        evaluate_recovery(recoverable()),
        RecoveryDecision::BeginReplacement
    );
}

#[test]
fn visibility_and_external_effects_close_recovery_independently() {
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            output_visible: true,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::OutputVisible)
    );
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            effect: ExecutionEffect::External,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::ExternalEffect)
    );
}

#[test]
fn unsupported_capability_never_restarts_a_remote_attempt() {
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            failure: AttemptFailureClass::Unsupported,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
    );
}

#[test]
fn execution_failure_never_restarts_a_remote_attempt() {
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            failure: AttemptFailureClass::ExecutionFailure,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
    );
}

#[test]
fn local_resource_governance_never_restarts_a_remote_attempt() {
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            failure: AttemptFailureClass::ResourceGovernance,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
    );
}

#[test]
fn witnessed_identity_refusal_adds_a_basis_without_reclassifying_or_bypassing_policy() {
    let mut ids = OperationIds::new(IdSeed::new(1, 100));
    let participation = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
        BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
    );
    let operation_id = ids.issue();
    let input = ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("original identity refusal"),
        ExecutionFailureSite::Operation {
            participation,
            operation_id,
            kind: OperationKind::CreateTask,
        },
    );
    let witness = PreReadyProcessReplacementWitness::try_new(
        &input,
        participation,
        operation_id,
        BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
    )
    .unwrap();
    let mut failure = ExecutionFailure::from_input(input);
    failure.attach_pre_ready_replacement(witness).unwrap();
    let basis = RecoveryInput {
        failure: failure.class(),
        pre_ready_replacement: failure.pre_ready_replacement(),
        ..recoverable()
    };
    assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
    assert_eq!(evaluate_recovery(basis), RecoveryDecision::BeginReplacement);
    for (input, expected) in [
        (
            RecoveryInput {
                pre_ready_replacement: None,
                ..basis
            },
            RecoveryRefusal::FailureClass,
        ),
        (
            RecoveryInput {
                mode: RecoveryMode::NoRecovery,
                ..basis
            },
            RecoveryRefusal::Mode,
        ),
        (
            RecoveryInput {
                output_visible: true,
                ..basis
            },
            RecoveryRefusal::OutputVisible,
        ),
        (
            RecoveryInput {
                deadline_reached: true,
                ..basis
            },
            RecoveryRefusal::Deadline,
        ),
        (
            RecoveryInput {
                attempts_started: 2,
                ..basis
            },
            RecoveryRefusal::AttemptBudget,
        ),
    ] {
        assert_eq!(evaluate_recovery(input), RecoveryDecision::Refuse(expected));
    }
}

#[test]
fn failure_basis_precedes_visibility_budget_and_deadline_refusals() {
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            failure: AttemptFailureClass::ExecutionFailure,
            output_visible: true,
            attempts_started: 2,
            deadline_reached: true,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
    );
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            mode: RecoveryMode::NoRecovery,
            failure: AttemptFailureClass::ExecutionFailure,
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::Mode)
    );
}

#[test]
fn a_qualification_unknown_fact_does_not_become_backend_loss() {
    let input = ExecutionFailureInput::new(
        QueryFailureReason::ObservationLost.failure_fact(),
        SafeDetail::truncating("BackendLost is only diagnostic text"),
        ExecutionFailureSite::Coordinator,
    );
    assert_eq!(
        evaluate_recovery(RecoveryInput {
            failure: input.attempt_class(),
            ..recoverable()
        }),
        RecoveryDecision::Refuse(RecoveryRefusal::FailureClass)
    );
}
