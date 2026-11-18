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
    ExecutionFailureSite, IdSeed, OperationIds, PreReadyProcessReplacementWitness,
    QueryFailureReason, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    OperationKind, ParticipationRef, SafeDetail,
    failure::{FailureClass, FailureFact, FailureProjection},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId};

fn execution(attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(21, 22), AttemptId::new(attempt).unwrap()).unwrap()
}
fn activation(attempt: u64, actor: u64, generation: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(actor, execution(attempt), generation)
}
fn initial() -> AttemptActivationIdentity {
    activation(1, 17, 9)
}
fn state(output: LogicalOutputMode) -> LogicalExecutionState {
    LogicalExecutionState::new(
        initial(),
        RecoveryMode::RestartAttemptBeforeVisibility,
        ExecutionEffect::None,
        output,
        3,
    )
    .unwrap()
}
fn prerequisites() -> ReplacementPrerequisites {
    ReplacementPrerequisites {
        reachable_participations_closed: true,
        attempt_isolation_proven: true,
        stale_usage_accounted: true,
        new_capacity_admitted: true,
    }
}
fn original(seed: u128, detail: &str) -> ExecutionFailureInput {
    let backend = BackendProcessId::try_from_bytes(
        OperationIds::new(IdSeed::new(seed, 100)).issue().to_bytes(),
    )
    .unwrap();
    ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Backend { process: backend },
    )
}
fn failure() -> ExecutionFailure {
    ExecutionFailure::from_input(original(1, "original failed backend"))
}
fn begin(state: &mut LogicalExecutionState) -> QualificationIdentity {
    state
        .begin_replacement(initial(), execution(2), &failure(), false)
        .unwrap()
}
fn successor(state: &mut LogicalExecutionState) -> AttemptActivationIdentity {
    let identity = begin(state);
    let next = state
        .activate_replacement(identity, prerequisites())
        .unwrap();
    state.mark_running(next).unwrap();
    next
}

#[test]
fn initial_attempt_requires_exact_running_capability_once() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    assert_eq!(
        state.mark_running(activation(1, 17, 1)),
        Err(ActorStateError::StaleActivation)
    );
    assert_eq!(
        state.mark_running(activation(1, 18, 9)),
        Err(ActorStateError::StaleActivation)
    );
    state.mark_running(initial()).unwrap();
    assert_eq!(
        state.mark_running(initial()),
        Err(ActorStateError::WrongPhase)
    );
    state
        .conclude(initial(), LogicalConclusion::Succeeded)
        .unwrap();
    assert_eq!(state.conclusion(), Some(LogicalConclusion::Succeeded));
    assert_eq!(state.check_invariants(), Ok(()));
}

#[test]
fn begin_delivery_then_replacement_is_refused() {
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    state.mark_output_visible(initial()).unwrap();
    assert_eq!(
        state.begin_replacement(initial(), execution(2), &failure(), false),
        Err(ActorStateError::RecoveryRefused(
            RecoveryRefusal::OutputVisible
        ))
    );
    assert_eq!(state.current_activation(), Some(initial()));
}

#[test]
fn replacement_revokes_first_and_activates_only_after_owned_facts() {
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    let identity = begin(&mut state);
    assert_eq!(identity.generation, initial().generation() + 1);
    assert_eq!(state.current_activation(), None);
    assert_eq!(
        state.attempt(execution(1)).unwrap().disposition,
        AttemptDisposition::Residual
    );
    assert_eq!(
        state.activate_replacement(identity, ReplacementPrerequisites::default()),
        Err(ActorStateError::ReplacementNotReady)
    );
    let second = state
        .activate_replacement(identity, prerequisites())
        .unwrap();
    assert_eq!(second, activation(2, 17, 10));
    assert_eq!(state.attempts_started(), 2);
    assert_eq!(state.check_invariants(), Ok(()));
    assert_eq!(
        state.activate_replacement(identity, prerequisites()),
        Err(ActorStateError::StaleReplacement)
    );
}

#[test]
fn completion_only_execution_cannot_enter_result_delivery() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    state.mark_running(initial()).unwrap();
    assert_eq!(
        state.mark_output_visible(initial()),
        Err(ActorStateError::WrongPhase)
    );
    assert_eq!(
        state.commit_stream_success(initial()),
        Err(ActorStateError::WrongPhase)
    );
    state
        .conclude(initial(), LogicalConclusion::Succeeded)
        .unwrap();
}

#[test]
fn streamed_success_requires_the_visibility_owner_end_decision() {
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    assert_eq!(
        state.conclude(initial(), LogicalConclusion::Succeeded),
        Err(ActorStateError::SuccessRequiresEndOfStream)
    );
    state.commit_stream_success(initial()).unwrap();
    assert_eq!(state.conclusion(), Some(LogicalConclusion::Succeeded));
    assert!(
        !state.output_visible(),
        "empty success need not claim a visible data packet"
    );
}

#[test]
fn failure_wins_before_success_eos_authorization() {
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    state
        .conclude(initial(), LogicalConclusion::Failed)
        .unwrap();
    assert_eq!(
        state.commit_stream_success(initial()),
        Err(ActorStateError::AlreadyConcluded)
    );
    assert_eq!(state.conclusion(), Some(LogicalConclusion::Failed));
}

#[test]
fn committed_stream_success_is_not_a_provisional_eof_permit() {
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    state.commit_stream_success(initial()).unwrap();
    for late in [LogicalConclusion::Failed, LogicalConclusion::Cancelled] {
        assert_eq!(
            state.conclude(initial(), late),
            Err(ActorStateError::AlreadyConcluded)
        );
    }
    assert_eq!(state.conclusion(), Some(LogicalConclusion::Succeeded));
    assert_eq!(state.check_invariants(), Ok(()));
}

#[test]
fn replacement_token_binds_the_proposed_attempt() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    state.mark_running(initial()).unwrap();
    let identity = begin(&mut state);
    let foreign = QualificationIdentity {
        replacement: execution(3),
        ..identity
    };
    assert_eq!(
        state.activate_replacement(foreign, prerequisites()),
        Err(ActorStateError::StaleReplacement)
    );
    let wrong_generation = QualificationIdentity {
        generation: identity.generation + 1,
        ..identity
    };
    assert_eq!(
        state.activate_replacement(wrong_generation, prerequisites()),
        Err(ActorStateError::StaleReplacement)
    );
    state
        .activate_replacement(identity, prerequisites())
        .unwrap();
}

#[test]
fn residual_attempt_capability_cannot_conclude_its_replacement() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    state.mark_running(initial()).unwrap();
    let second = successor(&mut state);
    assert_eq!(
        state.conclude(initial(), LogicalConclusion::Failed),
        Err(ActorStateError::NotCurrent)
    );
    assert_eq!(state.current_activation(), Some(second));
    assert_eq!(state.conclusion(), None);
}

#[test]
fn exact_replacement_capability_can_cancel_while_no_attempt_is_current() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    state.mark_running(initial()).unwrap();
    let identity = begin(&mut state);
    state
        .conclude_replacement(identity, LogicalConclusion::Cancelled)
        .unwrap();
    assert_eq!(state.conclusion(), Some(LogicalConclusion::Cancelled));
    assert_eq!(
        state.activate_replacement(identity, prerequisites()),
        Err(ActorStateError::AlreadyConcluded)
    );
    assert_eq!(state.check_invariants(), Ok(()));
}

#[test]
fn success_and_attempt_identity_cannot_cross_actor_instances() {
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    let foreign = activation(1, 18, 9);
    assert_eq!(
        state.commit_stream_success(foreign),
        Err(ActorStateError::StaleActivation)
    );
    assert_eq!(
        state.record_original_attempt_failure(foreign, original(2, "foreign")),
        Err(ActorStateError::StaleActivation)
    );
    assert_eq!(state.conclusion(), None);
    assert!(
        state
            .attempt(execution(1))
            .unwrap()
            .accepted_failure
            .is_none()
    );
}

#[test]
fn construction_failure_is_a_stable_statement_failure() {
    let mut state = state(LogicalOutputMode::ResultStream);
    let input = ExecutionFailureInput::new(
        QueryFailureReason::ResultProtocol.failure_fact(),
        SafeDetail::truncating("schema construction failed"),
        ExecutionFailureSite::Coordinator,
    );
    state
        .record_original_attempt_failure(initial(), input.clone())
        .unwrap();
    state
        .conclude(initial(), LogicalConclusion::Failed)
        .unwrap();
    assert_eq!(
        state.mark_running(initial()),
        Err(ActorStateError::AlreadyConcluded)
    );
    assert_eq!(
        state
            .attempt(execution(1))
            .unwrap()
            .accepted_failure
            .as_ref(),
        Some(&input)
    );
    assert_eq!(state.phase(), ExecutionPhase::Converging);
}

#[test]
fn replacement_witness_is_rechecked_against_actual_running_attempt() {
    let mut ids = OperationIds::new(IdSeed::new(1, 100));
    let foreign_participation = ParticipationRef::new(
        execution(2),
        FrontendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
        BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
    );
    let operation_id = ids.issue();
    let input = ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("exact refusal for another attempt"),
        ExecutionFailureSite::Operation {
            participation: foreign_participation,
            operation_id,
            kind: OperationKind::CreateTask,
        },
    );
    let witness = PreReadyProcessReplacementWitness::try_new(
        &input,
        foreign_participation,
        operation_id,
        BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
    )
    .unwrap();
    let mut failed = ExecutionFailure::from_input(input);
    failed.attach_pre_ready_replacement(witness).unwrap();
    let mut state = state(LogicalOutputMode::ResultStream);
    state.mark_running(initial()).unwrap();
    assert_eq!(
        state.begin_replacement(initial(), execution(2), &failed, false),
        Err(ActorStateError::NotCurrent)
    );
    assert_eq!(state.current_activation(), Some(initial()));
}

#[test]
fn same_original_author_can_refine_its_failure_without_contaminating_the_successor() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    state.mark_running(initial()).unwrap();
    let earlier = original(2, "earlier accepted cause");
    let original = original(3, "same original author selected the authoritative cause");
    state
        .record_original_attempt_failure(initial(), earlier)
        .unwrap();
    state
        .record_original_attempt_failure(initial(), original.clone())
        .unwrap();
    let second = successor(&mut state);
    assert_eq!(
        state
            .attempt(execution(1))
            .unwrap()
            .accepted_failure
            .as_ref(),
        Some(&original)
    );
    assert!(
        state
            .attempt(execution(2))
            .unwrap()
            .accepted_failure
            .is_none()
    );
    state
        .conclude(second, LogicalConclusion::Succeeded)
        .unwrap();
    assert_eq!(
        state
            .attempt(execution(1))
            .unwrap()
            .accepted_failure
            .as_ref(),
        Some(&original)
    );
}

#[test]
fn stale_failure_generation_cannot_replace_an_original_cause() {
    let mut state = state(LogicalOutputMode::CompletionOnly);
    let original = original(3, "same original cause");
    state
        .record_original_attempt_failure(initial(), original.clone())
        .unwrap();
    assert_eq!(
        state.record_original_attempt_failure(activation(1, 17, 8), failure().input().clone()),
        Err(ActorStateError::StaleActivation)
    );
    assert_eq!(
        state
            .attempt(execution(1))
            .unwrap()
            .accepted_failure
            .as_ref(),
        Some(&original)
    );
}

#[test]
fn internal_ready_is_the_cut_that_closes_recovery() {
    let mut state = state(LogicalOutputMode::InternalResultStream);
    state.mark_running(initial()).unwrap();
    assert!(!state.output_visible());
    assert!(state.may_observe_pre_ready_replacement(false));
    state.accept_computation_ready(initial()).unwrap();
    assert!(state.output_visible());
    assert_eq!(
        state.conclusion(),
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(
        state.conclude(initial(), LogicalConclusion::Cancelled),
        Err(ActorStateError::AlreadyConcluded)
    );
    assert!(!state.may_observe_pre_ready_replacement(false));
}

#[test]
fn replacement_identity_and_budget_remain_closed() {
    let mut state = LogicalExecutionState::new(
        initial(),
        RecoveryMode::RestartAttemptBeforeVisibility,
        ExecutionEffect::None,
        LogicalOutputMode::CompletionOnly,
        2,
    )
    .unwrap();
    state.mark_running(initial()).unwrap();
    let foreign_query =
        QueryExecutionId::new(QueryId::new(91, 92), AttemptId::new(2).unwrap()).unwrap();
    assert_eq!(
        state.begin_replacement(initial(), foreign_query, &failure(), false),
        Err(ActorStateError::ForeignQuery)
    );
    assert_eq!(
        state.begin_replacement(initial(), execution(1), &failure(), false),
        Err(ActorStateError::ReusedAttempt)
    );
    let second = successor(&mut state);
    assert_eq!(
        state.begin_replacement(second, execution(3), &failure(), false),
        Err(ActorStateError::RecoveryRefused(
            RecoveryRefusal::AttemptBudget
        ))
    );
    assert_eq!(state.check_invariants(), Ok(()));
}
