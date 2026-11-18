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
    ExecutionEffect, ExecutionFailureSite, IdSeed, OperationIds, RecoveryMode,
    TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
};
use novarocks_execution_contract::{
    OperationKind, ParticipationRef, SafeDetail,
    failure::{FailureClass, FailureFact},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryId};
use std::num::NonZeroU64;

fn activation(attempt: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        17,
        QueryExecutionId::new(QueryId::new(61, 62), AttemptId::new(attempt).unwrap()).unwrap(),
        attempt + 9,
    )
}
fn token(value: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(value).unwrap())
}
fn refusal(activation: AttemptActivationIdentity) -> ExecutionFailureInput {
    let mut ids = OperationIds::new(IdSeed::new(1, 100));
    ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("original identity refusal"),
        ExecutionFailureSite::Operation {
            participation: ParticipationRef::new(
                activation.execution(),
                FrontendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
                BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
            ),
            operation_id: ids.issue(),
            kind: OperationKind::CreateTask,
        },
    )
}
fn opportunity(activation: AttemptActivationIdentity) -> PreReadyOpportunity {
    PreReadyOpportunity {
        current_running: Some(activation),
        cancellation_pending: false,
        concluded: false,
        window: RecoveryWindowInput {
            mode: RecoveryMode::RestartAttemptBeforeVisibility,
            effect: ExecutionEffect::None,
            output_visible: false,
            attempts_started: 1,
            max_attempts: 3,
            deadline_reached: false,
        },
    }
}

#[test]
fn an_open_query_registers_without_a_self_sustaining_hint() {
    let first = activation(1);
    let mut observations = PreReadyObservations::default();
    assert!(
        observations
            .query(first, token(1), &refusal(first), opportunity(first), true)
            .unwrap()
    );
    assert_eq!(observations.len(), 1);
    assert_eq!(observations.take_change(), None);
    assert!(!observations.refresh_one(first.execution(), opportunity(first), true));
    assert_eq!(observations.take_change(), None);
}

#[test]
fn only_the_latest_watch_token_is_retained_for_an_activation() {
    let first = activation(1);
    let mut observations = PreReadyObservations::default();
    for value in 1..=128 {
        assert!(
            observations
                .query(
                    first,
                    token(value),
                    &refusal(first),
                    opportunity(first),
                    true
                )
                .unwrap()
        );
        assert_eq!(observations.len(), 1);
        assert_eq!(observations.take_change(), None);
    }
    let mut closed = opportunity(first);
    closed.window.output_visible = true;
    assert!(observations.refresh_one(first.execution(), closed, true));
    assert_eq!(
        observations.take_change(),
        Some(PreReadyChange {
            activation: first,
            token: token(128)
        })
    );
    assert_eq!(observations.take_change(), None);
}

#[test]
fn a_real_window_change_hints_once_and_requery_refuses() {
    let first = activation(1);
    let mut observations = PreReadyObservations::default();
    let input = refusal(first);
    observations
        .query(first, token(1), &input, opportunity(first), true)
        .unwrap();
    let mut closed = opportunity(first);
    closed.concluded = true;
    assert!(observations.refresh_one(first.execution(), closed, true));
    assert!(!observations.refresh_one(first.execution(), closed, true));
    assert!(observations.has_changes());
    assert_eq!(
        observations.take_change(),
        Some(PreReadyChange {
            activation: first,
            token: token(1)
        })
    );
    assert!(
        !observations
            .query(first, token(2), &input, closed, true)
            .unwrap()
    );
    assert_eq!(observations.take_change(), None);
    assert_eq!(observations.len(), 0);
}

#[test]
fn late_parent_settlement_rechecks_the_same_ledger_correlation() {
    let first = activation(1);
    let mut observations = PreReadyObservations::default();
    let input = refusal(first);
    observations
        .query(first, token(1), &input, opportunity(first), true)
        .unwrap();
    assert_eq!(observations.refusal(first.execution()), Some(&input));
    assert!(observations.refresh_one(first.execution(), opportunity(first), false));
    assert_eq!(
        observations.take_change(),
        Some(PreReadyChange {
            activation: first,
            token: token(1)
        })
    );
    assert!(
        !observations
            .query(first, token(2), &input, opportunity(first), false)
            .unwrap()
    );
    assert_eq!(observations.take_change(), None);
}

#[test]
fn another_refusal_cannot_displace_the_original_observer() {
    let first = activation(1);
    let mut observations = PreReadyObservations::default();
    let input = refusal(first);
    observations
        .query(first, token(1), &input, opportunity(first), true)
        .unwrap();
    let other = ExecutionFailureInput::new(
        input.fact(),
        SafeDetail::truncating("another refusal"),
        input.site(),
    );
    assert_eq!(
        observations.query(first, token(2), &other, opportunity(first), true),
        Err(PreReadyObservationError::ConflictingRefusal)
    );
    let mut closed = opportunity(first);
    closed.cancellation_pending = true;
    observations.refresh_one(first.execution(), closed, true);
    assert_eq!(observations.take_change().unwrap().token, token(1));
}

#[test]
fn current_activation_policy_and_budget_all_gate_observation() {
    let first = activation(1);
    let open = opportunity(first);
    let mut no_current = open;
    no_current.current_running = Some(activation(2));
    let mut cancelled = open;
    cancelled.cancellation_pending = true;
    let mut visible = open;
    visible.window.output_visible = true;
    let mut budget = open;
    budget.window.attempts_started = budget.window.max_attempts;
    let mut deadline = open;
    deadline.window.deadline_reached = true;
    let mut no_recovery = open;
    no_recovery.window.mode = RecoveryMode::NoRecovery;
    for denied in [
        no_current,
        cancelled,
        visible,
        budget,
        deadline,
        no_recovery,
    ] {
        let mut observations = PreReadyObservations::default();
        assert!(
            !observations
                .query(first, token(1), &refusal(first), denied, true)
                .unwrap()
        );
        assert_eq!(observations.len(), 0);
        assert_eq!(observations.take_change(), None);
    }
}

#[test]
fn watch_and_hint_traversals_can_be_budgeted_one_activation_at_a_time() {
    let first = activation(1);
    let second = activation(2);
    let mut observations = PreReadyObservations::default();
    observations
        .query(first, token(1), &refusal(first), opportunity(first), true)
        .unwrap();
    observations
        .query(
            second,
            token(2),
            &refusal(second),
            opportunity(second),
            true,
        )
        .unwrap();
    let mut closed = opportunity(second);
    closed.concluded = true;
    let one = observations.next_watch_after(None).unwrap();
    observations.refresh_one(one, closed, false);
    assert_eq!(
        observations.take_change().unwrap().activation.execution(),
        one
    );
    assert_eq!(observations.take_change(), None);
    let two = observations.next_watch_after(Some(one)).unwrap();
    observations.refresh_one(two, closed, false);
    assert_eq!(
        observations.take_change().unwrap().activation.execution(),
        two
    );
    assert_eq!(observations.next_watch_after(Some(two)), None);
}

#[test]
fn stale_activation_cannot_rebind_or_remove_the_original_watch() {
    let first = activation(1);
    let mut observations = PreReadyObservations::default();
    observations
        .query(first, token(1), &refusal(first), opportunity(first), true)
        .unwrap();
    let stale = AttemptActivationIdentity::from_parts(
        first.actor().get(),
        first.execution(),
        first.generation() + 1,
    );
    assert_eq!(
        observations.query(stale, token(2), &refusal(first), opportunity(stale), true),
        Err(PreReadyObservationError::StaleActivation)
    );
    assert_eq!(
        observations.remove(stale),
        Err(PreReadyObservationError::StaleActivation)
    );
    let mut closed = opportunity(first);
    closed.concluded = true;
    observations.refresh_one(first.execution(), closed, true);
    observations.remove(first).unwrap();
    assert_eq!(observations.take_change(), None);
    assert_eq!(observations.len(), 0);
}
