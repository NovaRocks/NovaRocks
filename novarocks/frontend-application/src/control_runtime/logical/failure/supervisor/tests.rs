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

//! Passive original-governance/deadline boundaries only. These unit probes
//! supply no Native resource, producer, Scope or credentialed-Join exit proof.

use super::*;
use novarocks_coordination_contract::{MonotonicInstant, QueryExecutionErrorKind};
use novarocks_logical_execution_protocol::input::ObligationKind;
use novarocks_query_application::coordination::executor::GovernanceObligationSnapshot;
use novarocks_types::{QueryId, identity::AttemptId};
use std::time::{Duration, Instant};

fn activation(actor: u64, generation: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        actor,
        QueryExecutionId::new(QueryId::new(7, 11), AttemptId::new(2).unwrap()).unwrap(),
        generation,
    )
}
fn row(
    activation: AttemptActivationIdentity,
    kind: ObligationKind,
    state: GovernanceObligationState,
) -> GovernanceObligationSnapshot {
    GovernanceObligationSnapshot {
        activation,
        kind,
        state,
    }
}
fn assert_conflict(error: QueryExecutionError) {
    assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
    assert_eq!(
        error.message(),
        "original governance kinds have conflicting remote dispositions"
    );
}

#[test]
fn completed_normal_disposition_is_retained_when_late_opposite_observation_is_refused() {
    let original = activation(3, 19);
    for (settled, opposite, positive) in [
        (
            GovernanceObligationState::Resolved,
            GovernanceObligationState::RemoteTrackingEnded,
            true,
        ),
        (
            GovernanceObligationState::RemoteTrackingEnded,
            GovernanceObligationState::Resolved,
            false,
        ),
    ] {
        let mut remote = Vec::new();
        remember_completed(
            &mut remote,
            &[row(original, ObligationKind::RunningWork, settled)],
        )
        .unwrap();
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].activation, original);
        assert_eq!(remote[0].positive, positive);
        // Same-owner completed rows remain idempotent across rescans.
        remember_completed(
            &mut remote,
            &[
                row(original, ObligationKind::RunningWork, settled),
                row(original, ObligationKind::RetiredAttempt, settled),
            ],
        )
        .unwrap();
        assert_eq!(remote.len(), 1);
        let error = remember_completed(
            &mut remote,
            &[row(original, ObligationKind::RetiredAttempt, opposite)],
        )
        .unwrap_err();
        assert_conflict(error);
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].activation, original);
        assert_eq!(
            remote[0].positive, positive,
            "later observation cannot overwrite the retained normal end"
        );
    }
}

#[test]
fn conflicting_completed_governance_kinds_are_refused_in_either_order() {
    let original = activation(3, 19);
    let resolved = row(
        original,
        ObligationKind::RunningWork,
        GovernanceObligationState::Resolved,
    );
    let ended = row(
        original,
        ObligationKind::RetiredAttempt,
        GovernanceObligationState::RemoteTrackingEnded,
    );
    for (rows, first_positive) in [([resolved, ended], true), ([ended, resolved], false)] {
        let mut remote = Vec::new();
        assert_conflict(remember_completed(&mut remote, &rows).unwrap_err());
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].activation, original);
        assert_eq!(
            remote[0].positive, first_positive,
            "conflict preserves the first original completed fact"
        );
    }
}

#[test]
fn completed_dispositions_keep_full_activation_generation_and_actor_separate() {
    let first = activation(3, 19);
    let next_generation = activation(3, 20);
    let other_actor = activation(4, 19);
    assert_eq!(first.execution(), next_generation.execution());
    let mut remote = Vec::new();
    remember_completed(
        &mut remote,
        &[
            row(
                first,
                ObligationKind::RunningWork,
                GovernanceObligationState::Resolved,
            ),
            row(
                next_generation,
                ObligationKind::RunningWork,
                GovernanceObligationState::RemoteTrackingEnded,
            ),
            row(
                other_actor,
                ObligationKind::RetiredAttempt,
                GovernanceObligationState::Resolved,
            ),
        ],
    )
    .unwrap();
    assert_eq!(remote.len(), 3);
    for (identity, positive) in [(first, true), (next_generation, false), (other_actor, true)] {
        let selected = remote
            .iter()
            .filter(|entry| entry.activation == identity)
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].positive, positive);
    }
}

#[test]
fn absent_held_and_in_flight_rows_supply_no_completed_remote_disposition() {
    let original = activation(3, 19);
    let other = activation(3, 20);
    for state in [
        GovernanceObligationState::Absent,
        GovernanceObligationState::Registering,
        GovernanceObligationState::Held,
        GovernanceObligationState::Settling,
    ] {
        let mut remote = Vec::new();
        remember_completed(
            &mut remote,
            &[
                row(original, ObligationKind::RunningWork, state),
                row(original, ObligationKind::RetiredAttempt, state),
            ],
        )
        .unwrap();
        assert!(
            remote.is_empty(),
            "an unfinished original row proves no positive or negative remote end"
        );
        remote.push(Remote {
            activation: other,
            positive: false,
        });
        remember_completed(
            &mut remote,
            &[row(original, ObligationKind::RunningWork, state)],
        )
        .unwrap();
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].activation, other);
        assert!(!remote[0].positive);
    }
}

fn monotonic(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}

#[test]
fn earlier_original_cut_strictly_shortens_the_same_fixed_absolute_deadline() {
    // One sampled absolute anchor, never sampled again during reconstruction.
    let fixed = Instant::now()
        .checked_add(Duration::from_secs(300))
        .unwrap();
    let end = monotonic(100);
    let original = shorten_deadline(fixed, end, end).unwrap();
    let earlier = shorten_deadline(fixed, end, monotonic(80)).unwrap();
    let earliest = shorten_deadline(fixed, end, monotonic(35)).unwrap();
    assert_eq!(original, fixed);
    assert!(earliest < earlier && earlier < original);
    assert_eq!(fixed.duration_since(earlier), Duration::from_secs(20));
    assert_eq!(fixed.duration_since(earliest), Duration::from_secs(65));
}

#[test]
fn repeated_or_later_cut_never_renews_the_original_absolute_window() {
    let fixed = Instant::now()
        .checked_add(Duration::from_secs(300))
        .unwrap();
    let end = monotonic(100);
    let retained = shorten_deadline(fixed, end, monotonic(80)).unwrap();
    for _ in 0..4 {
        assert_eq!(
            shorten_deadline(fixed, end, monotonic(80)).unwrap(),
            retained
        );
    }
    // A cut beyond the fixed end has zero shortening; it cannot extend end.
    assert_eq!(shorten_deadline(fixed, end, monotonic(120)).unwrap(), fixed);
    assert_eq!(shorten_deadline(fixed, end, monotonic(100)).unwrap(), fixed);
    assert!(retained < fixed);
}

#[test]
fn closure_requires_the_exact_duplicate_free_frozen_roster_for_either_disposition() {
    use novarocks_coordination_contract::AttemptParticipationConvergence as C;
    use novarocks_execution_contract::ParticipationRef;
    use novarocks_types::{BackendProcessId, FrontendProcessId};
    let execution = activation(3, 19).execution();
    let frontend = FrontendProcessId::new_v7();
    let a = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
    let b = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
    let foreign = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
    for rows in [
        vec![a, b],
        vec![b, a],
        vec![a, a],
        vec![a, foreign],
        vec![a],
        vec![],
    ] {
        let expected = rows.len() == 2 && rows.contains(&a) && rows.contains(&b);
        assert_eq!(
            closure_matches_roster(
                &[a, b],
                &AttemptConvergenceClosure::Evidence(
                    rows.iter().map(|p| (*p, C::NeverEstablished)).collect()
                )
            ),
            expected
        );
        assert_eq!(
            closure_matches_roster(
                &[a, b],
                &AttemptConvergenceClosure::RemoteTrackingEnded {
                    exact_participations: rows.into()
                }
            ),
            expected
        );
    }
    assert!(!closure_matches_roster(
        &[a, a],
        &AttemptConvergenceClosure::Evidence(vec![
            (a, C::WorkerProcessReplaced),
            (a, C::WorkerStoppedAndParticipationFenced)
        ])
    ));
    assert!(
        closure_matches_roster(&[], &AttemptConvergenceClosure::Evidence(vec![])),
        "a genuinely frozen empty roster is explicit, never unavailable"
    );
}
