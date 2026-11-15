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
use crate::test_support::*;
fn registered(supervised: bool) -> InternalReady {
    let mut state = InternalReady::new(PLAN, budgets());
    state
        .register(
            source(1),
            activation(1),
            root(1),
            PLAN,
            supervised,
            true,
            false,
        )
        .unwrap();
    state
}
fn proposal() -> ReadyProposal {
    ReadyProposal {
        source: source(1),
        token: token(1),
        activation: activation(1),
        root: root(1),
        plan: PLAN,
        receipt: receipt(1),
        end: end(0, 27),
    }
}
fn pending(supervised: bool) -> InternalReady {
    let mut state = registered(supervised);
    state
        .decoded_end(source(1), activation(1), receipt(1), end(0, 27))
        .unwrap();
    assert_eq!(state.propose(proposal(), false), Ok(None));
    state
}
fn accept(state: &mut InternalReady) -> ReadyAcceptance {
    state
        .try_accept(
            now(100),
            activation(1),
            true,
            true,
            true,
            Some(end(0, 27)),
            false,
            false,
        )
        .unwrap()
        .unwrap()
}
#[test]
fn registration_requires_current_activation_original_root_and_plan() {
    let mut state = InternalReady::new(PLAN, budgets());
    assert_eq!(
        state.register(source(1), activation(1), root(1), PLAN, false, false, false),
        Err(ReadyRefusal::ForeignActivation)
    );
    assert_eq!(
        state.register(source(1), activation(1), root(2), PLAN, false, true, false),
        Err(ReadyRefusal::ForeignRoot)
    );
    assert_eq!(
        state.register(
            source(1),
            activation(1),
            root(1),
            LogicalPlanIdentity([98; 16]),
            false,
            true,
            false
        ),
        Err(ReadyRefusal::ForeignPlan)
    );
    state
        .register(source(1), activation(1), root(1), PLAN, false, true, false)
        .unwrap();
    assert_eq!(
        state.register(source(2), activation(1), root(1), PLAN, false, true, false),
        Err(ReadyRefusal::DuplicateSource)
    );
}
#[test]
fn proof_requires_original_decoded_receipt_not_just_matching_end_bytes() {
    let mut state = registered(false);
    assert_eq!(
        state.propose(proposal(), false),
        Err(ReadyRefusal::MissingEndReceipt)
    );
    state
        .decoded_end(source(1), activation(1), receipt(1), end(0, 27))
        .unwrap();
    let mut wrong = proposal();
    wrong.receipt = receipt(2);
    assert_eq!(
        state.propose(wrong, false),
        Err(ReadyRefusal::MissingEndReceipt)
    );
    let mut wrong = proposal();
    wrong.end.output_rows = 28;
    assert_eq!(
        state.propose(wrong, false),
        Err(ReadyRefusal::MissingEndReceipt)
    );
    assert_eq!(
        state.decoded_end(source(1), activation(1), receipt(2), end(0, 27)),
        Err(ReadyRefusal::ConflictingEndReceipt)
    );
}
#[test]
fn each_common_protocol_gate_blocks_ready_until_its_original_fact_arrives() {
    for (finished, sealed, establish, pending_failure) in [
        (false, true, true, false),
        (true, false, true, false),
        (true, true, false, false),
        (true, true, true, true),
    ] {
        let mut state = pending(false);
        assert_eq!(
            state
                .try_accept(
                    now(100),
                    activation(1),
                    finished,
                    sealed,
                    establish,
                    Some(end(0, 27)),
                    pending_failure,
                    false
                )
                .unwrap(),
            None
        );
        assert!(state.accepted().is_none());
        accept(&mut state);
    }
}
#[test]
fn decoded_end_can_arrive_while_root_is_running_without_emitting_ready() {
    let mut state = pending(false);
    assert_eq!(
        state
            .try_accept(
                now(50),
                activation(1),
                false,
                false,
                true,
                Some(end(0, 27)),
                false,
                false
            )
            .unwrap(),
        None
    );
    let accepted = accept(&mut state);
    assert_eq!(accepted.cut.cut, now(100));
    assert_eq!(accepted.cut.end, end(0, 27));
}
#[test]
fn consumed_end_requires_independent_sequence_and_output_row_equality() {
    for wrong in [end(1, 27), end(0, 28)] {
        let mut state = pending(false);
        let error = state
            .try_accept(
                now(100),
                activation(1),
                true,
                true,
                true,
                Some(wrong),
                false,
                false,
            )
            .unwrap_err();
        assert_eq!(
            error,
            LogicalRequestFailure::ResultProtocolViolation.error()
        );
        assert!(state.accepted().is_none());
    }
}
#[test]
fn unsupervised_source_drop_before_cut_fails_closed() {
    let mut state = pending(false);
    let error = state
        .source_closed(source(1), activation(1))
        .unwrap()
        .unwrap();
    assert_eq!(
        error,
        query_failure(
            QueryFailureReason::LogicalOwnerStopped,
            "the internal consumer exited before readiness"
        )
    );
    assert!(
        state
            .try_accept(
                now(100),
                activation(1),
                true,
                true,
                true,
                Some(end(0, 27)),
                false,
                false
            )
            .is_err()
    );
}
#[test]
fn supervised_source_loss_waits_for_reserved_actual_exit_and_preserves_original_error() {
    let mut state = pending(true);
    assert_eq!(state.source_closed(source(1), activation(1)).unwrap(), None);
    assert_eq!(
        state
            .try_accept(
                now(100),
                activation(1),
                true,
                true,
                true,
                Some(end(0, 27)),
                false,
                false
            )
            .unwrap(),
        None
    );
    let original = query_failure(
        QueryFailureReason::InternalConsumerPanicked,
        "original supervised consumer verdict",
    );
    assert_eq!(
        state
            .exit_observed(source(1), activation(1), Some(Err(original.clone())))
            .unwrap(),
        Some(original)
    );
    assert!(state.accepted().is_none());
    assert!(state.original_consumer_exited());
}
#[test]
fn successful_or_missing_consumer_exit_before_ready_is_owner_stopped() {
    for outcome in [None, Some(Ok(()))] {
        let mut state = pending(true);
        assert_eq!(
            state
                .exit_observed(source(1), activation(1), outcome)
                .unwrap(),
            Some(query_failure(
                QueryFailureReason::LogicalOwnerStopped,
                "the internal consumer exited before readiness"
            ))
        );
    }
}
#[test]
fn ready_freezes_original_cut_and_late_exit_does_not_revoke_it() {
    let mut state = pending(true);
    let accepted = accept(&mut state);
    let error = query_failure(QueryFailureReason::LogicalOwnerStopped, "late failure");
    assert_eq!(
        state
            .exit_observed(source(1), activation(1), Some(Err(error)))
            .unwrap(),
        None
    );
    assert_eq!(state.accepted(), Some(accepted));
    let mut repeat = proposal();
    repeat.token = token(2);
    assert_eq!(
        state.propose(repeat, true),
        Ok(Some(ReadyAcceptance {
            token: token(2),
            ..accepted
        }))
    );
}
#[test]
fn cancellation_or_registry_close_reduced_before_cut_prevents_ready() {
    let mut state = pending(false);
    assert_eq!(
        state
            .try_accept(
                now(100),
                activation(1),
                true,
                true,
                true,
                Some(end(0, 27)),
                false,
                true
            )
            .unwrap(),
        None
    );
    assert!(state.accepted().is_none());
}
#[test]
fn old_source_cannot_mutate_or_release_successor_consumer() {
    let mut state = registered(true);
    state
        .register(source(2), activation(2), root(2), PLAN, true, true, false)
        .unwrap();
    assert_eq!(
        state.source_closed(source(1), activation(1)),
        Err(ReadyRefusal::ForeignSource)
    );
    assert_eq!(
        state.exit_observed(source(1), activation(1), None),
        Err(ReadyRefusal::ForeignSource)
    );
    assert_eq!(
        state.propose(proposal(), false),
        Err(ReadyRefusal::ForeignSource)
    );
    assert!(!state.original_consumer_exited());
}
#[test]
fn diagnostic_and_convergence_publications_are_independent_and_never_delay_ready() {
    let mut state = pending(false);
    let accepted = accept(&mut state);
    assert_eq!(accepted.cut.diagnostic_deadline, now(120));
    assert_eq!(accepted.cut.convergence_deadline, now(190));
    let unknown = ConvergenceSnapshot {
        all_participations_closed: false,
        remote_tracking_ended: false,
        local_resources_exited: false,
    };
    assert!(!state.begin_diagnostics(now(119), false));
    assert!(!state.begin_convergence(now(189), unknown));
    assert!(state.begin_diagnostics(now(120), false));
    assert_eq!(state.convergence(), PublicationState::Waiting);
    assert!(state.begin_convergence(now(190), unknown));
    assert_eq!(state.accepted(), Some(accepted));
    assert_eq!(state.pending_observation_deadline(), None);
    assert!(state.diagnostics_published());
    assert!(!state.diagnostics_published());
    assert!(state.convergence_published());
    assert!(!state.convergence_published());
}
#[test]
fn complete_convergence_can_publish_before_diagnostics_without_waiting_its_deadline() {
    let mut state = pending(false);
    accept(&mut state);
    assert!(state.begin_convergence(
        now(101),
        ConvergenceSnapshot {
            all_participations_closed: true,
            remote_tracking_ended: false,
            local_resources_exited: true
        }
    ));
    assert_eq!(state.diagnostics(), PublicationState::Waiting);
    assert!(state.begin_diagnostics(now(102), true));
}

#[test]
fn successor_registration_returns_old_pending_request_for_exact_rejection() {
    let mut state = pending(true);
    assert_eq!(
        state
            .register(source(2), activation(2), root(2), PLAN, true, true, false)
            .unwrap(),
        Some(proposal())
    );
    assert_eq!(state.take_pending_proposal(), None);
    assert!(state.source_matches(source(2), activation(2), root(2)));
    assert_eq!(
        state.retire_source(activation(1)),
        Err(ReadyRefusal::ForeignActivation)
    );
}

#[test]
fn terminal_cleanup_consumes_the_pending_reply_exactly_once() {
    let mut state = pending(false);
    assert_eq!(state.take_pending_proposal(), Some(proposal()));
    assert_eq!(state.take_pending_proposal(), None);
    assert_eq!(state.retire_source(activation(1)).unwrap(), None);
}
