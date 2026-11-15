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

//! Actual bounded LP/AP progression; script commands are external facts only.
use crate::fixtures::history::CapturedHistory;
use crate::fixtures::{
    AttemptCase, AttemptScript, FrontendFixture, LogicalCase, LogicalObservation, LogicalScript,
    ProgressBudget, ProgressSelection,
};
use novarocks_control_runtime_core::{history::HistoryFact, identity::ActorId};
use novarocks_coordination_contract::{
    AttemptConvergenceClosure, AttemptRequestToken, ExecutionFailure, ExecutionFailureInput,
    ExecutionFailureSite, QueryFailureReason,
    logical_port::{AttemptTerminal, AttemptToLogicalBody, LogicalToAttemptBody},
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_logical_execution_protocol::effect::{ConsumerEffect, LogicalEffectBody};
use std::{num::NonZeroU64, sync::Arc, time::Duration};

pub(super) async fn until(
    fixture: &mut FrontendFixture,
    logical: &LogicalCase,
    predicate: impl Fn(&LogicalObservation) -> bool,
) -> LogicalObservation {
    for _ in 0..256 {
        let actual = fixture.observe(logical);
        if predicate(&actual) {
            return actual;
        }
        // Pure Core continuation deadlines use the same finite FE timeline.
        // Move that actual clock by one tick; this neither fills a Timer input
        // nor advances the failure supervisor's independent Tokio deadline.
        fixture.advance_time(Duration::from_nanos(1));
        fixture
            .progress(ProgressSelection::All, ProgressBudget::default())
            .await;
    }
    let actual = fixture.observe(logical);
    panic!(
        "the actual shared LP/AP runtime did not satisfy its bounded progress oracle: actor={:?}, next_deadline={:?}, failed={}, exited={}, request_occupied={}, fact_occupied={}, error={:?}, governor={:?}, received={:?}, native_cleanup={:?}, effects={:?}",
        fixture.runtime().snapshot(actual.registration.actor()),
        fixture.runtime().next_deadline(),
        actual.failed,
        actual.exited,
        actual.request_occupied,
        actual.fact_occupied,
        actual.error,
        actual.governor,
        actual.received,
        actual.native_cleanup,
        actual.effects,
    );
}

pub(super) fn publish_empty_success(attempt: &AttemptCase) {
    attempt
        .script
        .enqueue(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(Vec::new()))),
            lifecycle_runtime_filters: None,
        });
}

pub(super) fn completed(actual: &LogicalObservation) -> bool {
    actual.effects.iter().any(|effect| {
        matches!(
            effect,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )
    })
}

pub(super) async fn finish_with_history(fixture: &mut FrontendFixture) -> CapturedHistory {
    fixture.finish().await;
    let actual = CapturedHistory::drain(&fixture.history(), usize::MAX);
    actual
        .check_capture()
        .expect("complete ordered original runtime history");
    actual
        .check_completed_runtime(fixture.runtime(), fixture.ledger())
        .expect("independent history rules for actual retired work, inputs and Scopes");
    actual
}

pub(super) fn assert_actual_exit(
    fixture: &FrontendFixture,
    logical: &LogicalCase,
    attempt: &AttemptCase,
    history: &CapturedHistory,
) {
    let actual = fixture.observe(logical);
    assert!(
        actual.exited,
        "the original logical registration must really be removed"
    );
    assert!(
        attempt.exited(),
        "the exact original AP HostExit must report removal"
    );
    assert_eq!(actual.scope, actual.registration.scope_id());
    history
        .check_scope_removed(actual.scope)
        .expect("all original logical Work retired before its exact Scope removal");
    history
        .check_child_scope_removed(attempt.registration.scope_id(), actual.scope)
        .expect("the exact original AP child must really retire before its same LP parent");
}

fn ordinary_query(token: u64) -> AttemptToLogicalBody {
    AttemptToLogicalBody::PreReadyReplacementQuery {
        token: AttemptRequestToken::new(NonZeroU64::new(token).unwrap()),
        refusal: ExecutionFailure::from_input(ExecutionFailureInput::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original scripted pre-ready query"),
            ExecutionFailureSite::Coordinator,
        )),
    }
}
async fn while_logical_paused(
    fixture: &mut FrontendFixture,
    logical: &LogicalCase,
    predicate: impl Fn(&FrontendFixture) -> bool,
) {
    for _ in 0..64 {
        let actual = fixture
            .progress(
                ProgressSelection::ExceptLogical(logical.start.actor_instance),
                ProgressBudget::default(),
            )
            .await;
        if actual.held_logical && predicate(fixture) {
            return;
        }
    }
    panic!("an actual AP step or original port handoff was blocked by the paused logical actor");
}
fn assert_actual_ap_consumption_between_logical_claim_and_consumption(
    history: &CapturedHistory,
    logical: ActorId,
    attempt: ActorId,
) {
    let mut claimed = false;
    let mut ap_consumed = false;
    let mut witnessed = false;
    for entry in history.entries() {
        match entry.fact {
            HistoryFact::Claimed { target } if target.actor == logical => {
                claimed = true;
                ap_consumed = false;
            }
            HistoryFact::InputConsumed { target, .. } if target.actor == attempt && claimed => {
                ap_consumed = true;
            }
            HistoryFact::InputConsumed { target, .. } if target.actor == logical => {
                witnessed |= claimed && ap_consumed;
                claimed = false;
            }
            _ => {}
        }
    }
    assert!(
        witnessed,
        "the independent history must contain a real AP input consumption while the original LP claim is held"
    );
}

#[tokio::test]
async fn occupied_ordinary_request_does_not_prevent_actual_lifecycle_handoff() {
    let mut fixture = FrontendFixture::new();
    let script = LogicalScript::default();
    let start = LogicalScript::start(8101, 0, 1);
    let logical = fixture.install_logical(start, script);
    let attempt = fixture
        .activate_attempt(
            &logical,
            logical.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let ordinary = attempt.script.enqueue(ordinary_query(501));
    while_logical_paused(&mut fixture, &logical, |f| {
        attempt.script.published().contains(&ordinary) && f.observe(&logical).request_occupied
    })
    .await;
    let lifecycle = attempt
        .script
        .enqueue(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    while_logical_paused(&mut fixture, &logical, |f| {
        attempt.script.published().contains(&lifecycle) && f.observe(&logical).fact_occupied
    })
    .await;
    assert!(
        fixture.observe(&logical).request_occupied,
        "the actual ordinary request remains occupied when Lifecycle enters its independent role"
    );
    let before = attempt.script.steps();
    attempt.script.ping();
    while_logical_paused(&mut fixture, &logical, |_| attempt.script.steps() > before).await;
    until(&mut fixture, &logical, completed).await;
    for _ in 0..64 {
        if attempt.script.received().iter().any(|message| {
            matches!(
                message.body,
                LogicalToAttemptBody::PreReadyReplacementReply { .. }
            )
        }) {
            break;
        }
        fixture
            .progress(ProgressSelection::All, ProgressBudget::default())
            .await;
    }
    assert!(
        attempt.script.received().iter().any(|message| matches!(
            message.body,
            LogicalToAttemptBody::PreReadyReplacementReply { .. }
        )),
        "the original ordinary reply also resumes after LP consumption"
    );
    attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(Vec::new()))),
            lifecycle_runtime_filters: None,
        });
    let committed = fixture.observe(&logical);
    assert!(
        !committed.exited,
        "committed Completion is not original Scope exit"
    );
    assert!(
        !attempt.exited(),
        "accepted convergence is not an AP HostExit"
    );
    assert_eq!(
        committed
            .received
            .iter()
            .filter(|message| matches!(message.body,
        LogicalToAttemptBody::PreReadyReplacementReply { token, .. }
        if token == AttemptRequestToken::new(NonZeroU64::new(501).unwrap())))
            .count(),
        1,
        "the observed exact original request reply is delivered once"
    );
    logical.script.run.open();
    let lp = fixture.observe(&logical).registration.actor();
    let ap = attempt.registration.actor();
    let history = finish_with_history(&mut fixture).await;
    assert_actual_ap_consumption_between_logical_claim_and_consumption(&history, lp, ap);
    assert_actual_exit(&fixture, &logical, &attempt, &history);
}

#[tokio::test]
async fn full_original_fact_position_keeps_its_ticket_while_ap_steps_then_resumes_in_order() {
    let mut fixture = FrontendFixture::new();
    let logical =
        fixture.install_logical(LogicalScript::start(8102, 0, 1), LogicalScript::default());
    let attempt = fixture
        .activate_attempt(
            &logical,
            logical.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let first = attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: false,
            closure: None,
            lifecycle_runtime_filters: None,
        });
    while_logical_paused(&mut fixture, &logical, |f| {
        attempt.script.published().contains(&first) && f.observe(&logical).fact_occupied
    })
    .await;
    let second = attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: false,
            closure: None,
            lifecycle_runtime_filters: None,
        });
    let before = attempt.script.steps();
    attempt.script.ping();
    while_logical_paused(&mut fixture, &logical, |_| attempt.script.steps() > before).await;
    assert_eq!(
        attempt.script.published(),
        vec![first],
        "Full must retain the exact second AP ticket, not claim an uncommitted handoff"
    );
    for _ in 0..8 {
        fixture
            .progress(
                ProgressSelection::ExceptLogical(logical.start.actor_instance),
                ProgressBudget::default(),
            )
            .await;
    }
    let idle = fixture
        .progress(
            ProgressSelection::ExceptLogical(logical.start.actor_instance),
            ProgressBudget::default(),
        )
        .await;
    assert!(idle.held_logical);
    assert_eq!(
        (idle.actors, idle.hosts),
        (0, 0),
        "the retained Full ticket must wait on actual retirement, not reschedule idle AP turns"
    );
    for _ in 0..64 {
        fixture
            .progress(ProgressSelection::All, ProgressBudget::default())
            .await;
        if attempt.script.published().contains(&second) {
            break;
        }
    }
    assert_eq!(
        attempt.script.published(),
        vec![first, second],
        "the original retained ticket resumes exactly once after actual LP retirement"
    );
    publish_empty_success(&attempt);
    logical.script.run.open();
    until(&mut fixture, &logical, completed).await;
    let lp = fixture.observe(&logical).registration.actor();
    let ap = attempt.registration.actor();
    let history = finish_with_history(&mut fixture).await;
    assert_actual_ap_consumption_between_logical_claim_and_consumption(&history, lp, ap);
    assert_actual_exit(&fixture, &logical, &attempt, &history);
}

#[tokio::test]
async fn one_held_logical_claim_does_not_exclude_a_sibling_from_the_shared_runtime() {
    let mut fixture = FrontendFixture::new();
    let first = fixture.install_logical(LogicalScript::start(8103, 0, 1), LogicalScript::default());
    let first_attempt = fixture
        .activate_attempt(
            &first,
            first.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let second =
        fixture.install_logical(LogicalScript::start(8104, 0, 1), LogicalScript::default());
    let second_attempt = fixture
        .activate_attempt(
            &second,
            second.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let held = first_attempt.script.enqueue(ordinary_query(502));
    while_logical_paused(&mut fixture, &first, |f| {
        first_attempt.script.published().contains(&held) && f.observe(&first).request_occupied
    })
    .await;
    publish_empty_success(&second_attempt);
    second.script.run.open();
    while_logical_paused(&mut fixture, &first, |f| completed(&f.observe(&second))).await;
    assert!(
        fixture.observe(&first).request_occupied,
        "sibling progress must not counterfeit retirement of the blocked original request"
    );
    assert!(!fixture.observe(&second).failed);
    publish_empty_success(&first_attempt);
    first.script.run.open();
    until(&mut fixture, &first, completed).await;
    let history = finish_with_history(&mut fixture).await;
    assert_actual_exit(&fixture, &first, &first_attempt, &history);
    assert_actual_exit(&fixture, &second, &second_attempt, &history);
}
