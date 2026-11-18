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

//! Failure is local to its original LP; remote closure is independently proven.
use super::progress::{
    assert_actual_exit, completed, finish_with_history, publish_empty_success, until,
};
use crate::fixtures::{
    AttemptCase, AttemptScript, FrontendFixture, LogicalCase, LogicalObservation, LogicalScript,
};
use novarocks_coordination_contract::{
    AttemptStopReason,
    logical_port::{AttemptToLogicalBody, LogicalToAttemptBody},
};
use novarocks_execution_contract::failure::FailureClass;
use novarocks_logical_execution_protocol::effect::NativeEffect;
use novarocks_query_application::coordination::executor::GovernanceObligationState;
use std::{sync::atomic::Ordering, time::Duration};

fn actual_owner_failed_stop(attempt: &AttemptCase) -> bool {
    attempt.script.received().iter().any(|message| matches!(&message.body,
        LogicalToAttemptBody::Stop { reason: AttemptStopReason::LogicalExecutionFailed, failure, .. }
        if failure.input().fact().class() == FailureClass::Invariant))
}
fn assert_actual_obligations_still_held(actual: &LogicalObservation) {
    assert!(
        !actual.governor.is_empty(),
        "the original WC governor must own real obligations"
    );
    assert!(
        actual
            .governor
            .iter()
            .all(|row| row.state == GovernanceObligationState::Held),
        "without positive facts or actual deadline, an LP failure cannot resolve or end tracking"
    );
}
async fn assert_original_deadline_abandons(fixture: &mut FrontendFixture, logical: &LogicalCase) {
    // The failure supervisor waits on the ORIGINAL actual Tokio deadline. Advancing
    // only the FE protocol clock would not prove that this timer has expired.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let actual = until(fixture, logical, |actual| {
        !actual.governor.is_empty()
            && actual
                .governor
                .iter()
                .all(|row| row.state == GovernanceObligationState::RemoteTrackingEnded)
    })
    .await;
    assert!(
        !actual
            .governor
            .iter()
            .any(|row| row.state == GovernanceObligationState::Resolved)
    );
    assert!(
        actual.native_cleanup.iter().any(|call| matches!(call,
        NativeEffect::AbandonResources { activation }
        if *activation == logical.start.first_attempt.activation)),
        "the same scripted Native owner must actually receive Abandon, not a predicted Core effect"
    );
    assert!(!actual.native_cleanup.iter().any(|call| matches!(call,
        NativeEffect::FinishResources { activation }
        if *activation == logical.start.first_attempt.activation)));
}

#[tokio::test]
async fn core_panic_stops_its_actual_attempt_but_sibling_completes_without_false_remote_resolution()
{
    let mut fixture = FrontendFixture::new();
    let mut start = LogicalScript::start(8201, 2, 1);
    start.budgets.remote_cleanup_timeout = Duration::from_secs(1);
    let failed = fixture.install_logical(start, LogicalScript::default());
    let failed_attempt = fixture
        .activate_attempt(
            &failed,
            failed.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let sibling =
        fixture.install_logical(LogicalScript::start(8202, 0, 1), LogicalScript::default());
    let sibling_attempt = fixture
        .activate_attempt(
            &sibling,
            sibling.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    failed.script.core_panic.store(true, Ordering::SeqCst);
    // Setting the test injection flag is not a runtime input or wake. Publish
    // one original passive AP fact so the real LP handle consumes that flag.
    // This fact proves neither local exit nor any positive remote closure.
    let trigger = failed_attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: false,
            closure: None,
            lifecycle_runtime_filters: None,
        });
    let original = until(&mut fixture, &failed, |actual| {
        actual.failed && actual_owner_failed_stop(&failed_attempt)
    })
    .await;
    assert_eq!(
        original
            .error
            .as_ref()
            .unwrap()
            .execution_failure()
            .unwrap()
            .fact()
            .class(),
        FailureClass::Invariant
    );
    assert!(
        failed_attempt.script.published().contains(&trigger),
        "the panic injection is reached through an actually published original AP input"
    );
    assert!(
        !failed.script.core_panic.load(Ordering::SeqCst),
        "the original LP handle must actually consume the one-shot Core panic flag"
    );
    assert_actual_obligations_still_held(&original);
    publish_empty_success(&sibling_attempt);
    sibling.script.run.open();
    until(&mut fixture, &sibling, completed).await;
    assert!(!fixture.observe(&sibling).failed);
    assert_actual_obligations_still_held(&fixture.observe(&failed));
    failed.script.run.open();
    assert_original_deadline_abandons(&mut fixture, &failed).await;
    assert!(actual_owner_failed_stop(&failed_attempt));
    let history = finish_with_history(&mut fixture).await;
    assert_actual_exit(&fixture, &failed, &failed_attempt, &history);
    assert_actual_exit(&fixture, &sibling, &sibling_attempt, &history);
}

#[tokio::test]
async fn mandatory_executor_actual_exit_without_result_is_invariant_and_does_not_poison_sibling() {
    let mut fixture = FrontendFixture::new();
    let script = LogicalScript::default();
    script.omit_run_result.store(true, Ordering::SeqCst);
    let mut start = LogicalScript::start(8203, 2, 1);
    start.budgets.remote_cleanup_timeout = Duration::from_secs(1);
    let failed = fixture.install_logical(start, script);
    let failed_attempt = fixture
        .activate_attempt(
            &failed,
            failed.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let sibling =
        fixture.install_logical(LogicalScript::start(8204, 0, 1), LogicalScript::default());
    let sibling_attempt = fixture
        .activate_attempt(
            &sibling,
            sibling.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    failed.script.run.open();
    let original = until(&mut fixture, &failed, |actual| {
        actual.failed && actual_owner_failed_stop(&failed_attempt)
    })
    .await;
    assert_eq!(
        original
            .error
            .as_ref()
            .unwrap()
            .execution_failure()
            .unwrap()
            .fact()
            .class(),
        FailureClass::Invariant
    );
    assert_actual_obligations_still_held(&original);
    publish_empty_success(&sibling_attempt);
    sibling.script.run.open();
    until(&mut fixture, &sibling, completed).await;
    assert!(!fixture.observe(&sibling).failed);
    assert_original_deadline_abandons(&mut fixture, &failed).await;
    let history = finish_with_history(&mut fixture).await;
    assert_actual_exit(&fixture, &failed, &failed_attempt, &history);
    assert_actual_exit(&fixture, &sibling, &sibling_attempt, &history);
}
