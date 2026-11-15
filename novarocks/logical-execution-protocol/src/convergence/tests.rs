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

//! Functional cleanup/exit sequences. No runtime, clocks or host substitutes
//! are needed to falsify these pure retirement predicates.
use super::*;
use novarocks_coordination_contract::QueryExecutionErrorKind;
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use std::{num::NonZeroU64, time::Duration};

struct Budget(usize);
impl StepBudget for Budget {
    fn charge(&mut self, n: usize) -> bool {
        if self.0 < n {
            false
        } else {
            self.0 -= n;
            true
        }
    }
}
fn time(ms: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_millis(ms))
}
fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        1,
        QueryExecutionId::new(QueryId::new(17, 23), AttemptId::new(1).unwrap()).unwrap(),
        1,
    )
}
fn action(n: u64) -> LogicalActionId {
    LogicalActionId::new(NonZeroU64::new(n).unwrap())
}
fn positive() -> RemoteProgress {
    RemoteProgress {
        initialized: true,
        responsibility_settled: true,
        positive_resource_closure: true,
        tracking_complete: true,
    }
}
fn unknown() -> RemoteProgress {
    RemoteProgress {
        positive_resource_closure: false,
        ..positive()
    }
}
fn pending() -> RemoteProgress {
    RemoteProgress {
        tracking_complete: false,
        positive_resource_closure: false,
        ..positive()
    }
}
fn record(relay: bool, resources: bool, kinds: ObligationPresence) -> ConvergenceRecord {
    let mut r = ConvergenceRecord::new(activation(), relay, resources, kinds);
    r.begin(AttemptConvergenceMode::Normal, time(100));
    if kinds.running_work {
        r.observe_obligation_registered(ObligationKind::RunningWork)
            .unwrap();
    }
    if kinds.retired_attempt {
        r.observe_obligation_registered(ObligationKind::RetiredAttempt)
            .unwrap();
    }
    r
}
fn poll(r: &mut ConvergenceRecord, now: u64, remote: RemoteProgress, id: u64) -> CleanupPoll {
    r.poll_cleanup(time(now), remote, action(id), &mut Budget(1))
        .unwrap()
}
fn issued(r: &mut ConvergenceRecord, now: u64, remote: RemoteProgress, id: u64) -> CleanupIntent {
    let CleanupPoll::Issued(d) = poll(r, now, remote, id) else {
        panic!("fixture expected one cleanup action")
    };
    assert_eq!(d.action, action(id));
    d.intent
}
fn gov(r: &mut ConvergenceRecord, id: u64, kind: ObligationKind) {
    r.observe_governance_result(action(id), kind, Ok(()))
        .unwrap();
    r.observe_action_exit(action(id)).unwrap();
}

#[test]
fn positive_resource_closure_resolves_before_finish_and_waits_for_real_exit() {
    let mut r = record(
        false,
        true,
        ObligationPresence {
            running_work: true,
            retired_attempt: false,
        },
    );
    assert_eq!(
        issued(&mut r, 0, positive(), 1),
        CleanupIntent::ResolveObligation(ObligationKind::RunningWork)
    );
    r.observe_governance_result(action(1), ObligationKind::RunningWork, Ok(()))
        .unwrap();
    assert_eq!(poll(&mut r, 0, positive(), 2), CleanupPoll::Idle);
    assert!(!r.cleanup_complete(positive()));
    r.observe_action_exit(action(1)).unwrap();
    assert_eq!(
        issued(&mut r, 0, positive(), 2),
        CleanupIntent::FinishResources
    );
    r.observe_resource_result(action(2), Ok(())).unwrap();
    r.observe_attempt_local_exit();
    assert!(!r.cleanup_complete(positive()));
    r.observe_action_exit(action(2)).unwrap();
    assert!(r.cleanup_complete(positive()));
}

#[test]
fn tracking_end_waits_for_original_deadline_and_abandons_instead_of_finishing() {
    let mut r = record(
        false,
        true,
        ObligationPresence {
            running_work: true,
            retired_attempt: false,
        },
    );
    assert_eq!(poll(&mut r, 99, unknown(), 1), CleanupPoll::Idle);
    assert_eq!(r.selected_path(), None);
    assert_eq!(
        issued(&mut r, 100, unknown(), 1),
        CleanupIntent::EndRemoteTracking(ObligationKind::RunningWork)
    );
    gov(&mut r, 1, ObligationKind::RunningWork);
    assert_eq!(
        issued(&mut r, 100, unknown(), 2),
        CleanupIntent::AbandonResources
    );
    r.observe_resource_result(action(2), Ok(())).unwrap();
    r.observe_action_exit(action(2)).unwrap();
    r.observe_attempt_local_exit();
    assert!(r.cleanup_complete(unknown()));
    assert_eq!(r.selected_path(), Some(CleanupPath::EndTrackingThenAbandon));
}

#[test]
fn worker_closure_alone_never_authorizes_resolution_or_retirement() {
    let mut r = record(
        false,
        false,
        ObligationPresence {
            running_work: true,
            retired_attempt: false,
        },
    );
    r.observe_attempt_local_exit();
    assert_eq!(poll(&mut r, 1000, pending(), 1), CleanupPoll::Idle);
    assert!(!r.cleanup_complete(pending()));
    assert_eq!(r.selected_path(), None);
}

#[test]
fn relay_stopped_unsettled_and_actual_local_exit_are_independent() {
    let mut r = record(true, false, ObligationPresence::default());
    assert_eq!(poll(&mut r, 0, positive(), 1), CleanupPoll::Idle);
    r.observe_relay_stopped(false);
    assert!(!r.cleanup_complete(positive()));
    r.observe_attempt_local_exit();
    assert!(!r.cleanup_complete(positive()));
    r.observe_relay_stopped(true);
    assert!(r.cleanup_complete(positive()));
    r.observe_relay_stopped(false);
    assert!(r.cleanup_complete(positive()));
}

#[test]
fn original_two_obligation_kinds_settle_independently_before_resources() {
    let mut r = record(
        false,
        true,
        ObligationPresence {
            running_work: true,
            retired_attempt: true,
        },
    );
    assert_eq!(
        issued(&mut r, 0, positive(), 1),
        CleanupIntent::ResolveObligation(ObligationKind::RunningWork)
    );
    assert_eq!(
        issued(&mut r, 0, positive(), 2),
        CleanupIntent::ResolveObligation(ObligationKind::RetiredAttempt)
    );
    gov(&mut r, 2, ObligationKind::RetiredAttempt);
    assert_eq!(poll(&mut r, 0, positive(), 3), CleanupPoll::Idle);
    assert_eq!(
        r.observe_governance_result(action(1), ObligationKind::RetiredAttempt, Ok(())),
        Err(ConvergenceError::UnknownAction)
    );
    gov(&mut r, 1, ObligationKind::RunningWork);
    assert_eq!(
        issued(&mut r, 0, positive(), 3),
        CleanupIntent::FinishResources
    );
}

#[test]
fn awaiting_registration_is_not_an_absent_original_obligation() {
    let mut r = ConvergenceRecord::new(
        activation(),
        false,
        true,
        ObligationPresence {
            running_work: true,
            retired_attempt: false,
        },
    );
    r.begin(AttemptConvergenceMode::Cleanup, time(100));
    assert_eq!(poll(&mut r, 0, positive(), 1), CleanupPoll::Idle);
    assert!(!r.cleanup_complete(positive()));
    assert_eq!(
        r.observe_obligation_registered(ObligationKind::RetiredAttempt),
        Err(ConvergenceError::UnexpectedObligation)
    );
    r.observe_obligation_registered(ObligationKind::RunningWork)
        .unwrap();
    assert_eq!(
        issued(&mut r, 0, positive(), 1),
        CleanupIntent::ResolveObligation(ObligationKind::RunningWork)
    );
}

#[test]
fn settlement_failure_preserves_original_typed_error_and_prevents_finish_or_retirement() {
    let mut r = record(
        false,
        true,
        ObligationPresence {
            running_work: true,
            retired_attempt: false,
        },
    );
    issued(&mut r, 0, positive(), 1);
    let original = QueryExecutionError::new(
        QueryExecutionErrorKind::Failed,
        "original responsibility refusal",
    );
    assert_eq!(
        r.observe_governance_result(
            action(1),
            ObligationKind::RunningWork,
            Err(original.clone())
        ),
        Err(ConvergenceError::ResponsibilitySettlementFailed)
    );
    r.observe_action_exit(action(1)).unwrap();
    r.observe_attempt_local_exit();
    assert_eq!(r.failure(), Some(&original));
    assert_eq!(
        r.poll_cleanup(time(1000), positive(), action(2), &mut Budget(1)),
        Err(ConvergenceError::ResponsibilitySettlementFailed)
    );
    assert!(!r.cleanup_complete(positive()));
}

#[test]
fn actual_exit_before_result_does_not_fabricate_success_or_replay_cleanup() {
    let mut r = record(false, true, ObligationPresence::default());
    assert_eq!(
        issued(&mut r, 0, positive(), 1),
        CleanupIntent::FinishResources
    );
    r.observe_action_exit(action(1)).unwrap();
    r.observe_attempt_local_exit();
    assert!(!r.cleanup_complete(positive()));
    assert_eq!(poll(&mut r, 0, positive(), 2), CleanupPoll::Idle);
    r.observe_resource_result(action(1), Ok(())).unwrap();
    assert!(r.cleanup_complete(positive()));
    r.observe_resource_result(action(1), Ok(())).unwrap();
    assert_eq!(poll(&mut r, 0, positive(), 3), CleanupPoll::Idle);
    assert_eq!(
        r.observe_resource_result(
            action(1),
            Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "contrary result"
            ))
        ),
        Err(ConvergenceError::ConflictingResult)
    );
    assert!(r.cleanup_complete(positive()));
}

#[test]
fn late_positive_remote_facts_do_not_rewrite_an_already_selected_unknown_path() {
    let mut r = record(
        false,
        true,
        ObligationPresence {
            running_work: true,
            retired_attempt: false,
        },
    );
    issued(&mut r, 100, unknown(), 1);
    gov(&mut r, 1, ObligationKind::RunningWork);
    assert_eq!(
        issued(&mut r, 101, positive(), 2),
        CleanupIntent::AbandonResources
    );
    assert_eq!(r.selected_path(), Some(CleanupPath::EndTrackingThenAbandon));
}

#[test]
fn deadline_only_shortens_and_cleanup_mode_never_reopens_normal() {
    let mut r = record(false, false, ObligationPresence::default());
    r.begin(AttemptConvergenceMode::Cleanup, time(50));
    r.begin(AttemptConvergenceMode::Normal, time(500));
    assert_eq!(r.remote_deadline(), Some(time(50)));
    assert_eq!(r.mode, Some(AttemptConvergenceMode::Cleanup));
    assert_eq!(poll(&mut r, 49, unknown(), 1), CleanupPoll::Idle);
    assert_eq!(poll(&mut r, 50, unknown(), 1), CleanupPoll::Idle);
    assert_eq!(r.selected_path(), Some(CleanupPath::EndTrackingThenAbandon));
}

#[test]
fn exhausted_step_budget_retains_the_real_cleanup_continuation() {
    let mut r = record(false, true, ObligationPresence::default());
    assert_eq!(
        r.poll_cleanup(time(0), positive(), action(1), &mut Budget(0))
            .unwrap(),
        CleanupPoll::Yielded
    );
    assert!(r.resources.is_none());
    assert_eq!(
        issued(&mut r, 0, positive(), 1),
        CleanupIntent::FinishResources
    );
    assert_eq!(
        r.observe_action_exit(action(99)),
        Err(ConvergenceError::UnknownAction)
    );
}

#[test]
fn no_declared_relay_does_not_excuse_original_attempt_host_exit() {
    let mut r = record(false, false, ObligationPresence::default());
    assert_eq!(poll(&mut r, 0, positive(), 1), CleanupPoll::Idle);
    assert!(!r.cleanup_complete(positive()));
    r.observe_attempt_local_exit();
    assert!(r.cleanup_complete(positive()));
}
