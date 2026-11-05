// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_control_runtime_core::completion::Consumption;

fn consumers_still_pending(f: &Fixture) -> bool {
    named().into_iter().all(|role| {
        [core(), observer()].into_iter().all(|consumer| {
            f.completion_set.report(role).consumption(consumer).unwrap() == Consumption::Pending
        })
    })
}

fn finish_actual_roles(f: Fixture) {
    for role in named() {
        for consumer in [core(), observer()] {
            f.completion_set.report(role).consume(consumer).unwrap();
        }
    }
    // The original settlement child must retire before its finisher parent;
    // this reaping is deliberately later than execution-borrow eligibility.
    for _ in 0..4 {
        f.completion_set.try_retire();
        f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
    }
    assert!(f.completion_set.is_retired());
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}

#[test]
fn settlement_call_and_capture_exit_allow_borrow_close_before_any_report_consumption() {
    let mut f = fixture();
    let (finisher, wall_time, publisher) = f.admitted.take().unwrap().into_parts();
    let (permit, mut work) = finisher.into_parts();
    work.bind();
    work.started();
    let body_entered = Arc::new(Barrier::new(2));
    let body_release = Arc::new(Barrier::new(2));
    let capture_entered = Arc::new(Barrier::new(2));
    let capture_release = Arc::new(Barrier::new(2));
    let capture = Capture {
        entered: capture_entered.clone(),
        release: capture_release.clone(),
    };
    let callback_entered = body_entered.clone();
    let callback_release = body_release.clone();
    permit.register(Box::new(move |_| {
        callback_entered.wait();
        callback_release.wait();
        std::hint::black_box(&capture);
    }));
    // These original holders have no remaining body or captures. The child
    // callback below is a separate actual obligation, not their report leg.
    work.completed();
    drop(wall_time);
    drop(publisher);
    f.completion_set.publish_settlement_barrier(None);
    let roles = f.completion_set.clone();
    let thread = std::thread::spawn(move || roles.run_settlement());
    body_entered.wait();
    let blocked_by_body = !f.completion_set.execution_calls_exited();
    body_release.wait();
    capture_entered.wait();
    let blocked_by_capture = !f.completion_set.execution_calls_exited();
    let mut invocation = Invocation {
        release: capture_release,
        thread: Some(thread),
    };
    // Release and join every controlled thread before evaluating fail oracles.
    invocation.join();
    let may_close_after_actual_exit = f.completion_set.execution_calls_exited();
    let report_legs_pending = consumers_still_pending(&f);
    let child_report_not_retired = f.completion_set.settlement_phase() == SettlementPhase::Exited;
    let finisher_not_retired =
        f.completion_set.phase(TaskLifecycleInputKind::Finisher) == ExitCustodyPhase::Exited;
    let all_original_funding_held = f.ledger.used(dimension()) == 4;
    let healthy = f.health.failure().is_none();
    finish_actual_roles(f);
    assert!(blocked_by_body);
    assert!(blocked_by_capture);
    assert!(may_close_after_actual_exit);
    assert!(report_legs_pending);
    assert!(child_report_not_retired);
    assert!(finisher_not_retired);
    assert!(all_original_funding_held);
    assert!(healthy);
}

#[test]
fn unused_token_needs_the_original_finisher_holder_exit_before_borrow_close() {
    let mut f = fixture();
    let (finisher, wall_time, publisher) = f.admitted.take().unwrap().into_parts();
    let (permit, work) = finisher.into_parts();
    drop(permit);
    drop(wall_time);
    drop(publisher);
    f.completion_set.observe_unused_settlement();
    let token_drop_is_not_exit = f.completion_set.settlement_phase() == SettlementPhase::Unused
        && !f.completion_set.execution_calls_exited();
    // An unbound original holder has no callback/capture; its real Drop is
    // the existing positive unused-exit path, not a fabricated invocation.
    drop(work);
    let holder_exit_does_not_infer_child_exit = !f.completion_set.execution_calls_exited();
    f.completion_set.observe_unused_settlement();
    let original_unused_path_exited = f.completion_set.settlement_phase()
        == SettlementPhase::Exited
        && f.completion_set.execution_calls_exited();
    let report_legs_pending = consumers_still_pending(&f);
    let original_registration_revoked = f.registration.load(Ordering::SeqCst) == 2;
    let all_original_funding_held = f.ledger.used(dimension()) == 4;
    finish_actual_roles(f);
    assert!(token_drop_is_not_exit);
    assert!(holder_exit_does_not_infer_child_exit);
    assert!(original_unused_path_exited);
    assert!(report_legs_pending);
    assert!(original_registration_revoked);
    assert!(all_original_funding_held);
}

#[test]
fn panicked_settlement_keeps_borrow_close_blocked_and_original_unknown_custody() {
    let mut f = fixture();
    let (finisher, wall_time, publisher) = f.admitted.take().unwrap().into_parts();
    let (permit, mut work) = finisher.into_parts();
    work.bind();
    work.started();
    permit.register(Box::new(|_| {
        panic!("injected original settlement callback panic");
    }));
    work.completed();
    drop(wall_time);
    drop(publisher);
    f.completion_set.publish_settlement_barrier(None);
    f.completion_set.run_settlement();
    // Only the independently exited original roles can consume their own
    // reports. The panicked child's report is not filled or acknowledged.
    for role in [
        TaskLifecycleInputKind::Finisher,
        TaskLifecycleInputKind::WallTime,
        TaskLifecycleInputKind::TerminalPublisher,
    ] {
        for consumer in [core(), observer()] {
            f.completion_set.report(role).consume(consumer).unwrap();
        }
    }
    f.completion_set.try_retire();
    f.ledger.reap_scope_ready(f.scope.id(), 4).unwrap();
    let blocked = !f.completion_set.execution_calls_exited();
    let child_unknown = f.completion_set.settlement_phase() == SettlementPhase::Unresolved;
    let child_report_pending = [core(), observer()].into_iter().all(|consumer| {
        f.completion_set
            .report(TaskLifecycleInputKind::Settlement)
            .consumption(consumer)
            .unwrap()
            == Consumption::Pending
    });
    let parent_still_held =
        f.completion_set.phase(TaskLifecycleInputKind::Finisher) == ExitCustodyPhase::Exited;
    let original_unknown_funding_held = f.ledger.used(dimension()) == 2;
    let health_failed = f.health.failure().is_some();
    let not_retired = !f.completion_set.try_retire();
    // The original ContinuationSlot/ExitCustody self-retain the finite unknown
    // inventory and panic payload. No timeout, fake exit or S8 claim releases
    // that child or its real parent; the unrelated two roles retired above.
    assert!(blocked);
    assert!(child_unknown);
    assert!(child_report_pending);
    assert!(parent_still_held);
    assert!(original_unknown_funding_held);
    assert!(health_failed);
    assert!(not_retired);
}
