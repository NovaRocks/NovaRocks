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

//! Deterministic owner-level tests for actual capture exit and admission.
//! These use real ledger obligations; no executor or external fixture is needed.

use super::*;
use crate::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase},
    history::HistoryCapture,
    obligation::{ReservationObligation, WorkChildAuthority, WorkObligation},
    observation::RuntimeObservation,
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    history::HistoryFact,
    identity::{ConsumerId, Generation, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkAdmission, WorkKind},
};
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Waker,
};

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn role_resources() -> ReservationResources {
    ReservationResources::new(
        ResourceVector::default(),
        OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures: 1,
            ..Default::default()
        },
    )
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    history: HistoryCapture,
    custody: ExitCustody,
    obligation: Option<ExitObligation>,
    report: ReportSlot,
    child_authority: WorkChildAuthority,
    work: WorkId,
}
fn reported(
    scope: &ScopeHandle,
    admission: WorkAdmission,
) -> (WorkObligation, ReservationObligation, ReportSlot) {
    let (work, mut capacity) = scope
        .reserve_next(
            generation(),
            role_resources(),
            WorkKind::Business,
            admission,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation(),
        1,
        [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
    )
    .unwrap();
    (work, capacity, report)
}
fn fixture() -> Fixture {
    let history = HistoryCapture::new(256);
    let ledger = RuntimeLedger::with_owner_usage_dimensions_and_observation(
        ResourceVector::default(),
        [],
        Some(history.clone()),
        RuntimeObservation::new(None),
    )
    .unwrap();
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (work, capacity, report) = reported(&scope, WorkAdmission::External);
    work.set_diagnostic_label("test-work").unwrap();
    let child_authority = work.child_authority().unwrap();
    let id = work.id();
    let (custody, obligation) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
    Fixture {
        ledger,
        scope,
        history,
        custody,
        obligation: Some(obligation),
        report,
        child_authority,
        work: id,
    }
}
impl Fixture {
    fn take(&mut self) -> ExitObligation {
        self.obligation.take().unwrap()
    }
    fn retire(self, started: bool) {
        if started {
            self.report.consume(ConsumerId::new(1).unwrap()).unwrap();
        }
        assert_eq!(self.custody.phase(), ExitCustodyPhase::Exited);
        assert!(self.custody.try_retire().unwrap());
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}
struct Capture {
    custody: ExitCustody,
    history: HistoryCapture,
    destroyed: std::sync::Arc<AtomicBool>,
    panic: bool,
}
impl Drop for Capture {
    fn drop(&mut self) {
        assert!(!self.custody.captures_exited());
        assert!(
            !self.history.drain(256).entries.iter().any(|entry| matches!(
                entry.fact,
                HistoryFact::BodyExited { .. } | HistoryFact::CapturesExited { .. }
            ))
        );
        self.destroyed.store(true, Ordering::SeqCst);
        assert!(!self.panic, "capture destructor panic");
    }
}
fn capture(f: &Fixture, panic: bool) -> (Capture, std::sync::Arc<AtomicBool>) {
    let destroyed = std::sync::Arc::new(AtomicBool::new(false));
    (
        Capture {
            custody: f.custody.clone(),
            history: f.history.clone(),
            destroyed: destroyed.clone(),
            panic,
        },
        destroyed,
    )
}
struct WorkFuture {
    _capture: Capture,
    pending: bool,
    panic: bool,
}
impl Future for WorkFuture {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        assert!(!self.panic, "work body panic");
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(7)
        }
    }
}
#[test]
fn future_never_polled_and_cancelled_future_destroy_captures_before_positive_exit() {
    for started in [false, true] {
        let mut f = fixture();
        let (capture, destroyed) = capture(&f, false);
        let mut future = run_future(
            f.take(),
            WorkFuture {
                _capture: capture,
                pending: true,
                panic: false,
            },
            PanicSink::new(),
        );
        if started {
            assert!(poll(&mut future).is_pending());
        }
        drop(future);
        assert!(destroyed.load(Ordering::SeqCst));
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
        f.retire(started);
    }
}
#[test]
fn ready_future_exit_history_follows_actual_capture_destruction() {
    let mut f = fixture();
    let (capture, destroyed) = capture(&f, false);
    let mut future = run_future(
        f.take(),
        WorkFuture {
            _capture: capture,
            pending: false,
            panic: false,
        },
        PanicSink::new(),
    );
    assert_eq!(poll(&mut future), Poll::Ready(Ok(7)));
    assert!(destroyed.load(Ordering::SeqCst));
    let facts = f.history.drain(256);
    assert!(facts.complete);
    assert!(
        facts
            .entries
            .iter()
            .any(|entry| matches!(entry.fact, HistoryFact::BodyExited { .. }))
    );
    assert!(
        facts
            .entries
            .iter()
            .any(|entry| matches!(entry.fact, HistoryFact::CapturesExited { .. }))
    );
    drop(future);
    f.retire(true);
}
#[test]
fn stopped_start_gate_destroys_future_without_running_and_revokes_unused_work() {
    let mut f = fixture();
    f.scope.request_stop().unwrap();
    let (capture, destroyed) = capture(&f, false);
    let mut future = run_future(
        f.take(),
        WorkFuture {
            _capture: capture,
            pending: false,
            panic: true,
        },
        PanicSink::new(),
    );
    assert!(matches!(
        poll(&mut future),
        Poll::Ready(Err(ExitWorkError::NotStarted(_)))
    ));
    assert!(destroyed.load(Ordering::SeqCst));
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
    drop(future);
    f.retire(false);
}
#[test]
fn future_panic_keeps_original_payload_in_custody_and_continues_unwinding() {
    let mut f = fixture();
    let (capture, destroyed) = capture(&f, false);
    let sink = PanicSink::new();
    let mut future = run_future(
        f.take(),
        WorkFuture {
            _capture: capture,
            pending: false,
            panic: true,
        },
        sink.clone(),
    );
    let panic = catch_unwind(AssertUnwindSafe(|| poll(&mut future))).unwrap_err();
    assert!(panic.is::<ForwardedWorkPanic>());
    assert!(
        sink.take()
            .unwrap()
            .downcast_ref::<&str>()
            .is_some_and(|value| *value == "work body panic")
    );
    assert!(destroyed.load(Ordering::SeqCst));
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
    drop(future);
    f.retire(true);
}
#[test]
fn explicit_future_capture_destructor_panic_never_becomes_positive_exit() {
    // Explicit destruction happens after Ready, on cancellation of a pending
    // future, before the first poll, and after a refused start gate.
    for (started, ready, stopped) in [
        (true, true, false),
        (true, false, false),
        (false, false, false),
        (false, false, true),
    ] {
        let mut f = fixture();
        if stopped {
            f.scope.request_stop().unwrap();
        }
        let (capture, destroyed) = capture(&f, true);
        let sink = PanicSink::new();
        let mut future = run_future(
            f.take(),
            WorkFuture {
                _capture: capture,
                pending: !ready,
                panic: false,
            },
            sink.clone(),
        );
        let panic = if ready || stopped {
            let panic = catch_unwind(AssertUnwindSafe(|| poll(&mut future))).unwrap_err();
            drop(future);
            panic
        } else {
            if started {
                assert!(poll(&mut future).is_pending());
            }
            catch_unwind(AssertUnwindSafe(|| drop(future))).unwrap_err()
        };
        assert!(panic.is::<ForwardedWorkPanic>());
        assert!(sink.has_panic());
        assert_eq!(
            sink.take().unwrap().downcast_ref::<&str>(),
            Some(&"capture destructor panic")
        );
        assert!(!sink.has_panic());
        assert!(sink.take_cleanup().is_none());
        assert!(destroyed.load(Ordering::SeqCst));
        assert_eq!(f.custody.phase(), ExitCustodyPhase::Unresolved);
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
        assert!(!f.custody.try_retire().unwrap());
    }
}
#[test]
fn blocking_job_never_started_and_completed_job_destroy_captures_before_exit() {
    for started in [false, true] {
        let mut f = fixture();
        let (capture, destroyed) = capture(&f, false);
        let job = run_blocking(
            f.take(),
            move || {
                drop(capture);
                11
            },
            PanicSink::new(),
        );
        if started {
            assert_eq!(job.run(), Ok(11));
        } else {
            drop(job);
        }
        assert!(destroyed.load(Ordering::SeqCst));
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
        f.retire(started);
    }
}
#[test]
fn explicit_blocking_capture_destructor_panic_remains_unresolved() {
    let mut f = fixture();
    let (capture, destroyed) = capture(&f, true);
    let sink = PanicSink::new();
    let job = run_blocking(f.take(), move || drop(capture), sink.clone());
    let panic = catch_unwind(AssertUnwindSafe(|| drop(job))).unwrap_err();
    assert!(panic.is::<ForwardedWorkPanic>());
    assert!(sink.has_panic());
    assert_eq!(
        sink.take().unwrap().downcast_ref::<&str>(),
        Some(&"capture destructor panic")
    );
    assert!(!sink.has_panic());
    assert!(destroyed.load(Ordering::SeqCst));
    assert_eq!(f.custody.phase(), ExitCustodyPhase::Unresolved);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
    assert!(!f.custody.try_retire().unwrap());
}

struct PayloadDropProbe {
    stage: &'static str,
    dropped: std::sync::Arc<AtomicUsize>,
}
impl Drop for PayloadDropProbe {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct DoublePanicFuture {
    body_dropped: std::sync::Arc<AtomicUsize>,
    cleanup_dropped: std::sync::Arc<AtomicUsize>,
}
impl Future for DoublePanicFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        std::panic::panic_any(PayloadDropProbe {
            stage: "body",
            dropped: self.body_dropped.clone(),
        });
    }
}
impl Drop for DoublePanicFuture {
    fn drop(&mut self) {
        std::panic::panic_any(PayloadDropProbe {
            stage: "cleanup",
            dropped: self.cleanup_dropped.clone(),
        });
    }
}
#[test]
fn poll_then_explicit_drop_panics_keep_both_original_boxes_and_unresolved_custody() {
    for take_cleanup_first in [false, true] {
        let mut f = fixture();
        let body_dropped = std::sync::Arc::new(AtomicUsize::new(0));
        let cleanup_dropped = std::sync::Arc::new(AtomicUsize::new(0));
        let sink = PanicSink::new();
        let mut future = run_future(
            f.take(),
            DoublePanicFuture {
                body_dropped: body_dropped.clone(),
                cleanup_dropped: cleanup_dropped.clone(),
            },
            sink.clone(),
        );
        let panic = catch_unwind(AssertUnwindSafe(|| poll(&mut future))).unwrap_err();
        assert!(panic.is::<ForwardedWorkPanic>());
        drop(future);
        assert_eq!(body_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(cleanup_dropped.load(Ordering::SeqCst), 0);
        assert!(sink.has_panic());
        assert_eq!(f.custody.phase(), ExitCustodyPhase::Unresolved);
        assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
        assert!(!f.custody.try_retire().unwrap());
        assert!(!f.history.drain(256).entries.iter().any(|entry| matches!(
            entry.fact,
            HistoryFact::BodyExited { .. } | HistoryFact::CapturesExited { .. }
        )));
        let first = if take_cleanup_first {
            sink.take_cleanup().unwrap()
        } else {
            sink.take().unwrap()
        };
        assert!(sink.has_panic());
        let second = sink.take().unwrap();
        assert_eq!(
            first.downcast_ref::<PayloadDropProbe>().unwrap().stage,
            if take_cleanup_first {
                "cleanup"
            } else {
                "body"
            }
        );
        assert_eq!(
            second.downcast_ref::<PayloadDropProbe>().unwrap().stage,
            if take_cleanup_first {
                "body"
            } else {
                "cleanup"
            }
        );
        assert!(!sink.has_panic());
        assert!(sink.take().is_none());
        assert!(sink.take_cleanup().is_none());
        assert_eq!(body_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(cleanup_dropped.load(Ordering::SeqCst), 0);
        drop((first, second));
        assert_eq!(body_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(cleanup_dropped.load(Ordering::SeqCst), 1);
    }
}
#[test]
fn abandoned_panic_sink_never_runs_foreign_payload_destructors() {
    let dropped = std::sync::Arc::new(AtomicUsize::new(0));
    let sink = PanicSink::new();
    sink.store_panics(
        Box::new(PayloadDropProbe {
            stage: "body",
            dropped: dropped.clone(),
        }),
        Some(Box::new(PayloadDropProbe {
            stage: "cleanup",
            dropped: dropped.clone(),
        })),
    );
    // An owner that abandons custody deliberately retains both foreign boxes;
    // their destructors must not run as a side effect in a control poller.
    drop(sink);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
}
#[test]
fn live_proof_rejects_exited_and_unresolved_original_parent() {
    for unresolved in [false, true] {
        let mut f = fixture();
        assert!(f.child_authority.verify_live().is_ok());
        if unresolved {
            drop(f.take());
        } else {
            f.take().revoke_unused_and_exit().unwrap();
        }
        assert!(matches!(
            f.child_authority.verify_live(),
            Err(RuntimeError::UnresolvedWork | RuntimeError::Scope(ScopeError::InvalidTransition))
        ));
        if !unresolved {
            f.retire(false);
        }
    }
}
#[test]
fn dropped_unexited_work_emits_one_unresolved_fact_and_diagnostic_label() {
    let mut f = fixture();
    drop(f.take());
    let inventory = f.scope.work_inventory().unwrap();
    assert_eq!(inventory.live_by_label["test-work"], 1);
    assert_eq!(inventory.unresolved_by_label["test-work"], 1);
    assert_eq!(inventory.entries[0].work, f.work);
    let sample = f
        .ledger
        .try_observation_inventory(32)
        .unwrap()
        .unwrap()
        .completed
        .unwrap();
    assert_eq!(sample.live_by_label["test-work"], 1);
    assert_eq!(sample.unresolved_by_label["test-work"], 1);
    assert_eq!(
        f.history
            .drain(256)
            .entries
            .iter()
            .filter(|entry| matches!(entry.fact, HistoryFact::WorkUnresolved { .. }))
            .count(),
        1
    );
}
#[test]
fn child_custody_must_retire_before_parent_then_labels_and_headers_are_reaped() {
    let mut f = fixture();
    let (work, capacity, report) = reported(&f.scope, WorkAdmission::Derived(f.work));
    work.set_diagnostic_label("child").unwrap();
    assert!(work.set_diagnostic_label("replacement").is_err());
    let (child_custody, child) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report).unwrap());
    f.take().revoke_unused_and_exit().unwrap();
    assert!(!f.custody.try_retire().unwrap());
    child.revoke_unused_and_exit().unwrap();
    assert!(!f.custody.try_retire().unwrap());
    assert!(child_custody.try_retire().unwrap());
    f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap();
    assert!(
        !f.scope
            .work_inventory()
            .unwrap()
            .live_by_label
            .contains_key("child")
    );
    f.retire(false);
}
#[test]
fn scope_removal_records_actual_root_and_child_removals_once() {
    let mut f = fixture();
    let (child_scope, registration) = f
        .obligation
        .as_mut()
        .unwrap()
        .open_child_scope_with_root(
            generation(),
            WorkKind::Cleanup,
            ReservationResources::work_only(),
        )
        .unwrap();
    let child_id = child_scope.id();
    let (mut work, mut capacity) = registration.bind(WorkKind::Cleanup).unwrap();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    child_scope.reap_ready(8).unwrap();
    capacity.finish().unwrap();
    child_scope.close_external().unwrap();
    f.ledger.remove_scope(child_id).unwrap();
    assert!(f.ledger.remove_scope(child_id).is_err());
    let root_id = f.scope.id();
    let history = f.history.clone();
    f.take().revoke_unused_and_exit().unwrap();
    f.retire(false);
    let removals: Vec<_> = history
        .drain(256)
        .entries
        .into_iter()
        .filter_map(|entry| {
            if let HistoryFact::ScopeRemoved { scope } = entry.fact {
                Some(scope)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(removals, [child_id, root_id]);
}

#[test]
fn blocking_body_panic_is_kept_and_start_refusal_never_invokes_the_job() {
    for stopped in [false, true] {
        let mut f = fixture();
        let (capture, destroyed) = capture(&f, false);
        if stopped {
            f.scope.request_stop().unwrap();
        }
        let sink = PanicSink::new();
        let job = run_blocking(
            f.take(),
            move || {
                drop(capture);
                panic!("blocking body panic")
            },
            sink.clone(),
        );
        if stopped {
            assert!(matches!(job.run(), Err(ExitWorkError::NotStarted(_))));
            assert!(!sink.has_panic());
        } else {
            let panic = catch_unwind(AssertUnwindSafe(|| job.run())).unwrap_err();
            assert!(panic.is::<ForwardedWorkPanic>());
            assert!(sink.has_panic());
            assert_eq!(
                sink.take().unwrap().downcast_ref::<&str>(),
                Some(&"blocking body panic")
            );
        }
        assert!(destroyed.load(Ordering::SeqCst));
        f.retire(!stopped);
    }
}
#[test]
fn repeated_unresolved_mark_then_unexited_drop_records_one_original_fact() {
    let history = HistoryCapture::new(32);
    let ledger = RuntimeLedger::with_history(ResourceVector::default(), history.clone());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (work, capacity, report) = reported(&scope, WorkAdmission::External);
    work.mark_unresolved();
    work.mark_unresolved();
    drop(work);
    assert_eq!(scope.snapshot().unwrap().unresolved, 1);
    assert_eq!(
        history
            .drain(32)
            .entries
            .iter()
            .filter(|entry| matches!(entry.fact, HistoryFact::WorkUnresolved { .. }))
            .count(),
        1
    );
    // This intentional defect cannot retire: capacity/report stay with its
    // original unresolved owner until this isolated test process exits.
    drop(report);
    drop(capacity);
}
