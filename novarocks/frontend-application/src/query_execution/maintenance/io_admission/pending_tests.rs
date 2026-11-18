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

//! Original request receiving while the same Source progress Body is borrowed.
use super::fixtures::{drive_original, original_governor};
use super::*;
use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
use novarocks_workload_control::{CancellationReason, RootWork, WorkloadControl};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

#[derive(Default)]
struct OriginalWake(AtomicUsize);
impl Wake for OriginalWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn source(root: &WorkOwner) -> Arc<MetadataIoAdmission> {
    let bytes = MetadataIoAdmission::body_metadata_bytes().unwrap();
    let profile = ConnectorMetadataMaintenanceIoProfile::try_new(
        std::num::NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(bytes).unwrap(),
        NonZeroU64::new(128).unwrap(),
        vec![SharedDependency::LocalHost],
    )
    .unwrap();
    MetadataIoAdmission::new(root.scope(), profile, bytes).unwrap()
}
fn route() -> SharedDependency {
    SharedDependency::Network(
        novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
            &url::Url::parse("https://original-pending-source.invalid").unwrap(),
        )
        .unwrap(),
    )
}
fn context(source: &Arc<MetadataIoAdmission>) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(10),
        ConnectorStopOwner::new().view(),
        1024,
        4096,
    )
    .unwrap()
    .with_io_admission(source.clone())
}
fn ready(
    source: &MetadataIoAdmission,
    root: &WorkOwner,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    cx: &mut Context<'_>,
) -> bool {
    for _ in 0..256 {
        if source.poll_progress(root, cx).unwrap() {
            return true;
        }
        drive_original(driver);
    }
    false
}
fn finish(
    source: Arc<MetadataIoAdmission>,
    root: RootWork,
    control: WorkloadControl,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    cx: &mut Context<'_>,
) -> bool {
    let mut exited = false;
    for _ in 0..256 {
        if let Poll::Ready(Ok(_)) = source.poll_actual_quiescent_continuing(cx) {
            exited = true;
            break;
        }
        drive_original(driver);
    }
    let healthy = source.health_failure().is_none();
    drop(source);
    root.business.release();
    root.owner.complete_after_terminal_cancel_settled();
    control.close_admission();
    drive_original(driver);
    let drained = control.shutdown().is_ok();
    exited && healthy && drained
}
fn released(error: &ConnectorError) -> bool {
    use std::error::Error;
    error
        .source()
        .and_then(|source| source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>())
        .and_then(|cause| cause.downcast_ref::<WorkError>())
        .is_some_and(|error| *error == WorkError::Released)
}

#[test]
fn receiving_wakes_the_same_source_and_dropped_caller_never_dispatches() {
    let (control, mut driver, root) = original_governor();
    let source = source(&root.owner);
    let wake = Arc::new(OriginalWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut cx = Context::from_waker(&waker);
    let was_ready = ready(&source, &root.owner, &mut driver, &mut cx);
    let context = context(&source);
    let dependency = route();
    let borrowed = loan(&source.original).unwrap();
    source.wake.register(&waker);
    let before = wake.0.load(Ordering::SeqCst);
    let (mut request, first) = std::thread::scope(|threads| {
        let context = context.clone();
        let dependency = dependency.clone();
        threads
            .spawn(move || {
                let mut request =
                    Box::pin(async move { context.admit_selected_io(dependency).await });
                let first = request
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                (request, first)
            })
            .join()
            .unwrap()
    });
    let received_wake = wake.0.load(Ordering::SeqCst) > before;
    let first_pending = first.is_pending();
    let (identity, ungranted) = {
        let incoming = source.requests.lock().unwrap();
        incoming.pending.first().map_or((0, false), |cell| {
            let state = cell.original.lock().unwrap();
            let state = state.as_ref().unwrap();
            (
                Arc::as_ptr(cell) as usize,
                state.grant.is_none()
                    && state.admission.is_none()
                    && state.source.is_none()
                    && state.full.is_none()
                    && !state.owned_admitted,
            )
        })
    };
    let mut polls_stayed_pending = true;
    let before_checks = wake.0.load(Ordering::SeqCst);
    if first_pending {
        for _ in 0..4 {
            source.wake.register(&waker);
            polls_stayed_pending &= request
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending();
        }
    }
    let no_check_wake = wake.0.load(Ordering::SeqCst) == before_checks;
    // Drop only the constructor borrow. The SAME Source still owns its record.
    drop(request);
    let retained = source.requests.lock().unwrap().pending.len() == 1;
    drop(borrowed);
    let phase_exited = matches!(
        source.poll_quiescent(&root.owner, &mut cx),
        Poll::Ready(Ok(()))
    );
    let (same_original, no_dispatch) = {
        let original = source.original.lock().unwrap();
        let body = original.as_ref().unwrap();
        (
            body.cells
                .iter()
                .any(|cell| Arc::as_ptr(cell) as usize == identity),
            !body.bound.contains(&dependency)
                && body.cells.iter().all(|cell| {
                    let cell = cell.original.lock().unwrap();
                    let cell = cell.as_ref().unwrap();
                    cell.grant.is_none() && cell.admission.is_none()
                }),
        )
    };
    drop((first, context));
    let cleaned = finish(source, root, control, &mut driver, &mut cx);
    assert!(cleaned && was_ready && phase_exited);
    assert!(
        received_wake,
        "actual registration wakes the original Source"
    );
    assert!(first_pending && ungranted && retained && same_original && no_dispatch);
    assert!(
        polls_stayed_pending && no_check_wake,
        "unchanged admission checks create no wake debt"
    );
}

#[test]
fn never_started_withdrawal_rejects_but_retains_the_original_pending_caller() {
    let (control, mut driver, root) = original_governor();
    let source = source(&root.owner);
    let mut cx = Context::from_waker(Waker::noop());
    let borrowed = loan(&source.original).unwrap();
    let request = source.request(route()).unwrap();
    let mut permit = None;
    let first_pending = request.poll_admitted_in(&mut permit, &mut cx).is_pending();
    drop(borrowed);
    let withdrawal_waited = source.withdraw_unstarted_in().unwrap().is_none();
    let late = source.request(SharedDependency::LocalHost).err();
    let original = match request.poll_admitted_in(&mut permit, &mut cx) {
        Poll::Ready(Err(error)) => Some(error),
        _ => None,
    };
    let alias_kept_exit_pending = source
        .poll_actual_quiescent_continuing(&mut cx)
        .is_pending();
    let no_bootstrap = {
        let body = source.original.lock().unwrap();
        let body = body.as_ref().unwrap();
        !body.started && body.payload.is_none() && body.initial.is_none()
    };
    drop((request, permit));
    let withdrawal_completed = source.withdraw_unstarted_in().unwrap().is_some();
    let cleaned = finish(source, root, control, &mut driver, &mut cx);
    assert!(cleaned && first_pending && withdrawal_waited && alias_kept_exit_pending);
    assert!(original.as_ref().is_some_and(released) && late.as_ref().is_some_and(released));
    assert!(
        withdrawal_completed && no_bootstrap,
        "withdrawal cannot start canceled bootstrap work"
    );
}

#[test]
fn pending_request_keeps_exact_root_cancellation_until_actual_alias_exit() {
    for reason in [
        CancellationReason::Requested,
        CancellationReason::DeadlineExceeded,
    ] {
        let (control, mut driver, root) = original_governor();
        let source = source(&root.owner);
        let mut cx = Context::from_waker(Waker::noop());
        let was_ready = ready(&source, &root.owner, &mut driver, &mut cx);
        let borrowed = loan(&source.original).unwrap();
        let request = source.request(SharedDependency::LocalHost).unwrap();
        let mut permit = None;
        let first_pending = request.poll_admitted_in(&mut permit, &mut cx).is_pending();
        drop(borrowed);
        root.owner.cancel(reason.clone());
        let mut observed = None;
        for _ in 0..256 {
            source.poll_progress(&root.owner, &mut cx).unwrap();
            drive_original(&mut driver);
            if let Poll::Ready(Err(error)) = request.poll_admitted_in(&mut permit, &mut cx) {
                observed = Some(error);
                break;
            }
        }
        let same_failure = source.failure() == Some(WorkError::Cancelled(reason.clone()));
        let held_alias = source
            .poll_actual_quiescent_continuing(&mut cx)
            .is_pending();
        let no_permit = permit.is_none();
        drop((request, permit));
        let observed_exit = match source.poll_actual_quiescent_continuing(&mut cx) {
            Poll::Ready(Ok(observed)) => observed.into_ordinary_failure(),
            _ => None,
        };
        let cleaned = finish(source, root, control, &mut driver, &mut cx);
        assert!(cleaned && was_ready && first_pending && held_alias && no_permit);
        assert!(observed.is_some_and(|error| error.kind() == ConnectorErrorKind::Cancelled));
        assert!(same_failure && observed_exit == Some(WorkError::Cancelled(reason)));
    }
}

#[test]
fn phase_quiescence_keeps_the_same_source_open_for_the_next_original_request() {
    let (control, mut driver, root) = original_governor();
    let source = source(&root.owner);
    let mut cx = Context::from_waker(Waker::noop());
    let was_ready = ready(&source, &root.owner, &mut driver, &mut cx);
    let phase_exited = matches!(
        source.poll_quiescent(&root.owner, &mut cx),
        Poll::Ready(Ok(()))
    );
    let borrowed = loan(&source.original).unwrap();
    let request = source.request(route()).unwrap();
    let mut permit = None;
    let first_pending = request.poll_admitted_in(&mut permit, &mut cx).is_pending();
    drop(borrowed);
    for _ in 0..256 {
        drive_original(&mut driver);
        source.poll_progress(&root.owner, &mut cx).unwrap();
        if let Poll::Ready(result) = request.poll_admitted_in(&mut permit, &mut cx) {
            result.unwrap();
            break;
        }
    }
    let original_granted = permit.is_some();
    let held = source.poll_quiescent(&root.owner, &mut cx).is_pending();
    drop((permit, request));
    let next_phase_exited = matches!(
        source.poll_quiescent(&root.owner, &mut cx),
        Poll::Ready(Ok(()))
    );
    // An original journal fact, not registration or quiescence, selects the
    // existing necessary-settlement lane after original Root cancellation.
    source.publication_obligation_started();
    root.owner.cancel(CancellationReason::Requested);
    let borrowed = loan(&source.original).unwrap();
    let settlement = source.request(SharedDependency::LocalHost).unwrap();
    let mut permit = None;
    let settlement_pending = settlement
        .poll_admitted_in(&mut permit, &mut cx)
        .is_pending();
    drop(borrowed);
    for _ in 0..256 {
        drive_original(&mut driver);
        source.poll_continuing(&mut cx).unwrap();
        if let Poll::Ready(result) = settlement.poll_admitted_in(&mut permit, &mut cx) {
            result.unwrap();
            break;
        }
    }
    let settlement_granted = permit.is_some();
    let settlement_held = source
        .poll_actual_quiescent_continuing(&mut cx)
        .is_pending();
    drop((settlement, permit));
    let cleaned = finish(source, root, control, &mut driver, &mut cx);
    assert!(cleaned && was_ready && phase_exited && first_pending && original_granted && held);
    assert!(next_phase_exited && settlement_pending && settlement_granted && settlement_held);
}

#[test]
fn withdrawn_request_exit_wakes_the_current_source_without_repeating_its_refusal() {
    let (control, mut driver, root) = original_governor();
    let source = source(&root.owner);
    let prior_wake = Arc::new(OriginalWake::default());
    let prior_waker = Waker::from(Arc::clone(&prior_wake));
    let current_wake = Arc::new(OriginalWake::default());
    let current_waker = Waker::from(Arc::clone(&current_wake));
    let caller_wake = Arc::new(OriginalWake::default());
    let caller_waker = Waker::from(Arc::clone(&caller_wake));
    let mut current_cx = Context::from_waker(&current_waker);
    let mut caller_cx = Context::from_waker(&caller_waker);

    let borrowed = loan(&source.original).unwrap();
    let request = source.request(route()).unwrap();
    let mut permit = None;
    let first_pending = request
        .poll_admitted_in(&mut permit, &mut caller_cx)
        .is_pending();
    drop(borrowed);
    let withdrawal_waited = source.withdraw_unstarted_in().unwrap().is_none();
    // Same existing fixed wake header, with an earlier Source registration.
    // The real pending exit poll must replace it with its own current waker.
    source.wake.register(&prior_waker);
    let current_exit_pending = source
        .poll_actual_quiescent_continuing(&mut current_cx)
        .is_pending();
    let mut same_released = true;
    let caller_before = caller_wake.0.load(Ordering::SeqCst);
    for _ in 0..4 {
        // This is the reusable poll_admitted_in API, not a completed Future.
        same_released &= matches!(
            request.poll_admitted_in(&mut permit, &mut caller_cx),
            Poll::Ready(Err(ref error)) if released(error)
        );
        let _ = source.poll_actual_quiescent_continuing(&mut current_cx);
    }
    let unchanged_refusal_is_quiet = caller_wake.0.load(Ordering::SeqCst) == caller_before;
    let current_before_exit = current_wake.0.load(Ordering::SeqCst);
    let prior_before_exit = prior_wake.0.load(Ordering::SeqCst);
    drop((request, permit));
    // Capture notification BEFORE any later cleanup poll can rescue it.
    let actual_exit_woke_current = current_wake.0.load(Ordering::SeqCst) > current_before_exit;
    let stale_source_was_not_woken = prior_wake.0.load(Ordering::SeqCst) == prior_before_exit;
    let cleaned = finish(source, root, control, &mut driver, &mut current_cx);
    assert!(cleaned && first_pending && withdrawal_waited && current_exit_pending && same_released);
    assert!(
        actual_exit_woke_current && stale_source_was_not_woken,
        "the original request alias exits before notifying the current Source"
    );
    assert!(
        unchanged_refusal_is_quiet,
        "unchanged Released observations cannot self-wake"
    );
}
