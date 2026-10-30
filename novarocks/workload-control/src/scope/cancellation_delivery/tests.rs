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

//! Actual expiry delivery, callback custody, and durable retry readiness.

use super::*;
use crate::joint_admission::{JointProgress, JointProgressDriver};
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Waker},
};

fn process(fixed: bool) -> (WorkloadControl, Option<JointProgressDriver>) {
    if fixed {
        let (control, _, driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
        (control, Some(driver))
    } else {
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        control.mark_ready().unwrap();
        (control, None)
    }
}
fn expiring(control: &WorkloadControl) -> RootWork {
    control
        .try_begin_root(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(Instant::now() + Duration::from_secs(1)),
        })
        .unwrap()
}
fn acknowledge(control: &WorkloadControl) {
    while let Some(intent) = control.next_control() {
        intent.acknowledge();
    }
}
fn reap(driver: &mut Option<JointProgressDriver>) {
    let Some(driver) = driver else {
        return;
    };
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original reaper did not reach finite idle");
}
struct PanicWhenArmed(AtomicBool);
impl std::task::Wake for PanicWhenArmed {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional expiry notification wake panic");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn post_state_wake_unwind_restores_the_original_pending_delivery() {
    let (control, mut driver) = process(true);
    let root = expiring(&control);
    let id = root.owner.scope().id();
    let expiry = control.deadline_expiry_handle();
    let wake = Waker::from(Arc::new(PanicWhenArmed(AtomicBool::new(true))));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(catch_unwind(AssertUnwindSafe(|| expiry.expire_deadlines())).is_err());
    drop(progress);
    {
        let state = control.inner.state.lock().unwrap();
        assert!(matches!(
            state.nodes[&id].cancellation_notification,
            Notification::Pending(CancellationReason::DeadlineExceeded)
        ));
        assert!(!state.nodes[&id].cancellation_signalled);
    }
    let marker = expiry.progress_revision();
    let mut readiness = Box::pin(expiry.wait_state_change(marker));
    assert!(
        readiness
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(
        marker,
        expiry.progress_revision(),
        "readiness does not invent progress"
    );
    assert_eq!(expiry.next_deadline(), Some(Instant::now()));
    expiry.expire_deadlines();
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.nodes[&id].cancellation_notification.is_idle());
        assert!(state.nodes[&id].cancellation_signalled);
    }
    acknowledge(&control);
    root.business.release();
    root.owner.complete();
    control.close_admission();
    reap(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

struct PausedWake {
    armed: AtomicBool,
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl std::task::Wake for PausedWake {
    fn wake(self: Arc<Self>) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        if self.entered.send(()).is_err() {
            return;
        }
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    }
}
#[tokio::test(start_paused = true)]
async fn actual_delivery_retains_completed_nodes_and_excludes_their_old_deadlines() {
    for fixed in [false, true] {
        let (control, mut driver) = process(fixed);
        let root = expiring(&control);
        let id = root.owner.scope().id();
        let observer = root.owner.retirement_observer().ok();
        let view = root.owner.scope().cancellation().unwrap();
        let expiry = control.deadline_expiry_handle();
        let (entered, entry) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let wake = Arc::new(PausedWake {
            armed: AtomicBool::new(false),
            entered,
            release: Mutex::new(released),
        });
        let waker = Waker::from(Arc::clone(&wake));
        let mut cancelled = Box::pin(view.cancelled());
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        // Timer readiness does not poll this local future. Pause only the
        // actual cancellation notification after discovery and delivery claim.
        tokio::time::advance(Duration::from_secs(1)).await;
        wake.armed.store(true, Ordering::Release);
        let helper = control.deadline_expiry_handle();
        let runtime = tokio::runtime::Handle::current();
        let worker = std::thread::spawn(move || {
            let _entered = runtime.enter();
            helper.expire_deadlines();
        });
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            control.cancel_active_roots(CancellationReason::ServerShutdown),
            0,
            "the in-flight delivery already committed its first-wins reason"
        );
        acknowledge(&control);
        root.business.release();
        root.owner.complete();
        {
            let state = control.inner.state.lock().unwrap();
            assert!(state.nodes[&id].completed);
            assert!(matches!(
                state.nodes[&id].cancellation_notification,
                Notification::Delivering
            ));
        }
        // The same Node cannot be taken by an independent helper, and its
        // past deadline cannot make an otherwise idle supervisor busy-loop.
        expiry.expire_deadlines();
        assert_eq!(expiry.next_deadline(), None);
        let marker = expiry.progress_revision();
        let mut readiness = Box::pin(expiry.wait_state_change(marker));
        assert!(
            readiness
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(readiness);
        release.send(()).unwrap();
        worker.join().unwrap();
        drop(cancelled);
        assert!(!control.inner.state.lock().unwrap().nodes.contains_key(&id));
        if let Some(observer) = &observer {
            assert_eq!(observer.is_reclaimed(), Ok(false));
        }
        drop(view);
        control.close_admission();
        reap(&mut driver);
        if let Some(observer) = &observer {
            assert_eq!(observer.is_reclaimed(), Ok(true));
        }
        assert!(control.inner.state.lock().unwrap().is_drained());
    }
}

#[tokio::test(start_paused = true)]
async fn propagation_unwind_remains_retryable_after_completion_and_does_not_requeue_an_ack() {
    for terminal_settled in [false, true] {
        let (control, mut driver) = process(true);
        let root = expiring(&control);
        let id = root.owner.scope().id();
        let observer = root.owner.retirement_observer().unwrap();
        let view = root.owner.scope().cancellation().unwrap();
        let wake = Arc::new(PanicWhenArmed(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut cancelled = Box::pin(view.cancelled());
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        // Timer readiness only wakes this manually-polled local future. Arm
        // afterward so the next panic is the actual Cell Notify propagation.
        tokio::time::advance(Duration::from_secs(1)).await;
        wake.0.store(true, Ordering::Release);
        let expiry = control.deadline_expiry_handle();
        assert!(catch_unwind(AssertUnwindSafe(|| expiry.expire_deadlines())).is_err());
        drop(cancelled);
        assert_eq!(view.reason(), Some(CancellationReason::DeadlineExceeded));
        // A different real requester may already have marked signalled. Its
        // first-wins fact must survive the pending notification's retry.
        root.owner.cancel(CancellationReason::Requested);
        assert_eq!(view.reason(), Some(CancellationReason::DeadlineExceeded));
        if terminal_settled {
            // No Native/output/action exists in this local fixture.
            root.business.release();
            root.owner.complete_after_terminal_cancel_settled();
        } else {
            acknowledge(&control);
            root.business.release();
            root.owner.complete();
        }
        {
            let state = control.inner.state.lock().unwrap();
            let node = &state.nodes[&id];
            assert!(node.completed && node.cancellation_signalled);
            assert!(matches!(
                node.cancellation_notification,
                Notification::Pending(_)
            ));
            assert!(
                node.control_pending.is_empty() && !node.control_inflight && !node.control_queued
            );
        }
        assert_eq!(expiry.next_deadline(), Some(Instant::now()));
        expiry.expire_deadlines();
        assert!(
            !control.inner.state.lock().unwrap().nodes.contains_key(&id),
            "retry must not recreate the already settled Cancel intent"
        );
        assert_eq!(observer.is_reclaimed(), Ok(false));
        drop(view);
        control.close_admission();
        reap(&mut driver);
        assert_eq!(observer.is_reclaimed(), Ok(true));
        assert!(control.inner.state.lock().unwrap().is_drained());
    }
}

#[tokio::test(start_paused = true)]
async fn inherited_ready_cancellation_without_a_deadline_cannot_be_hidden_by_marker_capture() {
    let (control, mut driver) = process(true);
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let child = root
        .owner
        .scope()
        .child(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let id = child.scope().id();
    root.owner.cancel(CancellationReason::Requested);
    let expiry = control.deadline_expiry_handle();
    // Capture after cancellation: a revision-only waiter would miss this
    // already-ready child, which has no timer deadline of its own.
    let marker = expiry.progress_revision();
    assert_eq!(expiry.next_deadline(), Some(Instant::now()));
    let mut ready = Box::pin(expiry.wait_state_change(marker));
    assert!(
        ready
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(marker, expiry.progress_revision());
    expiry.expire_deadlines();
    assert!(control.inner.state.lock().unwrap().nodes[&id].cancellation_signalled);
    acknowledge(&control);
    root.business.release();
    root.owner.complete();
    child.complete();
    control.close_admission();
    reap(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}
