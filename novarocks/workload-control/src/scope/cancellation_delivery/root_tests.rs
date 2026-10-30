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

//! Root decisions precede callbacks; original Nodes retain pending delivery.

use super::*;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Waker},
};

struct SealAnother {
    sealer: WorkSuccessSealer,
    outcome: Mutex<Option<WorkSuccessSealOutcome>>,
}
impl std::task::Wake for SealAnother {
    fn wake(self: Arc<Self>) {
        *self.outcome.lock().unwrap() = Some(self.sealer.seal().unwrap());
    }
}
fn acknowledge(control: &WorkloadControl) {
    while let Some(intent) = control.next_control() {
        intent.acknowledge();
    }
}
#[tokio::test]
async fn batch_decisions_and_counts_precede_cross_root_reentrant_callbacks() {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    control.mark_ready().unwrap();
    let first = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let second = control
        .try_begin_root(WorkRequest::new(WorkClass::TableMaintenance))
        .unwrap();
    let sealed = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let existing = control
        .try_begin_root(WorkRequest::new(WorkClass::Statistics))
        .unwrap();
    let child = first
        .owner
        .scope()
        .child(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let child_view = child.scope().cancellation().unwrap();
    assert_eq!(
        sealed.owner.success_sealer().seal(),
        Ok(WorkSuccessSealOutcome::Sealed)
    );
    let original = CancellationReason::ExplicitKill {
        requester_session: 31,
    };
    existing.owner.cancel(original.clone());
    let first_view = first.owner.scope().cancellation().unwrap();
    let existing_view = existing.owner.scope().cancellation().unwrap();
    let sealed_view = sealed.owner.scope().cancellation().unwrap();
    let wake = Arc::new(SealAnother {
        sealer: second.owner.success_sealer(),
        outcome: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let mut cancelled = Box::pin(first_view.cancelled());
    assert!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(
        control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded),
        2
    );
    assert_eq!(
        *wake.outcome.lock().unwrap(),
        Some(WorkSuccessSealOutcome::Cancelled(
            CancellationReason::FrontendDrainDeadlineExceeded
        ))
    );
    assert_eq!(existing_view.reason(), Some(original));
    assert_eq!(sealed_view.reason(), None);
    assert_eq!(
        child_view.reason(),
        Some(CancellationReason::FrontendDrainDeadlineExceeded)
    );
    assert_eq!(
        control.cancel_active_roots(CancellationReason::ServerShutdown),
        0
    );
    let totals = control
        .snapshot()
        .root_lifecycle
        .frontend_drain_deadline_cancelled;
    assert_eq!(
        (
            totals.query,
            totals.table_maintenance,
            totals.statistics,
            totals.management
        ),
        (1, 1, 0, 0)
    );
    drop(cancelled);
    acknowledge(&control);
    child.complete();
    for root in [first, second, sealed, existing] {
        root.business.release();
        root.owner.complete();
    }
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
}

struct PanicOnce(AtomicBool);
impl std::task::Wake for PanicOnce {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional batch post-State notification panic");
        }
    }
}
#[tokio::test]
async fn whole_recorded_batch_survives_post_state_unwind_and_terminal_completion() {
    let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
    let first = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let second = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let ids = [first.owner.scope().id(), second.owner.scope().id()];
    let observers = [
        first.owner.retirement_observer().unwrap(),
        second.owner.retirement_observer().unwrap(),
    ];
    let views = [
        first.owner.scope().cancellation().unwrap(),
        second.owner.scope().cancellation().unwrap(),
    ];
    let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| control.cancel_active_roots(
            CancellationReason::FrontendDrainDeadlineExceeded
        )))
        .is_err()
    );
    drop(progress);
    for id in ids {
        let state = control.inner.state.lock().unwrap();
        assert!(matches!(
            state.nodes[&id].cancellation_notification,
            Notification::Pending(CancellationReason::FrontendDrainDeadlineExceeded)
        ));
        assert_eq!(
            state.root_lifecycle.frontend_drain_deadline_cancelled.query,
            2
        );
    }
    for root in [first, second] {
        root.business.release();
        root.owner.complete_after_terminal_cancel_settled();
    }
    assert_eq!(control.inner.state.lock().unwrap().nodes.len(), 2);
    // The original expiry owner also drives already-recorded completed tails.
    control.deadline_expiry_handle().expire_deadlines();
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
    assert!(
        control.next_control().is_none(),
        "retry cannot recreate terminal-settled Cancel"
    );
    assert_eq!(
        control.cancel_active_roots(CancellationReason::ServerShutdown),
        0
    );
    for observer in &observers {
        assert_eq!(observer.is_reclaimed(), Ok(false));
    }
    drop(views);
    control.close_admission();
    for _ in 0..10000 {
        if matches!(
            driver.step(),
            crate::joint_admission::JointProgress::Idle(_)
        ) {
            break;
        }
    }
    for observer in &observers {
        assert_eq!(observer.is_reclaimed(), Ok(true));
    }
    assert!(control.inner.state.lock().unwrap().is_drained());
}

// Register a second progress subscriber from discovery's wake. The second
// wake then pauses the actual delivery claim before Cancellation::request.
struct PauseClaim {
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl std::task::Wake for PauseClaim {
    fn wake(self: Arc<Self>) {
        if self.entered.send(()).is_ok() {
            let _ = self
                .release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5));
        }
    }
}
struct SubscribeClaim {
    inner: Arc<Inner>,
    wake: Waker,
    pending: Mutex<Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>>>,
}
impl std::task::Wake for SubscribeClaim {
    fn wake(self: Arc<Self>) {
        let inner = Arc::clone(&self.inner);
        let mut next = Box::pin(async move {
            let notified = inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            notified.await;
        });
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&self.wake))
                .is_pending()
        );
        *self.pending.lock().unwrap() = Some(next);
    }
}
#[derive(Default)]
struct CountWake(std::sync::atomic::AtomicUsize);
impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
#[tokio::test(start_paused = true)]
async fn batch_record_while_deadline_delivery_is_claimed_still_notifies_the_original_root() {
    let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
    let root = control
        .try_begin_root(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(Instant::now() + Duration::from_secs(1)),
        })
        .unwrap();
    let id = root.owner.scope().id();
    let view = root.owner.scope().cancellation().unwrap();
    let count = Arc::new(CountWake::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut cancelled = Box::pin(view.cancelled());
    assert!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let subscription = Arc::new(SubscribeClaim {
        inner: Arc::clone(&control.inner),
        wake: Waker::from(Arc::new(PauseClaim {
            entered,
            release: Mutex::new(released),
        })),
        pending: Mutex::new(None),
    });
    let progress_waker = Waker::from(Arc::clone(&subscription));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&progress_waker))
            .is_pending()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    count.0.store(0, Ordering::Relaxed);
    let expiry = control.deadline_expiry_handle();
    let runtime = tokio::runtime::Handle::current();
    let worker = std::thread::spawn(move || {
        let _entered = runtime.enter();
        expiry.expire_deadlines();
    });
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    let claimed = matches!(
        control.inner.state.lock().unwrap().nodes[&id].cancellation_notification,
        Notification::Delivering
    );
    let requested = control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded);
    let still_claimed = matches!(
        control.inner.state.lock().unwrap().nodes[&id].cancellation_notification,
        Notification::Delivering
    );
    let _ = release.send(());
    worker.join().unwrap();
    assert!(claimed && still_claimed);
    assert_eq!(requested, 1);
    assert!(
        count.0.load(Ordering::Relaxed) > 0,
        "pre-recorded first-wins must still notify the original root"
    );
    assert_eq!(
        view.reason(),
        Some(CancellationReason::FrontendDrainDeadlineExceeded)
    );
    assert_eq!(
        control
            .snapshot()
            .root_lifecycle
            .frontend_drain_deadline_cancelled
            .query,
        1
    );
    drop((cancelled, progress, subscription));
    acknowledge(&control);
    root.business.release();
    root.owner.complete();
    drop(view);
    control.close_admission();
    for _ in 0..10000 {
        if matches!(
            driver.step(),
            crate::joint_admission::JointProgress::Idle(_)
        ) {
            break;
        }
    }
    assert!(control.inner.state.lock().unwrap().is_drained());
}
