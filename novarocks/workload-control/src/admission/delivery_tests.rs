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

//! Callback exit, original-row custody, and panic-safe admission handoffs.

use super::*;
use crate::{WorkClass, WorkRequest, WorkloadControl};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

fn control(waiting_limit: usize) -> WorkloadControl {
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        waiting_limit,
        preparation_limit: 1,
        execution_limit: 1,
        executions_per_root: 1,
        query_concurrency_limit: 1,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    control.mark_ready().unwrap();
    control
}
fn stage(scope: &WorkScope) -> StageAdmission {
    scope
        .acquire(StageRequest {
            stage: Stage::Preparation,
            retained_bytes: 13,
        })
        .unwrap()
}

struct PausedWake {
    pending: Mutex<Option<StageAdmission>>,
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl std::task::Wake for PausedWake {
    fn wake(self: Arc<Self>) {
        // The receiving future is logically withdrawn while this exact
        // original callback is still executing on another thread.
        drop(self.pending.lock().unwrap().take());
        // A fixture assertion can unwind before the worker starts. Do not
        // turn its already-dropped observer into a second destructor panic.
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

#[tokio::test]
async fn an_executing_callback_keeps_its_original_row_bytes_and_node_until_actual_exit() {
    let control = control(1);
    let blocker = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let owner = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let permit = blocker
        .owner
        .scope()
        .try_acquire(Stage::Preparation)
        .unwrap();
    let mut pending = stage(&owner.owner.scope());
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let wake = Arc::new(PausedWake {
        pending: Mutex::new(None),
        entered,
        release: Mutex::new(released),
    });
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    *wake.pending.lock().unwrap() = Some(pending);
    drop((wake, waker));
    let worker = std::thread::spawn(move || drop(permit));
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    let id = owner.owner.scope().id();
    owner.business.release();
    owner.owner.complete();
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.requests.len(), 1);
        let row = state.requests.values().next().unwrap();
        assert!(row.retired && row.delivery_in_flight);
        assert_eq!(state.waiting_bytes, 13);
        assert_eq!(state.nodes[&id].pending_admissions, 1);
        assert!(state.nodes[&id].completed);
    }
    assert!(matches!(
        blocker.owner.scope().acquire(StageRequest {
            stage: Stage::Preparation,
            retained_bytes: 0
        }),
        Err(WorkError::Capacity("waiting entries"))
    ));
    release.send(()).unwrap();
    worker.join().unwrap();
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.requests.is_empty());
        assert_eq!(state.waiting_bytes, 0);
        assert!(!state.nodes.contains_key(&id));
    }
    let next = stage(&blocker.owner.scope()).await.unwrap();
    drop(next);
    blocker.business.release();
    blocker.owner.complete();
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

struct LockCheckingWake {
    inner: Weak<Inner>,
    dropped: Arc<AtomicBool>,
}
impl std::task::Wake for LockCheckingWake {
    fn wake(self: Arc<Self>) {
        // Consume this original alias; its last Drop checks lock-free retirement.
        drop(self);
    }
}
impl Drop for LockCheckingWake {
    fn drop(&mut self) {
        assert!(
            self.inner.upgrade().unwrap().state.try_lock().is_ok(),
            "Waker drop ran under State"
        );
        self.dropped.store(true, Ordering::Release);
    }
}
#[tokio::test]
async fn replacing_and_withdrawing_registered_wakers_destroys_them_outside_state() {
    let control = control(2);
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let blocker = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let permit = blocker
        .owner
        .scope()
        .try_acquire(Stage::Preparation)
        .unwrap();
    let mut pending = stage(&root.owner.scope());
    let old_dropped = Arc::new(AtomicBool::new(false));
    let new_dropped = Arc::new(AtomicBool::new(false));
    let old = Waker::from(Arc::new(LockCheckingWake {
        inner: Arc::downgrade(&control.inner),
        dropped: Arc::clone(&old_dropped),
    }));
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&old))
            .is_pending()
    );
    drop(old);
    assert!(!old_dropped.load(Ordering::Acquire));
    let new = Waker::from(Arc::new(LockCheckingWake {
        inner: Arc::downgrade(&control.inner),
        dropped: Arc::clone(&new_dropped),
    }));
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&new))
            .is_pending()
    );
    assert!(old_dropped.load(Ordering::Acquire));
    drop(new);
    drop(pending);
    assert!(new_dropped.load(Ordering::Acquire));
    assert_eq!(control.snapshot().admission_records, 0);
    drop(permit);
    for root in [root, blocker] {
        root.business.release();
        root.owner.complete();
    }
}

struct PanicOnce(AtomicBool);
impl std::task::Wake for PanicOnce {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional admission handoff wake panic");
        }
    }
}
fn progress_panic() -> Waker {
    Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))))
}

#[tokio::test]
async fn queue_and_receipt_publication_unwind_return_each_original_grant_once() {
    for query in [false, true] {
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            waiting_limit: 2,
            query_concurrency_limit: 1,
            preparation_limit: 1,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let capacity = control
            .configure_result_capacity(crate::ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .begin_query_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        // The actual future already exists when its original ticket commits.
        let wake = progress_panic();
        let mut watcher = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            watcher
                .as_mut()
                .poll(&mut Context::from_waker(&wake))
                .is_pending()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                if query {
                    drop(scope.admit_query_with_result(ResultWindowClass::Local));
                } else {
                    drop(scope.acquire(StageRequest {
                        stage: Stage::Preparation,
                        retained_bytes: 13,
                    }));
                }
            }))
            .is_err()
        );
        drop(watcher);
        assert_eq!(control.snapshot().admission_records, 0);
        assert_eq!(control.snapshot().preparation, 0);
        assert_eq!(control.snapshot().admitted_queries, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        // A raw grant cannot leave State without its original RAII receiver.
        if query {
            let mut pending = scope
                .admit_query_with_result(ResultWindowClass::Local)
                .unwrap();
            let wake = progress_panic();
            let mut watcher = Box::pin(control.wait_progress(control.progress_revision()));
            assert!(
                watcher
                    .as_mut()
                    .poll(&mut Context::from_waker(&wake))
                    .is_pending()
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop()))
                }))
                .is_err()
            );
            drop((watcher, pending));
        } else {
            let mut pending = stage(&scope);
            let wake = progress_panic();
            let mut watcher = Box::pin(control.wait_progress(control.progress_revision()));
            assert!(
                watcher
                    .as_mut()
                    .poll(&mut Context::from_waker(&wake))
                    .is_pending()
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop()))
                }))
                .is_err()
            );
            drop((watcher, pending));
        }
        assert_eq!(control.snapshot().admission_records, 0);
        assert_eq!(control.snapshot().preparation, 0);
        assert_eq!(control.snapshot().admitted_queries, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        root.owner.complete();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }
}

struct CountWake(Arc<AtomicUsize>);
impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn callback_unwind_leaves_other_pending_rows_visible_to_the_original_progress_owner() {
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        preparation_limit: 1,
        capacity_wait_timeout: Duration::from_millis(50),
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    control.mark_ready().unwrap();
    let blocker = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let permit = blocker
        .owner
        .scope()
        .try_acquire(Stage::Preparation)
        .unwrap();
    let first = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let second = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let mut a = stage(&first.owner.scope());
    let mut b = stage(&second.owner.scope());
    let first_wake = progress_panic();
    let count = Arc::new(AtomicUsize::new(0));
    let second_wake = Waker::from(Arc::new(CountWake(Arc::clone(&count))));
    assert!(
        Pin::new(&mut a)
            .poll(&mut Context::from_waker(&first_wake))
            .is_pending()
    );
    assert!(
        Pin::new(&mut b)
            .poll(&mut Context::from_waker(&second_wake))
            .is_pending()
    );
    // Keep the current-thread timer driver out of this policy transaction;
    // the actual recorded wait deadlines still expire normally.
    std::thread::sleep(Duration::from_millis(60));
    assert!(catch_unwind(AssertUnwindSafe(|| control.inner.update(|_| ()))).is_err());
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert!(delivery::has_pending_wakes(
        &control.inner.state.lock().unwrap()
    ));
    let marker = control.progress_revision();
    let mut progress = Box::pin(control.wait_progress(marker));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(count.load(Ordering::Acquire), 1);
    drop((progress, a, b, permit));
    for root in [blocker, first, second] {
        root.business.release();
        root.owner.complete();
    }
    assert_eq!(control.snapshot().admission_records, 0);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[tokio::test]
async fn an_independent_update_can_wake_another_row_while_one_callback_is_paused() {
    let control = control(2);
    let blocker = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let a = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let b = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let preparation = blocker
        .owner
        .scope()
        .try_acquire(Stage::Preparation)
        .unwrap();
    let execution = blocker.owner.scope().try_acquire(Stage::Execution).unwrap();
    let mut first = stage(&a.owner.scope());
    let mut second = b
        .owner
        .scope()
        .acquire(StageRequest {
            stage: Stage::Execution,
            retained_bytes: 0,
        })
        .unwrap();
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let wake = Arc::new(PausedWake {
        pending: Mutex::new(None),
        entered,
        release: Mutex::new(released),
    });
    let first_wake = Waker::from(Arc::clone(&wake));
    assert!(
        Pin::new(&mut first)
            .poll(&mut Context::from_waker(&first_wake))
            .is_pending()
    );
    *wake.pending.lock().unwrap() = Some(first);
    drop((wake, first_wake));
    let count = Arc::new(AtomicUsize::new(0));
    let second_wake = Waker::from(Arc::new(CountWake(Arc::clone(&count))));
    assert!(
        Pin::new(&mut second)
            .poll(&mut Context::from_waker(&second_wake))
            .is_pending()
    );
    let worker = std::thread::spawn(move || drop(preparation));
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    // This transaction must not wait for A's independent callback to return.
    drop(execution);
    assert_eq!(count.load(Ordering::Acquire), 1);
    let Poll::Ready(Ok(second_permit)) =
        Pin::new(&mut second).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("independent execution grant");
    };
    assert_eq!(control.snapshot().admission_records, 1);
    release.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(control.snapshot().admission_records, 0);
    drop((second_permit, second));
    for root in [blocker, a, b] {
        root.business.release();
        root.owner.complete();
    }
    assert_eq!(control.snapshot().root_responsibilities, 0);
}
