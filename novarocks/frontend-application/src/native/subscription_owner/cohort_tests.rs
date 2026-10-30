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

//! Actual scan and subscription sources borrow one original cohort.
use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests::{
    bind, cohort, drive, finish, fixture,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{
    task::{Context, Waker},
    time::Duration,
};

struct CheckStableWake {
    core: Arc<Core>,
    index: usize,
    wakes: std::sync::atomic::AtomicUsize,
}
impl std::fmt::Debug for CheckStableWake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckStableWake")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}
impl StatusIntakeWake for CheckStableWake {
    fn wake(&self) {
        let state = self
            .core
            .state
            .try_lock()
            .expect("no Native Core lock across notice");
        let record = state.records[self.index].as_ref().unwrap();
        assert!(matches!(record.state, OriginalState::Reserved));
        assert!(
            record
                .backing
                .owner
                .try_lock()
                .expect("stable receiver unlocked before notice")
                .is_some()
        );
        self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct ActualFuture {
    entered: Arc<Notify>,
    released: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}
impl Future for ActualFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.entered.notify_one();
        if self.released.load(std::sync::atomic::Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
impl Drop for ActualFuture {
    fn drop(&mut self) {
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_and_original_subscription_consume_same_cohort_with_other_control_builders_live() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    bind(&scope, &mut driver);
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = window.retain_alias();
    let cohort = cohort(&scope, 2, &mut driver);
    let mut scan_admission =
        ConnectorBlockingIoResponsibility::request_from_cohort(&scope, &alias, cohort.clone())
            .unwrap();
    let scan = scan_admission.ready().await.unwrap();
    assert_eq!(cohort.remaining(), 1);
    // Other unsubmitted control builders grant no Source authority. The
    // subscription must still consume the SAME cohort's remaining position.
    let second_builder = match scope.try_begin_control_binding().unwrap() {
        novarocks_workload_control::joint_admission::JointTryBegin::Started(builder) => builder,
        novarocks_workload_control::joint_admission::JointTryBegin::Full(_) => {
            panic!("control responsibility has no separate proposal quota")
        }
    };
    let blockers = [scope.begin_control_binding().unwrap(), second_builder];
    let (mut owner, port) = NativeSubscriptionOwner::new(Handle::current(), 8).unwrap();
    // Run the exact original service turn synchronously to observe the lock cut.
    owner.core.state.lock().unwrap().started = true;
    let reservation = port
        .prepare_from_cohort(&scope, cohort.clone(), None, None)
        .unwrap();
    assert_eq!(
        cohort.remaining(),
        1,
        "port prepare records only original metadata"
    );
    let wake = Arc::new(CheckStableWake {
        core: Arc::clone(&owner.core),
        index: reservation.index,
        wakes: std::sync::atomic::AtomicUsize::new(0),
    });
    assert!(
        !reservation
            .ready(&(wake.clone() as Arc<dyn StatusIntakeWake>))
            .unwrap()
    );
    assert!(poll_record(
        &owner.core,
        reservation.index,
        &mut Context::from_waker(Waker::noop())
    ));
    assert_eq!(
        cohort.remaining(),
        0,
        "one source position per actual subscription, independent of BE count guesses"
    );
    assert_eq!(wake.wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    let child_id = owner.core.state.lock().unwrap().records[reservation.index]
        .as_ref()
        .unwrap()
        .backing
        .owner
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .scope()
        .id();
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lease = reservation
        .start(ActualFuture {
            entered: Arc::clone(&entered),
            released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            dropped: Arc::clone(&dropped),
        })
        .unwrap();
    entered.notified().await;
    // Source children are independent; closing scan does not retire the stream.
    drop(scan);
    assert!(control.snapshot().scopes.iter().any(|s| s.id == child_id));
    drop(lease); // cancellation alone is not the original F destructor cut.
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
    owner.core.state.lock().unwrap().started = false;
    owner.start_reaper().unwrap();
    assert_eq!(
        owner
            .drain_until(Instant::now() + Duration::from_secs(3))
            .await,
        DrainObservation::Joined
    );
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!control.snapshot().scopes.iter().any(|s| s.id == child_id));
    drop((
        blockers,
        scan_admission,
        cohort,
        alias,
        window,
        capacity,
        scope,
    ));
    drive(&mut driver);
    finish(control, root, driver);
}

#[tokio::test]
async fn subscription_refuses_exhausted_cohort_without_individual_admission() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    bind(&scope, &mut driver);
    let cohort = cohort(&scope, 1, &mut driver);
    let mut consumed = None;
    cohort
        .take_child_in(&scope, &mut consumed)
        .unwrap()
        .notify();
    consumed
        .take()
        .unwrap()
        .complete_after_terminal_cancel_settled();
    let (mut owner, port) = NativeSubscriptionOwner::new(Handle::current(), 8).unwrap();
    owner.core.state.lock().unwrap().started = true;
    let reservation = port
        .prepare_from_cohort(&scope, cohort.clone(), None, None)
        .unwrap();
    assert!(poll_record(
        &owner.core,
        reservation.index,
        &mut Context::from_waker(Waker::noop())
    ));
    {
        let state = owner.core.state.lock().unwrap();
        let record = state.records[reservation.index].as_ref().unwrap();
        assert!(matches!(
            record.state,
            OriginalState::ChildFailed(RegistrationError::Work(WorkError::Capacity(
                "Native Source cohort exhausted"
            )))
        ));
        assert!(record.backing.owner.lock().unwrap().is_none());
    }
    drop(reservation);
    owner.core.state.lock().unwrap().started = false;
    owner.start_reaper().unwrap();
    assert_eq!(
        owner
            .drain_until(Instant::now() + Duration::from_secs(3))
            .await,
        DrainObservation::Joined
    );
    drop((cohort, capacity, scope));
    finish(control, root, driver);
}
