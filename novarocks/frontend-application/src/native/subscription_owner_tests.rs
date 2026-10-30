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

//! Original Tokio task/cause retirement component regressions.
use super::*;
use crate::task_execution::status_intake::{ObservationIntake, StatusIntakeWake};
use novarocks_workload_control::{
    RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::sync::{
    Condvar,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

fn work(limit: usize) -> (WorkloadControl, RootWork) {
    work_class(limit, WorkClass::Query)
}
fn work_class(limit: usize, class: WorkClass) -> (WorkloadControl, RootWork) {
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        scope_records_limit: limit,
        root_limit: 1,
        business_limit: 1,
        ..Default::default()
    })
    .unwrap()
    .owner;
    control.mark_ready().unwrap();
    let root = control.try_begin_root(WorkRequest::new(class)).unwrap();
    (control, root)
}
fn finish_work(control: WorkloadControl, root: RootWork) {
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    control.close_admission();
    assert!(control.shutdown().is_ok());
}
fn owner(limit: usize) -> (NativeSubscriptionOwner, NativeSubscriptionPort) {
    let (mut owner, port) = NativeSubscriptionOwner::new(Handle::current(), limit).unwrap();
    owner.start_reaper().unwrap();
    (owner, port)
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
struct Hold {
    entered: Notify,
    entered_flag: AtomicBool,
    released: Mutex<bool>,
    ready: Condvar,
    exited: AtomicBool,
}
impl Hold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            entered_flag: AtomicBool::new(false),
            released: Mutex::new(false),
            ready: Condvar::new(),
            exited: AtomicBool::new(false),
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.ready.notify_all();
    }
    fn block(&self) {
        self.entered_flag.store(true, Ordering::Release);
        self.entered.notify_one();
        let mut release = self.released.lock().unwrap();
        while !*release {
            release = self.ready.wait(release).unwrap();
        }
        self.exited.store(true, Ordering::Release);
    }
    fn watchdog(self: &Arc<Self>) -> std::thread::JoinHandle<()> {
        let held = Arc::clone(self);
        std::thread::spawn(move || {
            let release = held.released.lock().unwrap();
            let (release, timeout) = held
                .ready
                .wait_timeout_while(release, Duration::from_secs(3), |release| !*release)
                .unwrap();
            drop(release);
            if timeout.timed_out() {
                held.release();
            }
        })
    }
}
struct HeldFuture {
    held: Arc<Hold>,
}
impl Future for HeldFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}
impl Drop for HeldFuture {
    fn drop(&mut self) {
        self.held.block();
    }
}
struct RawPayload {
    held: Arc<Hold>,
}
impl Drop for RawPayload {
    fn drop(&mut self) {
        self.held.block();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_subscription_responsibility_keeps_the_original_workload_class() {
    for class in [
        WorkClass::Query,
        WorkClass::MaterializedView,
        WorkClass::Statistics,
        WorkClass::TableMaintenance,
        WorkClass::Management,
    ] {
        let (control, root) = work_class(2, class);
        let (mut owner, port) = owner(2);
        let reserved = port.reserve(&root.owner.scope(), None, None).unwrap();
        let snapshot = control.snapshot();
        let child = snapshot
            .scopes
            .iter()
            .find(|s| s.parent == Some(root.owner.scope().id()))
            .unwrap();
        assert_eq!(child.class, class);
        drop(reserved);
        let actual = owner
            .drain_until(Instant::now() + Duration::from_secs(2))
            .await;
        finish_work(control, root);
        assert_eq!(actual, DrainObservation::Joined);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_reservation_drop_releases_only_unstarted_original_child() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let reserved = port.reserve(&root.owner.scope(), None, None).unwrap();
    assert!(matches!(
        port.reserve(&root.owner.scope(), None, None),
        Err(RegistrationError::Work(WorkError::Capacity(
            "scope records"
        )))
    ));
    drop(reserved);
    let again = port.reserve(&root.owner.scope(), None, None).unwrap();
    drop(again);
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    finish_work(control, root);
    assert_eq!(actual, DrainObservation::Joined);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requested_stop_joins_same_original_handle_before_child_completion() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let (entered, original_entered) = tokio::sync::oneshot::channel();
    let lease = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(async move {
            entered.send(tokio::task::id()).unwrap();
            std::future::pending::<()>().await;
        })
        .unwrap();
    let original_id = tokio::time::timeout(Duration::from_secs(2), original_entered)
        .await
        .expect("the original subscription task must actually enter")
        .unwrap();
    let mut joined_id = None;
    until(|| {
        let state = owner.core.state.lock().unwrap();
        match &state.records[lease.index].as_ref().unwrap().state {
            OriginalState::Running(handle) => {
                joined_id = Some(handle.id());
                true
            }
            // The reaper lends this SAME handle outside Core for each poll.
            // Its temporary phase is not a terminal or a replacement task.
            OriginalState::Polling => false,
            _ => panic!("the entered pending original cannot retire before stop"),
        }
    })
    .await;
    assert_eq!(joined_id, Some(original_id));
    lease.stop();
    drop(lease);
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    let retained = owner.failure_facts();
    finish_work(control, root);
    assert_eq!(actual, DrainObservation::Joined);
    assert!(retained.original.is_none());
    assert!(
        owner
            .core
            .state
            .lock()
            .unwrap()
            .records
            .iter()
            .all(Option::is_none)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_destructor_remains_off_waiter_and_same_child_until_actual_exit() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let held = Hold::new();
    let watchdog = held.watchdog();
    let lease = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(HeldFuture {
            held: Arc::clone(&held),
        })
        .unwrap();
    lease.stop();
    tokio::time::timeout(Duration::from_secs(2), held.entered.notified())
        .await
        .unwrap();
    let first = owner
        .drain_until(Instant::now() + Duration::from_millis(10))
        .await;
    let capacity = root.owner.scope().child(WorkRequest::new(WorkClass::Query));
    // Release/join before assertions, including the independent watchdog.
    held.release();
    let second = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    drop(lease);
    watchdog.join().unwrap();
    finish_work(control, root);
    assert_eq!(first, DrainObservation::Deadline);
    assert_eq!(second, DrainObservation::Joined);
    assert!(matches!(
        capacity,
        Err(WorkError::Capacity("scope records"))
    ));
    assert!(held.exited.load(Ordering::Acquire));
}

#[derive(Debug)]
struct Wake;
impl StatusIntakeWake for Wake {
    fn wake(&self) {}
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_publisher_position_survives_original_abort_and_held_future_drop() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let intake = ObservationIntake::new(1, 3, 4096, 1, Arc::new(Wake)).unwrap();
    let others = [intake.subscribe().unwrap(), intake.subscribe().unwrap()];
    let publisher = Arc::new(intake.subscribe().unwrap());
    let held = Hold::new();
    let watchdog = held.watchdog();
    let lease = port
        .reserve(&root.owner.scope(), Some(Arc::clone(&publisher)), None)
        .unwrap()
        .start(HeldFuture {
            held: Arc::clone(&held),
        })
        .unwrap();
    drop(publisher);
    lease.stop();
    tokio::time::timeout(Duration::from_secs(2), held.entered.notified())
        .await
        .unwrap();
    let refused = intake.subscribe().is_err();
    held.release();
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    let recovered = intake.subscribe();
    let recovered_ok = recovered.is_ok();
    drop(lease);
    drop(recovered);
    drop(others);
    watchdog.join().unwrap();
    finish_work(control, root);
    assert!(refused);
    assert!(recovered_ok);
    assert_eq!(actual, DrainObservation::Joined);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_panic_payload_destructor_is_actually_retired_before_scope_release() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let held = Hold::new();
    let watchdog = held.watchdog();
    let raw = RawPayload {
        held: Arc::clone(&held),
    };
    let lease = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(async move {
            std::panic::panic_any(raw);
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), held.entered.notified())
        .await
        .unwrap();
    let first = owner
        .drain_until(Instant::now() + Duration::from_millis(10))
        .await;
    let facts = owner.failure_facts();
    let capacity = root.owner.scope().child(WorkRequest::new(WorkClass::Query));
    held.release();
    let second = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    drop(lease);
    watchdog.join().unwrap();
    finish_work(control, root);
    assert_eq!(first, DrainObservation::Deadline);
    assert!(facts.original.unwrap().panic);
    assert!(matches!(second, DrainObservation::Failed(_)));
    assert!(matches!(
        capacity,
        Err(WorkError::Capacity("scope records"))
    ));
    assert!(held.exited.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_lease_drop_cannot_abort_recycled_original_position() {
    struct ReadyTracked(Arc<AtomicBool>);
    impl Future for ReadyTracked {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }
    impl Drop for ReadyTracked {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let deadline = Instant::now() + Duration::from_secs(2);
    let reservation = port.reserve(&root.owner.scope(), None, None).unwrap();
    let retained_backing = {
        let state = owner.core.state.lock().unwrap();
        Arc::clone(&state.records[reservation.index].as_ref().unwrap().backing)
    };
    let original_scope = retained_backing
        .owner
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .scope();
    let future_destroyed = Arc::new(AtomicBool::new(false));
    let stale = reservation
        .start(ReadyTracked(Arc::clone(&future_destroyed)))
        .unwrap();
    tokio::time::timeout_at(deadline.into(), async {
        while owner.core.state.lock().unwrap().records[stale.index].is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A free role record does not release another owner's WorkScope. Retain
    // the last real backing alias to make that interval deterministic.
    let future_really_destroyed = future_destroyed.load(Ordering::Acquire);
    let original_retired = retained_backing.retired.load(Ordering::Acquire);
    let scope_still_registered = original_scope.check().is_ok();
    let refused_before_actual_release = matches!(
        port.reserve(&root.owner.scope(), None, None),
        Err(RegistrationError::Work(WorkError::Capacity(
            "scope records"
        )))
    );
    drop(retained_backing);
    tokio::time::timeout_at(deadline.into(), original_scope.wait_released())
        .await
        .unwrap();
    let scope_really_released = matches!(original_scope.check(), Err(WorkError::Released));
    // Reserve once after the actual authority reports release; do not retry
    // capacity failures or infer this fact from the role-record vacancy.
    let live = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(std::future::pending())
        .unwrap();
    let reused_original_position = live.index == stale.index;
    let old_generation = stale.generation;
    let new_generation = live.generation;
    drop(stale);
    let requested = owner.core.state.lock().unwrap().records[live.index]
        .as_ref()
        .unwrap()
        .stop_requested;
    live.stop();
    let actual = owner.drain_until(deadline).await;
    drop(live);
    finish_work(control, root);
    assert!(future_really_destroyed);
    assert!(original_retired);
    assert!(scope_still_registered);
    assert!(refused_before_actual_release);
    assert!(scope_really_released);
    assert!(reused_original_position);
    assert_ne!(old_generation, new_generation);
    assert!(!requested);
    assert_eq!(actual, DrainObservation::Joined);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_overflow_is_sticky_before_any_original_spawn() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    owner.core.state.lock().unwrap().next_generation = u64::MAX;
    let rejected = port.reserve(&root.owner.scope(), None, None);
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    finish_work(control, root);
    assert!(matches!(rejected, Err(RegistrationError::Generation)));
    assert!(matches!(
        actual,
        DrainObservation::Failed(FailureFacts {
            counter_overflow: true,
            ..
        })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_after_reservation_retires_unpolled_future_without_spawning_it() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let reservation = port.reserve(&root.owner.scope(), None, None).unwrap();
    owner.request_abort();
    let polled = Arc::new(AtomicBool::new(false));
    let copy = Arc::clone(&polled);
    let result = reservation.start(async move {
        copy.store(true, Ordering::Release);
    });
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    finish_work(control, root);
    assert!(matches!(result, Err(RegistrationError::Closed)));
    assert_eq!(actual, DrainObservation::Joined);
    assert!(!polled.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unexpected_original_join_sets_same_query_subscription_state_rejected() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let observed = Arc::new(Mutex::new(SubscriptionState::Live));
    let lease = port
        .reserve(&root.owner.scope(), None, Some(Arc::clone(&observed)))
        .unwrap()
        .start(async {
            panic!("original subscription panic canary");
        })
        .unwrap();
    until(|| owner.failure_facts().original.is_some()).await;
    let state = *observed.lock().unwrap();
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    drop(lease);
    finish_work(control, root);
    assert_eq!(state, SubscriptionState::Rejected);
    assert!(matches!(actual, DrainObservation::Failed(_)));
}

struct TwoPanicFuture {
    join_cause: Option<RawPayload>,
    drop_cause: Option<RawPayload>,
}
impl Future for TwoPanicFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
        std::panic::panic_any(self.join_cause.take().expect("one original join panic"));
    }
}
impl Drop for TwoPanicFuture {
    fn drop(&mut self) {
        std::panic::panic_any(
            self.drop_cause
                .take()
                .expect("one original F destructor panic"),
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_destructor_panic_cannot_destroy_second_original_raw_cause() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let future_drop = Hold::new();
    let original_join = Hold::new();
    let guards = [future_drop.watchdog(), original_join.watchdog()];
    let lease = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(TwoPanicFuture {
            join_cause: Some(RawPayload {
                held: Arc::clone(&original_join),
            }),
            drop_cause: Some(RawPayload {
                held: Arc::clone(&future_drop),
            }),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), future_drop.entered.notified())
        .await
        .unwrap();
    let second_not_entered = !original_join.entered_flag.load(Ordering::Acquire);
    let still_charged = root.owner.scope().child(WorkRequest::new(WorkClass::Query));
    future_drop.release();
    tokio::time::timeout(Duration::from_secs(2), original_join.entered.notified())
        .await
        .unwrap();
    original_join.release();
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    drop(lease);
    for guard in guards {
        guard.join().unwrap();
    }
    finish_work(control, root);
    assert!(second_not_entered);
    assert!(matches!(
        still_charged,
        Err(WorkError::Capacity("scope records"))
    ));
    assert!(future_drop.exited.load(Ordering::Acquire));
    assert!(original_join.exited.load(Ordering::Acquire));
    assert!(matches!(actual, DrainObservation::Failed(facts) if facts.cleanup_failures > 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_borrowed_drain_retains_same_original_reaper_and_records() {
    fn cleanup_id(owner: &NativeSubscriptionOwner, lease: &SubscriptionLease) -> Option<Id> {
        let state = owner.core.state.lock().unwrap();
        let record = state.records[lease.index].as_ref()?;
        let OriginalState::Retiring(retirement) = &record.state else {
            return None;
        };
        let future = retirement.future.as_ref()?;
        match &future.state {
            super::super::original_retirement::CleanupState::Running(original) => {
                Some(original.id())
            }
            _ => None,
        }
    }
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let held = Hold::new();
    let watchdog = held.watchdog();
    let lease = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(HeldFuture {
            held: Arc::clone(&held),
        })
        .unwrap();
    lease.stop();
    tokio::time::timeout(Duration::from_secs(2), held.entered.notified())
        .await
        .unwrap();
    let original_id = owner.reaper.as_ref().unwrap().id();
    let original_cleanup_id = cleanup_id(&owner, &lease);
    let deadline = Instant::now() + Duration::from_secs(2);
    let first_poll = {
        let mut borrowed = Box::pin(owner.drain_until(deadline));
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        let polled = borrowed.as_mut().poll(&mut cx);
        drop(borrowed);
        polled
    };
    let same_original = owner
        .reaper
        .as_ref()
        .is_some_and(|handle| handle.id() == original_id);
    let same_cleanup =
        original_cleanup_id.is_some() && cleanup_id(&owner, &lease) == original_cleanup_id;
    held.release();
    let actual = owner.drain_until(deadline).await;
    drop(lease);
    watchdog.join().unwrap();
    finish_work(control, root);
    assert!(first_poll.is_pending());
    assert!(same_original && same_cleanup);
    assert_eq!(actual, DrainObservation::Joined);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn previously_joined_owner_cannot_succeed_under_expired_original_clock() {
    let (control, root) = work(2);
    let (mut owner, _port) = owner(2);
    let first = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    let expired = owner
        .drain_until(Instant::now().checked_sub(Duration::from_secs(1)).unwrap())
        .await;
    finish_work(control, root);
    assert_eq!(first, DrainObservation::Joined);
    assert_eq!(expired, DrainObservation::Deadline);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_owner_closes_projection_and_aborts_without_minting_join_success() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let lease = port
        .reserve(&root.owner.scope(), None, None)
        .unwrap()
        .start(std::future::pending())
        .unwrap();
    // This test's outer owner keeps the SAME actual reaper handle before dropping
    // the facade, so teardown still actually joins it. Production Drop alone
    // deliberately supplies no such successful-exit proof.
    let original_reaper = owner.reaper.take().unwrap();
    drop(owner);
    let rejected = port.reserve(&root.owner.scope(), None, None);
    let actual = tokio::time::timeout(Duration::from_secs(2), original_reaper)
        .await
        .unwrap();
    drop(lease);
    finish_work(control, root);
    assert!(matches!(rejected, Err(RegistrationError::Closed)));
    assert!(actual.is_ok());
}

struct DropPanicFuture {
    payload: Option<RawPayload>,
}
impl Future for DropPanicFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}
impl Drop for DropPanicFuture {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.take().expect("one original destructor panic"));
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_only_panic_rejects_same_state_while_successor_still_held() {
    let (control, root) = work(2);
    let (mut owner, port) = owner(2);
    let held = Hold::new();
    let watchdog = held.watchdog();
    let state = Arc::new(Mutex::new(SubscriptionState::Live));
    let lease = port
        .reserve(&root.owner.scope(), None, Some(Arc::clone(&state)))
        .unwrap()
        .start(DropPanicFuture {
            payload: Some(RawPayload {
                held: Arc::clone(&held),
            }),
        })
        .unwrap();
    lease.stop();
    tokio::time::timeout(Duration::from_secs(2), held.entered.notified())
        .await
        .unwrap();
    until(|| owner.failure_facts().cleanup_failures != 0).await;
    let before = owner.failure_facts();
    let observed = *state.lock().unwrap();
    held.release();
    let actual = owner
        .drain_until(Instant::now() + Duration::from_secs(2))
        .await;
    drop(lease);
    watchdog.join().unwrap();
    finish_work(control, root);
    assert!(before.original.is_none());
    assert_eq!(observed, SubscriptionState::Rejected);
    assert!(matches!(actual, DrainObservation::Failed(facts) if facts.cleanup_failures > 0));
}
