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

use super::super::transport_supervisor::{
    NativeTransportLane, NativeTransportReadyWake, NativeTransportSupervisor,
};
use super::*;
use std::sync::{
    Condvar,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Debug)]
struct Wake;
impl NativeTransportReadyWake for Wake {
    fn wake(&self) {}
}
fn original_charge(supervisor: &NativeTransportSupervisor) -> ApplyReservations {
    let waiter = supervisor.register_waiter(Arc::new(Wake)).unwrap();
    let peer = novarocks_types::BackendProcessId::new_v7();
    let mut queue = supervisor
        .try_reserve_queue(&waiter, peer, NativeTransportLane::Ordinary, 1, 1)
        .unwrap();
    let mut encoding = supervisor
        .try_reserve_encoding(&waiter, peer, NativeTransportLane::Ordinary)
        .unwrap();
    encoding.shrink_to(1);
    queue.mark_in_flight();
    ApplyReservations::new(vec![Box::new(queue)], encoding)
}
fn setup() -> (ApplySendOwner, ApplySendPort, NativeTransportSupervisor) {
    let supervisor = NativeTransportSupervisor::from_transport(TransportBudget::DEFAULT).unwrap();
    let (mut owner, port) =
        ApplySendOwner::new(Handle::current(), TransportBudget::DEFAULT).unwrap();
    owner.start_reaper().unwrap();
    (owner, port, supervisor)
}
fn storage(owner: &ApplySendOwner) -> (usize, usize, usize) {
    let state = owner.core.state.lock().unwrap();
    (
        state.records.as_ptr() as usize,
        state.records.len(),
        state.records.iter().flatten().count(),
    )
}
fn cleanup_id(owner: &ApplySendOwner) -> Option<Id> {
    let state = owner.core.state.lock().unwrap();
    state.records.iter().flatten().find_map(|r| match &r.state {
        OriginalState::Retiring(retirement) => match &retirement.state {
            CleanupState::Running(join) => Some(join.id()),
            _ => None,
        },
        _ => None,
    })
}
fn send_id(owner: &ApplySendOwner) -> Option<Id> {
    let state = owner.core.state.lock().unwrap();
    state.records.iter().flatten().find_map(|r| match &r.state {
        OriginalState::Running { join, .. } => Some(join.id()),
        _ => None,
    })
}
struct DropControl {
    entered: Notify,
    released: Mutex<bool>,
    ready: Condvar,
    drops: AtomicUsize,
    identity_seen: AtomicBool,
}
impl DropControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            released: Mutex::new(false),
            ready: Condvar::new(),
            drops: AtomicUsize::new(0),
            identity_seen: AtomicBool::new(false),
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.ready.notify_all();
    }
}
struct RawPayload {
    control: Arc<DropControl>,
    original: Arc<DropControl>,
    successor: bool,
}
impl std::fmt::Debug for RawPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RAW_CAUSE_FORMAT_CANARY")
    }
}
impl Drop for RawPayload {
    fn drop(&mut self) {
        self.control.identity_seen.store(
            Arc::ptr_eq(&self.control, &self.original),
            Ordering::Release,
        );
        self.control.drops.fetch_add(1, Ordering::AcqRel);
        self.control.entered.notify_one();
        let mut released = self.control.released.lock().unwrap();
        while !*released {
            released = self.control.ready.wait(released).unwrap();
        }
        drop(released);
        if self.successor {
            std::panic::panic_any(RawPayload {
                control: Arc::clone(&self.control),
                original: Arc::clone(&self.original),
                successor: false,
            });
        }
    }
}
fn payload(control: &Arc<DropControl>, successor: bool) -> RawPayload {
    RawPayload {
        control: Arc::clone(control),
        original: Arc::clone(control),
        successor,
    }
}

#[test]
fn impossible_fixed_storage_fails_before_any_original_task_exists() {
    // Exercise std's actual allocation error, not a synthetic source.
    let failure = allocate_original_slots(usize::MAX).err();
    assert!(failure.is_some());
}

#[tokio::test]
async fn unstarted_original_owner_has_no_child_and_empty_drop_needs_no_spawn_or_cleanup_clock() {
    let (owner, port) = ApplySendOwner::new(Handle::current(), TransportBudget::DEFAULT).unwrap();
    let no_child = owner.reaper.is_none();
    let no_records = storage(&owner).2 == 0;
    // No original Native F, job or join was ever published.
    drop(port);
    drop(owner);
    assert!(no_child && no_records);
}

#[tokio::test]
async fn shared_bank_drain_retries_until_the_original_reserved_row_exits() {
    let (mut owner, port) =
        ApplySendOwner::new(Handle::current(), TransportBudget::DEFAULT).unwrap();
    let before = storage(&owner);
    owner.open_shared_bank().unwrap();
    assert!(owner.reaper.is_none());
    let slot = port.reserve_shared_slot(1).unwrap();

    assert_eq!(
        owner.drain_until(Instant::now()).await,
        DrainObservation::Deadline
    );
    assert!(!owner.is_joined());
    assert_eq!(storage(&owner), (before.0, before.1, 1));

    drop(slot);
    let deadline = Instant::now() + Duration::from_secs(2);
    assert_eq!(
        owner.drain_until(deadline).await,
        DrainObservation::Complete
    );
    assert!(owner.is_joined());
    assert_eq!(storage(&owner), before);
}

#[tokio::test]
async fn continuous_reaper_returns_capacity_without_next_submit_and_keeps_original_storage() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut owner, port, supervisor) = setup();
    let before = storage(&owner);
    let charge = original_charge(&supervisor);
    let slot = port.reserve_original_slot(1).unwrap();
    let (release, wait) = tokio::sync::oneshot::channel();
    let disposition = slot.commit(charge, async move {
        let _ = wait.await;
    });
    let held = supervisor.snapshot();
    let occupied = storage(&owner);
    let _ = release.send(());
    let progressed = tokio::time::timeout_at(deadline.into(), async {
        while supervisor.snapshot().retained_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    if !progressed {
        owner.request_abort();
    }
    let drained = owner.drain_until(deadline).await;
    let after = supervisor.snapshot();
    let final_storage = storage(&owner);
    assert_eq!(disposition, OriginalDisposition::Started);
    assert!(progressed);
    assert_eq!(drained, DrainObservation::Complete);
    assert_eq!(held.retained_items, 1);
    assert_eq!(after.retained_items, 0);
    assert_eq!(before.0, occupied.0);
    assert_eq!(occupied.0, final_storage.0);
    assert_eq!(
        final_storage.1,
        TransportBudget::DEFAULT.max_backend_queued_operations()
    );
    assert_eq!(occupied.2, 1);
    assert_eq!(final_storage.2, 0);
}

#[tokio::test]
async fn closed_before_reservation_leaves_original_charge_with_caller_and_constructs_no_future() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut owner, port, supervisor) = setup();
    let original_batch_charge = original_charge(&supervisor);
    let constructed = AtomicUsize::new(0);
    owner.close();
    let result = port.reserve_original_slot(1);
    let failure = match result {
        Err(failure) => Some(failure),
        Ok(slot) => {
            constructed.fetch_add(1, Ordering::AcqRel);
            drop(slot);
            None
        }
    };
    let held = supervisor.snapshot();
    // The synchronous caller has its original carrier for definitely-unsent rollback.
    drop(original_batch_charge);
    let drained = owner.drain_until(deadline).await;
    let after = supervisor.snapshot();
    assert_eq!(failure, Some(RegistrationFailure::Closed));
    assert_eq!(constructed.load(Ordering::Acquire), 0);
    assert_eq!(held.retained_items, 1);
    assert_eq!(after.retained_items, 0);
    assert_eq!(drained, DrainObservation::RegistrationFailure);
    assert!(owner.is_joined());
}

#[tokio::test]
async fn empty_reserved_ticket_blocks_drain_and_cannot_refresh_original_deadline() {
    let cleanup = Instant::now() + Duration::from_secs(2);
    let original_deadline = Instant::now() + Duration::from_millis(30);
    let (mut owner, port, supervisor) = setup();
    let original_batch_charge = original_charge(&supervisor);
    let slot = port.reserve_original_slot(1).unwrap();
    let first = owner.drain_until(original_deadline).await;
    let held = supervisor.snapshot();
    let occupied = storage(&owner).2;
    drop(slot); // Ticket has no F; release slot before its original items.
    drop(original_batch_charge);
    let joined = owner.drain_until(cleanup).await;
    let late = owner.drain_until(original_deadline).await;
    let after = supervisor.snapshot();
    assert_eq!(first, DrainObservation::Deadline);
    assert_eq!(held.retained_items, 1);
    assert_eq!(occupied, 1);
    assert_eq!(joined, DrainObservation::Complete);
    assert_eq!(late, DrainObservation::Deadline);
    assert_eq!(after.retained_items, 0);
}

#[tokio::test]
async fn closed_after_reservation_keeps_original_unpolled_future_in_same_retiring_slot() {
    let cleanup = Instant::now() + Duration::from_secs(2);
    let original_deadline = Instant::now() + Duration::from_millis(40);
    let (mut owner, port, supervisor) = setup();
    let control = DropControl::new();
    let first_polled = Arc::new(AtomicBool::new(false));
    let charge = original_charge(&supervisor);
    let slot = port.reserve_original_slot(1).unwrap();
    let before = storage(&owner);
    owner.close();
    let raw = payload(&control, false);
    let polled = Arc::clone(&first_polled);
    let disposition = slot.commit(charge, async move {
        let _raw = raw;
        polled.store(true, Ordering::Release);
        std::future::pending::<()>().await;
    });
    let entered = tokio::time::timeout_at(original_deadline.into(), control.entered.notified())
        .await
        .is_ok();
    let id_before = cleanup_id(&owner);
    let first = owner.drain_until(original_deadline).await;
    let id_after = cleanup_id(&owner);
    let held = supervisor.snapshot();
    let occupied = storage(&owner);
    control.release(); // Release actual destructor before any result assertions.
    let joined = owner.drain_until(cleanup).await;
    let late = owner.drain_until(original_deadline).await;
    let after = supervisor.snapshot();
    assert_eq!(disposition, OriginalDisposition::ClosedAndRetiring);
    assert!(entered && id_before.is_some());
    assert_eq!(id_before, id_after);
    assert_eq!(before.0, occupied.0);
    assert_eq!(occupied.2, 1);
    assert!(!first_polled.load(Ordering::Acquire));
    assert_eq!(first, DrainObservation::Deadline);
    assert_eq!(joined, DrainObservation::RegistrationFailure);
    assert_eq!(late, DrainObservation::Deadline);
    assert_eq!(held.retained_items, 1);
    assert_eq!(after.retained_items, 0);
    assert_eq!(control.drops.load(Ordering::Acquire), 1);
    assert!(control.identity_seen.load(Ordering::Acquire));
}

#[tokio::test]
async fn actual_cancelled_send_joins_before_its_arbitrary_future_destructor_and_capacity_return() {
    let cleanup = Instant::now() + Duration::from_secs(2);
    let original_deadline = Instant::now() + Duration::from_millis(40);
    let (mut owner, port, supervisor) = setup();
    let control = DropControl::new();
    let raw = payload(&control, false);
    let slot = port.reserve_original_slot(1).unwrap();
    let disposition = slot.commit(original_charge(&supervisor), async move {
        let _raw = raw;
        std::future::pending::<()>().await;
    });
    let actual_id = send_id(&owner);
    owner.request_abort();
    let entered = tokio::time::timeout_at(original_deadline.into(), control.entered.notified())
        .await
        .is_ok();
    let id_before = cleanup_id(&owner);
    let first = owner.drain_until(original_deadline).await;
    let id_after = cleanup_id(&owner);
    let held = supervisor.snapshot();
    control.release();
    let joined = owner.drain_until(cleanup).await;
    let after = supervisor.snapshot();
    let failure = owner.failure_snapshot();
    assert_eq!(disposition, OriginalDisposition::Started);
    assert!(entered && id_before.is_some());
    assert_eq!(id_before, id_after);
    assert_eq!(first, DrainObservation::Deadline);
    assert_eq!(joined, DrainObservation::OriginalSendFailure);
    assert_eq!(
        failure.original_send,
        Some(JoinFacts {
            id: actual_id.unwrap(),
            panic: false,
            cancelled: true
        })
    );
    assert_eq!(held.retained_items, 1);
    assert_eq!(after.retained_items, 0);
    assert_eq!(control.drops.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn actual_panic_payload_successor_keeps_same_slot_backing_and_finite_failure() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut owner, port, supervisor) = setup();
    let control = DropControl::new();
    let before = storage(&owner);
    let raw = payload(&control, true);
    let slot = port.reserve_original_slot(1).unwrap();
    let disposition = slot.commit(original_charge(&supervisor), async move {
        std::panic::panic_any(raw);
    });
    let entered = tokio::time::timeout_at(deadline.into(), control.entered.notified())
        .await
        .is_ok();
    let held = supervisor.snapshot();
    let occupied = storage(&owner);
    control.release();
    let joined = owner.drain_until(deadline).await;
    let failure = owner.failure_snapshot();
    let after = supervisor.snapshot();
    assert_eq!(disposition, OriginalDisposition::Started);
    assert!(entered);
    assert_eq!(before.0, occupied.0);
    assert_eq!(occupied.2, 1);
    assert_eq!(joined, DrainObservation::OriginalSendFailure);
    assert_eq!(failure.cleanup_failures, 1);
    assert_eq!(held.retained_items, 1);
    assert_eq!(after.retained_items, 0);
    assert_eq!(control.drops.load(Ordering::Acquire), 2);
    assert!(control.identity_seen.load(Ordering::Acquire));
    assert!(!format!("{failure:?}").contains("RAW_CAUSE_FORMAT_CANARY"));
}

#[tokio::test]
async fn never_polled_original_reaper_exit_only_closes_admission_and_actual_join_is_failure() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut owner, port, supervisor) = setup();
    let actual_reaper = owner.reaper.as_ref().unwrap().id();
    owner.reaper.as_ref().unwrap().abort();
    let exited = tokio::time::timeout_at(deadline.into(), async {
        loop {
            if owner.core.state.lock().unwrap().closed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let failure = match port.reserve_original_slot(1) {
        Err(failure) => Some(failure),
        Ok(slot) => {
            drop(slot);
            None
        }
    };
    let before_joined = owner.is_joined();
    let drained = owner.drain_until(deadline).await;
    let snapshot = owner.failure_snapshot();
    assert!(exited);
    assert!(!before_joined);
    assert_eq!(failure, Some(RegistrationFailure::Closed));
    assert_eq!(drained, DrainObservation::OriginalReaperFailure);
    assert_eq!(
        snapshot.original_reaper,
        Some(JoinFacts {
            id: actual_reaper,
            panic: false,
            cancelled: true
        })
    );
    assert!(owner.is_joined());
    assert_eq!(supervisor.snapshot().retained_items, 0);
}
#[test]
fn actual_original_role_joins_before_original_runtime_teardown() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (drained, joined, retained, records) = runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_secs(2);
        let (mut owner, port, supervisor) = setup();
        let charge = original_charge(&supervisor);
        let slot = port.reserve_original_slot(1).unwrap();
        slot.commit(charge, async {});
        let observation = owner.drain_until(deadline).await;
        (
            observation,
            owner.is_joined(),
            supervisor.snapshot().retained_items,
            storage(&owner).2,
        )
    });
    // Destroy the actual scheduling runtime only AFTER its original role
    // task and protected destructor joins, not before constructing a sink.
    runtime.shutdown_timeout(Duration::from_secs(2));
    assert_eq!(drained, DrainObservation::Complete);
    assert!(joined);
    assert_eq!(retained, 0);
    assert_eq!(records, 0);
}

struct ReadyOriginal {
    _raw: RawPayload,
}
impl Future for ReadyOriginal {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
#[tokio::test]
async fn successful_original_poll_with_actual_destructor_panic_can_never_drain_complete() {
    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut owner, port, supervisor) = setup();
    let control = DropControl::new();
    let charge = original_charge(&supervisor);
    let slot = port.reserve_original_slot(1).unwrap();
    let disposition = slot.commit(
        charge,
        ReadyOriginal {
            _raw: payload(&control, true),
        },
    );
    let entered = tokio::time::timeout_at(deadline.into(), control.entered.notified())
        .await
        .is_ok();
    let held = supervisor.snapshot();
    control.release();
    let drained = owner.drain_until(deadline).await;
    let retry = owner.drain_until(deadline).await;
    let failure = owner.failure_snapshot();
    let after = supervisor.snapshot();
    assert_eq!(disposition, OriginalDisposition::Started);
    assert!(entered);
    assert_eq!(failure.original_send, None); // The actual original poll returned ReadyOk.
    assert_eq!(failure.cleanup_failures, 1);
    assert_eq!(drained, DrainObservation::OriginalCleanupFailure);
    assert_eq!(retry, drained);
    assert_eq!(held.retained_items, 1);
    assert_eq!(after.retained_items, 0);
    assert_eq!(control.drops.load(Ordering::Acquire), 2);
    assert!(control.identity_seen.load(Ordering::Acquire));
    assert!(!format!("{failure:?}").contains("RAW_CAUSE_FORMAT_CANARY"));
}
