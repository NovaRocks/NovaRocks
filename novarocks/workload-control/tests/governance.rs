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

use novarocks_workload_control::*;
use std::{
    future::Future,
    pin::Pin,
    sync::Barrier,
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::time::Instant;

fn config() -> WorkloadConfig {
    WorkloadConfig {
        root_limit: 8,
        business_limit: 8,
        preparation_limit: 2,
        execution_limit: 2,
        executions_per_root: 4,
        waiting_limit: 4,
        waiting_bytes: 256,
        capacity_wait_timeout: Duration::from_secs(30),
        restarts_per_work: 2,
        old_attempts_per_work: 2,
        old_attempts_limit: 2,
        unknown_creates_limit: 2,
        scope_records_limit: 32,
        obligation_records_limit: 16,
        control_inflight_limit: 2,
        control_ready_limit: 2,
    }
}

fn resources() -> ResourceConfig {
    ResourceConfig {
        total_bytes: 128,
        control_bytes: 16,
        per_scope_bytes: 112,
    }
}
fn control() -> WorkloadControl {
    let control = WorkloadControl::try_new(config(), resources()).unwrap();
    control.mark_ready().unwrap();
    control
}
fn root(control: &WorkloadControl, class: WorkClass) -> RootWork {
    control.mark_ready().unwrap();
    control.try_begin_root(WorkRequest::new(class)).unwrap()
}
fn child(scope: &WorkScope) -> WorkOwner {
    scope.child(WorkRequest::new(WorkClass::Query)).unwrap()
}
fn request(scope: &WorkScope) -> StageAdmission {
    scope
        .acquire(StageRequest {
            stage: Stage::Execution,
            retained_bytes: 32,
        })
        .unwrap()
}
fn key(id: u8) -> ObligationKey {
    ObligationKey([id; 32])
}
fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}
fn drain_control(control: &WorkloadControl) {
    while let Some(work) = control.next_control() {
        work.acknowledge();
    }
}

#[test]
fn invalid_and_excessive_configuration_is_rejected() {
    let mut invalid = config();
    invalid.root_limit = 0;
    assert!(matches!(
        WorkloadControl::try_new(invalid, resources()),
        Err(WorkError::InvalidConfig(_))
    ));
    let mut invalid = config();
    invalid.scope_records_limit = 7;
    assert!(invalid.validate().is_err());
    let mut invalid = config();
    invalid.waiting_limit = usize::MAX;
    assert!(invalid.validate().is_err());
    let mut invalid = config();
    invalid.waiting_bytes = u64::MAX;
    assert!(invalid.validate().is_err());
    let mut invalid = config();
    invalid.old_attempts_per_work = 3;
    assert!(invalid.validate().is_err());
    assert_eq!(
        WorkloadConfig::default().capacity_wait_timeout,
        Duration::from_secs(30)
    );
    for timeout in [Duration::ZERO, Duration::from_secs(31), Duration::MAX] {
        let mut invalid = config();
        invalid.capacity_wait_timeout = timeout;
        assert_eq!(
            invalid.validate(),
            Err(WorkError::InvalidConfig("capacity_wait_timeout"))
        );
    }
    for invalid in [
        ResourceConfig {
            total_bytes: 0,
            control_bytes: 1,
            per_scope_bytes: 1,
        },
        ResourceConfig {
            total_bytes: 10,
            control_bytes: 10,
            per_scope_bytes: 10,
        },
        ResourceConfig {
            total_bytes: u64::MAX,
            control_bytes: 1,
            per_scope_bytes: 1,
        },
        ResourceConfig {
            total_bytes: 10,
            control_bytes: 1,
            per_scope_bytes: 11,
        },
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn construction_does_not_admit_work_and_closed_roles_never_reopen() {
    let control = WorkloadControl::try_new(config(), resources()).unwrap();
    assert_eq!(control.snapshot().serving, ServingState::Initializing);
    assert!(matches!(
        control.try_begin_root(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::NotReady)
    ));
    control.mark_ready().unwrap();
    control.mark_ready().unwrap();
    assert_eq!(control.snapshot().serving, ServingState::Ready);
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    control.close_admission();
    assert_eq!(control.snapshot().serving, ServingState::Closed);
    assert_eq!(control.mark_ready(), Err(WorkError::Closed));
    work.owner.complete();
    work.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[tokio::test]
async fn one_mv_four_queries_charge_one_business_and_four_independent_executions() {
    let control = control();
    let mv = root(&control, WorkClass::MaterializedView);
    let scope = mv.owner.scope();
    let children = (0..4).map(|_| child(&scope)).collect::<Vec<_>>();
    let aliases = children
        .iter()
        .flat_map(|owner| [owner.scope(), owner.scope()])
        .collect::<Vec<_>>();
    let first = request(&aliases[0]).await.unwrap();
    let second = request(&aliases[2]).await.unwrap();
    let mut third = request(&aliases[4]);
    let mut fourth = request(&aliases[6]);
    assert!(poll(&mut third).is_pending());
    assert!(poll(&mut fourth).is_pending());
    let snapshot = control.snapshot();
    assert_eq!(
        (
            snapshot.root_responsibilities,
            snapshot.businesses,
            snapshot.execution
        ),
        (1, 1, 2)
    );
    assert_eq!(snapshot.scopes.len(), 5);
    drop(first);
    let third = third.await.unwrap();
    assert!(poll(&mut fourth).is_pending());
    drop(second);
    let fourth = fourth.await.unwrap();
    mv.owner.complete();
    mv.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    drop((third, fourth));
    for child in children {
        child.complete();
    }
    scope.wait_released().await;
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert_eq!(scope.check(), Err(WorkError::Released));
}

#[tokio::test]
async fn parent_waiting_for_children_holds_no_idle_stage_capacity() {
    let control = control();
    let mv = root(&control, WorkClass::MaterializedView);
    let scope = mv.owner.scope();
    let preparation = scope.try_acquire(Stage::Preparation).unwrap();
    let child = child(&scope);
    preparation.release();
    let run = child.scope().try_acquire(Stage::Execution).unwrap();
    mv.owner.complete();
    mv.business.release();
    let snapshot = control.snapshot();
    assert_eq!(snapshot.preparation, 0);
    assert_eq!(snapshot.execution, 1);
    assert_eq!(snapshot.root_responsibilities, 1);
    run.release();
    child.complete();
    scope.wait_released().await;
}

#[tokio::test]
async fn round_robin_keeps_root_fifo_without_starving_other_businesses() {
    let mut limits = config();
    limits.execution_limit = 1;
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let a = root(&control, WorkClass::MaterializedView);
    let b = root(&control, WorkClass::Query);
    let a1 = child(&a.owner.scope());
    let a2 = child(&a.owner.scope());
    let a3 = child(&a.owner.scope());
    let running = a.owner.scope().try_acquire(Stage::Execution).unwrap();
    let first = request(&a1.scope());
    let mut second = request(&a2.scope());
    let mut third = request(&a3.scope());
    let mut other = request(&b.owner.scope());
    drop(running);
    let first = first.await.unwrap();
    assert!(poll(&mut other).is_pending());
    drop(first);
    let other = other.await.unwrap();
    assert!(poll(&mut second).is_pending());
    drop(other);
    let second = second.await.unwrap();
    assert!(poll(&mut third).is_pending());
    drop(second);
    drop(third.await.unwrap());
}

#[tokio::test]
async fn an_exhausted_root_does_not_block_another_runnable_root() {
    let mut limits = config();
    limits.executions_per_root = 1;
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let a = root(&control, WorkClass::MaterializedView);
    let a1 = child(&a.owner.scope());
    let b = root(&control, WorkClass::Query);
    let active = a.owner.scope().try_acquire(Stage::Execution).unwrap();
    let mut waiting = request(&a1.scope());
    assert!(poll(&mut waiting).is_pending());
    let other = request(&b.owner.scope()).await.unwrap();
    assert!(poll(&mut waiting).is_pending());
    assert_eq!(control.snapshot().execution, 2);
    drop((active, other));
    drop(waiting.await.unwrap());
}

#[tokio::test]
async fn object_lock_contention_returns_stage_before_requeue() {
    let mut limits = config();
    limits.execution_limit = 1;
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let a = root(&control, WorkClass::TableMaintenance);
    let b = root(&control, WorkClass::Query);
    let object_lock = tokio::sync::Mutex::new(());
    let object_guard = object_lock.lock().await;
    let admitted = request(&a.owner.scope()).await.unwrap();
    let other = request(&b.owner.scope());
    assert!(object_lock.try_lock().is_err());
    drop(admitted);
    let mut requeued = request(&a.owner.scope());
    let other = other.await.unwrap();
    assert!(poll(&mut requeued).is_pending());
    drop(object_guard);
    drop(other);
    let admitted = requeued.await.unwrap();
    let object_guard = object_lock.try_lock().unwrap();
    drop((object_guard, admitted));
}

#[tokio::test(start_paused = true)]
async fn deadlines_are_inherited_and_wake_waiters_without_releasing_running_work() {
    let control = control();
    let now = Instant::now();
    let parent = control
        .try_begin_root(WorkRequest {
            class: WorkClass::MaterializedView,
            deadline: Some(now + Duration::from_secs(5)),
        })
        .unwrap();
    let child = parent
        .owner
        .scope()
        .child(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(now + Duration::from_secs(30)),
        })
        .unwrap();
    let view = child.scope().cancellation().unwrap();
    assert_eq!(view.deadline(), Some(now + Duration::from_secs(5)));
    let running = child
        .scope()
        .register_obligation(key(1), ObligationKind::RunningWork)
        .unwrap();
    let blocker = root(&control, WorkClass::Query);
    let b1 = blocker.owner.scope().try_acquire(Stage::Execution).unwrap();
    let b2 = blocker
        .owner
        .scope()
        .child(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let b2_run = b2.scope().try_acquire(Stage::Execution).unwrap();
    let mut waiting = request(&child.scope());
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        waiting.await.err(),
        Some(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    );
    control.expire_deadlines();
    let before = control.snapshot();
    assert_eq!(before.execution, 2);
    assert!(before.scopes.iter().any(|node| node.obligations.len() == 1));
    parent.owner.complete();
    parent.business.release();
    child.complete();
    drain_control(&control);
    assert_eq!(control.snapshot().root_responsibilities, 2);
    assert!(running.resolve());
    assert_eq!(control.snapshot().root_responsibilities, 1);
    drop((b1, b2_run));
}

#[tokio::test]
async fn cancellation_is_first_wins_downward_and_has_no_lost_subscription() {
    let control = control();
    let parent = root(&control, WorkClass::MaterializedView);
    let a = child(&parent.owner.scope());
    let b = child(&parent.owner.scope());
    a.cancel(CancellationReason::Requested);
    parent.owner.cancel(CancellationReason::ServerShutdown);
    assert_eq!(
        a.scope().cancellation().unwrap().cancelled().await,
        CancellationReason::Requested
    );
    assert_eq!(
        b.scope().cancellation().unwrap().cancelled().await,
        CancellationReason::ServerShutdown
    );
    assert!(matches!(
        parent
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::Cancelled(_))
    ));
    assert!(matches!(
        a.scope().try_acquire(Stage::Preparation),
        Err(WorkError::Cancelled(_))
    ));
}

#[tokio::test]
async fn waiting_entries_and_bytes_are_bounded_and_drop_returns_unreceived_grants() {
    let control = control();
    let mv = root(&control, WorkClass::MaterializedView);
    let children = (0..6).map(|_| child(&mv.owner.scope())).collect::<Vec<_>>();
    let pending = children[..4]
        .iter()
        .map(|child| request(&child.scope()))
        .collect::<Vec<_>>();
    assert_eq!(control.snapshot().execution, 2);
    assert!(matches!(
        children[4].scope().acquire(StageRequest {
            stage: Stage::Execution,
            retained_bytes: 1
        }),
        Err(WorkError::Capacity("waiting entries"))
    ));
    drop(pending);
    assert_eq!(control.snapshot().execution, 0);
    assert_eq!(control.snapshot().waiting_bytes, 0);
    assert!(matches!(
        children[4].scope().acquire(StageRequest {
            stage: Stage::Execution,
            retained_bytes: 257
        }),
        Err(WorkError::Capacity("waiting bytes"))
    ));
    let mut pending = request(&children[5].scope());
    children[5].cancel(CancellationReason::Requested);
    assert!(matches!(
        poll(&mut pending),
        Poll::Ready(Err(WorkError::Cancelled(_)))
    ));
    assert_eq!(control.snapshot().execution, 0);
}

#[tokio::test]
async fn grant_receipt_racing_cancellation_returns_capacity_exactly_once() {
    let control = control();
    for _ in 0..64 {
        let work = root(&control, WorkClass::Query);
        let mut admission = request(&work.owner.scope());
        let barrier = Barrier::new(2);
        std::thread::scope(|threads| {
            threads.spawn(|| {
                barrier.wait();
                work.owner.cancel(CancellationReason::Requested);
            });
            barrier.wait();
            match poll(&mut admission) {
                Poll::Ready(Ok(permit)) => drop(permit),
                Poll::Ready(Err(WorkError::Cancelled(_))) => {}
                other => panic!(
                    "Unexpected admission state: {}",
                    if other.is_pending() {
                        "pending"
                    } else {
                        "error"
                    }
                ),
            }
        });
        assert_eq!(control.snapshot().execution, 0);
        work.owner.complete();
        work.business.release();
        drain_control(&control);
    }
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn stage_capability_and_resource_authority_are_bound_to_the_exact_scope() {
    let control = control();
    let foreign = WorkloadControl::try_new(config(), resources()).unwrap();
    let a = root(&control, WorkClass::Query);
    let b = root(&control, WorkClass::Query);
    let c = root(&foreign, WorkClass::Query);
    let permit = a.owner.scope().try_acquire(Stage::Preparation).unwrap();
    assert_eq!(
        permit.check(&b.owner.scope(), Stage::Preparation),
        Err(WorkError::ForeignAuthority)
    );
    assert_eq!(
        permit.check(&a.owner.scope(), Stage::Execution),
        Err(WorkError::Conflict)
    );
    assert!(matches!(
        a.owner.scope().try_acquire(Stage::Preparation),
        Err(WorkError::AlreadyAdmitted)
    ));
    assert!(matches!(
        control
            .resources()
            .reserve(&c.owner.scope(), 1, ResourceClass::Data),
        Err(WorkError::ForeignAuthority)
    ));
}

#[test]
fn reservation_and_usage_share_one_charge_and_slices_keep_the_original_allocation() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let scope = work.owner.scope();
    let resources = control.resources();
    let mut reservation = resources.reserve(&scope, 100, ResourceClass::Data).unwrap();
    let allocation = reservation.charge(80).unwrap();
    let slice = allocation.clone();
    let snapshot = resources.snapshot();
    assert_eq!(
        (
            snapshot.data_reserved_bytes,
            snapshot.data_used_bytes,
            snapshot.held_bytes()
        ),
        (20, 80, 100)
    );
    assert!(matches!(reservation.grow(13), Err(WorkError::Capacity(_))));
    reservation.grow(12).unwrap();
    assert_eq!(resources.snapshot().held_bytes(), 112);
    drop(reservation);
    assert_eq!(resources.snapshot().held_bytes(), 80);
    work.owner.complete();
    work.business.release();
    drop(allocation);
    assert_eq!(resources.snapshot().data_used_bytes, 80);
    assert_eq!(control.snapshot().root_responsibilities, 1);
    drop(slice);
    assert_eq!(resources.snapshot().held_bytes(), 0);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn commit_wait_releases_execution_but_keeps_output_and_business_responsibility() {
    let control = control();
    let work = root(&control, WorkClass::MaterializedView);
    let query = child(&work.owner.scope());
    let execution = query.scope().try_acquire(Stage::Execution).unwrap();
    let mut reserve = control
        .resources()
        .reserve(&query.scope(), 64, ResourceClass::Data)
        .unwrap();
    let output = reserve.charge(48).unwrap();
    drop((execution, reserve));
    let commit = work
        .owner
        .scope()
        .register_obligation(key(1), ObligationKind::ExternalCompletion)
        .unwrap();
    query.complete();
    let snapshot = control.snapshot();
    assert_eq!((snapshot.execution, snapshot.businesses), (0, 1));
    assert_eq!(control.resources().snapshot().data_used_bytes, 48);
    drop(output);
    assert_eq!(control.snapshot().scopes.len(), 1);
    work.owner.complete();
    work.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    commit.resolve();
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn allocation_handoff_is_atomic_and_foreign_processes_cannot_receive_it() {
    let control = control();
    let other_control = WorkloadControl::try_new(config(), resources()).unwrap();
    let a = root(&control, WorkClass::Query);
    let b = root(&control, WorkClass::Query);
    let foreign = root(&other_control, WorkClass::Query);
    let mut reservation = control
        .resources()
        .reserve(&a.owner.scope(), 80, ResourceClass::Data)
        .unwrap();
    let allocation = reservation.charge(80).unwrap();
    drop(reservation);
    a.owner.complete();
    a.business.release();
    assert_eq!(
        allocation.transfer_to(&foreign.owner.scope()),
        Err(WorkError::ForeignAuthority)
    );
    allocation.transfer_to(&b.owner.scope()).unwrap();
    assert_eq!(control.resources().snapshot().held_bytes(), 80);
    assert_eq!(control.snapshot().root_responsibilities, 1);
    b.owner.complete();
    b.business.release();
    drop(allocation);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

fn result_credit_at(
    control: &WorkloadControl,
    scope: &WorkScope,
    stage: ResultCreditStage,
) -> ResultCredit {
    let authority = control.resources();
    let credit = authority.reserve_result_credit(scope, 64).unwrap();
    if stage == ResultCreditStage::ReservedBeforeFetch {
        return credit;
    }
    let credit = credit.begin_fetch().unwrap();
    if stage == ResultCreditStage::InFlightRaw {
        return credit;
    }
    let credit = credit.retain_raw(32).unwrap();
    if stage == ResultCreditStage::RawRetained {
        return credit;
    }
    let credit = credit.reserve_decode(&authority, 48).unwrap();
    if stage == ResultCreditStage::DecodeReserved {
        return credit;
    }
    let credit = credit.queue_decoded(40).unwrap();
    if stage == ResultCreditStage::DecodedQueued {
        return credit;
    }
    let credit = credit.reserve_protocol(&authority, 48).unwrap();
    if stage == ResultCreditStage::ProtocolReserved {
        return credit;
    }
    credit.begin_protocol_write(32).unwrap()
}

fn small_decoded_credit(control: &WorkloadControl, scope: &WorkScope) -> ResultCredit {
    let authority = control.resources();
    authority
        .reserve_result_credit(scope, 24)
        .unwrap()
        .begin_fetch()
        .unwrap()
        .retain_raw(20)
        .unwrap()
        .reserve_decode(&authority, 24)
        .unwrap()
        .queue_decoded(20)
        .unwrap()
}

#[test]
fn result_credit_transitions_share_the_data_ledger_and_hold_slow_output() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let scope = work.owner.scope();
    let authority = control.resources();

    let credit = authority.reserve_result_credit(&scope, 80).unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(
        (snapshot.data_reserved_bytes, snapshot.data_used_bytes),
        (80, 0)
    );
    assert_eq!(snapshot.result_credit.reserved_before_fetch_bytes, 80);
    assert_eq!(snapshot.result_credit.held_bytes(), snapshot.held_bytes());

    let credit = credit.begin_fetch().unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(snapshot.result_credit.in_flight_raw_bytes, 80);
    assert_eq!(
        (snapshot.data_reserved_bytes, snapshot.data_used_bytes),
        (80, 0)
    );

    let credit = credit.retain_raw(48).unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(snapshot.result_credit.raw_retained_bytes, 48);
    assert_eq!(
        (snapshot.data_reserved_bytes, snapshot.data_used_bytes),
        (0, 48)
    );

    let credit = credit.reserve_decode(&authority, 56).unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(snapshot.result_credit.decode_reserved_bytes, 104);
    assert_eq!(
        (snapshot.data_reserved_bytes, snapshot.data_used_bytes),
        (56, 48)
    );
    assert_eq!(snapshot.result_credit.held_bytes(), snapshot.held_bytes());

    let credit = credit.queue_decoded(40).unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(snapshot.result_credit.decoded_queued_bytes, 40);
    assert_eq!(
        (snapshot.data_reserved_bytes, snapshot.data_used_bytes),
        (0, 40)
    );

    let credit = credit.reserve_protocol(&authority, 24).unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(snapshot.result_credit.protocol_reserved_bytes, 64);
    assert_eq!(
        (snapshot.data_reserved_bytes, snapshot.data_used_bytes),
        (24, 40)
    );

    let credit = credit.begin_protocol_write(16).unwrap();
    assert_eq!(
        authority.snapshot().result_credit.protocol_writing_bytes,
        56
    );
    work.owner.complete();
    work.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert_eq!(authority.snapshot().data_used_bytes, 56);
    credit.consume().unwrap();
    assert_eq!(authority.snapshot().held_bytes(), 0);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[tokio::test]
async fn protocol_result_credit_waiters_share_a_scope_and_grant_fifo() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let blocker_work = root(&control, WorkClass::Query);
    let authority = control.resources();
    let first = small_decoded_credit(&control, &work.owner.scope());
    let second = small_decoded_credit(&control, &work.owner.scope());
    let mut blocker = authority
        .reserve(&blocker_work.owner.scope(), 72, ResourceClass::Data)
        .unwrap();
    let mut first = Box::pin(first.reserve_protocol_when_available(&authority, 16));
    let mut second = Box::pin(second.reserve_protocol_when_available(&authority, 16));
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    assert_eq!(control.snapshot().resource_waiters, 2);

    blocker.release_unused(16).unwrap();
    assert!(poll(&mut second).is_pending());
    let first = first.await.unwrap();
    assert_eq!(first.stage(), ResultCreditStage::ProtocolReserved);
    assert_eq!(control.snapshot().resource_waiters, 1);
    drop(first);

    let second = second.await.unwrap();
    assert_eq!(second.stage(), ResultCreditStage::ProtocolReserved);
    assert_eq!(control.snapshot().resource_waiters, 0);
    drop((second, blocker));
    assert_eq!(authority.snapshot().held_bytes(), 0);
    drop((work, blocker_work));
}

#[test]
fn dropping_result_credit_at_every_stage_returns_capacity() {
    for stage in [
        ResultCreditStage::ReservedBeforeFetch,
        ResultCreditStage::InFlightRaw,
        ResultCreditStage::RawRetained,
        ResultCreditStage::DecodeReserved,
        ResultCreditStage::DecodedQueued,
        ResultCreditStage::ProtocolReserved,
        ResultCreditStage::ProtocolWriting,
    ] {
        let control = control();
        let work = root(&control, WorkClass::Query);
        let credit = result_credit_at(&control, &work.owner.scope(), stage);
        assert_eq!(credit.stage(), stage);
        assert_ne!(control.resources().snapshot().held_bytes(), 0);
        drop(credit);
        assert_eq!(control.resources().snapshot().held_bytes(), 0);
        work.owner.complete();
        work.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }
}

#[test]
fn result_credit_limits_foreign_authorities_and_invalid_transitions_fail_closed() {
    let local = control();
    let foreign = control();
    let work = root(&local, WorkClass::Query);
    let foreign_work = root(&foreign, WorkClass::Query);
    let authority = local.resources();
    assert!(matches!(
        authority.reserve_result_credit(&foreign_work.owner.scope(), 1),
        Err(WorkError::ForeignAuthority)
    ));
    assert!(matches!(
        authority.reserve_result_credit(&work.owner.scope(), 0),
        Err(WorkError::Capacity(_))
    ));

    let credit = authority
        .reserve_result_credit(&work.owner.scope(), 64)
        .unwrap()
        .begin_fetch()
        .unwrap()
        .retain_raw(32)
        .unwrap();
    let rejection = match credit.reserve_decode(&foreign.resources(), 16) {
        Ok(_) => panic!("foreign authority must be rejected"),
        Err(rejection) => rejection,
    };
    assert!(matches!(rejection.error(), WorkError::ForeignAuthority));
    let (_, credit) = rejection.into_parts();
    assert_eq!(authority.snapshot().held_bytes(), 32);
    drop(credit);
    assert_eq!(authority.snapshot().held_bytes(), 0);

    let credit = authority
        .reserve_result_credit(&work.owner.scope(), 16)
        .unwrap();
    let rejection = match credit.begin_protocol_write(1) {
        Ok(_) => panic!("invalid protocol transition must be rejected"),
        Err(rejection) => rejection,
    };
    assert!(matches!(
        rejection.error(),
        WorkError::InvalidResultCreditTransition {
            from: ResultCreditStage::ReservedBeforeFetch,
            requested: ResultCreditStage::ProtocolWriting,
        }
    ));
    let (_, credit) = rejection.into_parts();
    assert_eq!(authority.snapshot().held_bytes(), 16);
    drop(credit);
    assert_eq!(authority.snapshot().held_bytes(), 0);

    let credit = authority
        .reserve_result_credit(&work.owner.scope(), 16)
        .unwrap()
        .begin_fetch()
        .unwrap()
        .retain_raw(8)
        .unwrap()
        .reserve_decode(&authority, 8)
        .unwrap()
        .queue_decoded(8)
        .unwrap()
        .reserve_protocol(&authority, 4)
        .unwrap();
    let rejection = match credit.begin_protocol_write(5) {
        Ok(_) => panic!("protocol bytes beyond the reservation must be rejected"),
        Err(rejection) => rejection,
    };
    assert!(matches!(rejection.error(), WorkError::Capacity(_)));
    let (_, credit) = rejection.into_parts();
    assert_eq!(credit.stage(), ResultCreditStage::ProtocolReserved);
    assert_eq!(authority.snapshot().held_bytes(), 12);
    drop(credit);
    assert_eq!(authority.snapshot().held_bytes(), 0);

    let credit = authority
        .reserve_result_credit(&work.owner.scope(), 16)
        .unwrap()
        .begin_fetch()
        .unwrap()
        .retain_raw(8)
        .unwrap();
    let rejection = match credit.reserve_decode(&authority, 0) {
        Ok(_) => panic!("zero-byte decode reservation must be rejected"),
        Err(rejection) => rejection,
    };
    assert!(matches!(rejection.error(), WorkError::Capacity(_)));
    let (_, credit) = rejection.into_parts();
    assert_eq!(authority.snapshot().held_bytes(), 8);
    drop(credit);
    assert_eq!(authority.snapshot().held_bytes(), 0);

    let ordinary = authority
        .reserve(&work.owner.scope(), 1, ResourceClass::Data)
        .unwrap();
    let before = authority.snapshot();
    assert!(matches!(
        authority.reserve_result_credit(&work.owner.scope(), u64::MAX),
        Err(WorkError::ArithmeticOverflow)
    ));
    assert_eq!(authority.snapshot(), before);
    drop(ordinary);
}

#[test]
fn result_credit_enforces_process_and_scope_limits_under_competition() {
    let control = control();
    let a = root(&control, WorkClass::Query);
    let b = root(&control, WorkClass::Query);
    let authority = control.resources();
    let first = authority
        .reserve_result_credit(&a.owner.scope(), 80)
        .unwrap();
    assert!(matches!(
        authority.reserve_result_credit(&b.owner.scope(), 33),
        Err(WorkError::Capacity("local allocation bytes"))
    ));
    let second = authority
        .reserve_result_credit(&b.owner.scope(), 32)
        .unwrap();
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop((first, second));

    let raw = authority
        .reserve_result_credit(&a.owner.scope(), 80)
        .unwrap()
        .begin_fetch()
        .unwrap()
        .retain_raw(80)
        .unwrap();
    let rejection = match raw.reserve_decode(&authority, 33) {
        Ok(_) => panic!("scope capacity overflow must be rejected"),
        Err(rejection) => rejection,
    };
    assert!(matches!(
        rejection.error(),
        WorkError::Capacity("scope allocation bytes")
    ));
    let (_, raw) = rejection.into_parts();
    assert_eq!(authority.snapshot().held_bytes(), 80);
    drop(raw);
    assert_eq!(authority.snapshot().held_bytes(), 0);

    let start = std::sync::Arc::new(Barrier::new(2));
    let attempted = std::sync::Arc::new(Barrier::new(2));
    let winners = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut joins = Vec::new();
    for scope in [a.owner.scope(), b.owner.scope()] {
        let authority = authority.clone();
        let start = start.clone();
        let attempted = attempted.clone();
        let winners = winners.clone();
        joins.push(std::thread::spawn(move || {
            start.wait();
            let credit = authority.reserve_result_credit(&scope, 80).ok();
            if credit.is_some() {
                winners.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            attempted.wait();
            drop(credit);
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    assert_eq!(winners.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(authority.snapshot().held_bytes(), 0);
}

#[tokio::test]
async fn result_credit_wait_inherits_scope_cancellation() {
    let control = control();
    let holder = root(&control, WorkClass::Query);
    let waiting_work = root(&control, WorkClass::Query);
    let authority = control.resources();
    let held = authority
        .reserve_result_credit(&holder.owner.scope(), 112)
        .unwrap();
    let waiting_scope = waiting_work.owner.scope();
    let mut waiting = Box::pin(authority.wait_for_result_credit(&waiting_scope, 16));
    assert!(poll(&mut waiting).is_pending());
    waiting_work.owner.cancel(CancellationReason::Requested);
    assert!(matches!(waiting.await, Err(WorkError::Cancelled(_))));
    assert_eq!(control.snapshot().resource_waiters, 0);
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop(held);
}

#[tokio::test]
async fn data_saturation_and_cancellation_preserve_control_progress_and_memory() {
    let control = control();
    let works = (0..5)
        .map(|_| root(&control, WorkClass::Query))
        .collect::<Vec<_>>();
    let mut reserve = control
        .resources()
        .reserve(&works[0].owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let data = reserve.charge(112).unwrap();
    for work in &works {
        work.owner.cancel(CancellationReason::Requested);
    }
    assert_eq!(control.snapshot().control_ready, 2);
    assert!(matches!(
        control
            .resources()
            .reserve(&works[1].owner.scope(), 1, ResourceClass::Data),
        Err(WorkError::Cancelled(_))
    ));
    let mut control_reserve = control
        .resources()
        .reserve(&works[0].owner.scope(), 16, ResourceClass::Control)
        .unwrap();
    let control_bytes = control_reserve.charge(16).unwrap();
    assert_eq!(control.resources().snapshot().held_bytes(), 128);
    let first = control.next_control().unwrap();
    let second = control.next_control().unwrap();
    assert!(control.next_control().is_none());
    let retried = first.scope().id();
    drop(first);
    second.acknowledge();
    let mut seen = Vec::new();
    while let Some(work) = control.next_control() {
        assert!(work.intents().contains(ControlIntent::Cancel));
        seen.push(work.scope().id());
        work.acknowledge();
    }
    assert_eq!(seen.len(), 4);
    assert!(seen.contains(&retried));
    assert_eq!(control.resources().snapshot().data_used_bytes, 112);
    drop((data, reserve, control_bytes, control_reserve));
    for work in works {
        work.owner.complete();
        work.business.release();
    }
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[tokio::test]
async fn allocation_waiter_wakes_on_real_release_and_cancel() {
    let control = control();
    let a = root(&control, WorkClass::Query);
    let b = root(&control, WorkClass::Query);
    let authority = control.resources();
    let reserve = authority
        .reserve(&a.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let scope = b.owner.scope();
    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 16, ResourceClass::Data));
    assert!(poll(&mut waiting).is_pending());
    assert_eq!(control.snapshot().resource_waiters, 1);
    b.owner.cancel(CancellationReason::Requested);
    assert!(matches!(waiting.await, Err(WorkError::Cancelled(_))));
    assert_eq!(control.snapshot().resource_waiters, 0);
    assert_eq!(authority.snapshot().held_bytes(), 112);
    let c = root(&control, WorkClass::Query);
    let scope = c.owner.scope();
    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 16, ResourceClass::Data));
    assert!(poll(&mut waiting).is_pending());
    drop(reserve);
    waiting.await.unwrap();
    assert_eq!(control.snapshot().resource_waiters, 0);
    assert!(matches!(
        authority
            .wait_for_capacity(&scope, 113, ResourceClass::Data)
            .await,
        Err(WorkError::Capacity(_))
    ));
}

#[test]
fn unknown_create_and_old_attempt_bounds_preserve_last_known_usage() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let scope = work.owner.scope();
    let old = scope
        .register_obligation(key(1), ObligationKind::RetiredAttempt)
        .unwrap();
    let duplicate = scope
        .register_obligation(key(1), ObligationKind::RetiredAttempt)
        .unwrap();
    assert_eq!(control.snapshot().old_attempts, 1);
    old.observe_usage(10_000).unwrap();
    old.mark_current_unknown().unwrap();
    let snapshot = control.snapshot();
    let usage = snapshot.scopes[0].obligations[0].usage.as_ref().unwrap();
    assert_eq!(usage.last_known_bytes, 10_000);
    assert!(usage.current_unknown);
    assert_eq!(control.resources().snapshot().held_bytes(), 0);
    let unknown1 = scope
        .register_obligation(key(2), ObligationKind::UnknownCreate)
        .unwrap();
    let unknown2 = scope
        .register_obligation(key(3), ObligationKind::UnknownCreate)
        .unwrap();
    assert!(matches!(
        scope.register_obligation(key(4), ObligationKind::UnknownCreate),
        Err(WorkError::Capacity(_))
    ));
    assert!(matches!(
        scope.register_obligation(key(5), ObligationKind::RetiredAttempt),
        Err(WorkError::Capacity(_))
    ));
    unknown1.resolve();
    let second = scope
        .register_obligation(key(5), ObligationKind::RetiredAttempt)
        .unwrap();
    old.resolve();
    assert!(!duplicate.resolve());
    second.resolve();
    assert!(matches!(
        scope.register_obligation(key(6), ObligationKind::RetiredAttempt),
        Err(WorkError::Capacity("restart allowance"))
    ));
    work.owner.complete();
    work.business.release();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    unknown2.resolve();
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn stale_obligation_handle_cannot_resolve_a_new_generation() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let scope = work.owner.scope();
    let old = scope
        .register_obligation(key(1), ObligationKind::UnknownCreate)
        .unwrap();
    assert!(matches!(
        scope.register_obligation(key(1), ObligationKind::RunningWork),
        Err(WorkError::Conflict)
    ));
    assert!(old.resolve());
    let new = scope
        .register_obligation(key(1), ObligationKind::UnknownCreate)
        .unwrap();
    assert!(!old.resolve());
    assert_eq!(old.observe_usage(0), Err(WorkError::Released));
    assert_eq!(control.snapshot().unknown_creates, 1);
    new.resolve();
}

#[test]
fn owner_drop_or_handoff_does_not_erase_unresolved_responsibility() {
    let control = control();
    let work = root(&control, WorkClass::Statistics);
    let scope = work.owner.scope();
    let unknown = scope
        .register_obligation(key(1), ObligationKind::UnknownCreate)
        .unwrap();
    let owner = work.owner.handoff().unwrap();
    assert_eq!(control.snapshot().scopes[0].handoffs, 1);
    drop(owner);
    work.business.release();
    drain_control(&control);
    assert_eq!(control.snapshot().scopes[0].owner, OwnerState::Orphaned);
    let owner = control.adopt(scope.id()).unwrap();
    assert!(matches!(
        control.adopt(scope.id()),
        Err(WorkError::OwnerStillPresent)
    ));
    owner.complete();
    assert_eq!(control.snapshot().root_responsibilities, 1);
    unknown.resolve();
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn closing_admission_and_releasing_business_do_not_free_root_responsibility() {
    let mut limits = config();
    limits.root_limit = 1;
    limits.business_limit = 1;
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let work = root(&control, WorkClass::Query);
    work.business.release();
    assert_eq!(control.snapshot().businesses, 0);
    assert!(matches!(
        control.try_begin_root(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::Capacity("root responsibilities"))
    ));
    control.close_admission();
    assert!(matches!(
        control.try_begin_root(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::Closed)
    ));
    let child = child(&work.owner.scope());
    child.complete();
    work.owner.complete();
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn an_empty_reservation_has_no_release_claim_and_cannot_revive_completed_work() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let scope = work.owner.scope();
    let mut reserve = control
        .resources()
        .reserve(&scope, 10, ResourceClass::Data)
        .unwrap();
    reserve.release_unused(10).unwrap();
    assert_eq!(control.snapshot().scopes[0].resource_holders, 0);
    reserve.grow(10).unwrap();
    let allocation = reserve.charge(10).unwrap();
    assert_eq!(control.snapshot().scopes[0].resource_holders, 1);
    work.owner.complete();
    work.business.release();
    drop(allocation);
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert_eq!(reserve.grow(1), Err(WorkError::Released));
    reserve.release_unused(0).unwrap();
    drop(reserve);
}

#[test]
fn supervisor_recovers_an_existing_obligation_without_reopening_work() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let scope = work.owner.scope();
    let unknown = scope
        .register_obligation(key(1), ObligationKind::UnknownCreate)
        .unwrap();
    drop(unknown);
    work.owner.complete();
    work.business.release();
    let reporter = control.recover_obligation(scope.id(), key(1)).unwrap();
    assert_eq!(scope.check(), Err(WorkError::Released));
    assert_eq!(control.snapshot().unknown_creates, 1);
    reporter.mark_current_unknown().unwrap();
    reporter.resolve();
    assert_eq!(control.snapshot().root_responsibilities, 0);
    assert!(matches!(
        control.recover_obligation(scope.id(), key(1)),
        Err(WorkError::Released)
    ));
}

#[test]
fn scope_and_obligation_metadata_stay_bounded() {
    let mut limits = config();
    limits.scope_records_limit = 8;
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let work = root(&control, WorkClass::MaterializedView);
    let children = (0..7)
        .map(|_| child(&work.owner.scope()))
        .collect::<Vec<_>>();
    assert!(matches!(
        work.owner.scope().child(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::Capacity("scope records"))
    ));
    let mut records = Vec::new();
    for index in 0..16 {
        records.push(
            work.owner
                .scope()
                .register_obligation(key(index), ObligationKind::ExternalCompletion)
                .unwrap(),
        );
    }
    assert!(matches!(
        work.owner
            .scope()
            .register_obligation(key(20), ObligationKind::RunningWork),
        Err(WorkError::Capacity("obligation records"))
    ));
    records.pop().unwrap().resolve();
    work.owner
        .scope()
        .register_obligation(key(20), ObligationKind::RunningWork)
        .unwrap();
    for child in children {
        child.complete();
    }
    assert_eq!(control.snapshot().scopes.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn stage_capacity_timeout_does_not_cancel_unlimited_logical_work() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let blocker = root(&control, WorkClass::MaterializedView);
    let other = child(&blocker.owner.scope());
    let first = blocker.owner.scope().try_acquire(Stage::Execution).unwrap();
    let second = other.scope().try_acquire(Stage::Execution).unwrap();
    let scope = work.owner.scope();
    let mut waiting = request(&scope);
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(29)).await;
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(waiting.await.err(), Some(WorkError::CapacityWaitTimeout));
    assert_eq!(scope.check(), Ok(()));
    assert_eq!(scope.cancellation().unwrap().reason(), None);
    let snapshot = control.snapshot();
    assert_eq!(
        (
            snapshot.execution,
            snapshot.admission_records,
            snapshot.waiting_bytes
        ),
        (2, 0, 0)
    );
    drop((first, second));
    drop(request(&scope).await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn stage_dispatch_does_not_grant_an_expired_unpolled_request() {
    let mut limits = config();
    limits.capacity_wait_timeout = Duration::from_secs(2);
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let blocker = root(&control, WorkClass::Query);
    let other = child(&blocker.owner.scope());
    let work = root(&control, WorkClass::Query);
    let first = blocker.owner.scope().try_acquire(Stage::Execution).unwrap();
    let second = other.scope().try_acquire(Stage::Execution).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let waiting = request(&work.owner.scope());
    assert_eq!(control.next_deadline(), Some(deadline));
    tokio::time::advance(Duration::from_secs(2)).await;
    drop(first);
    assert_eq!(control.snapshot().execution, 1);
    assert_eq!(waiting.await.err(), Some(WorkError::CapacityWaitTimeout));
    assert_eq!(work.owner.scope().check(), Ok(()));
    drop(second);
}

#[tokio::test(start_paused = true)]
async fn grant_unreceived_at_wait_deadline_is_returned_once() {
    let control = control();
    let work = root(&control, WorkClass::Query);
    let waiting = request(&work.owner.scope());
    assert_eq!(control.snapshot().execution, 1);
    tokio::time::advance(Duration::from_secs(30)).await;
    control.expire_deadlines();
    assert_eq!(control.snapshot().execution, 0);
    assert_eq!(waiting.await.err(), Some(WorkError::CapacityWaitTimeout));
    assert_eq!(control.snapshot().execution, 0);
    assert!(control.next_control().is_none());
    assert_eq!(work.owner.scope().cancellation().unwrap().reason(), None);
}

#[tokio::test(start_paused = true)]
async fn resource_wait_timeout_is_absolute_despite_repeated_capacity_notifications() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let work = root(&control, WorkClass::Query);
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let scope = work.owner.scope();
    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Data));
    assert!(poll(&mut waiting).is_pending());
    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(10)).await;
        let signal = authority
            .reserve(&blocker.owner.scope(), 1, ResourceClass::Control)
            .unwrap();
        drop(signal);
        assert!(poll(&mut waiting).is_pending());
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(waiting.await, Err(WorkError::CapacityWaitTimeout));
    assert_eq!(control.snapshot().resource_waiters, 0);
    assert_eq!(scope.check(), Ok(()));
    assert_eq!(scope.cancellation().unwrap().reason(), None);
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop(memory);
    authority
        .wait_for_capacity(&scope, 1, ResourceClass::Data)
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn resource_wait_uses_the_earlier_inherited_query_deadline() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let work = control
        .try_begin_root(WorkRequest {
            class: WorkClass::MaterializedView,
            deadline: Some(Instant::now() + Duration::from_secs(3)),
        })
        .unwrap();
    let query = work
        .owner
        .scope()
        .child(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(Instant::now() + Duration::from_secs(60)),
        })
        .unwrap();
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let scope = query.scope();
    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Data));
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(
        waiting.await,
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    );
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop(memory);
}

#[tokio::test(start_paused = true)]
async fn cleanup_capacity_wait_is_bounded_even_when_data_work_is_cancelled() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let work = root(&control, WorkClass::Query);
    work.owner.cancel(CancellationReason::Requested);
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 16, ResourceClass::Control)
        .unwrap();
    let scope = work.owner.scope();
    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Control));
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(waiting.await, Err(WorkError::CapacityWaitTimeout));
    assert_eq!(
        scope.cancellation().unwrap().reason(),
        Some(CancellationReason::Requested)
    );
    assert_eq!(authority.snapshot().control_reserved_bytes, 16);
    assert!(control.next_control().is_some());
    drop(memory);
}

#[tokio::test(start_paused = true)]
async fn cleanup_capacity_can_become_available_after_inherited_deadline_expires() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let work = control
        .try_begin_root(WorkRequest {
            class: WorkClass::MaterializedView,
            deadline: Some(Instant::now() + Duration::from_secs(1)),
        })
        .unwrap();
    let query = child(&work.owner.scope());
    let scope = query.scope();
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 16, ResourceClass::Control)
        .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        scope.check(),
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    );

    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Control));
    assert!(poll(&mut waiting).is_pending());
    assert_eq!(control.snapshot().resource_waiters, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(poll(&mut waiting).is_pending());
    drop(memory);
    waiting.await.unwrap();
    assert_eq!(control.snapshot().resource_waiters, 0);
    let cleanup = authority
        .reserve(&scope, 1, ResourceClass::Control)
        .unwrap();
    assert_eq!(authority.snapshot().control_reserved_bytes, 1);
    assert_eq!(
        scope.check(),
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    );
    drop(cleanup);
}

#[tokio::test(start_paused = true)]
async fn cleanup_capacity_uses_its_full_independent_timeout_after_parent_deadline() {
    let mut limits = config();
    limits.capacity_wait_timeout = Duration::from_secs(2);
    let control = WorkloadControl::try_new(limits, resources()).unwrap();
    let blocker = root(&control, WorkClass::Query);
    let work = control
        .try_begin_root(WorkRequest {
            class: WorkClass::MaterializedView,
            deadline: Some(Instant::now() + Duration::from_secs(1)),
        })
        .unwrap();
    let query = child(&work.owner.scope());
    let scope = query.scope();
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 16, ResourceClass::Control)
        .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        scope.check(),
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    );

    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Control));
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(poll(&mut waiting).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(waiting.await, Err(WorkError::CapacityWaitTimeout));
    assert_eq!(control.snapshot().resource_waiters, 0);
    assert_eq!(authority.snapshot().control_reserved_bytes, 16);
    assert_eq!(
        scope.cancellation().unwrap().reason(),
        Some(CancellationReason::DeadlineExceeded)
    );
    drop(memory);
}

#[tokio::test]
async fn resource_wait_registration_is_unique_under_concurrent_polling() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let work = root(&control, WorkClass::Query);
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let scope = work.owner.scope();
    let start = Barrier::new(2);
    let polled = Barrier::new(2);
    let runtime = tokio::runtime::Handle::current();
    let outcomes = std::thread::scope(|threads| {
        let threads = (0..2)
            .map(|_| {
                threads.spawn(|| {
                    let _runtime = runtime.enter();
                    let mut waiting =
                        Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Data));
                    start.wait();
                    let outcome = poll(&mut waiting);
                    // Keep the winning registration alive until both calls have polled.
                    polled.wait();
                    outcome
                })
            })
            .collect::<Vec<_>>();
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.is_pending())
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                Poll::Ready(Err(WorkError::AlreadyWaitingForResource(
                    ResourceClass::Data
                )))
            ))
            .count(),
        1
    );
    assert_eq!(control.snapshot().waiting_records, 0);
    assert_eq!(control.snapshot().resource_waiters, 0);
    assert_eq!(control.snapshot().peak_waiting_records, 1);
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop(memory);
}

#[tokio::test]
async fn distinct_resource_classes_have_distinct_bounded_registrations() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let work = root(&control, WorkClass::Query);
    let authority = control.resources();
    let data = authority
        .reserve(&blocker.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let cleanup = authority
        .reserve(&blocker.owner.scope(), 16, ResourceClass::Control)
        .unwrap();
    let scope = work.owner.scope();
    let mut data_wait = Box::pin(authority.wait_for_capacity(&scope, 100, ResourceClass::Data));
    let mut control_wait =
        Box::pin(authority.wait_for_capacity(&scope, 10, ResourceClass::Control));
    assert!(poll(&mut data_wait).is_pending());
    assert!(poll(&mut control_wait).is_pending());
    let snapshot = control.snapshot();
    assert_eq!(
        (
            snapshot.resource_waiters,
            snapshot.waiting_records,
            snapshot.waiting_bytes
        ),
        (2, 2, 0)
    );
    let pending = snapshot
        .scopes
        .iter()
        .find(|node| node.id == scope.id())
        .unwrap();
    assert_eq!(
        (
            pending.resource_waiters,
            pending.resource_holders,
            pending.reserved_bytes,
            pending.used_bytes
        ),
        (2, 0, 0, 0)
    );
    assert_eq!(authority.snapshot().held_bytes(), 128);
    drop((data_wait, control_wait));
    assert_eq!(control.snapshot().waiting_records, 0);
    let mut replacement = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Data));
    assert!(poll(&mut replacement).is_pending());
    drop(replacement);
    assert_eq!(control.snapshot().waiting_records, 0);
    drop((data, cleanup));
}

#[tokio::test]
async fn stage_and_resource_waits_share_one_global_entry_limit() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let blocking_child = child(&blocker.owner.scope());
    let first_execution = blocker.owner.scope().try_acquire(Stage::Execution).unwrap();
    let second_execution = blocking_child
        .scope()
        .try_acquire(Stage::Execution)
        .unwrap();
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let stage_parent = root(&control, WorkClass::MaterializedView);
    let stage_children = (0..3)
        .map(|_| child(&stage_parent.owner.scope()))
        .collect::<Vec<_>>();
    let mut stages = stage_children[..2]
        .iter()
        .map(|work| request(&work.scope()))
        .collect::<Vec<_>>();
    let owners = (0..3)
        .map(|_| root(&control, WorkClass::Query))
        .collect::<Vec<_>>();
    let scopes = owners
        .iter()
        .map(|work| work.owner.scope())
        .collect::<Vec<_>>();
    let mut waits = scopes[..2]
        .iter()
        .map(|scope| Box::pin(authority.wait_for_capacity(scope, 1, ResourceClass::Data)))
        .collect::<Vec<_>>();
    for waiting in &mut waits {
        assert!(poll(waiting).is_pending());
    }
    let snapshot = control.snapshot();
    assert_eq!(
        (
            snapshot.admission_records,
            snapshot.resource_waiters,
            snapshot.waiting_records
        ),
        (2, 2, 4)
    );
    assert_eq!(snapshot.waiting_bytes, 64);
    assert!(matches!(
        stage_children[2].scope().acquire(StageRequest {
            stage: Stage::Execution,
            retained_bytes: 32
        }),
        Err(WorkError::Capacity("waiting entries"))
    ));
    assert_eq!(
        authority
            .wait_for_capacity(&scopes[2], 1, ResourceClass::Data)
            .await,
        Err(WorkError::Capacity("waiting entries"))
    );
    drop(stages.remove(0));
    let mut replacement = Box::pin(authority.wait_for_capacity(&scopes[2], 1, ResourceClass::Data));
    assert!(poll(&mut replacement).is_pending());
    waits.push(replacement);
    assert_eq!(control.snapshot().waiting_records, 4);
    drop(waits.remove(0));
    stages.push(request(&stage_children[2].scope()));
    assert_eq!(control.snapshot().waiting_records, 4);
    assert_eq!(control.snapshot().peak_waiting_records, 4);
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop((stages, waits));
    let snapshot = control.snapshot();
    assert_eq!(
        (
            snapshot.waiting_records,
            snapshot.waiting_bytes,
            snapshot.resource_waiters
        ),
        (0, 0, 0)
    );
    assert_eq!(snapshot.execution, 2);
    drop((memory, first_execution, second_execution));
}

#[tokio::test]
async fn pending_resource_wait_retains_completed_scope_until_future_drop() {
    let control = control();
    let blocker = root(&control, WorkClass::Query);
    let authority = control.resources();
    let memory = authority
        .reserve(&blocker.owner.scope(), 112, ResourceClass::Data)
        .unwrap();
    let parent = root(&control, WorkClass::MaterializedView);
    let query = child(&parent.owner.scope());
    let scope = query.scope();
    let mut waiting = Box::pin(authority.wait_for_capacity(&scope, 1, ResourceClass::Data));
    assert!(poll(&mut waiting).is_pending());
    query.complete();
    parent.owner.complete();
    parent.business.release();
    let snapshot = control.snapshot();
    assert_eq!(snapshot.root_responsibilities, 2);
    let pending = snapshot
        .scopes
        .iter()
        .find(|node| node.id == scope.id())
        .unwrap();
    assert!(pending.own_completed);
    assert_eq!((pending.resource_waiters, pending.resource_holders), (1, 0));
    drop(waiting);
    let snapshot = control.snapshot();
    assert_eq!(
        (snapshot.root_responsibilities, snapshot.waiting_records),
        (1, 0)
    );
    assert!(!snapshot.scopes.iter().any(|node| node.id == scope.id()));
    assert_eq!(authority.snapshot().held_bytes(), 112);
    drop(memory);
}
