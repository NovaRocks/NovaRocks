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

use super::*;
use novarocks_control_runtime::{
    exit_work::run_future,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{Consumption, SlotState},
    identity::{Generation, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    future::{Ready, poll_fn, ready},
    sync::atomic::{AtomicUsize, Ordering},
};
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        Self { ledger, scope }
    }
    fn reported(&self, parent: Option<WorkId>) -> ReportedWork {
        let (work, mut capacity) = self
            .scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                parent.map_or(WorkAdmission::External, WorkAdmission::Derived),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            Generation::new(1).unwrap(),
            1,
            [(observer(), ConsumerKind::Observer)],
        )
        .unwrap();
        ReportedWork::try_new(work, capacity, report).unwrap()
    }
    fn finish(self) {
        self.ledger.reap_scope_ready(self.scope.id(), 128).unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn observer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}

#[tokio::test]
async fn transport_local_join_precedes_full_child_tree_retirement() {
    let fixture = Fixture::new();
    let parent = fixture.reported(None);
    let report = parent.report().clone();
    let child = fixture.reported(Some(parent.work().id()));
    let child_report = child.report().clone();
    let (child_custody, child_exit) = ExitCustody::from_admitted(child);
    let panic = PanicSink::new();
    let inner = panic.clone();
    let mut parent = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        parent,
        observer(),
        panic,
        move |exit, _| {
            let original = run_future(exit, async {}, inner);
            async move {
                original.await.unwrap();
            }
        },
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let complete = poll_fn(|cx| parent.poll_shared_completion(cx))
        .await
        .unwrap();
    let local = parent.role.custody.body_exited() && parent.role.custody.captures_exited();
    let parent_waited = !parent.try_retire().unwrap();
    let before = report.publication_state().unwrap();
    let consumed = report.consumption(observer()).unwrap();
    child_exit.revoke_unused_and_exit().unwrap();
    let unused = child_report.publication_state().unwrap();
    assert!(child_custody.try_retire().unwrap());
    // ExitCustody returns the child's capacity; the original bounded runtime
    // reaper separately retires its header and removes the parent child link.
    let parent_waited_for_reaper = !parent.try_retire().unwrap();
    let reaped = fixture
        .ledger
        .reap_scope_ready(fixture.scope.id(), 1)
        .unwrap();
    assert!(parent.try_retire().unwrap());
    fixture.finish();
    assert!(complete.is_none() && local && parent_waited && parent_waited_for_reaper);
    assert_eq!(reaped, 1);
    assert_eq!(before, SlotState::Filled);
    assert_eq!(consumed, Consumption::Consumed);
    assert_eq!(unused, SlotState::RevokedUnused);
}

#[tokio::test]
async fn unused_transport_report_is_revoked_without_fabricated_consumer_ack() {
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let report = original.report().clone();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let mut role = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        original,
        observer(),
        panic,
        move |exit, _| {
            drop(run_future(exit, std::future::pending::<()>(), inner));
            ready(())
        },
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let result = poll_fn(|cx| role.poll_shared_completion(cx)).await.unwrap();
    let publication = report.publication_state().unwrap();
    let consumption = report.consumption(observer()).unwrap();
    assert!(role.try_retire().unwrap());
    fixture.finish();
    assert!(result.is_none());
    assert_eq!(publication, SlotState::RevokedUnused);
    assert_eq!(consumption, Consumption::Pending);
}

struct OriginalPanic(Arc<AtomicUsize>);
impl Drop for OriginalPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn factory_panic_returns_owned_failed_role_and_same_raw_cause_without_join_proof() {
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let dropped = Arc::new(AtomicUsize::new(0));
    let captured = dropped.clone();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let mut role = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        original,
        observer(),
        panic,
        move |exit, _| -> Ready<()> {
            drop(run_future(exit, std::future::pending::<()>(), inner));
            std::panic::panic_any(OriginalPanic(captured));
        },
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let observed = poll_fn(|cx| role.poll_shared_completion(cx)).await;
    let retained = dropped.load(Ordering::SeqCst) == 0;
    let cause = role.take_handoff_failure().unwrap();
    let same = cause.is::<OriginalPanic>();
    let no_duplicate = role.take_handoff_failure().is_none();
    drop(cause);
    assert!(role.retire_unused_without_join().unwrap());
    fixture.finish();
    assert!(matches!(observed, Err(CustodyError::OriginalHandoffFailed)));
    assert!(retained && same && no_duplicate);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_observer_returns_original_reported_work_before_factory_runs() {
    let fixture = Fixture::new();
    let original = fixture.reported(None);
    let report = original.report().clone();
    let ran = Arc::new(AtomicUsize::new(0));
    let actual = ran.clone();
    let result = OriginalTransportJoinObligation::spawn(
        &Handle::current(),
        original,
        ConsumerId::new(99).unwrap(),
        PanicSink::new(),
        move |_, _| {
            actual.fetch_add(1, Ordering::SeqCst);
            ready(())
        },
    );
    let refused = match result {
        Err(refused) => refused,
        Ok(_) => panic!("foreign observer cannot own this role"),
    };
    let same = refused.original.report().same_slot(&report);
    let (custody, exit) = ExitCustody::from_admitted(refused.original);
    exit.revoke_unused_and_exit().unwrap();
    assert!(custody.try_retire().unwrap());
    fixture.finish();
    assert!(same);
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}
